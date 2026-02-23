from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

import steam_one_time_crawl as crawl_mod
from steam_project_paths import DATASETS_ROOT, PROJECT_ROOT, dataset_display_id, ensure_datasets_root, resolve_dataset_path


WORKSPACE_ROOT = PROJECT_ROOT
DEFAULT_DATASET_DIR = "steam_dataset"
STEAM_APPDETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={appid}&l=english"
APPDETAILS_COMPACT_DIRNAME = "appdetails"
APPDETAILS_RAW_DIRNAME = "appdetails_raw"
EXPLORE_SCHEMA_CACHE_FILENAME = ".explore_schema_cache.json"
EXPLORE_SCHEMA_MAX_REVIEW_ROWS = 40000
MAX_RPM_WITH_ENRICH_APPDETAILS = 240
APPDETAILS_SAFE_MAX_RPM = 100
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class CrawlJobRequest(BaseModel):
    tags: list[str] = Field(..., min_length=1)
    exclude_tags: list[str] | None = Field(default=None)
    exclude_coming_soon: bool = Field(default=True)
    dataset_dir: str = Field(default=DEFAULT_DATASET_DIR)
    max_games: int | None = Field(default=None, ge=1)
    search_page_size: int = Field(default=50, ge=1, le=100)
    sleep_search: float = Field(default=0.1, ge=0.0, le=10.0)
    sleep_reviews: float = Field(default=0.25, ge=0.0, le=10.0)
    max_review_pages_per_game: int | None = Field(default=None, ge=1)
    timeout: int = Field(default=30, ge=5, le=300)
    resume: bool = Field(default=True)
    enrich_appdetails: bool = Field(default=True)
    store_raw_appdetails: bool = Field(default=True)
    workers: int = Field(default=12, ge=1, le=64)
    max_rpm: int = Field(default=800, ge=0, le=10000)
    http_retries: int = Field(default=6, ge=1, le=20)
    http_retry_base_sleep: float = Field(default=1.0, ge=0.0, le=120.0)
    http_retry_jitter: float = Field(default=0.6, ge=0.0, le=120.0)
    max_app_retries: int = Field(default=4, ge=1, le=20)
    review_source: str = Field(default="appreviews", pattern="^(appreviews|community)$")
    appreviews_num_per_page: int = Field(default=100, ge=1, le=100)
    include_app_types: list[str] | None = Field(default=None)
    exclude_app_types: list[str] | None = Field(default=None)


class CrawlEstimateRequest(BaseModel):
    tags: list[str] = Field(..., min_length=1)
    exclude_tags: list[str] | None = Field(default=None)
    include_app_types: list[str] | None = Field(default=None)
    exclude_app_types: list[str] | None = Field(default=None)
    timeout: int = Field(default=30, ge=5, le=300)
    search_page_size: int = Field(default=50, ge=1, le=100)
    sample_size: int = Field(default=60, ge=1, le=300)
    max_games: int | None = Field(default=None, ge=1)


class SearchJobRequest(BaseModel):
    dataset_dir: str = Field(default=DEFAULT_DATASET_DIR)
    keywords: list[str] = Field(..., min_length=1)
    sample_per_game: int = Field(default=2, ge=1, le=20)
    keyword_mode: str = Field(default="any", pattern="^(any|all)$")
    case_sensitive: bool = Field(default=False)
    min_matched_reviews: int = Field(default=1, ge=1)
    top_n: int | None = Field(default=None, ge=1)


class ExploreFilter(BaseModel):
    field: str = Field(..., min_length=1)
    op: str = Field(
        default="eq",
        pattern=(
            "^(eq|neq|contains|not_contains|in|not_in|gte|lte|gt|lt|between|"
            "exists|not_exists|array_contains|array_any|array_all)$"
        ),
    )
    value: Any | None = None
    value_to: Any | None = None
    case_sensitive: bool = Field(default=False)


class ExploreQueryRequest(BaseModel):
    include_keywords: list[str] | None = Field(default=None)
    exclude_keywords: list[str] | None = Field(default=None)
    keyword_mode: str = Field(default="any", pattern="^(any|all)$")
    case_sensitive: bool = Field(default=False)
    min_matched_reviews: int = Field(default=1, ge=1)
    game_filters: list[ExploreFilter] | None = Field(default=None)
    review_filters: list[ExploreFilter] | None = Field(default=None)
    sort_by: str = Field(
        default="matched_reviews",
        pattern="^(matched_reviews|match_rate|name|total_reviews|positive_matched_reviews|negative_matched_reviews)$",
    )
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")
    skip: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=500)
    sample_reviews_per_game: int = Field(default=2, ge=0, le=20)


class ExploreGameReviewsRequest(BaseModel):
    include_keywords: list[str] | None = Field(default=None)
    exclude_keywords: list[str] | None = Field(default=None)
    keyword_mode: str = Field(default="any", pattern="^(any|all)$")
    case_sensitive: bool = Field(default=False)
    review_filters: list[ExploreFilter] | None = Field(default=None)
    sort_by: str = Field(default="timestamp_desc", pattern="^(none|timestamp_desc|timestamp_asc|hours_desc|hours_asc)$")
    skip: int = Field(default=0, ge=0)
    limit: int = Field(default=200, ge=1, le=2000)
    highlight: bool = Field(default=True)


class JobCancelledError(Exception):
    pass


@dataclass
class JobState:
    id: str
    job_type: str
    params: dict[str, Any]
    status: str = "queued"
    progress: float = 0.0
    message: str = ""
    error: str | None = None
    result: dict[str, Any] | None = None
    cancel_requested: bool = False
    cancel_requested_at: float | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobState] = {}

    def create(self, job_type: str, params: dict[str, Any]) -> JobState:
        job = JobState(id=str(uuid4()), job_type=job_type, params=params)
        with self._lock:
            self._jobs[job.id] = job
        return job

    def list_jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            jobs = list(self._jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return [self._to_dict(x) for x in jobs]

    def get(self, job_id: str) -> JobState:
        with self._lock:
            job = self._jobs.get(job_id)
        if not job:
            raise KeyError(job_id)
        return job

    def patch(self, job_id: str, **kwargs: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for key, value in kwargs.items():
                setattr(job, key, value)

    def finish_ok(self, job_id: str, result: dict[str, Any]) -> None:
        self.patch(job_id, status="completed", progress=1.0, result=result, finished_at=time.time())

    def finish_error(self, job_id: str, error: str) -> None:
        self.patch(job_id, status="failed", error=error, finished_at=time.time())

    def finish_cancelled(self, job_id: str, message: str = "Cancelled by user") -> None:
        self.patch(
            job_id,
            status="cancelled",
            message=message,
            finished_at=time.time(),
        )

    def request_cancel(self, job_id: str) -> JobState:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(job_id)

            if job.status in {"completed", "failed", "cancelled"}:
                return job

            job.cancel_requested = True
            job.cancel_requested_at = time.time()
            if job.status == "queued":
                job.status = "cancelled"
                job.message = "Cancelled before start"
                job.finished_at = time.time()
            elif not job.message:
                job.message = "Cancellation requested"
            return job

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            return bool(job.cancel_requested)

    def as_dict(self, job_id: str) -> dict[str, Any]:
        return self._to_dict(self.get(job_id))

    @staticmethod
    def _to_dict(job: JobState) -> dict[str, Any]:
        return {
            "id": job.id,
            "type": job.job_type,
            "params": job.params,
            "status": job.status,
            "progress": job.progress,
            "message": job.message,
            "error": job.error,
            "result": job.result,
            "cancel_requested": job.cancel_requested,
            "cancel_requested_at": job.cancel_requested_at,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
        }


def safe_dataset_path(dataset_dir: str) -> Path:
    try:
        return resolve_dataset_path(dataset_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_release_year(date_text: str | None) -> int | None:
    if not date_text:
        return None
    m = re.search(r"(19|20)\d{2}", date_text)
    return int(m.group(0)) if m else None


def parse_hours_on_record(raw: str | None) -> float | None:
    if not raw:
        return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*hrs", raw.lower())
    return float(m.group(1)) if m else None


def parse_datetime_to_unix(value: str | None) -> float | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    # Already numeric timestamp.
    if re.fullmatch(r"\d{10}(?:\.\d+)?", text):
        return float(text)
    if re.fullmatch(r"\d{13}", text):
        return float(text) / 1000.0

    try:
        iso = text.replace("Z", "+00:00")
        return datetime.fromisoformat(iso).timestamp()
    except (ValueError, OSError):
        pass

    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d.%m.%Y",
        "%m/%d/%Y",
        "%d %b, %Y",
        "%b %d, %Y",
        "%d %B, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%b %d %Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).timestamp()
        except (ValueError, OSError):
            continue
    return None


def extract_numeric_scalar_from_string(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        pass

    hrs = parse_hours_on_record(text)
    if hrs is not None:
        return hrs

    unit_match = re.fullmatch(r"^\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:usd|eur|rub|\$|€)?$", normalized.casefold())
    if unit_match:
        return float(unit_match.group(1))

    return None


def highlight_matches(text: str, keywords: list[str], case_sensitive: bool = False) -> str:
    if not keywords or not text:
        return text
    escaped = [re.escape(k) for k in keywords if k]
    if not escaped:
        return text
    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = re.compile("(" + "|".join(escaped) + ")", flags=flags)
    return pattern.sub(r"<mark>\1</mark>", text)


def appdetails_paths(dataset_dir: Path, appid: int) -> tuple[Path, Path]:
    compact_path = dataset_dir / APPDETAILS_COMPACT_DIRNAME / f"{appid}.json"
    raw_path = dataset_dir / APPDETAILS_RAW_DIRNAME / f"{appid}.json"
    return compact_path, raw_path


_FIELD_MISSING = object()


def normalize_field_params(field: list[str] | None) -> list[str]:
    values: list[str] = []
    for item in field or []:
        for token in str(item).split(","):
            clean = token.strip()
            if clean and clean not in values:
                values.append(clean)
    return values


def get_value_by_path(data: Any, path: str) -> Any:
    current = data
    parts = [x for x in path.split(".") if x]
    if not parts:
        return _FIELD_MISSING

    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return _FIELD_MISSING
            current = current.get(part)
            continue

        if isinstance(current, list):
            if not part.isdigit():
                return _FIELD_MISSING
            idx = int(part)
            if idx < 0 or idx >= len(current):
                return _FIELD_MISSING
            current = current[idx]
            continue

        return _FIELD_MISSING

    return current


def extract_fields(record: Any, fields: list[str]) -> tuple[dict[str, Any], list[str]]:
    values: dict[str, Any] = {}
    missing: list[str] = []
    for path in fields:
        value = get_value_by_path(record, path)
        if value is _FIELD_MISSING:
            missing.append(path)
        else:
            values[path] = value
    return values, missing


def find_game_row(dataset_dir: Path, appid: int) -> dict[str, Any] | None:
    for row in read_jsonl(dataset_dir / "games.jsonl"):
        if row.get("appid") == appid:
            return row
    return None


def get_or_refresh_dataset_appdetails(
    dataset_dir: Path,
    appid: int,
    timeout: int,
    refresh: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    compact_path, raw_path = appdetails_paths(dataset_dir, appid)
    compact = read_json(compact_path)
    raw = read_json(raw_path)

    needs_fetch = refresh or compact is None or raw is None
    if needs_fetch:
        details = fetch_appdetails_live(appid, timeout)
        if details:
            raw = details
            compact = compact_appdetails(details, appid)
            write_json(compact_path, compact)
            write_json(raw_path, raw)

    return compact, raw


def fetch_appdetails_live(appid: int, timeout: int) -> dict[str, Any] | None:
    url = STEAM_APPDETAILS_URL.format(appid=appid)
    payload = json.loads(crawl_mod.http_get(url, timeout))
    item = payload.get(str(appid), {})
    if not item.get("success"):
        return None
    data = item.get("data")
    return data if isinstance(data, dict) else None


def compact_appdetails(data: dict[str, Any], appid: int) -> dict[str, Any]:
    release = data.get("release_date") if isinstance(data.get("release_date"), dict) else {}
    date_text = release.get("date") if isinstance(release.get("date"), str) else ""
    release_year = parse_release_year(date_text)

    recommendations = data.get("recommendations") if isinstance(data.get("recommendations"), dict) else {}
    rec_total = recommendations.get("total") if isinstance(recommendations.get("total"), int) else None

    metacritic = data.get("metacritic") if isinstance(data.get("metacritic"), dict) else {}
    metacritic_score = metacritic.get("score") if isinstance(metacritic.get("score"), int) else None

    price = data.get("price_overview") if isinstance(data.get("price_overview"), dict) else {}
    initial = price.get("initial") if isinstance(price.get("initial"), int) else None
    final = price.get("final") if isinstance(price.get("final"), int) else None
    discount = price.get("discount_percent") if isinstance(price.get("discount_percent"), int) else None
    currency = price.get("currency") if isinstance(price.get("currency"), str) else None

    platforms = data.get("platforms") if isinstance(data.get("platforms"), dict) else {}

    categories = data.get("categories") if isinstance(data.get("categories"), list) else []
    genres = data.get("genres") if isinstance(data.get("genres"), list) else []
    category_names = [x.get("description") for x in categories if isinstance(x, dict) and isinstance(x.get("description"), str)]
    genre_names = [x.get("description") for x in genres if isinstance(x, dict) and isinstance(x.get("description"), str)]

    result = {
        "appid": appid,
        "name": data.get("name"),
        "type": data.get("type"),
        "is_free": bool(data.get("is_free", False)),
        "required_age": data.get("required_age"),
        "release_date_text": date_text,
        "release_year": release_year,
        "coming_soon": bool(release.get("coming_soon", False)),
        "recommendations_total": rec_total,
        "metacritic_score": metacritic_score,
        "price_initial": initial,
        "price_final": final,
        "price_discount_percent": discount,
        "price_currency": currency,
        "platforms": {
            "windows": bool(platforms.get("windows", False)),
            "mac": bool(platforms.get("mac", False)),
            "linux": bool(platforms.get("linux", False)),
        },
        "developers": data.get("developers") if isinstance(data.get("developers"), list) else [],
        "publishers": data.get("publishers") if isinstance(data.get("publishers"), list) else [],
        "genres": genre_names,
        "categories": category_names,
        "supported_languages_raw": data.get("supported_languages") if isinstance(data.get("supported_languages"), str) else "",
        "short_description": data.get("short_description") if isinstance(data.get("short_description"), str) else "",
        "website": data.get("website") if isinstance(data.get("website"), str) else "",
    }
    return result


def load_review_counts(dataset_dir: Path) -> dict[int, int]:
    counts: dict[int, int] = {}
    reviews_dir = dataset_dir / "reviews"
    if not reviews_dir.exists():
        return counts

    for path in reviews_dir.glob("*.jsonl"):
        if not path.stem.isdigit():
            continue
        appid = int(path.stem)
        with path.open("r", encoding="utf-8") as f:
            counts[appid] = sum(1 for line in f if line.strip())
    return counts


def normalize_text(text: str, case_sensitive: bool) -> str:
    return text if case_sensitive else text.casefold()


def keyword_hit(text: str, keywords: list[str], mode: str, case_sensitive: bool) -> tuple[bool, list[str]]:
    if not keywords:
        return True, []
    text_norm = normalize_text(text, case_sensitive)
    kw_norm = [normalize_text(x, case_sensitive) for x in keywords if x.strip()]
    hits = [k for k in kw_norm if k in text_norm]
    if mode == "all":
        return len(hits) == len(kw_norm), hits
    return len(hits) > 0, hits


def unique_strings(values: list[str] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        value = str(item).strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def parse_multi_value(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw]
    if isinstance(raw, tuple):
        return [x for x in raw]
    if isinstance(raw, str):
        return [x.strip() for x in raw.split(",") if x.strip()]
    return [raw]


def to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None

        scalar = extract_numeric_scalar_from_string(stripped)
        if scalar is not None:
            return scalar

        dt = parse_datetime_to_unix(stripped)
        if dt is not None:
            return dt

        # Fallback for mixed strings (e.g. free text with a number token).
        m = re.search(r"[-+]?[0-9]+(?:[.,][0-9]+)?", stripped)
        if m:
            try:
                return float(m.group(0).replace(",", "."))
            except ValueError:
                return None

        stripped = stripped.replace(",", ".")
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().casefold()
        if v in {"1", "true", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "no", "n", "off"}:
            return False
    return None


def normalize_for_compare(value: Any, case_sensitive: bool) -> Any:
    if isinstance(value, str):
        return value if case_sensitive else value.casefold()
    return value


def value_exists(value: Any) -> bool:
    if value is _FIELD_MISSING:
        return False
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def field_filter_match(value: Any, flt: ExploreFilter) -> bool:
    op = flt.op
    case_sensitive = bool(flt.case_sensitive)
    exists = value_exists(value)

    if op == "exists":
        return exists
    if op == "not_exists":
        return not exists
    if not exists:
        return False

    if op in {"eq", "neq"}:
        if isinstance(value, bool):
            right_bool = to_bool(flt.value)
            if right_bool is None:
                right_bool = bool(flt.value)
            result = value == right_bool
            return result if op == "eq" else not result
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            left_num = float(value)
            right_num = to_float(flt.value)
            if right_num is None:
                left_cmp = normalize_for_compare(value, case_sensitive)
                right_cmp = normalize_for_compare(str(flt.value), case_sensitive)
                result = left_cmp == right_cmp
                return result if op == "eq" else not result
            result = left_num == right_num
            return result if op == "eq" else not result

        left_cmp = normalize_for_compare(value, case_sensitive)
        right_cmp = normalize_for_compare(flt.value, case_sensitive)
        result = left_cmp == right_cmp
        return result if op == "eq" else not result

    if op in {"contains", "not_contains"}:
        needle = str(flt.value or "")
        if not case_sensitive:
            needle = needle.casefold()
        if isinstance(value, list):
            haystack_items = [str(x) for x in value]
            if not case_sensitive:
                haystack_items = [x.casefold() for x in haystack_items]
            result = any(needle in item for item in haystack_items)
        else:
            haystack = str(value)
            if not case_sensitive:
                haystack = haystack.casefold()
            result = needle in haystack
        return result if op == "contains" else not result

    if op in {"in", "not_in"}:
        raw_values = parse_multi_value(flt.value)
        values_cmp = [normalize_for_compare(x, case_sensitive) for x in raw_values]
        left_cmp = normalize_for_compare(value, case_sensitive)
        result = left_cmp in values_cmp
        return result if op == "in" else not result

    if op in {"gte", "lte", "gt", "lt", "between"}:
        left_num = to_float(value)
        if left_num is None:
            return False

        if op == "between":
            low = to_float(flt.value)
            high = to_float(flt.value_to)
            if low is None or high is None:
                return False
            if low > high:
                low, high = high, low
            return low <= left_num <= high

        right_num = to_float(flt.value)
        if right_num is None:
            return False
        if op == "gte":
            return left_num >= right_num
        if op == "lte":
            return left_num <= right_num
        if op == "gt":
            return left_num > right_num
        return left_num < right_num

    if op in {"array_contains", "array_any", "array_all"}:
        if not isinstance(value, list):
            return False
        normalized_items = [normalize_for_compare(x, case_sensitive) for x in value]

        if op == "array_contains":
            needle = normalize_for_compare(flt.value, case_sensitive)
            return needle in normalized_items

        candidates = [normalize_for_compare(x, case_sensitive) for x in parse_multi_value(flt.value)]
        if not candidates:
            return False
        if op == "array_any":
            return any(candidate in normalized_items for candidate in candidates)
        return all(candidate in normalized_items for candidate in candidates)

    return False


def row_matches_filters(row: dict[str, Any], filters: list[ExploreFilter]) -> bool:
    for flt in filters:
        value = get_value_by_path(row, flt.field)
        if not field_filter_match(value, flt):
            return False
    return True


def review_keyword_match(
    review_text: str,
    include_keywords: list[str],
    exclude_keywords: list[str],
    keyword_mode: str,
    case_sensitive: bool,
) -> tuple[bool, list[str]]:
    if include_keywords:
        include_ok, include_hits = keyword_hit(review_text, include_keywords, keyword_mode, case_sensitive)
        if not include_ok:
            return False, []
    else:
        include_hits = []

    if exclude_keywords:
        exclude_ok, _exclude_hits = keyword_hit(review_text, exclude_keywords, "any", case_sensitive)
        if exclude_ok:
            return False, []

    return True, include_hits


def compact_game_for_explorer(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.pop("short_description", None)
    out.pop("supported_languages_raw", None)
    appid = out.get("appid")
    out["store_url"] = f"https://store.steampowered.com/app/{appid}/" if appid is not None else ""
    return out


def iter_record_fields(record: Any, prefix: str = ""):
    if isinstance(record, dict):
        for key, value in record.items():
            field = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                yield from iter_record_fields(value, field)
            else:
                yield field, value
        return
    if prefix:
        yield prefix, record


def init_field_profile() -> dict[str, Any]:
    return {
        "present_count": 0,
        "null_count": 0,
        "kinds": set(),
        "number_min": None,
        "number_max": None,
        "max_string_length": 0,
        "string_numeric_count": 0,
        "string_numeric_min": None,
        "string_numeric_max": None,
        "string_datetime_count": 0,
        "string_datetime_min": None,
        "string_datetime_max": None,
        "_enum_values": [],
        "_enum_seen": set(),
    }


def _add_enum_value(profile: dict[str, Any], value: Any, max_enum_values: int = 80) -> None:
    if len(profile["_enum_values"]) >= max_enum_values:
        return
    v = str(value)
    if len(v) > 80:
        return
    key = v.casefold()
    if key in profile["_enum_seen"]:
        return
    profile["_enum_seen"].add(key)
    profile["_enum_values"].append(v)


def update_field_profile(profile: dict[str, Any], value: Any) -> None:
    profile["present_count"] += 1
    if value is None:
        profile["null_count"] += 1
        profile["kinds"].add("null")
        return

    if isinstance(value, bool):
        profile["kinds"].add("boolean")
        _add_enum_value(profile, value)
        return

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        profile["kinds"].add("number")
        num = float(value)
        if profile["number_min"] is None or num < profile["number_min"]:
            profile["number_min"] = num
        if profile["number_max"] is None or num > profile["number_max"]:
            profile["number_max"] = num
        _add_enum_value(profile, value)
        return

    if isinstance(value, str):
        profile["kinds"].add("string")
        text_value = str(value)
        profile["max_string_length"] = max(profile["max_string_length"], len(text_value))

        scalar = extract_numeric_scalar_from_string(text_value)
        if scalar is not None:
            profile["string_numeric_count"] += 1
            if profile["string_numeric_min"] is None or scalar < profile["string_numeric_min"]:
                profile["string_numeric_min"] = scalar
            if profile["string_numeric_max"] is None or scalar > profile["string_numeric_max"]:
                profile["string_numeric_max"] = scalar

        dt = parse_datetime_to_unix(text_value)
        if dt is not None:
            profile["string_datetime_count"] += 1
            if profile["string_datetime_min"] is None or dt < profile["string_datetime_min"]:
                profile["string_datetime_min"] = dt
            if profile["string_datetime_max"] is None or dt > profile["string_datetime_max"]:
                profile["string_datetime_max"] = dt

        _add_enum_value(profile, value)
        return

    if isinstance(value, list):
        if not value:
            profile["kinds"].add("array_empty")
            return
        if all(isinstance(x, bool) for x in value):
            profile["kinds"].add("array_boolean")
        elif all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in value):
            profile["kinds"].add("array_number")
        elif all(isinstance(x, str) for x in value):
            profile["kinds"].add("array_string")
        else:
            profile["kinds"].add("array_mixed")
        for item in value:
            _add_enum_value(profile, item)
        return

    profile["kinds"].add("mixed")


def finalize_field_kind(kinds: set[str]) -> str:
    if "number" in kinds and kinds.issubset({"number", "null"}):
        return "number"
    if "boolean" in kinds and kinds.issubset({"boolean", "null"}):
        return "boolean"
    if "array_number" in kinds and kinds.issubset({"array_number", "array_empty", "null"}):
        return "array_number"
    if "array_boolean" in kinds and kinds.issubset({"array_boolean", "array_empty", "null"}):
        return "array_boolean"
    if "array_string" in kinds and kinds.issubset({"array_string", "array_empty", "null"}):
        return "array_string"
    if "string" in kinds and kinds.issubset({"string", "null"}):
        return "string"
    if "array_string" in kinds:
        return "array_string"
    if "array_number" in kinds:
        return "array_number"
    if "array_boolean" in kinds:
        return "array_boolean"
    if "string" in kinds:
        return "string"
    if "number" in kinds:
        return "number"
    if "boolean" in kinds:
        return "boolean"
    return "mixed"


def finalize_schema_profiles(profiles: dict[str, dict[str, Any]], total_records: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field, profile in profiles.items():
        kinds = profile["kinds"]
        kind = finalize_field_kind(kinds)
        item: dict[str, Any] = {
            "field": field,
            "kind": kind,
            "present_count": profile["present_count"],
            "null_count": profile["null_count"],
            "coverage": round(profile["present_count"] / max(1, total_records), 6),
        }
        if kind == "number":
            item["number_min"] = profile["number_min"]
            item["number_max"] = profile["number_max"]
        if kind == "string":
            item["max_string_length"] = profile["max_string_length"]
            item["is_long_text"] = profile["max_string_length"] > 180

            non_null = max(1, profile["present_count"] - profile["null_count"])
            field_lc = str(field).casefold()

            numeric_count = int(profile.get("string_numeric_count") or 0)
            numeric_ratio = numeric_count / non_null
            numeric_name_hint = any(x in field_lc for x in {"hour", "price", "score", "count", "rating", "percent"})
            if numeric_count > 0 and (numeric_ratio >= 0.7 or numeric_name_hint):
                item["numeric_like"] = True
                item["numeric_like_count"] = numeric_count
                item["numeric_like_ratio"] = round(numeric_ratio, 6)
                item["numeric_min"] = profile.get("string_numeric_min")
                item["numeric_max"] = profile.get("string_numeric_max")

            datetime_count = int(profile.get("string_datetime_count") or 0)
            datetime_ratio = datetime_count / non_null
            datetime_name_hint = any(x in field_lc for x in {"date", "time", "timestamp", "created", "updated"})
            if datetime_count > 0 and (datetime_ratio >= 0.5 or datetime_name_hint):
                item["datetime_like"] = True
                item["datetime_like_count"] = datetime_count
                item["datetime_like_ratio"] = round(datetime_ratio, 6)
                item["datetime_min"] = profile.get("string_datetime_min")
                item["datetime_max"] = profile.get("string_datetime_max")

        enum_values = profile["_enum_values"]
        if enum_values and len(enum_values) <= 80:
            if kind == "number":
                item["enum_values"] = sorted({to_float(x) for x in enum_values if to_float(x) is not None})
            elif kind in {"boolean", "array_boolean"}:
                bool_values = []
                for x in enum_values:
                    bv = to_bool(x)
                    if bv is not None and bv not in bool_values:
                        bool_values.append(bv)
                item["enum_values"] = bool_values
            else:
                item["enum_values"] = sorted(enum_values, key=lambda x: x.casefold())
        rows.append(item)

    rows.sort(key=lambda x: x["field"])
    return rows


def jsonl_line_count(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def list_available_datasets() -> list[dict[str, Any]]:
    ensure_datasets_root()
    items: list[dict[str, Any]] = []
    for path in sorted(DATASETS_ROOT.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not path.is_dir():
            continue
        games_file = path / "games.jsonl"
        reviews_dir = path / "reviews"
        if not games_file.exists() or not reviews_dir.exists():
            continue

        review_files = list(reviews_dir.glob("*.jsonl"))
        meta_path = path / "meta.json"
        meta = read_json(meta_path) if meta_path.exists() else {}
        if not isinstance(meta, dict):
            meta = {}
        tags = [str(x) for x in meta.get("input_tags", []) if str(x).strip()]
        app_types = [str(x) for x in (meta.get("config", {}) or {}).get("include_app_types", []) if str(x).strip()]

        label_parts = [path.name]
        if app_types:
            label_parts.append("/".join(app_types))
        if tags:
            label_parts.append(", ".join(tags[:3]))

        items.append(
            {
                "name": path.name,
                "dataset_dir": dataset_display_id(path),
                "label": " | ".join(label_parts),
                "games_count": jsonl_line_count(games_file),
                "review_files_count": len(review_files),
                "meta_exists": meta_path.exists(),
                "input_tags": tags,
                "include_app_types": app_types,
                "updated_at_unix": int(path.stat().st_mtime),
            }
        )
    return items


def aggregate_keyword_matches(
    dataset_dir: Path,
    keywords: list[str],
    sample_per_game: int,
    keyword_mode: str,
    case_sensitive: bool,
    min_matched_reviews: int,
    top_n: int | None,
) -> list[dict[str, Any]]:
    games_map = {x.get("appid"): x for x in read_jsonl(dataset_dir / "games.jsonl") if isinstance(x.get("appid"), int)}
    reviews_dir = dataset_dir / "reviews"
    if not reviews_dir.exists():
        raise FileNotFoundError(f"Missing {reviews_dir}")

    rows: list[dict[str, Any]] = []
    for review_file in sorted(reviews_dir.glob("*.jsonl")):
        if not review_file.stem.isdigit():
            continue
        appid = int(review_file.stem)

        total_reviews = 0
        matched_reviews = 0
        matched_keywords: set[str] = set()
        samples: list[str] = []

        with review_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_reviews += 1
                row = json.loads(line)
                text = str(row.get("review_text", ""))
                ok, hits = keyword_hit(text, keywords, keyword_mode, case_sensitive)
                if not ok:
                    continue

                matched_reviews += 1
                matched_keywords.update(hits)
                if len(samples) < sample_per_game:
                    samples.append(" ".join(text.split())[:260])

        if matched_reviews < min_matched_reviews:
            continue

        game = games_map.get(appid, {})
        rows.append(
            {
                "appid": appid,
                "name": game.get("name", f"appid_{appid}"),
                "input_tags": game.get("input_tags", []),
                "search_tags": game.get("search_tags", []),
                "total_reviews": total_reviews,
                "matched_reviews": matched_reviews,
                "match_rate": round((matched_reviews / total_reviews), 6) if total_reviews else 0.0,
                "matched_keywords": sorted(matched_keywords),
                "sample_reviews": samples,
            }
        )

    rows.sort(key=lambda x: x["matched_reviews"], reverse=True)
    if top_n:
        rows = rows[:top_n]
    return rows


def run_crawl_job(job_id: str, req: CrawlJobRequest, jobs: JobManager) -> None:
    try:
        jobs.patch(job_id, status="running", started_at=time.time(), progress=0.01, message="Stage 1/3: loading tags")
        LOGGER.info(
            "[job=%s] Crawl started dataset=%s tags=%s exclude_tags=%s include_types=%s exclude_types=%s "
            "max_games=%s workers=%s max_rpm=%s enrich_appdetails=%s exclude_coming_soon=%s",
            job_id,
            req.dataset_dir,
            req.tags,
            req.exclude_tags or [],
            req.include_app_types or [],
            req.exclude_app_types or [],
            req.max_games,
            req.workers,
            req.max_rpm,
            req.enrich_appdetails,
            req.exclude_coming_soon,
        )

        runtime_max_rpm = int(req.max_rpm)
        if req.enrich_appdetails and runtime_max_rpm > MAX_RPM_WITH_ENRICH_APPDETAILS:
            runtime_max_rpm = MAX_RPM_WITH_ENRICH_APPDETAILS
            jobs.patch(
                job_id,
                message=(
                    f"Stage 1/3: loading tags (max_rpm auto-capped to {runtime_max_rpm} "
                    "because enrich_appdetails=true)"
                ),
            )
            LOGGER.warning(
                "[job=%s] max_rpm auto-capped from %s to %s because enrich_appdetails=true",
                job_id,
                req.max_rpm,
                runtime_max_rpm,
            )
        stage2_appdetails_rpm = (
            min(runtime_max_rpm, APPDETAILS_SAFE_MAX_RPM)
            if req.enrich_appdetails
            else runtime_max_rpm
        )

        crawl_mod.configure_http_runtime(
            max_rpm=runtime_max_rpm,
            http_retries=req.http_retries,
            retry_base_sleep=req.http_retry_base_sleep,
            retry_jitter=req.http_retry_jitter,
        )

        dataset_dir = safe_dataset_path(req.dataset_dir)
        reviews_dir = dataset_dir / "reviews"
        games_file = dataset_dir / "games.jsonl"
        completed_file = dataset_dir / "completed_appids.txt"
        errors_file = dataset_dir / "errors.jsonl"
        meta_file = dataset_dir / "meta.json"
        appdetails_dir = dataset_dir / APPDETAILS_COMPACT_DIRNAME
        appdetails_raw_dir = dataset_dir / APPDETAILS_RAW_DIRNAME
        reviews_dir.mkdir(parents=True, exist_ok=True)
        appdetails_dir.mkdir(parents=True, exist_ok=True)
        if req.store_raw_appdetails:
            appdetails_raw_dir.mkdir(parents=True, exist_ok=True)

        all_tags = crawl_mod.fetch_english_tags(req.timeout)
        resolved_tags, unknown = crawl_mod.resolve_input_tags(req.tags, all_tags)
        if unknown:
            raise RuntimeError("Unknown tags: " + ", ".join(unknown))
        if not resolved_tags:
            raise RuntimeError("No valid tags resolved")

        resolved_exclude_tags, unknown_exclude = crawl_mod.resolve_input_tags(req.exclude_tags or [], all_tags)
        if unknown_exclude:
            raise RuntimeError("Unknown exclude_tags: " + ", ".join(unknown_exclude))

        tag_ids = [x["tagid"] for x in resolved_tags]
        exclude_tag_ids = [x["tagid"] for x in resolved_exclude_tags]
        tag_name_by_id = {x["tagid"]: x["name"] for x in all_tags}

        include_types = {
            crawl_mod.normalize_app_type(x)
            for x in (req.include_app_types or [])
            if isinstance(x, str) and x.strip()
        }
        exclude_types = {
            crawl_mod.normalize_app_type(x)
            for x in (req.exclude_app_types or [])
            if isinstance(x, str) and x.strip()
        }
        use_type_filter = bool(include_types or exclude_types)
        search_category1, search_category1_app_type = crawl_mod.infer_search_category1(include_types, exclude_types)

        SEARCH_STAGE_START = 0.03
        SEARCH_STAGE_END = 0.25
        PREP_STAGE_END = 0.40
        REVIEW_STAGE_START = PREP_STAGE_END
        REVIEW_STAGE_END = 0.99

        def _check_cancel(stage_message: str) -> None:
            if jobs.is_cancel_requested(job_id):
                raise JobCancelledError(stage_message)

        def _sleep_with_cancel(seconds: float, stage_message: str) -> None:
            if seconds <= 0:
                return
            deadline = time.time() + float(seconds)
            while True:
                _check_cancel(stage_message)
                remain = deadline - time.time()
                if remain <= 0:
                    break
                time.sleep(min(1.0, remain))

        def _phase_progress(start: float, end: float, ratio: float) -> float:
            clamped = max(0.0, min(1.0, float(ratio)))
            return start + (end - start) * clamped

        def _review_progress(done_items: int, total_items: int) -> float:
            if total_items <= 0:
                return REVIEW_STAGE_END
            ratio = done_items / max(1, total_items)
            return _phase_progress(REVIEW_STAGE_START, REVIEW_STAGE_END, ratio)

        jobs.patch(job_id, progress=SEARCH_STAGE_START, message="Stage 1/3: searching games by tags")
        if use_type_filter:
            if search_category1 is not None:
                jobs.patch(
                    job_id,
                    progress=SEARCH_STAGE_START,
                    message=(
                        "Stage 1/3: searching games by tags "
                        f"(optimized category1={search_category1}, app_type={search_category1_app_type})"
                    ),
                )

            def _on_filter_progress(state: dict[str, Any]) -> None:
                _check_cancel("Cancelled during Stage 1/3 (search)")
                total_count = int(state.get("total_count", 0))
                if req.max_games:
                    ratio = state["kept"] / max(1, req.max_games)
                elif total_count > 0:
                    ratio = state["checked_source"] / total_count
                else:
                    ratio = 0.0
                progress = _phase_progress(SEARCH_STAGE_START, SEARCH_STAGE_END, ratio)
                jobs.patch(
                    job_id,
                    progress=progress,
                    message=(
                        f"Stage 1/3: selecting games - checked={state['checked_source']} "
                        f"kept={state['kept']} filtered={state['filtered_out_by_type']}"
                    ),
                )

            games, filtered_out_by_type, source_games_considered = crawl_mod.search_games_by_tags_until_filtered_count(
                tag_ids=tag_ids,
                untag_ids=exclude_tag_ids,
                timeout=req.timeout,
                include_types=include_types,
                exclude_types=exclude_types,
                page_size=req.search_page_size,
                max_games=req.max_games,
                sleep_between_pages=req.sleep_search,
                progress_callback=_on_filter_progress,
                search_category1=search_category1,
                search_category1_app_type=search_category1_app_type,
            )
        else:
            def _on_search_progress(state: dict[str, Any]) -> None:
                _check_cancel("Cancelled during Stage 1/3 (search)")
                total_count = int(state.get("total_count", 0))
                found = int(state.get("found", 0))
                if req.max_games:
                    ratio = found / max(1, req.max_games)
                elif total_count > 0:
                    ratio = found / total_count
                else:
                    ratio = 0.0
                progress = _phase_progress(SEARCH_STAGE_START, SEARCH_STAGE_END, ratio)
                jobs.patch(
                    job_id,
                    progress=progress,
                    message=f"Stage 1/3: selecting games - found={found} total={total_count}",
                )

            games = crawl_mod.search_games_by_tags(
                tag_ids=tag_ids,
                untag_ids=exclude_tag_ids,
                timeout=req.timeout,
                page_size=req.search_page_size,
                max_games=req.max_games,
                sleep_between_pages=req.sleep_search,
                search_category1=search_category1,
                progress_callback=_on_search_progress,
            )
            filtered_out_by_type = 0
            source_games_considered = len(games)

        jobs.patch(
            job_id,
            progress=SEARCH_STAGE_END,
            message=(
                f"Stage 1/3 complete: source={source_games_considered}, "
                f"kept={len(games)}"
            ),
        )
        LOGGER.info(
            "[job=%s] Stage 1/3 complete source=%s kept=%s include_types=%s exclude_types=%s",
            job_id,
            source_games_considered,
            len(games),
            sorted(include_types),
            sorted(exclude_types),
        )

        sorted_games = sorted(games, key=lambda x: x["appid"])
        total_found_games = len(sorted_games)
        game_rows: list[dict[str, Any]] = []
        stage2_started_at = time.time()
        skipped_coming_soon = 0
        consecutive_stage2_429 = 0
        if req.enrich_appdetails and stage2_appdetails_rpm != runtime_max_rpm:
            crawl_mod.set_runtime_rate_limit(stage2_appdetails_rpm)
            LOGGER.warning(
                "[job=%s] Stage 2/3 appdetails limiter enabled: %s rpm (global runtime rpm=%s)",
                job_id,
                stage2_appdetails_rpm,
                runtime_max_rpm,
            )
        LOGGER.info(
            "[job=%s] Stage 2/3 started apps=%s enrich_appdetails=%s store_raw_appdetails=%s",
            job_id,
            total_found_games,
            req.enrich_appdetails,
            req.store_raw_appdetails,
        )
        for idx, game in enumerate(sorted_games, start=1):
            _check_cancel("Cancelled during Stage 2/3 (prepare app list)")
            details = None
            app_type = game.get("app_type")

            if idx == 1 or idx == total_found_games or (idx % 25 == 0):
                stats = crawl_mod.get_request_stats()
                LOGGER.info(
                    "[job=%s] Stage 2/3 progress idx=%s/%s appid=%s name=%s requests=%s retries=%s failed=%s",
                    job_id,
                    idx,
                    total_found_games,
                    game["appid"],
                    game["name"],
                    stats.get("requests_total"),
                    stats.get("retries_total"),
                    stats.get("requests_failed"),
                )

            if req.enrich_appdetails:
                try:
                    details = fetch_appdetails_live(game["appid"], req.timeout)
                    consecutive_stage2_429 = 0
                    t = details.get("type") if isinstance(details, dict) else None
                    app_type = crawl_mod.normalize_app_type(t) if isinstance(t, str) else "unknown"
                except Exception as exc:  # noqa: BLE001
                    details = None
                    status_code = getattr(exc, "code", None)
                    cooldown = 0.0
                    if status_code == 429:
                        consecutive_stage2_429 += 1
                        # Progressive cooldown for rolling appdetails limits.
                        cooldown = min(180.0, 8.0 * (2 ** min(consecutive_stage2_429 - 1, 5)))
                        jobs.patch(
                            job_id,
                            progress=_phase_progress(SEARCH_STAGE_END, PREP_STAGE_END, idx / max(1, total_found_games)),
                            message=(
                                f"Stage 2/3: Steam throttling on appdetails (429). "
                                f"Cooling down {int(cooldown)}s (idx={idx}/{total_found_games}, "
                                f"appid={game['appid']})"
                            ),
                        )
                        LOGGER.warning(
                            "[job=%s] Stage 2/3 appdetails 429 appid=%s idx=%s/%s consecutive_429=%s cooldown=%.1fs",
                            job_id,
                            game["appid"],
                            idx,
                            total_found_games,
                            consecutive_stage2_429,
                            cooldown,
                        )
                    else:
                        consecutive_stage2_429 = 0
                    LOGGER.exception(
                        "[job=%s] Stage 2/3 appdetails fetch failed appid=%s name=%s: %s",
                        job_id,
                        game["appid"],
                        game["name"],
                        exc,
                    )
                    crawl_mod.append_jsonl(
                        errors_file,
                        {
                            "stage": "stage2_appdetails_fetch",
                            "appid": int(game["appid"]),
                            "name": game["name"],
                            "error": str(exc),
                            "ts": int(time.time()),
                        },
                    )
                    _sleep_with_cancel(cooldown, "Cancelled during Stage 2/3 (cooldown)")

            is_coming_soon = False
            if isinstance(details, dict):
                release = details.get("release_date")
                if isinstance(release, dict):
                    is_coming_soon = bool(release.get("coming_soon", False))

            if req.exclude_coming_soon and is_coming_soon:
                skipped_coming_soon += 1
                LOGGER.info(
                    "[job=%s] Stage 2/3 skipped coming soon appid=%s name=%s (%s skipped total)",
                    job_id,
                    game["appid"],
                    game["name"],
                    skipped_coming_soon,
                )
                continue

            row = {
                "appid": game["appid"],
                "name": game["name"],
                "input_tag_ids": tag_ids,
                "input_tags": [tag_name_by_id[t] for t in tag_ids if t in tag_name_by_id],
                "exclude_tag_ids": exclude_tag_ids,
                "exclude_tags": [tag_name_by_id[t] for t in exclude_tag_ids if t in tag_name_by_id],
                "search_tag_ids": game["search_tag_ids"],
                "search_tags": [tag_name_by_id[t] for t in game["search_tag_ids"] if t in tag_name_by_id],
                "app_type": app_type,
            }

            if req.enrich_appdetails and details:
                try:
                    compact = compact_appdetails(details, game["appid"])
                    row.update(compact)
                    write_json(appdetails_dir / f"{game['appid']}.json", compact)
                    if req.store_raw_appdetails:
                        write_json(appdetails_raw_dir / f"{game['appid']}.json", details)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception(
                        "[job=%s] Stage 2/3 appdetails save failed appid=%s name=%s: %s",
                        job_id,
                        game["appid"],
                        game["name"],
                        exc,
                    )
                    crawl_mod.append_jsonl(
                        errors_file,
                        {
                            "stage": "stage2_appdetails_save",
                            "appid": int(game["appid"]),
                            "name": game["name"],
                            "error": str(exc),
                            "ts": int(time.time()),
                        },
                    )

            game_rows.append(row)

            progress = _phase_progress(SEARCH_STAGE_END, PREP_STAGE_END, idx / max(1, total_found_games))
            if idx == 1 or idx == total_found_games or (idx % 10 == 0):
                jobs.patch(
                    job_id,
                    progress=progress,
                    message=(
                        f"Stage 2/3: preparing app list {idx}/{total_found_games} "
                        f"({game['name']})"
                    ),
                )

        if req.max_games is not None and len(game_rows) < req.max_games:
            jobs.patch(
                job_id,
                progress=PREP_STAGE_END,
                message=(
                    f"Only {len(game_rows)} matches available for current filters "
                    f"(requested max_games={req.max_games}, "
                    f"coming soon skipped={skipped_coming_soon}). Continuing without error."
                ),
            )

        write_jsonl(games_file, game_rows)
        LOGGER.info(
            "[job=%s] Stage 2/3 complete app_rows=%s skipped_coming_soon=%s duration_sec=%.2f",
            job_id,
            len(game_rows),
            skipped_coming_soon,
            time.time() - stage2_started_at,
        )
        if not (req.max_games is not None and len(game_rows) < req.max_games):
            jobs.patch(
                job_id,
                progress=PREP_STAGE_END,
                message=(
                    f"Stage 2/3 complete: app list saved ({len(game_rows)} apps, "
                    f"coming soon skipped={skipped_coming_soon})"
                ),
            )

        if req.enrich_appdetails and stage2_appdetails_rpm != runtime_max_rpm:
            crawl_mod.set_runtime_rate_limit(runtime_max_rpm)
            LOGGER.info(
                "[job=%s] Restored runtime limiter to %s rpm for Stage 3/3",
                job_id,
                runtime_max_rpm,
            )

        completed = crawl_mod.load_completed_appids(completed_file) if req.resume else set()

        total_reviews_saved = 0
        processed_count = 0
        skipped_count = 0
        errors_count = 0
        total_games = len(game_rows)

        pending_games: list[dict[str, Any]] = []
        for game in game_rows:
            appid = int(game["appid"])
            review_file = reviews_dir / f"{appid}.jsonl"
            if req.resume and appid in completed and review_file.exists():
                skipped_count += 1
                continue
            pending_games.append(game)

        total_pending = len(pending_games)
        done_pending = 0
        done_total = skipped_count + done_pending
        _check_cancel("Cancelled before Stage 3/3 (review crawl)")
        jobs.patch(
            job_id,
            progress=_review_progress(done_total, total_games),
            message=(
                f"Stage 3/3: crawling reviews - done={done_total}/{total_games} "
                f"(skipped={skipped_count}, pending={total_pending})"
            ),
        )

        if total_pending > 0:
            max_workers = max(1, req.workers)
            executor = ThreadPoolExecutor(max_workers=max_workers)
            future_map: dict[Any, dict[str, Any]] = {}
            next_game_idx = 0
            heartbeat_seconds = 1.5
            heartbeat_idle_ticks = 0

            def _submit_next() -> bool:
                nonlocal next_game_idx
                if next_game_idx >= total_pending:
                    return False
                game = pending_games[next_game_idx]
                next_game_idx += 1
                future = executor.submit(
                    crawl_mod.crawl_game_reviews_with_retries,
                    game,
                    req.timeout,
                    req.sleep_reviews,
                    req.max_review_pages_per_game,
                    req.max_app_retries,
                    req.review_source,
                    req.appreviews_num_per_page,
                    lambda: jobs.is_cancel_requested(job_id),
                )
                future_map[future] = game
                return True

            try:
                for _ in range(max_workers):
                    if not _submit_next():
                        break

                while future_map:
                    _check_cancel("Cancelled during Stage 3/3 (review crawl)")

                    in_flight = set(future_map.keys())
                    done, _ = wait(
                        in_flight,
                        timeout=heartbeat_seconds,
                        return_when=FIRST_COMPLETED,
                    )

                    if not done:
                        heartbeat_idle_ticks += 1
                        stats = crawl_mod.get_request_stats()
                        done_total = skipped_count + done_pending
                        base_progress = _review_progress(done_total, total_games)
                        step = (REVIEW_STAGE_END - REVIEW_STAGE_START) / max(1, total_games)
                        in_flight_bonus = min(step * 0.8, heartbeat_idle_ticks * step * 0.08)
                        progress = min(REVIEW_STAGE_END, base_progress + in_flight_bonus)
                        jobs.patch(
                            job_id,
                            progress=progress,
                            message=(
                                f"Stage 3/3: crawling reviews - done={done_total}/{total_games} "
                                f"pending={len(future_map)} requests={stats['requests_total']} "
                                f"retries={stats['retries_total']} fail_rate={stats['failure_rate']}"
                            ),
                        )
                        continue

                    for future in done:
                        heartbeat_idle_ticks = 0
                        game = future_map.pop(future, None)
                        if game is None:
                            continue
                        appid = int(game["appid"])
                        review_file = reviews_dir / f"{appid}.jsonl"
                        done_pending += 1

                        try:
                            result = future.result()
                        except Exception as exc:  # noqa: BLE001
                            errors_count += 1
                            LOGGER.exception(
                                "[job=%s] Stage 3/3 worker crash appid=%s name=%s: %s",
                                job_id,
                                appid,
                                game["name"],
                                exc,
                            )
                            crawl_mod.append_jsonl(
                                errors_file,
                                {
                                    "appid": appid,
                                    "name": game["name"],
                                    "error": str(exc),
                                    "attempts": req.max_app_retries,
                                    "ts": int(time.time()),
                                },
                            )
                            done_total = skipped_count + done_pending
                            jobs.patch(
                                job_id,
                                progress=_review_progress(done_total, total_games),
                                message=(
                                    f"Stage 3/3: app {done_total}/{total_games} failed "
                                    f"({game['name']} | appid={appid})"
                                ),
                            )
                            if not jobs.is_cancel_requested(job_id):
                                _submit_next()
                            continue

                        if result.get("status") == "cancelled":
                            raise JobCancelledError("Cancelled during Stage 3/3 (review crawl)")

                        if result.get("status") == "ok":
                            reviews = result.get("reviews", [])
                            for item in reviews:
                                item["appid"] = appid
                                item["game_name"] = game["name"]
                                item["hours_on_record_value"] = parse_hours_on_record(
                                    str(item.get("hours_on_record", ""))
                                )
                            crawl_mod.write_jsonl(review_file, reviews)
                            crawl_mod.append_completed_appid(completed_file, appid)
                            total_reviews_saved += len(reviews)
                            processed_count += 1
                        else:
                            errors_count += 1
                            LOGGER.error(
                                "[job=%s] Stage 3/3 app failed appid=%s name=%s status=%s error=%s attempts=%s",
                                job_id,
                                appid,
                                game["name"],
                                result.get("status"),
                                result.get("error", "Unknown error"),
                                result.get("attempts", req.max_app_retries),
                            )
                            crawl_mod.append_jsonl(
                                errors_file,
                                {
                                    "appid": appid,
                                    "name": game["name"],
                                    "error": str(result.get("error", "Unknown error")),
                                    "attempts": int(result.get("attempts", req.max_app_retries)),
                                    "ts": int(time.time()),
                                },
                            )

                        done_total = skipped_count + done_pending
                        jobs.patch(
                            job_id,
                            progress=_review_progress(done_total, total_games),
                            message=(
                                f"Stage 3/3: done={done_total}/{total_games} "
                                f"processed={processed_count} skipped={skipped_count} "
                                f"errors={errors_count} reviews_saved={total_reviews_saved} "
                                f"last={game['name']} ({appid})"
                            ),
                        )
                        if not jobs.is_cancel_requested(job_id):
                            _submit_next()
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        else:
            jobs.patch(
                job_id,
                progress=_review_progress(done_total, total_games),
                message=(
                    f"Stage 3/3: no pending apps for review crawl "
                    f"(done={done_total}/{total_games}, skipped={skipped_count})"
                ),
            )

        crawl_mod.finish_request_stats()
        request_stats = crawl_mod.get_request_stats()

        meta = {
            "input_tags": [x["name"] for x in resolved_tags],
            "input_tag_ids": tag_ids,
            "exclude_tags": [x["name"] for x in resolved_exclude_tags],
            "exclude_tag_ids": exclude_tag_ids,
            "total_games_found": len(game_rows),
            "requested_max_games": req.max_games,
            "effective_max_games": min(req.max_games, len(game_rows)) if req.max_games is not None else len(game_rows),
            "processed_games": processed_count,
            "skipped_games": skipped_count,
            "errors": errors_count,
            "skipped_coming_soon": skipped_coming_soon,
            "total_reviews_saved": total_reviews_saved,
            "dataset_dir": str(dataset_dir),
            "generated_at_unix": int(time.time()),
            "enrich_appdetails": req.enrich_appdetails,
            "store_raw_appdetails": req.store_raw_appdetails,
            "exclude_coming_soon": req.exclude_coming_soon,
            "config": {
                "workers": req.workers,
                "max_rpm": req.max_rpm,
                "effective_runtime_max_rpm": runtime_max_rpm,
                "effective_stage2_appdetails_rpm": stage2_appdetails_rpm if req.enrich_appdetails else None,
                "http_retries": req.http_retries,
                "http_retry_base_sleep": req.http_retry_base_sleep,
                "http_retry_jitter": req.http_retry_jitter,
                "max_app_retries": req.max_app_retries,
                "review_source": req.review_source,
                "appreviews_num_per_page": req.appreviews_num_per_page,
                "exclude_tags": [x["name"] for x in resolved_exclude_tags],
                "exclude_tag_ids": exclude_tag_ids,
                "include_app_types": sorted(include_types),
                "exclude_app_types": sorted(exclude_types),
                "search_category1": search_category1,
                "search_category1_app_type": search_category1_app_type,
                "filtered_out_by_type": filtered_out_by_type,
                "source_games_considered": source_games_considered,
                "store_raw_appdetails": req.store_raw_appdetails,
                "exclude_coming_soon": req.exclude_coming_soon,
                "skipped_coming_soon": skipped_coming_soon,
            },
            "request_stats": request_stats,
        }
        write_json(meta_file, meta)
        LOGGER.info(
            "[job=%s] Crawl completed processed=%s skipped=%s errors=%s reviews_saved=%s dataset=%s",
            job_id,
            processed_count,
            skipped_count,
            errors_count,
            total_reviews_saved,
            dataset_dir,
        )
        jobs.finish_ok(job_id, meta)
    except JobCancelledError as exc:
        try:
            crawl_mod.finish_request_stats()
        except Exception:
            pass
        LOGGER.warning("[job=%s] Crawl cancelled: %s", job_id, exc)
        jobs.finish_cancelled(job_id, str(exc) or "Cancelled by user")
    except Exception as exc:  # noqa: BLE001
        try:
            crawl_mod.finish_request_stats()
        except Exception:
            pass
        LOGGER.exception("[job=%s] Crawl failed with unhandled exception: %s", job_id, exc)
        jobs.finish_error(job_id, str(exc))


def run_search_job(job_id: str, req: SearchJobRequest, jobs: JobManager) -> None:
    try:
        LOGGER.info(
            "[job=%s] Search started dataset=%s keywords=%s keyword_mode=%s case_sensitive=%s",
            job_id,
            req.dataset_dir,
            req.keywords,
            req.keyword_mode,
            req.case_sensitive,
        )
        if jobs.is_cancel_requested(job_id):
            raise JobCancelledError("Cancelled before search started")
        jobs.patch(job_id, status="running", started_at=time.time(), message="Searching keywords in dataset")
        dataset_dir = safe_dataset_path(req.dataset_dir)

        if jobs.is_cancel_requested(job_id):
            raise JobCancelledError("Cancelled before keyword scan")
        rows = aggregate_keyword_matches(
            dataset_dir=dataset_dir,
            keywords=req.keywords,
            sample_per_game=req.sample_per_game,
            keyword_mode=req.keyword_mode,
            case_sensitive=req.case_sensitive,
            min_matched_reviews=req.min_matched_reviews,
            top_n=req.top_n,
        )

        if jobs.is_cancel_requested(job_id):
            raise JobCancelledError("Cancelled after keyword scan")
        out_file = dataset_dir / "search_results" / f"{job_id}.json"
        write_json(out_file, rows)

        result = {
            "matched_games": len(rows),
            "keywords": req.keywords,
            "dataset_dir": str(dataset_dir),
            "result_file": str(out_file.relative_to(WORKSPACE_ROOT)),
            "keyword_mode": req.keyword_mode,
            "case_sensitive": req.case_sensitive,
            "min_matched_reviews": req.min_matched_reviews,
            "top_n": req.top_n,
        }
        LOGGER.info(
            "[job=%s] Search completed matched_games=%s result_file=%s",
            job_id,
            len(rows),
            result["result_file"],
        )
        jobs.finish_ok(job_id, result)
    except JobCancelledError as exc:
        LOGGER.warning("[job=%s] Search cancelled: %s", job_id, exc)
        jobs.finish_cancelled(job_id, str(exc) or "Cancelled by user")
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("[job=%s] Search failed with unhandled exception: %s", job_id, exc)
        jobs.finish_error(job_id, str(exc))


def app_type_matches_filters(app_type: str, include_types: set[str], exclude_types: set[str]) -> bool:
    if include_types and app_type not in include_types:
        return False
    if exclude_types and app_type in exclude_types:
        return False
    return True


app = FastAPI(title="Steam Review Intelligence API", version="0.3.0")
job_manager = JobManager()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tags")
def get_tags(timeout: int = Query(default=30, ge=5, le=300)) -> dict[str, Any]:
    tags = crawl_mod.fetch_english_tags(timeout)
    tags.sort(key=lambda x: x["name"].casefold())
    return {"count": len(tags), "tags": tags}


@app.get("/steam/appdetails/{appid}")
def get_live_appdetails(
    appid: int,
    mode: str = Query(default="compact", pattern="^(compact|raw|both)$"),
    timeout: int = Query(default=30, ge=5, le=300),
) -> dict[str, Any]:
    data = fetch_appdetails_live(appid, timeout)
    if not data:
        raise HTTPException(status_code=404, detail="App details not found")
    compact = compact_appdetails(data, appid)
    if mode == "raw":
        return data
    if mode == "both":
        return {"compact": compact, "raw": data}
    return compact


@app.post("/crawl/estimate")
def estimate_crawl_scope(req: CrawlEstimateRequest) -> dict[str, Any]:
    all_tags = crawl_mod.fetch_english_tags(req.timeout)
    resolved_tags, unknown = crawl_mod.resolve_input_tags(req.tags, all_tags)
    if unknown:
        raise HTTPException(status_code=400, detail="Unknown tags: " + ", ".join(unknown))
    if not resolved_tags:
        raise HTTPException(status_code=400, detail="No valid tags resolved")

    resolved_exclude_tags, unknown_exclude = crawl_mod.resolve_input_tags(req.exclude_tags or [], all_tags)
    if unknown_exclude:
        raise HTTPException(status_code=400, detail="Unknown exclude_tags: " + ", ".join(unknown_exclude))

    tag_ids = [x["tagid"] for x in resolved_tags]
    exclude_tag_ids = [x["tagid"] for x in resolved_exclude_tags]

    include_types = {
        crawl_mod.normalize_app_type(x)
        for x in (req.include_app_types or [])
        if isinstance(x, str) and x.strip()
    }
    exclude_types = {
        crawl_mod.normalize_app_type(x)
        for x in (req.exclude_app_types or [])
        if isinstance(x, str) and x.strip()
    }
    use_type_filter = bool(include_types or exclude_types)
    search_category1, search_category1_app_type = crawl_mod.infer_search_category1(include_types, exclude_types)

    base_total_count = crawl_mod.fetch_search_total_count(
        tag_ids=tag_ids,
        untag_ids=exclude_tag_ids,
        timeout=req.timeout,
        search_category1=None,
    )

    estimate_mode = "exact"
    estimated_total_count = int(base_total_count)
    sample_checked = 0
    sample_kept = 0
    sample_filtered_out_by_type = 0

    if use_type_filter:
        if search_category1 is not None:
            estimated_total_count = crawl_mod.fetch_search_total_count(
                tag_ids=tag_ids,
                untag_ids=exclude_tag_ids,
                timeout=req.timeout,
                search_category1=search_category1,
            )
        elif base_total_count <= 0:
            estimated_total_count = 0
        else:
            estimate_mode = "sampled"
            sample_target = min(int(req.sample_size), int(base_total_count))
            sample_games = crawl_mod.search_games_by_tags(
                tag_ids=tag_ids,
                untag_ids=exclude_tag_ids,
                timeout=req.timeout,
                page_size=req.search_page_size,
                max_games=sample_target,
                sleep_between_pages=0.0,
                search_category1=None,
            )
            sample_checked = len(sample_games)

            if sample_checked <= 0:
                estimated_total_count = 0
                estimate_mode = "exact"
            else:
                def detect_type(appid: int) -> str:
                    try:
                        details = fetch_appdetails_live(appid, req.timeout)
                        t = details.get("type") if isinstance(details, dict) else None
                        return crawl_mod.normalize_app_type(t) if isinstance(t, str) else "unknown"
                    except Exception:
                        return "unknown"

                max_workers = max(1, min(12, sample_checked))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(detect_type, int(game["appid"])) for game in sample_games]
                    for future in as_completed(futures):
                        app_type = future.result()
                        if app_type_matches_filters(app_type, include_types, exclude_types):
                            sample_kept += 1
                        else:
                            sample_filtered_out_by_type += 1

                if sample_checked >= base_total_count:
                    estimate_mode = "exact"
                    estimated_total_count = sample_kept
                else:
                    ratio = sample_kept / sample_checked
                    estimated_total_count = int(round(base_total_count * ratio))
                    estimated_total_count = max(0, min(base_total_count, estimated_total_count))

    cap_at = None
    note = ""
    if req.max_games is not None:
        cap_at = min(int(req.max_games), int(estimated_total_count))
        if req.max_games > estimated_total_count:
            note = (
                f"Requested max_games={req.max_games}, but available matches are around "
                f"{estimated_total_count}. Crawl will finish with fewer games without errors."
            )

    return {
        "input_tags": [x["name"] for x in resolved_tags],
        "input_tag_ids": tag_ids,
        "exclude_tags": [x["name"] for x in resolved_exclude_tags],
        "exclude_tag_ids": exclude_tag_ids,
        "include_app_types": sorted(include_types),
        "exclude_app_types": sorted(exclude_types),
        "use_type_filter": use_type_filter,
        "search_category1": search_category1,
        "search_category1_app_type": search_category1_app_type,
        "base_total_count": int(base_total_count),
        "estimated_total_count": int(estimated_total_count),
        "estimate_mode": estimate_mode,
        "sample_size": int(req.sample_size),
        "sample_checked": int(sample_checked),
        "sample_kept": int(sample_kept),
        "sample_filtered_out_by_type": int(sample_filtered_out_by_type),
        "requested_max_games": req.max_games,
        "effective_max_games": cap_at,
        "note": note,
    }


@app.post("/jobs/crawl")
def create_crawl_job(req: CrawlJobRequest) -> dict[str, Any]:
    job = job_manager.create("crawl", req.model_dump())
    thread = threading.Thread(target=run_crawl_job, args=(job.id, req, job_manager), daemon=True)
    thread.start()
    return {"job_id": job.id, "status": job.status}


@app.post("/jobs/search")
def create_search_job(req: SearchJobRequest) -> dict[str, Any]:
    job = job_manager.create("search", req.model_dump())
    thread = threading.Thread(target=run_search_job, args=(job.id, req, job_manager), daemon=True)
    thread.start()
    return {"job_id": job.id, "status": job.status}


@app.get("/jobs")
def list_jobs() -> dict[str, Any]:
    return {"jobs": job_manager.list_jobs()}


@app.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    try:
        return job_manager.as_dict(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found") from None


@app.get("/jobs/{job_id}/result")
def get_job_result(job_id: str) -> dict[str, Any]:
    try:
        job = job_manager.get(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found") from None

    if job.status != "completed":
        raise HTTPException(status_code=409, detail=f"Job status is '{job.status}'")

    if job.job_type == "search":
        result_file = Path(job.result["result_file"]) if job.result else None
        if not result_file:
            raise HTTPException(status_code=500, detail="Search result file is missing")
        full_path = (WORKSPACE_ROOT / result_file).resolve()
        if not full_path.exists():
            raise HTTPException(status_code=404, detail="Search result file not found")
        return {"result": job.result, "rows": json.loads(full_path.read_text(encoding="utf-8"))}

    return {"result": job.result}


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict[str, Any]:
    try:
        _ = job_manager.request_cancel(job_id)
        return job_manager.as_dict(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found") from None


def _schema_cache_path(dataset_dir: Path) -> Path:
    return dataset_dir / EXPLORE_SCHEMA_CACHE_FILENAME


def _schema_source_fingerprint(games_file: Path, review_files: list[Path]) -> dict[str, Any]:
    games_stat = games_file.stat()
    review_latest_mtime_ns = 0
    review_total_size = 0
    for path in review_files:
        st = path.stat()
        review_total_size += int(st.st_size)
        if int(st.st_mtime_ns) > review_latest_mtime_ns:
            review_latest_mtime_ns = int(st.st_mtime_ns)
    return {
        "games_mtime_ns": int(games_stat.st_mtime_ns),
        "games_size": int(games_stat.st_size),
        "review_files_count": len(review_files),
        "reviews_latest_mtime_ns": review_latest_mtime_ns,
        "reviews_total_size": review_total_size,
    }


def _load_cached_schema(dataset_dir: Path, fingerprint: dict[str, Any]) -> dict[str, Any] | None:
    cache_path = _schema_cache_path(dataset_dir)
    cached = read_json(cache_path)
    if not isinstance(cached, dict):
        return None
    if cached.get("_source_fingerprint") != fingerprint:
        return None
    if int(cached.get("_max_review_rows", 0)) != int(EXPLORE_SCHEMA_MAX_REVIEW_ROWS):
        return None
    payload = dict(cached)
    payload.pop("_source_fingerprint", None)
    payload.pop("_max_review_rows", None)
    payload["cache_hit"] = True
    return payload


def _save_cached_schema(dataset_dir: Path, payload: dict[str, Any], fingerprint: dict[str, Any]) -> None:
    cache_path = _schema_cache_path(dataset_dir)
    cached = dict(payload)
    cached["cache_hit"] = False
    cached["_source_fingerprint"] = fingerprint
    cached["_max_review_rows"] = EXPLORE_SCHEMA_MAX_REVIEW_ROWS
    write_json(cache_path, cached)


def build_explore_schema(dataset_dir: Path) -> dict[str, Any]:
    games_file = dataset_dir / "games.jsonl"
    reviews_dir = dataset_dir / "reviews"
    games = read_jsonl(games_file)
    review_files = list(reviews_dir.glob("*.jsonl"))
    source_fp = _schema_source_fingerprint(games_file, review_files)
    cached = _load_cached_schema(dataset_dir, source_fp)
    if cached is not None:
        return cached

    game_profiles: dict[str, dict[str, Any]] = {}
    for row in games:
        if not isinstance(row, dict):
            continue
        for field, value in iter_record_fields(row):
            profile = game_profiles.setdefault(field, init_field_profile())
            update_field_profile(profile, value)

    review_profiles: dict[str, dict[str, Any]] = {}
    profiled_rows = 0
    profiled_limit = max(1, int(EXPLORE_SCHEMA_MAX_REVIEW_ROWS))
    sampled = False
    for path in review_files:
        if profiled_rows >= profiled_limit:
            sampled = True
            break
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                if profiled_rows >= profiled_limit:
                    sampled = True
                    continue
                profiled_rows += 1
                for field, value in iter_record_fields(row):
                    profile = review_profiles.setdefault(field, init_field_profile())
                    update_field_profile(profile, value)
                if profiled_rows >= profiled_limit:
                    sampled = True
                    break

    payload = {
        "dataset_dir": dataset_display_id(dataset_dir),
        "games_count": len(games),
        "review_files_count": len(review_files),
        "review_rows_count": profiled_rows,
        "review_rows_profiled": profiled_rows,
        "review_rows_sampled": sampled,
        "game_fields": finalize_schema_profiles(game_profiles, len(games)),
        "review_fields": finalize_schema_profiles(review_profiles, max(1, profiled_rows)),
        "generated_at_iso": datetime.utcnow().isoformat() + "Z",
        "cache_hit": False,
    }
    _save_cached_schema(dataset_dir, payload, source_fp)
    return payload


def sort_explore_game_rows(rows: list[dict[str, Any]], sort_by: str, sort_order: str) -> None:
    reverse = sort_order == "desc"

    def _key(row: dict[str, Any]) -> Any:
        if sort_by == "name":
            return str(row.get("name", "")).casefold()
        if sort_by == "match_rate":
            return float(row.get("match_rate", 0.0))
        if sort_by == "positive_matched_reviews":
            return int(row.get("positive_matched_reviews", 0))
        if sort_by == "negative_matched_reviews":
            return int(row.get("negative_matched_reviews", 0))
        if sort_by == "total_reviews":
            return int(row.get("total_reviews", 0))
        return int(row.get("matched_reviews", 0))

    rows.sort(key=_key, reverse=reverse)


def execute_explore_query(dataset_dir: Path, req: ExploreQueryRequest) -> dict[str, Any]:
    games = read_jsonl(dataset_dir / "games.jsonl")
    reviews_dir = dataset_dir / "reviews"
    if not reviews_dir.exists():
        raise FileNotFoundError(f"Missing {reviews_dir}")

    include_keywords = unique_strings(req.include_keywords)
    exclude_keywords = unique_strings(req.exclude_keywords)
    game_filters = req.game_filters or []
    review_filters = req.review_filters or []

    rows: list[dict[str, Any]] = []
    scanned_games = 0
    passed_game_filters = 0
    scanned_reviews = 0
    matched_reviews_total = 0

    for game in games:
        if not isinstance(game, dict):
            continue
        appid = game.get("appid")
        if not isinstance(appid, int):
            continue

        scanned_games += 1
        if game_filters and not row_matches_filters(game, game_filters):
            continue
        passed_game_filters += 1

        review_path = reviews_dir / f"{appid}.jsonl"
        if not review_path.exists():
            continue

        total_reviews = 0
        matched_reviews = 0
        positive_matched = 0
        negative_matched = 0
        keyword_hits_count: dict[str, int] = {}
        sample_reviews: list[dict[str, Any]] = []

        with review_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    review = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(review, dict):
                    continue

                total_reviews += 1
                scanned_reviews += 1

                if review_filters and not row_matches_filters(review, review_filters):
                    continue

                review_text = str(review.get("review_text", ""))
                ok, include_hits = review_keyword_match(
                    review_text=review_text,
                    include_keywords=include_keywords,
                    exclude_keywords=exclude_keywords,
                    keyword_mode=req.keyword_mode,
                    case_sensitive=req.case_sensitive,
                )
                if not ok:
                    continue

                matched_reviews += 1
                matched_reviews_total += 1
                if review.get("recommended") is True:
                    positive_matched += 1
                elif review.get("recommended") is False:
                    negative_matched += 1

                for hit in include_hits:
                    keyword_hits_count[hit] = keyword_hits_count.get(hit, 0) + 1

                if len(sample_reviews) < req.sample_reviews_per_game:
                    sample_reviews.append(
                        {
                            "review_url": review.get("review_url"),
                            "recommended": review.get("recommended"),
                            "language": review.get("language"),
                            "matched_keywords": include_hits,
                            "review_text": " ".join(review_text.split())[:260],
                        }
                    )

        if matched_reviews < req.min_matched_reviews:
            continue

        top_keywords = sorted(keyword_hits_count.items(), key=lambda x: x[1], reverse=True)
        row = {
            "appid": appid,
            "name": game.get("name", f"appid_{appid}"),
            "store_url": f"https://store.steampowered.com/app/{appid}/",
            "total_reviews": total_reviews,
            "matched_reviews": matched_reviews,
            "match_rate": round((matched_reviews / total_reviews), 6) if total_reviews else 0.0,
            "positive_matched_reviews": positive_matched,
            "negative_matched_reviews": negative_matched,
            "matched_keywords": [x[0] for x in top_keywords],
            "matched_keywords_top": [{"keyword": k, "count": c} for k, c in top_keywords[:12]],
            "sample_reviews": sample_reviews,
            "game": compact_game_for_explorer(game),
        }
        rows.append(row)

    sort_explore_game_rows(rows, req.sort_by, req.sort_order)
    total = len(rows)
    paged = rows[req.skip : req.skip + req.limit]

    return {
        "dataset_dir": dataset_display_id(dataset_dir),
        "filters": {
            "include_keywords": include_keywords,
            "exclude_keywords": exclude_keywords,
            "keyword_mode": req.keyword_mode,
            "case_sensitive": req.case_sensitive,
            "min_matched_reviews": req.min_matched_reviews,
            "game_filters_count": len(game_filters),
            "review_filters_count": len(review_filters),
        },
        "stats": {
            "games_scanned": scanned_games,
            "games_after_game_filters": passed_game_filters,
            "reviews_scanned": scanned_reviews,
            "matched_reviews_total": matched_reviews_total,
        },
        "total": total,
        "skip": req.skip,
        "limit": req.limit,
        "rows": paged,
    }


def sort_explore_reviews(rows: list[dict[str, Any]], sort_by: str) -> None:
    if sort_by == "none":
        return

    if sort_by in {"timestamp_desc", "timestamp_asc"}:
        reverse = sort_by == "timestamp_desc"

        def _ts_key(row: dict[str, Any]) -> float:
            ts = row.get("timestamp_created")
            if isinstance(ts, (int, float)):
                return float(ts)
            return -1.0

        rows.sort(key=_ts_key, reverse=reverse)
        return

    reverse = sort_by == "hours_desc"

    def _hours_key(row: dict[str, Any]) -> float:
        hv = row.get("hours_on_record_value")
        if isinstance(hv, (int, float)):
            return float(hv)
        pv = parse_hours_on_record(str(row.get("hours_on_record", "")))
        return pv if pv is not None else -1.0

    rows.sort(key=_hours_key, reverse=reverse)


def execute_explore_game_reviews(dataset_dir: Path, appid: int, req: ExploreGameReviewsRequest) -> dict[str, Any]:
    review_path = dataset_dir / "reviews" / f"{appid}.jsonl"
    if not review_path.exists():
        raise FileNotFoundError(f"Missing review file for appid={appid}")

    include_keywords = unique_strings(req.include_keywords)
    exclude_keywords = unique_strings(req.exclude_keywords)
    review_filters = req.review_filters or []

    game = find_game_row(dataset_dir, appid)
    game_compact = compact_game_for_explorer(game) if isinstance(game, dict) else {
        "appid": appid,
        "name": f"appid_{appid}",
        "store_url": f"https://store.steampowered.com/app/{appid}/",
    }

    rows: list[dict[str, Any]] = []
    total_reviews = 0
    with review_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                review = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(review, dict):
                continue
            total_reviews += 1

            if review_filters and not row_matches_filters(review, review_filters):
                continue

            review_text = str(review.get("review_text", ""))
            ok, include_hits = review_keyword_match(
                review_text=review_text,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                keyword_mode=req.keyword_mode,
                case_sensitive=req.case_sensitive,
            )
            if not ok:
                continue

            row = dict(review)
            row["matched_keywords"] = include_hits
            if req.highlight and include_keywords:
                row["review_text_highlighted"] = highlight_matches(review_text, include_keywords, req.case_sensitive)
            rows.append(row)

    sort_explore_reviews(rows, req.sort_by)
    total = len(rows)
    paged = rows[req.skip : req.skip + req.limit]

    return {
        "dataset_dir": dataset_display_id(dataset_dir),
        "appid": appid,
        "game": game_compact,
        "total_reviews_in_file": total_reviews,
        "total_matched_reviews": total,
        "skip": req.skip,
        "limit": req.limit,
        "reviews": paged,
        "filters": {
            "include_keywords": include_keywords,
            "exclude_keywords": exclude_keywords,
            "keyword_mode": req.keyword_mode,
            "case_sensitive": req.case_sensitive,
            "review_filters_count": len(review_filters),
        },
    }


@app.get("/datasets")
def datasets_index() -> dict[str, Any]:
    items = list_available_datasets()
    return {
        "count": len(items),
        "datasets": items,
        "generated_at_iso": datetime.utcnow().isoformat() + "Z",
    }


@app.get("/datasets/{dataset_dir}/explore/schema")
def explore_schema(dataset_dir: str) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    games_file = ds / "games.jsonl"
    reviews_dir = ds / "reviews"
    if not games_file.exists() or not reviews_dir.exists():
        raise HTTPException(status_code=404, detail="Dataset is missing games.jsonl or reviews directory")
    return build_explore_schema(ds)


@app.post("/datasets/{dataset_dir}/explore/query")
def explore_query(dataset_dir: str, req: ExploreQueryRequest) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    games_file = ds / "games.jsonl"
    reviews_dir = ds / "reviews"
    if not games_file.exists() or not reviews_dir.exists():
        raise HTTPException(status_code=404, detail="Dataset is missing games.jsonl or reviews directory")
    return execute_explore_query(ds, req)


@app.post("/datasets/{dataset_dir}/explore/games/{appid}/reviews")
def explore_game_reviews(dataset_dir: str, appid: int, req: ExploreGameReviewsRequest) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    try:
        return execute_explore_game_reviews(ds, appid, req)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Review file not found") from None


@app.get("/datasets/{dataset_dir}/games")
def list_games(
    dataset_dir: str,
    q: str | None = None,
    include_tag: list[str] | None = Query(default=None),
    exclude_tag: list[str] | None = Query(default=None),
    app_type: str | None = None,
    is_free: bool | None = None,
    platform: str | None = Query(default=None, pattern="^(windows|mac|linux)$"),
    release_year_gte: int | None = Query(default=None, ge=1980, le=2100),
    release_year_lte: int | None = Query(default=None, ge=1980, le=2100),
    min_recommendations: int | None = Query(default=None, ge=0),
    min_metacritic: int | None = Query(default=None, ge=0, le=100),
    include_review_counts: bool = False,
    sort_by: str = Query(default="name", pattern="^(name|release_year|recommendations|metacritic|reviews_count)$"),
    sort_order: str = Query(default="asc", pattern="^(asc|desc)$"),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    rows = read_jsonl(ds / "games.jsonl")

    review_counts = load_review_counts(ds) if include_review_counts or sort_by == "reviews_count" else {}

    def tagset(row: dict[str, Any]) -> set[str]:
        values = []
        values.extend(row.get("input_tags", []))
        values.extend(row.get("search_tags", []))
        values.extend(row.get("genres", []))
        values.extend(row.get("categories", []))
        return {str(v).casefold() for v in values if str(v).strip()}

    out: list[dict[str, Any]] = []
    inc = [x.casefold() for x in (include_tag or []) if x.strip()]
    exc = [x.casefold() for x in (exclude_tag or []) if x.strip()]

    for row in rows:
        name = str(row.get("name", ""))
        if q and q.casefold() not in name.casefold():
            continue

        tags = tagset(row)
        if inc and not all(x in tags for x in inc):
            continue
        if exc and any(x in tags for x in exc):
            continue

        if app_type:
            row_type = row.get("type") or row.get("app_type") or ""
            if str(row_type).casefold() != app_type.casefold():
                continue
        if is_free is not None and bool(row.get("is_free", False)) != is_free:
            continue

        if platform:
            pf = row.get("platforms") if isinstance(row.get("platforms"), dict) else {}
            if not bool(pf.get(platform, False)):
                continue

        release_year = row.get("release_year")
        if release_year_gte is not None and (not isinstance(release_year, int) or release_year < release_year_gte):
            continue
        if release_year_lte is not None and (not isinstance(release_year, int) or release_year > release_year_lte):
            continue

        rec_total = row.get("recommendations_total")
        if min_recommendations is not None and (not isinstance(rec_total, int) or rec_total < min_recommendations):
            continue

        metacritic = row.get("metacritic_score")
        if min_metacritic is not None and (not isinstance(metacritic, int) or metacritic < min_metacritic):
            continue

        row_out = dict(row)
        if review_counts:
            row_out["reviews_count"] = review_counts.get(int(row.get("appid", 0)), 0)
        out.append(row_out)

    reverse = sort_order == "desc"

    def sort_key(row: dict[str, Any]) -> Any:
        if sort_by == "release_year":
            return row.get("release_year") or -1
        if sort_by == "recommendations":
            return row.get("recommendations_total") or -1
        if sort_by == "metacritic":
            return row.get("metacritic_score") or -1
        if sort_by == "reviews_count":
            return row.get("reviews_count") or 0
        return str(row.get("name", "")).casefold()

    out.sort(key=sort_key, reverse=reverse)

    total = len(out)
    out = out[skip : skip + limit]
    return {"total": total, "skip": skip, "limit": limit, "games": out}


@app.get("/datasets/{dataset_dir}/games/{appid}")
def get_game(dataset_dir: str, appid: int) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    row = find_game_row(ds, appid)
    if row is not None:
        return row

    compact_path, _raw_path = appdetails_paths(ds, appid)
    compact = read_json(compact_path)
    if isinstance(compact, dict):
        return compact

    raise HTTPException(status_code=404, detail="Game not found")


@app.get("/datasets/{dataset_dir}/games/{appid}/variables")
def get_game_variables(
    dataset_dir: str,
    appid: int,
    field: list[str] = Query(...),
    source: str = Query(default="auto", pattern="^(auto|game|compact|raw)$"),
    refresh: bool = False,
    timeout: int = Query(default=30, ge=5, le=300),
    include_missing: bool = False,
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    fields = normalize_field_params(field)
    if not fields:
        raise HTTPException(status_code=400, detail="field must contain at least one variable path")

    game_row = find_game_row(ds, appid)
    compact: dict[str, Any] | None = None
    raw: dict[str, Any] | None = None

    if source in {"compact", "raw"}:
        compact_data, raw_data = get_or_refresh_dataset_appdetails(
            dataset_dir=ds,
            appid=appid,
            timeout=timeout,
            refresh=refresh,
        )
        compact = compact_data if isinstance(compact_data, dict) else None
        raw = raw_data if isinstance(raw_data, dict) else None
    elif source == "auto":
        compact_path, raw_path = appdetails_paths(ds, appid)
        compact_disk = read_json(compact_path)
        raw_disk = read_json(raw_path)
        compact = compact_disk if isinstance(compact_disk, dict) else None
        raw = raw_disk if isinstance(raw_disk, dict) else None
        if refresh:
            compact_data, raw_data = get_or_refresh_dataset_appdetails(
                dataset_dir=ds,
                appid=appid,
                timeout=timeout,
                refresh=True,
            )
            compact = compact_data if isinstance(compact_data, dict) else compact
            raw = raw_data if isinstance(raw_data, dict) else raw

    values: dict[str, Any] = {}
    missing: list[str] = []
    field_sources: dict[str, str] = {}

    if source == "game":
        if game_row is None:
            raise HTTPException(status_code=404, detail="Game row not found")
        values, missing = extract_fields(game_row, fields)
    elif source == "compact":
        if compact is None:
            raise HTTPException(status_code=404, detail="Compact appdetails not found")
        values, missing = extract_fields(compact, fields)
    elif source == "raw":
        if raw is None:
            raise HTTPException(status_code=404, detail="Raw appdetails not found")
        values, missing = extract_fields(raw, fields)
    else:
        for path in fields:
            found = False
            if game_row is not None:
                value = get_value_by_path(game_row, path)
                if value is not _FIELD_MISSING:
                    values[path] = value
                    field_sources[path] = "game"
                    found = True
            if not found and compact is not None:
                value = get_value_by_path(compact, path)
                if value is not _FIELD_MISSING:
                    values[path] = value
                    field_sources[path] = "compact"
                    found = True
            if not found and raw is not None:
                value = get_value_by_path(raw, path)
                if value is not _FIELD_MISSING:
                    values[path] = value
                    field_sources[path] = "raw"
                    found = True

            if not found:
                missing.append(path)

        if game_row is None and compact is None and raw is None:
            raise HTTPException(status_code=404, detail="Game data not found")

    response: dict[str, Any] = {
        "appid": appid,
        "source": source,
        "values": values,
        "fields_requested": fields,
    }
    if source == "auto":
        response["field_sources"] = field_sources
    if include_missing:
        response["missing_fields"] = missing
    return response


@app.get("/datasets/{dataset_dir}/games/{appid}/appdetails")
def get_game_appdetails(
    dataset_dir: str,
    appid: int,
    mode: str = Query(default="compact", pattern="^(compact|raw|both)$"),
    refresh: bool = False,
    timeout: int = Query(default=30, ge=5, le=300),
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    compact, raw = get_or_refresh_dataset_appdetails(
        dataset_dir=ds,
        appid=appid,
        timeout=timeout,
        refresh=refresh,
    )

    if mode == "raw":
        if not isinstance(raw, dict):
            raise HTTPException(status_code=404, detail="Raw appdetails not found")
        return raw

    if mode == "both":
        if not isinstance(compact, dict) and not isinstance(raw, dict):
            raise HTTPException(status_code=404, detail="App details not found")
        return {"compact": compact, "raw": raw}

    if not isinstance(compact, dict):
        raise HTTPException(status_code=404, detail="Compact appdetails not found")
    return compact


@app.get("/datasets/{dataset_dir}/games/{appid}/refresh-details")
def refresh_game_details(
    dataset_dir: str,
    appid: int,
    mode: str = Query(default="compact", pattern="^(compact|raw|both)$"),
    timeout: int = Query(default=30, ge=5, le=300),
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    compact, raw = get_or_refresh_dataset_appdetails(
        dataset_dir=ds,
        appid=appid,
        timeout=timeout,
        refresh=True,
    )
    if mode == "raw":
        if not isinstance(raw, dict):
            raise HTTPException(status_code=404, detail="Raw appdetails not found")
        return raw
    if mode == "both":
        if not isinstance(compact, dict) and not isinstance(raw, dict):
            raise HTTPException(status_code=404, detail="App details not found")
        return {"compact": compact, "raw": raw}
    if not isinstance(compact, dict):
        raise HTTPException(status_code=404, detail="Compact appdetails not found")
    return compact


@app.get("/datasets/{dataset_dir}/games/{appid}/reviews")
def get_reviews_for_game(
    dataset_dir: str,
    appid: int,
    keyword: list[str] | None = Query(default=None),
    keyword_mode: str = Query(default="any", pattern="^(any|all)$"),
    case_sensitive: bool = False,
    recommended: bool | None = None,
    min_hours: float | None = Query(default=None, ge=0),
    max_hours: float | None = Query(default=None, ge=0),
    sort_by: str = Query(default="none", pattern="^(none|hours_asc|hours_desc)$"),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    highlight: bool = Query(default=True),
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    review_path = ds / "reviews" / f"{appid}.jsonl"
    if not review_path.exists():
        raise HTTPException(status_code=404, detail="Review file not found")

    rows = read_jsonl(review_path)
    keywords = [k for k in (keyword or []) if k.strip()]
    filtered: list[dict[str, Any]] = []

    for row in rows:
        if recommended is not None and bool(row.get("recommended", False)) != recommended:
            continue

        hv = row.get("hours_on_record_value")
        if hv is None:
            hv = parse_hours_on_record(str(row.get("hours_on_record", "")))
        if min_hours is not None and (hv is None or hv < min_hours):
            continue
        if max_hours is not None and (hv is None or hv > max_hours):
            continue

        text = str(row.get("review_text", ""))
        ok, hits = keyword_hit(text, keywords, keyword_mode, case_sensitive)
        if not ok:
            continue

        item = dict(row)
        if hits:
            item["matched_keywords"] = hits
        if highlight and keywords:
            item["review_text_highlighted"] = highlight_matches(text, keywords, case_sensitive)
        filtered.append(item)

    if sort_by != "none":
        reverse = sort_by == "hours_desc"

        def hours_key(x: dict[str, Any]) -> float:
            v = x.get("hours_on_record_value")
            if isinstance(v, (int, float)):
                return float(v)
            pv = parse_hours_on_record(str(x.get("hours_on_record", "")))
            return pv if isinstance(pv, float) else -1.0

        filtered.sort(key=hours_key, reverse=reverse)

    total = len(filtered)
    filtered = filtered[skip : skip + limit]
    return {"total": total, "skip": skip, "limit": limit, "reviews": filtered}


@app.get("/datasets/{dataset_dir}/games/{appid}/reviews/variables")
def get_review_variables_for_game(
    dataset_dir: str,
    appid: int,
    field: list[str] = Query(...),
    recommendationid: str | None = None,
    review_url: str | None = None,
    keyword: list[str] | None = Query(default=None),
    keyword_mode: str = Query(default="any", pattern="^(any|all)$"),
    case_sensitive: bool = False,
    recommended: bool | None = None,
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
    include_missing: bool = False,
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    review_path = ds / "reviews" / f"{appid}.jsonl"
    if not review_path.exists():
        raise HTTPException(status_code=404, detail="Review file not found")

    fields = normalize_field_params(field)
    if not fields:
        raise HTTPException(status_code=400, detail="field must contain at least one variable path")

    keywords = [k for k in (keyword or []) if k.strip()]
    rows = read_jsonl(review_path)
    out: list[dict[str, Any]] = []

    for row in rows:
        if recommendationid is not None and str(row.get("recommendationid", "")) != recommendationid:
            continue
        if review_url is not None and str(row.get("review_url", "")) != review_url:
            continue
        if recommended is not None and bool(row.get("recommended", False)) != recommended:
            continue

        text = str(row.get("review_text", ""))
        ok, hits = keyword_hit(text, keywords, keyword_mode, case_sensitive)
        if not ok:
            continue

        values, missing = extract_fields(row, fields)
        item: dict[str, Any] = {
            "appid": appid,
            "recommendationid": row.get("recommendationid"),
            "review_url": row.get("review_url"),
            "values": values,
        }
        if hits:
            item["matched_keywords"] = hits
        if include_missing:
            item["missing_fields"] = missing
        out.append(item)

    total = len(out)
    out = out[skip : skip + limit]
    return {"total": total, "skip": skip, "limit": limit, "rows": out}


@app.get("/datasets/{dataset_dir}/keywords/aggregate")
def keyword_aggregate(
    dataset_dir: str,
    keyword: list[str] = Query(...),
    keyword_mode: str = Query(default="any", pattern="^(any|all)$"),
    case_sensitive: bool = False,
    sample_per_game: int = Query(default=2, ge=1, le=20),
    min_matched_reviews: int = Query(default=1, ge=1),
    top_n: int | None = Query(default=None, ge=1),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    rows = aggregate_keyword_matches(
        dataset_dir=ds,
        keywords=keyword,
        sample_per_game=sample_per_game,
        keyword_mode=keyword_mode,
        case_sensitive=case_sensitive,
        min_matched_reviews=min_matched_reviews,
        top_n=top_n,
    )

    total = len(rows)
    rows = rows[skip : skip + limit]
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "keyword_mode": keyword_mode,
        "case_sensitive": case_sensitive,
        "rows": rows,
    }


@app.get("/datasets/{dataset_dir}/overview")
def dataset_overview(dataset_dir: str) -> dict[str, Any]:
    ds = safe_dataset_path(dataset_dir)
    meta_file = ds / "meta.json"
    games_file = ds / "games.jsonl"
    reviews_dir = ds / "reviews"
    appdetails_dir = ds / APPDETAILS_COMPACT_DIRNAME
    appdetails_raw_dir = ds / APPDETAILS_RAW_DIRNAME

    meta = json.loads(meta_file.read_text(encoding="utf-8")) if meta_file.exists() else {}
    games = read_jsonl(games_file)
    review_files = list(reviews_dir.glob("*.jsonl")) if reviews_dir.exists() else []
    appdetail_files = list(appdetails_dir.glob("*.json")) if appdetails_dir.exists() else []
    appdetail_raw_files = list(appdetails_raw_dir.glob("*.json")) if appdetails_raw_dir.exists() else []

    total_reviews = 0
    for path in review_files:
        with path.open("r", encoding="utf-8") as f:
            total_reviews += sum(1 for line in f if line.strip())

    return {
        "dataset_dir": dataset_display_id(ds),
        "meta": meta,
        "games_count": len(games),
        "review_files_count": len(review_files),
        "appdetails_files_count": len(appdetail_files),
        "appdetails_raw_files_count": len(appdetail_raw_files),
        "total_reviews": total_reviews,
        "generated_at_iso": datetime.utcnow().isoformat() + "Z",
    }
