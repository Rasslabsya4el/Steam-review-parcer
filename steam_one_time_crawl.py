import argparse
import html
import json
import logging
import random
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from steam_project_paths import dataset_display_id, resolve_dataset_path

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SteamOneTimeCrawler/2.0"
STEAM_TAGS_URL = "https://store.steampowered.com/tagdata/populartags/english"
STEAM_SEARCH_URL = "https://store.steampowered.com/search/results/"
COMMUNITY_HOME_URL = "https://steamcommunity.com/app/{appid}/homecontent/"
APPREVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"
APPDETAILS_URL = "https://store.steampowered.com/api/appdetails?appids={appid}&l=english"
SEARCH_CATEGORY1_BY_APP_TYPE = {
    "game": 998,
    "demo": 10,
    "dlc": 21,
}
MAX_RPM_WITH_ENRICH_APPDETAILS = 240
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class RequestRateLimiter:
    def __init__(self, max_rpm: int):
        self.max_rpm = max_rpm
        self.interval = 60.0 / max_rpm if max_rpm > 0 else 0.0
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def acquire(self) -> None:
        if self.max_rpm <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                wait = self._next_ts - now
                if wait <= 0:
                    self._next_ts = max(self._next_ts, now) + self.interval
                    return
            time.sleep(min(wait, 0.5))


@dataclass
class RequestStats:
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    requests_total: int = 0
    requests_ok: int = 0
    requests_failed: int = 0
    retries_total: int = 0
    failures_by_code: dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_request(self) -> None:
        with self._lock:
            self.requests_total += 1

    def record_success(self) -> None:
        with self._lock:
            self.requests_ok += 1

    def record_failure(self, key: str) -> None:
        with self._lock:
            self.requests_failed += 1
            self.failures_by_code[key] = self.failures_by_code.get(key, 0) + 1

    def record_retry(self) -> None:
        with self._lock:
            self.retries_total += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            now = self.finished_at if self.finished_at is not None else time.time()
            elapsed = max(0.001, now - self.started_at)
            return {
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "elapsed_seconds": round(elapsed, 3),
                "requests_total": self.requests_total,
                "requests_ok": self.requests_ok,
                "requests_failed": self.requests_failed,
                "retries_total": self.retries_total,
                "failures_by_code": dict(self.failures_by_code),
                "effective_rpm": round((self.requests_total / elapsed) * 60.0, 3),
                "failure_rate": round((self.requests_failed / self.requests_total), 6)
                if self.requests_total
                else 0.0,
            }

    def mark_finished(self) -> None:
        with self._lock:
            self.finished_at = time.time()


_RUNTIME_LOCK = threading.Lock()
_RUNTIME_LIMITER: RequestRateLimiter | None = None
_RUNTIME_STATS = RequestStats()
_RUNTIME_HTTP_RETRIES = 6
_RUNTIME_RETRY_BASE_SLEEP = 1.0
_RUNTIME_RETRY_JITTER = 0.6


def configure_http_runtime(
    max_rpm: int,
    http_retries: int,
    retry_base_sleep: float,
    retry_jitter: float,
) -> None:
    global _RUNTIME_LIMITER, _RUNTIME_STATS
    global _RUNTIME_HTTP_RETRIES, _RUNTIME_RETRY_BASE_SLEEP, _RUNTIME_RETRY_JITTER

    with _RUNTIME_LOCK:
        _RUNTIME_LIMITER = RequestRateLimiter(max_rpm) if max_rpm > 0 else None
        _RUNTIME_STATS = RequestStats()
        _RUNTIME_HTTP_RETRIES = max(1, int(http_retries))
        _RUNTIME_RETRY_BASE_SLEEP = max(0.0, float(retry_base_sleep))
        _RUNTIME_RETRY_JITTER = max(0.0, float(retry_jitter))


def set_runtime_rate_limit(max_rpm: int) -> None:
    global _RUNTIME_LIMITER
    with _RUNTIME_LOCK:
        _RUNTIME_LIMITER = RequestRateLimiter(max_rpm) if max_rpm > 0 else None


def get_request_stats() -> dict[str, Any]:
    return _RUNTIME_STATS.snapshot()


def finish_request_stats() -> None:
    _RUNTIME_STATS.mark_finished()


def _parse_retry_after_seconds(exc: HTTPError) -> float | None:
    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    if retry_after and retry_after.strip().isdigit():
        return float(retry_after.strip())
    return None


def _compute_retry_sleep(attempt: int, base_sleep: float, extra_retry_after: float | None = None) -> float:
    backoff = base_sleep * (2 ** max(0, attempt - 1))
    jitter = random.uniform(0.0, _RUNTIME_RETRY_JITTER)
    delay = backoff + jitter
    if extra_retry_after is not None:
        delay = max(delay, extra_retry_after)
    return min(delay, 120.0)


def _short_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        path = parts.path or "/"
        if len(path) > 80:
            path = path[:77] + "..."
        base = f"{parts.scheme}://{parts.netloc}{path}"
        return f"{base}?..." if parts.query else base
    except Exception:
        return url[:120]


def http_get(url: str, timeout: int, retries: int | None = None, retry_sleep: float | None = None) -> str:
    attempts_limit = retries if retries is not None else _RUNTIME_HTTP_RETRIES
    base_sleep = retry_sleep if retry_sleep is not None else _RUNTIME_RETRY_BASE_SLEEP

    last_exc: Exception | None = None
    for attempt in range(1, attempts_limit + 1):
        if _RUNTIME_LIMITER is not None:
            _RUNTIME_LIMITER.acquire()

        _RUNTIME_STATS.record_request()

        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=timeout) as response:
                body = response.read().decode("utf-8", errors="replace")
            _RUNTIME_STATS.record_success()
            return body

        except HTTPError as exc:
            last_exc = exc
            code = str(exc.code)
            _RUNTIME_STATS.record_failure(code)

            retryable = exc.code in {429, 500, 502, 503, 504}
            if attempt < attempts_limit and retryable:
                _RUNTIME_STATS.record_retry()
                retry_after = _parse_retry_after_seconds(exc)
                delay = _compute_retry_sleep(attempt, base_sleep, retry_after)
                LOGGER.warning(
                    "HTTP retry (code=%s, attempt=%s/%s, sleep=%.2fs, timeout=%ss, url=%s)",
                    exc.code,
                    attempt,
                    attempts_limit,
                    delay,
                    timeout,
                    _short_url(url),
                )
                time.sleep(delay)
                continue
            LOGGER.error(
                "HTTP failed (code=%s, attempt=%s/%s, timeout=%ss, url=%s)",
                exc.code,
                attempt,
                attempts_limit,
                timeout,
                _short_url(url),
            )
            raise

        except (URLError, TimeoutError) as exc:
            last_exc = exc
            _RUNTIME_STATS.record_failure("network")

            if attempt < attempts_limit:
                _RUNTIME_STATS.record_retry()
                delay = _compute_retry_sleep(attempt, base_sleep, None)
                LOGGER.warning(
                    "HTTP retry (network=%s, attempt=%s/%s, sleep=%.2fs, timeout=%ss, url=%s)",
                    type(exc).__name__,
                    attempt,
                    attempts_limit,
                    delay,
                    timeout,
                    _short_url(url),
                )
                time.sleep(delay)
                continue
            LOGGER.error(
                "HTTP failed (network=%s, attempt=%s/%s, timeout=%ss, url=%s)",
                type(exc).__name__,
                attempt,
                attempts_limit,
                timeout,
                _short_url(url),
            )
            raise

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("http_get failed without exception")


def fetch_english_tags(timeout: int):
    payload = json.loads(http_get(STEAM_TAGS_URL, timeout))
    tags = []
    for item in payload:
        if isinstance(item, dict) and isinstance(item.get("tagid"), int) and isinstance(item.get("name"), str):
            tags.append({"tagid": item["tagid"], "name": item["name"].strip()})
    return tags


def normalize_app_type(value: str) -> str:
    return value.strip().casefold()


def infer_search_category1(
    include_types: set[str],
    exclude_types: set[str],
) -> tuple[int | None, str | None]:
    if exclude_types:
        return None, None
    if len(include_types) != 1:
        return None, None

    only_type = next(iter(include_types))
    category = SEARCH_CATEGORY1_BY_APP_TYPE.get(only_type)
    if category is None:
        return None, None
    return category, only_type


def fetch_appdetails_live(appid: int, timeout: int) -> dict[str, Any] | None:
    url = APPDETAILS_URL.format(appid=appid)
    payload = json.loads(http_get(url, timeout))
    item = payload.get(str(appid), {})
    if not item.get("success"):
        return None
    data = item.get("data")
    return data if isinstance(data, dict) else None


def resolve_input_tags(input_tags, all_tags):
    by_name = {x["name"].casefold(): x for x in all_tags}
    by_id = {x["tagid"]: x for x in all_tags}
    resolved = []
    unknown = []
    for token in input_tags:
        value = token.strip()
        if not value:
            continue
        if value.isdigit():
            tag = by_id.get(int(value))
        else:
            tag = by_name.get(value.casefold())
        if tag:
            resolved.append(tag)
        else:
            unknown.append(value)
    unique = {x["tagid"]: x for x in resolved}
    return list(unique.values()), unknown


def normalize_cli_tag_inputs(raw_tokens: list[str], all_tags: list[dict[str, Any]]) -> list[str]:
    """
    CLI helper:
      - supports comma-separated chunks
      - auto-merges adjacent chunks into one multi-word tag if exact Steam tag exists
    """
    if not raw_tokens:
        return []

    chunks: list[str] = []
    for token in raw_tokens:
        for part in str(token).split(","):
            cleaned = part.strip().strip("\"").strip("'").strip()
            if cleaned:
                chunks.append(cleaned)

    if not chunks:
        return []

    by_name = {x["name"].casefold(): x["name"] for x in all_tags if isinstance(x.get("name"), str)}

    merged: list[str] = []
    i = 0
    n = len(chunks)
    while i < n:
        if chunks[i].isdigit():
            merged.append(chunks[i])
            i += 1
            continue

        matched = None
        next_i = i + 1
        for j in range(n, i, -1):
            phrase = " ".join(chunks[i:j]).strip()
            if phrase.casefold() in by_name:
                matched = by_name[phrase.casefold()]
                next_i = j
                break

        if matched is not None:
            merged.append(matched)
            i = next_i
            continue

        merged.append(chunks[i])
        i += 1

    return merged


def fetch_search_payload(
    tag_ids,
    timeout,
    start=0,
    page_size=50,
    search_category1: int | None = None,
    untag_ids=None,
):
    query = {
        "query": "",
        "start": start,
        "count": page_size,
        "dynamic_data": "",
        "sort_by": "_ASC",
        "supportedlang": "english",
        "tags": ",".join(str(t) for t in tag_ids),
        "snr": "1_7_7_230_7",
        "infinite": 1,
    }
    if untag_ids:
        query["untags"] = ",".join(str(t) for t in untag_ids)
    if search_category1 is not None:
        query["category1"] = int(search_category1)

    url = f"{STEAM_SEARCH_URL}?{urlencode(query)}"
    data = json.loads(http_get(url, timeout))
    if int(data.get("success", 0)) != 1:
        raise RuntimeError(f"Steam search failed at start={start}")
    return data


def fetch_search_total_count(
    tag_ids,
    timeout,
    search_category1: int | None = None,
    untag_ids=None,
):
    data = fetch_search_payload(
        tag_ids=tag_ids,
        timeout=timeout,
        start=0,
        page_size=1,
        search_category1=search_category1,
        untag_ids=untag_ids,
    )
    return int(data.get("total_count", 0))


def search_games_by_tags(
    tag_ids,
    timeout,
    page_size=50,
    max_games=None,
    sleep_between_pages=0.1,
    search_category1: int | None = None,
    untag_ids=None,
    progress_callback=None,
):
    games = {}
    start = 0
    while True:
        data = fetch_search_payload(
            tag_ids=tag_ids,
            timeout=timeout,
            start=start,
            page_size=page_size,
            search_category1=search_category1,
            untag_ids=untag_ids,
        )

        page_games = parse_search_results(data.get("results_html", ""))
        if not page_games:
            break

        for game in page_games:
            games.setdefault(game["appid"], game)
            if max_games and len(games) >= max_games:
                if progress_callback:
                    progress_callback(
                        {
                            "found": len(games),
                            "start": start,
                            "total_count": int(data.get("total_count", 0)),
                        }
                    )
                return list(games.values())

        if progress_callback:
            progress_callback(
                {
                    "found": len(games),
                    "start": start,
                    "total_count": int(data.get("total_count", 0)),
                }
            )

        start += page_size
        total = int(data.get("total_count", 0))
        if start >= total:
            break
        time.sleep(sleep_between_pages)

    if progress_callback:
        progress_callback(
            {
                "found": len(games),
                "start": start,
                "total_count": int(data.get("total_count", 0)) if "data" in locals() else 0,
            }
        )

    return list(games.values())


def search_games_by_tags_until_filtered_count(
    tag_ids,
    timeout,
    include_types: set[str],
    exclude_types: set[str],
    page_size=50,
    max_games=None,
    sleep_between_pages=0.1,
    progress_callback=None,
    search_category1: int | None = None,
    search_category1_app_type: str | None = None,
    untag_ids=None,
):
    """
    Stream search pages until we collect max_games after app-type filtering.
    This avoids guessing with prefilter multipliers.
    """
    kept: dict[int, dict[str, Any]] = {}
    seen_source: set[int] = set()
    filtered_out_by_type = 0
    checked_source = 0
    start = 0

    while True:
        data = fetch_search_payload(
            tag_ids=tag_ids,
            timeout=timeout,
            start=start,
            page_size=page_size,
            search_category1=search_category1,
            untag_ids=untag_ids,
        )

        page_games = parse_search_results(data.get("results_html", ""))
        if not page_games:
            break

        for game in page_games:
            appid = int(game["appid"])
            if appid in seen_source:
                continue
            seen_source.add(appid)
            checked_source += 1

            if isinstance(search_category1_app_type, str) and search_category1_app_type:
                app_type = search_category1_app_type
            else:
                try:
                    details = fetch_appdetails_live(appid, timeout)
                    t = details.get("type") if isinstance(details, dict) else None
                    app_type = normalize_app_type(t) if isinstance(t, str) else "unknown"
                except Exception:
                    app_type = "unknown"

            if include_types and app_type not in include_types:
                filtered_out_by_type += 1
            elif exclude_types and app_type in exclude_types:
                filtered_out_by_type += 1
            else:
                game = dict(game)
                game["app_type"] = app_type
                kept[appid] = game
                if max_games and len(kept) >= max_games:
                    if progress_callback:
                        progress_callback(
                            {
                                "checked_source": checked_source,
                                "kept": len(kept),
                                "filtered_out_by_type": filtered_out_by_type,
                                "start": start,
                                "total_count": int(data.get("total_count", 0)),
                            }
                        )
                    return list(kept.values()), filtered_out_by_type, checked_source

            if progress_callback and checked_source % 50 == 0:
                progress_callback(
                    {
                        "checked_source": checked_source,
                        "kept": len(kept),
                        "filtered_out_by_type": filtered_out_by_type,
                        "start": start,
                        "total_count": int(data.get("total_count", 0)),
                    }
                )

        start += page_size
        total = int(data.get("total_count", 0))
        if start >= total:
            break
        time.sleep(sleep_between_pages)

    if progress_callback:
        progress_callback(
            {
                "checked_source": checked_source,
                "kept": len(kept),
                "filtered_out_by_type": filtered_out_by_type,
                "start": start,
                "total_count": int(data.get("total_count", 0)),
            }
        )

    return list(kept.values()), filtered_out_by_type, checked_source


def parse_search_results(results_html):
    blocks = re.findall(r'<a\s+[^>]*class="search_result_row[^>]*>.*?</a>', results_html, flags=re.S)
    games = []
    for block in blocks:
        appid_m = re.search(r'data-ds-appid="(\d+)"', block)
        title_m = re.search(r'<span class="title">(.*?)</span>', block, flags=re.S)
        tagids_m = re.search(r'data-ds-tagids="(\[[^\"]*\])"', block)
        if not appid_m or not title_m:
            continue
        appid = int(appid_m.group(1))
        name = html.unescape(re.sub(r"\s+", " ", title_m.group(1))).strip()
        search_tag_ids = []
        if tagids_m:
            try:
                search_tag_ids = json.loads(tagids_m.group(1))
            except json.JSONDecodeError:
                search_tag_ids = []
        games.append({"appid": appid, "name": name, "search_tag_ids": search_tag_ids})
    return games


def extract_div_block(html_text: str, start_pos: int):
    token_re = re.compile(r"<div\b|</div>", flags=re.I)
    depth = 0
    began = False
    for m in token_re.finditer(html_text, start_pos):
        token = m.group(0).lower()
        if token.startswith("<div"):
            depth += 1
            began = True
        else:
            depth -= 1
            if began and depth == 0:
                return html_text[start_pos:m.end()], m.end()
    return None, start_pos


def extract_card_blocks(html_text: str):
    marker = 'class="apphub_Card modalContentLink interactable"'
    blocks = []
    pos = 0
    while True:
        idx = html_text.find(marker, pos)
        if idx == -1:
            break
        start = html_text.rfind("<div", 0, idx)
        if start == -1:
            break
        block, end_pos = extract_div_block(html_text, start)
        if not block:
            break
        blocks.append(block)
        pos = end_pos
    return blocks


def extract_text_content(card_html: str):
    marker = 'class="apphub_CardTextContent"'
    idx = card_html.find(marker)
    if idx == -1:
        return ""
    start = card_html.rfind("<div", 0, idx)
    if start == -1:
        return ""
    block, _ = extract_div_block(card_html, start)
    if not block:
        return ""

    block = re.sub(r'<div class="date_posted">.*?</div>', "", block, flags=re.S)
    block = re.sub(r"<br\s*/?>", "\n", block, flags=re.I)
    block = re.sub(r"<[^>]+>", "", block)
    block = html.unescape(block)
    block = re.sub(r"\r", "", block)
    block = re.sub(r"\n{3,}", "\n\n", block)
    block = re.sub(r"[ \t]+", " ", block)
    return block.strip()


def parse_next_params(html_text: str):
    def get_value(name: str):
        m = re.search(rf'name="{re.escape(name)}" value="([^"]*)"', html_text)
        return m.group(1) if m else None

    fields = [
        "userreviewscursor",
        "userreviewsoffset",
        "p",
        "itemspage",
        "screenshotspage",
        "videospage",
        "artpage",
        "allguidepage",
        "webguidepage",
        "integratedguidepage",
        "discussionspage",
    ]
    result = {}
    for field in fields:
        value = get_value(field)
        if value is not None:
            result[field] = value
    return result


def parse_review_cards(page_html: str):
    cards = extract_card_blocks(page_html)
    parsed = []
    for card in cards:
        url_m = re.search(r'data-modal-content-url="([^"]+/recommended/\d+/)"', card)
        rating_m = re.search(r'<div class="title">\s*(Recommended|Not Recommended)\s*</div>', card)
        date_m = re.search(r'<div class="date_posted">\s*(.*?)\s*</div>', card, flags=re.S)
        hours_m = re.search(r'<div class="hours">\s*(.*?)\s*</div>', card, flags=re.S)

        review_url = html.unescape(url_m.group(1)) if url_m else None
        if not review_url:
            continue

        author_url = review_url.split("/recommended/")[0] + "/"
        recommended = bool(rating_m and rating_m.group(1) == "Recommended")
        posted_raw = html.unescape(date_m.group(1)).strip() if date_m else ""
        hours_raw = html.unescape(hours_m.group(1)).strip() if hours_m else ""
        review_text = extract_text_content(card)

        parsed.append(
            {
                "review_url": review_url,
                "author_url": author_url,
                "recommended": recommended,
                "posted": posted_raw,
                "hours_on_record": hours_raw,
                "review_text": review_text,
            }
        )
    return parsed


def default_review_params(appid: int):
    return {
        "userreviewsoffset": "0",
        "p": "1",
        "itemspage": "1",
        "screenshotspage": "1",
        "videospage": "1",
        "artpage": "1",
        "allguidepage": "1",
        "webguidepage": "1",
        "integratedguidepage": "1",
        "discussionspage": "1",
        "appid": str(appid),
        "appHubSubSection": "10",
        "browsefilter": "mostrecent",
        "filterLanguage": "all",
        "searchText": "",
        "maxInappropriateScore": "100",
        "forceanon": "1",
    }


def default_appreviews_params(cursor: str, num_per_page: int):
    safe_num = max(1, min(100, int(num_per_page)))
    return {
        "json": "1",
        "language": "all",
        "review_type": "all",
        "purchase_type": "all",
        "filter": "recent",
        "num_per_page": str(safe_num),
        "cursor": cursor,
    }


def _format_hours_from_minutes(minutes: Any) -> str:
    if isinstance(minutes, (int, float)):
        return f"{float(minutes) / 60.0:.1f} hrs on record"
    return ""


def _format_posted_from_timestamp(ts: Any) -> str:
    if isinstance(ts, int) and ts > 0:
        return time.strftime("%Y-%m-%d", time.gmtime(ts))
    return ""


def map_appreview_to_internal(item: dict[str, Any], appid: int) -> dict[str, Any]:
    author = item.get("author") if isinstance(item.get("author"), dict) else {}
    steamid = str(author.get("steamid")) if author.get("steamid") is not None else ""
    review_url = f"https://steamcommunity.com/profiles/{steamid}/recommended/{appid}/" if steamid else ""
    author_url = f"https://steamcommunity.com/profiles/{steamid}/" if steamid else ""

    playtime_forever = author.get("playtime_forever")
    playtime_2weeks = author.get("playtime_last_two_weeks")

    return {
        "source": "appreviews",
        "recommendationid": str(item.get("recommendationid", "")),
        "review_url": review_url,
        "author_url": author_url,
        "author_steamid": steamid,
        "recommended": bool(item.get("voted_up", False)),
        "posted": _format_posted_from_timestamp(item.get("timestamp_created")),
        "hours_on_record": _format_hours_from_minutes(playtime_forever),
        "hours_on_record_2weeks": _format_hours_from_minutes(playtime_2weeks),
        "review_text": str(item.get("review", "")),
        "language": str(item.get("language", "")),
        "timestamp_created": item.get("timestamp_created"),
        "timestamp_updated": item.get("timestamp_updated"),
        "votes_up": item.get("votes_up"),
        "votes_funny": item.get("votes_funny"),
        "comment_count": item.get("comment_count"),
        "steam_purchase": item.get("steam_purchase"),
        "received_for_free": item.get("received_for_free"),
        "written_during_early_access": item.get("written_during_early_access"),
    }


def fetch_all_reviews_for_app_via_appreviews(
    appid: int,
    timeout: int,
    sleep_between_pages: float,
    max_pages: int | None = None,
    num_per_page: int = 100,
    should_stop=None,
):
    seen_ids: set[str] = set()
    all_reviews: list[dict[str, Any]] = []
    cursor = "*"
    page_idx = 0
    total_reviews_hint: int | None = None

    while True:
        if callable(should_stop) and should_stop():
            raise InterruptedError("cancelled")
        page_idx += 1
        if max_pages and page_idx > max_pages:
            break

        params = default_appreviews_params(cursor=cursor, num_per_page=num_per_page)
        url = f"{APPREVIEWS_URL.format(appid=appid)}?{urlencode(params)}"
        payload = json.loads(http_get(url, timeout))
        if int(payload.get("success", 0)) != 1:
            raise RuntimeError(f"appreviews returned success={payload.get('success')} for appid={appid}")

        query_summary = payload.get("query_summary") if isinstance(payload.get("query_summary"), dict) else {}
        if total_reviews_hint is None and isinstance(query_summary.get("total_reviews"), int):
            total_reviews_hint = int(query_summary["total_reviews"])

        page_reviews = payload.get("reviews", [])
        if not isinstance(page_reviews, list) or not page_reviews:
            break

        new_items = 0
        for item in page_reviews:
            if not isinstance(item, dict):
                continue
            mapped = map_appreview_to_internal(item, appid)
            rec_id = mapped.get("recommendationid") or mapped.get("review_url")
            if not rec_id:
                continue
            if rec_id in seen_ids:
                continue
            seen_ids.add(rec_id)
            all_reviews.append(mapped)
            new_items += 1

        next_cursor = payload.get("cursor")
        if not isinstance(next_cursor, str) or not next_cursor.strip():
            break
        cursor = next_cursor

        if new_items == 0:
            break

        if callable(should_stop) and should_stop():
            raise InterruptedError("cancelled")
        time.sleep(sleep_between_pages)

    return {
        "provider": "appreviews",
        "reviews": all_reviews,
        "total_reviews_hint": total_reviews_hint,
        "pages": page_idx,
    }


def fetch_all_reviews_for_app_via_community(
    appid: int,
    timeout: int,
    sleep_between_pages: float,
    max_pages=None,
    should_stop=None,
):
    params = default_review_params(appid)
    seen_review_urls = set()
    all_reviews = []
    visited_page_keys = set()
    page_idx = 0

    while True:
        if callable(should_stop) and should_stop():
            raise InterruptedError("cancelled")
        page_idx += 1
        if max_pages and page_idx > max_pages:
            break

        key = (params.get("userreviewscursor", ""), params.get("userreviewsoffset", ""), params.get("p", ""))
        if key in visited_page_keys:
            break
        visited_page_keys.add(key)

        url = f"{COMMUNITY_HOME_URL.format(appid=appid)}?{urlencode(params)}"
        page_html = http_get(url, timeout)
        page_reviews = parse_review_cards(page_html)
        if not page_reviews:
            break

        new_items = 0
        for item in page_reviews:
            if item["review_url"] in seen_review_urls:
                continue
            seen_review_urls.add(item["review_url"])
            all_reviews.append(item)
            new_items += 1

        next_params = parse_next_params(page_html)
        if not next_params:
            break

        for k, v in next_params.items():
            params[k] = v

        if new_items == 0:
            break

        if callable(should_stop) and should_stop():
            raise InterruptedError("cancelled")
        time.sleep(sleep_between_pages)

    return all_reviews


def fetch_all_reviews_for_app(
    appid: int,
    timeout: int,
    sleep_between_pages: float,
    max_pages=None,
    review_source: str = "appreviews",
    appreviews_num_per_page: int = 100,
    should_stop=None,
):
    """
    review_source:
      - appreviews: use store appreviews endpoint only
      - community: use steamcommunity homecontent only
    """
    source = review_source.casefold().strip()
    if source not in {"appreviews", "community"}:
        raise ValueError(f"Unsupported review_source: {review_source}")

    if source == "appreviews":
        result = fetch_all_reviews_for_app_via_appreviews(
            appid=appid,
            timeout=timeout,
            sleep_between_pages=sleep_between_pages,
            max_pages=max_pages,
            num_per_page=appreviews_num_per_page,
            should_stop=should_stop,
        )
        return result["reviews"]

    return fetch_all_reviews_for_app_via_community(
        appid=appid,
        timeout=timeout,
        sleep_between_pages=sleep_between_pages,
        max_pages=max_pages,
        should_stop=should_stop,
    )


def crawl_game_reviews_with_retries(
    game: dict[str, Any],
    timeout: int,
    sleep_between_pages: float,
    max_pages: int | None,
    max_app_retries: int,
    review_source: str = "appreviews",
    appreviews_num_per_page: int = 100,
    should_stop=None,
) -> dict[str, Any]:
    appid = int(game["appid"])
    name = str(game.get("name", appid))

    last_exc: Exception | None = None
    for attempt in range(1, max_app_retries + 1):
        if callable(should_stop) and should_stop():
            return {
                "status": "cancelled",
                "appid": appid,
                "name": name,
                "attempts": attempt,
                "error": "cancelled",
            }
        try:
            reviews = fetch_all_reviews_for_app(
                appid=appid,
                timeout=timeout,
                sleep_between_pages=sleep_between_pages,
                max_pages=max_pages,
                review_source=review_source,
                appreviews_num_per_page=appreviews_num_per_page,
                should_stop=should_stop,
            )
            return {
                "status": "ok",
                "appid": appid,
                "name": name,
                "attempts": attempt,
                "reviews": reviews,
            }
        except InterruptedError:
            return {
                "status": "cancelled",
                "appid": appid,
                "name": name,
                "attempts": attempt,
                "error": "cancelled",
            }
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < max_app_retries:
                if callable(should_stop) and should_stop():
                    return {
                        "status": "cancelled",
                        "appid": appid,
                        "name": name,
                        "attempts": attempt,
                        "error": "cancelled",
                    }
                time.sleep(min(90.0, 1.5 * (2 ** (attempt - 1))))

    return {
        "status": "error",
        "appid": appid,
        "name": name,
        "attempts": max_app_retries,
        "error": str(last_exc) if last_exc else "Unknown error",
    }


def load_completed_appids(path: Path):
    if not path.exists():
        return set()
    items = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s.isdigit():
            items.add(int(s))
    return items


def append_completed_appid(path: Path, appid: int):
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{appid}\n")


def write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "One-time crawl: find all games by input Steam tags and download all historical reviews "
            "for each game into local JSONL files."
        )
    )
    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help=(
            "Input tags. Recommended: quoted comma-separated, "
            'for example --tags "Action RPG, Hack and Slash"'
        ),
    )
    parser.add_argument(
        "--exclude-tags",
        nargs="+",
        default=None,
        help=(
            "Exclude tags from Steam search using untags parameter. "
            'Example: --exclude-tags "Massively Multiplayer, Free to Play"'
        ),
    )
    parser.add_argument(
        "--dataset-dir",
        default="steam_dataset",
        help="Dataset folder name (saved under datasets/ by default) or explicit relative path",
    )
    parser.add_argument("--max-games", type=int, default=None, help="Limit number of games (for testing)")
    parser.add_argument("--search-page-size", type=int, default=50, help="Steam search page size")
    parser.add_argument("--sleep-search", type=float, default=0.1, help="Sleep between Steam search pages")
    parser.add_argument("--sleep-reviews", type=float, default=0.25, help="Sleep between review pages per game")
    parser.add_argument("--max-review-pages-per-game", type=int, default=None, help="Limit review pages per game")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout")
    parser.add_argument("--resume", action="store_true", help="Skip already completed appids")
    parser.add_argument("--workers", type=int, default=12, help="Concurrent games to crawl")
    parser.add_argument("--max-rpm", type=int, default=800, help="Global max HTTP requests per minute (0 = no limit)")
    parser.add_argument("--http-retries", type=int, default=6, help="HTTP retries per request")
    parser.add_argument("--http-retry-base-sleep", type=float, default=1.0, help="Base backoff seconds for HTTP retries")
    parser.add_argument("--http-retry-jitter", type=float, default=0.6, help="Additional random jitter for retries")
    parser.add_argument("--max-app-retries", type=int, default=4, help="Retries for a full app crawl")
    parser.add_argument(
        "--review-source",
        choices=["appreviews", "community"],
        default="appreviews",
        help="Where to fetch reviews from. Default: appreviews",
    )
    parser.add_argument(
        "--appreviews-num-per-page",
        type=int,
        default=100,
        help="Page size for store appreviews endpoint. Default: 100",
    )
    parser.add_argument(
        "--include-app-types",
        nargs="+",
        default=None,
        help="Only crawl selected Steam app types (example: game dlc demo)",
    )
    parser.add_argument(
        "--exclude-app-types",
        nargs="+",
        default=None,
        help="Exclude selected Steam app types (example: dlc demo)",
    )
    parser.add_argument(
        "--heartbeat-seconds",
        type=float,
        default=10.0,
        help="Print progress heartbeat every N seconds while waiting for game workers",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce console output",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    def log(message: str) -> None:
        if not args.quiet:
            print(message, flush=True)

    runtime_max_rpm = int(args.max_rpm)
    enrich_appdetails_enabled = bool(getattr(args, "enrich_appdetails", False))
    if enrich_appdetails_enabled and runtime_max_rpm > MAX_RPM_WITH_ENRICH_APPDETAILS:
        runtime_max_rpm = MAX_RPM_WITH_ENRICH_APPDETAILS
        log(
            f"Notice: max_rpm auto-capped to {runtime_max_rpm} "
            "because enrich_appdetails=true (to reduce 429 throttling)"
        )

    configure_http_runtime(
        max_rpm=runtime_max_rpm,
        http_retries=args.http_retries,
        retry_base_sleep=args.http_retry_base_sleep,
        retry_jitter=args.http_retry_jitter,
    )

    dataset_dir = resolve_dataset_path(args.dataset_dir)
    dataset_dir_id = dataset_display_id(dataset_dir)
    log(
        "Starting crawl: "
        f"dataset={dataset_dir_id} workers={args.workers} max_rpm={runtime_max_rpm} "
        f"review_source={args.review_source}"
    )
    if args.max_review_pages_per_game is None:
        log(
            "Notice: max_review_pages_per_game is not set. "
            "This means full review history per app and can take a long time."
        )

    reviews_dir = dataset_dir / "reviews"
    games_file = dataset_dir / "games.jsonl"
    completed_file = dataset_dir / "completed_appids.txt"
    errors_file = dataset_dir / "errors.jsonl"
    meta_file = dataset_dir / "meta.json"

    reviews_dir.mkdir(parents=True, exist_ok=True)

    all_tags = fetch_english_tags(args.timeout)
    normalized_tag_inputs = normalize_cli_tag_inputs(args.tags, all_tags)
    resolved_tags, unknown = resolve_input_tags(normalized_tag_inputs, all_tags)
    if unknown:
        raise SystemExit("Unknown tags: " + ", ".join(unknown))
    if not resolved_tags:
        raise SystemExit("No valid tags passed")

    normalized_exclude_tag_inputs = normalize_cli_tag_inputs(args.exclude_tags or [], all_tags)
    resolved_exclude_tags, unknown_exclude = resolve_input_tags(normalized_exclude_tag_inputs, all_tags)
    if unknown_exclude:
        raise SystemExit("Unknown exclude tags: " + ", ".join(unknown_exclude))

    tag_ids = [x["tagid"] for x in resolved_tags]
    exclude_tag_ids = [x["tagid"] for x in resolved_exclude_tags]
    tag_name_by_id = {x["tagid"]: x["name"] for x in all_tags}

    include_types = {
        normalize_app_type(x)
        for x in (args.include_app_types or [])
        if isinstance(x, str) and x.strip()
    }
    exclude_types = {
        normalize_app_type(x)
        for x in (args.exclude_app_types or [])
        if isinstance(x, str) and x.strip()
    }
    use_type_filter = bool(include_types or exclude_types)
    search_category1, search_category1_app_type = infer_search_category1(include_types, exclude_types)
    if search_category1 is not None:
        log(
            "Type filter optimization enabled: "
            f"using Steam category1={search_category1} for app_type={search_category1_app_type}"
        )
    if use_type_filter:
        log(
            "Type filter enabled: scanning Steam pages until "
            f"{args.max_games if args.max_games is not None else 'all'} matched apps are collected."
        )

        def _on_filter_progress(state: dict[str, Any]) -> None:
            stats = get_request_stats()
            failure_codes = stats.get("failures_by_code", {})
            top_failure_codes = ",".join(
                f"{k}:{v}" for k, v in sorted(failure_codes.items(), key=lambda x: x[1], reverse=True)[:3]
            )
            log(
                f"[type-filter] checked={state['checked_source']} kept={state['kept']} "
                f"filtered={state['filtered_out_by_type']} "
                f"requests={stats['requests_total']} retries={stats['retries_total']} "
                f"failures={stats['requests_failed']} "
                f"failure_codes={top_failure_codes or '-'}"
            )

        games, filtered_out_by_type, source_games_considered = search_games_by_tags_until_filtered_count(
            tag_ids=tag_ids,
            untag_ids=exclude_tag_ids,
            timeout=args.timeout,
            include_types=include_types,
            exclude_types=exclude_types,
            page_size=args.search_page_size,
            max_games=args.max_games,
            sleep_between_pages=args.sleep_search,
            progress_callback=_on_filter_progress,
            search_category1=search_category1,
            search_category1_app_type=search_category1_app_type,
        )
    else:
        games = search_games_by_tags(
            tag_ids=tag_ids,
            untag_ids=exclude_tag_ids,
            timeout=args.timeout,
            page_size=args.search_page_size,
            max_games=args.max_games,
            sleep_between_pages=args.sleep_search,
            search_category1=search_category1,
        )
        filtered_out_by_type = 0
        source_games_considered = len(games)

    log(f"Found source games by tags: {source_games_considered}; kept after type filter: {len(games)}")
    if args.max_games is not None and len(games) < args.max_games:
        log(
            f"Only {len(games)} matches available for current filters "
            f"(requested max_games={args.max_games}). Continuing without error."
        )

    game_rows = []
    for game in sorted(games, key=lambda x: x["appid"]):
        game_rows.append(
            {
                "appid": game["appid"],
                "name": game["name"],
                "input_tag_ids": tag_ids,
                "input_tags": [tag_name_by_id[t] for t in tag_ids if t in tag_name_by_id],
                "exclude_tag_ids": exclude_tag_ids,
                "exclude_tags": [tag_name_by_id[t] for t in exclude_tag_ids if t in tag_name_by_id],
                "search_tag_ids": game["search_tag_ids"],
                "search_tags": [tag_name_by_id[t] for t in game["search_tag_ids"] if t in tag_name_by_id],
                "app_type": game.get("app_type"),
            }
        )
    write_jsonl(games_file, game_rows)
    log(f"Games written to: {games_file} ({len(game_rows)} rows)")

    completed = load_completed_appids(completed_file) if args.resume else set()

    total_reviews_saved = 0
    processed_count = 0
    skipped_count = 0
    errors_count = 0

    pending_games = []
    for game in game_rows:
        appid = int(game["appid"])
        review_file = reviews_dir / f"{appid}.jsonl"
        if args.resume and appid in completed and review_file.exists():
            skipped_count += 1
            continue
        pending_games.append(game)

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures_map = {
            executor.submit(
                crawl_game_reviews_with_retries,
                game,
                args.timeout,
                args.sleep_reviews,
                args.max_review_pages_per_game,
                args.max_app_retries,
                args.review_source,
                args.appreviews_num_per_page,
            ): game
            for game in pending_games
        }

        total_pending = len(futures_map)
        done_pending = 0
        pending = set(futures_map.keys())
        last_heartbeat = time.time()

        while pending:
            done, pending = wait(
                pending,
                timeout=max(0.5, float(args.heartbeat_seconds)),
                return_when=FIRST_COMPLETED,
            )

            now = time.time()
            if not done and (now - last_heartbeat) >= max(0.5, float(args.heartbeat_seconds)):
                stats = get_request_stats()
                log(
                    f"[heartbeat] done={done_pending}/{total_pending} pending={len(pending)} "
                    f"requests={stats['requests_total']} retries={stats['retries_total']} "
                    f"failure_rate={stats['failure_rate']}"
                )
                last_heartbeat = now
                continue

            for future in done:
                done_pending += 1
                game = futures_map[future]
                appid = int(game["appid"])
                review_file = reviews_dir / f"{appid}.jsonl"

                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors_count += 1
                    append_jsonl(
                        errors_file,
                        {
                            "appid": appid,
                            "name": game["name"],
                            "error": str(exc),
                            "attempts": args.max_app_retries,
                            "ts": int(time.time()),
                        },
                    )
                    log(f"[{done_pending}/{total_pending}] ERROR appid={appid}: {exc}")
                    continue

                if result["status"] == "ok":
                    reviews = result["reviews"]
                    for item in reviews:
                        item["appid"] = appid
                        item["game_name"] = game["name"]
                    write_jsonl(review_file, reviews)
                    append_completed_appid(completed_file, appid)
                    total_reviews_saved += len(reviews)
                    processed_count += 1
                    log(
                        f"[{done_pending}/{total_pending}] appid={appid} reviews={len(reviews)} attempts={result['attempts']}"
                    )
                else:
                    errors_count += 1
                    append_jsonl(
                        errors_file,
                        {
                            "appid": appid,
                            "name": game["name"],
                            "error": result.get("error", "Unknown error"),
                            "attempts": result.get("attempts", args.max_app_retries),
                            "ts": int(time.time()),
                        },
                    )
                    log(
                        f"[{done_pending}/{total_pending}] ERROR appid={appid} attempts={result.get('attempts')} "
                        f"error={result.get('error')}"
                    )

    finish_request_stats()
    request_stats = get_request_stats()

    meta = {
        "input_tags": [x["name"] for x in resolved_tags],
        "input_tag_ids": tag_ids,
        "exclude_tags": [x["name"] for x in resolved_exclude_tags],
        "exclude_tag_ids": exclude_tag_ids,
        "total_games_found": len(game_rows),
        "requested_max_games": args.max_games,
        "effective_max_games": min(args.max_games, len(game_rows)) if args.max_games is not None else len(game_rows),
        "processed_games": processed_count,
        "skipped_games": skipped_count,
        "errors": errors_count,
        "total_reviews_saved": total_reviews_saved,
        "dataset_dir": dataset_dir_id,
        "generated_at_unix": int(time.time()),
        "config": {
            "workers": args.workers,
            "max_rpm": args.max_rpm,
            "effective_runtime_max_rpm": runtime_max_rpm,
            "http_retries": args.http_retries,
            "http_retry_base_sleep": args.http_retry_base_sleep,
            "http_retry_jitter": args.http_retry_jitter,
            "max_app_retries": args.max_app_retries,
            "review_source": args.review_source,
            "appreviews_num_per_page": args.appreviews_num_per_page,
            "exclude_tags": [x["name"] for x in resolved_exclude_tags],
            "exclude_tag_ids": exclude_tag_ids,
            "include_app_types": sorted(include_types),
            "exclude_app_types": sorted(exclude_types),
            "search_category1": search_category1,
            "search_category1_app_type": search_category1_app_type,
            "filtered_out_by_type": filtered_out_by_type,
            "source_games_considered": source_games_considered,
        },
        "request_stats": request_stats,
    }
    meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Done")
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
