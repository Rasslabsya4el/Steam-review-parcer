from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests
from flask import Flask, jsonify, render_template, request


API_BASE_URL = os.getenv("STEAM_API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
HTTP_TIMEOUT = float(os.getenv("STEAM_UI_TIMEOUT_SECONDS", "120"))


APP_TYPE_OPTIONS = [
    {"value": "game", "label": "Game"},
    {"value": "demo", "label": "Demo"},
    {"value": "dlc", "label": "DLC"},
    {"value": "application", "label": "Application"},
    {"value": "video", "label": "Video"},
    {"value": "episode", "label": "Episode"},
    {"value": "series", "label": "Series"},
    {"value": "mod", "label": "Mod"},
    {"value": "music", "label": "Music"},
    {"value": "advertising", "label": "Advertising"},
    {"value": "hardware", "label": "Hardware"},
]


DEFAULT_CRAWL_CONFIG: dict[str, Any] = {
    "dataset_dir": "steam_dataset",
    "max_games": 300,
    "search_page_size": 50,
    "sleep_search": 0.1,
    "sleep_reviews": 0.25,
    "max_review_pages_per_game": 1,
    "timeout": 30,
    "resume": True,
    "enrich_appdetails": True,
    "store_raw_appdetails": True,
    "exclude_coming_soon": True,
    "workers": 12,
    "max_rpm": 800,
    "http_retries": 6,
    "http_retry_base_sleep": 1.0,
    "http_retry_jitter": 0.6,
    "max_app_retries": 4,
    "review_source": "appreviews",
    "appreviews_num_per_page": 100,
    "exclude_tags": [],
    "include_app_types": ["game"],
    "exclude_app_types": [],
}


CRAWL_ALLOWED_FIELDS = {
    "tags",
    "exclude_tags",
    "dataset_dir",
    "max_games",
    "search_page_size",
    "sleep_search",
    "sleep_reviews",
    "max_review_pages_per_game",
    "timeout",
    "resume",
    "enrich_appdetails",
    "store_raw_appdetails",
    "exclude_coming_soon",
    "workers",
    "max_rpm",
    "http_retries",
    "http_retry_base_sleep",
    "http_retry_jitter",
    "max_app_retries",
    "review_source",
    "appreviews_num_per_page",
    "include_app_types",
    "exclude_app_types",
}

CRAWL_ESTIMATE_ALLOWED_FIELDS = {
    "tags",
    "exclude_tags",
    "include_app_types",
    "exclude_app_types",
    "timeout",
    "search_page_size",
    "sample_size",
    "max_games",
}

EXPLORE_QUERY_ALLOWED_FIELDS = {
    "include_keywords",
    "exclude_keywords",
    "keyword_mode",
    "case_sensitive",
    "min_matched_reviews",
    "game_filters",
    "review_filters",
    "sort_by",
    "sort_order",
    "skip",
    "limit",
    "sample_reviews_per_game",
}

EXPLORE_REVIEWS_ALLOWED_FIELDS = {
    "include_keywords",
    "exclude_keywords",
    "keyword_mode",
    "case_sensitive",
    "review_filters",
    "sort_by",
    "skip",
    "limit",
    "highlight",
}

EXPLORE_FILTER_ALLOWED_FIELDS = {
    "field",
    "op",
    "value",
    "value_to",
    "case_sensitive",
}


app = Flask(__name__)


def backend_url(path: str) -> str:
    return f"{API_BASE_URL}{path}"


def json_error(message: str, status: int = 500) -> tuple[Any, int]:
    return jsonify({"detail": message}), status


def relay_response(resp: requests.Response) -> tuple[Any, int]:
    try:
        payload = resp.json()
    except ValueError:
        payload = {"detail": resp.text}
    return jsonify(payload), resp.status_code


def sanitize_crawl_payload(raw: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in raw.items():
        if key in CRAWL_ALLOWED_FIELDS:
            payload[key] = value

    tags = payload.get("tags")
    if not isinstance(tags, list):
        payload["tags"] = []
    else:
        payload["tags"] = [str(x).strip() for x in tags if str(x).strip()]

    exclude_tags = payload.get("exclude_tags")
    if exclude_tags is None:
        payload["exclude_tags"] = []
    elif not isinstance(exclude_tags, list):
        payload["exclude_tags"] = []
    else:
        payload["exclude_tags"] = [str(x).strip() for x in exclude_tags if str(x).strip()]

    for list_key in ("include_app_types", "exclude_app_types"):
        value = payload.get(list_key)
        if value is None:
            continue
        if not isinstance(value, list):
            payload[list_key] = []
            continue
        payload[list_key] = [str(x).strip().casefold() for x in value if str(x).strip()]

    return payload


def sanitize_estimate_payload(raw: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in raw.items():
        if key in CRAWL_ESTIMATE_ALLOWED_FIELDS:
            payload[key] = value

    tags = payload.get("tags")
    if not isinstance(tags, list):
        payload["tags"] = []
    else:
        payload["tags"] = [str(x).strip() for x in tags if str(x).strip()]

    exclude_tags = payload.get("exclude_tags")
    if exclude_tags is None:
        payload["exclude_tags"] = []
    elif not isinstance(exclude_tags, list):
        payload["exclude_tags"] = []
    else:
        payload["exclude_tags"] = [str(x).strip() for x in exclude_tags if str(x).strip()]

    for list_key in ("include_app_types", "exclude_app_types"):
        value = payload.get(list_key)
        if value is None:
            continue
        if not isinstance(value, list):
            payload[list_key] = []
            continue
        payload[list_key] = [str(x).strip().casefold() for x in value if str(x).strip()]

    return payload


def sanitize_filter_list(raw_filters: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_filters, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw_filters:
        if not isinstance(item, dict):
            continue
        normalized = {k: v for k, v in item.items() if k in EXPLORE_FILTER_ALLOWED_FIELDS}
        field = str(normalized.get("field", "")).strip()
        if not field:
            continue
        normalized["field"] = field
        if "op" in normalized:
            normalized["op"] = str(normalized["op"]).strip()
        out.append(normalized)
    return out


def sanitize_explore_payload(raw: dict[str, Any], reviews_mode: bool = False) -> dict[str, Any]:
    allowed = EXPLORE_REVIEWS_ALLOWED_FIELDS if reviews_mode else EXPLORE_QUERY_ALLOWED_FIELDS
    payload: dict[str, Any] = {}
    for key, value in raw.items():
        if key in allowed:
            payload[key] = value

    for key in ("include_keywords", "exclude_keywords"):
        values = payload.get(key)
        if not isinstance(values, list):
            payload[key] = []
            continue
        payload[key] = [str(x).strip() for x in values if str(x).strip()]

    payload["review_filters"] = sanitize_filter_list(payload.get("review_filters"))
    if not reviews_mode:
        payload["game_filters"] = sanitize_filter_list(payload.get("game_filters"))

    return payload


@app.get("/")
def index() -> str:
    return render_template("index.html", api_base_url=API_BASE_URL)


@app.get("/api/bootstrap")
def api_bootstrap() -> tuple[Any, int]:
    try:
        health_resp = requests.get(backend_url("/health"), timeout=HTTP_TIMEOUT)
        tags_resp = requests.get(backend_url("/tags"), timeout=HTTP_TIMEOUT)
        jobs_resp = requests.get(backend_url("/jobs"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot connect to backend API at {API_BASE_URL}: {exc}", status=502)

    if not health_resp.ok:
        return relay_response(health_resp)
    if not tags_resp.ok:
        return relay_response(tags_resp)
    if not jobs_resp.ok:
        return relay_response(jobs_resp)

    health_payload = health_resp.json()
    tags_payload = tags_resp.json()
    jobs_payload = jobs_resp.json()

    return (
        jsonify(
            {
                "api_base_url": API_BASE_URL,
                "health": health_payload,
                "tags": tags_payload.get("tags", []),
                "jobs": jobs_payload.get("jobs", []),
                "app_type_options": APP_TYPE_OPTIONS,
                "review_sources": [
                    {"value": "appreviews", "label": "Steam Store AppReviews (recommended)"},
                    {"value": "community", "label": "Steam Community HomeContent"},
                ],
                "defaults": DEFAULT_CRAWL_CONFIG,
            }
        ),
        200,
    )


@app.post("/api/crawl/start")
def api_start_crawl() -> tuple[Any, int]:
    raw_payload = request.get_json(silent=True)
    if not isinstance(raw_payload, dict):
        return json_error("Body must be a JSON object", status=400)

    payload = sanitize_crawl_payload(raw_payload)
    tags = payload.get("tags", [])
    if not tags:
        return json_error("At least one tag is required", status=400)

    try:
        resp = requests.post(backend_url("/jobs/crawl"), json=payload, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)

    return relay_response(resp)


@app.post("/api/crawl/estimate")
def api_estimate_crawl() -> tuple[Any, int]:
    raw_payload = request.get_json(silent=True)
    if not isinstance(raw_payload, dict):
        return json_error("Body must be a JSON object", status=400)

    payload = sanitize_estimate_payload(raw_payload)
    tags = payload.get("tags", [])
    if not tags:
        return json_error("At least one tag is required", status=400)

    try:
        resp = requests.post(backend_url("/crawl/estimate"), json=payload, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)

    return relay_response(resp)


@app.get("/api/jobs")
def api_list_jobs() -> tuple[Any, int]:
    try:
        resp = requests.get(backend_url("/jobs"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.get("/api/jobs/<job_id>")
def api_job_status(job_id: str) -> tuple[Any, int]:
    try:
        resp = requests.get(backend_url(f"/jobs/{job_id}"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.get("/api/jobs/<job_id>/result")
def api_job_result(job_id: str) -> tuple[Any, int]:
    try:
        resp = requests.get(backend_url(f"/jobs/{job_id}/result"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.post("/api/jobs/<job_id>/cancel")
def api_cancel_job(job_id: str) -> tuple[Any, int]:
    try:
        resp = requests.post(backend_url(f"/jobs/{job_id}/cancel"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.get("/api/datasets")
def api_datasets() -> tuple[Any, int]:
    try:
        resp = requests.get(backend_url("/datasets"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.get("/api/datasets/<path:dataset_dir>/explore/schema")
def api_explore_schema(dataset_dir: str) -> tuple[Any, int]:
    encoded = quote(dataset_dir, safe="")
    try:
        resp = requests.get(backend_url(f"/datasets/{encoded}/explore/schema"), timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.post("/api/datasets/<path:dataset_dir>/explore/query")
def api_explore_query(dataset_dir: str) -> tuple[Any, int]:
    raw_payload = request.get_json(silent=True)
    if raw_payload is None:
        raw_payload = {}
    if not isinstance(raw_payload, dict):
        return json_error("Body must be a JSON object", status=400)
    payload = sanitize_explore_payload(raw_payload, reviews_mode=False)
    encoded = quote(dataset_dir, safe="")
    try:
        resp = requests.post(backend_url(f"/datasets/{encoded}/explore/query"), json=payload, timeout=HTTP_TIMEOUT)
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


@app.post("/api/datasets/<path:dataset_dir>/explore/games/<int:appid>/reviews")
def api_explore_game_reviews(dataset_dir: str, appid: int) -> tuple[Any, int]:
    raw_payload = request.get_json(silent=True)
    if raw_payload is None:
        raw_payload = {}
    if not isinstance(raw_payload, dict):
        return json_error("Body must be a JSON object", status=400)
    payload = sanitize_explore_payload(raw_payload, reviews_mode=True)
    encoded = quote(dataset_dir, safe="")
    try:
        resp = requests.post(
            backend_url(f"/datasets/{encoded}/explore/games/{appid}/reviews"),
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as exc:
        return json_error(f"Cannot reach backend API: {exc}", status=502)
    return relay_response(resp)


if __name__ == "__main__":
    host = os.getenv("STEAM_UI_HOST", "127.0.0.1")
    port = int(os.getenv("STEAM_UI_PORT", "5050"))
    debug = os.getenv("STEAM_UI_DEBUG", "1") not in {"0", "false", "False"}
    app.run(host=host, port=port, debug=debug)
