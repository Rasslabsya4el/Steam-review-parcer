import argparse
import json
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode

import steam_one_time_crawl as crawl_mod


def build_reviews_url(appid: int) -> str:
    params = crawl_mod.default_review_params(appid)
    params["userreviewsoffset"] = "0"
    params["p"] = "1"
    return f"https://steamcommunity.com/app/{appid}/homecontent/?{urlencode(params)}"


def classify_error(exc: Exception) -> str:
    text = str(exc)
    m = re.search(r"HTTP Error\s+(\d+)", text)
    if m:
        return m.group(1)
    return "network"


def one_request(appid: int, timeout: int) -> None:
    url = build_reviews_url(appid)
    _ = crawl_mod.http_get(url, timeout=timeout, retries=1)


def run_probe(rpm: int, appids: list[int], requests_per_rpm: int, workers: int, timeout: int) -> dict:
    crawl_mod.configure_http_runtime(
        max_rpm=rpm,
        http_retries=1,
        retry_base_sleep=0.1,
        retry_jitter=0.0,
    )

    started = time.time()
    ok = 0
    failed = 0
    errors_by_code: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [executor.submit(one_request, random.choice(appids), timeout) for _ in range(requests_per_rpm)]
        for future in as_completed(futures):
            try:
                future.result()
                ok += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                key = classify_error(exc)
                errors_by_code[key] = errors_by_code.get(key, 0) + 1

    crawl_mod.finish_request_stats()
    stats = crawl_mod.get_request_stats()

    elapsed = max(0.001, time.time() - started)
    return {
        "target_rpm": rpm,
        "requests_planned": requests_per_rpm,
        "requests_ok": ok,
        "requests_failed": failed,
        "failure_rate": round((failed / requests_per_rpm), 6) if requests_per_rpm else 0.0,
        "errors_by_code": errors_by_code,
        "elapsed_seconds": round(elapsed, 3),
        "client_rpm": round((requests_per_rpm / elapsed) * 60.0, 3),
        "http_stats": stats,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Probe Steam endpoint and estimate safe RPM")
    parser.add_argument(
        "--rpm-values",
        nargs="+",
        type=int,
        default=[60, 90, 120, 150, 180, 210],
        help="RPM values to test",
    )
    parser.add_argument(
        "--requests-per-rpm",
        type=int,
        default=60,
        help="How many requests to send for each RPM",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Thread workers used for the probe",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout per request",
    )
    parser.add_argument(
        "--appids",
        nargs="+",
        type=int,
        default=[730, 570, 230410, 1091500, 578080],
        help="App IDs used for endpoint probing",
    )
    parser.add_argument(
        "--output",
        default="rpm_probe_results.json",
        help="Where to save probe results",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    results = []

    for rpm in args.rpm_values:
        print(f"Testing RPM={rpm} ...")
        row = run_probe(
            rpm=rpm,
            appids=args.appids,
            requests_per_rpm=args.requests_per_rpm,
            workers=args.workers,
            timeout=args.timeout,
        )
        results.append(row)
        print(
            f"RPM={rpm} ok={row['requests_ok']} failed={row['requests_failed']} "
            f"failure_rate={row['failure_rate']} client_rpm={row['client_rpm']}"
        )

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    safe = [r for r in results if r["requests_failed"] == 0]
    best = max(safe, key=lambda x: x["target_rpm"]) if safe else None

    print("Saved:", args.output)
    if best:
        print("Highest tested zero-failure RPM:", best["target_rpm"])
    else:
        print("No zero-failure RPM in tested range")


if __name__ == "__main__":
    main()
