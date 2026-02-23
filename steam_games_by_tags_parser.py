import argparse
import csv
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SteamGamesByTagsParser/1.0"
SEARCH_URL = "https://store.steampowered.com/search/results/"
TAGS_URL = "https://store.steampowered.com/tagdata/populartags/english"
APP_HOVER_URL = "https://store.steampowered.com/apphoverpublic/{appid}/"


def http_get(url: str, timeout: int) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_english_tags(timeout: int):
    raw = http_get(TAGS_URL, timeout)
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("Unexpected tags response format")

    tags = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        tagid = item.get("tagid")
        name = item.get("name")
        if isinstance(tagid, int) and isinstance(name, str) and name.strip():
            tags.append({"tagid": tagid, "name": name.strip()})
    return tags


def resolve_input_tags(input_tags, all_tags):
    by_lower_name = {t["name"].casefold(): t for t in all_tags}
    by_id = {t["tagid"]: t for t in all_tags}

    resolved = []
    unknown = []

    for raw in input_tags:
        token = raw.strip()
        if not token:
            continue

        if token.isdigit():
            tagid = int(token)
            tag = by_id.get(tagid)
            if tag:
                resolved.append(tag)
            else:
                unknown.append(token)
            continue

        tag = by_lower_name.get(token.casefold())
        if tag:
            resolved.append(tag)
        else:
            unknown.append(token)

    uniq = {t["tagid"]: t for t in resolved}
    return list(uniq.values()), unknown


def search_page(tag_ids, start, count, timeout):
    query = {
        "query": "",
        "start": start,
        "count": count,
        "dynamic_data": "",
        "sort_by": "_ASC",
        "supportedlang": "english",
        "tags": ",".join(str(t) for t in tag_ids),
        "snr": "1_7_7_230_7",
        "infinite": 1,
    }
    url = f"{SEARCH_URL}?{urlencode(query)}"
    raw = http_get(url, timeout)
    payload = json.loads(raw)
    if not isinstance(payload, dict) or int(payload.get("success", 0)) != 1:
        raise ValueError("Steam search returned unexpected response")
    return payload


def parse_search_results(results_html):
    anchors = re.findall(r"<a\s+[^>]*class=\"search_result_row[^>]*>.*?</a>", results_html, flags=re.S)

    games = []
    for anchor in anchors:
        appid_match = re.search(r'data-ds-appid=\"(\d+)\"', anchor)
        title_match = re.search(r'<span class=\"title\">(.*?)</span>', anchor, flags=re.S)
        tagids_match = re.search(r'data-ds-tagids=\"(\[[^\"]*\])\"', anchor)

        if not appid_match or not title_match:
            continue

        appid = int(appid_match.group(1))
        name = html.unescape(re.sub(r"\s+", " ", title_match.group(1))).strip()

        search_tag_ids = []
        if tagids_match:
            try:
                search_tag_ids = json.loads(tagids_match.group(1))
            except json.JSONDecodeError:
                search_tag_ids = []

        games.append(
            {
                "appid": appid,
                "name": name,
                "search_tag_ids": search_tag_ids,
            }
        )

    return games


def fetch_hover_tags(appid, timeout):
    url = APP_HOVER_URL.format(appid=appid)
    html_text = http_get(url, timeout)
    return re.findall(r'<div class="app_tag">\s*(.*?)\s*</div>', html_text)


def write_json(records, output_path: Path):
    output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(records, output_path: Path):
    fieldnames = [
        "appid",
        "name",
        "input_tag_ids",
        "input_tags",
        "search_tag_ids",
        "search_tags",
        "hover_tags",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow(
                {
                    "appid": row["appid"],
                    "name": row["name"],
                    "input_tag_ids": ",".join(str(x) for x in row["input_tag_ids"]),
                    "input_tags": "|".join(row["input_tags"]),
                    "search_tag_ids": ",".join(str(x) for x in row["search_tag_ids"]),
                    "search_tags": "|".join(row["search_tags"]),
                    "hover_tags": "|".join(row["hover_tags"]),
                }
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Find Steam games by input tags (English), then output appid, name and tags for each game."
        )
    )
    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="Input tags as names or IDs. Example: --tags Action Souls-like 19",
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=300,
        help="Maximum number of games to collect. Default: 300",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=50,
        help="Games requested per Steam search page. Default: 50",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format. Default: json",
    )
    parser.add_argument(
        "--output",
        default="steam_games_by_tags.json",
        help="Output file path. Default: steam_games_by_tags.json",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds. Default: 30",
    )
    parser.add_argument(
        "--hover-workers",
        type=int,
        default=8,
        help="Concurrent workers for fetching per-game hover tags. Default: 8",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        all_tags = fetch_english_tags(args.timeout)
    except (URLError, HTTPError, json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"Failed to load Steam english tags: {exc}") from exc

    resolved_tags, unknown_tags = resolve_input_tags(args.tags, all_tags)
    if unknown_tags:
        known_names = sorted(t["name"] for t in all_tags)
        sample = ", ".join(known_names[:30])
        raise SystemExit(
            "Unknown tag(s): "
            + ", ".join(unknown_tags)
            + "\nTip: pass exact English tag names or numeric tag IDs."
            + f"\nSample known tags: {sample}"
        )
    if not resolved_tags:
        raise SystemExit("No valid tags were resolved from --tags")

    tag_ids = [t["tagid"] for t in resolved_tags]
    tag_name_by_id = {t["tagid"]: t["name"] for t in all_tags}

    all_games = {}
    start = 0

    while len(all_games) < args.max_games:
        try:
            page = search_page(tag_ids, start, args.page_size, args.timeout)
        except (URLError, HTTPError, json.JSONDecodeError, ValueError) as exc:
            raise SystemExit(f"Steam search failed at offset {start}: {exc}") from exc

        page_games = parse_search_results(page.get("results_html", ""))
        if not page_games:
            break

        for game in page_games:
            all_games.setdefault(game["appid"], game)
            if len(all_games) >= args.max_games:
                break

        start += args.page_size
        if start >= int(page.get("total_count", 0)):
            break

    records = list(all_games.values())[: args.max_games]

    def enrich(game):
        hover_tags = []
        try:
            hover_tags = fetch_hover_tags(game["appid"], args.timeout)
            hover_tags = [html.unescape(t.strip()) for t in hover_tags if t.strip()]
        except Exception:
            hover_tags = []

        search_tag_names = [tag_name_by_id[t] for t in game["search_tag_ids"] if t in tag_name_by_id]

        return {
            "appid": game["appid"],
            "name": game["name"],
            "input_tag_ids": tag_ids,
            "input_tags": [tag_name_by_id[t] for t in tag_ids if t in tag_name_by_id],
            "search_tag_ids": game["search_tag_ids"],
            "search_tags": search_tag_names,
            "hover_tags": hover_tags,
        }

    enriched = []
    with ThreadPoolExecutor(max_workers=max(1, args.hover_workers)) as executor:
        futures = [executor.submit(enrich, game) for game in records]
        for future in as_completed(futures):
            enriched.append(future.result())

    enriched.sort(key=lambda x: x["appid"])

    output = Path(args.output)
    if args.format == "json":
        write_json(enriched, output)
    else:
        write_csv(enriched, output)

    print(
        "Resolved input tags: "
        + ", ".join(f"{t['name']} ({t['tagid']})" for t in resolved_tags)
    )
    print(f"Saved {len(enriched)} games to: {output}")


if __name__ == "__main__":
    main()
