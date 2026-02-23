import argparse
import csv
import json
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def fetch_tags(timeout: int):
    """Fetch Steam tags from the Steam english tag endpoint."""
    url = "https://store.steampowered.com/tagdata/populartags/english"
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SteamTagFetcher/1.0"
        },
    )

    with urlopen(req, timeout=timeout) as response:
        payload = response.read().decode("utf-8")

    data = json.loads(payload)
    if not isinstance(data, list):
        raise ValueError("Unexpected response format: expected a list of tags")

    normalized = []
    for item in data:
        if not isinstance(item, dict):
            continue
        tagid = item.get("tagid")
        name = item.get("name")
        if isinstance(tagid, int) and isinstance(name, str) and name.strip():
            normalized.append({"tagid": tagid, "name": name.strip()})

    return sorted(normalized, key=lambda x: x["tagid"])


def write_json(tags, output_path: Path):
    output_path.write_text(json.dumps(tags, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(tags, output_path: Path):
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["tagid", "name"])
        writer.writeheader()
        writer.writerows(tags)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Steam English tag catalog (tagid + name) and save it to a file."
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Output format. Default: json",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path. Default: steam_tags_en.<format>",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds. Default: 30",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output = Path(args.output) if args.output else Path(f"steam_tags_en.{args.format}")

    try:
        tags = fetch_tags(args.timeout)
    except URLError as exc:
        raise SystemExit(f"Network error while requesting Steam tags: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Steam returned invalid JSON: {exc}") from exc
    except Exception as exc:
        raise SystemExit(f"Failed to fetch tags: {exc}") from exc

    if args.format == "json":
        write_json(tags, output)
    else:
        write_csv(tags, output)

    print(f"Saved {len(tags)} tags to: {output}")


if __name__ == "__main__":
    main()
