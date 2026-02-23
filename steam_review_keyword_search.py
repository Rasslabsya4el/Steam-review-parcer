import argparse
import csv
import json
from pathlib import Path

from steam_project_paths import resolve_dataset_path


def load_games_map(games_file: Path):
    games = {}
    with games_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            appid = row.get("appid")
            if isinstance(appid, int):
                games[appid] = row
    return games


def iter_review_files(reviews_dir: Path):
    yield from sorted(reviews_dir.glob("*.jsonl"), key=lambda p: int(p.stem) if p.stem.isdigit() else p.stem)


def search_keywords_in_dataset(dataset_dir: Path, keywords, sample_per_game=2):
    games_file = dataset_dir / "games.jsonl"
    reviews_dir = dataset_dir / "reviews"

    if not games_file.exists():
        raise FileNotFoundError(f"Missing {games_file}")
    if not reviews_dir.exists():
        raise FileNotFoundError(f"Missing {reviews_dir}")

    games_map = load_games_map(games_file)
    kw_norm = [k.casefold() for k in keywords if k.strip()]

    stats = {}

    for review_file in iter_review_files(reviews_dir):
        if not review_file.stem.isdigit():
            continue
        appid = int(review_file.stem)

        total_reviews = 0
        matched_reviews = 0
        matched_keywords = set()
        samples = []

        with review_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_reviews += 1
                row = json.loads(line)
                text = str(row.get("review_text", ""))
                if not text:
                    continue

                text_norm = text.casefold()
                hit = [k for k in kw_norm if k in text_norm]
                if not hit:
                    continue

                matched_reviews += 1
                matched_keywords.update(hit)
                if len(samples) < sample_per_game:
                    snippet = " ".join(text.split())
                    samples.append(snippet[:220])

        if matched_reviews == 0:
            continue

        game = games_map.get(appid, {})
        name = game.get("name", f"appid_{appid}")
        input_tags = game.get("input_tags", [])

        stats[appid] = {
            "appid": appid,
            "name": name,
            "input_tags": input_tags,
            "total_reviews": total_reviews,
            "matched_reviews": matched_reviews,
            "match_rate": round((matched_reviews / total_reviews) if total_reviews else 0.0, 6),
            "matched_keywords": sorted(matched_keywords),
            "sample_reviews": samples,
        }

    return sorted(stats.values(), key=lambda x: x["matched_reviews"], reverse=True)


def write_json(rows, output: Path):
    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(rows, output: Path):
    fieldnames = [
        "appid",
        "name",
        "input_tags",
        "total_reviews",
        "matched_reviews",
        "match_rate",
        "matched_keywords",
        "sample_reviews",
    ]
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "appid": row["appid"],
                    "name": row["name"],
                    "input_tags": "|".join(row.get("input_tags", [])),
                    "total_reviews": row["total_reviews"],
                    "matched_reviews": row["matched_reviews"],
                    "match_rate": row["match_rate"],
                    "matched_keywords": "|".join(row.get("matched_keywords", [])),
                    "sample_reviews": " || ".join(row.get("sample_reviews", [])),
                }
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Search keywords in downloaded Steam reviews and return matched games with simple stats."
    )
    parser.add_argument(
        "--dataset-dir",
        default="steam_dataset",
        help="Dataset folder name (resolved under datasets/ by default) or explicit relative path",
    )
    parser.add_argument("--keywords", nargs="+", required=True, help="Keywords to search in reviews")
    parser.add_argument("--sample-per-game", type=int, default=2, help="Number of sample matched snippets per game")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="Output format")
    parser.add_argument("--output", default="keyword_matches.json", help="Output file path")
    return parser.parse_args()


def main():
    args = parse_args()
    dataset_dir = resolve_dataset_path(args.dataset_dir)

    rows = search_keywords_in_dataset(
        dataset_dir=dataset_dir,
        keywords=args.keywords,
        sample_per_game=args.sample_per_game,
    )

    output = Path(args.output)
    if args.format == "json":
        write_json(rows, output)
    else:
        write_csv(rows, output)

    print(f"Matched games: {len(rows)}")
    print(f"Saved to: {output}")


if __name__ == "__main__":
    main()

