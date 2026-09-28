"""Save the repository's traffic and star history to CSV files, so it is not lost.

GitHub keeps only the last 14 days of traffic, so this runs weekly (see .github/workflows/metrics.yml) and
merges each snapshot into the files in --out, which the workflow keeps on the `metrics` branch.

    python tools/archive_traffic.py --out ../metrics           # uses `gh`, and GH_TOKEN or your login

Files written to --out/traffic:
    views.csv, clones.csv      date, count, uniques           one row per day; a later snapshot replaces a day
    referrers.csv, paths.csv   collected, name, count, uniques   the top 10 of the 14 days ending on `collected`
    repo.csv                   date, stars, forks, watchers, open_issues   one row per day it ran

The traffic endpoints need push access to the repository. With a token that lacks it they return 403: this
script then warns, still records the repo counts, and exits 0.
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def gh_json(path: str, repo: str) -> dict | list | None:
    """GET a repository API path with the GitHub CLI. Returns None (with a warning) if it fails."""
    endpoint = f"repos/{repo}" + (f"/{path}" if path else "")
    result = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True)
    if result.returncode:
        first_line = (result.stderr or result.stdout).strip().splitlines()[:1]
        print(f"::warning::{path}: {first_line[0] if first_line else 'request failed'}")
        return None
    return json.loads(result.stdout)


def read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_rows(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def merge_daily(existing: list[dict], snapshot: list[dict], count_key: str) -> list[dict]:
    """Merge a 14-day snapshot into the stored days. The newer snapshot wins for a day both contain."""
    by_day = {row["date"]: row for row in existing}
    for entry in snapshot:
        day = entry["timestamp"][:10]
        by_day[day] = {"date": day, "count": entry[count_key], "uniques": entry["uniques"]}
    return [by_day[d] for d in sorted(by_day)]


def append_snapshot(existing: list[dict], collected: str, entries: list[dict], name_key: str) -> list[dict]:
    """Add one weekly top-10 list. A second run on the same day replaces that day's rows."""
    kept = [row for row in existing if row["collected"] != collected]
    return kept + [{"collected": collected, "name": e[name_key], "count": e["count"], "uniques": e["uniques"]} for e in entries]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, type=Path, help="folder to write to (the checked-out metrics branch)")
    parser.add_argument("--repo", help="owner/name; default: the repository of the current directory")
    args = parser.parse_args()
    repo = args.repo or subprocess.run(["gh", "repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"],
                                       capture_output=True, text=True, check=True).stdout.strip()
    today = datetime.now(timezone.utc).date().isoformat()
    folder = args.out / "traffic"

    info = gh_json("", repo)
    if info:
        rows = [r for r in read_rows(folder / "repo.csv") if r["date"] != today]
        rows.append({"date": today, "stars": info["stargazers_count"], "forks": info["forks_count"],
                     "watchers": info["subscribers_count"], "open_issues": info["open_issues_count"]})
        write_rows(folder / "repo.csv", ["date", "stars", "forks", "watchers", "open_issues"], rows)

    for name, key in (("views", "views"), ("clones", "clones")):
        data = gh_json(f"traffic/{name}", repo)
        if data:
            merged = merge_daily(read_rows(folder / f"{name}.csv"), data[key], "count")
            write_rows(folder / f"{name}.csv", ["date", "count", "uniques"], merged)
            print(f"{name}: {len(merged)} days stored")

    for name, path, key in (("referrers", "traffic/popular/referrers", "referrer"), ("paths", "traffic/popular/paths", "path")):
        data = gh_json(path, repo)
        if data is not None:
            merged = append_snapshot(read_rows(folder / f"{name}.csv"), today, data, key)
            write_rows(folder / f"{name}.csv", ["collected", "name", "count", "uniques"], merged)

    readme = args.out / "README.md"
    if not readme.exists():
        readme.write_text(
            "# Metrics\n\nRepository traffic and star history, saved weekly by `tools/archive_traffic.py` because GitHub "
            "keeps only 14 days. Written by a workflow; do not edit by hand.\n\n"
            "| File | Columns |\n|------|---------|\n"
            "| `traffic/views.csv`, `traffic/clones.csv` | `date`, `count`, `uniques` (one row per day) |\n"
            "| `traffic/referrers.csv`, `traffic/paths.csv` | `collected`, `name`, `count`, `uniques` (top 10 of the 14 days before `collected`) |\n"
            "| `traffic/repo.csv` | `date`, `stars`, `forks`, `watchers`, `open_issues` |\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
