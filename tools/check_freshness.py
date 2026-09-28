"""Validate the freshness front matter of every guide and report the ones that are overdue.

Usage:
    python tools/check_freshness.py                      # validate only (used on pull requests)
    python tools/check_freshness.py --max-age-days 180   # also fail on guides not reviewed recently
    python tools/check_freshness.py --max-age-days 180 --report stale.md   # write a Markdown list

Validation errors (missing or malformed fields, a lab_tested version that no longer
matches the lab that runs it) always fail. Overdue guides fail only when
--max-age-days is given, so a date passing never breaks an unrelated pull request.
"""
import argparse
import datetime
import sys
from pathlib import Path

import yaml

GUIDE_GLOBS = ("docs/0*/*.md", "docs/99-reference/*.md")


def front_matter(path: Path) -> dict:
    """The YAML block between the opening and closing `---` lines, or {} if there is none."""
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 3)
    if end == -1:
        return {}
    data = yaml.safe_load(text[4:end + 1])
    return data if isinstance(data, dict) else {}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-age-days", type=int, help="fail on guides reviewed longer ago than this")
    parser.add_argument("--report", help="write the overdue guides to this Markdown file")
    args = parser.parse_args()

    today = datetime.date.today()
    errors: list[str] = []
    overdue: list[tuple[int, Path, datetime.date]] = []
    checked = 0

    for pattern in GUIDE_GLOBS:
        for path in sorted(Path(".").glob(pattern)):
            checked += 1
            try:
                meta = front_matter(path)
            except yaml.YAMLError as exc:
                errors.append(f"{path}: front matter is not valid YAML ({str(exc).splitlines()[0]})")
                continue
            verified = meta.get("verified")
            if not isinstance(verified, datetime.date):
                errors.append(f"{path}: front matter needs `verified: YYYY-MM-DD`")
                continue
            if verified > today:
                errors.append(f"{path}: verified date {verified} is in the future")

            tested, source = meta.get("lab_tested"), meta.get("lab_source")
            if bool(tested) != bool(source):
                errors.append(f"{path}: `lab_tested` and `lab_source` go together")
            elif tested:
                src = Path(str(source))
                version = str(tested).split()[-1]
                if not src.is_file():
                    errors.append(f"{path}: lab_source {source} does not exist")
                elif version not in src.read_text(encoding="utf-8"):
                    errors.append(f"{path}: lab_tested says {tested}, but {source} does not contain {version}")

            age = (today - verified).days
            if args.max_age_days is not None and age > args.max_age_days:
                overdue.append((age, path, verified))

    for message in errors:
        print(f"ERROR {message}")
    overdue.sort(reverse=True)
    for age, path, verified in overdue:
        print(f"OVERDUE {path}: last reviewed {verified} ({age} days ago)")
    print(f"checked {checked} guides: {len(errors)} error(s), {len(overdue)} overdue")

    if args.report:
        if overdue:
            rows = "\n".join(f"| `{p.as_posix()}` | {v} | {a} |" for a, p, v in overdue)
            Path(args.report).write_text(
                f"These guides were last reviewed more than {args.max_age_days} days ago. "
                "Re-check commands, versions and links against the vendor documentation, then "
                "update `verified:` in the guide's front matter.\n\n"
                f"| Guide | Last reviewed | Days ago |\n|-------|---------------|---------:|\n{rows}\n",
                encoding="utf-8",
            )
        else:
            Path(args.report).unlink(missing_ok=True)

    return 1 if errors or overdue else 0


if __name__ == "__main__":
    sys.exit(main())
