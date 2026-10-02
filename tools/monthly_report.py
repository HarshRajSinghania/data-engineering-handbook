"""Open the monthly maintenance issue, and keep a supply of guide-review issues for contributors.

    python tools/monthly_report.py --dry-run                 # print what it would do
    python tools/monthly_report.py --metrics-dir ../metrics  # create the issues (needs `gh` and issues: write)

Runs on the first of each month (.github/workflows/monthly.yml). It
  * creates "Monthly maintenance YYYY-MM" with a checklist and the month's numbers, unless it already exists, and
  * makes sure at least --target-reviews "Review the ... guide" issues are open, choosing the guides to review next.

Guides are chosen in this order: those never individually reviewed (`review_status: baseline`) first, then those not
covered by a lab (a lab already exercises the others), then the oldest review date. A guide individually reviewed in the last 30 days, or that already has an open review issue, is skipped.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_freshness import GUIDE_GLOBS, front_matter  # noqa: E402

SITE = "https://sarangambekar1997.github.io/data-engineering-handbook/"
REPO_URL = "https://github.com/sarangambekar1997/data-engineering-handbook/"
STALE_AFTER_DAYS = 180
RECENT_DAYS = 30


def load_guides(root: Path) -> list[dict]:
    guides = []
    for pattern in GUIDE_GLOBS:
        for path in sorted(root.glob(pattern)):
            meta = front_matter(path)
            verified = meta.get("verified")
            if not isinstance(verified, datetime.date):
                continue
            h1 = re.search(r"^# (.+)$", path.read_text(encoding="utf-8"), re.M)
            rel = path.relative_to(root)
            guides.append({"title": h1.group(1).strip() if h1 else path.stem, "path": rel.as_posix(), "verified": verified,
                           "lab_tested": bool(meta.get("lab_tested")), "baseline": meta.get("review_status") == "baseline",
                           "url": SITE + rel.with_suffix("").relative_to("docs").as_posix() + "/"})
    return guides


def review_title(guide: dict) -> str:
    return f"Review the {guide['title']} guide against current documentation"


def pick_guides(guides: list[dict], open_titles: set[str], today: datetime.date, count: int) -> list[dict]:
    """The next `count` guides to ask contributors to review."""
    eligible = [g for g in guides
                if review_title(g) not in open_titles
                and (g["baseline"] or (today - g["verified"]).days > RECENT_DAYS)]   # a baseline date is not a review
    eligible.sort(key=lambda g: (not g["baseline"], g["lab_tested"], g["verified"], g["path"]))   # unreviewed guides first
    return eligible[:count]


def review_body(guide: dict) -> str:
    if guide.get("baseline"):
        intro = (f"The review date on this guide, {guide['verified']:%-d %B %Y}, is a shared starting date and not the result of "
                 "checking this guide, so the page says *Not yet individually reviewed*. This issue is to check the guide against "
                 "current vendor documentation and make the date true.")
        last = ("4. Set `verified:` in the guide's front matter to the date you checked, **remove the line `review_status: baseline`**, "
                "and list what you checked in the pull request description.\n\n")
    else:
        intro = (f"The review date on this guide is {guide['verified']:%-d %B %Y}. This issue is to check the guide against "
                 "current vendor documentation and refresh the date.")
        last = "4. Set `verified:` in the guide's front matter to the date you checked, and list what you checked in the pull request description.\n\n"
    return (
        f"**Guide:** [{guide['title']}]({guide['url']}) (`{guide['path']}`)\n\n"
        f"{intro}\n\n"
        "**Task**\n\n"
        "1. Run the guide's commands and code samples against the current release, or confirm them against the vendor documentation.\n"
        "2. Check the versions, defaults and configuration keys named in the text, and open the *Further Reading* links.\n"
        "3. Fix what is wrong in a pull request.\n"
        + last +
        f"See [Reviewing a guide]({REPO_URL}blob/main/docs/maintenance.md#reviewing-a-guide) and "
        f"[Contributing]({REPO_URL}blob/main/CONTRIBUTING.md). Comment here to claim it. Expected effort: 30 to 60 minutes.\n")


def read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def traffic_summary(metrics: Path | None, today: datetime.date) -> list[str]:
    """Lines about the last 30 days from the archived CSV files, or a note that there are none."""
    if metrics is None or not (metrics / "traffic").exists():
        return ["No archived traffic yet. The `metrics` branch is created by the weekly *Metrics* workflow."]
    since = (today - datetime.timedelta(days=30)).isoformat()
    lines = []
    for name, label in (("views", "Page views of the repository"), ("clones", "Clones")):
        rows = [r for r in read_rows(metrics / "traffic" / f"{name}.csv") if r["date"] >= since]
        if rows:
            lines.append(f"{label}: {sum(int(r['count']) for r in rows):,} in {len(rows)} days recorded")
    repo = read_rows(metrics / "traffic" / "repo.csv")
    recent = [r for r in repo if r["date"] >= since]
    if recent:
        lines.append(f"Stars: {recent[-1]['stars']} (change over the period: {int(recent[-1]['stars']) - int(recent[0]['stars']):+d})")
    referrers = [r for r in read_rows(metrics / "traffic" / "referrers.csv") if r["collected"] >= since]
    if referrers:
        latest = max(r["collected"] for r in referrers)
        top = [f"{r['name']} ({r['count']})" for r in referrers if r["collected"] == latest][:5]
        lines.append("Top referrers, latest snapshot: " + ", ".join(top))
    return lines or ["The archive has no rows for the last 30 days."]


def monthly_body(month: str, stats: dict, traffic: list[str], picked: list[dict], contributors: list[str]) -> str:
    def issue_link(guide: dict) -> str:
        return f"- {guide['title']}: {guide['url']}"
    reviews = "\n".join(issue_link(g) for g in picked) or "- None needed: every guide has a review issue or was reviewed recently."
    thanks = ", ".join(f"@{name}" for name in contributors) or "no outside contributors merged this month"
    return f"""Routine for {month}. Tick each item when it is done, and close the issue when the list is complete.

## The numbers

- Guides: {stats['guides']}; individually reviewed: {stats['reviewed']}; overdue for review (over {STALE_AFTER_DAYS} days): {stats['overdue']}
- Open issues: {stats['issues']}; open pull requests: {stats['prs']} (Dependabot: {stats['dependabot']})
- Open guide-review issues: {stats['review_issues']}
{chr(10).join('- ' + line for line in traffic)}

## Checklist

- [ ] **Triage.** Label and answer every new issue and pull request. Aim to reply within 7 days.
- [ ] **Dependency updates.** Merge or close the Dependabot pull requests. A change to a lab's pinned version must be matched by the guide's `lab_tested` line: the freshness check fails until it is.
- [ ] **Workflows.** Open the [Docs]({REPO_URL}actions/workflows/docs.yml) and [Labs]({REPO_URL}actions/workflows/labs.yml) runs on `main` and confirm the last scheduled ones passed. A failing weekly Labs run usually means a dependency released a breaking change.
- [ ] **Overdue guides.** Read the *Guides overdue for review* issue, if it exists, and assign or review the oldest.
- [ ] **Review issues.** Check that the guide-review issues below have a claimant, and nudge or reassign stale claims.
- [ ] **Changelog and release.** Move `[Unreleased]` entries in `CHANGELOG.md` under a new version and publish a release: a minor version for new guides, labs or site features, a patch for corrections.
- [ ] **Read the numbers.** Which guides and referrers brought readers? Note anything that suggests a topic to write next.
- [ ] **Say thank you** to this month's contributors: {thanks}.

## Guides for contributors to review next

{reviews}
"""


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--metrics-dir", type=Path)
    parser.add_argument("--target-reviews", type=int, default=5)
    parser.add_argument("--today", type=datetime.date.fromisoformat, default=datetime.date.today())
    args = parser.parse_args()
    today, month = args.today, f"{args.today:%Y-%m}"

    guides = load_guides(Path(__file__).resolve().parent.parent)
    issues = json.loads(gh("issue", "list", "--state", "all", "--limit", "500", "--json", "number,title,state,labels"))
    open_reviews = [i for i in issues if i["state"] == "OPEN" and any(l["name"] == "verify-guide" for l in i["labels"])]
    open_titles = {i["title"] for i in open_reviews}
    prs = json.loads(gh("pr", "list", "--state", "open", "--json", "number,author"))
    first_of_month = today.replace(day=1)
    last_month = (first_of_month - datetime.timedelta(days=1)).replace(day=1).isoformat()
    merged = json.loads(gh("pr", "list", "--state", "merged", "--search", f"merged:>={last_month}", "--json", "author"))
    owner = gh("repo", "view", "--json", "owner", "-q", ".owner.login").strip()
    contributors = sorted({p["author"]["login"] for p in merged if not p["author"].get("is_bot") and p["author"]["login"] != owner})

    picked = pick_guides(guides, open_titles, today, max(0, args.target_reviews - len(open_reviews)))
    stats = {"guides": len(guides), "reviewed": sum(not g["baseline"] for g in guides), "overdue": sum((today - g["verified"]).days > STALE_AFTER_DAYS for g in guides),
             "issues": sum(i["state"] == "OPEN" for i in issues), "prs": len(prs),
             "dependabot": sum(p["author"]["login"].endswith("dependabot") or p["author"]["login"] == "app/dependabot" for p in prs),
             "review_issues": len(open_reviews)}
    title = f"Monthly maintenance {month}"
    body = monthly_body(month, stats, traffic_summary(args.metrics_dir, today), picked, contributors)

    for guide in picked:
        print(f"review issue: {review_title(guide)}")
        if not args.dry_run:
            gh("issue", "create", "--title", review_title(guide), "--body", review_body(guide),
               "--label", "verify-guide", "--label", "help wanted", "--label", "good first issue")
    if any(i["title"] == title for i in issues):
        print(f"{title!r} already exists")
    elif args.dry_run:
        print(f"\n{title}\n\n{body}")
    else:
        gh("issue", "create", "--title", title, "--body", body, "--label", "maintenance")
        print(f"created {title!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
