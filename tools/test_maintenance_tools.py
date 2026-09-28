"""Tests for the maintenance tools: python -m unittest tools/test_maintenance_tools.py"""
import datetime
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import archive_traffic as at  # noqa: E402
import monthly_report as mr  # noqa: E402

TODAY = datetime.date(2026, 10, 1)


def guide(title, verified, lab_tested=False, path=None):
    return {"title": title, "verified": datetime.date.fromisoformat(verified), "lab_tested": lab_tested,
            "path": path or f"docs/01-storage/{title.lower()}.md", "url": f"https://example.org/{title.lower()}/"}


class MergeTraffic(unittest.TestCase):
    def test_new_snapshot_replaces_a_day_and_keeps_older_days(self):
        stored = [{"date": "2026-09-01", "count": "5", "uniques": "3"}, {"date": "2026-09-02", "count": "1", "uniques": "1"}]
        snapshot = [{"timestamp": "2026-09-02T00:00:00Z", "count": 4, "uniques": 2},
                    {"timestamp": "2026-09-03T00:00:00Z", "count": 7, "uniques": 6}]
        merged = at.merge_daily(stored, snapshot, "count")
        self.assertEqual([r["date"] for r in merged], ["2026-09-01", "2026-09-02", "2026-09-03"])
        self.assertEqual(merged[1]["count"], 4)          # the later snapshot has the fuller count for that day
        self.assertEqual(merged[0]["count"], "5")        # a day that fell out of the 14-day window is kept

    def test_a_second_run_on_the_same_day_replaces_the_top_list(self):
        first = at.append_snapshot([], "2026-09-28", [{"referrer": "a.org", "count": 1, "uniques": 1}], "referrer")
        second = at.append_snapshot(first, "2026-09-28", [{"referrer": "b.org", "count": 2, "uniques": 2}], "referrer")
        self.assertEqual([r["name"] for r in second], ["b.org"])
        third = at.append_snapshot(second, "2026-10-05", [{"referrer": "c.org", "count": 3, "uniques": 3}], "referrer")
        self.assertEqual([r["name"] for r in third], ["b.org", "c.org"])


class PickGuides(unittest.TestCase):
    def test_order_skips_and_limit(self):
        guides = [
            guide("Tested", "2026-01-01", lab_tested=True),
            guide("Oldest", "2026-02-01"),
            guide("Newer", "2026-05-01"),
            guide("Recent", "2026-09-20"),                 # reviewed within 30 days
            guide("HasIssue", "2026-01-15"),
        ]
        open_titles = {mr.review_title(guides[4])}
        picked = mr.pick_guides(guides, open_titles, TODAY, 3)
        self.assertEqual([g["title"] for g in picked], ["Oldest", "Newer", "Tested"])
        self.assertEqual(len(mr.pick_guides(guides, open_titles, TODAY, 1)), 1)
        self.assertEqual(mr.pick_guides(guides, open_titles, TODAY, 0), [])

    def test_ties_are_broken_by_path(self):
        a, b = guide("B", "2026-03-01", path="docs/a.md"), guide("A", "2026-03-01", path="docs/b.md")
        self.assertEqual([g["path"] for g in mr.pick_guides([b, a], set(), TODAY, 2)], ["docs/a.md", "docs/b.md"])


class Reports(unittest.TestCase):
    def test_review_issue_names_the_guide_and_the_task(self):
        body = mr.review_body(guide("Iceberg", "2026-09-27"))
        self.assertIn("https://example.org/iceberg/", body)
        self.assertIn("27 September 2026", body)
        self.assertIn("Set `verified:`", body)

    def test_monthly_body_lists_numbers_contributors_and_guides(self):
        stats = {"guides": 62, "overdue": 1, "issues": 9, "prs": 3, "dependabot": 2, "review_issues": 4}
        body = mr.monthly_body("2026-10", stats, ["Clones: 5"], [guide("Iceberg", "2026-03-01")], ["harsh"])
        for expected in ("2026-10", "Guides: 62", "overdue for review (over 180 days): 1", "Dependabot: 2", "- Clones: 5",
                         "@harsh", "Iceberg: https://example.org/iceberg/"):
            self.assertIn(expected, body)

    def test_traffic_summary_without_and_with_an_archive(self):
        self.assertIn("No archived traffic yet", mr.traffic_summary(None, TODAY)[0])
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "traffic"
            at.write_rows(folder / "views.csv", ["date", "count", "uniques"],
                          [{"date": "2026-09-25", "count": 10, "uniques": 4}, {"date": "2026-08-01", "count": 99, "uniques": 9}])
            at.write_rows(folder / "repo.csv", ["date", "stars", "forks", "watchers", "open_issues"],
                          [{"date": "2026-09-05", "stars": 2, "forks": 1, "watchers": 0, "open_issues": 3},
                           {"date": "2026-09-28", "stars": 7, "forks": 1, "watchers": 0, "open_issues": 3}])
            lines = mr.traffic_summary(Path(tmp), TODAY)
            self.assertIn("Page views of the repository: 10 in 1 days recorded", lines)   # the August row is outside 30 days
            self.assertIn("Stars: 7 (change over the period: +5)", lines)


if __name__ == "__main__":
    unittest.main()
