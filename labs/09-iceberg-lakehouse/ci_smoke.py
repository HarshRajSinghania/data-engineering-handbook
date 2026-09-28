"""Check the Lab 09 pipeline and reference solutions end to end. CI runs this; you can too.

    python ci_smoke.py

It builds its own copy of the dataset in a temporary folder and deletes `lakehouse/` first, so it never
reads ../data/output and replaces any tables you built. It takes a few minutes.
"""
from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

LAB = Path(__file__).resolve().parent
DATA_TOOLS = LAB.parent / "data"
TMP = Path(tempfile.mkdtemp(prefix="lab09-"))
DATA = TMP / "data"
os.environ["LAB_DATA_DIR"] = str(DATA)                      # before lake.py is imported
sys.path.insert(0, str(LAB))

failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message, flush=True)
    if not ok:
        failures.append(message)


def script(*args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run([sys.executable, *args], cwd=LAB, capture_output=True, text=True, timeout=1200)
    if result.returncode:
        print(result.stdout[-3000:], result.stderr[-3000:])
    return result


def expected_daily_revenue() -> dict[str, tuple[int, Decimal]]:
    """Daily orders and USD revenue computed from the CSV files with the standard library only."""
    rows = lambda name: list(csv.DictReader(open(DATA / f"{name}.csv", encoding="utf-8")))  # noqa: E731
    latest: dict[str, dict] = {}
    for o in rows("orders"):
        if o["order_id"] not in latest or o["updated_at"] > latest[o["order_id"]]["updated_at"]:
            latest[o["order_id"]] = o
    products = {p["product_id"] for p in rows("products")}
    rate = {"USD": Decimal("1.00"), "EUR": Decimal("1.09"), "GBP": Decimal("1.27")}
    revenue: dict[str, Decimal] = defaultdict(Decimal)
    orders: dict[str, set] = defaultdict(set)
    for line in rows("order_items"):
        order = latest.get(line["order_id"])
        if not order or order["status"].lower() == "cancelled":
            continue
        if int(line["quantity"]) <= 0 or line["product_id"] not in products:
            continue
        day = datetime.fromisoformat(order["order_ts"].replace("Z", "+00:00")).date().isoformat()
        revenue[day] += int(line["quantity"]) * Decimal(line["unit_price"]) * rate[order["currency"]]
        orders[day].add(order["order_id"])
    return {d: (len(orders[d]), revenue[d]) for d in revenue}


def gold_matches_python(spark) -> bool:
    expected = expected_daily_revenue()
    actual = {str(r.order_date): (r.orders, Decimal(str(r.revenue_usd))) for r in spark.table("lab.gold.daily_revenue").collect()}
    return actual.keys() == expected.keys() and all(
        actual[d][0] == expected[d][0] and abs(actual[d][1] - expected[d][1]) < Decimal("0.05") for d in expected)


def row_counts(spark) -> dict[str, int]:
    names = ["silver.orders", "silver.order_items", "silver.order_items_quarantine", "silver.events", "gold.daily_revenue"]
    return {n: spark.table(f"lab.{n}").count() for n in names}


def snapshots(spark, name: str) -> int:
    return spark.sql(f"SELECT count(*) FROM lab.{name}.snapshots").first()[0]


def main() -> int:
    shutil.rmtree(LAB / "lakehouse", ignore_errors=True)
    script(str(DATA_TOOLS / "generate.py"), "--out", str(DATA))

    print("== The pipeline builds the tables, and gold matches an independent calculation ==")
    check(script("pipeline.py").returncode == 0, "pipeline.py runs")
    from lake import get_spark                                # imported late: LAB_DATA_DIR is set now
    spark = get_spark("ci")
    check(row_counts(spark) == {"silver.orders": 6000, "silver.order_items": 14769, "silver.order_items_quarantine": 146,
                                "silver.events": 30000, "gold.daily_revenue": 30}, "the row counts match Lab 03")
    check(gold_matches_python(spark), "daily revenue equals the standard-library calculation")
    silver_snaps = {t: snapshots(spark, f"silver.{t}") for t in ("orders", "order_items", "events")}

    print("\n== Running it again without changes commits nothing to silver ==")
    check(script("pipeline.py").returncode == 0, "pipeline.py runs again")
    check({t: snapshots(spark, f"silver.{t}") for t in silver_snaps} == silver_snaps, "no new silver snapshots")

    print("\n== A day of changes ==")
    script(str(DATA_TOOLS / "simulate_changes.py"), "--out", str(DATA))
    check(script("pipeline.py").returncode == 0, "pipeline.py processes the changes")
    check(row_counts(spark) == {"silver.orders": 6200, "silver.order_items": 15266, "silver.order_items_quarantine": 154,
                                "silver.events": 31000, "gold.daily_revenue": 31}, "the row counts include the new day")
    check(gold_matches_python(spark), "daily revenue still equals the standard-library calculation")

    print("\n== The reference solutions ==")
    import solutions
    r = {fn.__name__: fn(spark) for fn in solutions.EXERCISES}
    e1, e2, e3, e4, e5, e6, e7 = r.values()
    check(e1["metadata_json"] == 3 and e1["current_files"] == 1 and e1["data_files"] == 2,
          "1: three metadata versions, one current data file, and one replaced file still on disk")
    check((e2["inserted"], e2["updated"]) == (200, 1), "2: the MERGE inserted 200 orders and updated 1")
    check((e2["merge_records_removed"], e2["merge_records_added"]) == (6000, 6200), "2: the MERGE rewrote the whole file")
    check(e2["cow"] == {"written": 6200, "removed": 6200, "delete_files": 0}, "2: copy-on-write rewrites 6,200 rows for a 2-row UPDATE")
    check(e2["mor"] == {"written": 2, "removed": 0, "delete_files": 1}, "2: merge-on-read writes 2 rows and one delete file")
    check(e3["three_ways_agree"] and e3["changed_days"] == ["2024-03-29", "2024-03-31"],
          "3: three ways of reading the first snapshot agree; a late cancellation and a new day changed")
    check(e4["files_unchanged"] and e4["narrowing_rejected"] and e4["renamed_data_still_there"],
          "4: schema changes leave the data files alone; narrowing is rejected")
    check(e4["old_columns"][-2:] == ["currency", "updated_at"] and e4["current_columns"][-2:] == ["currency_code", "channel"],
          "4: an old snapshot keeps its old schema")
    check(e5["old_files_untouched"] and e5["spec1_files"] > 0 and e5["day_count_before"] == e5["day_count_after"],
          "5: old files keep the day spec, new files use the hour spec, and the query is unchanged")
    check((e6["small_files"], e6["compacted_files"]) == (108, 1) and e6["old_snapshot_readable_before_expiry"],
          "6: 108 small files compact into 1, and the old snapshot is still readable")
    check(e6["on_disk_after_compaction"] == 109 and e6["on_disk_after_expiry"] == 1 and e6["deleted_by_expiry"] == 108
          and e6["expired_snapshot_gone"] and e6["rows_after"] == e6["rows"],
          "6: expiring snapshots deletes the replaced files and the old snapshot")
    check((e7["main_before"], e7["audit_rows"], e7["main_after"]) == (999, 1008, 1008), "7: the branch is published by fast-forwarding main")

    print("\n== The exercise file runs as shipped ==")
    check(script("exercises.py").returncode == 0, "exercises.py runs before any TODO is filled in")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        code = main()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    print()
    print(f"{len(failures)} check(s) failed" if failures else "All checks passed")
    sys.exit(code)
