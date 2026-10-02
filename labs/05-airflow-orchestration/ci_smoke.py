"""End-to-end check of Lab 05 against a running Airflow. CI runs this; you can too.

It follows the README's exercises and asserts what the README claims:

  1. One run loads a day and triggers weekly_report through the asset.
  2. A backfill loads a week, one run per day.
  3. Clearing a day duplicates its rows with the starting DAG, because load is not
     idempotent. With the reference load, clearing a day again leaves it unchanged.
  4. With the reference quality gate, a day with too few orders fails at check_quality,
     publish_daily_revenue is upstream_failed, and the bad day is never published.

The script swaps in the reference DAG during step 3 and restores your file afterwards.
Airflow must be a fresh stack (no earlier runs). By default the script talks to the
Compose service; AIRFLOW_EXEC overrides the command prefix, for example to run against
Airflow installed on the host (set it to "env" and DAGS_DIR to the DAGs folder).

Usage:
    docker compose up -d --build --wait
    python ci_smoke.py
"""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

LAB = Path(__file__).resolve().parent
PREFIX = shlex.split(os.environ.get("AIRFLOW_EXEC", "docker compose exec -T airflow"))
DAGS_DIR = Path(os.environ.get("DAGS_DIR", LAB / "dags"))
FINAL_STATES = {"success", "failed"}
failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message, flush=True)
    if not ok:
        failures.append(message)


def sh(*args: str) -> str:
    result = subprocess.run([*PREFIX, *args], cwd=LAB, capture_output=True, text=True, timeout=300)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed:\n{result.stdout[-1500:]}\n{result.stderr[-1500:]}")
    return result.stdout


def af(*args: str) -> str:
    return sh("airflow", *args)


def af_json(*args: str) -> list[dict]:
    """Airflow prints log lines around its JSON output: parse from the first line that starts a list."""
    out = af(*args, "-o", "json")
    for i, line in enumerate(out.splitlines()):
        if line.startswith("["):
            return json.loads("\n".join(out.splitlines()[i:]))
    return []


def sql(query: str) -> list[list]:
    code = ("import duckdb, json, os, sys; p = os.environ.get('LAB_WAREHOUSE_DIR', '/opt/lab/warehouse') + '/shop.duckdb';"
            "print(json.dumps(duckdb.connect(p, read_only=True).execute(sys.argv[1]).fetchall(), default=str))")
    return json.loads(sh("python", "-c", code, query).strip().splitlines()[-1])


def runs(dag_id: str) -> list[dict]:
    return af_json("dags", "list-runs", dag_id)


def wait_for(description: str, condition, timeout: int = 300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(3)
    raise TimeoutError(f"timed out waiting for {description}")


def run_for(day: str) -> dict | None:
    return next((r for r in runs("shop_daily") if r["logical_date"].startswith(day)), None)


def wait_run(day: str, previous_end: str | None = None) -> dict:
    """Wait until the run for a logical date is finished (and has finished again, after a clear)."""
    def done():
        run = run_for(day)
        return run if run and run["state"] in FINAL_STATES and run["end_date"] != previous_end else None
    return wait_for(f"the run for {day}", done)


def trigger(day: str, conf: str | None = None) -> dict:
    extra = ["--conf", conf] if conf else []
    af("dags", "trigger", "shop_daily", "--logical-date", f"{day}T00:00:00+00:00", *extra)
    return wait_run(day)


def clear(day: str) -> dict:
    previous_end = run_for(day)["end_date"]
    af("tasks", "clear", "shop_daily", "--start-date", day, "--end-date", day, "--yes")
    return wait_run(day, previous_end)


def revenue() -> dict[str, float]:
    return {d: float(r) for d, r in sql("SELECT order_date, revenue FROM daily_revenue ORDER BY 1")}


def day_rows(day: str) -> tuple[int, int]:
    rows, orders = sql(f"SELECT count(*), count(DISTINCT order_id) FROM orders WHERE order_date = '{day}'")[0]
    return rows, orders


def main() -> int:
    print("Waiting for the scheduler and the DAGs")
    wait_for("a healthy scheduler", lambda: json.loads(sh("curl", "-s", "http://localhost:8080/api/v2/monitor/health"))
             ["scheduler"]["status"] == "healthy", 180)
    wait_for("the DAGs", lambda: {d["dag_id"] for d in af_json("dags", "list")} >= {"shop_daily", "weekly_report"}, 180)

    print("\n== 1. Run one day; the asset triggers weekly_report ==")
    check(trigger("2024-03-01")["state"] == "success", "shop_daily succeeds for 2024-03-01")
    check(sql("SELECT count(*) FROM daily_revenue")[0][0] == 1, "daily_revenue has one row")
    report = wait_for("weekly_report", lambda: any(r["state"] == "success" for r in runs("weekly_report")))
    check(bool(report), "weekly_report ran without being scheduled by time")
    check(sh("python", "-c", "import os; print(os.path.exists(os.environ.get('LAB_WAREHOUSE_DIR', '/opt/lab/warehouse')"
                             " + '/reports/revenue_last_7_days.csv'))").strip() == "True", "the report file was written")

    print("\n== 2. Backfill a week ==")
    af("backfill", "create", "--dag-id", "shop_daily", "--from-date", "2024-03-02", "--to-date", "2024-03-07")
    days = [f"2024-03-0{d}" for d in range(2, 8)]
    wait_for("the backfill", lambda: all((run_for(d) or {}).get("state") in FINAL_STATES for d in days), 600)
    check(all(run_for(d)["state"] == "success" for d in days), "all six backfilled days succeed")
    check(len(revenue()) == 7, "daily_revenue has seven rows")
    baseline = sorted(revenue().values())[3]

    print("\n== 3. Clearing a day duplicates it until load is idempotent ==")
    clear("2024-03-03")
    rows, orders = day_rows("2024-03-03")
    print(f"2024-03-03 after clear: {rows} rows, {orders} distinct orders")
    check((rows, orders) == (2 * orders, orders) and orders > 0, "the starting load duplicates every order")
    ratio = revenue()["2024-03-03"] / baseline
    check(2.5 < ratio < 4.5, f"the duplicated day's revenue is about 3.5 times a normal day (got {ratio:.1f})")

    shutil.copy(LAB / "solutions" / "shop_daily.py", DAGS_DIR / "shop_daily.py")
    af("dags", "reserialize")
    clear("2024-03-03")
    rows, orders = day_rows("2024-03-03")
    print(f"2024-03-03 after the fixed load: {rows} rows, {orders} distinct orders")
    check(rows == orders > 0, "the reference load leaves one row per order")
    check(0.5 < revenue()["2024-03-03"] / baseline < 1.5, "the day's revenue is back to normal")
    before = sql("SELECT count(*) FROM orders")[0][0]
    clear("2024-03-03")
    check(sql("SELECT count(*) FROM orders")[0][0] == before, "a second clear changes nothing")

    print("\n== 4. The quality gate stops a bad day ==")
    run = trigger("2024-03-10", conf='{"orders_per_day": 20}')
    check(run["state"] == "failed", "the run with 20 orders fails")
    states = {t["task_id"]: t["state"] for t in af_json("tasks", "states-for-dag-run", "shop_daily", run["run_id"])}
    print("task states:", states)
    check(states.get("check_quality") == "failed", "check_quality fails")
    check(states.get("publish_daily_revenue") == "upstream_failed", "publish_daily_revenue is upstream_failed")
    check("2024-03-10" not in revenue(), "the bad day never reaches daily_revenue")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("All checks passed")
    return 0


if __name__ == "__main__":
    original = (DAGS_DIR / "shop_daily.py").read_text(encoding="utf-8")
    try:
        sys.exit(main())
    finally:                                    # leave the exercise DAG as it was found
        (DAGS_DIR / "shop_daily.py").write_text(original, encoding="utf-8")
