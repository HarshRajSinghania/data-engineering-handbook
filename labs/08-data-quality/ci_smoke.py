"""Check the Lab 08 reference solution against the situations the README describes. CI runs this.

    python ci_smoke.py

It builds its own copies of the dataset in a temporary folder, so it never touches ../data/output.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

LAB = Path(__file__).resolve().parent
DATA_TOOLS = LAB.parent / "data"
failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message, flush=True)
    if not ok:
        failures.append(message)


def python(*args: str, data: Path | None = None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, **({"LAB_DATA_DIR": str(data)} if data else {})}
    return subprocess.run([sys.executable, *args], cwd=LAB, env=env, capture_output=True, text=True, timeout=600)


def run(script: str, data: Path, *args: str) -> tuple[int, list[dict]]:
    """Run a lab script; return its exit code and the outcomes it wrote to output/quality_report.json."""
    (LAB / "output" / "quality_report.json").unlink(missing_ok=True)
    result = python(script, *args, data=data)
    if result.returncode not in (0, 1):
        print(result.stdout[-2000:], result.stderr[-2000:])
    report = LAB / "output" / "quality_report.json"
    return result.returncode, json.loads(report.read_text())["outcomes"] if report.exists() else []


def failed(outcomes: list[dict], severity: str) -> dict[str, int | None]:
    return {f"{o['dataset']}.{o['check']}[{o['column']}]": o["unexpected_count"]
            for o in outcomes if not o["success"] and o["severity"] == severity}


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="lab08-"))
    try:
        full, changed, short, drift = tmp / "full", tmp / "changed", tmp / "short", tmp / "drift"
        python(str(DATA_TOOLS / "generate.py"), "--out", str(full))
        shutil.copytree(full, changed)
        python(str(DATA_TOOLS / "simulate_changes.py"), "--out", str(changed))
        python(str(DATA_TOOLS / "generate.py"), "--days", "10", "--out", str(short))
        shutil.copytree(full, drift)
        orders = drift / "orders.csv"
        orders.write_text(orders.read_text().replace(",status,", ",order_status,", 1))
        shutil.rmtree(LAB / "output", ignore_errors=True)

        print("== The files as they arrived: every planted problem is found, and the gate blocks ==")
        code, outcomes = run("solutions.py", full, "--stage", "raw")
        check(code == 1, "the raw files are blocked (exit code 1)")
        check(failed(outcomes, "critical") == {
            "orders.column_values_to_be_unique[order_id]": 622,
            "orders.column_values_to_be_in_set[status]": 202,
            "order_items.column_values_to_be_between[quantity]": 63,
            "order_items.column_values_to_be_in_set[product_id]": 83,
            "events.column_values_to_be_unique[event_id]": 642,
        }, "exactly the five planted problems fail as critical, with the documented counts")
        check(list(failed(outcomes, "warning")) == ["orders.column_values_to_not_be_null[customer_id]"],
              "the missing customer_id values raise a warning")
        check(not (LAB / "output" / "published").exists(), "nothing is published from a blocked run")

        print("\n== Cleaned data passes and is published; the warning does not block ==")
        code, outcomes = run("solutions.py", full)
        check(code == 0, "the cleaned data is published (exit code 0)")
        check(not failed(outcomes, "critical") and len(failed(outcomes, "warning")) == 1, "only the warning remains")
        rows = {t: len((LAB / "output" / "published" / f"{t}.csv").read_text().splitlines()) - 1
                for t in ("orders", "order_items", "events")}
        check(rows == {"orders": 6000, "order_items": 14769, "events": 30000}, f"published row counts (got {rows})")

        print("\n== A new day of changes still passes ==")
        code, _ = run("solutions.py", changed)
        check(code == 0, "the data after simulate_changes.py is published")

        print("\n== A stale extract is blocked by the freshness check ==")
        shutil.rmtree(LAB / "output" / "published")
        code, outcomes = run("solutions.py", short)
        check(code == 1 and list(failed(outcomes, "critical")) == ["orders.column_max_to_be_between[order_ts]"],
              "10 days of data fail only the freshness check")
        check(not (LAB / "output" / "published").exists(), "nothing is published from the stale extract")

        print("\n== Schema drift is blocked ==")
        code, outcomes = run("solutions.py", drift, "--stage", "raw")
        check(code == 1 and "orders.table_columns_to_match_set[]" in failed(outcomes, "critical"),
              "a renamed column fails the schema check")

        print("\n== The exercise file runs as shipped ==")
        code, _ = run("exercises.py", full, "--stage", "raw")
        check(code == 0, "exercises.py runs before any TODO is filled in")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        shutil.rmtree(LAB / "output", ignore_errors=True)

    print()
    print(f"{len(failures)} check(s) failed" if failures else "All checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
