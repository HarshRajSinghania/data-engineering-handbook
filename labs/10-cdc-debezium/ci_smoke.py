"""Run the whole of Lab 10 against a running stack and check what the README says happens. CI runs this.

    docker compose up -d --wait
    python ci_smoke.py

It builds its own copy of the dataset in a temporary folder, loads it into Postgres (replacing the lab's
tables), and registers and deletes connectors. Start from a fresh stack (`docker compose down -v`).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import psycopg

LAB = Path(__file__).resolve().parent
TMP = Path(tempfile.mkdtemp(prefix="lab10-"))
os.environ["LAB_DATA_DIR"] = str(TMP / "data")
os.environ["TARGET_DB"] = str(TMP / "target.duckdb")
sys.path.insert(0, str(LAB))

from common import PG_DSN  # noqa: E402
from show_events import read_topic, summarise  # noqa: E402

failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message, flush=True)
    if not ok:
        failures.append(message)


def script(*args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run([sys.executable, *args], cwd=LAB, capture_output=True, text=True, timeout=900)
    if result.returncode and "check.py" not in args[0] and not any("applier" in a for a in args):
        print(result.stdout[-2000:], result.stderr[-2000:])
    return result


def sql(statement: str, *params):
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        cursor = conn.execute(statement, params)
        return cursor.fetchall() if cursor.description else None


def wait_for(description: str, condition, timeout: int = 90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {description}")


def events(table: str, op: str, key: int | None = None) -> list[dict]:
    found = []
    for m in read_topic(f"shop.public.{table}"):
        if m.value() is None:
            continue
        e = json.loads(m.value())
        if e["op"] == op and (key is None or json.loads(m.key())["order_id"] == key):
            found.append(e)
    return found


def lag_bytes() -> int:
    return int(sql("SELECT max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots")[0][0])


def apply(*args: str) -> subprocess.CompletedProcess[str]:
    """Apply into a fresh target with a new consumer group, then compare with the source."""
    Path(os.environ["TARGET_DB"]).unlink(missing_ok=True)
    return script(*args, "--group", f"ci-{time.time_ns()}")


def target_matches() -> tuple[bool, str]:
    result = script("check.py")
    return result.returncode == 0, result.stdout


def main() -> int:
    script(str(LAB.parent / "data" / "generate.py"), "--out", os.environ["LAB_DATA_DIR"])
    check(script("load_source.py").returncode == 0, "the source database is loaded")
    check(script("register_connector.py", "register").returncode == 0, "the connector is registered and running")
    if failures:                                     # nothing below can work without the stack
        print(f"{len(failures)} check(s) failed")
        return 1

    print("\n== 1. The initial snapshot ==")
    wait_for("the snapshot", lambda: summarise("orders").get("r") == 6000 and summarise("order_items").get("r") == 14769)
    check(summarise("orders") == {"r": 6000} and summarise("order_items") == {"r": 14769},
          "6,000 orders and 14,769 lines arrive as snapshot reads (op r)")

    print("\n== 2. Live changes ==")
    check(script("change_source.py").returncode == 0, "change_source.py runs")
    expected_orders = {"r": 6000, "c": 50, "u": 102, "d": 10, "tombstone": 10}
    expected_items = {"r": 14769, "c": 100, "d": 25, "tombstone": 25}
    wait_for("batch 1", lambda: summarise("orders") == expected_orders and summarise("order_items") == expected_items)
    check(True, "50 inserts, 102 updates and 10 deletes on orders; 100 inserts and 25 cascaded deletes on lines")
    check(all(e["before"] is None for e in events("orders", "u")), "with the default replica identity an update's `before` is null")
    deleted = events("orders", "d")[0]
    check(deleted["after"] is None and deleted["before"]["status"] == "", "a delete's `before` has the key but placeholder values elsewhere")
    check(len({e["source"]["lsn"] for e in events("orders", "u")}) > 100, "every change carries its own log position")

    print("\n== 3. Applying the changes ==")
    check(apply("solutions/applier.py").returncode == 0 and target_matches()[0], "the reference applier makes the target match the source")
    for chaos, meaning in (("duplicate", "every batch applied twice"), ("shuffle", "each batch in random order"),
                           ("reverse", "the batches applied newest first")):
        apply("solutions/applier.py", "--chaos", chaos)
        check(target_matches()[0], f"still matches with {meaning}")
    apply("applier.py", "--chaos", "reverse")
    ok, output = target_matches()
    check(not ok and "extra 10" in output and "extra 25" in output and "different 100" in output,
          "the unfinished applier resurrects 10 orders and 25 lines, and 100 orders keep an old status, when batches arrive newest first")
    check(apply("applier.py", "--max-messages", "1000").returncode == 0, "applier.py runs as shipped")

    print("\n== 4. Pause, lag, resume, restart ==")
    script("register_connector.py", "pause")
    time.sleep(5)
    before_lag = lag_bytes()
    check(script("change_source.py", "--batch", "2").returncode == 0, "batch 2 is applied to the source while the connector is paused")
    time.sleep(10)
    check(summarise("orders") == expected_orders, "no events arrive while the connector is paused")
    check(lag_bytes() > before_lag, "the replication slot's lag grows while nothing reads it")
    paused_lag = lag_bytes()
    script("register_connector.py", "resume")
    after_orders = {"r": 6000, "c": 80, "u": 154, "d": 15, "tombstone": 15}
    after_items = {"r": 14769, "c": 160, "d": 36, "tombstone": 36}
    wait_for("batch 2", lambda: summarise("orders") == after_orders and summarise("order_items") == after_items)
    check(True, "after resume, every change made while paused arrives, none twice")
    wait_for("the slot to catch up", lambda: lag_bytes() < paused_lag)
    check(True, "the slot's lag falls again")
    script("register_connector.py", "restart")
    script("register_connector.py", "delete")
    sql("UPDATE orders SET status = 'delivered', updated_at = now() WHERE order_id BETWEEN 600301 AND 600310")   # while deleted
    script("register_connector.py", "register")
    wait_for("changes made while the connector was deleted", lambda: summarise("orders").get("u") == 164)
    check(summarise("orders").get("r") == 6000, "restarting or re-creating the connector does not repeat the snapshot; the slot kept the changes")
    apply("solutions/applier.py")
    check(target_matches()[0], "the target matches after the connector has been paused, restarted and re-created")

    print("\n== 5. A new snapshot ==")
    orders_now = sql("SELECT count(*) FROM orders")[0][0]
    items_now = sql("SELECT count(*) FROM order_items")[0][0]
    script("register_connector.py", "delete")
    wait_for("the slot to be released", lambda: sql("SELECT NOT active FROM pg_replication_slots WHERE slot_name = 'shop_cdc'")[0][0])
    sql("SELECT pg_drop_replication_slot('shop_cdc')")
    check(script("register_connector.py", "register", "--name", "shop-cdc-v2", "--slot", "shop_cdc_v2").returncode == 0,
          "a connector with a new name and slot starts a new snapshot")
    wait_for("the second snapshot", lambda: summarise("orders").get("r") == 6000 + orders_now)
    check(summarise("order_items").get("r") == 14769 + items_now, f"the snapshot re-reads all {orders_now:,} orders and {items_now:,} lines")
    # The target already holds these rows: apply the whole topic again, from the beginning, into the same target
    rows_before = script("check.py").stdout
    script("solutions/applier.py", "--group", f"ci-{time.time_ns()}")
    check(target_matches()[0] and rows_before.count("OK ") == 2, "applying the snapshot again changes nothing (the apply is idempotent)")

    print("\n== 6. Replica identity, and a new column ==")
    sql("ALTER TABLE orders REPLICA IDENTITY FULL")
    sql("UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id = 600400")
    full = events("orders", "u", 600400)[-1]
    check(full["before"] is not None and full["before"]["order_id"] == 600400 and full["before"]["status"] != "cancelled",
          "with REPLICA IDENTITY FULL an update's `before` holds the old row")
    sql("ALTER TABLE orders REPLICA IDENTITY DEFAULT")
    sql("ALTER TABLE orders ADD COLUMN channel TEXT")
    sql("UPDATE orders SET channel = 'web', updated_at = now() WHERE order_id BETWEEN 600401 AND 600420")
    wait_for("events with the new column", lambda: sum("channel" in (e["after"] or {}) for e in events("orders", "u")) >= 20)
    check(True, "events after the ALTER TABLE carry the new column")
    apply("applier.py")
    ok, output = target_matches()
    check(not ok and "columns the target lacks: ['channel']" in output, "the unfinished applier drops the new column, and check.py notices")
    apply("solutions/applier.py")
    check(target_matches()[0], "the reference applier adds the column and the target matches")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("All checks passed")
    return 0


if __name__ == "__main__":
    try:
        code = main()
    finally:
        shutil.rmtree(TMP, ignore_errors=True)
    sys.exit(code)
