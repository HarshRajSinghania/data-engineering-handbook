"""Lab 09 reference solutions.  Usage:  python solutions.py 3      (or no argument to run all seven)

Run pipeline.py first. Exercises 2 and 3 expect one day of changes and a second pipeline run:

    python ../data/simulate_changes.py && python pipeline.py

Each exercise works on its own scratch tables (lab.scratch.*), never on the silver and gold tables,
and returns a small dict of results that ci_smoke.py checks.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from lake import WAREHOUSE, get_spark, snapshot_ids, table

SCRATCH = "lab.scratch"


def fresh(spark: SparkSession, name: str, ddl: str, columns: str = "") -> str:
    """Drop and re-create a scratch table so every exercise starts from the same state."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH}")
    full = f"{SCRATCH}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")
    spark.sql(f"CREATE TABLE {full} {columns} USING iceberg {ddl}")
    return full


def files_on_disk(spark: SparkSession, full: str, pattern: str, folder: str) -> int:
    """Count files in a table's data/ or metadata/ folder, e.g. files_on_disk(spark, t, '*.parquet', 'data')."""
    schema, name = full.split(".")[1:]
    return len(list((WAREHOUSE / schema / name / folder).glob(pattern)))


def ex1_inspect_a_table(spark: SparkSession) -> dict:
    """Q1: What is an Iceberg table on disk? Count the files in each folder, then read the metadata tables."""
    orders = table("silver", "orders")
    counts = {
        "data_files": files_on_disk(spark, orders, "*.parquet", "data"),
        "metadata_json": files_on_disk(spark, orders, "*.metadata.json", "metadata"),
        "manifest_lists": files_on_disk(spark, orders, "snap-*.avro", "metadata"),
        "manifests": files_on_disk(spark, orders, "*-m*.avro", "metadata"),
    }
    print(counts)
    spark.sql(f"SELECT snapshot_id, operation, summary['total-records'] AS total_records FROM {orders}.snapshots").show()
    spark.sql(f"SELECT file_path, record_count, file_size_in_bytes FROM {orders}.files").show(truncate=60)
    spark.sql(f"SELECT length, added_data_files_count, deleted_data_files_count FROM {orders}.manifests").show()
    return {**counts, "current_files": spark.sql(f"SELECT * FROM {orders}.files").count()}


def ex2_row_level_changes(spark: SparkSession) -> dict:
    """Q2: What did the last MERGE do, and what does an UPDATE cost with copy-on-write versus merge-on-read?"""
    last = spark.sql(f"""
        SELECT summary['spark.merge-into.num-target-rows-inserted'] AS inserted,
               summary['spark.merge-into.num-target-rows-updated'] AS updated,
               summary['deleted-records'] AS records_removed, summary['added-records'] AS records_added
        FROM {table('silver', 'orders')}.snapshots ORDER BY committed_at DESC LIMIT 1""").first()
    print(f"Last MERGE into silver.orders: inserted {last.inserted}, updated {last.updated}; "
          f"rewrote the table's file: {last.records_removed} records out, {last.records_added} in")

    result = {"inserted": int(last.inserted), "updated": int(last.updated),
              "merge_records_removed": int(last.records_removed), "merge_records_added": int(last.records_added)}
    for mode, name in (("copy-on-write", "cow"), ("merge-on-read", "mor")):
        props = f"TBLPROPERTIES ('write.update.mode'='{mode}')"
        full = fresh(spark, f"orders_{name}", f"{props} AS SELECT * FROM {table('silver', 'orders')}")
        spark.sql(f"UPDATE {full} SET status = 'cancelled' WHERE order_id IN (600001, 600002)")
        s = spark.sql(f"""SELECT summary['added-records'] AS added, summary['deleted-records'] AS deleted,
                                 summary['added-delete-files'] AS delete_files
                          FROM {full}.snapshots ORDER BY committed_at DESC LIMIT 1""").first()
        print(f"UPDATE of 2 rows, {mode}: records written {s.added}, records removed {s.deleted}, delete files {s.delete_files}")
        result[name] = {"written": int(s.added), "removed": int(s.deleted or 0), "delete_files": int(s.delete_files or 0)}
    return result


def ex3_time_travel(spark: SparkSession) -> dict:
    """Q3: Compare daily revenue now with its first snapshot, by snapshot ID and by timestamp."""
    gold = table("gold", "daily_revenue")
    ids = snapshot_ids(spark, "gold", "daily_revenue")
    print("snapshots:", ids)
    first_committed = spark.sql(f"SELECT committed_at FROM {gold}.snapshots WHERE snapshot_id = {ids[0]}").first()[0]
    stamp = first_committed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    by_id = spark.sql(f"SELECT * FROM {gold} VERSION AS OF {ids[0]}")
    by_time = spark.sql(f"SELECT * FROM {gold} TIMESTAMP AS OF '{stamp}'")
    by_reader = spark.read.option("versionAsOf", ids[0]).table(gold)      # the DataFrame form of VERSION AS OF
    same = by_id.count() == by_time.count() == by_reader.count()

    then, now = by_id.alias("then"), spark.table(gold).alias("now")
    changed = (now.join(then, "order_date", "full_outer")
               .where("NOT (now.orders <=> then.orders) OR NOT (now.revenue_usd <=> then.revenue_usd)")
               .select("order_date", "then.orders", "now.orders", "then.revenue_usd", "now.revenue_usd")
               .orderBy("order_date"))
    changed.show()
    return {"snapshots": len(ids), "three_ways_agree": same,
            "changed_days": [str(r.order_date) for r in changed.collect()]}


def ex4_schema_evolution(spark: SparkSession) -> dict:
    """Q4: Add, rename, widen and drop columns. Which of them touch the data files?"""
    full = fresh(spark, "orders_evolve", f"AS SELECT * FROM {table('silver', 'orders')}")
    files = lambda: sorted(r.file_path for r in spark.sql(f"SELECT file_path FROM {full}.files").collect())  # noqa: E731
    before, first_snapshot = files(), snapshot_ids(spark, "scratch", "orders_evolve")[0]

    spark.sql(f"ALTER TABLE {full} ADD COLUMN channel STRING")
    spark.sql(f"ALTER TABLE {full} RENAME COLUMN currency TO currency_code")
    spark.sql(f"ALTER TABLE {full} ALTER COLUMN customer_id TYPE BIGINT")           # int -> long is a safe widening
    spark.sql(f"ALTER TABLE {full} DROP COLUMN updated_at")
    print("current columns:", spark.table(full).columns)

    try:
        spark.sql(f"ALTER TABLE {full} ALTER COLUMN order_id TYPE INT")             # bigint -> int would lose data
        narrowing_rejected = False
    except Exception as e:  # noqa: BLE001
        narrowing_rejected = "NOT_SUPPORTED_CHANGE_COLUMN" in str(e)
    print("narrowing bigint -> int rejected:", narrowing_rejected)

    old_columns = spark.sql(f"SELECT * FROM {full} VERSION AS OF {first_snapshot}").columns
    print("columns at the first snapshot:", old_columns)
    log = spark.sql(f"SELECT count(*) FROM {full}.metadata_log_entries").first()[0]
    return {"files_unchanged": files() == before, "narrowing_rejected": narrowing_rejected,
            "current_columns": spark.table(full).columns, "old_columns": old_columns,
            "renamed_data_still_there": spark.sql(f"SELECT count(currency_code) FROM {full}").first()[0] == 6200,
            "metadata_versions": log}


def ex5_partition_evolution(spark: SparkSession) -> dict:
    """Q5: Change a table's partitioning from days to hours without rewriting it."""
    events = table("silver", "events")
    full = fresh(spark, "events_evolve", f"PARTITIONED BY (days(event_ts)) AS SELECT * FROM {events}")
    day_query = ("SELECT count(*) FROM {t} WHERE event_ts >= TIMESTAMP '2024-03-10 00:00:00' "
                 "AND event_ts < TIMESTAMP '2024-03-11 00:00:00'")
    before = spark.sql(day_query.format(t=full)).first()[0]
    files_before = spark.sql(f"SELECT count(*) FROM {full}.files").first()[0]

    spark.sql(f"ALTER TABLE {full} REPLACE PARTITION FIELD days(event_ts) WITH hours(event_ts)")
    # New rows are written with the new spec. These are shifted copies of early March, so they do not change the day queried
    spark.sql(f"""INSERT INTO {full}
                  SELECT event_id + 1000000, customer_id, event_type, page, event_ts + INTERVAL 60 DAYS,
                         received_ts + INTERVAL 60 DAYS
                  FROM {events} WHERE event_ts < TIMESTAMP '2024-03-03'""")
    by_spec = {r.spec_id: r.files for r in spark.sql(
        f"SELECT spec_id, count(*) AS files FROM {full}.files GROUP BY spec_id ORDER BY spec_id").collect()}
    print("data files by partition spec:", by_spec)
    after = spark.sql(day_query.format(t=full)).first()[0]
    return {"files_before": files_before, "spec0_files": by_spec.get(0), "spec1_files": by_spec.get(1),
            "old_files_untouched": by_spec.get(0) == files_before, "day_count_before": before, "day_count_after": after}


def ex6_maintenance(spark: SparkSession) -> dict:
    """Q6: Many small commits leave many small files. Compact them, then expire the old snapshots."""
    events = table("silver", "events")
    full = fresh(spark, "events_small", "", "(event_id BIGINT, page STRING, event_ts TIMESTAMP)")
    for i in range(12):                                   # twelve small commits, like a micro-batch job
        spark.sql(f"INSERT INTO {full} SELECT event_id, page, event_ts FROM {events} WHERE event_id % 12 = {i}")
    count_files = lambda: spark.sql(f"SELECT count(*) FROM {full}.files").first()[0]  # noqa: E731
    small_files, rows = count_files(), spark.table(full).count()
    first_snapshot = snapshot_ids(spark, "scratch", "events_small")[0]
    first_rows = spark.sql(f"SELECT count(*) FROM {full} VERSION AS OF {first_snapshot}").first()[0]
    print(f"before compaction: {small_files} data files, {rows:,} rows, {files_on_disk(spark, full, '*.parquet', 'data')} on disk")

    spark.sql(f"CALL lab.system.rewrite_data_files(table => 'scratch.events_small')")
    compacted = count_files()
    still_readable = spark.sql(f"SELECT count(*) FROM {full} VERSION AS OF {first_snapshot}").first()[0] == first_rows
    on_disk_after_compaction = files_on_disk(spark, full, "*.parquet", "data")
    print(f"after compaction: {compacted} data file, but {on_disk_after_compaction} files on disk (old snapshots still need them)")

    cutoff = (datetime.now(timezone.utc) + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    expired = spark.sql(f"CALL lab.system.expire_snapshots(table => 'scratch.events_small', older_than => TIMESTAMP '{cutoff}', "
                        "retain_last => 1)").first()
    on_disk_after_expiry = files_on_disk(spark, full, "*.parquet", "data")
    print(f"after expiring snapshots: {expired.deleted_data_files_count} data files deleted, {on_disk_after_expiry} on disk")
    try:
        spark.sql(f"SELECT count(*) FROM {full} VERSION AS OF {first_snapshot}").collect()
        gone = False
    except Exception:  # noqa: BLE001
        gone = True
    return {"small_files": small_files, "rows": rows, "compacted_files": compacted, "old_snapshot_readable_before_expiry": still_readable,
            "on_disk_after_compaction": on_disk_after_compaction, "deleted_by_expiry": expired.deleted_data_files_count,
            "on_disk_after_expiry": on_disk_after_expiry, "expired_snapshot_gone": gone,
            "rows_after": spark.table(full).count()}


def ex7_branches(spark: SparkSession) -> dict:
    """Q7: Write-audit-publish with a branch: write to a branch, check it, then publish by fast-forwarding main."""
    full = fresh(spark, "orders_wap", f"AS SELECT * FROM {table('silver', 'orders')} WHERE order_id < 601000")
    spark.sql(f"ALTER TABLE {full} CREATE BRANCH audit")
    spark.sql(f"""INSERT INTO {full}.branch_audit
                  SELECT order_id + 900000, customer_id, order_ts, status, currency, updated_at
                  FROM {table('silver', 'orders')} WHERE order_id < 600010""")
    main_rows = spark.table(full).count()
    audit_rows = spark.sql(f"SELECT count(*) FROM {full} VERSION AS OF 'audit'").first()[0]
    print(f"main: {main_rows} rows, audit branch: {audit_rows} rows")

    # The audit: any check on the branch. Here, no order may appear twice
    duplicates = spark.sql(f"SELECT count(*) - count(DISTINCT order_id) FROM {full} VERSION AS OF 'audit'").first()[0]
    if duplicates == 0:
        spark.sql(f"CALL lab.system.fast_forward('scratch.orders_wap', 'main', 'audit')")     # publish
    published = spark.table(full).count()
    print(f"after the audit ({duplicates} duplicates): main has {published} rows")
    refs = {r.name: r.type for r in spark.sql(f"SELECT name, type FROM {full}.refs").collect()}
    return {"main_before": main_rows, "audit_rows": audit_rows, "main_after": published, "refs": refs}


EXERCISES = [ex1_inspect_a_table, ex2_row_level_changes, ex3_time_travel, ex4_schema_evolution,
             ex5_partition_evolution, ex6_maintenance, ex7_branches]


def main() -> None:
    spark = get_spark("solutions")
    chosen = [EXERCISES[int(a) - 1] for a in sys.argv[1:]] or EXERCISES
    for fn in chosen:
        print(f"\n=== {fn.__name__}: {fn.__doc__.splitlines()[0]}")
        fn(spark)


if __name__ == "__main__":
    main()
