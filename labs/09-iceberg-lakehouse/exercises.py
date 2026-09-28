"""Lab 09 exercises. Replace each TODO, then run:  python exercises.py 3

Every function runs as-is and prints a starting point. Reference answers: solutions.py.
Run pipeline.py first. Exercises 2 and 3 expect one day of changes and a second pipeline run:

    python ../data/simulate_changes.py && python pipeline.py

Exercises 2 and 4-7 work on scratch copies in lab.scratch, never on the silver and gold tables.
"""
from __future__ import annotations

import sys

from pyspark.sql import SparkSession

from lake import WAREHOUSE, get_spark, snapshot_ids, table  # noqa: F401  (used by your answers)

SCRATCH = "lab.scratch"


def fresh(spark: SparkSession, name: str, ddl: str, columns: str = "") -> str:
    """Drop and re-create a scratch table so every exercise starts from the same state."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SCRATCH}")
    full = f"{SCRATCH}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full} PURGE")
    spark.sql(f"CREATE TABLE {full} {columns} USING iceberg {ddl}")
    return full


def ex1_inspect_a_table(spark: SparkSession) -> None:
    """Q1: What is an Iceberg table on disk? Count the files in each folder, then read the metadata tables."""
    orders = table("silver", "orders")
    table_dir = WAREHOUSE / "silver" / "orders"
    print(sorted(p.name for p in table_dir.iterdir()))
    # TODO: count the Parquet files in data/, and the *.metadata.json, snap-*.avro (manifest lists)
    #       and other *.avro (manifests) files in metadata/
    # TODO: show the table's snapshots, its current data files, and its manifests, using the metadata
    #       tables: SELECT * FROM lab.silver.orders.snapshots (also .files and .manifests).
    #       Why are there more data files on disk than in `.files`?


def ex2_row_level_changes(spark: SparkSession) -> None:
    """Q2: What did the last MERGE do, and what does an UPDATE cost with copy-on-write versus merge-on-read?"""
    spark.sql(f"SELECT snapshot_id, operation, summary FROM {table('silver', 'orders')}.snapshots").show(truncate=80)
    # TODO: from the summary of the latest snapshot of silver.orders, show how many rows the MERGE
    #       inserted and updated ('spark.merge-into.num-target-rows-inserted' and '-updated'), and
    #       how many records were removed and added ('deleted-records', 'added-records').
    # TODO: create two scratch copies of silver.orders with fresh(), one with TBLPROPERTIES
    #       ('write.update.mode'='copy-on-write') and one with 'merge-on-read'. UPDATE the status of
    #       two orders in each, and compare the snapshot summaries: records written, records
    #       removed, and 'added-delete-files'.


def ex3_time_travel(spark: SparkSession) -> None:
    """Q3: Compare daily revenue now with its first snapshot, by snapshot ID and by timestamp."""
    gold = table("gold", "daily_revenue")
    print("snapshots:", snapshot_ids(spark, "gold", "daily_revenue"))
    spark.table(gold).orderBy("order_date", ascending=False).show(3)
    # TODO: read the first snapshot with SELECT ... VERSION AS OF <snapshot id>, then again with
    #       TIMESTAMP AS OF (use the snapshot's committed_at), then with the DataFrame reader option
    #       "versionAsOf". Do the three agree?
    # TODO: full outer join the first snapshot with the current table on order_date and keep the
    #       days whose orders or revenue differ. Hint: the null-safe comparison operator <=>


def ex4_schema_evolution(spark: SparkSession) -> None:
    """Q4: Add, rename, widen and drop columns. Which of them touch the data files?"""
    full = fresh(spark, "orders_evolve", f"AS SELECT * FROM {table('silver', 'orders')}")
    spark.table(full).printSchema()
    # TODO: ADD COLUMN channel STRING, RENAME COLUMN currency TO currency_code, widen customer_id
    #       to BIGINT (ALTER COLUMN ... TYPE), DROP COLUMN updated_at. After each, compare the list
    #       of data files (SELECT file_path FROM <table>.files) with the list before.
    # TODO: try to narrow order_id from BIGINT to INT. What happens?
    # TODO: read the table VERSION AS OF its first snapshot. Which column names do you get?


def ex5_partition_evolution(spark: SparkSession) -> None:
    """Q5: Change a table's partitioning from days to hours without rewriting it."""
    events = table("silver", "events")
    full = fresh(spark, "events_evolve", f"PARTITIONED BY (days(event_ts)) AS SELECT * FROM {events}")
    spark.sql(f"SELECT spec_id, count(*) AS files FROM {full}.files GROUP BY spec_id").show()
    # TODO: ALTER TABLE ... REPLACE PARTITION FIELD days(event_ts) WITH hours(event_ts)
    # TODO: INSERT some rows (for example a copy of early-March events shifted 60 days later), then
    #       count data files per spec_id again. Which files use the old spec?
    # TODO: count the events on 2024-03-10 before and after. Did the query need to change?


def ex6_maintenance(spark: SparkSession) -> None:
    """Q6: Many small commits leave many small files. Compact them, then expire the old snapshots."""
    events = table("silver", "events")
    full = fresh(spark, "events_small", "", "(event_id BIGINT, page STRING, event_ts TIMESTAMP)")
    for i in range(12):                                   # twelve small commits, like a micro-batch job
        spark.sql(f"INSERT INTO {full} SELECT event_id, page, event_ts FROM {events} WHERE event_id % 12 = {i}")
    spark.sql(f"SELECT count(*) AS data_files, sum(record_count) AS row_count FROM {full}.files").show()
    # TODO: compact with CALL lab.system.rewrite_data_files(table => 'scratch.events_small'), then count
    #       the data files in `.files` and the Parquet files in the table's data/ folder.
    # TODO: can you still read the first snapshot (VERSION AS OF)?
    # TODO: CALL lab.system.expire_snapshots(table => ..., older_than => <a timestamp in the future>,
    #       retain_last => 1). How many files are left on disk? Can you still read the first snapshot?


def ex7_branches(spark: SparkSession) -> None:
    """Q7: Write-audit-publish with a branch: write to a branch, check it, then publish by fast-forwarding main."""
    full = fresh(spark, "orders_wap", f"AS SELECT * FROM {table('silver', 'orders')} WHERE order_id < 601000")
    print("rows on main:", spark.table(full).count())
    # TODO: ALTER TABLE ... CREATE BRANCH audit, then INSERT INTO <table>.branch_audit new rows.
    # TODO: compare the row count on main with the count on the branch (VERSION AS OF 'audit').
    # TODO: run an audit query on the branch (for example: no order_id appears twice). If it passes,
    #       publish with CALL lab.system.fast_forward('scratch.orders_wap', 'main', 'audit').


EXERCISES = [ex1_inspect_a_table, ex2_row_level_changes, ex3_time_travel, ex4_schema_evolution,
             ex5_partition_evolution, ex6_maintenance, ex7_branches]


def main() -> None:
    spark = get_spark("exercises")
    chosen = [EXERCISES[int(a) - 1] for a in sys.argv[1:]] or EXERCISES
    for fn in chosen:
        print(f"\n=== {fn.__name__}: {fn.__doc__.splitlines()[0]}")
        fn(spark)


if __name__ == "__main__":
    main()
