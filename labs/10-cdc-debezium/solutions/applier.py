"""Apply Debezium change events to mirror tables in DuckDB. Reference solution.

    python solutions/applier.py                       # apply everything not yet applied, then stop when idle
    python solutions/applier.py --group fresh-1       # a new group starts from the beginning of the topics
    python solutions/applier.py --chaos duplicate     # apply every batch twice
    python solutions/applier.py --chaos shuffle       # random order within a batch
    python solutions/applier.py --chaos reverse       # apply the batches newest first

Rules that make the apply safe to repeat and to reorder:
  * within a batch, only the newest change per key counts (by log position, source.lsn)
  * a stored row is only changed by a change with a HIGHER log position than the one it holds
  * a delete keeps the row, marked _deleted, so a late or replayed older change cannot bring it back
  * tombstones (a Kafka message with no value, sent after each delete) are ignored by parse()
  * a column that appears in the source is added to the target before the change that carries it is merged
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))     # run from the lab folder or solutions/
from common import KEYS  # noqa: E402
from framework import Change, columns_of, run, stage_batch  # noqa: E402


def latest_per_key(changes: list[Change]) -> list[Change]:
    """Keep the change with the highest log position for each key."""
    newest: dict[tuple, Change] = {}
    for change in changes:
        if change.key not in newest or change.lsn > newest[change.key].lsn:
            newest[change.key] = change
    return list(newest.values())


def add_new_columns(con: duckdb.DuckDBPyConnection, table: str, changes: list[Change]) -> None:
    """Add every column that appears in the incoming rows but not in the target."""
    known = {name for name, _ in columns_of(con, table)}
    for change in changes:
        for name, value in change.row.items():
            if name not in known and value is not None:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {sql_type(value)}")
                known.add(name)


def sql_type(value) -> str:
    """A DuckDB type for a JSON value. bool is tested first because it is also an int in Python."""
    return {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE"}.get(type(value), "VARCHAR")


def merge(con: duckdb.DuckDBPyConnection, table: str, changes: list[Change]) -> None:
    """Upsert the changes into the target, and mark deleted rows instead of removing them."""
    if not changes:
        return
    stage_batch(con, table, changes)
    columns = [name for name, _ in columns_of(con, table)]
    on = " AND ".join(f"t.{k} = s.{k}" for k in KEYS[table])
    assignments = ", ".join(f"{c} = s.{c}" for c in columns if c not in KEYS[table]) + ", _lsn = s._lsn, _deleted = s._deleted"
    con.execute(f"""
        MERGE INTO {table} t USING stage s ON {on}
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}, _lsn, _deleted)
                              VALUES ({', '.join('s.' + c for c in columns)}, s._lsn, s._deleted)""")


if __name__ == "__main__":
    run(latest_per_key, merge, add_new_columns)
