"""Lab 10 exercise: apply Debezium change events to mirror tables in DuckDB. Replace the TODOs.

    python applier.py                        # apply everything not yet applied, then stop when idle
    python applier.py --group fresh-1        # a new group starts from the beginning of the topics
    python applier.py --chaos duplicate      # apply every batch twice
    python applier.py --chaos shuffle        # random order within a batch
    python applier.py --chaos reverse        # apply the batches newest first
    python check.py                          # compare the target with the source database

Reference answer: solutions/applier.py

As shipped, this applier is correct only while every event arrives exactly once and in order.
"""
from __future__ import annotations

import duckdb

from common import KEYS
from framework import Change, columns_of, run, stage_batch


def latest_per_key(changes: list[Change]) -> list[Change]:
    """Reduce a batch to one change per key: the one with the highest log position (`lsn`)."""
    # TODO: as shipped, every change is kept, in arrival order
    return changes


def add_new_columns(con: duckdb.DuckDBPyConnection, table: str, changes: list[Change]) -> None:
    """Add every column that appears in the incoming rows but not in the target (exercise 7)."""
    # TODO: compare each change's row (change.row, a dict of column -> value) with columns_of(con, table),
    #       and ALTER TABLE ... ADD COLUMN for any that are missing. Pick a DuckDB type from the Python value.
    #       Until you do, stage_batch() silently drops fields the target does not know.


def merge(con: duckdb.DuckDBPyConnection, table: str, changes: list[Change]) -> None:
    """Apply a batch of changes to the target table.

    The target has the source's columns plus `_lsn` (the log position of the change that last wrote the
    row) and `_deleted`. stage_batch() loads `changes` into a temporary table `stage` with the same columns.
    """
    if not changes:
        return
    stage_batch(con, table, changes)
    keys = ", ".join(KEYS[table])
    # TODO: replace the two statements below with one MERGE INTO {table} ... USING stage that
    #   - updates a matched row only when the incoming change has a higher _lsn than the stored one
    #   - inserts rows that are not there yet
    #   - marks a deleted row (_deleted = true) instead of removing it. Why keep it?
    con.execute(f"INSERT OR REPLACE INTO {table} BY NAME SELECT * FROM stage WHERE NOT _deleted")
    con.execute(f"DELETE FROM {table} WHERE ({keys}) IN (SELECT {keys} FROM stage WHERE _deleted)")


if __name__ == "__main__":
    run(latest_per_key, merge, add_new_columns)
