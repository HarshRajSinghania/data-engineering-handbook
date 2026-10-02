"""Compare the target tables with the source database, row by row.

    python check.py

Exit code 0 when every table matches, 1 otherwise. A row counts as "live" in the target when its
_deleted flag is not set.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from decimal import Decimal

import duckdb
import psycopg

from common import KEYS, PG_DSN, TARGET_DB

def normalise(value):
    """One representation for values that the two databases return differently."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def rows(cursor, table: str) -> dict[tuple, dict]:
    """Rows keyed by primary key, each as {column name: normalised value}."""
    names = [d[0] for d in cursor.description]
    return {tuple(r[names.index(k)] for k in KEYS[table]): {n: normalise(v) for n, v in zip(names, r)} for r in cursor.fetchall()}


def compare(source_rows: dict, target_rows: dict) -> tuple[list, list, list]:
    missing = sorted(source_rows.keys() - target_rows.keys())
    extra = sorted(target_rows.keys() - source_rows.keys())
    different = sorted(k for k in source_rows.keys() & target_rows.keys() if source_rows[k] != target_rows[k])
    return missing, extra, different


def main() -> int:
    ok = True
    with psycopg.connect(PG_DSN) as pg, duckdb.connect(str(TARGET_DB), read_only=True) as target:
        for table in KEYS:
            source = rows(pg.execute(f"SELECT * FROM {table}"), table)
            mirror = rows(target.execute(f"SELECT * EXCLUDE (_lsn, _deleted) FROM {table} WHERE NOT coalesce(_deleted, false)"), table)
            source_columns = set(next(iter(source.values()))) if source else set()
            target_columns = set(next(iter(mirror.values()))) if mirror else set()
            common = source_columns & target_columns                 # compare the columns both sides have
            trim = lambda table_rows: {k: {c: v for c, v in row.items() if c in common} for k, row in table_rows.items()}  # noqa: E731
            missing, extra, different = compare(trim(source), trim(mirror))
            no_column = sorted(source_columns - target_columns)
            clean = not (missing or extra or different or no_column)
            ok &= clean
            print(f"{'OK ' if clean else 'DIFF'} {table:<12} source {len(source):>7,}   target {len(mirror):>7,}   "
                  f"missing {len(missing)}   extra {len(extra)}   different {len(different)}")
            for label, keys in (("missing in target", missing), ("only in target", extra), ("different", different),
                                ("columns the target lacks", no_column)):
                if keys:
                    print(f"     {label}: {keys[:5]}{' ...' if len(keys) > 5 else ''}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
