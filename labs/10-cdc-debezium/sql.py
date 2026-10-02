"""Run SQL against the source database (Postgres) and print the rows. No psql needed.

Usage:
    python sql.py "SELECT count(*) FROM orders"
    python sql.py "UPDATE orders SET status = 'cancelled' WHERE order_id = 600002"
    python sql.py "SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots"
"""
from __future__ import annotations

import sys

import psycopg

from common import PG_DSN


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        for statement in sys.argv[1:]:
            cursor = conn.execute(statement)
            if cursor.description:
                print(" | ".join(d.name for d in cursor.description))
                for row in cursor.fetchall():
                    print(" | ".join(str(v) for v in row))
            else:
                print(f"{cursor.statusmessage}")


if __name__ == "__main__":
    main()
