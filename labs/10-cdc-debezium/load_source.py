"""Create the source database (Postgres) and load the shared dataset into it.

    python load_source.py

Creates the `orders` and `order_items` tables with primary keys, loads the current version of each
order and its valid lines (the same rows as Lab 03's silver tables), creates the replication user
and the publication Debezium reads from. Safe to run again on a fresh stack; drop the connector and
its replication slot first if one exists.
"""
from __future__ import annotations

import csv
import io

import psycopg

from common import PG_DSN, RAW_DIR

DDL = """
DROP PUBLICATION IF EXISTS shop_pub;
DROP TABLE IF EXISTS order_items, orders CASCADE;

CREATE TABLE orders (
    order_id    BIGINT PRIMARY KEY,
    customer_id INT,
    order_ts    TIMESTAMPTZ NOT NULL,
    status      TEXT NOT NULL,
    currency    TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
CREATE TABLE order_items (
    order_id   BIGINT NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    line_no    INT NOT NULL,
    product_id INT NOT NULL,
    quantity   INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Debezium connects as its own user, with the privileges a log reader needs
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
    END IF;
END $$;
GRANT SELECT ON orders, order_items TO debezium;
"""


def read_rows() -> tuple[list[dict], list[dict]]:
    """Latest version of each order, with status normalised, and only the valid order lines."""
    latest: dict[str, dict] = {}
    with open(RAW_DIR / "orders.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["order_id"] not in latest or row["updated_at"] > latest[row["order_id"]]["updated_at"]:
                latest[row["order_id"]] = row
    for row in latest.values():
        row["status"] = row["status"].lower()
    with open(RAW_DIR / "products.csv", newline="", encoding="utf-8") as f:
        products = {r["product_id"] for r in csv.DictReader(f)}
    with open(RAW_DIR / "order_items.csv", newline="", encoding="utf-8") as f:
        items = [r for r in csv.DictReader(f)
                 if r["order_id"] in latest and int(r["quantity"]) > 0 and r["product_id"] in products]
    return sorted(latest.values(), key=lambda r: int(r["order_id"])), items


def copy_rows(cur: psycopg.Cursor, table: str, columns: list[str], rows: list[dict]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for r in rows:
        writer.writerow([r[c] or "\\N" if c == "customer_id" else r[c] for c in columns])
    buffer.seek(0)
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')") as copy:
        copy.write(buffer.read())


def main() -> None:
    orders, items = read_rows()
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        conn.execute(DDL)
        with conn.cursor() as cur:
            copy_rows(cur, "orders", ["order_id", "customer_id", "order_ts", "status", "currency", "updated_at"], orders)
            copy_rows(cur, "order_items", ["order_id", "line_no", "product_id", "quantity", "unit_price"], items)
        conn.execute("CREATE PUBLICATION shop_pub FOR TABLE orders, order_items")
        for table in ("orders", "order_items"):
            print(f"{table:<12} {conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:>7,} rows")


if __name__ == "__main__":
    main()
