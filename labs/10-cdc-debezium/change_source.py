"""Simulate a burst of application activity on the source database, so there are changes to capture.

    python change_source.py             # batch 1
    python change_source.py --batch 2   # a second, different batch

Each batch, as separate transactions:
    - inserts new orders with two lines each              (batch 1: 50 orders, ids from 700001)
    - advances the status of existing orders              (batch 1: 100 orders, ids from 600001)
    - updates one order three times in a row              (the first order of the batch)
    - deletes orders; their lines are deleted by the cascade   (batch 1: 10 orders, ids from 600101)
Batch 2 uses other ids and different sizes. The counts it prints are what the exercises expect to see in Kafka.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import psycopg

from common import PG_DSN

BATCHES = {   # batch: (new order id start, count, first updated id, updates, first deleted id, deletes)
    1: (700001, 50, 600001, 100, 600101, 10),
    2: (701001, 30, 600201, 50, 600111, 5),
}
NEXT_STATUS = "CASE status WHEN 'placed' THEN 'paid' WHEN 'paid' THEN 'shipped' WHEN 'shipped' THEN 'delivered' ELSE status END"


def main() -> dict[str, int]:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch", type=int, choices=BATCHES, default=1)
    new_start, new_count, upd_start, upd_count, del_start, del_count = BATCHES[parser.parse_args().batch]
    now = datetime(2024, 4, 1, 9, 0, tzinfo=timezone.utc)

    with psycopg.connect(PG_DSN) as conn:
        for i in range(new_count):                                       # one transaction per new order
            order_id, at = new_start + i, now + timedelta(seconds=i)
            with conn.transaction():
                conn.execute("INSERT INTO orders VALUES (%s, %s, %s, 'placed', 'USD', %s)", (order_id, 1 + i % 3000, at, at))
                for line in (1, 2):
                    conn.execute("INSERT INTO order_items VALUES (%s, %s, %s, %s, %s)", (order_id, line, 1 + (i + line) % 20, line, 10 + line))

        for order_id in range(upd_start, upd_start + upd_count):        # one update per order
            conn.execute(f"UPDATE orders SET status = {NEXT_STATUS}, updated_at = %s WHERE order_id = %s",
                         (now + timedelta(minutes=5), order_id))
        for step in (1, 2):                                             # the same order, updated again and again
            conn.execute(f"UPDATE orders SET status = {NEXT_STATUS}, updated_at = %s WHERE order_id = %s",
                         (now + timedelta(minutes=5 + step), upd_start))

        doomed = range(del_start, del_start + del_count)
        lines = conn.execute("SELECT count(*) FROM order_items WHERE order_id = ANY(%s)", (list(doomed),)).fetchone()[0]
        conn.execute("DELETE FROM orders WHERE order_id = ANY(%s)", (list(doomed),))   # the cascade deletes the lines
        conn.commit()

    summary = {"orders_inserted": new_count, "lines_inserted": 2 * new_count, "orders_updated": upd_count + 2,
               "orders_deleted": del_count, "lines_deleted": lines}
    print(summary)
    return summary


if __name__ == "__main__":
    main()
