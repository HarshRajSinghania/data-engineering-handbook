"""Summarise the change events in a topic, and print some of them.

Usage:
    python show_events.py orders                      # counts by operation, partitions, then one example of each operation
    python show_events.py order_items --op d          # print delete events
    python show_events.py orders --key 600002         # every event for one order, in log order
    python show_events.py orders --op u --limit 3 --full

Reads the whole topic with a throwaway consumer group, so it never disturbs the applier.
"""
from __future__ import annotations

import argparse
import json
import uuid
from collections import Counter

from confluent_kafka import Consumer

from common import BOOTSTRAP, KEYS, TOPICS


def read_topic(topic: str) -> list:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"show-{uuid.uuid4()}", "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    consumer.subscribe([topic])
    messages, empty = [], 0
    while empty < 4:
        batch = consumer.consume(num_messages=1000, timeout=1.0)
        empty = 0 if batch else empty + 1
        messages += [m for m in batch if not m.error()]
    consumer.close()
    return messages


def summarise(table: str) -> dict:
    """Counts of events by operation, plus tombstones, for a table's topic (used by ci_smoke.py too)."""
    ops: Counter[str] = Counter()
    for m in read_topic(TOPICS[table]):
        ops["tombstone" if m.value() is None else json.loads(m.value())["op"]] += 1
    return dict(ops)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("table", choices=list(TOPICS))
    parser.add_argument("--op", choices=["r", "c", "u", "d"])
    parser.add_argument("--key", help="only events for this primary key (the first key column)")
    parser.add_argument("--limit", type=int, default=1, help="events to print per operation")
    parser.add_argument("--full", action="store_true", help="print the whole event, including the source block")
    args = parser.parse_args()

    messages = read_topic(TOPICS[args.table])
    ops: Counter[str] = Counter()
    partitions: Counter[int] = Counter()
    shown: Counter[str] = Counter()
    for m in messages:
        partitions[m.partition()] += 1
        if m.value() is None:
            ops["tombstone"] += 1
            continue
        event = json.loads(m.value())
        ops[event["op"]] += 1
        if args.op and event["op"] != args.op:
            continue
        if args.key and str(json.loads(m.key())[KEYS[args.table][0]]) != args.key:
            continue
        if shown[event["op"]] < (10**9 if args.key else args.limit):
            shown[event["op"]] += 1
            view = event if args.full else {"op": event["op"], "before": event["before"], "after": event["after"],
                                            "source": {k: event["source"][k] for k in ("snapshot", "lsn", "txId")}}
            print(f"partition {m.partition()} offset {m.offset()} key {m.key().decode()}")
            print(json.dumps(view, indent=2))
    print(f"\n{TOPICS[args.table]}: {len(messages):,} messages   by operation: {dict(sorted(ops.items()))}   "
          f"by partition: {dict(sorted(partitions.items()))}")


if __name__ == "__main__":
    main()
