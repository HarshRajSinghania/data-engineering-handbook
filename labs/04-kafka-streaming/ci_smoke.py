"""End-to-end check of Lab 04 against a running broker. CI runs this; you can too.

It follows the README's exercises and asserts what the README claims:

  1. The exercise processor crashes on the first malformed message (a poison pill).
  2. The reference processor sends about 100 messages of each invalid kind to the
     dead-letter topic, loses fewer than 100 events as too late, and double-counts none.
  3. After a crash and restart, results are incomplete, because offsets were committed
     for events whose counts were still in memory.

Usage (broker on localhost:9092, or set KAFKA_BOOTSTRAP):
    python ../data/generate.py
    python ci_smoke.py
"""
from __future__ import annotations

import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from check_hourly import actual_counts, expected_counts
from common import BOOTSTRAP, TOPIC_DLQ, TOPIC_EVENTS, TOPIC_HOURLY

LAB = Path(__file__).resolve().parent
TOPICS = {TOPIC_EVENTS: 3, TOPIC_HOURLY: 1, TOPIC_DLQ: 1}
failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message, flush=True)
    if not ok:
        failures.append(message)


def reset_topics(names: list[str]) -> None:
    """Delete and re-create topics, so each scenario starts from an empty topic."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = set(admin.list_topics(timeout=30).topics)
    to_delete = [n for n in names if n in existing]
    for name, future in (admin.delete_topics(to_delete).items() if to_delete else []):
        future.result()
    deadline = time.time() + 30
    while existing & set(names) & set(admin.list_topics(timeout=30).topics) and time.time() < deadline:
        time.sleep(0.5)
    for name, future in admin.create_topics([NewTopic(n, TOPICS[n], 1) for n in names]).items():
        future.result()
    time.sleep(1)                       # let partition leadership settle


def run(*args: str, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    print("$ python", " ".join(args), flush=True)
    result = subprocess.run([sys.executable, *args], cwd=LAB, capture_output=True, text=True, timeout=timeout)
    print(result.stdout.strip()[-1500:], flush=True)
    if result.returncode:
        print(result.stderr.strip()[-1500:], flush=True)
    return result


def dlq_reasons() -> Counter[str]:
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"dlq-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([TOPIC_DLQ])
    reasons: Counter[str] = Counter()
    empty = 0
    while empty < 5:
        batch = consumer.consume(num_messages=1000, timeout=1.0)
        empty = 0 if batch else empty + 1
        for msg in batch:
            headers = dict(msg.headers() or [])
            reasons[headers["reason"].decode().split(":")[0]] += 1
    consumer.close()
    return reasons


def compare() -> tuple[int, int, int]:
    """(windows, missing events, extra events) against the source file."""
    expected, actual = expected_counts(), actual_counts()
    keys = expected.keys() | actual.keys()
    missing = sum(expected[k] - actual[k] for k in keys if actual[k] < expected[k])
    extra = sum(actual[k] - expected[k] for k in keys if actual[k] > expected[k])
    return len(keys), missing, extra


def main() -> int:
    if not (LAB.parent / "data" / "output" / "events.jsonl").exists():
        run("../data/generate.py")

    print("\n== 1. The exercise processor stops at a poison pill ==")
    reset_topics(list(TOPICS))
    check(run("producer.py", "--bad-rate", "0.01").returncode == 0, "producer sends the events and the bad messages")
    result = run("stream_processor.py", "--group", "ci-exercise", "--idle-seconds", "5")
    check(result.returncode != 0 and "Traceback" in result.stderr,
          "the unfinished processor crashes on the first malformed message")

    print("\n== 2. The reference processor: dead-letter topic and window counts ==")
    reset_topics([TOPIC_HOURLY, TOPIC_DLQ])
    result = run("solutions/stream_processor.py", "--group", "ci-reference")
    check(result.returncode == 0, "the reference processor runs to completion")
    reasons = dlq_reasons()
    print("dead-letter reasons:", dict(reasons))
    for kind in ("invalid_json", "missing_fields", "invalid_event_ts"):
        check(80 <= reasons[kind] <= 120, f"about 100 messages in the dead-letter topic for {kind} (got {reasons[kind]})")
    check(0 < reasons["too_late"] < 100, f"fewer than 100 events are too late (got {reasons['too_late']})")
    windows, missing, extra = compare()
    print(f"windows={windows} missing={missing} extra={extra}")
    check(extra == 0, "no event is double-counted")
    check(missing == reasons["too_late"], "every missing event is a dead-lettered late event")

    print("\n== 3. A crash loses counts that were only in memory ==")
    reset_topics([TOPIC_HOURLY, TOPIC_DLQ])
    crashed = run("solutions/stream_processor.py", "--group", "ci-crash", "--crash-after-batches", "20")
    check(crashed.returncode == 1, "the processor exits abruptly after 20 batches")
    check(run("solutions/stream_processor.py", "--group", "ci-crash").returncode == 0, "the restarted processor completes")
    _, missing_after_crash, _ = compare()
    print(f"missing after crash and restart: {missing_after_crash}")
    check(missing_after_crash > 1000, f"thousands of events are missing after the crash (got {missing_after_crash})")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("All checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
