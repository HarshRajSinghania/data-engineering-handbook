# Lab 04 — Streaming with Kafka

Run a Kafka broker in Docker, replay a realistic event stream into it, and build a stream processor that counts page views per hour by event time. Along the way you handle the problems every streaming pipeline meets: malformed messages, duplicate deliveries, late data, consumer-group rebalances, and crashes.

| | |
|-|-|
| **Time** | 90–120 minutes |
| **Runs on** | Docker (one Kafka broker, ~300 MB of memory) + Python |
| **Guides** | [Kafka](../../docs/04-streaming/kafka-reference.md) · [Apache Flink](../../docs/04-streaming/flink-reference.md) · [Ingestion & CDC](../../docs/02-processing/ingestion-cdc.md) · [Docker](../../docs/06-infrastructure/docker-reference.md) |

## Setup

```bash
cd labs/04-kafka-streaming
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

docker compose up -d --wait         # Kafka 4.3 in KRaft mode on localhost:9092
python ../data/generate.py          # writes ../data/output/events.jsonl
```

The commands below use a shell function for Kafka's command-line tools, which are inside the container. On Windows, run the lab in WSL2 or Git Bash.

```bash
kafka() { tool=$1; shift; docker exec lab04-kafka /opt/kafka/bin/kafka-$tool.sh --bootstrap-server localhost:9092 "$@"; }
```

Create the topics. The broker is configured not to create topics automatically, as in most production clusters:

```bash
kafka topics --create --topic page_views        --partitions 3 --replication-factor 1
kafka topics --create --topic page_views_hourly --partitions 1 --replication-factor 1
kafka topics --create --topic page_views_dlq    --partitions 1 --replication-factor 1
kafka topics --describe --topic page_views
```

## Files

| File | Purpose |
|------|---------|
| [`producer.py`](producer.py) | Replays `events.jsonl` in **arrival** order, keyed by `customer_id`. `--rate` slows it down; `--bad-rate` adds malformed messages. |
| [`consumer.py`](consumer.py) | A plain consumer that prints partition and offset, commits manually, and can `--crash-after` N messages. |
| [`stream_processor.py`](stream_processor.py) | **The exercise.** Validates, deduplicates, and counts views per page in 1-hour event-time windows. Three functions are left for you. |
| [`check_hourly.py`](check_hourly.py) | Compares the processor's output with the true counts from the source file. |
| [`solutions/stream_processor.py`](solutions/stream_processor.py) | Reference answer. |
| [`ci_smoke.py`](ci_smoke.py) | Runs the exercises against the broker and checks the results described below. CI uses it; it needs a running broker and deletes and re-creates the three topics. |

## Exercises

### 1. Topics, partitions and keys

```bash
python producer.py --bad-rate 0.01
kafka topics --describe --topic page_views
python consumer.py --group explore --max 20
```

1. The producer prints how many messages went to each partition. Why are they roughly equal but not identical?
2. All events for one customer go to the same partition. What does that guarantee, and what does it **not** guarantee about events for different customers?
3. The producer sets `enable.idempotence` and `acks=all`. What failure does each one protect against? (See the producer section of the [Kafka guide](../../docs/04-streaming/kafka-reference.md).)

### 2. Consumer groups and rebalancing

Open two terminals and start a consumer in each, with the same group:

```bash
python consumer.py --group split --quiet        # terminal 1
python consumer.py --group split --quiet        # terminal 2, a few seconds later
```

The first consumer prints `Assigned partitions: [0, 1, 2]`, then `[0, 1]` when the second joins and takes `[2]`. Stop the second one with Ctrl+C and the first one gets all three partitions again. While they run, check the group from a third terminal:

```bash
kafka consumer-groups --describe --group split       # CURRENT-OFFSET, LOG-END-OFFSET, LAG per partition
```

1. What happens if you start a **fourth** consumer in a group reading a 3-partition topic?
2. What does `LAG` measure, and why is it the main health metric for a consumer?

### 3. At-least-once delivery

```bash
python consumer.py --group crashdemo --crash-after 50     # exits without committing
python consumer.py --group crashdemo --max 5              # starts again from offset 0
```

The consumer only commits after processing, so a crash causes messages to be **delivered again**, never lost. This is at-least-once delivery. Which part of a pipeline must be designed for it? (See idempotent writes in [Ingestion & CDC](../../docs/02-processing/ingestion-cdc.md).)

### 4. Validate messages and use a dead-letter topic

Run the processor as provided:

```bash
python stream_processor.py
```

It crashes on the first malformed message, and because the offset was never committed, it would crash on the same message again after every restart. A message like this is called a *poison pill*.

Implement `validate()` so that invalid messages are returned with a reason instead of raising. The processor already sends them to `page_views_dlq`, with the reason in a message header. Then run it again with a new group:

```bash
python stream_processor.py --group ex4

# Count dead-letter messages by reason
kafka console-consumer --topic page_views_dlq --from-beginning --timeout-ms 5000 \
  --formatter-property print.headers=true --formatter-property print.value=false 2>/dev/null | sort | uniq -c
```

You should see about 100 each of `invalid_json`, `missing_fields` and `invalid_event_ts`. There are also many `too_late` messages, which exercise 5 explains. To see full messages, drop `print.value=false` and add `--max-messages 5`.

### 5. Event-time windows and watermarks

Implement `window_start()`, so each event is counted in its hour, and check the results:

```bash
python stream_processor.py --group ex5
python check_hourly.py
```

Many events are reported `too_late` and most windows are wrong, even though only about 2% of events really arrive late. The cause is `compute_watermark()`. It uses the latest event time across all partitions, but Kafka orders messages only **within** a partition. The consumer reads the three partitions at different speeds, so one partition's event time runs hours ahead of the others, and windows close before the slower partitions have delivered their events.

Fix `compute_watermark()` to use the **slowest** partition: the minimum of the latest event time per partition, and no watermark at all until every assigned partition has delivered an event. This is how Flink and Spark Structured Streaming combine watermarks from parallel inputs.

Before re-running, clear the output topics so `check_hourly.py` only sees the new results:

```bash
for t in page_views_hourly page_views_dlq; do kafka topics --delete --topic $t; done
for t in page_views_hourly page_views_dlq; do kafka topics --create --topic $t --partitions 1 --replication-factor 1; done
python stream_processor.py --group ex5-fixed
python check_hourly.py
```

With the fix, fewer than 100 events are `too_late`, and every other window matches exactly.

1. Clear the output topics again and run with `--lateness-hours 36 --group ex5-36h`. Now every window matches. What did you give up in exchange?
2. Late events here are sent to the dead-letter topic. Name two other ways a pipeline could handle them.

### 6. What a crash does to state

Clear the output topics, then run the reference processor, stop it partway through, and restart it:

```bash
python solutions/stream_processor.py --group ex6 --crash-after-batches 20   # results written, offsets not committed
python solutions/stream_processor.py --group ex6                            # rejoins after ~10 s
python check_hourly.py
```

Thousands of events are now **missing**, although offsets were only committed after results were written. The processor committed offsets for events whose counts were still in memory, in windows that hadn't been emitted yet. The crash erased those counts, and Kafka won't redeliver messages that were already committed.

1. Why does the restarted process wait about 10 seconds before it receives any partitions? (Look at `session.timeout.ms`.)
2. How do Flink checkpoints and Kafka Streams changelog topics prevent this loss? (See the [Flink guide](../../docs/04-streaming/flink-reference.md).)
3. Without a framework, one fix is to commit only offsets whose events are in windows that have already been emitted. What would that cost?

## Going further

- Replace the in-memory `seen_ids` set with a bounded structure, for example one that forgets IDs older than the watermark. What does it cost in correctness?
- Rewrite the hourly count in Flink SQL with a `TUMBLE` window and a `WATERMARK` clause, and compare it with the Python version.
- Add a schema to the events with a schema registry and Avro or Protobuf, then send a message with an incompatible change.
- Increase `page_views` to 6 partitions (`kafka topics --alter --topic page_views --partitions 6`). What happens to the key-to-partition mapping for existing customers?

## Clean up

```bash
docker compose down -v      # stops the broker and deletes all topics
```
