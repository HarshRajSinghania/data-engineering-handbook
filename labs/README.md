# Hands-on Labs

Five labs and two capstone projects that turn the guides into practice. They all use one realistic e-commerce dataset, which has the problems real pipelines face: duplicate records, missing keys, invalid values, late and duplicate events, and data that changes after it is loaded.

Every lab runs on a laptop, with no cloud account. Each has exercises you run as-is and then complete, and reference solutions that were run end to end against the generated data.

| Lab | You build | Runs on | Time | Guides |
|-----|-----------|---------|------|--------|
| [01 — SQL Analytics](01-sql-analytics/README.md) | Ten analytical and data quality queries: deduplication, window functions, funnels, sessionization | Python + DuckDB | 60–90 min | [SQL](../docs/00-foundations/sql-reference.md), [DuckDB & Polars](../docs/02-processing/duckdb-polars.md) |
| [02 — dbt Transformations](02-dbt-transformations/README.md) | A layered dbt project with data and unit tests, an incremental model and an SCD Type 2 snapshot | Python + dbt + DuckDB | 90–120 min | [dbt](../docs/02-processing/dbt-reference.md), [Data Modeling](../docs/01-storage/data-modeling.md) |
| [03 — Spark Lakehouse](03-spark-lakehouse/README.md) | A bronze/silver/gold pipeline on Delta Lake with `MERGE`, time travel, schema evolution and compaction | Python + PySpark + Java 17 | 90–120 min | [PySpark](../docs/02-processing/pyspark-reference.md), [Databricks](../docs/02-processing/databricks-reference.md) |
| [04 — Kafka Streaming](04-kafka-streaming/README.md) | A stream processor with a dead-letter topic, deduplication, event-time windows and watermarks | Docker + Python | 90–120 min | [Kafka](../docs/04-streaming/kafka-reference.md), [Flink](../docs/04-streaming/flink-reference.md) |
| [05 — Airflow Orchestration](05-airflow-orchestration/README.md) | A daily DAG with backfills, an idempotent load, a quality gate, pools and asset-driven scheduling | Docker | 90–120 min | [Airflow](../docs/03-orchestration/airflow-reference.md), [Data Quality](../docs/05-quality-governance/data-quality.md) |
| [06 — Capstone: Dagster pipeline](06-capstone-ecommerce/README.md) | An end-to-end pipeline: raw → staging → marts, blocking quality checks, quarantine tables and a dashboard | Python + Dagster + DuckDB | 90–120 min | [Dagster](../docs/03-orchestration/dagster-reference.md), [Data Quality](../docs/05-quality-governance/data-quality.md) |
| [07 — Capstone: Docs RAG](07-docs-rag/README.md) | Chunking, a BM25 index and a retrieval eval (hit@k, MRR) over the handbook's own guides | Python only | 60–90 min | [RAG](../docs/07-ai/rag.md), [Evals](../docs/07-ai/eval-and-evals.md) |

The labs can be done in any order. Lab 01 is the best introduction to the dataset, and Labs 02 and 03 build the same daily revenue numbers with two different engines, so you can compare them.

## Requirements

- Python 3.10 or later. Each lab has its own `requirements.txt`, so use a separate virtual environment per lab.
- Labs 04 and 05: Docker with Compose v2, and about 1.5 GB of free memory.
- Lab 03: Java 17 or 21.
- Or skip the setup: open the repository in a [dev container or GitHub Codespace](../.devcontainer/devcontainer.json), which has Python 3.12, Java 17 and Docker ready. Give it at least 4 CPUs and 8 GB of memory for Labs 04 and 05.
- Windows: use WSL2. Spark and the shell commands in the lab instructions assume Linux or macOS.

## The dataset

[`data/generate.py`](data/generate.py) writes a deterministic dataset to `data/output/`. The same seed always gives the same data, so your results can be compared with the numbers in the lab instructions.

```bash
python data/generate.py                                    # 30 days from 2024-03-01
python data/generate.py --days 365 --orders-per-day 5000   # a larger dataset for performance work
```

| File | Rows (default) | Problems included |
|------|---------------:|-------------------|
| `customers.csv` | 3,000 | Some customers change country (for slowly changing dimensions) |
| `products.csv` | 20 | |
| `orders.csv` | ~6,300 | Duplicate order versions, inconsistent status casing, missing `customer_id` |
| `order_items.csv` | ~15,000 | Negative quantities, unknown `product_id`s |
| `events.jsonl` | ~30,300 | Duplicate deliveries, events that arrive hours late |

[`data/simulate_changes.py`](data/simulate_changes.py) applies one day of changes on top: new orders, a late cancellation of an already-loaded order, and three customers moving country. Labs 02 and 03 use it to show how incremental models, snapshots and `MERGE` handle changing data.

## Conventions

- `exercises.*` or the provided DAG or processor always runs before you change anything, and the TODOs mark what to write.
- Solutions are in `solutions.*` or `solutions/`. Compare after each attempt, not before.
- Every lab is checked in CI. Labs 04 and 05 include a `ci_smoke.py` that runs the exercises against the live broker or Airflow and asserts the outcomes the README describes, so the instructions cannot drift from what the tools do.
- Generated data, databases and build output are ignored by Git (see [`.gitignore`](.gitignore)), so you can reset a lab by deleting them and regenerating.
