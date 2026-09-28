# Data Platform Engineer Path

> From containers and cloud storage to streaming, orchestration, lakehouse tables and the operations that keep them running.

**Who it is for:** you build and run the systems that move, store and process data, and you are accountable for their reliability and cost.

**Labs in this path:** 10, 04, 05, 03, 09 and the Lab 06 capstone, about 9 to 12 hours in total. Labs 04, 05 and 10 need Docker. The [labs](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs) need no cloud account.

## Stage 1 — Working foundations

*Goal: be fluent in the tools everything else runs on.*

- **Read:** [Linux and Bash](../00-foundations/linux-bash.md), [Git](../00-foundations/git-for-de.md), [Docker](../06-infrastructure/docker-reference.md), [Cloud Storage](../01-storage/cloud-storage.md) and [Terraform](../06-infrastructure/terraform-for-de.md).
- **Also:** [DE Concepts](../00-foundations/de-concepts.md) for the vocabulary, if any term is new.

**Checkpoint:** write a Compose file for a service with a health check and a volume, and explain why the health check matters for the services that depend on it.

## Stage 2 — Getting data in

*Goal: move data from operational systems into the platform without losing or duplicating it.*

- **Read:** [Data Ingestion and CDC](../02-processing/ingestion-cdc.md), then [Apache Kafka](../04-streaming/kafka-reference.md).
- **Do:** [Lab 04, Kafka Streaming](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/04-kafka-streaming) (90–120 minutes): dead-letter topics, deduplication, event-time windows and watermarks.
- **Do:** [Lab 10, CDC with Debezium](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/10-cdc-debezium) (90–120 minutes): Postgres to Kafka, applying changes idempotently through duplicates, reordering and restarts.

**Checkpoint:** explain what a replication slot is, what happens to the source database when a connector stays down for a day, and how you would monitor it.

## Stage 3 — Orchestration

*Goal: schedule, retry and backfill pipelines safely.*

- **Read:** [Apache Airflow](../03-orchestration/airflow-reference.md), then [Dagster](../03-orchestration/dagster-reference.md) or [Prefect](../03-orchestration/prefect-reference.md) for a second model.
- **Do:** [Lab 05, Airflow Orchestration](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/05-airflow-orchestration) (90–120 minutes): backfills, an idempotent load, a quality gate, pools and asset scheduling.

**Checkpoint:** a task failed halfway through a load. Explain what you clear, what you rerun, and why a rerun cannot duplicate data.

## Stage 4 — Processing and lakehouse tables

*Goal: process data at scale and store it in tables with transactions.*

- **Read:** [PySpark](../02-processing/pyspark-reference.md), [Delta Lake](../01-storage/delta-lake.md) and [Apache Iceberg](../01-storage/apache-iceberg.md). [Trino](../02-processing/trino-federation.md) covers interactive SQL over the same tables.
- **Do:** [Lab 03, Spark Lakehouse](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/03-spark-lakehouse) (90–120 minutes), then [Lab 09, Iceberg Lakehouse](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/09-iceberg-lakehouse) (90–120 minutes). They build the same pipeline in two table formats, so you can compare them.
- **Run it on shared infrastructure:** [Kubernetes for Data Workloads](../06-infrastructure/kubernetes-for-de.md).

**Checkpoint:** a `MERGE` of 0.1% of a table's rows rewrote the whole table. Explain why, and name two ways to reduce the cost.

## Stage 5 — Reliability and operations

*Goal: prove changes are safe, see failures early, and respond well.*

- **Read:** [Testing and CI/CD](../06-infrastructure/testing-cicd.md), [Pipeline Observability](../05-quality-governance/pipeline-observability.md), [DataOps](../05-quality-governance/dataops-operations.md), [Data Security and Privacy](../05-quality-governance/data-security-privacy.md) and [Cost Optimization](../08-architecture/cost-optimization.md).

**Checkpoint:** write a runbook for one alert (a pipeline that is late), including who is paged, what they check first, and when they escalate.

## Stage 6 — Architecture

*Goal: turn requirements into a design, and defend the trade-offs.*

- **Read:** [System Design](../08-architecture/system-design.md), then [Choosing a Stack](../08-architecture/choosing-a-stack.md).
- **Practise:** the [system design case studies](../09-interviews/system-design-case-studies.md).
- **Do:** [Lab 06, Capstone: Dagster pipeline](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/06-capstone-ecommerce) (90–120 minutes) to combine ingestion, modelling, quality gates and orchestration.

**You are done when** you can take a set of requirements (freshness, volume, budget, team size), sketch a platform for them, and say which failure you expect first and how you would notice it.

## Going further

- Add streaming analytics with [Apache Flink](../04-streaming/flink-reference.md) or [Streaming SQL](../04-streaming/streaming-sql.md), and serve results from a [real-time analytics database](../01-storage/realtime-olap.md).
- Read [Apache Beam and Dataflow](../04-streaming/beam-dataflow.md) for the unified batch and streaming model.
- Add the data-facing skills with the [analytics engineer path](analytics-engineer.md).
