# Data Engineering Bible

A comprehensive reference for data engineers — from first query to production pipelines.
Each guide follows a **Basic → Intermediate → Advanced** progression with real, working code examples.

---

## Reference Guides

### Foundations

| Guide | What you'll learn |
|-------|------------------|
| [SQL Reference](00-foundations/sql-reference.md) | SELECT, filtering, joins, aggregates, CTEs, window functions, indexes, transactions |
| [Python for DE](00-foundations/python-reference.md) | Data types, OOP, generators, decorators, pandas, APIs, DE patterns |
| [Linux & Bash](00-foundations/linux-bash.md) | Filesystem, text processing, bash scripting, cron, SSH, DE workflows |
| [Git for DE](00-foundations/git-for-de.md) | Branching, merging, dbt CI/CD, git hooks, team workflows |
| [Cloud Storage](01-storage/cloud-storage.md) | S3, GCS, ADLS Gen2, medallion layout, partitioning, IAM, Python SDKs |

### Processing & Compute

| Guide | What you'll learn |
|-------|------------------|
| [DuckDB & Polars](02-processing/duckdb-polars.md) | Single-node SQL and DataFrame analytics, larger-than-memory processing, object storage, interoperability |
| [PySpark Reference](02-processing/pyspark-reference.md) | DataFrames, transformations, window functions, UDFs, streaming, optimization |
| [Docker for DE](06-infrastructure/docker-reference.md) | Images, Dockerfile, volumes, networking, Docker Compose, Airflow/Spark in Docker |
| [Databricks](02-processing/databricks-reference.md) | Delta Lake, Auto Loader, DLT, Unity Catalog, Workflows, Delta vs Iceberg vs Hudi |

### Orchestration & Streaming

| Guide | What you'll learn |
|-------|------------------|
| [Apache Airflow](03-orchestration/airflow-reference.md) | DAGs, operators, XComs, sensors, TaskFlow API, dynamic DAGs, CI/CD |
| [Apache Kafka](04-streaming/kafka-reference.md) | Topics, producers, consumers, Schema Registry, Kafka Connect, Kafka Streams, DLQ patterns |
| [Apache Flink](04-streaming/flink-reference.md) | Stateful stream processing, event time and watermarks, windows, stream joins, checkpoints, Flink SQL |
| [Data Ingestion & CDC](02-processing/ingestion-cdc.md) | API, file, and database ingestion; incremental loads; CDC with Debezium; applying changes with MERGE; build vs buy |

### Storage & Transformation

| Guide | What you'll learn |
|-------|------------------|
| [Snowflake Reference](01-storage/snowflake-reference.md) | Architecture, virtual warehouses, semi-structured data, streams & tasks, RBAC |
| [dbt Reference](02-processing/dbt-reference.md) | Models, materializations, tests, macros, incremental models, snapshots, CI/CD |
| [BigQuery](01-storage/bigquery-reference.md) | Serverless architecture, loading, partitioning and clustering, nested data, pricing and cost control, security |
| [Amazon Redshift](01-storage/redshift-reference.md) | Provisioned vs serverless, distribution and sort keys, COPY/UNLOAD, Spectrum, SUPER, workload management |
| [Apache Iceberg](01-storage/apache-iceberg.md) | Open table format, hidden partitioning, schema evolution, time travel, ACID, AWS Glue/Athena |

### Quality & Observability

| Guide | What you'll learn |
|-------|------------------|
| [Data Quality](05-quality-governance/data-quality.md) | SQL checks, Great Expectations, dbt tests, anomaly detection, data contracts, alerting |
| [Data Governance & Lineage](05-quality-governance/governance-lineage.md) | Catalogs, ownership, classification, access models, lineage and OpenLineage, contracts, retention and deletion |

### AI & Machine Learning

| Guide | What you'll learn |
|-------|------------------|
| [Prompt Engineering](07-ai/prompt-engineering.md) | Zero-shot, few-shot, CoT, structured output, chaining, versioning |
| [LLM APIs & SDKs](07-ai/llm-apis.md) | Anthropic Claude & OpenAI — streaming, tool use, vision, caching, batching |
| [Embeddings](07-ai/embeddings.md) | Generating embeddings, cosine similarity, chunking, semantic search, clustering |
| [RAG](07-ai/rag.md) | Build retrieval-augmented generation pipelines, hybrid search, re-ranking, evaluation |
| [Vector Databases](07-ai/vector-databases.md) | pgvector, Pinecone, Chroma, Weaviate — indexing, filtering, multi-tenancy |
| [AI Agents & Tool Use](07-ai/ai-agents.md) | Agentic loops, tool definitions, ReAct, multi-agent systems, human-in-the-loop |
| [LangChain & LlamaIndex](07-ai/langchain-llamaindex.md) | RAG chains, agents, LCEL, custom retrievers, LangSmith tracing |
| [Eval & Evals](07-ai/eval-and-evals.md) | Unit tests for LLMs, LLM-as-judge, RAGAS, regression testing, eval-driven development |
| [MLflow](07-ai/mlflow.md) | Experiment tracking, model registry, serving, custom models, DE integration |
| [Claude Code](07-ai/claude-code.md) | CLI setup, CLAUDE.md, MCP servers, hooks, skills, CI/headless mode, DE workflows |
| [Fine-Tuning LLMs](07-ai/fine-tuning.md) | LoRA/PEFT, full fine-tuning vs RAG decision, Hugging Face + OpenAI fine-tuning API |
| [AI Observability](07-ai/ai-observability.md) | Cost/latency/quality monitoring, LangSmith, Langfuse, OpenTelemetry, RAG tracing |
| [Local LLMs](07-ai/local-llms.md) | Ollama, vLLM, Hugging Face, quantization (4-bit/fp16), local RAG, hardware guide |

### Infrastructure

| Guide | What you'll learn |
|-------|------------------|
| [Terraform for DE](06-infrastructure/terraform-for-de.md) | IaC for S3, IAM, Snowflake, Databricks, MWAA Airflow — modules, remote state, CI patterns |

### Conceptual & Reference

| Guide | What you'll learn |
|-------|------------------|
| [DE Concepts](00-foundations/de-concepts.md) | OLTP/OLAP, batch vs streaming, lakehouse, medallion architecture, file formats, ETL/ELT |
| [Data Modeling](01-storage/data-modeling.md) | Star schema, SCDs, fact/dim design, snowflake schema, OBT, Data Vault, dbt layers |
| [Glossary](99-reference/glossary.md) | Definitions for every term used across all guides — one place to look things up |

### Architecture

| Guide | What you'll learn |
|-------|------------------|
| [Cost Optimization](08-architecture/cost-optimization.md) | Unit economics, attribution, spend monitoring per platform, compute/query/storage optimization, guardrails |
| [Data Engineering System Design](08-architecture/system-design.md) | Requirements, capacity estimation, architecture patterns, batch vs streaming, reliability, security, cost, worked designs |

---

## Hands-on Labs

Practise with [five labs](https://github.com/sarangambekar1997/de-workspace/tree/main/labs) that run locally, all on one realistic e-commerce dataset:

| Lab | Practise |
|-----|----------|
| [01 — SQL Analytics](https://github.com/sarangambekar1997/de-workspace/tree/main/labs/01-sql-analytics) | Deduplication, window functions, funnels, sessionization (DuckDB) |
| [02 — dbt Transformations](https://github.com/sarangambekar1997/de-workspace/tree/main/labs/02-dbt-transformations) | Layered models, data and unit tests, incremental models, snapshots |
| [03 — Spark Lakehouse](https://github.com/sarangambekar1997/de-workspace/tree/main/labs/03-spark-lakehouse) | Medallion pipeline on Delta Lake: `MERGE`, time travel, schema evolution |
| [04 — Kafka Streaming](https://github.com/sarangambekar1997/de-workspace/tree/main/labs/04-kafka-streaming) | Consumer groups, dead-letter topics, event-time windows, watermarks |
| [05 — Airflow Orchestration](https://github.com/sarangambekar1997/de-workspace/tree/main/labs/05-airflow-orchestration) | Backfills, idempotent loads, quality gates, pools, asset scheduling |

---

## Learning Paths

### Path 1: Complete beginner → job-ready

1. [DE Concepts](00-foundations/de-concepts.md) — understand the landscape
2. [Glossary](99-reference/glossary.md) — reference when you hit an unfamiliar term
3. [SQL Reference](00-foundations/sql-reference.md) — the universal language of data
4. [Python for DE](00-foundations/python-reference.md) — scripting and automation
5. [Data Modeling](01-storage/data-modeling.md) — design data structures that scale
6. [Linux & Bash](00-foundations/linux-bash.md) — work in production environments
7. [Git for DE](00-foundations/git-for-de.md) — collaborate and ship safely
8. [Cloud Storage](01-storage/cloud-storage.md) — store and retrieve data at scale
9. [Docker for DE](06-infrastructure/docker-reference.md) — package and run anything
10. [Terraform for DE](06-infrastructure/terraform-for-de.md) — provision infra as code
11. [Data Ingestion & CDC](02-processing/ingestion-cdc.md) — get data in reliably
12. [Data Engineering System Design](08-architecture/system-design.md) — put it all together

### Path 2: Warehouse & transformation focus

1. [SQL Reference](00-foundations/sql-reference.md)
2. A cloud warehouse: [Snowflake](01-storage/snowflake-reference.md), [BigQuery](01-storage/bigquery-reference.md), or [Amazon Redshift](01-storage/redshift-reference.md)
3. [dbt Reference](02-processing/dbt-reference.md)
4. [Data Quality](05-quality-governance/data-quality.md)
5. [Data Governance & Lineage](05-quality-governance/governance-lineage.md)
6. [Git for DE](00-foundations/git-for-de.md) — CI/CD section
7. [Cost Optimization](08-architecture/cost-optimization.md)

### Path 3: Spark & big data focus

1. [DE Concepts](00-foundations/de-concepts.md)
2. [DuckDB & Polars](02-processing/duckdb-polars.md) — single-node first
3. [PySpark Reference](02-processing/pyspark-reference.md)
4. [Databricks](02-processing/databricks-reference.md)
5. [Cloud Storage](01-storage/cloud-storage.md)
6. [Apache Kafka](04-streaming/kafka-reference.md)

### Path 4: Streaming & real-time

1. [DE Concepts](00-foundations/de-concepts.md) — streaming section
2. [Apache Kafka](04-streaming/kafka-reference.md)
3. [Apache Flink](04-streaming/flink-reference.md)
4. [Data Ingestion & CDC](02-processing/ingestion-cdc.md) — CDC with Debezium
5. [PySpark Reference](02-processing/pyspark-reference.md) — Structured Streaming section
6. [Databricks](02-processing/databricks-reference.md) — Auto Loader and DLT sections
7. [Data Quality](05-quality-governance/data-quality.md) — DQ in streaming pipelines

### Path 5: AI & LLM engineering

1. [Prompt Engineering](07-ai/prompt-engineering.md) — foundation for everything
2. [LLM APIs & SDKs](07-ai/llm-apis.md) — hands-on from day 1
3. [Embeddings](07-ai/embeddings.md) — prerequisite for RAG
4. [RAG](07-ai/rag.md) — most in-demand AI skill right now
5. [Vector Databases](07-ai/vector-databases.md) — implement RAG at scale
6. [AI Agents & Tool Use](07-ai/ai-agents.md) — where the field is heading
7. [LangChain & LlamaIndex](07-ai/langchain-llamaindex.md) — practical orchestration
8. [Eval & Evals](07-ai/eval-and-evals.md) — measure and improve quality
9. [MLflow](07-ai/mlflow.md) — bring it back to the data pipeline
10. [Claude Code](07-ai/claude-code.md) — use AI to build AI things
11. [Fine-Tuning LLMs](07-ai/fine-tuning.md) — when RAG isn't enough
12. [AI Observability](07-ai/ai-observability.md) — monitor production LLM apps
13. [Local LLMs](07-ai/local-llms.md) — run models without the API bill

---

## Quick Reference

### When should I use what?

| Scenario | Tool |
|----------|------|
| Ad-hoc data exploration | SQL |
| Scheduled batch pipeline | Airflow + PySpark or dbt |
| Real-time event processing | Kafka + PySpark Structured Streaming |
| Cloud data warehouse | Snowflake + dbt |
| Delta Lake / Lakehouse | Databricks |
| Containerized pipeline | Docker Compose |
| CI/CD for transformations | dbt + GitHub Actions |
| Data quality enforcement | dbt tests + Great Expectations |
| Build an LLM app on private data | RAG + vector DB |
| LLM with external actions | AI agents + tool use |
| Measure LLM app quality | Evals + LLM-as-judge |
| Track ML experiments | MLflow |
| Customize a model for your domain | Fine-tuning (LoRA/PEFT) |
| Monitor LLM app in production | AI Observability (LangSmith/Langfuse) |
| Run models privately / offline | Local LLMs (Ollama/vLLM) |
| Open table format for big data | Apache Iceberg |

### File format cheat sheet

| Format | Use when |
|--------|----------|
| **Parquet** | Columnar analytics, Spark, large-scale reads |
| **Avro** | Kafka messages, schema evolution, row-based streaming |
| **Delta** | Lakehouse tables with ACID, time travel, MERGE (Databricks-native) |
| **Iceberg** | Open lakehouse tables — multi-engine (Spark, Flink, Trino, Athena), hidden partitioning |
| **JSON** | Raw landing zone, semi-structured, API payloads |
| **CSV** | External hand-offs, small seeds, human-readable exports |
| **ORC** | Hive/Hadoop ecosystems (prefer Parquet elsewhere) |

### Materializations comparison

| Type | When to use |
|------|-------------|
| `view` | Lightweight, always fresh, no storage cost |
| `table` | Expensive query that many models read |
| `incremental` | Large tables where only new/changed rows matter |
| `ephemeral` | Staging logic used once, not queried directly |

### Delivery guarantees

| Guarantee | Meaning |
|-----------|---------|
| At-most-once | May lose messages, never duplicate |
| At-least-once | May duplicate, never lose |
| Exactly-once | No duplicates, no loss (hardest to achieve) |

---

## Concepts at a Glance

### Medallion Architecture

```
Bronze (raw)      → Silver (cleaned, conformed)      → Gold (aggregated, business-ready)
Exact copy          Deduped, typed, validated           Fact/dim tables, aggregates, KPIs
of source data      Joined where needed                 Consumed by BI / ML / APIs
```

### Data Warehouse vs Data Lake vs Lakehouse

| | Data Warehouse | Data Lake | Lakehouse |
|-|----------------|-----------|-----------|
| **Storage** | Proprietary (Snowflake, BigQuery) | Object storage (S3, GCS, ADLS) | Object storage |
| **Format** | Vendor-specific | Any (Parquet, CSV, JSON…) | Open (Delta, Iceberg, Hudi) |
| **Schema** | Schema-on-write | Schema-on-read | Both |
| **ACID** | Yes | No | Yes (with Delta/Iceberg) |
| **Cost** | Higher compute | Lower storage | Balanced |
| **Examples** | Snowflake, Redshift | S3 + Glue | Databricks, Delta Lake |

### ETL vs ELT

```
ETL (traditional):  Extract → Transform → Load      (transform before loading)
ELT (modern):       Extract → Load → Transform      (load raw, transform in warehouse)
```

ELT is dominant today because cloud warehouses are cheap and powerful enough to handle transforms at scale.

---

## Resources

**Data Engineering**
- [dbt Documentation](https://docs.getdbt.com)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Databricks Documentation](https://docs.databricks.com)
- [Snowflake Documentation](https://docs.snowflake.com)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Great Expectations Documentation](https://docs.greatexpectations.io)

**AI & LLMs**
- [Anthropic API Documentation](https://platform.claude.com/docs/en/home)
- [OpenAI API Documentation](https://developers.openai.com/api/docs)
- [LangChain Documentation](https://python.langchain.com)
- [LlamaIndex Documentation](https://docs.llamaindex.ai)
- [MLflow Documentation](https://mlflow.org/docs/latest)
- [RAGAS Documentation](https://docs.ragas.io)
- [Voyage AI (Embeddings)](https://docs.voyageai.com)
