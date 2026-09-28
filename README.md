# Sarang's Data Engineering Handbook

All of my data engineering knowledge in one place: concepts, tools, and production patterns,
from first query to production pipelines, plus the AI/LLM engineering that now sits alongside them.

Every guide goes **Basic → Intermediate → Advanced** with working code examples.

**→ Read it as a website: [sarangambekar1997.github.io/data-engineering-handbook](https://sarangambekar1997.github.io/data-engineering-handbook/)** — searchable, with navigation and dark mode

**→ Or start here on GitHub: [Full index & learning paths](docs/README.md)**

**→ Run the labs without local setup:** [open in a GitHub Codespace](https://codespaces.new/sarangambekar1997/data-engineering-handbook) (Python, Java and Docker included)

---

## Layout

| Folder | Covers |
|--------|--------|
| [`docs/00-foundations`](docs/00-foundations) | DE concepts, SQL, Python, Linux & Bash, Git |
| [`docs/01-storage`](docs/01-storage) | Cloud storage, data modeling, Snowflake, BigQuery, Redshift, Azure & Fabric, NoSQL and operational stores, Delta Lake, Hudi, Apache Iceberg, real-time analytics databases (ClickHouse, Druid, Pinot) |
| [`docs/02-processing`](docs/02-processing) | Data ingestion & CDC, DuckDB & Polars, PySpark, Databricks, Trino, dbt, semantic layer, BI tools (Superset, Metabase) |
| [`docs/03-orchestration`](docs/03-orchestration) | Apache Airflow, Dagster, Prefect |
| [`docs/04-streaming`](docs/04-streaming) | Apache Kafka, Apache Flink, Apache Beam and Dataflow, streaming SQL (RisingWave, Materialize) |
| [`docs/05-quality-governance`](docs/05-quality-governance) | Data quality, governance, lineage, contracts, data catalogs (DataHub, OpenMetadata), security & privacy, pipeline observability, DataOps and incident response |
| [`docs/06-infrastructure`](docs/06-infrastructure) | Docker, Kubernetes, Terraform, testing and CI/CD for data pipelines |
| [`docs/07-ai`](docs/07-ai) | Prompting, LLM APIs, embeddings, RAG, vector DBs, agents, MCP and text-to-SQL, evals, MLflow, fine-tuning |
| [`docs/08-architecture`](docs/08-architecture) | System design, choosing a stack, cost optimization |
| [`docs/09-interviews`](docs/09-interviews) | Interview roadmap, SQL patterns, system design case studies |
| [`docs/99-reference`](docs/99-reference) | Glossary |
| [`labs`](labs) | Hands-on labs (SQL, dbt, Spark & Delta Lake, Kafka, Airflow, data quality, Iceberg, change data capture) and two capstone projects |
| [`docs/projects`](docs/projects/index.md) | Capstone projects: an end-to-end pipeline and a RAG system with evals |

Folders are numbered roughly in learning order. New topics go in the folder that matches
where they sit in a pipeline, and a new top-level area gets the next free number.

## Where to start

| If you are… | Read |
|-------------|------|
| New to data engineering | [DE Concepts](docs/00-foundations/de-concepts.md) → [SQL](docs/00-foundations/sql-reference.md) → [Python](docs/00-foundations/python-reference.md) |
| Focused on the warehouse | A warehouse ([Snowflake](docs/01-storage/snowflake-reference.md) / [BigQuery](docs/01-storage/bigquery-reference.md) / [Redshift](docs/01-storage/redshift-reference.md)) → [dbt](docs/02-processing/dbt-reference.md) → [Data Quality](docs/05-quality-governance/data-quality.md) |
| Focused on big data | [PySpark](docs/02-processing/pyspark-reference.md) → [Databricks](docs/02-processing/databricks-reference.md) → [Kafka](docs/04-streaming/kafka-reference.md) |
| Building with LLMs | [Prompt Engineering](docs/07-ai/prompt-engineering.md) → [LLM APIs](docs/07-ai/llm-apis.md) → [RAG](docs/07-ai/rag.md) |
| Preparing for design interviews | [System Design](docs/08-architecture/system-design.md) → [Ingestion & CDC](docs/02-processing/ingestion-cdc.md) → [Data Modeling](docs/01-storage/data-modeling.md) |
| Looking up a term | [Glossary](docs/99-reference/glossary.md) |

The full learning paths, the "when should I use what" tables, and the cheat sheets are in the [index](docs/README.md).

## Hands-on labs

[Eight labs and two capstone projects](labs/README.md) turn the guides into practice on one realistic e-commerce dataset, with its duplicates, missing keys, late events and changing records. Each runs locally without a cloud account, and each has runnable exercises and reference solutions that were run end to end.

| Lab | Runs on |
|-----|---------|
| [01 — SQL Analytics](labs/01-sql-analytics/README.md) | DuckDB |
| [02 — dbt Transformations](labs/02-dbt-transformations/README.md) | dbt + DuckDB |
| [03 — Spark Lakehouse](labs/03-spark-lakehouse/README.md) | PySpark + Delta Lake |
| [04 — Kafka Streaming](labs/04-kafka-streaming/README.md) | Docker (Kafka) |
| [05 — Airflow Orchestration](labs/05-airflow-orchestration/README.md) | Docker (Airflow) |

## Writing a new guide

Copy [`docs/_template.md`](docs/_template.md). Every guide uses the same sections:

1. Front matter with `verified: YYYY-MM-DD` (see [how the handbook is maintained](docs/maintenance.md))
2. Prerequisites / Related links at the top, plus a Practice link when a lab covers the topic
3. Overview (the problem the topic solves and how, before any code)
4. Table of contents split into Basic / Intermediate / Advanced
5. Content sections with runnable code
6. Common Pitfalls
7. Cheat Sheet
8. Interview Questions
9. Further Reading
10. Next / Back navigation at the bottom

Then add the guide to [`docs/README.md`](docs/README.md), to any learning path it belongs in, and to the `nav` section of [`mkdocs.yml`](mkdocs.yml).

## Previewing the site locally

```bash
pip install -r requirements-docs.txt
mkdocs serve                 # live preview at http://127.0.0.1:8000
mkdocs build --strict        # the same check CI runs: fails on broken links or anchors
python tools/check_code_blocks.py   # every python/json/yaml block must parse
python tools/check_model_ids.py     # no retired model IDs
python tools/check_freshness.py     # review dates and lab versions are valid
```

Pull requests that touch `docs/` are built in strict mode by CI and every code block is parsed; merges to `main` deploy the site to GitHub Pages. Pull requests that touch `labs/` run every lab end to end. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Community

Corrections, new topics and reviews are welcome, and there are small [good first issues](https://github.com/sarangambekar1997/data-engineering-handbook/issues?q=is%3Aopen+label%3A%22good+first+issue%22) for a first contribution.

- [Contributing](CONTRIBUTING.md): the first-contribution path and the writing rules
- [Roadmap](docs/roadmap.md): what is planned and where help is wanted
- [How it is maintained](docs/maintenance.md): what the review dates mean
- [Discussions](https://github.com/sarangambekar1997/data-engineering-handbook/discussions): questions and ideas
- [Changelog](CHANGELOG.md): what changed in each release

To cite the handbook, use the **Cite this repository** button on GitHub, which reads [`CITATION.cff`](CITATION.cff).

## License

[MIT](LICENSE) © Sarang Ambekar
