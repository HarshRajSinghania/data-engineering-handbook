# Analytics Engineer Path

> From SQL to tested, documented, dbt-modelled data that the business trusts, and the BI layer that serves it.

**Who it is for:** you write SQL, and you want the models everyone else builds dashboards on to be correct, tested and easy to change.

**Labs in this path:** 01, 02, 08 and the Lab 06 capstone, about 5 to 7 hours in total. The [labs](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs) run locally and need no cloud account.

## Stage 1 — SQL and modelling

*Goal: write correct analytical SQL and design tables that are easy to query.*

- **Read:** [SQL](../00-foundations/sql-reference.md), then [Data Modeling](../01-storage/data-modeling.md).
- **Do:** [Lab 01, SQL Analytics](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/01-sql-analytics) (60–90 minutes): deduplication, window functions, funnels and sessionisation on data with real defects.
- **Practise:** the [SQL interview patterns](../09-interviews/sql-interview-patterns.md), which are tested query patterns you will reuse in models.

**Checkpoint:** explain the grain of each table in Lab 01, and say which model choice (star schema or one big table) you would make for daily revenue, and why.

## Stage 2 — A warehouse

*Goal: know how one warehouse stores, prices and runs your queries.*

- **Read one of:** [Snowflake](../01-storage/snowflake-reference.md), [BigQuery](../01-storage/bigquery-reference.md), [Amazon Redshift](../01-storage/redshift-reference.md) or [Azure and Microsoft Fabric](../01-storage/azure-fabric.md).
- **Optional, locally:** [DuckDB and Polars](../02-processing/duckdb-polars.md) run the same SQL on your laptop.

**Checkpoint:** name the main cost driver of your warehouse and one change that reduces it. The [cost optimisation guide](../08-architecture/cost-optimization.md) collects them.

## Stage 3 — Transformation

*Goal: build a layered dbt project with tests, incremental models and snapshots.*

- **Read:** [dbt](../02-processing/dbt-reference.md), then [Semantic Layer and Metrics](../02-processing/semantic-layer-metrics.md).
- **Do:** [Lab 02, dbt Transformations](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/02-dbt-transformations) (90–120 minutes): staging and marts, data and unit tests, an incremental model and an SCD Type 2 snapshot.

**Checkpoint:** describe what happens to an incremental model and a snapshot when a source row changes after it was loaded.

## Stage 4 — Quality and change safety

*Goal: stop bad data before it reaches a dashboard, and ship changes without breaking consumers.*

- **Read:** [Data Quality](../05-quality-governance/data-quality.md), then [Testing and CI/CD for Data Pipelines](../06-infrastructure/testing-cicd.md).
- **Do:** [Lab 08, Data Quality Gates](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/08-data-quality) (60–90 minutes): expectation suites, severities, and a gate that blocks bad, stale and schema-changed data.

**Checkpoint:** for a column of your choice, decide which check blocks the pipeline and which only warns, and defend the threshold.

## Stage 5 — Serving and trust

*Goal: make the data findable, explained and safe to use.*

- **Read:** [BI Tools](../02-processing/bi-tools.md), [Data Governance and Lineage](../05-quality-governance/governance-lineage.md) and [Data Catalogs in Practice](../05-quality-governance/data-catalogs.md).
- **Then:** [Data Security and Privacy](../05-quality-governance/data-security-privacy.md), for row-level security and masking.

**Checkpoint:** a metric changed on a dashboard. List the three places you would look first, in order.

## Stage 6 — Run it

*Goal: schedule the models, watch them, and respond when they fail.*

- **Read:** [Dagster](../03-orchestration/dagster-reference.md) or [Airflow](../03-orchestration/airflow-reference.md), then [Pipeline Observability](../05-quality-governance/pipeline-observability.md) and [DataOps](../05-quality-governance/dataops-operations.md).
- **Do:** [Lab 06, Capstone: Dagster pipeline](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/06-capstone-ecommerce) (90–120 minutes): raw to marts with blocking quality checks, quarantine tables and a dashboard.

**You are done when** you can rebuild the capstone's revenue numbers with SQL and dbt, explain each quality gate, and say what you would page on.

## Going further

- Read [Choosing a Stack](../08-architecture/choosing-a-stack.md) before proposing a new tool.
- Add the [Airflow lab](https://github.com/sarangambekar1997/data-engineering-handbook/tree/main/labs/05-airflow-orchestration) for idempotent loads and backfills.
- Move towards platform work with the [data platform engineer path](platform-engineer.md).
