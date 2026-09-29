---
verified: 2026-09-29
---

# BigQuery Reference
> Google Cloud's serverless data warehouse — architecture, loading, partitioning, cost control, and security.

**Prerequisites:** [SQL](../00-foundations/sql-reference.md) · [Cloud Storage](cloud-storage.md)

**Related:** [Snowflake](snowflake-reference.md) · [Amazon Redshift](redshift-reference.md) · [Data Modeling](data-modeling.md) · [Cost Optimization](../08-architecture/cost-optimization.md) · [Glossary](../99-reference/glossary.md)

---

## Overview

**Challenge:** Running a warehouse usually means sizing clusters, tuning indexes, and paying for idle capacity. Teams want to query terabytes with SQL without managing infrastructure.

**Solution:** BigQuery is a serverless warehouse: storage and compute are separate, there are no clusters to manage, and each query is executed on a shared pool of compute units called **slots**. Compute is billed either per byte scanned (on-demand) or per slot capacity (editions with autoscaling), and storage is billed separately.

```
            SQL (console · bq CLI · client libraries · BI tools)
                               │
          Query engine (Dremel) — work split across thousands of slots
                               │
     Distributed shuffle ──── Colossus storage (columnar, replicated)
                               │
     Native tables · external tables (Cloud Storage, BigLake / Iceberg) · streaming ingestion
```

**Relevance to data engineering:** BigQuery's cost and performance depend almost entirely on how much data each query reads, so partitioning, clustering, and query discipline matter more than any server tuning.

```mermaid
flowchart LR
    Q["SQL query"] --> DR["Dremel execution<br/>thousands of slots"]
    DR <-->|"Jupiter network"| CS[("Colossus<br/>columnar storage")]
    L["Load, streaming,<br/>external tables"] --> CS
    DR --> R["Result<br/>billed by bytes scanned<br/>or slot time"]
```

---

**On this page**

**Basic**
- [Resource Hierarchy](#resource-hierarchy)
- [Querying](#querying)
- [Nested and Repeated Data](#nested-and-repeated-data)

**Intermediate**
- [Loading Data](#loading-data)
- [Partitioning and Clustering](#partitioning-and-clustering)
- [DML and MERGE](#dml-and-merge)
- [Python Client](#python-client)

**Advanced**
- [Pricing Models and Cost Control](#pricing-models-and-cost-control)
- [Performance](#performance)
- [Time Travel, Snapshots, and Clones](#time-travel-snapshots-and-clones)
- [Security and Governance](#security-and-governance)

**Reference**
- [Common Pitfalls](#common-pitfalls)
- [Cheat Sheet](#cheat-sheet)
- [Interview Questions](#interview-questions)
- [Further Reading](#further-reading)

---

## Resource Hierarchy

```
Organization
  └── Project (billing, quotas, IAM)
        └── Dataset (a location: US, EU, or a region; default table settings)
              └── Tables · views · materialized views · routines (UDFs, procedures) · models
```

- Fully qualified name: `` `project.dataset.table` ``
- A dataset's **location** is fixed at creation; queries can only join datasets in the same location
- Access is granted with IAM on the project, dataset, or table

```sql
CREATE SCHEMA IF NOT EXISTS analytics
OPTIONS (location = "EU", default_table_expiration_days = NULL);
```

---

## Querying

BigQuery uses GoogleSQL (standard SQL). Most of the [SQL guide](../00-foundations/sql-reference.md) applies directly.

```sql
-- Latest record per key with QUALIFY
SELECT *
FROM `my-project.analytics.orders`
WHERE order_date >= "2024-03-01"
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

-- Useful functions
SELECT
  DATE_TRUNC(order_date, MONTH)             AS month,
  SAFE_DIVIDE(SUM(refunds), SUM(revenue))   AS refund_rate,      -- NULL instead of divide-by-zero
  APPROX_COUNT_DISTINCT(customer_id)        AS approx_customers, -- fast, ~1% error
  COUNTIF(status = "cancelled")             AS cancelled
FROM `my-project.analytics.orders`
GROUP BY month;

-- Parameterized query (from client libraries)
SELECT * FROM `my-project.analytics.orders` WHERE order_date = @run_date;
```

**Before running an expensive query:** check the estimated bytes in the console, or run `bq query --dry_run`.

---

## Nested and Repeated Data

BigQuery stores nested (`STRUCT`) and repeated (`ARRAY`) fields natively — often better than joining child tables.

```sql
CREATE TABLE analytics.orders_nested (
  order_id    STRING,
  customer    STRUCT<id STRING, country STRING>,
  line_items  ARRAY<STRUCT<sku STRING, quantity INT64, price NUMERIC>>,
  order_ts    TIMESTAMP
);

-- Explode line items into rows
SELECT o.order_id, li.sku, li.quantity * li.price AS line_total
FROM analytics.orders_nested AS o,
     UNNEST(o.line_items) AS li
WHERE o.customer.country = "DE";

-- Aggregate back into an array
SELECT customer.id, ARRAY_AGG(order_id ORDER BY order_ts DESC LIMIT 5) AS last_orders
FROM analytics.orders_nested
GROUP BY customer.id;

-- Semi-structured JSON column
SELECT JSON_VALUE(properties, "$.campaign") AS campaign
FROM analytics.events;
```

---

## Loading Data

| Method | Use for | Notes |
|--------|---------|-------|
| Batch load jobs (`bq load`, `LOAD DATA`) | Files in Cloud Storage | Free to load (you pay for storage) |
| Storage Write API | Streaming and high-throughput programmatic writes | Exactly-once semantics with committed streams |
| External tables / BigLake | Query files in place (Parquet, ORC, Avro, CSV, JSON, Iceberg) | No load step; performance depends on file layout |
| BigQuery Data Transfer Service | Scheduled imports from SaaS and other clouds | Managed connectors |
| Datastream | CDC from operational databases | Near-real-time replication |

```sql
-- Load Parquet files from Cloud Storage
LOAD DATA INTO analytics.orders
FROM FILES (
  format = "PARQUET",
  uris   = ["gs://my-lake/orders/order_date=2024-03-15/*.parquet"]
);

-- External table over files in Cloud Storage
CREATE EXTERNAL TABLE raw.clickstream
OPTIONS (format = "PARQUET", uris = ["gs://my-lake/clickstream/*.parquet"]);
```

```bash
bq load --source_format=PARQUET analytics.orders "gs://my-lake/orders/order_date=2024-03-15/*.parquet"
bq load --source_format=CSV --skip_leading_rows=1 --autodetect raw.customers gs://my-lake/customers.csv
```

---

## Partitioning and Clustering

```sql
CREATE TABLE analytics.events (
  event_id    STRING NOT NULL,
  user_id     STRING,
  event_type  STRING,
  event_ts    TIMESTAMP,
  properties  JSON
)
PARTITION BY DATE(event_ts)                 -- one partition per day
CLUSTER BY user_id, event_type              -- up to 4 columns, sorted within each partition
OPTIONS (
  partition_expiration_days = 730,          -- drop partitions older than 2 years
  require_partition_filter  = TRUE          -- reject queries without a partition filter
);
```

| Partitioning type | Syntax | Use for |
|-------------------|--------|---------|
| Time-unit column | `PARTITION BY DATE(ts)` / `TIMESTAMP_TRUNC(ts, HOUR)` | Event and transaction data (most common) |
| Ingestion time | `PARTITION BY _PARTITIONDATE` | Data without a reliable event timestamp |
| Integer range | `PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 10000))` | Tables filtered by numeric ranges |

- **Partitioning** prunes whole partitions and makes bytes-scanned estimates accurate before a query runs
- **Clustering** sorts data within partitions so filters and aggregations on clustered columns read fewer blocks; BigQuery re-clusters automatically at no charge
- Keep partitions reasonably large (avoid hourly partitions on small tables); there is a per-table partition limit

---

## DML and MERGE

```sql
MERGE analytics.customers AS t
USING staging.customers_changes AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.is_deleted THEN DELETE
WHEN MATCHED THEN UPDATE SET email = s.email, country = s.country, updated_at = s.updated_at
WHEN NOT MATCHED AND NOT s.is_deleted THEN
  INSERT (customer_id, email, country, updated_at)
  VALUES (s.customer_id, s.email, s.country, s.updated_at);
```

- DML is billed by bytes scanned (on-demand) like queries — filter on the partition column in the `ON`/`WHERE` clause
- Prefer batching changes into periodic `MERGE`s over many small single-row updates
- Multi-statement transactions are supported: `BEGIN TRANSACTION; ... COMMIT TRANSACTION;`

---

## Python Client

```python
from google.cloud import bigquery

client = bigquery.Client(project="my-project")        # uses Application Default Credentials

job_config = bigquery.QueryJobConfig(
    query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", "2024-03-15")],
    maximum_bytes_billed=50 * 1024**3,                # fail instead of scanning more than 50 GiB
)
sql = """
    SELECT customer_id, SUM(amount) AS revenue
    FROM `my-project.analytics.orders`
    WHERE order_date = @run_date
    GROUP BY customer_id
"""
df = client.query(sql, job_config=job_config).to_dataframe()

# Load a DataFrame into a table (replace the day's partition)
load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(
    df, "my-project.analytics.daily_revenue$20240315", job_config=load_config
).result()
```

The `$YYYYMMDD` partition decorator with `WRITE_TRUNCATE` replaces a single partition — an idempotent daily load.

---

## Pricing Models and Cost Control

| Model | Billed on | Good for |
|-------|-----------|----------|
| On-demand | Bytes processed per query (first TiB per month free) | Unpredictable or light workloads |
| Capacity (editions: Standard, Enterprise, Enterprise Plus) | Slot-hours, with autoscaling and optional commitments | Steady, heavy workloads; predictable spend |
| Storage | Active vs long-term (not modified for 90 days, cheaper); logical or physical (compressed) billing | Every table |

Prices vary by region and change over time — check the current pricing page.

**Cost controls**
- `require_partition_filter = TRUE` on large partitioned tables
- `maximum_bytes_billed` on queries and jobs; custom quotas per user or project
- Select only needed columns — `SELECT *` reads every column
- Materialized views or summary tables for dashboards
- Monitor spend with `INFORMATION_SCHEMA.JOBS`:

```sql
SELECT user_email,
       COUNT(*)                                   AS queries,
       ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed
FROM `region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = "QUERY"
GROUP BY user_email
ORDER BY tib_billed DESC;
```

---

## Performance

- **Read less:** filter on partition and clustering columns; select only needed columns
- **Filter and aggregate early**, before joins; put the largest table first in joins so smaller tables can be broadcast
- **Avoid** `ORDER BY` without `LIMIT` on large results, self-joins that could be window functions, and JavaScript UDFs in hot paths
- **Pre-compute** with materialized views (incrementally maintained) or scheduled summary tables
- **BI Engine** caches data in memory for sub-second dashboard queries
- **Search indexes** speed up needle-in-haystack lookups on text/log columns
- **Inspect** the query execution graph (stages, slot time, bytes shuffled, spilled) to find bottlenecks

---

## Time Travel, Snapshots, and Clones

```sql
-- Query a table as it was an hour ago (time travel window: 2–7 days, default 7)
SELECT * FROM analytics.orders
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);

-- Restore after a bad write
CREATE OR REPLACE TABLE analytics.orders AS
SELECT * FROM analytics.orders
FOR SYSTEM_TIME AS OF TIMESTAMP("2024-03-15 06:00:00+00");

-- Read-only snapshot (pay only for changed data)
CREATE SNAPSHOT TABLE backups.orders_20240315 CLONE analytics.orders;

-- Writable, zero-copy clone for development
CREATE TABLE dev.orders CLONE analytics.orders;
```

After time travel expires, a 7-day **fail-safe** period allows recovery only through Google Cloud support.

---

## Security and Governance

| Control | Mechanism |
|---------|-----------|
| Dataset / table access | IAM roles (`roles/bigquery.dataViewer`, `dataEditor`, `jobUser`, ...) |
| Share query results, not base tables | Authorized views and authorized datasets |
| Row-level security | Row access policies |
| Column-level security | Policy tags (Data Catalog taxonomies) + dynamic data masking |
| Encryption | Google-managed by default; customer-managed keys (CMEK) available |
| Network perimeter | VPC Service Controls |
| Audit | Cloud Audit Logs (admin activity and data access) |

```sql
CREATE ROW ACCESS POLICY emea_only
ON analytics.orders
GRANT TO ("group:emea-analysts@example.com")
FILTER USING (region = "EMEA");
```

---

## Common Pitfalls

| Pitfall | Symptom | Fix |
|---------|---------|-----|
| `SELECT *` on wide tables | Large bills for small questions | Select only needed columns; `LIMIT` does **not** reduce bytes scanned |
| Queries without a partition filter | Full-table scans | `require_partition_filter`; filter directly on the partition column |
| Wrapping the partition column in a function | No pruning | Filter the raw column (`event_ts >= ...`) or partition by the expression you filter on |
| Many single-row DML statements | Slow pipelines, quota errors, high cost | Batch changes and `MERGE` periodically |
| Datasets in different locations | Joins fail | Choose locations deliberately; keep related datasets together |
| Legacy streaming inserts for high-volume ingestion | Higher cost, weaker guarantees | Storage Write API |
| Over-partitioning small tables | Partition limits and overhead | Partition by day/month only when tables are large; cluster instead |
| No cost guardrails | One exploratory query costs more than a month of pipelines | `maximum_bytes_billed`, quotas, dry runs, spend monitoring |
| Sharded tables (`events_20240315`, ...) | Slow wildcard queries, schema drift | One partitioned table |

---

## Cheat Sheet

| Task | Command / SQL |
|------|---------------|
| Estimate cost | `bq query --use_legacy_sql=false --dry_run 'SELECT ...'` |
| Run a query | `bq query --use_legacy_sql=false 'SELECT ...'` |
| Show schema | `bq show --schema --format=prettyjson project:dataset.table` |
| Load files | `bq load --source_format=PARQUET dataset.table "gs://bucket/path/*.parquet"` |
| Replace one partition | Write to `table$YYYYMMDD` with `WRITE_TRUNCATE` |
| Partition + cluster | `PARTITION BY DATE(ts) CLUSTER BY a, b` |
| Explode array | `FROM t, UNNEST(t.items) AS item` |
| Safe math | `SAFE_DIVIDE(a, b)` · `SAFE_CAST(x AS INT64)` |
| Latest per key | `QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1` |
| Time travel | `FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)` |
| Dev copy | `CREATE TABLE dev.t CLONE prod.t` |
| Spend by user | `` `region-xx`.INFORMATION_SCHEMA.JOBS_BY_PROJECT `` → `total_bytes_billed` |

**Transformation tools that target BigQuery:** SQL in scheduled queries, Dataform (native), dbt, SQLMesh

---

## Interview Questions

**Q: How is BigQuery's architecture different from a traditional warehouse?**
A: It is serverless and separates storage from compute. Data lives in Colossus in a columnar format; queries run on Dremel, which splits work across many slots (units of compute) connected by a fast network and a distributed shuffle. There are no clusters to size or indexes to tune — capacity comes from a shared pool (on-demand) or from reserved, autoscaling slots (editions).

**Q: What is the difference between partitioning and clustering in BigQuery?**
A: Partitioning splits a table into segments (usually by day) so queries filtering on the partition column skip whole partitions, and the bytes to be scanned are known before the query runs. Clustering sorts data within each partition by up to four columns, so filters and aggregations on those columns read fewer storage blocks; its savings are only known after execution. Use both: partition by date, cluster by the most common secondary filters.

**Q: How would you control BigQuery costs for a large analytics team?**
A: Enforce partition filters on large tables, set `maximum_bytes_billed` and per-user or per-project quotas, and teach column selection (no `SELECT *`; `LIMIT` doesn't reduce cost). Pre-aggregate for dashboards with materialized views or BI Engine. Monitor `INFORMATION_SCHEMA.JOBS` for the most expensive users and queries. For steady heavy usage, compare on-demand with capacity pricing (editions with autoscaling and commitments), and separate workloads with reservations.

**Q: How do you load data into BigQuery idempotently?**
A: Load each run into a specific partition with `WRITE_TRUNCATE` (using the `table$YYYYMMDD` decorator or a `MERGE` scoped to that partition), so reruns replace rather than duplicate data. For upserts, stage the batch and `MERGE` on keys. For streaming, use the Storage Write API with committed streams and offsets for exactly-once writes, or deduplicate by event ID downstream.

**Q: When would you use nested and repeated fields instead of separate tables?**
A: When child records are always accessed with their parent — like order line items or event properties. Storing them as `ARRAY<STRUCT<...>>` keeps them co-located, avoids large joins, and reduces shuffle. Use separate tables when the child data is large, queried independently, or updated on its own.

---

## Further Reading

- [BigQuery documentation](https://cloud.google.com/bigquery/docs)
- [Partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) and [clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables)
- [Optimize query computation](https://cloud.google.com/bigquery/docs/best-practices-performance-compute)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
- *Google BigQuery: The Definitive Guide* — Valliappa Lakshmanan & Jordan Tigani (O'Reilly)

---

**Previous:** [Snowflake](snowflake-reference.md) · **Next:** [Amazon Redshift](redshift-reference.md) · **Back to:** [Index](../README.md)
