"""Build the silver and gold Iceberg tables from the raw files. Safe to run again: every step is idempotent.

    raw CSV + JSONL ──► silver (MERGE, keyed) ──► gold (createOrReplace)

Silver:  orders, products, order_items, order_items_quarantine, events (partitioned by day)
Gold:    daily_revenue

Usage:
    python pipeline.py

This is the same pipeline as Lab 03 (Delta Lake), minus the bronze layer, so the two table formats
can be compared on the same data. Tables are addressed by catalog name, for example lab.silver.orders.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lake import RAW_DIR, get_spark, table

SCHEMAS = {
    "orders": "order_id BIGINT, customer_id INT, order_ts TIMESTAMP, status STRING, currency STRING, updated_at TIMESTAMP",
    "products": "product_id INT, sku STRING, name STRING, category STRING, list_price DECIMAL(10,2)",
    "order_items": "order_id BIGINT, line_no INT, product_id INT, quantity INT, unit_price DECIMAL(10,2)",
    "order_items_quarantine": ("order_id BIGINT, line_no INT, product_id INT, quantity INT, unit_price DECIMAL(10,2), "
                               "reject_reason STRING"),
    "events": ("event_id BIGINT, customer_id INT, event_type STRING, page STRING, event_ts TIMESTAMP, "
               "received_ts TIMESTAMP"),
}
PARTITIONING = {"events": "PARTITIONED BY (days(event_ts))"}      # hidden partitioning: no date column needed
UPDATE_GUARD = {"orders": "s.updated_at > t.updated_at"}          # only a newer version replaces a row (orders can change; the other tables are insert-only)


def upsert(spark: SparkSession, df: DataFrame, name: str, keys: list[str]) -> None:
    """Create the table on first use, then MERGE only the rows that are new or changed.

    A MERGE rewrites every data file that contains a matched row, even when the row does not change
    (copy-on-write). Sending the whole source on every run would rewrite the whole table each time,
    so rows that are already present, and not newer, are filtered out first.
    """
    target = table("silver", name)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({SCHEMAS[name]}) USING iceberg {PARTITIONING.get(name, '')}")
    existing = spark.table(target)
    on = [df[k] == existing[k] for k in keys]
    if name in UPDATE_GUARD:                                  # a newer version of a known row, or a new row
        pending = df.join(existing.select(*keys, F.col("updated_at").alias("_current")), keys, "left") \
                    .where("_current IS NULL OR updated_at > _current").drop("_current")
    else:                                                     # rows are immutable: only new keys
        pending = df.join(existing, on, "left_anti")
    # Materialize the source first: Spark 4.1 cannot plan a MERGE whose source still reads the target table
    pending = pending.localCheckpoint()
    if pending.isEmpty():
        return
    pending.createOrReplaceTempView("updates")
    merge_on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    guard = f" AND {UPDATE_GUARD[name]}" if name in UPDATE_GUARD else ""
    spark.sql(f"""
        MERGE INTO {target} t USING updates s ON {merge_on}
        WHEN MATCHED{guard} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def build_silver(spark: SparkSession) -> None:
    read_csv = lambda name: spark.read.option("header", True).csv(str(RAW_DIR / f"{name}.csv"))  # noqa: E731

    latest = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    orders = (
        read_csv("orders")
        .select(F.col("order_id").cast("bigint"), F.col("customer_id").cast("int"),
                F.to_timestamp("order_ts").alias("order_ts"), F.lower("status").alias("status"), "currency",
                F.to_timestamp("updated_at").alias("updated_at"))
        .withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")
    )
    upsert(spark, orders, "orders", ["order_id"])

    products = read_csv("products").select(
        F.col("product_id").cast("int"), "sku", "name", "category",
        F.col("unit_price").cast("decimal(10,2)").alias("list_price"))
    upsert(spark, products, "products", ["product_id"])

    # Order lines: valid rows go to silver, invalid ones to a quarantine table with a reason
    lines = read_csv("order_items").select(
        F.col("order_id").cast("bigint"), F.col("line_no").cast("int"), F.col("product_id").cast("int"),
        F.col("quantity").cast("int"), F.col("unit_price").cast("decimal(10,2)"))
    known = products.select("product_id", F.lit(True).alias("_known"))
    checked = lines.join(F.broadcast(known), "product_id", "left").withColumn(
        "reject_reason",
        F.when(F.col("_known").isNull(), "unknown_product").when(F.col("quantity") <= 0, "non_positive_quantity"),
    ).drop("_known").select("order_id", "line_no", "product_id", "quantity", "unit_price", "reject_reason")
    upsert(spark, checked.where("reject_reason IS NULL").drop("reject_reason"), "order_items", ["order_id", "line_no"])
    upsert(spark, checked.where("reject_reason IS NOT NULL"), "order_items_quarantine", ["order_id", "line_no"])

    # Events: drop duplicate deliveries by event_id (keep the first received)
    first = Window.partitionBy("event_id").orderBy("received_ts")
    events = (
        spark.read.schema(SCHEMAS["events"].replace("TIMESTAMP", "STRING")).json(str(RAW_DIR / "events.jsonl"))
        .withColumn("event_ts", F.to_timestamp("event_ts")).withColumn("received_ts", F.to_timestamp("received_ts"))
        .withColumn("_rn", F.row_number().over(first)).where("_rn = 1").drop("_rn")
    )
    upsert(spark, events, "events", ["event_id"])


def build_gold(spark: SparkSession) -> None:
    orders = spark.table(table("silver", "orders")).where("status != 'cancelled'")
    lines = spark.table(table("silver", "order_items"))
    rates = spark.createDataFrame([("USD", 1.00), ("EUR", 1.09), ("GBP", 1.27)], "currency string, rate_to_usd double")
    daily = (
        lines.join(orders, "order_id").join(F.broadcast(rates), "currency")
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("amount_usd", F.col("quantity") * F.col("unit_price") * F.col("rate_to_usd"))
        .groupBy("order_date")
        .agg(F.countDistinct("order_id").alias("orders"), F.countDistinct("customer_id").alias("customers"),
             F.round(F.sum("amount_usd"), 2).alias("revenue_usd"))
    )
    # createOrReplace swaps the table's contents in a new snapshot; the earlier snapshots stay readable
    daily.writeTo(table("gold", "daily_revenue")).using("iceberg").createOrReplace()


def main() -> None:
    spark = get_spark("pipeline")
    for layer in ("silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table(layer, '')[:-1]}")
    build_silver(spark)
    print("silver  done")
    build_gold(spark)
    print("gold    done")

    print()
    for layer, name in [("silver", "orders"), ("silver", "order_items"), ("silver", "order_items_quarantine"),
                        ("silver", "events"), ("gold", "daily_revenue")]:
        print(f"  {layer}.{name:<24} {spark.table(table(layer, name)).count():>7,} rows")


if __name__ == "__main__":
    main()
