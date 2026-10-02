"""Shared helpers: a local Spark session with an Iceberg catalog, and small display utilities."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

LAB_DIR = Path(__file__).resolve().parent
RAW_DIR = Path(os.environ.get("LAB_DATA_DIR", LAB_DIR.parent / "data" / "output"))
WAREHOUSE = LAB_DIR / "lakehouse"

# The runtime JAR is downloaded from Maven Central on first use. Its name must match the Spark
# and Scala versions in requirements.txt (Spark 4.1, Scala 2.13).
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
CATALOG = "lab"


def get_spark(app_name: str = "lab09") -> SparkSession:
    """Start (or reuse) a small local Spark session with an Iceberg catalog named `lab`."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # A catalog is where table names are resolved. This one keeps everything in a folder, so it
        # needs no server. Production catalogs (REST, Glue, Nessie...) coordinate concurrent writers.
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(WAREHOUSE))
        # By default a catalog keeps each table's metadata in memory for 30 seconds, so a session can miss a commit that
        # another process made in that time. The lab runs the pipeline in one process and reads in another: turn it off.
        .config(f"spark.sql.catalog.{CATALOG}.cache-enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")      # the default of 200 is sized for clusters
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def table(layer: str, name: str) -> str:
    """Fully qualified table name, e.g. table("silver", "orders") -> "lab.silver.orders"."""
    return f"{CATALOG}.{layer}.{name}"


def metadata(spark: SparkSession, layer: str, name: str, kind: str) -> DataFrame:
    """One of Iceberg's metadata tables: snapshots, history, files, manifests, partitions, refs."""
    return spark.sql(f"SELECT * FROM {table(layer, name)}.{kind}")


def snapshot_ids(spark: SparkSession, layer: str, name: str) -> list[int]:
    """Snapshot IDs of a table, oldest first."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table(layer, name)}.snapshots ORDER BY committed_at").collect()
    return [r.snapshot_id for r in rows]
