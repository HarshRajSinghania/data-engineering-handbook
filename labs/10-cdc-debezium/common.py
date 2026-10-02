"""Shared settings for the lab scripts. Every value can be overridden with an environment variable."""
from __future__ import annotations

import os
from pathlib import Path

LAB = Path(__file__).resolve().parent
RAW_DIR = Path(os.environ.get("LAB_DATA_DIR", LAB.parent / "data" / "output"))

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
PG_DSN = os.environ.get("PG_DSN", "host=localhost port=5432 user=postgres password=postgres dbname=shop")
TARGET_DB = Path(os.environ.get("TARGET_DB", LAB / "target.duckdb"))

CONNECTOR = "shop-cdc"
TOPIC_PREFIX = "shop"                              # topics are <prefix>.<schema>.<table>
TOPICS = {"orders": f"{TOPIC_PREFIX}.public.orders", "order_items": f"{TOPIC_PREFIX}.public.order_items"}
KEYS = {"orders": ["order_id"], "order_items": ["order_id", "line_no"]}
