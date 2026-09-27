"""The warehouse connection, shared by every asset."""
from __future__ import annotations

from pathlib import Path

import dagster as dg
import duckdb

LAB_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = LAB_DIR.parent / "data" / "output"


class DuckDBWarehouse(dg.ConfigurableResource):
    """A DuckDB file used as the warehouse. Swap this one class to target Snowflake or BigQuery."""

    path: str = str(LAB_DIR / "shop.duckdb")
    data_dir: str = str(DATA_DIR)

    def execute(self, sql: str) -> None:
        con = duckdb.connect(self.path)
        try:
            con.execute(sql)
        finally:
            con.close()

    def query(self, sql: str) -> list[tuple]:
        con = duckdb.connect(self.path, read_only=True)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def scalar(self, sql: str):
        return self.query(sql)[0][0]
