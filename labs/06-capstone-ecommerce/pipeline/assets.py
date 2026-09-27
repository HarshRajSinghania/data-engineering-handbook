"""Ingest -> clean -> model -> publish, as Dagster assets on one DuckDB warehouse.

Layers (schemas):  raw  ->  staging  ->  marts  ->  a static HTML dashboard
Every asset is idempotent: it rebuilds its output from its inputs, so running the
pipeline twice, or after new source data arrives, always gives a consistent result.
"""
from __future__ import annotations

from pathlib import Path

import dagster as dg

from .resources import LAB_DIR, DuckDBWarehouse

# Assumption for the exercise: fixed FX rates to USD. A real pipeline would join a rates table.
FX_TO_USD = {"USD": 1.00, "EUR": 1.08, "GBP": 1.27}
DASHBOARD_PATH = LAB_DIR / "dashboard.html"


def _count(warehouse: DuckDBWarehouse, table: str) -> int:
    return int(warehouse.scalar(f"SELECT COUNT(*) FROM {table}"))


# --------------------------------------------------------------------------- raw
@dg.asset(group_name="raw", kinds={"duckdb"})
def raw_tables(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """Load the source files as-is. No cleaning here: raw is the audit copy."""
    d = warehouse.data_dir
    warehouse.execute(f"""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS marts;
        CREATE OR REPLACE TABLE raw.customers   AS SELECT * FROM read_csv('{d}/customers.csv', header = true);
        CREATE OR REPLACE TABLE raw.products    AS SELECT * FROM read_csv('{d}/products.csv', header = true);
        CREATE OR REPLACE TABLE raw.orders      AS SELECT * FROM read_csv('{d}/orders.csv', header = true);
        CREATE OR REPLACE TABLE raw.order_items AS SELECT * FROM read_csv('{d}/order_items.csv', header = true);
        CREATE OR REPLACE TABLE raw.events      AS SELECT * FROM read_json('{d}/events.jsonl', format = 'newline_delimited');
    """)
    counts = {t: _count(warehouse, f"raw.{t}") for t in ("customers", "products", "orders", "order_items", "events")}
    return dg.MaterializeResult(metadata={f"rows_{k}": v for k, v in counts.items()})


# ----------------------------------------------------------------------- staging
@dg.asset(deps=[raw_tables], group_name="staging", kinds={"duckdb"})
def stg_orders(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """One current row per order: latest version wins, status normalised, bad rows quarantined."""
    warehouse.execute("""
        CREATE OR REPLACE TABLE staging.orders AS
        SELECT order_id, customer_id, order_ts, lower(trim(status)) AS status, currency, updated_at
        FROM raw.orders
        WHERE customer_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1;

        CREATE OR REPLACE TABLE staging.rejected_orders AS
        SELECT DISTINCT o.*, 'missing customer_id' AS reason
        FROM raw.orders o
        WHERE customer_id IS NULL;
    """)
    return dg.MaterializeResult(metadata={
        "rows": _count(warehouse, "staging.orders"),
        "rejected": _count(warehouse, "staging.rejected_orders"),
    })


@dg.asset(deps=[raw_tables], group_name="staging", kinds={"duckdb"})
def stg_order_items(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """Valid order lines only: positive quantity and a known product."""
    warehouse.execute("""
        CREATE OR REPLACE TABLE staging.order_items AS
        SELECT i.order_id, i.line_no, i.product_id, i.quantity, i.unit_price
        FROM raw.order_items i
        JOIN raw.products p USING (product_id)
        WHERE i.quantity > 0;

        CREATE OR REPLACE TABLE staging.rejected_order_items AS
        SELECT i.*, CASE WHEN i.quantity <= 0 THEN 'non-positive quantity' ELSE 'unknown product' END AS reason
        FROM raw.order_items i
        LEFT JOIN raw.products p USING (product_id)
        WHERE i.quantity <= 0 OR p.product_id IS NULL;
    """)
    return dg.MaterializeResult(metadata={
        "rows": _count(warehouse, "staging.order_items"),
        "rejected": _count(warehouse, "staging.rejected_order_items"),
    })


@dg.asset(deps=[raw_tables], group_name="staging", kinds={"duckdb"})
def stg_events(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """Page-view events with duplicate deliveries removed."""
    warehouse.execute("""
        CREATE OR REPLACE TABLE staging.events AS
        SELECT event_id, customer_id, event_type, page,
               event_ts::TIMESTAMP AS event_ts, received_ts::TIMESTAMP AS received_ts
        FROM raw.events
        QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_ts) = 1;
    """)
    return dg.MaterializeResult(metadata={"rows": _count(warehouse, "staging.events")})


# --------------------------------------------------------------- staging checks
# blocking=True: if a check fails, the marts are not rebuilt, so bad data never reaches them.
@dg.asset_check(asset=stg_orders, blocking=True)
def orders_are_unique(warehouse: DuckDBWarehouse) -> dg.AssetCheckResult:
    dupes = int(warehouse.scalar(
        "SELECT COUNT(*) FROM (SELECT order_id FROM staging.orders GROUP BY 1 HAVING COUNT(*) > 1)"
    ))
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicate_order_ids": dupes})


@dg.asset_check(asset=stg_orders, blocking=True)
def order_status_is_known(warehouse: DuckDBWarehouse) -> dg.AssetCheckResult:
    rows = warehouse.query(
        "SELECT DISTINCT status FROM staging.orders "
        "WHERE status NOT IN ('placed', 'paid', 'shipped', 'delivered', 'cancelled')"
    )
    return dg.AssetCheckResult(passed=not rows, metadata={"unexpected": str([r[0] for r in rows])})


@dg.asset_check(asset=stg_order_items, blocking=True)
def items_reference_known_orders(warehouse: DuckDBWarehouse) -> dg.AssetCheckResult:
    orphans = int(warehouse.scalar(
        "SELECT COUNT(*) FROM staging.order_items i LEFT JOIN staging.orders o USING (order_id) "
        "WHERE o.order_id IS NULL"
    ))
    # Lines of quarantined orders are expected to be orphans; anything else would point at a load problem.
    quarantined = int(warehouse.scalar(
        "SELECT COUNT(*) FROM staging.order_items i "
        "WHERE i.order_id IN (SELECT order_id FROM staging.rejected_orders)"
    ))
    return dg.AssetCheckResult(passed=orphans == quarantined, metadata={"orphans": orphans, "quarantined": quarantined})


# ------------------------------------------------------------------------- marts
@dg.asset(deps=[stg_orders, stg_order_items], group_name="marts", kinds={"duckdb"})
def fct_orders(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """One row per order with its revenue in USD (line quantity x price, converted)."""
    fx_values = ", ".join(f"('{c}', {r})" for c, r in FX_TO_USD.items())
    warehouse.execute(f"""
        CREATE OR REPLACE TABLE marts.fct_orders AS
        WITH fx(currency, to_usd) AS (VALUES {fx_values}),
        totals AS (
            SELECT order_id, SUM(quantity * unit_price) AS amount, SUM(quantity) AS units
            FROM staging.order_items GROUP BY order_id
        )
        SELECT o.order_id, o.customer_id, c.country, CAST(o.order_ts AS DATE) AS order_date,
               o.status, o.currency,
               COALESCE(t.units, 0) AS units,
               ROUND(COALESCE(t.amount, 0), 2) AS amount,
               ROUND(COALESCE(t.amount, 0) * fx.to_usd, 2) AS amount_usd
        FROM staging.orders o
        JOIN fx USING (currency)
        LEFT JOIN totals t USING (order_id)
        LEFT JOIN raw.customers c USING (customer_id);
    """)
    return dg.MaterializeResult(metadata={"rows": _count(warehouse, "marts.fct_orders")})


@dg.asset(deps=[fct_orders], group_name="marts", kinds={"duckdb"})
def daily_revenue(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """Revenue per day. Cancelled orders are excluded: this is the number finance uses."""
    warehouse.execute("""
        CREATE OR REPLACE TABLE marts.daily_revenue AS
        SELECT order_date,
               COUNT(*)                      AS orders,
               ROUND(SUM(amount_usd), 2)     AS revenue_usd,
               ROUND(AVG(amount_usd), 2)     AS avg_order_value_usd
        FROM marts.fct_orders
        WHERE status <> 'cancelled'
        GROUP BY order_date;
    """)
    total = float(warehouse.scalar("SELECT COALESCE(SUM(revenue_usd), 0) FROM marts.daily_revenue"))
    return dg.MaterializeResult(metadata={"days": _count(warehouse, "marts.daily_revenue"), "total_revenue_usd": total})


@dg.asset(deps=[fct_orders], group_name="marts", kinds={"duckdb"})
def revenue_by_country(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    warehouse.execute("""
        CREATE OR REPLACE TABLE marts.revenue_by_country AS
        SELECT COALESCE(country, 'unknown') AS country,
               COUNT(*) AS orders,
               ROUND(SUM(amount_usd), 2) AS revenue_usd
        FROM marts.fct_orders
        WHERE status <> 'cancelled'
        GROUP BY 1;
    """)
    return dg.MaterializeResult(metadata={"countries": _count(warehouse, "marts.revenue_by_country")})


# ------------------------------------------------------------------- mart checks
@dg.asset_check(asset=daily_revenue)
def revenue_reconciles_with_orders(warehouse: DuckDBWarehouse) -> dg.AssetCheckResult:
    """The published daily numbers must add up to the order-level facts."""
    from_facts = float(warehouse.scalar(
        "SELECT COALESCE(SUM(amount_usd), 0) FROM marts.fct_orders WHERE status <> 'cancelled'"
    ))
    from_mart = float(warehouse.scalar("SELECT COALESCE(SUM(revenue_usd), 0) FROM marts.daily_revenue"))
    diff = abs(from_facts - from_mart)
    return dg.AssetCheckResult(passed=diff < 1.0, metadata={"facts": from_facts, "mart": from_mart, "difference": diff})


@dg.asset_check(asset=daily_revenue)
def no_negative_revenue(warehouse: DuckDBWarehouse) -> dg.AssetCheckResult:
    bad = int(warehouse.scalar("SELECT COUNT(*) FROM marts.daily_revenue WHERE revenue_usd < 0"))
    return dg.AssetCheckResult(passed=bad == 0, metadata={"negative_days": bad})


# --------------------------------------------------------------------- publish
@dg.asset(deps=[daily_revenue, revenue_by_country], group_name="publish")
def revenue_dashboard(warehouse: DuckDBWarehouse) -> dg.MaterializeResult:
    """Write a self-contained HTML dashboard (inline SVG, no JavaScript) from the marts."""
    days = warehouse.query("SELECT order_date, revenue_usd, orders FROM marts.daily_revenue ORDER BY order_date")
    countries = warehouse.query("SELECT country, revenue_usd FROM marts.revenue_by_country ORDER BY revenue_usd DESC LIMIT 8")
    total = sum(r[1] for r in days)
    order_count = sum(r[2] for r in days)

    width, height, pad = 720, 220, 30
    peak = max((r[1] for r in days), default=1) or 1
    bar_w = (width - 2 * pad) / max(len(days), 1)
    bars = "".join(
        f'<rect x="{pad + i * bar_w:.1f}" y="{height - pad - (r[1] / peak) * (height - 2 * pad):.1f}" '
        f'width="{bar_w * 0.8:.1f}" height="{(r[1] / peak) * (height - 2 * pad):.1f}" fill="#4f46e5">'
        f"<title>{r[0]}: ${r[1]:,.0f}</title></rect>"
        for i, r in enumerate(days)
    )
    country_rows = "".join(f"<tr><td>{c}</td><td>${v:,.0f}</td></tr>" for c, v in countries)
    html = f"""<!doctype html><html lang="en"><meta charset="utf-8"><title>Revenue dashboard</title>
<style>body{{font:16px system-ui;max-width:760px;margin:2rem auto;padding:0 1rem}}
.kpi{{display:flex;gap:2rem}}.kpi b{{display:block;font-size:1.6rem}}td,th{{padding:.25rem .75rem;text-align:left}}</style>
<h1>Revenue dashboard</h1>
<div class="kpi"><div><b>${total:,.0f}</b>revenue (USD, excl. cancelled)</div><div><b>{order_count:,}</b>orders</div>
<div><b>{len(days)}</b>days</div></div>
<h2>Revenue by day</h2><svg viewBox="0 0 {width} {height}" role="img" aria-label="Daily revenue">{bars}</svg>
<h2>Top countries</h2><table><tr><th>Country</th><th>Revenue</th></tr>{country_rows}</table></html>"""
    DASHBOARD_PATH.write_text(html, encoding="utf-8")
    return dg.MaterializeResult(metadata={"path": str(DASHBOARD_PATH), "total_revenue_usd": total})
