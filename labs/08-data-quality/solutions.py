"""Lab 08 reference solutions: expectation suites, severities and a gate for the e-commerce dataset.

Run:
    python solutions.py --stage raw       # what the suites find in the files as they arrived (exit code 1)
    python solutions.py                   # clean, validate, gate, publish
"""
from __future__ import annotations

from datetime import datetime, timezone

from great_expectations import expectations as gxe

from framework import Outcome, Reference, main

# The dataset covers March 2024. Freshness is checked against that window; in production the
# bounds would be relative to the current time.
FRESH_FROM = datetime(2024, 3, 25, tzinfo=timezone.utc)
FRESH_TO = datetime(2024, 4, 7, tzinfo=timezone.utc)


def orders_expectations(ref: Reference) -> list:
    return [
        # Schema and volume: a renamed column or an empty load should stop the pipeline
        gxe.ExpectTableColumnsToMatchSet(column_set=["order_id", "customer_id", "order_ts", "status", "currency", "updated_at"]),
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=50_000),
        # Freshness: the newest order must be recent, or the extract silently stopped
        gxe.ExpectColumnMaxToBeBetween(column="order_ts", min_value=FRESH_FROM, max_value=FRESH_TO),
        # Keys
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        # customer_id: block above 5% missing, warn above 0.5%
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.95),
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id", mostly=0.995, severity="warning"),
        # Domains
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["placed", "paid", "shipped", "delivered", "cancelled"]),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=["USD", "EUR", "GBP"]),
    ]


def items_expectations(ref: Reference) -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=200_000),
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["order_id", "line_no"]),
        gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=1_000),
        gxe.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0.01, max_value=10_000),
        # Referential integrity against the parent tables
        gxe.ExpectColumnValuesToBeInSet(column="product_id", value_set=ref.product_ids),
        gxe.ExpectColumnValuesToBeInSet(column="order_id", value_set=ref.order_ids),
    ]


def events_expectations(ref: Reference) -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=500_000),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gxe.ExpectColumnValuesToBeInSet(column="event_type", value_set=["page_view"]),
        gxe.ExpectColumnValuesToBeInSet(column="page", value_set=["/", "/search", "/product", "/cart", "/checkout"]),
        # An event cannot be received before it happened
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(column_A="received_ts", column_B="event_ts", or_equal=True),
    ]


def gate(outcomes: list[Outcome]) -> list[Outcome]:
    """The failures that stop publishing: critical ones. Warnings are reported and let the run continue."""
    return [o for o in outcomes if not o.success and o.severity == "critical"]


if __name__ == "__main__":
    main(orders_expectations, items_expectations, events_expectations, gate)
