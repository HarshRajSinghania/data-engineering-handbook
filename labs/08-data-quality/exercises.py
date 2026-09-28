"""Lab 08 exercises: write the expectation suites and the gate. Replace each TODO.

Run:
    python exercises.py --stage raw       # what your suites find in the files as they arrived
    python exercises.py                   # clean, validate, gate, publish
Reference answers: solutions.py
"""
from __future__ import annotations

from datetime import datetime, timezone

from great_expectations import expectations as gxe

from framework import Outcome, Reference, main

FRESH_FROM = datetime(2024, 3, 25, tzinfo=timezone.utc)      # the dataset covers March 2024
FRESH_TO = datetime(2024, 4, 7, tzinfo=timezone.utc)


def orders_expectations(ref: Reference) -> list:
    return [
        # TODO 1 (keys): order_id is never null and unique.
        # TODO 2 (domains): status is one of placed, paid, shipped, delivered, cancelled;
        #                   currency is one of USD, EUR, GBP.
        # TODO 5 (thresholds): block when more than 5% of customer_id values are missing, and
        #                      also raise a non-blocking warning when more than 0.5% are missing.
        #                      Use `mostly=` and `severity="warning"`.
        # TODO 7 (schema, volume, freshness): the table has exactly the six columns order_id,
        #        customer_id, order_ts, status, currency, updated_at; it has between 1,000 and
        #        50,000 rows; and its newest order_ts is between FRESH_FROM and FRESH_TO.
        gxe.ExpectTableRowCountToBeBetween(min_value=1),      # placeholder: keeps the report from being empty
    ]


def items_expectations(ref: Reference) -> list:
    return [
        # TODO 3: (order_id, line_no) is unique; quantity is between 1 and 1,000; unit_price is
        #         between 0.01 and 10,000; every product_id is in ref.product_ids and every
        #         order_id is in ref.order_ids (referential integrity).
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
    ]


def events_expectations(ref: Reference) -> list:
    return [
        # TODO 4: event_id is unique; event_type is "page_view"; page is one of /, /search,
        #         /product, /cart, /checkout; and received_ts is never earlier than event_ts.
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
    ]


def gate(outcomes: list[Outcome]) -> list[Outcome]:
    """Return the failed outcomes that must stop publishing. An empty list lets the run publish."""
    # TODO 6: block on failed outcomes whose severity is "critical"; let warnings through.
    return []


if __name__ == "__main__":
    main(orders_expectations, items_expectations, events_expectations, gate)
