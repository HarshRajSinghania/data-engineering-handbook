"""Everything Dagster loads: assets, checks, the job, the schedule and the warehouse resource."""
import dagster as dg

from . import assets
from .resources import DuckDBWarehouse

all_assets = dg.load_assets_from_modules([assets])
all_checks = dg.load_asset_checks_from_modules([assets])

# DuckDB allows one writer at a time, so run the steps in a single process, one after another.
daily_job = dg.define_asset_job(
    "daily_refresh",
    selection=dg.AssetSelection.all(),
    executor_def=dg.in_process_executor,
)

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    jobs=[daily_job],
    schedules=[dg.ScheduleDefinition(job=daily_job, cron_schedule="0 6 * * *")],
    resources={"warehouse": DuckDBWarehouse()},
)
