"""Run the whole pipeline once, without the UI:  python run.py

Prints every asset check, and exits non-zero if the run or any blocking check fails.
For the UI, use:  dagster dev -m pipeline.definitions
"""
import sys

import dagster as dg

from pipeline.definitions import all_assets, all_checks
from pipeline.resources import DuckDBWarehouse


def main() -> int:
    result = dg.materialize(
        [*all_assets, *all_checks],
        resources={"warehouse": DuckDBWarehouse()},
        raise_on_error=False,
    )
    print("\nAsset checks")
    failed = 0
    for check in result.get_asset_check_evaluations():
        mark = "PASS" if check.passed else "FAIL"
        failed += 0 if check.passed else 1
        print(f"  {mark}  {check.check_name:<36} {check.asset_key.to_user_string()}")
    print(f"\nrun {'succeeded' if result.success else 'FAILED'}; {failed} check(s) failed")
    return 0 if result.success and not failed else 1


if __name__ == "__main__":
    sys.exit(main())
