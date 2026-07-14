"""Pipeline runner for ozone hourly staging.

Runs the ozone hourly consolidation pipeline which transforms AQS (44201)
hourly sample records into annual staged fact tables at:

    staged/fct_ozone_hourly/fct_ozone_hourly_{year}.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_ozone_hourly import run_consolidation


def main() -> None:
    """Run the ozone hourly staging pipeline."""
    print("Starting Ozone Hourly Staging Pipeline")
    run_consolidation()


if __name__ == "__main__":
    main()
