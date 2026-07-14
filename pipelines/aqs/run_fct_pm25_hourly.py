"""Pipeline runner for PM2.5 hourly staging.

Loads Envista monitor metadata, then runs the PM2.5 hourly consolidation
pipeline which merges AQS (88101/88502) and Envista (88502 POC=99) hourly
records into annual staged fact tables at:

    staged/fct_pm25_hourly/fct_pm25_hourly_{year}.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

import config
from stage.consolidate_fct_pm25_hourly import run_consolidation


def main() -> None:
    """Run the PM2.5 hourly staging pipeline."""
    print("Starting PM2.5 Hourly Staging Pipeline")

    monitors_path = config.RAW_ENV_MONITORS / "envista_stations_monitors.csv"

    if not monitors_path.exists():
        print(f"Envista monitors file not found: {monitors_path}")
        print("Please run the Envista monitors extraction pipeline first.")
        return

    monitors = pd.read_csv(monitors_path)
    unique_monitors = monitors[["station_id", "stations_tag"]].drop_duplicates()

    run_consolidation(unique_monitors)


if __name__ == "__main__":
    main()
