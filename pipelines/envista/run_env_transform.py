"""Pipeline for transforming Envista daily data.

This pipeline reads raw Envista daily summary files, combines all pollutants
for each year, applies transformations, and writes cleaned data to the
transform layer organized by year.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from envista.transformers.transform_env import transform_env_daily_for_year
from loaders.filesystem import write_csv


def run():
    """Run the Envista transformation pipeline."""
    print("Starting Envista Transformation Pipeline")
    print(f"Date: {date.today()}")

    config.ensure_dirs()

    raw_monitors_dir = config.RAW_ENV_MONITORS
    raw_daily_dir = config.RAW_ENV_DAILY
    trans_daily_dir = config.TRANS_DAILY

    if not raw_monitors_dir.exists():
        print(f"Raw monitors directory not found: {raw_monitors_dir}")
        print("Please run the monitors extraction pipeline first.")
        return

    if not raw_daily_dir.exists():
        print(f"Raw daily directory not found: {raw_daily_dir}")
        print("Please run the daily extraction pipeline first.")
        return

    # Output directories
    if not trans_daily_dir.exists():
        trans_daily_dir.mkdir(parents=True, exist_ok=True)

    # Create unique monitor and channel table
    print("Creating unique monitor and channel tables")
    monitors = pd.read_csv(raw_monitors_dir / "envista_stations_monitors.csv")
    unique_monitors = monitors[["station_id", "stations_tag"]].drop_duplicates()

    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\nProcessing year {year_str}...")
       
        transform_daily_df = transform_env_daily_for_year(year_str, raw_daily_dir, unique_monitors)

        if transform_daily_df.empty:
            print(f"No data for year {year_str}, skipping")
            continue

        # Write to transform layer
        output_path = trans_daily_dir / f"envista_daily_{year_str}.csv"
        write_csv(transform_daily_df, output_path)

        print(f"Wrote {len(transform_daily_df)} AQI records to {output_path}")

        years_processed += 1
        total_records += len(transform_daily_df)

    print("\nAQI daily transformation complete!")
    print(f"Processed {years_processed} years with {total_records} total records")

if __name__ == "__main__":
    run()
