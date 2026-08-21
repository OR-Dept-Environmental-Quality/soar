"""Pipeline for transforming Envista daily data.

This pipeline reads raw Envista daily summary files, combines all pollutants
for each year, applies transformations, and writes cleaned data to the
transform layer organized by year.
"""

from __future__ import annotations

import argparse
import os
import sys

from datetime import date
from pathlib import Path
import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from envista.transformers.transform_env import transform_env_daily_for_year
from envista.transformers.transform_env_hourly import transform_env_hourly_for_year
from loaders.filesystem import write_csv

def _load_envista_pollutant_data() -> pd.DataFrame:
    """Load dimPollutant data."""

    df = pd.read_csv("ops/dimPollutant_Envista.csv", dtype=str)
    normalized_cols = {str(col).strip(): col for col in df.columns}
    required_columns = [
        "group_store",
        "aqs_parameter_code",
        "aqs_parameter",
        "aqs_method_code",
        "aqs_method",
        "aqs_units",
    ]
    missing_columns = [column for column in required_columns if column not in normalized_cols]

    if not missing_columns:
        df = df[[normalized_cols[column] for column in required_columns]].copy()
        df.columns = required_columns
        df = df.dropna(subset=required_columns).drop_duplicates()
        for column in required_columns:
            df[column] = df[column].astype(str).str.strip()
        return df

    raise ValueError(
        "Envista pollutant catalog is missing required columns: "
        f"{', '.join(missing_columns)}"
    )

def _parse_requested_filters(argv: list[str] | None = None) -> tuple[list[str] | None, str]:
    """Return selected group_store values and chosen service from CLI args or env."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--group-store",
        "--group-stores",
        nargs="+",
        default=None,
        help="One or more group_store values to retrieve; e.g. --group-store pm25 carbonaceous_aerosol",
    )
    parser.add_argument(
        "--service",
        choices=["hourly", "daily", "both"],
        default=None,
        help="Which Envista service to run: hourly, daily, or both",
    )
    args, _ = parser.parse_known_args(argv)

    raw_group_values: list[str] = []
    if args.group_store:
        raw_group_values.extend(args.group_store)

    env_group_value = os.getenv("ENV_GROUP_STORE", "").strip()
    if env_group_value:
        raw_group_values.extend(part.strip() for part in env_group_value.split(","))

    if raw_group_values:
        normalized_group_values = []
        seen: set[str] = set()
        for value in raw_group_values:
            for part in str(value).split(","):
                cleaned = part.strip()
                if not cleaned:
                    continue
                key = cleaned.casefold()
                if key not in seen:
                    normalized_group_values.append(cleaned)
                    seen.add(key)
        requested_group_stores = normalized_group_values
    else:
        requested_group_stores = None

    if args.service:
        requested_service = args.service
    else:
        env_service_value = os.getenv("ENV_SERVICE", "").strip().casefold()
        requested_service = env_service_value if env_service_value in {"hourly", "daily", "both"} else "both"

    return requested_group_stores, requested_service

def run(argv: list[str] | None = None) -> None:
    """Run the Envista transformation pipeline."""
    requested_group_stores, requested_service = _parse_requested_filters(argv)

    print("Starting Envista Transformation Pipeline")
    print(f"Date: {date.today()}")
    if requested_group_stores:
        print(f"Requested group stores: {requested_group_stores}")
    else:
        print("Requested group stores: all configured Envista groups")
    print(f"Requested service: {requested_service}")

    config.ensure_dirs()

    raw_monitors_dir = config.RAW_ENV_MONITORS
    raw_daily_dir = config.RAW_ENV_DAILY
    raw_hourly_dir = config.RAW_ENV_HOURLY
    trans_daily_dir = config.TRANS_DAILY
    trans_hourly_dir = config.TRANS_SAMPLE
    trans_aqi_dir = config.TRANS_AQI

    if not raw_monitors_dir.exists():
        print(f"Raw monitors directory not found: {raw_monitors_dir}")
        print("Please run the monitors extraction pipeline first.")
        return

    # Output directories
    if not trans_daily_dir.exists():
        trans_daily_dir.mkdir(parents=True, exist_ok=True)

    if not trans_hourly_dir.exists():
        trans_hourly_dir.mkdir(parents=True, exist_ok=True)

    # Create unique monitor and channel table
    print("Creating unique monitor and channel tables")
    monitors = pd.read_csv(raw_monitors_dir / "envista_stations_monitors.csv")
    unique_monitors = monitors[["station_id", "stations_tag"]].drop_duplicates()

    # Read in pollutant catalog
    pollutant_catalog = _load_envista_pollutant_data()
    if requested_group_stores:
        requested_norm = {value.casefold() for value in requested_group_stores}
        pollutant_catalog = pollutant_catalog[
            pollutant_catalog["group_store"].astype(str).str.casefold().isin(requested_norm)
        ].copy()

    years_processed = 0
    total_records = 0

    if requested_service in {"daily", "both"}:
        if not raw_daily_dir.exists():
            print(f"Raw daily directory not found: {raw_daily_dir}")
            print("Please run the daily extraction pipeline first.")
            return

        for year in range(config.START_YEAR, config.END_YEAR + 1):
            year_str = str(year)
            print(f"\nProcessing daily year {year_str}...")

            transform_daily_df = transform_env_daily_for_year(
                year_str,
                raw_daily_dir,
                unique_monitors,
                pollutant_catalog,
                requested_group_stores,
            )

            if transform_daily_df.empty:
                print(f"No daily data for year {year_str}, skipping")
                continue

            aqi_output_path = trans_aqi_dir / f"aqi_envista_daily_{year_str}.csv"
            write_csv(transform_daily_df, aqi_output_path)
            print(f"Wrote {len(transform_daily_df)} AQI records to {aqi_output_path}")

            years_processed += 1
            total_records += len(transform_daily_df)

    if requested_service in {"hourly", "both"}:
        if not raw_hourly_dir.exists():
            print(f"Raw hourly directory not found: {raw_hourly_dir}")
            print("Please run the hourly extraction pipeline first.")
            return

        for year in range(config.START_YEAR, config.END_YEAR + 1):
            year_str = str(year)
            print(f"\nProcessing hourly year {year_str}...")

            transform_hourly_df = transform_env_hourly_for_year(
                year_str,
                raw_hourly_dir,
                unique_monitors,
                pollutant_catalog,
            )

            if transform_hourly_df.empty:
                print(f"No hourly data for year {year_str}, skipping")
                continue

            hourly_output_path = trans_hourly_dir / f"hourly_envista_{year_str}.csv"
            write_csv(transform_hourly_df, hourly_output_path)
            print(f"Wrote {len(transform_hourly_df)} hourly records to {hourly_output_path}")

            years_processed += 1
            total_records += len(transform_hourly_df)

    print("\nEnvista transformation complete!")
    print(f"Processed {years_processed} year blocks with {total_records} total records")

if __name__ == "__main__":
    run()
