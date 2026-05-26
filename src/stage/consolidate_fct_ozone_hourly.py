"""Ozone Hourly Consolidation Script.

Consolidates hourly ozone data from AQS (parameter 44201) into a staged
fact table. AQS is the sole source for ozone — no Envista equivalent.

Output: staged/fct_ozone_hourly/fct_ozone_hourly_{year}.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.sample_hourly import transform_aqs_sample_hourly_for_year

_OZONE_PARAM_CODES = ["44201"]

_OUTPUT_COLUMNS = [
    "site_code",
    "date_local",
    "time_local",
    "parameter_code",
    "poc",
    "parameter",
    "sample_measurement",
    "units_of_measure",
    "sample_duration_code",
    "sample_duration",
    "validity_indicator",
    "method_code",
    "method",
    "qualifier",
    "source",
]


def consolidate_ozone_hourly_for_year(
    year: str,
    raw_sample_dir: Path,
) -> pd.DataFrame:
    """Consolidate ozone hourly records for a given year.

    Transforms AQS hourly ozone data and returns it ready for staging.
    No deduplication is applied (AQS is the single source).

    Args:
        year: Four-digit year string.
        raw_sample_dir: Directory containing raw AQS sample CSV files.

    Returns:
        Consolidated DataFrame. Empty DataFrame if no data is available.
    """
    print(f"  [Ozone hourly] Transforming AQS sample data for {year}...")
    df = transform_aqs_sample_hourly_for_year(
        year, raw_sample_dir, parameter_codes=_OZONE_PARAM_CODES
    )

    if df.empty:
        print(f"  [Ozone hourly] No data available for {year}")
        return pd.DataFrame()

    # Ensure all output columns are present
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    result = df[_OUTPUT_COLUMNS].copy()

    print(f"  [Ozone hourly] {year}: {len(result)} records")

    return result


def run_consolidation() -> None:
    """Run ozone hourly consolidation for all configured years."""
    print("Starting Ozone Hourly Consolidation Pipeline")

    raw_sample_dir = config.RAW_AQS_SAMPLE

    if not raw_sample_dir.exists():
        print(f"AQS sample directory not found: {raw_sample_dir}")
        print("Please run the AQS sample extraction pipeline first.")
        return

    staged_dir = config.ROOT / "staged" / "fct_ozone_hourly"
    staged_dir.mkdir(parents=True, exist_ok=True)

    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\nConsolidating ozone hourly for year {year_str}...")

        consolidated_df = consolidate_ozone_hourly_for_year(year_str, raw_sample_dir)

        if consolidated_df.empty:
            print(f"  No consolidated data for {year_str}, skipping")
            continue

        output_path = staged_dir / f"fct_ozone_hourly_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"  Wrote {len(consolidated_df)} records to {output_path.name}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\nOzone hourly consolidation complete!")
    print(f"Processed {years_processed} years with {total_records} total records")
    print(f"Output: staged/fct_ozone_hourly/fct_ozone_hourly_{{year}}.csv")


if __name__ == "__main__":
    run_consolidation()
