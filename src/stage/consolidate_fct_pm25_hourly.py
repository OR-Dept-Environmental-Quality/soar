"""PM2.5 Hourly Consolidation Script.

Consolidates hourly PM2.5 data from AQS (parameters 88101 and 88502) and
Envista (parameter 88502, POC 99) into a single deduplicated fact table.

When multiple sources cover the same site-hour, one record is kept using a
priority hierarchy that matches the criteria daily pipeline:
    1 — AQS 88101 (FRM/FEM, highest quality)
    2 — AQS 88502, POC ≠ 99 (non-FRM/FEM regulatory monitor)
    3 — Envista 88502, POC = 99 (SensOR low-cost sensor)

Output: staged/fct_pm25_hourly/fct_pm25_hourly_{year}.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.sample_hourly import transform_aqs_sample_hourly_for_year
from envista.transformers.transform_env_hourly import transform_env_hourly_for_year

# PM2.5 parameter codes to pull from AQS sample files
_PM25_PARAM_CODES = ["88101", "88502"]
_SAMPLE_DURATION_CODES = ["1"]

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


def _assign_priority(df: pd.DataFrame) -> "pd.Series[int]":
    """Return a priority integer Series (1 = best) for each row.

    Priority rules:
        1 — parameter_code == "88101"
        2 — parameter_code == "88502" and poc != 99
        3 — parameter_code == "88502" and poc == 99  (Envista / SensOR)
    """
    poc_numeric = pd.to_numeric(df["poc"], errors="coerce")
    param = df["parameter_code"].astype(str)

    priority = np.where(
        param == "88101",
        1,
        np.where(
            (param == "88502") & (poc_numeric != 99),
            2,
            3,
        ),
    )
    return pd.Series(priority, index=df.index, dtype="int64")


def consolidate_pm25_hourly_for_year(
    year: str,
    raw_sample_dir: Path,
    raw_env_sample_dir: Path,
    unique_monitors: pd.DataFrame,
) -> pd.DataFrame:
    """Consolidate PM2.5 hourly records for a given year.

    Transforms and merges AQS and Envista hourly PM2.5 data, then deduplicates
    to one record per (site_code, date_local, time_local) using the priority
    hierarchy described in the module docstring.

    Args:
        year: Four-digit year string.
        raw_sample_dir: Directory containing raw AQS sample CSV files.
        raw_env_sample_dir: Directory containing raw Envista hourly CSV files.
        unique_monitors: DataFrame with ``station_id`` and ``stations_tag``
            columns used to map Envista station IDs to site codes.

    Returns:
        Consolidated DataFrame. Empty DataFrame if no data is available.
    """
    print(f"  [PM2.5 hourly] Transforming AQS sample data for {year}...")
    aqs_df = transform_aqs_sample_hourly_for_year(
        year, raw_sample_dir, parameter_codes=_PM25_PARAM_CODES
    )

    print(f"  [PM2.5 hourly] Transforming Envista hourly data for {year}...")
    env_df = transform_env_hourly_for_year(year, raw_env_sample_dir, unique_monitors)

    frames = [df for df in (aqs_df, env_df) if not df.empty]

    if not frames:
        print(f"  [PM2.5 hourly] No data available for {year}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    if combined.empty:
        return pd.DataFrame()

    # Assign priority and sort so keep='first' retains the best record
    combined["_priority"] = _assign_priority(combined)
    combined = combined.sort_values(
        ["site_code", "date_local", "time_local", "_priority"],
        na_position="last",
    )

    # Deduplicate: one record per site-hour
    before = len(combined)
    combined = combined.drop_duplicates(
        subset=["site_code", "date_local", "time_local"],
        keep="first",
    )

    # Thin to hourly sample durations only
    combined = combined[combined["sample_duration_code"].isin(_SAMPLE_DURATION_CODES)]

    after = len(combined)

    combined = combined.drop(columns=["_priority"])

    # Ensure all output columns are present
    for col in _OUTPUT_COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA

    result = combined[_OUTPUT_COLUMNS].copy()

    print(
        f"  [PM2.5 hourly] {year}: {before} rows → {after} after deduplication"
    )

    return result


def run_consolidation(unique_monitors: pd.DataFrame) -> None:
    """Run PM2.5 hourly consolidation for all configured years.

    Args:
        unique_monitors: DataFrame with ``station_id`` and ``stations_tag``
            columns (loaded from raw Envista monitors CSV by the pipeline runner).
    """
    print("Starting PM2.5 Hourly Consolidation Pipeline")

    raw_sample_dir = config.RAW_AQS_SAMPLE
    raw_env_sample_dir = config.RAW_ENV_SAMPLE

    if not raw_sample_dir.exists():
        print(f"AQS sample directory not found: {raw_sample_dir}")
        print("Please run the AQS sample extraction pipeline first.")
        return

    staged_dir = config.ROOT / "staged" / "fct_pm25_hourly"
    staged_dir.mkdir(parents=True, exist_ok=True)

    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\nConsolidating PM2.5 hourly for year {year_str}...")

        consolidated_df = consolidate_pm25_hourly_for_year(
            year_str, raw_sample_dir, raw_env_sample_dir, unique_monitors
        )

        if consolidated_df.empty:
            print(f"  No consolidated data for {year_str}, skipping")
            continue

        output_path = staged_dir / f"fct_pm25_hourly_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"  Wrote {len(consolidated_df)} records to {output_path.name}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\nPM2.5 hourly consolidation complete!")
    print(f"Processed {years_processed} years with {total_records} total records")
    print(f"Output: staged/fct_pm25_hourly/fct_pm25_hourly_{{year}}.csv")


if __name__ == "__main__":
    import pandas as _pd

    _monitors_path = config.RAW_ENV_MONITORS / "envista_stations_monitors.csv"
    if not _monitors_path.exists():
        raise FileNotFoundError(f"Monitors file not found: {_monitors_path}")

    _monitors = _pd.read_csv(_monitors_path)
    _unique_monitors = _monitors[["station_id", "stations_tag"]].drop_duplicates()

    run_consolidation(_unique_monitors)
