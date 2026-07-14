"""Transformers for Envista hourly (sample) data.

This module provides functions to transform raw Envista hourly PM2.5 data
into cleaned hourly records with a schema matching the AQS hourly fact table.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

# Fixed field values that align Envista data with AQS parameter conventions
_PARAMETER_CODE = "88502"
_POC = 99
_PARAMETER = "Acceptable PM2.5 AQI & Speciation Mass"
_SAMPLE_DURATION_CODE = "1"
_SAMPLE_DURATION = "1 HOUR"
_UNITS = "Micrograms per cubic meter (LC)"
_METHOD_CODE = 999
_METHOD = "SensOR PM2.5 Monitor"
_SOURCE = "Envista"

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


def transform_env_hourly(
    raw_files: List[Path],
    unique_monitors: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw Envista hourly PM2.5 files into hourly records.

    Reads one or more raw Envista hourly CSV files, filters out sentinel
    -9999 values, joins to monitor metadata to obtain site_code, splits
    the datetime into date_local and time_local, maps the validity flag,
    and populates fixed AQS-convention fields.

    No validity_indicator filtering is applied — all records are kept.

    Args:
        raw_files: List of paths to raw Envista hourly CSV files.
        unique_monitors: DataFrame with at least columns ``station_id``
            and ``stations_tag`` (the AQS-formatted site_code).

    Returns:
        Transformed DataFrame with the hourly schema columns. Empty DataFrame
        if no data was found or no files could be read.
    """
    if not raw_files:
        return pd.DataFrame()

    frames = []
    for file_path in raw_files:
        try:
            df = pd.read_csv(file_path)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"Warning: Failed to read {file_path}: {e}")
            continue

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    if combined.empty:
        return pd.DataFrame()

    # Drop sentinel missing-value rows
    combined = combined[combined["data_channels_value"] != -9999].copy()

    if combined.empty:
        return pd.DataFrame()

    # Join monitor metadata to obtain stations_tag (site_code)
    merged = pd.merge(
        combined,
        unique_monitors[["station_id", "stations_tag"]],
        how="left",
        left_on="stationId",
        right_on="station_id",
    )

    # Parse datetime; extract date and time components
    dt = pd.to_datetime(merged["data_datetime"], errors="coerce")
    merged["date_local"] = dt.dt.strftime("%Y-%m-%d")
    merged["time_local"] = dt.dt.strftime("%H:%M")

    # Map boolean validity to Y/N strings
    merged["validity_indicator"] = merged["data_channels_value"].map(
        lambda _: pd.NA  # placeholder; overwritten below
    )
    merged["validity_indicator"] = merged["data_channels_valid"].map(
        {True: "Y", False: "N", "True": "Y", "False": "N", 1: "Y", 0: "N"}
    )

    # Populate fixed fields
    merged["parameter_code"] = _PARAMETER_CODE
    merged["poc"] = _POC
    merged["parameter"] = _PARAMETER
    merged["sample_duration_code"] = _SAMPLE_DURATION_CODE
    merged["sample_duration"] = _SAMPLE_DURATION
    merged["units_of_measure"] = _UNITS
    merged["method_code"] = _METHOD_CODE
    merged["method"] = _METHOD
    merged["qualifier"] = pd.NA
    merged["source"] = _SOURCE

    # Rename to output schema names
    merged = merged.rename(
        columns={
            "data_channels_value": "sample_measurement",
            "stations_tag": "site_code",
        }
    )

    result = merged[_OUTPUT_COLUMNS].copy()
    result = result.drop_duplicates()

    print(f"  Transformed {len(result)} Envista hourly records")

    return result


def transform_env_hourly_for_year(
    year: str,
    raw_env_sample_dir: Path,
    unique_monitors: pd.DataFrame,
) -> pd.DataFrame:
    """Transform Envista hourly PM2.5 data for a specific year.

    Globs all files matching env_hourly_pm25_{year}.csv in raw_env_sample_dir,
    then delegates to transform_env_hourly.

    Args:
        year: Four-digit year string (e.g. "2023").
        raw_env_sample_dir: Directory containing raw Envista hourly CSV files.
        unique_monitors: DataFrame with ``station_id`` and ``stations_tag`` columns.

    Returns:
        Transformed DataFrame for the year.
    """
    pattern = f"env_hourly_pm25_{year}.csv"
    raw_files = list(raw_env_sample_dir.glob(pattern))

    if not raw_files:
        print(f"  No Envista hourly files found for year {year} in {raw_env_sample_dir}")
        return pd.DataFrame()

    print(f"  Found {len(raw_files)} Envista hourly file(s) for year {year}")

    return transform_env_hourly(raw_files, unique_monitors)
