"""Transformers for Envista hourly (sample) data.

This module provides functions to transform raw Envista hourly PM2.5 data
into cleaned hourly records with a schema matching the AQS hourly fact table.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List

import pandas as pd


def _infer_group_store_from_filename(file_path: Path) -> str | None:
    """Infer the Envista group_store from a raw hourly filename."""
    match = re.match(r"^env_hourly_(.+)_(\d{4})$", file_path.stem, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


# Default fixed field values that align Envista data with AQS parameter conventions
_SAMPLE_DURATION_CODE = "1"
_SAMPLE_DURATION = "1 HOUR"
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
    pollutant_catalog: pd.DataFrame,
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
                group_store = _infer_group_store_from_filename(file_path)
                if group_store:
                    df["group_store"] = str(group_store)
                frames.append(df)
        except Exception as e:
            print(f"Warning: Failed to read {file_path}: {e}")
            continue

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    if combined.empty:
        return pd.DataFrame()

    pollutant_lookup = {}
    if not pollutant_catalog.empty:
        catalog = pollutant_catalog[
            ["group_store", "aqs_parameter_code", "aqs_parameter", "aqs_method_code", "aqs_method", "aqs_units"]
        ].drop_duplicates()
        for row in catalog.itertuples(index=False):
            pollutant_lookup[str(row.group_store).strip()] = {
                "parameter_code": str(row.aqs_parameter_code).strip() if pd.notna(row.aqs_parameter_code) else None,
                "parameter": str(row.aqs_parameter).strip() if pd.notna(row.aqs_parameter) else None,
                "method_code": str(row.aqs_method_code).strip() if pd.notna(row.aqs_method_code) else None,
                "method": str(row.aqs_method).strip() if pd.notna(row.aqs_method) else None,
                "units_of_measure": str(row.aqs_units).strip() if pd.notna(row.aqs_units) else None,
            }

    # Drop sentinel missing-value rows
    combined = combined[combined["data_channels_value"] != -9999].copy()

    if combined.empty:
        return pd.DataFrame()

    merged = pd.merge(
        combined,
        unique_monitors[["station_id", "stations_tag"]],
        how="left",
        left_on="stationId",
        right_on="station_id",
    )

    merged["group_store"] = merged["group_store"].astype(str)
    if pollutant_lookup:
        merged["parameter_code"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("parameter_code", None)
        )
        merged["parameter"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("parameter", None)
        )
        merged["method_code"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("method_code", None)
        )
        merged["method"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("method", None)
        )
        merged["units_of_measure"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("units_of_measure", None)
        )
    else:
        merged["parameter_code"] = None
        merged["parameter"] = None
        merged["method_code"] = None
        merged["method"] = None
        merged["units_of_measure"] = None

    dt = pd.to_datetime(merged["data_datetime"], errors="coerce")
    merged["date_local"] = dt.dt.strftime("%Y-%m-%d")
    merged["time_local"] = dt.dt.strftime("%H:%M")

    merged = merged[merged["data_channels_valid"] == "TRUE"]

    merged["validity_indicator"] = merged["data_channels_valid"].map(
        {True: "Y", False: "N", "True": "Y", "False": "N", 1: "Y", 0: "N"}
    )

    merged["poc"] = _POC
    merged["sample_duration_code"] = _SAMPLE_DURATION_CODE
    merged["sample_duration"] = _SAMPLE_DURATION
    merged["qualifier"] = pd.NA
    merged["source"] = _SOURCE

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
    pollutant_catalog: pd.DataFrame,
) -> pd.DataFrame:
    """Transform Envista hourly PM2.5 data for a specific year.

    Globs all files matching env_hourly_pm25_{year}.csv in raw_env_sample_dir,
    then delegates to transform_env_hourly.

    Args:
        year: Four-digit year string (e.g. "2023").
        raw_env_sample_dir: Directory containing raw Envista hourly CSV files.
        unique_monitors: DataFrame with ``station_id`` and ``stations_tag`` columns.
        pollutant_catalog: DataFrame containing pollutant information.

    Returns:
        Transformed DataFrame for the year.
    """
    pattern = f"env_hourly_pm25_{year}.csv"
    raw_files = list(raw_env_sample_dir.glob(pattern))

    if not raw_files:
        print(f"  No Envista hourly files found for year {year} in {raw_env_sample_dir}")
        return pd.DataFrame()

    print(f"  Found {len(raw_files)} Envista hourly file(s) for year {year}")

    return transform_env_hourly(raw_files, unique_monitors, pollutant_catalog)
