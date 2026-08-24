"""Transformers for Envista dataframes.

This module provides functions to transform Envista raw data
into cleaned records with a dataframe schema matching AQS.
"""

from __future__ import annotations
from pathlib import Path
import re

from .calculate_aqi import calculate_aqi
import pandas as pd

# Default fixed field values that align Envista data with AQS parameter conventions
_POC = 999
_SAMPLE_DURATION_CODE = "X"
_SAMPLE_DURATION = "24-HR BLK AVG"
_EVENT_TYPE = pd.NA
_OBSERVATION_COUNT = pd.NA
_OBSERVATION_PERCENT = pd.NA
_FIRST_MAX_VALUE = pd.NA
_FIRST_MAX_HOUR = pd.NA
_SOURCE = "Envista"

_OUTPUT_COLUMNS = [
    "site_code",
    "parameter_code",
    "poc",
    "parameter",
    "sample_duration_code",
    "sample_duration",
    "date_local",
    "units_of_measure",
    "event_type",
    "observation_count",
    "observation_percent",
    "validity_indicator",
    "arithmetic_mean",
    "first_max_value",
    "first_max_hour",
    "aqi",
    "method_code",
    "method",
    "source"
]

def _infer_group_store_from_filename(file_path: Path) -> str | None:
    """Infer the Envista group_store from a raw sample filename."""
    match = re.match(r"^env_daily_(.+)_(\d{4})$", file_path.stem, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def transform_env_daily(raw_daily_files: list[Path], unique_monitors: pd.DataFrame, pollutant_catalog: pd.DataFrame) -> pd.DataFrame:
    """Transform raw Envista daily data for a given year.

    This function reads the raw daily data from the specified input path,
    merges it with unique monitor information, applies necessary transformations,
    and returns a cleaned DataFrame.

    Args:
        input_path (Path): Path to the raw daily data CSV file.
        unique_monitors (pd.DataFrame): DataFrame containing unique monitor information.
        pollutant_catalog (pd.DataFrame): DataFrame containing pollutant information.

    Returns:
        pd.DataFrame: Transformed and cleaned DataFrame.
    """

    if not raw_daily_files:
        return pd.DataFrame()

    # Read and concatenate all files
    frames = []
    for file_path in raw_daily_files:
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
                "parameter_code": str(row.aqs_parameter_code).strip() if pd.notna(row.aqs_parameter_code) else "No value",
                "parameter": str(row.aqs_parameter).strip() if pd.notna(row.aqs_parameter) else "No value",
                "method_code": str(row.aqs_method_code).strip() if pd.notna(row.aqs_method_code) else "No value",
                "method": str(row.aqs_method).strip() if pd.notna(row.aqs_method) else "No value",
                "units_of_measure": str(row.aqs_units).strip() if pd.notna(row.aqs_units) else "No value",
            }

    # Drop sentinel missing-value rows
    combined = combined[combined["data_channels_valid"] != False].copy()

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
            lambda value: pollutant_lookup.get(value, {}).get("parameter_code")
        )
        merged["parameter"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("parameter")
        )
        merged["method_code"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("method_code")
        )
        merged["method"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("method")
        )
        merged["units_of_measure"] = merged["group_store"].map(
            lambda value: pollutant_lookup.get(value, {}).get("units_of_measure")
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
    merged["event_type"] = _EVENT_TYPE
    merged["observation_count"] = _OBSERVATION_COUNT
    merged["observation_percent"] = _OBSERVATION_PERCENT
    merged["first_max_value"] = _FIRST_MAX_VALUE
    merged["first_max_hour"] = _FIRST_MAX_HOUR

    merged = merged.rename(
        columns={
            "data_channels_value": "sample_measurement",
            "stations_tag": "site_code",
        }
    )

    result = merged[_OUTPUT_COLUMNS].copy()
    result = result.drop_duplicates()

    print(f"  Transformed {len(result)} Envista sample records")

    # Calculate AQI values
    final_df = calculate_aqi(transformed_df)

    return result

def transform_env_daily_for_year(
    year: str,
    raw_daily_dir: Path,
    unique_monitors: pd.DataFrame,
    pollutant_catalog: pd.DataFrame | None = None,
    requested_group_stores: list[str] | None = None,
) -> pd.DataFrame:
    """Transform Envista daily data for a specific year.

    Finds all daily files for the given year, combines them, and applies transformations.

    Args:
        year: Year string (e.g., "2023")
        raw_daily_dir: Directory containing raw daily files
        unique_monitors: Monitor metadata keyed by station_id
        pollutant_catalog: Optional pollutant catalog used to filter by group_store
        requested_group_stores: Optional subset of group_store names to include

    Returns:
        Transformed DataFrame for the year
    """
    # Find all daily files for this year
    # Files are named like env_daily_{pollutant}_{year}.csv
    pattern = f"env_daily_*_{year}.csv"
    daily_files = list(raw_daily_dir.glob(pattern))

    if requested_group_stores:
        requested_norm = {value.casefold() for value in requested_group_stores}
        daily_files = [
            file_path for file_path in daily_files
            if any(
                group_name.casefold() in file_path.name.casefold()
                for group_name in requested_norm
            )
        ]

    if not daily_files:
        print(f"No daily files found for year {year}")
        return pd.DataFrame()

    print(f"Found {len(daily_files)} daily files for year {year}")
    for file_path in daily_files:
        print(f"{file_path.name}")

    return transform_env_daily(daily_files, unique_monitors, pollutant_catalog)
