"""Transformers for Envista dataframes.

This module provides functions to transform Envista raw data
into cleaned records with a dataframe schema matching AQS.
"""

from __future__ import annotations
from pathlib import Path

from .calculate_aqi import calculate_aqi
import pandas as pd

def transform_env_daily(year: str, raw_daily_files: list[Path], unique_monitors: pd.DataFrame) -> pd.DataFrame:
    """Transform raw Envista daily data for a given year.

    This function reads the raw daily data from the specified input path,
    merges it with unique monitor information, applies necessary transformations,
    and returns a cleaned DataFrame.

    Args:
        input_path (Path): Path to the raw daily data CSV file.
        unique_monitors (pd.DataFrame): DataFrame containing unique monitor information.

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
                frames.append(df)
        except Exception as e:
            print(f"Warning: Failed to read {file_path}: {e}")
            continue

    if not frames:
        return pd.DataFrame()

    # Concatenate all data
    combined = pd.concat(frames, ignore_index=True)

    if combined.empty:
        return pd.DataFrame()
    
    # Join monitor data with measurement data
    combined = combined[combined["data_channels_value"] != -9999]
    merged_df = pd.merge(combined, unique_monitors, how = "left", left_on="stationId", right_on="station_id")

    # Select and rename columns
    merged_df = merged_df[["data_datetime", "data_channels_name", "data_channels_value", "data_channels_valid", "stations_tag"]]
    transformed_df = merged_df.rename(columns={
        "data_datetime": "date_local", 
        "data_channels_name":"method", 
        "data_channels_value": "arithmetic_mean",
        "data_channels_valid":"validity_indicator",
        "stations_tag":"site_code"})
    
    # Standardize date_local format to match AQS (YYYY-MM-DD)
    transformed_df["date_local"] = pd.to_datetime(transformed_df["date_local"]).dt.strftime("%Y-%m-%d")
    
    # Add and populate columns to match AQS schema
    transformed_df["poc"] = 99 # Dummy value to distinguish from AQS data
    transformed_df["parameter_code"] = 88502 # True AQS code for non-regulatory PM2.5 data
    transformed_df["parameter"] = "Acceptable PM2.5 AQI & Speciation Mass" # True AQS description for non-regulatory PM2.5 data
    transformed_df["sample_duration_code"] = "X" #True AQS code for calculated daily PM2.5 
    transformed_df["sample_duration"] = "24-HR BLK AVG" # True AQS description for calculated daily PM2.5 data
    transformed_df["units_of_measure"] = "Micrograms per cubic meter" # True AQS units for PM2.5 data
    transformed_df["event_type"] = "No Events" # True AQS default for days impacted by exceptional events; not applicable to non-regulatory data
    transformed_df["method_code"] = 999 # Dummy value to distinguish from AQS data
    transformed_df["method"] = "SensOR PM2.5 Monitor" # Custom description for PM2.5 data from SensOR
    transformed_df["aqi"] = pd.NA
    transformed_df["observation_count"] = pd.NA
    transformed_df["observation_percent"] = pd.NA
    transformed_df["first_max_value"] = pd.NA
    transformed_df["first_max_hour"] = pd.NA

    # Map validity indicator from boolean to "Y"/"N"
    transformed_df["validity_indicator"] = transformed_df["validity_indicator"].map({True: "Y", False: "N"})

    # Calculate AQI values
    final_df = calculate_aqi(transformed_df)

    return final_df[["parameter_code", "poc", "parameter", "sample_duration_code", "sample_duration", 
                "date_local", "units_of_measure", "event_type", "observation_count", "observation_percent", "validity_indicator", 
                "arithmetic_mean", "first_max_value", "first_max_hour", "aqi", "method_code", "method", "site_code"]]

def transform_env_daily_for_year(year: str, raw_daily_dir: Path, unique_monitors: pd.DataFrame) -> pd.DataFrame:
    """Transform Envista daily data for a specific year.

    Finds all daily files for the given year, combines them, and applies transformations.

    Args:
        year: Year string (e.g., "2023")
        raw_daily_dir: Directory containing raw daily files

    Returns:
        Transformed DataFrame for the year
    """
    # Find all daily files for this year
    # Files are named like env_daily_{pollutant}_{year}.csv
    pattern = f"env_daily_*_{year}.csv"
    daily_files = list(raw_daily_dir.glob(pattern))

    if not daily_files:
        print(f"No daily files found for year {year}")
        return pd.DataFrame()

    print(f"Found {len(daily_files)} daily files for year {year}")
    for file_path in daily_files:
        print(f"{file_path.name}")

    return transform_env_daily(year, daily_files, unique_monitors)
