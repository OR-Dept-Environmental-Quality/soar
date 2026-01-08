"""Transformers for Envista dataframes.

This module provides functions to transform Envista raw data
into cleaned records with a dataframe schema matching AQS.
"""

from __future__ import annotations
from pathlib import Path

from .calculate_aqi import calculate_aqi
import pandas as pd

def transform_env_df(input_path: Path, unique_monitors: pd.DataFrame) -> pd.DataFrame:
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

    df = pd.read_csv(input_path)
    merged_df = pd.merge(df, unique_monitors, how = "left", left_on="stationId", right_on="station_id")
    
    merged_df = merged_df[["data_datetime", "data_channels_name", "data_channels_value", "data_channels_valid", "station_id"]]

    merged_df = merged_df.rename(columns={
        "data_datetime": "date_local", 
        "data_channels_name":"method", 
        "data_channels_value": "arithmetic_mean",
        "data_channels_valid":"validity_indicator",
        "stations_tag":"site_code"})
    
    merged_df["poc"] = "99"
    merged_df["parameter_code"] = "88502"
    merged_df["parameter"] = "Acceptable PM2.5 AQI & Speciation Mass"
    merged_df["sample_duration_code"] = "1"
    merged_df["sample_duration"] = "1 HOUR"
    merged_df["units_of_measure"] = "Micrograms per cubic meter"
    merged_df["event_type"] = "No Events"
    merged_df["method_code"] = "999"
    merged_df["method"] = "SensOR PM2.5 Monitor"
    merged_df["aqi"] = pd.NA
    merged_df["observation_count"] = pd.NA
    merged_df["observation_percent"] = pd.NA
    merged_df["first_max_value"] = pd.NA
    merged_df["first_max_hour"] = pd.NA

    final_df = calculate_aqi(merged_df)

    return final_df[["parameter_code", "poc", "parameter", "sample_duration_code", "sample_duration", 
                "date_local", "units_of_measure", "event_type", "observation_count", "observation_percent", "validity_indicator", 
                "arithmetic_mean", "first_max_value", "first_max_hour", "aqi", "method_code", "method", "site_code"]]