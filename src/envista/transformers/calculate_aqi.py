"""
This module provides functions to calculate AQI values for Envista data.
"""

from __future__ import annotations
from typing import Any

import pandas as pd

def pm25_to_aqi_old(concentration: float) -> int | Any:
    """Convert PM2.5 concentration to AQI value based on AQI formula for data before May 6, 2024."""
    if pd.isna(concentration):
        return pd.NA
    if concentration <= 12.0:
        return round((50 / 12.0) * concentration)
    elif concentration <= 35.4:
        return round(((100 - 51) / (35.4 - 12.1)) * (concentration - 12.1) + 51)
    elif concentration <= 55.4:
        return round(((150 - 101) / (55.4 - 35.5)) * (concentration - 35.5) + 101)
    elif concentration <= 150.4:
        return round(((200 - 151) / (150.4 - 55.5)) * (concentration - 55.5) + 151)
    elif concentration <= 250.4:
        return round(((300 - 201) / (250.4 - 150.5)) * (concentration - 150.5) + 201)
    elif concentration <= 350.4:
        return round(((400 - 301) / (350.4 - 250.5)) * (concentration - 250.5) + 301)
    elif concentration <= 500.4:
        return round(((500 - 401) / (500.4 - 350.5)) * (concentration - 350.5) + 401)
    else:
        return 500

def pm25_to_aqi_new(concentration: float) -> int | Any:
    """Convert PM2.5 concentration to AQI value based on AQI formula for data after May 6, 2024."""
    if pd.isna(concentration):
        return pd.NA
    if concentration <= 9.0:
        return round((50 / 9.0) * concentration)
    elif concentration <= 35.4:
        return round(((100 - 51) / (35.4 - 9.1)) * (concentration - 9.1) + 51)
    elif concentration <= 55.4:
        return round(((150 - 101) / (55.4 - 35.5)) * (concentration - 35.5) + 101)
    elif concentration <= 125.4:
        return round(((200 - 151) / (125.4 - 55.5)) * (concentration - 55.5) + 151)
    elif concentration <= 225.4:
        return round(((300 - 201) / (225.4 - 125.5)) * (concentration - 125.5) + 201)
    elif concentration <= 350.4:
        return round(((400 - 301) / (350.4 - 225.5)) * (concentration - 225.5) + 301)
    elif concentration <= 500.4:
        return round(((500 - 401) / (500.4 - 350.5)) * (concentration - 350.5) + 401)
    else:
        return 500

def pm25_to_aqi_with_date_check(row: pd.Series) -> int | Any:
    """Apply appropriate AQI calculation based on date."""
    concentration = row["arithmetic_mean"]
    date_local = pd.to_datetime(row["date_local"])
    cutoff_date = pd.to_datetime("2024-05-06 00:00:00-0800")
    
    if date_local < cutoff_date:
        return pm25_to_aqi_old(concentration)
    else:
        return pm25_to_aqi_new(concentration)
        
def calculate_aqi(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate AQI values based on PM2.5 concentrations.

    This function applies the EPA AQI calculation formula for PM2.5
    to compute the AQI values for each record in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing 'arithmetic_mean' column with PM2.5 values.

    Returns:
        pd.DataFrame: DataFrame with an additional 'aqi' column.
    """

    df["aqi"] = df.apply(pm25_to_aqi_with_date_check, axis=1)
    return df