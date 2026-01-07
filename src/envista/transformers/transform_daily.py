"""Transformers for Envista daily data.

This module provides functions to transform raw Envista daily summary data
into cleaned, deduplicated records with standardized fields.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd


def transform_aqi_daily(raw_daily_files: List[Path]) -> pd.DataFrame:
    """Transform raw Envista daily data into cleaned records.

    Reads multiple daily files (typically for different pollutants in the same year),
    concatenates them, creates site_code, selects specific fields, removes duplicates,
    and filters out records with empty AQI values.

    Args:
        raw_daily_files: List of paths to raw daily CSV files

    Returns:
        Transformed DataFrame with selected fields and deduplicated records
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

    combined = combined.drop(columns=["summary", "count"], errors="ignore")

    # Define the fields to keep (in the order requested)
    fields_to_keep = [
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
        "site_code",
    ]

    # Filter to only the fields that exist in the data
    available_fields = [field for field in fields_to_keep if field in combined.columns]
    if not available_fields:
        raise ValueError(
            "None of the requested fields are present in the raw daily data"
        )

    # Select the available fields
    transformed = combined[available_fields].copy()

    # Filter out records with empty AQI values
    transformed = transformed.dropna(subset=["aqi"])
    transformed = transformed[transformed["aqi"] != ""]

    # Remove exact duplicate records after field selection
    original_count = len(transformed)
    transformed = transformed.drop_duplicates()
    deduped_count = len(transformed)

    print(
        f"✅ Processed AQI daily data: {original_count} → {deduped_count} records after deduplication and AQI filtering"
    )

    return transformed

def transform_aqi_daily_for_year(year: str, raw_daily_dir: Path) -> pd.DataFrame:
    """Transform AQI daily data for a specific year.

    Finds all daily files for the given year, combines them, and applies transformations.

    Args:
        year: Year string (e.g., "2023")
        raw_daily_dir: Directory containing raw daily files

    Returns:
        Transformed DataFrame for the year
    """
    # Find all daily files for this year
    # Files are named like aqs_daily_{pollutant}_{year}.csv
    pattern = f"aqs_daily_*_{year}.csv"
    daily_files = list(raw_daily_dir.glob(pattern))

    if not daily_files:
        print(f"⚠️  No daily files found for year {year}")
        return pd.DataFrame()

    print(f"📊 Found {len(daily_files)} daily files for year {year}")
    for file_path in daily_files:
        print(f"  📄 {file_path.name}")

    return transform_aqi_daily(daily_files)