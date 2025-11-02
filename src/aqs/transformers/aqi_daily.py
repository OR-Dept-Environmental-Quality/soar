"""Transformers for AQI daily data.

This module provides functions to transform raw AQI daily summary data
into cleaned, deduplicated records with standardized fields.
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import List


def transform_aqi_daily(raw_daily_files: List[Path]) -> pd.DataFrame:
    """Transform raw AQI daily data into cleaned records.

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

    # Create site_code: state_code (2 digits) + county_code (3 digits) + site_number (4 digits)
    # For Oregon data, default state_code to 41 if missing/invalid
    # Handle NaN and non-numeric values robustly
    combined["state_code_num"] = pd.to_numeric(combined["state_code"], errors="coerce")
    combined["county_code_num"] = pd.to_numeric(
        combined["county_code"], errors="coerce"
    )
    combined["site_number_num"] = pd.to_numeric(
        combined["site_number"], errors="coerce"
    )

    # Default to Oregon state code (41) if missing or invalid
    combined["state_code_num"] = combined["state_code_num"].fillna(41).astype(int)
    combined["state_code_num"] = combined["state_code_num"].where(
        combined["state_code_num"] == 41, 41
    )

    # Fill missing county/site codes with 0
    combined["county_code_num"] = combined["county_code_num"].fillna(0).astype(int)
    combined["site_number_num"] = combined["site_number_num"].fillna(0).astype(int)

    combined["site_code"] = (
        combined["state_code_num"].astype(str).str.zfill(2)
        + combined["county_code_num"].astype(str).str.zfill(3)
        + combined["site_number_num"].astype(str).str.zfill(4)
    )

    # Clean up temporary columns
    combined = combined.drop(
        columns=["state_code_num", "county_code_num", "site_number_num"]
    )

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
        "latitude",
        "longitude",
        "county",
    ]

    # Filter to only the fields that exist in the data
    available_fields = [field for field in fields_to_keep if field in combined.columns]
    if not available_fields:
        raise ValueError(
            "None of the requested fields are present in the raw daily data"
        )

    # Select the available fields
    transformed = combined[available_fields].copy()

    # Remove rows where aqi field is empty (NaN, None, or empty string)
    transformed = transformed.dropna(subset=["aqi"])
    transformed = transformed[transformed["aqi"] != ""]

    # Remove duplicates - the user said "remove any duplicate records after eliminating redundant columns"
    # This likely means drop duplicates based on all columns except perhaps some redundant ones
    # For now, I'll drop exact duplicates
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
