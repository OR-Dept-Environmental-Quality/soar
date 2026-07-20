"""Transformers for AQS hourly (sample) data.

This module provides functions to transform raw AQS sample data into cleaned
hourly records with standardized fields suitable for diurnal analysis.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pandas as pd

# Columns to carry through to the staged hourly schema
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


def transform_aqs_sample_hourly(
    raw_files: List[Path],
    parameter_codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Transform raw AQS sample files into hourly records.

    Reads one or more raw sample CSV files (typically covering different
    parameters for the same year), concatenates them, builds site_code,
    optionally filters to specific parameter codes, selects the hourly
    schema columns, and removes exact duplicate rows.

    No validity_indicator filtering is applied — all records are kept.

    Args:
        raw_files: List of paths to raw AQS sample CSV files.
        parameter_codes: Optional list of AQS parameter code strings to keep
            (e.g. ["88101", "88502"]). Pass None to keep all parameters.

    Returns:
        Transformed DataFrame with the hourly schema columns. Empty DataFrame
        if no data was found or no files could be read.
    """
    if not raw_files:
        return pd.DataFrame()

    frames = []
    for file_path in raw_files:
        try:
            df = pd.read_csv(file_path, dtype=str)
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

    # Build site_code: zero-padded state(2) + county(3) + site_number(4)
    # Default state to Oregon (41) if missing or non-numeric
    combined["_state"] = (
        pd.to_numeric(combined.get("state_code", pd.Series(dtype="float")), errors="coerce")
        .fillna(41)
        .astype(int)
    )
    combined["_county"] = (
        pd.to_numeric(combined.get("county_code", pd.Series(dtype="float")), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    combined["_site"] = (
        pd.to_numeric(combined.get("site_number", pd.Series(dtype="float")), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    combined["site_code"] = (
        combined["_state"].astype(str).str.zfill(2)
        + combined["_county"].astype(str).str.zfill(3)
        + combined["_site"].astype(str).str.zfill(4)
    )
    combined = combined.drop(columns=["_state", "_county", "_site"])

    # Optional parameter filter
    if parameter_codes is not None:
        combined = combined[
            combined["parameter_code"].isin(parameter_codes)
        ].copy()

    if combined.empty:
        return pd.DataFrame()

    ## Add hourly filter
    before_count = len(combined)
    combined = combined[combined["sample_duration_code"] == "1"]
    print(f"  Filtered to {len(combined)} hourly records (from {before_count})")
    
    if combined.empty:
        return pd.DataFrame()
    

    combined["source"] = "AQS"

    # Select output columns; fill missing schema columns with NA
    available = [c for c in _OUTPUT_COLUMNS if c in combined.columns]
    missing = [c for c in _OUTPUT_COLUMNS if c not in combined.columns]

    result = combined[available].copy()
    for col in missing:
        result[col] = pd.NA

    result = result[_OUTPUT_COLUMNS]
    result = result.drop_duplicates()

    print(
        f"  Transformed {len(result)} AQS hourly records"
        + (f" for parameters {parameter_codes}" if parameter_codes else "")
    )

    return result


def transform_aqs_sample_hourly_for_year(
    year: str,
    raw_sample_dir: Path,
    parameter_codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Transform AQS hourly sample data for a specific year.

    Globs all files matching aqs_sample_*_{year}.csv in raw_sample_dir,
    then delegates to transform_aqs_sample_hourly.

    Args:
        year: Four-digit year string (e.g. "2023").
        raw_sample_dir: Directory containing raw AQS sample CSV files.
        parameter_codes: Optional list of parameter code strings to keep.

    Returns:
        Transformed DataFrame for the year.
    """
    pattern = f"aqs_sample_*_{year}.csv"
    raw_files = list(raw_sample_dir.glob(pattern))

    if not raw_files:
        print(f"  No AQS sample files found for year {year} in {raw_sample_dir}")
        return pd.DataFrame()

    print(f"  Found {len(raw_files)} AQS sample file(s) for year {year}")

    return transform_aqs_sample_hourly(raw_files, parameter_codes)
