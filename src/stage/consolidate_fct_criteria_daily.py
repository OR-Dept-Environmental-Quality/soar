"""Criteria Daily Consolidation Script.

This script consolidates daily criteria pollutant data from the transform layer into the staged layer.
It reads AQI daily transform data and creates fact tables for criteria daily data,
excluding geographic fields (latitude, longitude, county) to maintain proper separation
between measurement data and site dimension data.

Output: soar/staged/fct_criteria_daily_YYYY.csv files
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config


def consolidate_criteria_daily_for_year(year: str, transform_dir: Path) -> pd.DataFrame:
    """Consolidate criteria daily data for a specific year.

    Reads the transformed AQI daily data and prepares it for the staged layer by
    selecting required fields and excluding geographic coordinates.

    Args:
        year: Year string (e.g., "2023")
        transform_dir: Directory containing transformed AQI daily files

    Returns:
        Consolidated DataFrame with criteria daily fact data
    """
    # Find all transformed data files matching the pattern for this year
    pattern = f"*aqi*{year}.csv"
    matching_files = list(transform_dir.glob(pattern))
    
    if not matching_files:
        print(f"⚠️  No AQI daily files found for year {year} matching pattern: {pattern}")
        return pd.DataFrame()

    print(f"   📂 Found {len(matching_files)} file(s) matching pattern for {year}")
    
    dfs = []
    for input_file in matching_files:
        try:
            file_df = pd.read_csv(input_file)
            print(f"   ✓ Read {len(file_df)} records from {input_file.name}")
            dfs.append(file_df)
        except Exception as e:
            print(f"❌ Error reading {input_file}: {e}")
            continue
    
    if not dfs:
        print(f"⚠️  No files were successfully read for year {year}")
        return pd.DataFrame()
    
    # Combine all dataframes
    df = pd.concat(dfs, ignore_index=True)
    
    if df.empty:
        print(f"⚠️  All combined files are empty for year {year}")
        return pd.DataFrame()

    # Define the required columns for fct_criteria_daily fact table
    # Geographic fields (latitude, longitude, county) are excluded as they belong in dim_sites
    required_columns = [
        'parameter_code',
        'poc',
        'parameter',
        'sample_duration_code',
        'sample_duration',
        'date_local',
        'units_of_measure',
        'event_type',
        'observation_count',
        'observation_percent',
        'validity_indicator',
        'arithmetic_mean',
        'first_max_value',
        'first_max_hour',
        'aqi',
        'method_code',
        'method',
        'site_code'
    ]

    # Check which required columns exist in the transform data
    available_columns = []
    missing_columns = []
    
    for col in required_columns:
        if col in df.columns:
            available_columns.append(col)
        else:
            missing_columns.append(col)
    
    if missing_columns:
        print(f"⚠️  Missing columns in year {year}: {missing_columns}")
    
    # Select available columns
    if not available_columns:
        print(f"❌ No required columns found for year {year}")
        return pd.DataFrame()
    
    result = df[available_columns].copy()
    
    # Add any missing columns with null values
    for col in missing_columns:
        result[col] = pd.NA
    
    # Reorder columns to match the required schema
    result = result[required_columns]
    
    # Remove any rows that are completely empty
    result = result.dropna(how='all')
    
    print(f"✅ Consolidated criteria daily data for year {year}: {len(result)} records")
    print(f"   📊 Columns: {len(available_columns)} available, {len(missing_columns)} missing")
    
    return result


def run_consolidation():
    """Run the criteria daily consolidation pipeline."""
    print("🚀 Starting Criteria Daily Consolidation Pipeline")

    # Input directory (transformed AQI daily data)
    transform_dir = config.ROOT / "transform" / "aqi"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the AQI daily transformation pipeline first.")
        return

    # Output directory (staged criteria daily data)
    staged_dir = config.ROOT / "staged" / "fct_criteria_daily"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Consolidating year {year_str}...")

        # Consolidate data for this year
        consolidated_df = consolidate_criteria_daily_for_year(year_str, transform_dir)

        if consolidated_df.empty:
            print(f"⚠️  No consolidated data for year {year_str}, skipping")
            continue

        # Write to staged layer
        output_path = staged_dir / f"fct_criteria_daily_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"✅ Wrote {len(consolidated_df)} criteria daily records to {output_path}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\n🎉 Criteria daily consolidation complete!")
    print(f"📊 Processed {years_processed} years with {total_records} total records")
    
    if years_processed > 0:
        print(f"📁 Output files: soar/staged/fct_criteria_daily/fct_criteria_daily_{{year}}.csv")
        print("🚫 Excluded geographic fields: latitude, longitude, county")


if __name__ == "__main__":
    run_consolidation()