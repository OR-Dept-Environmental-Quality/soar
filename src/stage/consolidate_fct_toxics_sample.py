"""Toxics Sample Consolidation Script.

This script consolidates sample toxics data from the transform layer into the staged layer.
It reads TRV sample transform data and creates fact tables for toxics sample data,
excluding geographic fields (latitude, longitude, county) to maintain proper separation
between measurement data and site dimension data.

Output: soar/staged/fct_toxics_sample_YYYY.csv files
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


def consolidate_toxics_sample_for_year(year: str, transform_dir: Path) -> pd.DataFrame:
    """Consolidate toxics sample data for a specific year.

    Reads the transformed TRV sample data and prepares it for the staged layer by
    selecting required fields and excluding geographic coordinates.

    Args:
        year: Year string (e.g., "2023")
        transform_dir: Directory containing transformed TRV sample files

    Returns:
        Consolidated DataFrame with toxics sample fact data
    """
    # Read the transformed data for this year
    input_file = transform_dir / f"trv_sample_{year}.csv"
    if not input_file.exists():
        print(f"⚠️  No TRV sample file found for year {year}: {input_file}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"❌ Error reading {input_file}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠️  Empty TRV sample file for year {year}")
        return pd.DataFrame()

    # Define the required columns for fct_toxics_sample fact table
    # Geographic fields (latitude, longitude, county) are excluded as they belong in dim_sites
    required_columns = [
        'site_code',
        'parameter_code',
        'poc',
        'parameter',
        'date_local',
        'sample_measurement',
        'units_of_measure',
        'sample_measurement_ug_m3',
        'trv_cancer',
        'trv_noncancer',
        'trv_acute',
        'xtrv_cancer',
        'xtrv_noncancer',
        'xtrv_acute',
        'qualifier',
        'sample_duration',
        'sample_frequency',
        'detection_limit',
        'uncertainty',
        'method_type',
        'method',
        'method_code'
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
    
    print(f"✅ Consolidated toxics sample data for year {year}: {len(result)} records")
    print(f"   📊 Columns: {len(available_columns)} available, {len(missing_columns)} missing")
    
    return result


def run_consolidation():
    """Run the toxics sample consolidation pipeline."""
    print("🚀 Starting Toxics Sample Consolidation Pipeline")

    # Input directory (transformed TRV sample data)
    transform_dir = config.ROOT / "transform" / "trv" / "sample"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the toxics sample transformation pipeline first.")
        return

    # Output directory (staged toxics sample data)
    staged_dir = config.ROOT / "staged" / "fct_toxics_sample"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Consolidating year {year_str}...")

        # Consolidate data for this year
        consolidated_df = consolidate_toxics_sample_for_year(year_str, transform_dir)

        if consolidated_df.empty:
            print(f"⚠️  No consolidated data for year {year_str}, skipping")
            continue

        # Write to staged layer
        output_path = staged_dir / f"fct_toxics_sample_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"✅ Wrote {len(consolidated_df)} toxics sample records to {output_path}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\n🎉 Toxics sample consolidation complete!")
    print(f"📊 Processed {years_processed} years with {total_records} total records")
    
    if years_processed > 0:
        print(f"📁 Output files: soar/staged/fct_toxics_sample/fct_toxics_sample_{{year}}.csv")
        print("🚫 Excluded geographic fields: latitude, longitude, county")


if __name__ == "__main__":
    run_consolidation()