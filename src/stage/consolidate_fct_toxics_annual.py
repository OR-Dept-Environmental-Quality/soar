"""Toxics Annual Consolidation Script.

This script consolidates annual toxics data from the transform layer into the staged layer.
It reads TRV annual transform data and creates fact tables for toxics annual data,
excluding geographic fields (latitude, longitude, county) to maintain proper separation
between measurement data and site dimension data.

Output: soar/staged/fct_toxics_annual_YYYY.csv files
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


def consolidate_toxics_annual_for_year(year: str, transform_dir: Path) -> pd.DataFrame:
    """Consolidate toxics annual data for a specific year.

    Reads the transformed TRV annual data and prepares it for the staged layer by
    selecting required fields and excluding geographic coordinates.

    Args:
        year: Year string (e.g., "2023")
        transform_dir: Directory containing transformed TRV annual files

    Returns:
        Consolidated DataFrame with toxics annual fact data
    """
    # Read the transformed data for this year
    input_file = transform_dir / f"trv_annual_{year}.csv"
    if not input_file.exists():
        print(f"⚠️  No TRV annual file found for year {year}: {input_file}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"❌ Error reading {input_file}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠️  Empty TRV annual file for year {year}")
        return pd.DataFrame()

    # Define the required columns for fct_toxics_annual fact table
    # Geographic fields (latitude, longitude, county) are excluded as they belong in dim_sites
    required_columns = [
        'site_code',
        'parameter',
        'sample_duration',
        'parameter_code',
        'poc',
        'method',
        'year',
        'units_of_measure',
        'observation_count',
        'observation_percent',
        'validity_indicator',
        'valid_day_count',
        'required_day_count',
        'exceptional_data_count',
        'null_observation_count',
        'primary_exceedance_count',
        'secondary_exceedance_count',
        'certification_indicator',
        'arithmetic_mean',
        'arithmetic_mean_ug_m3',
        'ugm3_converted',
        'xtrv_cancer',
        'xtrv_noncancer',
        'standard_deviation',
        'first_max_value',
        'first_max_value_ug_m3',
        'xtrv_acute_first',
        'first_max_datetime',
        'second_max_value',
        'second_max_value_ug_m3',
        'xtrv_acute_second',
        'second_max_datetime',
        'third_max_value',
        'third_max_datetime',
        'fourth_max_value',
        'fourth_max_datetime',
        'first_max_nonoverlap_value',
        'first_max_n_o_datetime',
        'second_max_nonoverlap_value',
        'second_max_n_o_datetime',
        'ninety_ninth_percentile',
        'ninety_eighth_percentile',
        'ninety_fifth_percentile',
        'ninetieth_percentile',
        'seventy_fifth_percentile',
        'fiftieth_percentile',
        'tenth_percentile'
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
    
    print(f"✅ Consolidated toxics annual data for year {year}: {len(result)} records")
    print(f"   📊 Columns: {len(available_columns)} available, {len(missing_columns)} missing")
    
    return result


def run_consolidation():
    """Run the toxics annual consolidation pipeline."""
    print("🚀 Starting Toxics Annual Consolidation Pipeline")

    # Input directory (transformed TRV annual data)
    transform_dir = config.ROOT / "transform" / "trv" / "annual"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the toxics annual transformation pipeline first.")
        return

    # Output directory (staged toxics annual data)
    staged_dir = config.ROOT / "staged" / "fct_toxics_annual"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Consolidating year {year_str}...")

        # Consolidate data for this year
        consolidated_df = consolidate_toxics_annual_for_year(year_str, transform_dir)

        if consolidated_df.empty:
            print(f"⚠️  No consolidated data for year {year_str}, skipping")
            continue

        # Write to staged layer
        output_path = staged_dir / f"fct_toxics_annual_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"✅ Wrote {len(consolidated_df)} toxics annual records to {output_path}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\n🎉 Toxics annual consolidation complete!")
    print(f"📊 Processed {years_processed} years with {total_records} total records")
    
    if years_processed > 0:
        print(f"📁 Output files: soar/staged/fct_toxics_annual/fct_toxics_annual_{{year}}.csv")
        print("🚫 Excluded geographic fields: latitude, longitude, county")


if __name__ == "__main__":
    run_consolidation()