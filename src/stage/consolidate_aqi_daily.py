"""AQI Daily Consolidation Script.

This script consolidates AQI daily data from the transform layer into the staged layer.
It creates one AQI value per site per date by selecting the highest AQI among pollutants
where validity_indicator is 'Y'. For PM25 with multiple parameters, it selects the higher
mean concentration to calculate AQI_PM25.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config


def load_aqi_categories() -> pd.DataFrame:
    """Load AQI category definitions from dimAQI.csv."""
    dim_aqi_path = ROOT / "ops" / "dimAQI.csv"  # ops is at repo root level
    try:
        df = pd.read_csv(dim_aqi_path)
        # Get unique AQI ranges and categories (since categories are the same across pollutants)
        categories = df[['aqi_category', 'low_aqi', 'high_aqi']].drop_duplicates()
        return categories.sort_values('low_aqi')
    except Exception as e:
        print(f"❌ Error loading dimAQI.csv: {e}")
        return pd.DataFrame()


def get_aqi_category(aqi_value: float, categories_df: pd.DataFrame) -> str:
    """Determine AQI category based on AQI value."""
    if pd.isna(aqi_value):
        return None
    
    # Find the category where low_aqi <= aqi_value <= high_aqi
    for _, row in categories_df.iterrows():
        if row['low_aqi'] <= aqi_value <= row['high_aqi']:
            return row['aqi_category']
    
    # If no category found (AQI > highest range), assign highest category
    # This handles cases where AQI exceeds the maximum defined range (e.g., >999)
    if not categories_df.empty:
        highest_category = categories_df.loc[categories_df['low_aqi'].idxmax(), 'aqi_category']
        return highest_category
    
    return None  # Should not happen with valid categories


def consolidate_aqi_daily_for_year(year: str, transform_dir: Path, categories_df: pd.DataFrame) -> pd.DataFrame:
    """Consolidate AQI daily data for a specific year.

    Reads the transformed AQI daily data, consolidates multiple pollutants per site/date
    into a single record with overall AQI as the maximum of pollutant AQIs.

    Args:
        year: Year string (e.g., "2023")
        transform_dir: Directory containing transformed AQI daily files

    Returns:
        Consolidated DataFrame with one row per site per date
    """
    # Read all transformed data files for this year (both AQS and Envista)
    import glob
    pattern = str(transform_dir / f"*aqi*{year}.csv")
    input_files = glob.glob(pattern)
    
    if not input_files:
        print(f"⚠️  No transformed AQI files found for year {year} matching pattern: {pattern}")
        return pd.DataFrame()
    
    print(f"   Found {len(input_files)} file(s) for year {year}")
    
    # Read and concatenate all files
    dfs = []
    try:
        for input_file in input_files:
            df_temp = pd.read_csv(input_file)
            dfs.append(df_temp)
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        print(f"❌ Error reading files for year {year}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"⚠️  Empty AQI file for year {year}")
        return pd.DataFrame()

    # Filter only valid records (validity_indicator == 'Y')
    df = df[df['validity_indicator'] == 'Y'].copy()

    if df.empty:
        print(f"⚠️  No valid AQI records for year {year}")
        return pd.DataFrame()

    # Identify pollutant types
    # Ozone: parameter_code == 44201
    # PM25: parameter_code in [88101, 88502] (88101 is PM2.5, 88502 might be another PM25 variant)
    df['pollutant'] = df['parameter_code'].map({
        44201: 'ozone',
        88101: 'pm25',
        88502: 'pm25'  # Assuming this is also PM25
    })

    # Drop rows where pollutant is not identified
    df = df.dropna(subset=['pollutant'])

    if df.empty:
        print(f"⚠️  No recognized pollutants for year {year}")
        return pd.DataFrame()

    # For PM25 with multiple parameters, apply priority hierarchy:
    # Priority 1: parameter_code 88101 (FRM/FEM)
    # Priority 2: parameter_code 88502 with POC != 99 (non-FRM/FEM AQS monitors)
    # Priority 3: parameter_code 88502 with POC == 99 (Envista sensors)
    # Within same priority, select highest arithmetic_mean
    pm25_df = df[df['pollutant'] == 'pm25'].copy()
    if not pm25_df.empty:
        # Assign priority levels
        def assign_pm25_priority(row):
            if row['parameter_code'] == 88101:
                return 1  # Highest priority
            elif row['parameter_code'] == 88502 and row['poc'] != 99:
                return 2  # Medium priority
            elif row['parameter_code'] == 88502 and row['poc'] == 99:
                return 3  # Lowest priority (Envista)
            else:
                return 999  # Unknown, shouldn't happen
        
        pm25_df['priority'] = pm25_df.apply(assign_pm25_priority, axis=1)
        
        # Sort by priority (ascending), then arithmetic_mean (descending)
        pm25_df = pm25_df.sort_values(['priority', 'arithmetic_mean'], ascending=[True, False])
        
        # Group by site_code, date_local and select first row (highest priority, then highest mean)
        pm25_consolidated = pm25_df.groupby(['site_code', 'date_local']).first().reset_index()
        pm25_consolidated = pm25_consolidated.drop(columns=['priority'])
    else:
        pm25_consolidated = pd.DataFrame()

    # For ozone, just take all (should be unique per site/date anyway)
    ozone_df = df[df['pollutant'] == 'ozone'].copy()

    # Merge ozone and PM25 data into consolidated records
    # Start with all unique site_code + date_local combinations
    all_sites_dates = pd.concat([
        df[['site_code', 'date_local', 'event_type']].drop_duplicates()
        for df in [ozone_df, pm25_consolidated] if not df.empty
    ]).drop_duplicates(subset=['site_code', 'date_local'])

    # Merge ozone data
    result = all_sites_dates.copy()
    if not ozone_df.empty:
        ozone_cols = ozone_df[['site_code', 'date_local', 'poc', 'observation_percent', 'validity_indicator', 'aqi']].rename(
            columns={
                'poc': 'ozone_poc',
                'observation_percent': 'ozone_observation_percent',
                'validity_indicator': 'ozone_validity_indicator',
                'aqi': 'ozone_aqi'
            }
        )
        result = result.merge(ozone_cols, on=['site_code', 'date_local'], how='left')

    # Merge PM25 data
    if not pm25_consolidated.empty:
        pm25_cols = pm25_consolidated[['site_code', 'date_local', 'poc', 'observation_percent', 'validity_indicator', 'aqi']].rename(
            columns={
                'poc': 'pm25_poc',
                'observation_percent': 'pm25_observation_percent',
                'validity_indicator': 'pm25_validity_indicator',
                'aqi': 'pm25_aqi'
            }
        )
        result = result.merge(pm25_cols, on=['site_code', 'date_local'], how='left')

    # Calculate overall AQI as maximum of pollutant-specific AQI values
    # Ensure both columns exist before calculating
    for col in ['ozone_aqi', 'pm25_aqi']:
        if col not in result.columns:
            result[col] = np.nan
    
    # Use np.nanmax to handle cases where only one pollutant is present
    result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].apply(lambda row: np.nanmax(row.values), axis=1)

    # Assign AQI category based on the overall AQI value
    result['aqi_category'] = result['aqi'].apply(lambda x: get_aqi_category(x, categories_df))

    # Select final columns in the required order
    final_columns = [
        'site_code',
        'date_local',
        'event_type',
        'aqi',
        'aqi_category',
        'ozone_poc',
        'ozone_observation_percent',
        'ozone_validity_indicator',
        'ozone_aqi',
        'pm25_poc',
        'pm25_observation_percent',
        'pm25_validity_indicator',
        'pm25_aqi'
    ]

    # Ensure all columns exist (fill missing with NaN)
    for col in final_columns:
        if col not in result.columns:
            result[col] = pd.NA

    result = result[final_columns]

    # Filter out records without valid AQI values
    result = result.dropna(subset=['aqi'])

    # Ensure uniqueness by site_code and date_local
    result = result.drop_duplicates(subset=['site_code', 'date_local'])

    print(f"✅ Consolidated AQI data for year {year}: {len(result)} site-date records")

    return result


def run_consolidation():
    """Run the AQI daily consolidation pipeline."""
    print("🚀 Starting AQI Daily Consolidation Pipeline")

    # Load AQI categories for classification
    categories_df = load_aqi_categories()
    if categories_df.empty:
        print("❌ Could not load AQI categories from dimAQI.csv")
        return

    # Input directory (transformed AQI data)
    transform_dir = config.ROOT / "transform" / "aqi"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the AQI daily transformation pipeline first.")
        return

    # Output directory (staged AQI data)
    staged_dir = config.ROOT / "staged" / "fct_aqi_daily"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Consolidating year {year_str}...")

        # Consolidate data for this year
        consolidated_df = consolidate_aqi_daily_for_year(year_str, transform_dir, categories_df)

        if consolidated_df.empty:
            print(f"⚠️  No consolidated data for year {year_str}, skipping")
            continue

        # Write to staged layer
        output_path = staged_dir / f"aqi_aqs_daily_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"✅ Wrote {len(consolidated_df)} consolidated AQI records to {output_path}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\n🎉 AQI daily consolidation complete!")
    print(f"📊 Processed {years_processed} years with {total_records} total consolidated records")


if __name__ == "__main__":
    run_consolidation()