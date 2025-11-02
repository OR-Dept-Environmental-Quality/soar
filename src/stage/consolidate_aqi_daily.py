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

import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config


def consolidate_aqi_daily_for_year(year: str, transform_dir: Path) -> pd.DataFrame:
    """Consolidate AQI daily data for a specific year.

    Reads the transformed AQI daily data, consolidates multiple pollutants per site/date
    into a single record with overall AQI as the maximum of pollutant AQIs.

    Args:
        year: Year string (e.g., "2023")
        transform_dir: Directory containing transformed AQI daily files

    Returns:
        Consolidated DataFrame with one row per site per date
    """
    # Read the transformed data for this year
    input_file = transform_dir / f"aqi_aqs_daily_{year}.csv"
    if not input_file.exists():
        print(f"⚠️  No transformed AQI file found for year {year}: {input_file}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"❌ Error reading {input_file}: {e}")
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

    # For PM25 with multiple parameters, select the one with higher arithmetic_mean
    pm25_df = df[df['pollutant'] == 'pm25'].copy()
    if not pm25_df.empty:
        # Group by site_code, date_local and select the row with max arithmetic_mean
        pm25_consolidated = pm25_df.loc[
            pm25_df.groupby(['site_code', 'date_local'])['arithmetic_mean'].idxmax()
        ]
    else:
        pm25_consolidated = pd.DataFrame()

    # For ozone, just take all (should be unique per site/date anyway)
    ozone_df = df[df['pollutant'] == 'ozone'].copy()

    # Now merge ozone and pm25 data
    # Start with all unique site_code + date_local combinations
    all_sites_dates = pd.concat([
        df[['site_code', 'date_local', 'event_type']].drop_duplicates()
        for df in [ozone_df, pm25_consolidated] if not df.empty
    ]).drop_duplicates()

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

    # Calculate overall AQI as max of ozone_aqi and pm25_aqi
    result['aqi'] = result[['ozone_aqi', 'pm25_aqi']].max(axis=1)

    # Select final columns in the required order
    final_columns = [
        'site_code',
        'date_local',
        'event_type',
        'aqi',
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

    # Remove rows where aqi is NaN (no valid pollutants)
    result = result.dropna(subset=['aqi'])

    print(f"✅ Consolidated AQI data for year {year}: {len(result)} site-date records")

    return result


def run_consolidation():
    """Run the AQI daily consolidation pipeline."""
    print("🚀 Starting AQI Daily Consolidation Pipeline")

    # Input directory (transformed AQI data)
    transform_dir = config.ROOT / "transform" / "aqi"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the AQI daily transformation pipeline first.")
        return

    # Output directory (staged AQI data)
    # Note: This is outside the repo in the data lake
    staged_dir = Path(r"C:\Users\abiberi\Oregon\DEQ - Air Data Team - DataRepo\soar\staged\aqi")
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Consolidating year {year_str}...")

        # Consolidate data for this year
        consolidated_df = consolidate_aqi_daily_for_year(year_str, transform_dir)

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