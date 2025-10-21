"""Pipeline for transforming AQI daily data.

This pipeline reads raw AQI daily summary files, combines all pollutants
for each year, applies transformations, and writes cleaned data to the
transform layer organized by year.
"""

from __future__ import annotations
import sys
from pathlib import Path
from datetime import date


# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.aqi_daily import transform_aqi_daily_for_year
from loaders.filesystem import write_csv


def run():
    """Run the AQI daily transformation pipeline."""
    print("🚀 Starting AQI Daily Transformation Pipeline")
    print(f"📅 Date: {date.today()}")

    # Setup
    config.ensure_dirs()

    # Input directory (raw daily data)
    raw_daily_dir = config.RAW_DAILY

    if not raw_daily_dir.exists():
        print(f"❌ Raw daily directory not found: {raw_daily_dir}")
        print("   Please run the daily extraction pipeline first.")
        return

    # Output directory
    output_dir = config.ROOT / "transform" / "aqi"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Process each year in the date range
    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\n📅 Processing year {year_str}...")

        # Transform data for this year
        transformed_df = transform_aqi_daily_for_year(year_str, raw_daily_dir)

        if transformed_df.empty:
            print(f"⚠️  No data for year {year_str}, skipping")
            continue

        # Write to transform layer
        output_path = output_dir / f"aqi_aqs_daily_{year_str}.csv"
        write_csv(transformed_df, output_path)

        print(f"✅ Wrote {len(transformed_df)} AQI records to {output_path}")

        years_processed += 1
        total_records += len(transformed_df)

    print("\n🎉 AQI daily transformation complete!")
    print(f"📊 Processed {years_processed} years with {total_records} total records")


if __name__ == "__main__":
    run()
