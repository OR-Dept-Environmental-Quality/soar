"""Pipeline for extracting all Oregon monitors metadata from AQS API.

This pipeline fetches monitor metadata for all relevant parameters in Oregon
from 2005-2025, deduplicates by site_code, and writes the results to the data lake.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.extractors.monitors import fetch_all_monitors_for_oregon
from loaders.filesystem import write_csv


def run():
    """Run the Oregon monitors extraction pipeline."""
    print("🚀 Starting Oregon Monitors Extraction Pipeline")
    print(f"📅 Date: {date.today()}")

    # Setup
    config.ensure_dirs()
    config.set_aqs_credentials()

    # Date range
    bdate = date(2005, 1, 1)
    edate = date(2025, 12, 31)
    print(
        f"📅 Processing monitors from {bdate} to {edate} (iterating through each calendar year)"
    )

    # Fetch monitors
    print("\n📡 Fetching monitor metadata from AQS API...")
    monitors_df = fetch_all_monitors_for_oregon(bdate, edate)

    if monitors_df.empty:
        print("❌ No monitors found")
        return

    # Write to data lake
    output_dir = config.ROOT / "raw" / "aqs" / "monitors"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "oregon_monitors_2005_2025.csv"

    write_csv(monitors_df, output_path)
    print(f"\n✅ Wrote {len(monitors_df)} unique monitor records to {output_path}")

    print("\n🎉 Monitors extraction complete!")


if __name__ == "__main__":
    run()
