"""Pipeline for consolidating AQI daily data to staged layer.

This pipeline reads transformed AQI daily data, consolidates multiple pollutants
per site into single AQI values, and writes the results to the staged layer.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from stage.consolidate_aqi_daily import run_consolidation


def run():
    """Run the AQI daily consolidation pipeline."""
    print("🚀 Starting AQI Daily Consolidation Pipeline")
    print(f"📅 Date: {date.today()}")

    # Setup
    config.ensure_dirs()

    # Check if transform data exists
    transform_dir = config.ROOT / "transform" / "aqi"
    if not transform_dir.exists():
        print(f"❌ Transform directory not found: {transform_dir}")
        print("   Please run the AQI daily transformation pipeline first.")
        return

    # Run the consolidation
    run_consolidation()


if __name__ == "__main__":
    run()