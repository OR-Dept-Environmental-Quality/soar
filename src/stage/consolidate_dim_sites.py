"""Dim Sites Consolidation Script.

This script copies monitor/site dimension data from the transform layer to the staged layer.
It reads the aqs_monitors.csv from transform/monitors and creates a dim_sites table
for the staged analytical layer.

Output: soar/staged/dim_sites.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config


def consolidate_dim_sites():
    """Copy monitor dimension data to staged layer as dim_sites."""
    
    # Input file (transformed monitors data)
    monitors_file = config.ROOT / "transform" / "monitors" / "aqs_monitors.csv"
    if not monitors_file.exists():
        print(f"❌ Monitors file not found: {monitors_file}")
        print("   Please run the monitors transformation pipeline first.")
        return False

    try:
        monitors_df = pd.read_csv(monitors_file)
    except Exception as e:
        print(f"❌ Error reading monitors file: {e}")
        return False

    if monitors_df.empty:
        print("⚠️  Empty monitors file")
        return False

    # Output directory (staged dimension data)
    staged_dir = config.ROOT / "staged" / "dim_sites"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Write to staged layer as dim_sites
    output_path = staged_dir / "dim_sites.csv"
    monitors_df.to_csv(output_path, index=False)

    print(f"✅ Copied {len(monitors_df)} site records from {monitors_file}")
    print(f"✅ Created dim_sites table at {output_path}")
    
    # Display available columns for reference
    print(f"📊 Columns in dim_sites: {', '.join(monitors_df.columns[:10])}...")
    
    return True


def run_consolidation():
    """Run the dim_sites consolidation pipeline."""
    print("🚀 Starting Dim Sites Consolidation Pipeline")

    success = consolidate_dim_sites()
    
    if success:
        print("\n🎉 Dim sites consolidation complete!")
        print("📁 Output file: soar/staged/dim_sites/dim_sites.csv")
        print("📄 This is a direct copy of transform/monitors/aqs_monitors.csv")
    else:
        print("\n❌ Dim sites consolidation failed!")


if __name__ == "__main__":
    run_consolidation()