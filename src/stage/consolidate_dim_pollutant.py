"""Dim Pollutant Consolidation Script.

This script copies pollutant dimension data from the ops directory to the staged layer.
It reads dimPollutant.csv from ops/ and creates a dim_pollutant table
for the staged analytical layer.

Output: soar/staged/dim_pollutant.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config


def consolidate_dim_pollutant():
    """Copy pollutant dimension data to staged layer as dim_pollutant."""
    
    # Input file (ops dimPollutant.csv)
    pollutant_file = ROOT / "ops" / "dimPollutant.csv"
    if not pollutant_file.exists():
        print(f"❌ Pollutant dimension file not found: {pollutant_file}")
        print("   Please ensure dimPollutant.csv exists in the ops/ directory.")
        return False

    try:
        pollutant_df = pd.read_csv(pollutant_file)
    except Exception as e:
        print(f"❌ Error reading pollutant dimension file: {e}")
        return False

    if pollutant_df.empty:
        print("⚠️  Empty pollutant dimension file")
        return False

    # Output directory (staged dimension data)
    staged_dir = config.ROOT / "staged" / "dim_pollutant"
    staged_dir.mkdir(parents=True, exist_ok=True)

    # Write to staged layer as dim_pollutant
    output_path = staged_dir / "dim_pollutant.csv"
    pollutant_df.to_csv(output_path, index=False)

    print(f"✅ Copied {len(pollutant_df)} pollutant records from {pollutant_file}")
    print(f"✅ Created dim_pollutant table at {output_path}")
    
    # Display summary by group_store for reference
    if 'group_store' in pollutant_df.columns:
        group_counts = pollutant_df['group_store'].value_counts()
        print("📊 Pollutant groups:")
        for group, count in group_counts.items():
            print(f"   {group}: {count} parameters")
    
    return True


def run_consolidation():
    """Run the dim_pollutant consolidation pipeline."""
    print("🚀 Starting Dim Pollutant Consolidation Pipeline")

    success = consolidate_dim_pollutant()
    
    if success:
        print("\n🎉 Dim pollutant consolidation complete!")
        print("📁 Output file: soar/staged/dim_pollutant/dim_pollutant.csv")
        print("📄 This is a direct copy of ops/dimPollutant.csv")
    else:
        print("\n❌ Dim pollutant consolidation failed!")


if __name__ == "__main__":
    run_consolidation()