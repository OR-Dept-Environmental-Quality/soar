"""Pipeline for transforming Oregon monitors metadata.

This pipeline reads raw monitor metadata, selects specific fields,
removes duplicates by site_code, and writes cleaned monitor records
to the transform layer.
"""

from __future__ import annotations
import sys
from pathlib import Path
from datetime import date

import pandas as pd

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.monitors import transform_monitors
from loaders.filesystem import write_csv


def run():
    """Run the Oregon monitors transformation pipeline."""
    print("🚀 Starting Oregon Monitors Transformation Pipeline")
    print(f"📅 Date: {date.today()}")

    # Setup
    config.ensure_dirs()

    # Input path (raw monitors)
    input_path = (
        config.ROOT / "raw" / "aqs" / "monitors" / "oregon_monitors_2005_2025.csv"
    )

    if not input_path.exists():
        print(f"❌ Raw monitors file not found: {input_path}")
        print("   Please run the monitors extraction pipeline first.")
        return

    # Read raw monitors data
    print(f"\n📖 Reading raw monitors from {input_path}")
    raw_monitors_df = pd.read_csv(input_path)

    if raw_monitors_df.empty:
        print("❌ No raw monitors data found")
        return

    print(f"📊 Loaded {len(raw_monitors_df)} raw monitor records")

    # Transform monitors
    print("\n🔄 Transforming monitor data...")
    transformed_df = transform_monitors(raw_monitors_df)

    if transformed_df.empty:
        print("❌ Transformation resulted in empty dataset")
        return

    # Write to transform layer
    output_dir = config.ROOT / "transform" / "monitors"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "aqs_monitors.csv"

    write_csv(transformed_df, output_path)
    print(
        f"\n✅ Wrote {len(transformed_df)} transformed monitor records to {output_path}"
    )

    print("\n🎉 Monitors transformation complete!")


if __name__ == "__main__":
    run()
