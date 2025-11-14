"""Pipeline to run dimension table staging.

This pipeline copies monitor and pollutant dimension data from transform/ops
to the staged layer for analytical use.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_dim_sites import run_consolidation as run_dim_sites
from stage.consolidate_dim_pollutant import run_consolidation as run_dim_pollutant


def main():
    """Run both dimension table staging pipelines."""
    print("🚀 Starting Dimension Tables Staging Pipeline")
    print("=" * 50)
    
    # Run sites dimension
    print("1️⃣  Creating dim_sites table...")
    try:
        run_dim_sites()
        sites_result = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in dim_sites: {e}")
        sites_result = f"❌ FAILED: {e}"
    
    print()
    
    # Run pollutant dimension  
    print("2️⃣  Creating dim_pollutant table...")
    try:
        run_dim_pollutant()
        pollutant_result = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in dim_pollutant: {e}")
        pollutant_result = f"❌ FAILED: {e}"
    
    print()
    print("=" * 50)
    print("DIMENSION TABLES SUMMARY:")
    print(f"dim_sites     : {sites_result}")
    print(f"dim_pollutant : {pollutant_result}")


if __name__ == "__main__":
    main()