"""Comprehensive Staging Pipeline Runner.

This script runs all staging consolidation pipelines to create fact and dimension tables
from the transform layer data. It processes toxics annual, toxics sample, criteria daily,
site dimensions, and pollutant dimensions.

Output: All staged fact and dimension tables in soar/staged/
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

import config
from stage.consolidate_fct_toxics_annual import run_consolidation as run_toxics_annual
from stage.consolidate_fct_toxics_sample import run_consolidation as run_toxics_sample
from stage.consolidate_fct_criteria_daily import run_consolidation as run_criteria_daily
from stage.consolidate_dim_sites import run_consolidation as run_dim_sites
from stage.consolidate_dim_pollutant import run_consolidation as run_dim_pollutant
from stage.consolidate_fct_pm25_hourly import run_consolidation as run_pm25_hourly
from stage.consolidate_fct_ozone_hourly import run_consolidation as run_ozone_hourly
from stage.consolidate_fct_ozone_dv import run_consolidation as run_ozone_dv
from stage.consolidate_fct_pm25_dv import run_consolidation as run_pm25_dv


def run_all_staging():
    """Run all staging consolidation pipelines."""
    print("🚀 Starting Comprehensive Staging Pipeline")
    print("=" * 60)
    
    # Ensure staged directory exists
    staged_dir = config.ROOT / "staged"
    staged_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 Staging output directory: {staged_dir}")
    print()
    
    # Track success/failure of each pipeline
    results = {}
    
    # 1. Run toxics annual staging
    print("1️⃣  Running Toxics Annual Staging...")
    try:
        run_toxics_annual()
        results["toxics_annual"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in toxics annual staging: {e}")
        results["toxics_annual"] = f"❌ FAILED: {e}"
    print()
    
    # 2. Run toxics sample staging
    print("2️⃣  Running Toxics Sample Staging...")
    try:
        run_toxics_sample()
        results["toxics_sample"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in toxics sample staging: {e}")
        results["toxics_sample"] = f"❌ FAILED: {e}"
    print()
    
    # 3. Run criteria daily staging
    print("3️⃣  Running Criteria Daily Staging...")
    try:
        run_criteria_daily()
        results["criteria_daily"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in criteria daily staging: {e}")
        results["criteria_daily"] = f"❌ FAILED: {e}"
    print()
    
    # 4. Run site dimension staging
    print("4️⃣  Running Sites Dimension Staging...")
    try:
        run_dim_sites()
        results["dim_sites"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in sites dimension staging: {e}")
        results["dim_sites"] = f"❌ FAILED: {e}"
    print()
    
    # 5. Run pollutant dimension staging
    print("5️⃣  Running Pollutant Dimension Staging...")
    try:
        run_dim_pollutant()
        results["dim_pollutant"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in pollutant dimension staging: {e}")
        results["dim_pollutant"] = f"❌ FAILED: {e}"
    print()

    # 6. Run PM2.5 hourly staging
    print("6️⃣  Running PM2.5 Hourly Staging...")
    try:
        monitors_path = config.RAW_ENV_MONITORS / "envista_stations_monitors.csv"
        if monitors_path.exists():
            monitors = pd.read_csv(monitors_path)
            unique_monitors = monitors[["station_id", "stations_tag"]].drop_duplicates()
        else:
            print(f"   ⚠️  Envista monitors file not found: {monitors_path}")
            print("   Envista data will be skipped; only AQS PM2.5 will be staged.")
            unique_monitors = pd.DataFrame(columns=["station_id", "stations_tag"])
        run_pm25_hourly(unique_monitors)
        results["pm25_hourly"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in PM2.5 hourly staging: {e}")
        results["pm25_hourly"] = f"❌ FAILED: {e}"
    print()

    # 7. Run ozone hourly staging
    print("7️⃣  Running Ozone Hourly Staging...")
    try:
        run_ozone_hourly()
        results["ozone_hourly"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in ozone hourly staging: {e}")
        results["ozone_hourly"] = f"❌ FAILED: {e}"
    print()

    # 8. Run ozone design value staging
    print("8️⃣  Running Ozone Design Value Staging...")
    try:
        run_ozone_dv()
        results["ozone_dv"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in ozone DV staging: {e}")
        results["ozone_dv"] = f"❌ FAILED: {e}"
    print()

    # 9. Run PM2.5 design value staging
    print("9️⃣  Running PM2.5 Design Value Staging...")
    try:
        run_pm25_dv()
        results["pm25_dv"] = "✅ SUCCESS"
    except Exception as e:
        print(f"❌ Error in PM2.5 DV staging: {e}")
        results["pm25_dv"] = f"❌ FAILED: {e}"
    print()

    # Summary
    print("=" * 60)
    print("🎉 STAGING PIPELINE SUMMARY")
    print("=" * 60)
    
    success_count = 0
    for pipeline, result in results.items():
        print(f"{pipeline:20} : {result}")
        if "SUCCESS" in result:
            success_count += 1
    
    print()
    print(f"📊 Overall Status: {success_count}/{len(results)} pipelines completed successfully")
    
    if success_count == len(results):
        print("🎉 ALL STAGING PIPELINES COMPLETED SUCCESSFULLY!")
        print()
        print("📁 Generated staging files:")
        print("   Fact Tables:")
        print("   • fct_toxics_annual/fct_toxics_annual_{year}.csv")
        print("   • fct_toxics_sample/fct_toxics_sample_{year}.csv")  
        print("   • fct_criteria_daily/fct_criteria_daily_{year}.csv")
        print("   • fct_pm25_hourly/fct_pm25_hourly_{year}.csv")
        print("   • fct_ozone_hourly/fct_ozone_hourly_{year}.csv")
        print("   • fct_ozone_dv/fct_ozone_dv.csv")
        print("   • fct_pm25_dv/fct_pm25_dv.csv")
        print("   Dimension Tables:")
        print("   • dim_sites/dim_sites.csv")
        print("   • dim_pollutant/dim_pollutant.csv")
        print()
        print("🚫 Geographic fields excluded from fact tables:")
        print("   • latitude, longitude, county")
        print("   (Available in dim_sites for joins on site_code)")
    else:
        print("⚠️  Some pipelines failed. Check the error messages above.")


if __name__ == "__main__":
    run_all_staging()