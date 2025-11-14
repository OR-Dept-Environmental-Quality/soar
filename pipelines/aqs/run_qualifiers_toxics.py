"""Pipeline to extract qualifier data for toxics pollutants from AQS API.

This script fetches qualifier codes and descriptions for all toxics parameters
(group_store = "toxics" in dimPollutant.csv) using the transactionsSample/byState
endpoint. Qualifiers provide data quality context for TRV risk assessments.

Output: raw/aqs/qualifiers/aqs_qualifiers_toxics_{year}.csv
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.extractors.measurements import write_qualifiers_for_toxics


def run_qualifiers_toxics():
    """Run the toxics qualifiers extraction pipeline."""
    print("🚀 Starting Toxics Qualifiers Extraction Pipeline")

    # Use the same date range as other AQS extractions
    bdate = date(config.START_YEAR, 1, 1)
    edate = date(config.END_YEAR, 12, 31)
    state_fips = config.STATE

    print(f"📅 Date range: {bdate} to {edate}")
    print(f"📍 State FIPS: {state_fips}")

    # Extract qualifiers for all toxics parameters
    results = write_qualifiers_for_toxics(bdate, edate, state_fips)

    # Report results
    print("\n📊 Extraction Results:")
    print(f"Status: {results['status']}")
    print(f"Parameters processed: {len(results['parameters'])}")
    print(f"Years processed: {len(results['years'])}")

    if results['status'] == 'ok':
        total_rows = sum(year_data.get('rows', 0) for year_data in results['years'].values())
        print(f"Total qualifier records: {total_rows}")

        print("\n📁 Output files:")
        for year, year_data in results['years'].items():
            if year_data.get('rows', 0) > 0:
                print(f"  {year}: {year_data['rows']} records")
    else:
        print(f"❌ Error: {results.get('error', 'Unknown error')}")

    print("\n🎉 Toxics qualifiers extraction complete!")


if __name__ == "__main__":
    run_qualifiers_toxics()