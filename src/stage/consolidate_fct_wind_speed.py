
"""Wind Speed (Meteorological) Consolidation Script

Consolidates hourly wind speed data from AQS (parameter 61103, Scalar Wind Speed) into a staged fact table, for use in air stagnation analysis 
alongside PM2.5 and wood smoke tosical dinural patterns,

Outout: staged/fct_wind_speed/fct_wind_speed_{year}.csv
"""


from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.sample_hourly import transform_aqs_sample_hourly_for_year

_WIND_SPEED_PARAM_CODES = ['61103']

_OUTPUT_COLUMNS = [
    "site_code",
    "date_local",
    "time_local",
    "parameter_code",
    "poc",
    "parameter",
    "sample_measurement",
    "units_of_measure",
    "sample_duration_code",
    "sample_duration",
    "validity_indicator",
    "method_code",
    "method",
    "qualifier",
    "source",
]


def consolidate_wind_speed_for_year(year: str,raw_sample_dir: Path) -> pd.DataFrame:
    print(f"  [Wind speed] Transforming AQS sample data for {year}...")
    result = transform_aqs_sample_hourly_for_year(
        year, raw_sample_dir, parameter_codes=_WIND_SPEED_PARAM_CODES
    )
    if result.empty:
        print(f"  [Wind speed] No data available for {year}")
        return pd.DataFrame()
    
    for col in _OUTPUT_COLUMNS:
        if col not in result.columns:
            result[col] = pd.NA
    return result[_OUTPUT_COLUMNS]


def run_consolidation() -> None:
    print("Starting Wind Speed Consolidation Pipeline")

    raw_sample_dir = config.RAW_AQS_SAMPLE

    if not raw_sample_dir.exists():
        print(f"AQS sample directory not found: {raw_sample_dir}")
        print("Please run the AQS sample extraction pipeline first.")
        return

    staged_dir = config.ROOT / "staged" / "fct_wind_speed"
    staged_dir.mkdir(parents=True, exist_ok=True)

    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        year_str = str(year)
        print(f"\nConsolidating wind speed for year {year_str}...")

        consolidated_df = consolidate_wind_speed_for_year(
            year_str, raw_sample_dir
        )

        if consolidated_df.empty:
            print(f"  No consolidated data for {year_str}, skipping")
            continue

        output_path = staged_dir / f"fct_wind_speed_{year_str}.csv"
        consolidated_df.to_csv(output_path, index=False)

        print(f"  Wrote {len(consolidated_df)} records to {output_path.name}")

        years_processed += 1
        total_records += len(consolidated_df)

    print("\nWind speed consolidation complete!")
    print(f"Processed {years_processed} years with {total_records} total records")
    print(f"Output: staged/fct_wind_speed/fct_wind_speed_{year}.csv")


if __name__ == "__main__":
    run_consolidation()


