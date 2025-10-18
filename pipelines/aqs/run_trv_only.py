"""Run TRV transformations only on existing toxics data.

This script processes existing toxics sample and annual files in the data lake,
applies TRV transformations, and saves to transform/aqs/trv/.
Assumes extraction has already been completed.
"""
from __future__ import annotations
import sys
from pathlib import Path
import glob

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.trv_sample import transform_toxics_trv
from aqs.transformers.trv_annual import transform_toxics_annual_trv
from loaders.filesystem import write_csv
import pandas as pd


def main():
    # Paths
    dim_trv_path = ROOT / "ops" / "dimPollutant.csv"
    sample_dir = config.RAW_SAMPLE
    annual_dir = config.RAW_ANNUAL
    transform_dir = config.ROOT / "transform" / "trv" / "sample"
    annual_transform_dir = config.ROOT / "transform" / "trv" / "annual"

    transform_dir.mkdir(parents=True, exist_ok=True)
    annual_transform_dir.mkdir(parents=True, exist_ok=True)

    print("Starting TRV transformation for sample toxics data...")
    # Process sample toxics
    sample_toxics_files = glob.glob(str(sample_dir / "aqs_sample_toxics_*.csv"))
    for toxics_file in sample_toxics_files:
        year = toxics_file.split("_")[-1].replace(".csv", "")
        print(f"Processing sample toxics for {year}...")
        df = pd.read_csv(toxics_file)
        if df.empty:
            print(f"No data for {year}, skipping.")
            continue
        transformed_df = transform_toxics_trv(df, str(dim_trv_path))
        output_path = transform_dir / f"trv_sample_{year}.csv"
        write_csv(transformed_df, output_path)
        print(f"Transformed sample toxics for {year}: {len(transformed_df)} rows -> {output_path}")

    print("Starting TRV transformation for annual toxics data...")
    # Process annual toxics
    annual_toxics_files = glob.glob(str(annual_dir / "aqs_annual_toxics_*.csv"))
    for annual_file in annual_toxics_files:
        year = annual_file.split("_")[-1].replace(".csv", "")
        print(f"Processing annual toxics for {year}...")
        df = pd.read_csv(annual_file)
        if df.empty:
            print(f"No data for {year}, skipping.")
            continue
        transformed_df = transform_toxics_annual_trv(df, str(dim_trv_path))
        output_path = annual_transform_dir / f"trv_annual_{year}.csv"
        write_csv(transformed_df, output_path)
        print(f"Transformed annual toxics for {year}: {len(transformed_df)} rows -> {output_path}")

    print("TRV transformations completed.")


if __name__ == "__main__":
    main()