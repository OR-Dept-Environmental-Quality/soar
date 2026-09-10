"""Consolidate dailty mixing height CSVs into one CSV per year, matching fct_*_{year}.csv convention."""

from __future__ import annotations

from pathlib import Path
from datetime import date

import pandas as pd

import config

def consolidate_year(year: int, daily_dir: Path, out_dir: Path)-> None:
    """ Combine every daily CSV for one year into a single yearly CSV."""
    daily_files = sorted(daily_dir.glob(f"mixing_height_{year}-*.csv"))
    if not daily_files:
        print(f"{year}: no daily files found, skipping")
        return

    expected_days = (date(year + 1,1,1) - date(year, 1,1)).days
    if len(daily_files) != expected_days:
        print(
            f"{year}: WARNING - found {len(daily_files)} daily files,"
            f" expected {expected_days} (year may be incomplete"
        )

    year_df = pd.concat(
        (pd.read_csv(f, dtype={"site_code": str}) for f in daily_files), ignore_index= True
    )

    out_path = out_dir / f"fct_mixing_height_{year}.csv"
    year_df.to_csv(out_path, index=False)
    print(f"{year}: consolidated {len(daily_files)} days, {len(year_df)} rows -> {out_path.name}")

def run_consolidation() -> None:
    daily_dir = config.ROOT / "transform" / "hrrr_mixing_height"
    out_dir = config.ROOT /"staged" / "fct_mixing_height"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(config.START_YEAR, config.END_YEAR + 1):
        consolidate_year(year, daily_dir, out_dir)