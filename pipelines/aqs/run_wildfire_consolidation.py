"""Pipeline for staging wildfire smoke data.

Reads from the staged layer (fct_aqi_daily, fct_criteria_daily, dim_sites)
and produces two output tables in staged/wildfire/:
  stg_wildfire_aqi_daily.parquet      — one row per site per date
  stg_wildfire_annual_summary.parquet — one row per site per year (wildfire season only)

Prerequisites:
  - run_aqi_daily_consolidation.py must have produced fct_aqi_daily files
  - run_fct_criteria_daily.py must have produced fct_criteria_daily files
  - run_dim_tables.py must have produced dim_sites
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_wildfire import run_consolidation

if __name__ == "__main__":
    run_consolidation()
