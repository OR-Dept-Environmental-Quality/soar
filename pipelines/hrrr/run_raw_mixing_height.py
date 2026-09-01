""" Pipeline to run HRRR raw mixing height extrtaction."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from hrrr.extractors.pull_raw_mixing_height import run_extraction

_START_YEAR = 2020
_END_YEAR= 2025

def _prompt_keep_raw()-> bool:
    num_years = _END_YEAR - _START_YEAR +1
    total_gb = 25 * num_years
    response = input(
        f"Keep raw HRRR GRIB2 files (~25GB/year, ~{total_gb}GB total for {_START_YEAR}-{_END_YEAR})? [y/N]: "
    ).strip().lower()
    return response in ("y", "yes")

if __name__ == "__main__":
    keep_raw = _prompt_keep_raw()
    run_extraction(_START_YEAR, _END_YEAR, keep_raw=keep_raw)