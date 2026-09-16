"""Pipeline to run HRRR mixing height transform (per-site IDW extraction)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from hrrr.transformers.mixing_height import transform_years

_START_YEAR = config.START_YEAR
_END_YEAR = config.END_YEAR

if __name__ == "__main__":
    transform_years(_START_YEAR, _END_YEAR, sites=None, grib_dir=None, out_dir=None)