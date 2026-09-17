""" Pipeline to run HRRR raw mixing height extrtaction."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from hrrr.extractors.pull_raw_mixing_height import run_extraction

_START_YEAR = config.START_YEAR
_END_YEAR = config.END_YEAR

if __name__ == "__main__":
    run_extraction(_START_YEAR, _END_YEAR)