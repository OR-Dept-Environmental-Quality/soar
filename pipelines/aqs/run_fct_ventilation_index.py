""" Pipeline to run the Ventilation Index"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.transformers.ventilation_index import run_transform

if __name__ == "__main__":
    run_transform(config.START_YEAR, config.END_YEAR)