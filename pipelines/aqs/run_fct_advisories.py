""" Pipeline to run air quality advisory tracking staging."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_advisories import run_consolidation

if __name__ == '__main__':
    run_consolidation()