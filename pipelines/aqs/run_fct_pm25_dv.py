"""Pipeline to run PM2.5 design value staging.

Reads staged/fct_criteria_daily and produces staged/fct_pm25_dv/fct_pm25_dv.csv
per EPA 40 CFR Part 50 Appendix N.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_pm25_dv import run_consolidation


def main() -> None:
    """Run the PM2.5 design value staging pipeline."""
    run_consolidation()


if __name__ == "__main__":
    main()
