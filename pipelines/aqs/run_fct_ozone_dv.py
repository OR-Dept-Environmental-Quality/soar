"""Pipeline to run ozone design value staging.

Reads staged/fct_criteria_daily and produces staged/fct_ozone_dv/fct_ozone_dv.csv
per EPA 40 CFR Part 50 Appendix P.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_ozone_dv import run_consolidation


def main() -> None:
    """Run the ozone design value staging pipeline."""
    run_consolidation()


if __name__ == "__main__":
    main()
