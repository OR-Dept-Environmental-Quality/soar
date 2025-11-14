"""Pipeline to run criteria daily staging.

This pipeline reads AQI daily transform data and creates staged fact tables
for criteria daily data, excluding geographic fields.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_criteria_daily import run_consolidation


def main():
    """Run the criteria daily staging pipeline."""
    print("🚀 Starting Criteria Daily Staging Pipeline")
    run_consolidation()


if __name__ == "__main__":
    main()