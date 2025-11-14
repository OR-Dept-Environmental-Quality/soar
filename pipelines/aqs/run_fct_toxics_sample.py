"""Pipeline to run toxics sample staging.

This pipeline reads TRV sample transform data and creates staged fact tables
for sample toxics data, excluding geographic fields.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src directory to Python path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_toxics_sample import run_consolidation


def main():
    """Run the toxics sample staging pipeline."""
    print("🚀 Starting Toxics Sample Staging Pipeline")
    run_consolidation()


if __name__ == "__main__":
    main()