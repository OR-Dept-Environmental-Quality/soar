""" Pipeline to run wood smoke toxics staging.

Entry point that filters the staged toxics sample fact table down to wood smoke toxics parameters and writes the result to a new fact table.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stage.consolidate_fct_wood_smoke_toxics import run_consolidation

def main():
    print("Running Wood Smoke Toxics Consolidation Pipeline")
    run_consolidation()

if __name__ == "__main__":
        main()

