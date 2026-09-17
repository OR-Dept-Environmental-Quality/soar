import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import config
from hrrr.extractors.pull_raw_mixing_height import run_years

run_years(2024, 2024)