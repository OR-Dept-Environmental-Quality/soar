import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import config
from hrrr.extractors.pull_raw_mixing_height import run_day

grib_dir = config.ROOT / "raw" / "hrrr_grib"
run_day(datetime(2023, 1, 1), grib_dir)