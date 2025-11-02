"""Run extraction only for pm25 group parameters.

This lightweight wrapper reads `ops/dimPollutant.csv`, filters rows with
`group_store == 'pm25'`, and uses the existing `_process_parameter` helper
from `run_aqs_service` to run sample, annual, and daily extraction for each
parameter. Runs in foreground and prints progress for visibility.

Usage (from repo root):
  $env:PYTHONPATH = 'src'; python -u pipelines/aqs/run_pm25_only.py
"""

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Ensure 'src' is importable when executing as a script
ROOT = Path(__file__).resolve().parents[2]
# Ensure repo root and src are importable
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import config
from pipelines.aqs import run_aqs_service as runner


def main(workers: int = 4) -> None:
    df = pd.read_csv(Path("ops") / "dimPollutant.csv", dtype=str)
    if "group_store" not in df.columns:
        raise KeyError("ops/dimPollutant.csv must contain 'group_store' column")

    pm25 = df[df["group_store"].str.lower() == "pm25"]
    params = list(
        pm25[["aqs_parameter", "analyte_name"]]
        .dropna()
        .itertuples(index=False, name=None)
    )
    print(f"Processing {len(params)} pm25 parameters with {workers} workers")

    results = {}
    bdate = config.clamped_bdate()
    edate = config.EDATE
    state = config.STATE

    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {
            exe.submit(runner._process_parameter, code, label, bdate, edate, state): (
                code,
                label,
            )
            for code, label in params
        }
        for fut in as_completed(futures):
            code, label = futures[fut]
            try:
                _, count = fut.result()
            except Exception as exc:
                print(f"Parameter {label} ({code}) failed: {exc}")
                results[label] = 0
            else:
                print(f"DONE {label} ({code}): {count} rows")
                results[label] = count

    total = sum(results.values())
    print(f"Finished pm25 extraction. Total rows fetched: {total}")


if __name__ == "__main__":
    main()
