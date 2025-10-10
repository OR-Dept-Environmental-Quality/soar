"""Sample-run pipeline for AQS monitor sample data.

Reads `ops/parameters.csv` (column `AQS_Parameter`), loops through parameters,
fetches sample-level data (per-site, per-year split) and writes per-parameter
files to the configured `DATAREPO_ROOT` raw area.

This script defaults to the R behavior for URL construction and date chunking
and uses 4 workers by default to parallelize parameter fetches. It calls
`config.set_aqs_credentials()` at runtime so AQS credentials loaded from the
.env are registered with `pyaqsapi`.
"""
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

# Ensure src is importable when running from the repo root
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from soar import config
from soar.aqs.extractors.monitors import (
    load_parameters_csv,
    fetch_samples_for_parameter,
)
from soar.loaders.filesystem import write_parquet, write_csv


RAW_DIR = config.RAW
DEFAULT_WORKERS = 4


def _write_parameter_outputs(param: str, frame) -> None:
    """Write per-parameter outputs (parquet + csv) into RAW_DIR.

    Filenames: monitors_samples_{param}.parquet / .csv
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = RAW_DIR / f"monitors_samples_{param}.parquet"
    csv_path = RAW_DIR / f"monitors_samples_{param}.csv"
    try:
        write_parquet(frame, parquet_path)
    except Exception:
        # Fallback: write CSV if parquet unavailable
        write_csv(frame, csv_path)
    else:
        # Also write CSV for human inspection/compatibility
        write_csv(frame, csv_path)


def _process_parameter(param: str, bdate: str, edate: str, state: str) -> tuple[str, int]:
    """Fetch samples for a single parameter and write outputs.

    Returns a tuple (parameter, row_count)
    """
    print(f"Starting fetch for parameter {param}")
    df = fetch_samples_for_parameter(param, bdate, edate, state)
    if df is None or df.empty:
        print(f"No data for parameter {param}")
        return param, 0
    _write_parameter_outputs(param, df)
    count = len(df)
    print(f"Wrote {count} rows for parameter {param}")
    return param, count


def run(parameters: List[str] | None = None, workers: int = DEFAULT_WORKERS) -> None:
    """Execute the sample-run pipeline.

    - parameters: optional list of AQS parameter codes (strings). If not
      provided, the script will load them from `ops/parameters.csv`.
    - workers: number of concurrent worker threads to fetch parameters.
    """
    config.ensure_dirs(config.RAW, config.TRANS, config.STAGED)
    # Register credentials for pyaqsapi (reads AQS_EMAIL/AQS_KEY from env)
    config.set_aqs_credentials()

    params = parameters or load_parameters_csv(str(config.PARAMS_CSV))
    print(f"Running sample ingest for {len(params)} parameters with {workers} workers")

    results = {}
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(_process_parameter, p, config.BDATE, config.EDATE, config.STATE): p for p in params}
        for fut in as_completed(futures):
            p = futures[fut]
            try:
                _, count = fut.result()
            except Exception as exc:
                print(f"Parameter {p} failed: {exc}")
                results[p] = 0
            else:
                results[p] = count

    total = sum(results.values())
    print(f"Finished sample ingest. Total rows fetched: {total}")


if __name__ == "__main__":
    run()
