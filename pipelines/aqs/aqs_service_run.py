"""AQS service pipeline orchestration for sample, annual, and daily data.

This pipeline extracts air quality data from EPA's AQS API for three service types:
- Sample data: Hourly/sub-daily measurements from monitoring stations
- Annual data: Annual statistical aggregates (mean, max, percentiles, etc.)
- Daily data: Daily statistical summaries (mean, max, AQI, etc.)

Output files are organized by pollutant group_store (toxics, pm25, ozone, other) and year,
with all files written directly to service folders (no year subdirectories).
"""
from __future__ import annotations
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import re

# Add src directory to Python path for direct module execution
# Allows running: python pipelines/aqs/aqs_service_run.py
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config
from aqs.extractors.site_extractors import fetch_monitors, build_aqs_requests, fetch_aqs_response
from aqs.extractors.aqs_service import fetch_samples_dispatch
from aqs.extractors.data import write_annual_for_parameter, write_daily_for_parameter
from loaders.filesystem import write_csv, append_csv
from aqs import _client
from utils import get_parameter_group
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

SAMPLE_BASE_DIR = config.RAW_SAMPLE
DEFAULT_WORKERS = 4


def _write_parameter_outputs(param_label: str, frame: pd.DataFrame) -> None:
    SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    safe_label = _sanitize_filename(param_label)
    csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{safe_label}.csv"
    write_csv(frame, csv_path)


def _process_parameter(param_code: str, param_label: str, bdate: str, edate: str, state: str) -> tuple[str, int]:
    """Extract and write all three data types (sample, annual, daily) for one parameter.
    
    This function coordinates the extraction of sample, annual, and daily data for a single
    air quality parameter. Data is organized by group_store category and written to CSV files.
    
    Args:
        param_code: AQS parameter code (e.g., "44201" for Ozone)
        param_label: Human-readable parameter name (e.g., "Ozone")
        bdate: Begin date in YYYYMMDD format
        edate: End date in YYYYMMDD format
        state: State FIPS code (e.g., "41" for Oregon)
    
    Returns:
        Tuple of (parameter_name, total_sample_rows_written)
    
    Output files:
        - raw/aqs/sample/aqs_sample_{group_store}_{year}.csv
        - raw/aqs/annual/aqs_annual_{group_store}_{year}.csv  
        - raw/aqs/daily/aqs_daily_{group_store}_{year}.csv
    
    Note: Annual and daily extractions use best-effort error handling; failures are logged
    but don't stop sample data extraction.
    """
    group_store = get_parameter_group(param_code)
    total = 0

    # Check if response is a generator (by_state mode) or DataFrame (legacy by_site mode)
    # by_state mode: yields (year, DataFrame) tuples for memory-efficient yearly streaming
    # by_site mode: returns single concatenated DataFrame with all years
    res = fetch_samples_dispatch(param_code, bdate, edate, state)
    if hasattr(res, "__iter__") and not isinstance(res, pd.DataFrame):
        # Streaming mode: process yearly data chunks as they arrive
        # Writes to: raw/aqs/sample/aqs_sample_{group_store}_{year}.csv
        SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
        for year_token, df in res:
            if df is None or df.empty:
                continue
            year_csv = SAMPLE_BASE_DIR / f"aqs_sample_{group_store}_{year_token}.csv"
            append_csv(df, year_csv)  # Appends to existing file or creates with header
            total += len(df)
    else:
        # Legacy batch mode: entire dataset returned at once
        # Writes to: raw/aqs/sample/aqs_sample_{group_store}.csv (no year in filename)
        df_all = res
        if df_all is None or df_all.empty:
            return param_label, 0
        SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{group_store}.csv"
        append_csv(df_all, csv_path)
        total += len(df_all)

    # Extract annual aggregates (mean, max, percentiles by year)
    # Best-effort: failures logged but don't stop sample extraction
    try:
        write_annual_for_parameter(param_code, param_label, bdate, edate, state, group_store=group_store)
    except Exception as exc:  # pragma: no cover - runtime safety
        print(f"Annual data fetch failed for {param_label} ({param_code}): {exc}")

    # Extract daily summaries (daily mean, max, AQI by date)
    # Best-effort: failures logged but don't stop sample extraction
    try:
        write_daily_for_parameter(param_code, param_label, bdate, edate, state, group_store=group_store)
    except Exception as exc:  # pragma: no cover - runtime safety
        print(f"Daily data fetch failed for {param_label} ({param_code}): {exc}")

    return param_label, total


def _sanitize_filename(name: str, max_len: int = 80) -> str:
    """Convert parameter name to filesystem-safe filename component.
    
    Transforms human-readable names like "PM2.5 - Local Conditions" into
    safe filenames like "PM2-5-Local-Conditions".
    
    Rules:
        - Whitespace → single dash
        - Remove unsafe characters (keep only alphanumeric, dash, underscore, dot)
        - Collapse repeated separators
        - Strip leading/trailing separators
        - Limit length to max_len characters
    
    Args:
        name: Parameter name to sanitize
        max_len: Maximum allowed filename length (default 80)
    
    Returns:
        Filesystem-safe string suitable for use in filenames
    """
    if not name:
        return "unknown"

    # Normalize whitespace to single dash
    s = re.sub(r"\s+", "-", name)
    # Remove unsafe characters (keep only alphanumeric, dot, dash, underscore)
    s = re.sub(r"[^A-Za-z0-9._-]", "", s)
    # Collapse multiple consecutive dashes/underscores
    s = re.sub(r"[-_]{2,}", "-", s)
    # Strip leading/trailing non-alphanumeric characters
    s = re.sub(r"(^[^A-Za-z0-9]+)|([^A-Za-z0-9]+$)", "", s)
    if not s:
        return "unknown"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-_.")
    return s


def run(workers: int = DEFAULT_WORKERS) -> None:
    """Execute full AQS data extraction pipeline for sample, annual, and daily data.
    
    This is the main entry point that coordinates the entire extraction process:
    1. Validates environment and AQS API health
    2. Loads parameter list from ops/dimPollutant.csv
    3. Spawns worker threads to process parameters concurrently
    4. Extracts sample, annual, and daily data for each parameter
    
    Args:
        workers: Number of concurrent worker threads (default 4)
    
    Environment Variables Required:
        BDATE: Begin date (YYYY-MM-DD format)
        EDATE: End date (YYYY-MM-DD format)
        STATE_CODE: State FIPS code (e.g., "41" for Oregon)
        DATAREPO_ROOT: Root directory for data lake output
        AQS_EMAIL: EPA AQS API email credential
        AQS_KEY: EPA AQS API key credential
    """
    config.ensure_dirs(config.RAW_SAMPLE, config.RAW_ANNUAL, config.TRANS, config.STAGED)
    config.set_aqs_credentials()

    # Check if AQS API circuit breaker is open (too many recent failures)
    # If open, skip extraction and write degraded status manifest
    if _client.circuit_is_open():
        from loaders.filesystem import atomic_write_json

        manifest_dir = Path(__file__).resolve().parents[2] / "metadata"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        degraded = {
            "status": "degraded",
            "reason": "AQS circuit is open; skipping heavy extraction",
        }
        atomic_write_json(manifest_dir / "run_manifest_degraded.json", degraded)
        print("AQS circuit is open — wrote degraded manifest and exiting")
        return

    # Load parameter list from pollutant dimension table
    # This CSV defines which parameters to extract and their metadata
    dfp = pd.read_csv("ops/dimPollutant.csv", dtype=str)
    if "aqs_parameter" not in dfp.columns or "analyte_name" not in dfp.columns:
        raise KeyError("ops/dimPollutant.csv must contain 'aqs_parameter' and 'analyte_name' columns")
    params_list = list(dfp[["aqs_parameter", "analyte_name"]].dropna().itertuples(index=False, name=None))

    print(f"Running sample ingest for {len(params_list)} parameters with {workers} workers")

    results = {}
    # Apply policy-enforced begin date (cannot be earlier than 2005-01-01)
    bdate = config.clamped_bdate()
    # Process parameters concurrently using thread pool
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = {exe.submit(_process_parameter, code, label, bdate, config.EDATE, config.STATE): (code, label) for code, label in params_list}
        for fut in as_completed(futures):
            code, label = futures[fut]
            try:
                _, count = fut.result()
            except Exception as exc:
                print(f"Parameter {label} ({code}) failed: {exc}")
                results[label] = 0
            else:
                results[label] = count

    total = sum(results.values())
    print(f"Finished sample ingest. Total rows fetched: {total}")


if __name__ == "__main__":
    run()
