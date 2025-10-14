"""AQS service pipeline orchestration for sample, annual, and daily data.

This runner orchestrates all AQS data ingestion services (sample, annual, daily)
and writes per-parameter outputs organized by group_store and year.
"""
from __future__ import annotations
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import re

# Ensure src is importable when running this module directly
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
    """Stream-fetch samples for a parameter and append results to group-based CSVs.

        This function streams sample data for a parameter and appends results to
        per-group, per-year CSV files. File naming: aqs_sample_{group_store}_{year}.csv
        where group_store comes from dimPollutant.csv (toxics, pm25, ozone, other, etc.)
        
        Behavior depends on `config.SAMPLE_MODE`:
        - by_state: `fetch_samples_dispatch` yields (year_token, DataFrame) tuples
            containing all sites for the parameter in that year; each yielded frame
            is appended immediately to aqs_sample_{group}_{year}.csv.
        - by_site (legacy): `fetch_samples_dispatch` returns a single DataFrame
            containing concatenated per-site results; the full DataFrame is appended
            to the default per-parameter CSV.
    """
    group_store = get_parameter_group(param_code)
    total = 0

    # Dispatch to configured mode. For by_state mode `fetch_samples_dispatch`
    # returns a generator yielding (year_token, DataFrame) tuples. For the
    # legacy per-site behavior it returns a single DataFrame object.
    res = fetch_samples_dispatch(param_code, bdate, edate, state)
    if hasattr(res, "__iter__") and not isinstance(res, pd.DataFrame):
        # by_state generator - write to group-based year files
        for year_token, df in res:
            if df is None or df.empty:
                continue
            year_dir = SAMPLE_BASE_DIR / year_token
            year_dir.mkdir(parents=True, exist_ok=True)
            year_csv = year_dir / f"aqs_sample_{group_store}_{year_token}.csv"
            append_csv(df, year_csv)
            total += len(df)
    else:
        # legacy per-site DataFrame - write to group-based file at root
        df_all = res
        if df_all is None or df_all.empty:
            return param_label, 0
        # append all rows into a legacy sample file at the root sample folder
        SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{group_store}.csv"
        append_csv(df_all, csv_path)
        total += len(df_all)

    # write annual aggregates to RAW_ANNUAL (best effort, ignore errors per parameter)
    try:
        write_annual_for_parameter(param_code, param_label, bdate, edate, state, group_store=group_store)
    except Exception as exc:  # pragma: no cover - runtime safety
        print(f"Annual data fetch failed for {param_label} ({param_code}): {exc}")

    # write daily summaries to RAW_DAILY (best effort, ignore errors per parameter)
    try:
        write_daily_for_parameter(param_code, param_label, bdate, edate, state, group_store=group_store)
    except Exception as exc:  # pragma: no cover - runtime safety
        print(f"Daily data fetch failed for {param_label} ({param_code}): {exc}")

    return param_label, total


def _sanitize_filename(name: str, max_len: int = 80) -> str:
    """Return a filesystem-safe token based on `name`.

    Behavior:
    - normalize whitespace to single dashes
    - remove characters that are not alphanumeric, dash, underscore, or dot
    - collapse repeated separators
    - strip leading/trailing separators
    - limit total length to max_len
    """
    if not name:
        return "unknown"

    # normalize whitespace to single dash
    s = re.sub(r"\s+", "-", name)
    # remove unsafe characters
    s = re.sub(r"[^A-Za-z0-9._-]", "", s)
    # collapse multiple dashes/underscores
    s = re.sub(r"[-_]{2,}", "-", s)
    # strip leading/trailing non-alnum
    s = re.sub(r"(^[^A-Za-z0-9]+)|([^A-Za-z0-9]+$)", "", s)
    if not s:
        return "unknown"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-_.")
    return s


def run(workers: int = DEFAULT_WORKERS) -> None:
    config.ensure_dirs(config.RAW_SAMPLE, config.RAW_ANNUAL, config.TRANS, config.STAGED)
    config.set_aqs_credentials()

    # Short-circuit if AQS is currently marked unhealthy by the circuit-breaker
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

    # Load parameters: always use the pollutant dimension file provided by the
    # project, which contains the approved parameter list for sample runs.
    dfp = pd.read_csv("ops/dimPollutant.csv", dtype=str)
    if "aqs_parameter" not in dfp.columns or "analyte_name" not in dfp.columns:
        raise KeyError("ops/dimPollutant.csv must contain 'aqs_parameter' and 'analyte_name' columns")
    params_list = list(dfp[["aqs_parameter", "analyte_name"]].dropna().itertuples(index=False, name=None))

    print(f"Running sample ingest for {len(params_list)} parameters with {workers} workers")

    results = {}
    # Use the policy-enforced start date (clamped to >= 2005-01-01)
    bdate = config.clamped_bdate()
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
