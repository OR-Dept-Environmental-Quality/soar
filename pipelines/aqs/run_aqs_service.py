"""AQS pipeline orchestration for sample, annual, daily, and transform data.

This runner extracts air quality data from EPA's AQS API. The set of parameters to
extract is driven by `ops/dimPollutant.csv`:

- `aqs_parameter` is the AQS parameter code used for API calls.
- `analyte_name_deq` (or `analyte_name`) is used for display/logging.
- `group_store` controls how output files are grouped and named.

Services run consecutively, with year-by-year processing inside each service:
1. Sample data: sub-daily measurements for ALL parameters (toxics + non-toxics)
2. Annual data: annual aggregates for ALL parameters (toxics + non-toxics)
3. Daily data: daily summaries for NON-TOXICS parameters only

Filtering by group_store
------------------------
You can filter what runs by `group_store` using either:

- CLI flag: `--group-store pm25,ozone`
- Env var: `AQS_GROUP_STORE=pm25,ozone`

If neither is provided, all parameters in `ops/dimPollutant.csv` are included.

Selecting services
------------------
You can also limit which services execute (sample, annual, daily, transform):

- CLI flag: `--services sample,daily`
- Env var: `AQS_SERVICES=sample`

If neither is provided, the runner executes sample, annual, and daily services in order.
"""

from __future__ import annotations

import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

# Add src directory to Python path for direct module execution
# Allows running: python pipelines/aqs/run_aqs_service.py
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

import config
from aqs import _client
from aqs.extractors.aqs_service import fetch_samples_dispatch
from aqs.extractors.measurements import (
    write_annual_for_parameter,
    write_daily_for_parameter,
)
from aqs.transformers.trv_annual import transform_toxics_annual_trv
from aqs.transformers.trv_sample import transform_toxics_trv
from loaders.filesystem import append_csv, atomic_write_json, write_csv
from logging_config import (
    get_logger,
    log_error_with_context,
    log_pipeline_end,
    log_pipeline_start,
    setup_logging,
)

SAMPLE_BASE_DIR = config.RAW_AQS_SAMPLE

_session_local = threading.local()
TEST_MODE = os.getenv("AQS_TEST_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
_checkpoint_registry_lock = threading.Lock()
_checkpoint_locks: dict[str, threading.Lock] = {}
VALID_SERVICES = {"sample", "annual", "daily", "transform"}
DEFAULT_SERVICES = ("sample", "annual", "daily")


def _checkpoint_key(service: str, year: str | None) -> str:
    return f"{service}:{year or 'global'}"


def _get_checkpoint_lock(service: str, year: str | None) -> threading.Lock:
    key = _checkpoint_key(service, year)
    with _checkpoint_registry_lock:
        lock = _checkpoint_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _checkpoint_locks[key] = lock
    return lock


def _get_session():
    """Provide a thread-local requests.Session for connection reuse."""
    session = getattr(_session_local, "session", None)
    if session is None:
        session = _client.make_session(timeout=config.AQS_TIMEOUT)
        _session_local.session = session
    return session


def _checkpoint_path(service: str, year: str | None = None) -> Path:
    if year and year != "completed":
        filename = f"{service}_checkpoint_{year}.json"
    else:
        filename = f"{service}_checkpoint.json"
    return config.CTL_DIR / filename


def _load_checkpoint(service: str, year: str | None = None) -> dict:
    """Load checkpoint metadata for a service/year combination."""
    checkpoint_path = _checkpoint_path(service, year)
    lock = _get_checkpoint_lock(service, year)
    with lock:
        if checkpoint_path.exists():
            try:
                import json

                with open(checkpoint_path, encoding="utf-8") as fh:
                    return json.load(fh)
            except Exception:
                pass
    return {"last_param_index": -1}


def _save_checkpoint(service: str, year: str | None, param_index: int) -> None:
    """Persist checkpoint metadata for a service/year combination."""
    checkpoint = {"year": year, "last_param_index": param_index}
    checkpoint_path = _checkpoint_path(service, year)
    lock = _get_checkpoint_lock(service, year)
    with lock:
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(checkpoint_path, checkpoint)


def _clear_checkpoint(service: str, year: str | None = None) -> None:
    path = _checkpoint_path(service, year)
    lock = _get_checkpoint_lock(service, year)
    with lock:
        if path.exists():
            path.unlink()


def _write_parameter_outputs(param_label: str, frame: pd.DataFrame) -> None:
    SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    safe_label = _sanitize_filename(param_label)
    csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{safe_label}.csv"
    write_csv(frame, csv_path)


def _process_parameter_for_year(
    param_code: str,
    param_label: str,
    group_store: str,
    year: str,
    state: str,
    service: str,
) -> tuple[str, int, bool, str | None]:
    """Extract data for one parameter in one specific year.

    Returns a tuple of (parameter_name, rows_written, succeeded, error_message) so callers can
    decide whether to advance checkpoints and log failures."""
    bdate = f"{year}0101"
    edate = f"{year}1231"

    print(f"  Processing {service} data for {param_label} ({param_code}) in {year}")

    session = _get_session()

    try:
        if service == "sample":
            # Sample data extraction
            res = fetch_samples_dispatch(
                param_code, bdate, edate, state, session=session
            )
            total = 0

            if hasattr(res, "__iter__") and not isinstance(res, pd.DataFrame):
                # Streaming mode: process yearly data chunks
                SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
                for year_token, df in res:
                    if df is None or df.empty:
                        continue
                    year_csv = (
                        SAMPLE_BASE_DIR / f"aqs_sample_{group_store}_{year_token}.csv"
                    )
                    append_csv(df, year_csv)
                    total += len(df)
            else:
                # Legacy batch mode
                df_all = res
                if df_all is None or df_all.empty:
                    return param_label, 0, True, None
                SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
                csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{group_store}_{year}.csv"
                append_csv(df_all, csv_path)
                total += len(df_all)
            return param_label, total, True, None

        elif service == "annual":
            # Annual data extraction
            write_annual_for_parameter(
                param_code,
                param_label,
                bdate,
                edate,
                state,
                session=session,
                group_store=group_store,
            )
            # Count rows written (simplified - actual count would need to be returned from write_annual_for_parameter)
            total = 1  # Placeholder
            return param_label, total, True, None

        elif service == "daily":
            # Daily data extraction (criteria only)
            write_daily_for_parameter(
                param_code,
                param_label,
                bdate,
                edate,
                state,
                session=session,
                group_store=group_store,
            )
            # Count rows written (simplified)
            total = 1  # Placeholder
            return param_label, total, True, None

        raise ValueError(f"Unknown service type '{service}'")

    except Exception as exc:
        error_msg = str(exc)
        print(
            f"    ERROR: Parameter {param_label} ({param_code}) failed in {year}: {error_msg}"
        )
        return param_label, 0, False, error_msg


def _log_skipped_parameters(
    skipped_params: list[dict], service: str, year: str
) -> None:
    """Log skipped/failed parameters to CSV file in logs directory.
    
    Args:
        skipped_params: List of dicts with keys: param_code, param_label, group_store,
                       year, service, error_message, timestamp
        service: Service name (sample, annual, daily)
        year: Year being processed
    """
    if not skipped_params:
        return
    
    logs_dir = config.ROOT / "raw" / "aqs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = logs_dir / "skipped_parameters.csv"
    
    # Convert to DataFrame
    df = pd.DataFrame(skipped_params)
    
    # Append to existing file or create new with header
    from loaders.filesystem import append_csv
    append_csv(df, log_file)
    
    print(f"   📝 Logged {len(skipped_params)} skipped parameter(s) to {log_file.name}")


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


def _process_year_sample(
    year: str, all_params: list[tuple[str, str, str]], state: str
) -> int:
    """Process sample data extraction for a single year. Returns total rows written."""
    print(f"\n📅 Processing SAMPLE data for year {year}")
    print("-" * 40)

    year_sample_rows = 0
    skipped_params = []

    # Check checkpoint for resume capability
    checkpoint = _load_checkpoint("sample", year)
    start_param_index = checkpoint.get("last_param_index", -1) + 1

    # Process parameters sequentially (one at a time for retry runs)
    for index, (param_code, param_label, group_store) in enumerate(all_params):
        if index < start_param_index:
            print(f"  ⏭️  Skipping already processed {param_label} ({param_code})")
            continue
        _, rows, succeeded, error_msg = _process_parameter_for_year(
            param_code, param_label, group_store, year, state, "sample"
        )
        if succeeded:
            # Save checkpoint after each successful parameter
            _save_checkpoint("sample", year, index)
            year_sample_rows += rows
        else:
            # Log failure details for CSV output
            skipped_params.append({
                "param_code": param_code,
                "param_label": param_label,
                "group_store": group_store,
                "year": year,
                "service": "sample",
                "error_message": error_msg or "Unknown error",
                "timestamp": datetime.now().isoformat(),
            })
            print(
                f"  ⚠️  Skipping {param_label} after failure (will not block other parameters)"
            )
            # Still advance checkpoint so we don't retry this failed param forever
            _save_checkpoint("sample", year, index)

    _clear_checkpoint("sample", year)

    # Log skipped parameters to CSV
    if skipped_params:
        print(f"\n⚠️  {len(skipped_params)} parameter(s) failed for {year}:")
        for sp in skipped_params[:10]:  # Show first 10
            print(f"     - {sp['param_label']} ({sp['param_code']}): {sp['error_message'][:60]}")
        if len(skipped_params) > 10:
            print(f"     ... and {len(skipped_params) - 10} more")
        _log_skipped_parameters(skipped_params, "sample", year)

    print(f"✅ Completed SAMPLE extraction for {year}: {year_sample_rows} total rows")
    return year_sample_rows


def run_sample_service(
    years: list[str], all_params: list[tuple[str, str, str]], state: str
) -> None:
    """Run sample data extraction service for ALL parameters (toxics + criteria), year by year."""
    print("\n" + "=" * 60)
    print("STARTING SAMPLE SERVICE EXTRACTION (All Parameters)")
    print("=" * 60)

    total_sample_rows = 0

    # Process years concurrently (configurable to balance speed vs API limits)
    with ThreadPoolExecutor(max_workers=config.AQS_SAMPLE_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(_process_year_sample, year, all_params, state)
            for year in years
        ]
        for future in futures:
            total_sample_rows += future.result()

    # Mark service as completed
    _save_checkpoint("sample", None, -1)

    print(f"\n🎉 SAMPLE SERVICE COMPLETE: {total_sample_rows} total rows extracted")


def _process_year_annual(
    year: str, all_params: list[tuple[str, str, str]], state: str
) -> None:
    """Process annual data extraction for a single year."""
    print(f"\n📅 Processing ANNUAL data for year {year}")
    print("-" * 40)

    skipped_params = []

    for param_code, param_label, group_store in all_params:
        _, _, succeeded, error_msg = _process_parameter_for_year(
            param_code, param_label, group_store, year, state, "annual"
        )
        if not succeeded:
            skipped_params.append({
                "param_code": param_code,
                "param_label": param_label,
                "group_store": group_store,
                "year": year,
                "service": "annual",
                "error_message": error_msg or "Unknown error",
                "timestamp": datetime.now().isoformat(),
            })

    if skipped_params:
        print(f"\n⚠️  {len(skipped_params)} parameter(s) failed for {year}")
        _log_skipped_parameters(skipped_params, "annual", year)

    print(f"✅ Completed ANNUAL extraction for {year}")


def run_annual_service(
    years: list[str], all_params: list[tuple[str, str, str]], state: str
) -> None:
    """Run annual data extraction service for ALL parameters (toxics + criteria), year by year."""
    print("\n" + "=" * 60)
    print("STARTING ANNUAL SERVICE EXTRACTION (All Parameters)")
    print("=" * 60)

    # Process years concurrently (configurable to balance speed vs API limits)
    with ThreadPoolExecutor(max_workers=config.AQS_ANNUAL_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(_process_year_annual, year, all_params, state)
            for year in years
        ]
        for future in futures:
            future.result()  # Wait for completion

    print("\n🎉 ANNUAL SERVICE COMPLETE")


def _process_year_daily(
    year: str, daily_params: list[tuple[str, str, str]], state: str
) -> None:
    """Process daily data extraction for a single year."""
    print(f"\n📅 Processing DAILY data for year {year}")
    print("-" * 40)

    skipped_params = []

    for param_code, param_label, group_store in daily_params:
        _, _, succeeded, error_msg = _process_parameter_for_year(
            param_code, param_label, group_store, year, state, "daily"
        )
        if not succeeded:
            skipped_params.append({
                "param_code": param_code,
                "param_label": param_label,
                "group_store": group_store,
                "year": year,
                "service": "daily",
                "error_message": error_msg or "Unknown error",
                "timestamp": datetime.now().isoformat(),
            })

    if skipped_params:
        print(f"\n⚠️  {len(skipped_params)} parameter(s) failed for {year}")
        _log_skipped_parameters(skipped_params, "daily", year)

    print(f"✅ Completed DAILY extraction for {year}")


def run_daily_service(
    years: list[str], daily_params: list[tuple[str, str, str]], state: str
) -> None:
    """Run daily data extraction service for criteria pollutants only, year by year."""
    print("\n" + "=" * 60)
    print("STARTING DAILY SERVICE EXTRACTION (Criteria Pollutants Only)")
    print("=" * 60)

    # Process years concurrently (configurable to balance speed vs API limits)
    with ThreadPoolExecutor(max_workers=config.AQS_DAILY_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(_process_year_daily, year, daily_params, state)
            for year in years
        ]
        for future in futures:
            future.result()  # Wait for completion

    print("\n🎉 DAILY SERVICE COMPLETE")


def run_transform_service() -> None:
    """Run TRV transformation service for toxics data."""
    print("\n" + "=" * 60)
    print("STARTING TRANSFORM SERVICE (TRV Calculations)")
    print("=" * 60)

    # Transform sample toxics data to TRV exceedances
    print("\n🔄 Transforming SAMPLE toxics data to TRV exceedances...")
    import glob

    toxics_files = glob.glob(str(SAMPLE_BASE_DIR / "aqs_sample_toxics_*.csv"))
    dim_pollutant_path = ROOT / "ops" / "dimPollutant.csv"
    transform_dir = config.ROOT / "transform" / "trv" / "sample"
    transform_dir.mkdir(parents=True, exist_ok=True)

    for toxics_file in toxics_files:
        year = toxics_file.split("_")[-1].replace(".csv", "")  # Extract YYYY
        print(f"  Processing TRV for sample data year {year}...")
        df = pd.read_csv(toxics_file)
        if df.empty:
            continue
        transformed_df = transform_toxics_trv(df, str(dim_pollutant_path))
        output_path = transform_dir / f"trv_sample_{year}.csv"
        write_csv(transformed_df, output_path)
        print(f"  ✅ Transformed sample data for {year}: {len(transformed_df)} rows")

    # Transform annual toxics data to TRV exceedances
    print("\n🔄 Transforming ANNUAL toxics data to TRV exceedances...")
    annual_dir = config.RAW_AQS_ANNUAL
    annual_toxics_files = glob.glob(str(annual_dir / "aqs_annual_toxics_*.csv"))
    annual_transform_dir = config.ROOT / "transform" / "trv" / "annual"
    annual_transform_dir.mkdir(parents=True, exist_ok=True)

    for annual_file in annual_toxics_files:
        year = annual_file.split("_")[-1].replace(".csv", "")  # Extract YYYY
        print(f"  Processing TRV for annual data year {year}...")
        df = pd.read_csv(annual_file)
        if df.empty:
            continue
        transformed_df = transform_toxics_annual_trv(df, str(dim_pollutant_path))
        output_path = annual_transform_dir / f"trv_annual_{year}.csv"
        write_csv(transformed_df, output_path)
        print(f"  ✅ Transformed annual data for {year}: {len(transformed_df)} rows")

    print("\n🎉 TRANSFORM SERVICE COMPLETE")


def run(
    selected_group_stores: set[str] | None = None,
    selected_services: set[str] | None = None,
) -> None:
    """Execute full AQS data extraction pipeline in consecutive service order.

    Service execution order:
    1. Sample service (ALL parameters: toxics + criteria) - year by year, parameter by parameter
    2. Annual service (ALL parameters: toxics + criteria) - year by year, parameter by parameter
    3. Daily service (criteria only) - year by year, parameter by parameter
    4. Transform service - TRV calculations for toxics data only

    Environment Variables Required:
        BDATE: Begin date (YYYY-MM-DD format)
        EDATE: End date (YYYY-MM-DD format)
        STATE_CODE: State FIPS code (e.g., "41" for Oregon)
        DATAREPO_ROOT: Root directory for data lake output
        AQS_EMAIL: EPA AQS API email credential
        AQS_KEY: EPA AQS API key credential

    Optional controls:
        AQS_GROUP_STORE: Comma-separated list of group_store values to run
            (example: "pm25,ozone").
        AQS_SERVICES: Comma-separated subset of services to run
            (choose from "sample", "annual", "daily", "transform").
        AQS_TEST_MODE: When truthy, limits the run to a small number of parameters/years.
    """
    # Setup logging
    log_level = "DEBUG" if TEST_MODE else "INFO"
    setup_logging(level=log_level, verbose=TEST_MODE)

    logger = get_logger(__name__)
    log_pipeline_start(
        "AQS Full Pipeline", start_year=config.BDATE.year, end_year=config.EDATE.year
    )

    logger.info("🚀 Starting AQS Pipeline Execution")
    logger.info(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Setup and validation
    config.ensure_dirs(
        config.RAW_AQS_SAMPLE,
        config.RAW_AQS_ANNUAL,
        config.RAW_AQS_DAILY,
        config.CTL_DIR,
    )
    config.set_aqs_credentials()

    if selected_services:
        unknown_services = selected_services - VALID_SERVICES
        if unknown_services:
            raise ValueError(
                "Unknown service(s): "
                + ", ".join(sorted(unknown_services))
                + f". Valid options: {sorted(VALID_SERVICES)}"
            )
        services_to_run = set(selected_services)
    else:
        services_to_run = set(DEFAULT_SERVICES)

    logger.info(f"🧩 Services scheduled: {', '.join(sorted(services_to_run))}")

    # Check circuit breaker
    if _client.circuit_is_open():
        from loaders.filesystem import atomic_write_json

        manifest_dir = ROOT / "metadata"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        degraded = {
            "status": "degraded",
            "reason": "AQS circuit is open; skipping heavy extraction",
        }
        atomic_write_json(manifest_dir / "run_manifest_degraded.json", degraded)
        logger.warning("❌ AQS circuit is open — wrote degraded manifest and exiting")
        log_pipeline_end(
            "AQS Full Pipeline", success=False, reason="circuit_breaker_open"
        )
        return

    # Load parameter lists
    print("\n📋 Loading parameter definitions...")
    dfp = pd.read_csv("ops/dimPollutant.csv", dtype=str)
    if (
        "aqs_parameter" not in dfp.columns
        or "group_store" not in dfp.columns
    ):
        raise KeyError(
            "ops/dimPollutant.csv must contain 'aqs_parameter' and 'group_store' columns"
        )

    label_col = "analyte_name_deq" if "analyte_name_deq" in dfp.columns else "analyte_name"
    if label_col not in dfp.columns:
        raise KeyError(
            "ops/dimPollutant.csv must contain either 'analyte_name_deq' or 'analyte_name' column"
        )

    dfp["group_store_norm"] = dfp["group_store"].astype(str).str.strip().str.lower()

    if selected_group_stores:
        available = set(dfp["group_store_norm"].dropna().unique())
        unknown = set(selected_group_stores) - available
        if unknown:
            raise ValueError(
                f"Unknown group_store(s): {sorted(unknown)}. Available: {sorted(available)}"
            )
        dfp_run = dfp[dfp["group_store_norm"].isin(selected_group_stores)]
    else:
        dfp_run = dfp

    all_params = list(
        dfp_run[["aqs_parameter", label_col, "group_store_norm"]]
        .dropna()
        .drop_duplicates(subset=["aqs_parameter"])
        .itertuples(index=False, name=None)
    )

    if not all_params:
        raise ValueError(
            "No parameters selected from ops/dimPollutant.csv (check group_store values and any group-store filter)."
        )

    daily_params = list(
        dfp_run[dfp_run["group_store_norm"] != "toxics"]
        [["aqs_parameter", label_col, "group_store_norm"]]
        .dropna()
        .drop_duplicates(subset=["aqs_parameter"])
        .itertuples(index=False, name=None)
    )

    if TEST_MODE and len(all_params) > 5:
        print("🧪 TEST MODE: Limiting to 5 parameters")
        all_params = all_params[:5]

    if TEST_MODE and len(daily_params) > 5:
        daily_params = daily_params[:5]

    print(f"📊 Selected {len(all_params)} parameters for sample+annual")
    print(f"📊 Selected {len(daily_params)} parameters for daily (non-toxics)")

    # Generate years list
    start_year = config.BDATE.year
    end_year = config.EDATE.year
    years = [str(year) for year in range(start_year, end_year + 1)]

    if TEST_MODE and len(years) > 2:
        print("🧪 TEST MODE: Limiting to 2 years")
        years = years[:2]

    print(f"📅 Processing years: {', '.join(years)}")

    # Execute services in consecutive order (extraction only, skip transform)
    try:
        if "sample" in services_to_run:
            run_sample_service(years, all_params, config.STATE)

        if "annual" in services_to_run:
            run_annual_service(years, all_params, config.STATE)

        if "daily" in services_to_run:
            run_daily_service(years, daily_params, config.STATE)

        if "transform" in services_to_run:
            run_transform_service()

        logger.info("=" * 60)
        logger.info("🎉 AQS PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        log_pipeline_end(
            "AQS Full Pipeline",
            success=True,
            total_years=len(years),
            total_params=len(all_params),
        )

    except Exception as e:
        log_error_with_context(e, "AQS Full Pipeline", pipeline_stage="execution")
        logger.error(f"❌ Pipeline execution failed: {e}")
        log_pipeline_end("AQS Full Pipeline", success=False, error=str(e))
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run AQS extraction services driven by ops/dimPollutant.csv"
    )
    parser.add_argument(
        "--group-store",
        help=(
            "Comma-separated list of group_store values to run (e.g. 'pm25' or 'pm25,ozone'). "
            "If omitted, runs all group_store values. Can also be set via AQS_GROUP_STORE env var."
        ),
    )
    parser.add_argument(
        "--services",
        help=(
            "Comma-separated list of services to run (choose from 'sample', 'annual', 'daily', 'transform'). "
            "If omitted, runs the default sample+annual+daily trio. Can also be set via AQS_SERVICES env var."
        ),
    )
    args = parser.parse_args()

    raw_group = (args.group_store or os.getenv("AQS_GROUP_STORE", "")).strip()
    selected_groups = {s.strip().lower() for s in raw_group.split(",") if s.strip()} or None

    raw_services = (args.services or os.getenv("AQS_SERVICES", "")).strip()
    selected_services = {
        s.strip().lower() for s in raw_services.split(",") if s.strip()
    } or None

    run(
        selected_group_stores=selected_groups,
        selected_services=selected_services,
    )
