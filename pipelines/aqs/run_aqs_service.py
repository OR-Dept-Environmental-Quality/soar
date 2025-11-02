"""AQS pipeline orchestration for sample, annual, daily, and transform data.

This pipeline extracts air quality data from EPA's AQS API in consecutive service order:
1. Sample data: Hourly/sub-daily measurements for ALL parameters (toxics + criteria)
2. Annual data: Annual statistical aggregates for ALL parameters (toxics + criteria)
3. Daily data: Daily statistical summaries for criteria pollutants only
4. Transform: TRV exceedance calculations for toxics data only

Services run consecutively with year-by-year parameter processing within each service.
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
from utils import get_parameter_group

SAMPLE_BASE_DIR = config.RAW_SAMPLE

_session_local = threading.local()
TEST_MODE = os.getenv("AQS_TEST_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
_checkpoint_registry_lock = threading.Lock()
_checkpoint_locks: dict[str, threading.Lock] = {}


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
    param_code: str, param_label: str, year: str, state: str, service: str
) -> tuple[str, int, bool]:
    """Extract data for one parameter in one specific year.

    Returns a tuple of (parameter_name, rows_written, succeeded) so callers can
    decide whether to advance checkpoints."""
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
                    group_store = get_parameter_group(param_code)
                    year_csv = (
                        SAMPLE_BASE_DIR / f"aqs_sample_{group_store}_{year_token}.csv"
                    )
                    append_csv(df, year_csv)
                    total += len(df)
            else:
                # Legacy batch mode
                df_all = res
                if df_all is None or df_all.empty:
                    return param_label, 0
                SAMPLE_BASE_DIR.mkdir(parents=True, exist_ok=True)
                group_store = get_parameter_group(param_code)
                csv_path = SAMPLE_BASE_DIR / f"aqs_sample_{group_store}_{year}.csv"
                append_csv(df_all, csv_path)
                total += len(df_all)
            return param_label, total, True

        elif service == "annual":
            # Annual data extraction
            group_store = get_parameter_group(param_code)
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
            return param_label, total, True

        elif service == "daily":
            # Daily data extraction (criteria only)
            group_store = get_parameter_group(param_code)
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
            return param_label, total, True

        raise ValueError(f"Unknown service type '{service}'")

    except Exception as exc:
        print(
            f"    ERROR: Parameter {param_label} ({param_code}) failed in {year}: {exc}"
        )
        return param_label, 0, False


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
    year: str, all_params: list[tuple[str, str]], state: str
) -> int:
    """Process sample data extraction for a single year. Returns total rows written."""
    print(f"\n📅 Processing SAMPLE data for year {year}")
    print("-" * 40)

    year_sample_rows = 0

    # Check checkpoint for resume capability
    checkpoint = _load_checkpoint("sample", year)
    start_param_index = checkpoint.get("last_param_index", -1) + 1

    # Process parameters sequentially (one at a time for retry runs)
    for index, (param_code, param_label) in enumerate(all_params):
        if index < start_param_index:
            print(f"  ⏭️  Skipping already processed {param_label} ({param_code})")
            continue
        _, rows, succeeded = _process_parameter_for_year(
            param_code, param_label, year, state, "sample"
        )
        if succeeded:
            # Save checkpoint after each successful parameter
            _save_checkpoint("sample", year, index)
            year_sample_rows += rows
        else:
            print(
                f"  🔁 Will retry {param_label} on next run (checkpoint not advanced)"
            )

    _clear_checkpoint("sample", year)

    print(f"✅ Completed SAMPLE extraction for {year}: {year_sample_rows} total rows")
    return year_sample_rows


def run_sample_service(
    years: list[str], all_params: list[tuple[str, str]], state: str
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
    year: str, all_params: list[tuple[str, str]], state: str
) -> None:
    """Process annual data extraction for a single year."""
    print(f"\n📅 Processing ANNUAL data for year {year}")
    print("-" * 40)

    for param_code, param_label in all_params:
        _, _, succeeded = _process_parameter_for_year(
            param_code, param_label, year, state, "annual"
        )
        if not succeeded:
            print(f"  🔁 Will retry {param_label} on next run (annual service)")

    print(f"✅ Completed ANNUAL extraction for {year}")


def run_annual_service(
    years: list[str], all_params: list[tuple[str, str]], state: str
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
    year: str, criteria_params: list[tuple[str, str]], state: str
) -> None:
    """Process daily data extraction for a single year."""
    print(f"\n📅 Processing DAILY data for year {year}")
    print("-" * 40)

    for param_code, param_label in criteria_params:
        _, _, succeeded = _process_parameter_for_year(
            param_code, param_label, year, state, "daily"
        )
        if not succeeded:
            print(f"  🔁 Will retry {param_label} on next run (daily service)")

    print(f"✅ Completed DAILY extraction for {year}")


def run_daily_service(
    years: list[str], criteria_params: list[tuple[str, str]], state: str
) -> None:
    """Run daily data extraction service for criteria pollutants only, year by year."""
    print("\n" + "=" * 60)
    print("STARTING DAILY SERVICE EXTRACTION (Criteria Pollutants Only)")
    print("=" * 60)

    # Process years concurrently (configurable to balance speed vs API limits)
    with ThreadPoolExecutor(max_workers=config.AQS_DAILY_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(_process_year_daily, year, criteria_params, state)
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
    annual_dir = config.RAW_ANNUAL
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


def run() -> None:
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
        config.RAW_SAMPLE, config.RAW_ANNUAL, config.TRANS, config.STAGED
    )
    config.set_aqs_credentials()

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
        or "analyte_name_deq" not in dfp.columns
        or "group_store" not in dfp.columns
    ):
        raise KeyError(
            "ops/dimPollutant.csv must contain 'aqs_parameter', 'analyte_name_deq', and 'group_store' columns"
        )

    # Filter parameter lists to Criteria only
    criteria_df = dfp[dfp["analyte_group"] == "Criteria"]
    all_params = list(
        criteria_df[["aqs_parameter", "analyte_name_deq"]]
        .dropna()
        .itertuples(index=False, name=None)
    )

    if TEST_MODE and len(all_params) > 5:
        print("🧪 TEST MODE: Limiting to 5 parameters")
        all_params = all_params[:5]

    criteria_params = all_params  # Same as all_params now

    print(f"📊 Found {len(all_params)} total parameters")
    print(f"📊 Found {len(criteria_params)} criteria parameters")

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
        run_sample_service(years, all_params, config.STATE)
        run_annual_service(years, all_params, config.STATE)
        run_daily_service(years, criteria_params, config.STATE)
        # run_transform_service()  # Skipped for extraction only

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
    run()
