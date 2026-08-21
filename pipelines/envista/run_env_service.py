"""Envista service pipeline for extracting station metadata and measurements.

Retrieves Envista station metadata, builds monitor information tables,
and processes measurement data using the Envista API with concurrent
site-year extraction.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import pandas as pd

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import after path is set
import config

from logging_config import (
     setup_logging, get_logger, log_pipeline_start, 
     log_error_with_context, log_pipeline_end)
from loaders.filesystem import write_csv
from envista.extractors.monitors import extract_envista_station_data
from envista.extractors.measurements import get_envista_hourly, get_envista_daily


ENV_TEST_MODE = str(config.ENV_TEST_MODE).lower() in ("1", "true", "yes")
ENV_MONITOR_DIR = config.RAW_ENV_MONITORS
ENV_SAMPLE_DIR = config.RAW_ENV_SAMPLE
ENV_DAILY_DIR = config.RAW_ENV_DAILY

BDATE = config.BDATE
EDATE = config.EDATE
ENV_SAMPLE_YEAR_WORKERS = max(1, int(os.getenv("ENV_SAMPLE_YEAR_WORKERS", "3")))
ENV_SAMPLE_SITE_WORKERS = max(1, int(os.getenv("ENV_SAMPLE_SITE_WORKERS", "3")))

_session_local = threading.local()
_data_lock = threading.Lock()
_combined_sample_results: dict[tuple[str, str], pd.DataFrame] = {}  # Key format: (group_store, year)
_combined_daily_results: dict[tuple[str, str], pd.DataFrame] = {}  # Key format: (group_store, year)

if BDATE < date(2018, 7, 1): BDATE = date(2018, 7, 1)  # Envista data starts mid-2018


def _load_envista_group_catalog() -> pd.DataFrame:
    """Load monitor_name to group_store mappings from Envista dimension tables."""
    
    df = pd.read_csv("ops/dimPollutant_Envista.csv", dtype=str)
    normalized_cols = {str(col).strip(): col for col in df.columns}

    if "monitor_name" in normalized_cols and "group_store" in normalized_cols:
        df = df[[normalized_cols["monitor_name"], normalized_cols["group_store"]]].copy()
        df.columns = ["monitor_name", "group_store"]
        df = df.dropna(subset=["monitor_name", "group_store"]).drop_duplicates()
        df["monitor_name"] = df["monitor_name"].astype(str).str.strip()
        df["group_store"] = df["group_store"].astype(str).str.strip()
        return df

    raise FileNotFoundError(
        "No Envista pollutant catalog file with monitor_name and group_store columns was found in ops/."
    )


def _parse_requested_filters(argv: list[str] | None = None) -> tuple[list[str] | None, str]:
    """Return selected group_store values and chosen service from CLI args or env."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--group-store",
        "--group-stores",
        nargs="+",
        default=None,
        help="One or more group_store values to retrieve; e.g. --group-store pm25 carbonaceous_aerosol",
    )
    parser.add_argument(
        "--service",
        choices=["hourly", "daily", "both"],
        default=None,
        help="Which Envista service to run: hourly, daily, or both",
    )
    args, _ = parser.parse_known_args(argv)

    raw_group_values: list[str] = []
    if args.group_store:
        raw_group_values.extend(args.group_store)

    env_group_value = os.getenv("ENV_GROUP_STORE", "").strip()
    if env_group_value:
        raw_group_values.extend(part.strip() for part in env_group_value.split(","))

    if raw_group_values:
        normalized_group_values = []
        seen: set[str] = set()
        for value in raw_group_values:
            for part in str(value).split(","):
                cleaned = part.strip()
                if not cleaned:
                    continue
                key = cleaned.casefold()
                if key not in seen:
                    normalized_group_values.append(cleaned)
                    seen.add(key)
        requested_group_stores = normalized_group_values
    else:
        requested_group_stores = None

    if args.service:
        requested_service = args.service
    else:
        env_service_value = os.getenv("ENV_SERVICE", "").strip().casefold()
        requested_service = env_service_value if env_service_value in {"hourly", "daily", "both"} else "both"

    return requested_group_stores, requested_service


def _process_parameter_for_year(
    station_name: str,
    station_id: str,
    channel_name: str,
    channel_id: str,
    year: str,
    group_store: str,
    service: str,
) -> tuple[str, str, str, int, bool]:
    """Extract Envista data for one site and one calendar year, by selected service."""
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    logger = get_logger(__name__)
    logger.debug(
        f"Extracting {service} data for {station_name}:{channel_name}, {station_id}:{channel_id} in {year}"
    )

    try:
        if service == "hourly":
            data = get_envista_hourly(
                station_id=station_id,
                channel_id=channel_id,
                from_date=from_date,
                to_date=to_date,
            )
            storage_dict = _combined_sample_results
            storage_label = "hourly"
        else:
            data = get_envista_daily(
                station_id=station_id,
                channel_id=channel_id,
                from_date=from_date,
                to_date=to_date,
            )
            storage_dict = _combined_daily_results
            storage_label = "daily"

        if data is None or data.empty:
            logger.warning(
                f"No {storage_label} data retrieved for {station_name}:{station_id}, {channel_name}:{channel_id} "
                f"in {year}"
            )
            return station_id, channel_id, year, 0, False

        all_na_cols = data.columns[data.isna().all()].tolist()
        if len(all_na_cols) == len(data.columns):
            logger.warning(
                f"No {storage_label} data for {station_name}:{station_id}, {channel_name}:{channel_id} in {year}."
            )
            return station_id, channel_id, year, 0, False

        row_count = len(data)
        logger.info(
            f"Retrieved {row_count} {storage_label} records for {station_name}:{station_id}, "
            f"{channel_name}:{channel_id} in {year}."
        )

        with _data_lock:
            key = (group_store, year)
            if key in storage_dict:
                storage_dict[key] = pd.concat(
                    [storage_dict[key], data],
                    ignore_index=True,
                )
            else:
                storage_dict[key] = data.copy()

        return station_id, channel_id, year, row_count, True

    except Exception as e:
        logger.error(
            f"Error retrieving {service} data for {station_name}:{station_id}, {channel_name}:{channel_id} "
            f"in {year}: {e}",
            exc_info=True,
        )
        return station_id, channel_id, year, 0, False


def _filter_sites_for_year(year: str, sites: list[dict]) -> list[dict]:
    """Return only the sites whose mon_start_date year is on or before the selected year."""
    year_num = int(year)
    filtered_sites: list[dict] = []

    for site in sites:
        mon_start_date = site.get("mon_start_date")
        if mon_start_date in (None, "", pd.NaT):
            continue

        try:
            start_year = pd.to_datetime(mon_start_date).year
        except (TypeError, ValueError):
            continue

        if start_year <= year_num:
            filtered_sites.append(site)

    return filtered_sites


def _process_sample_year(
    year: str, sites: list[dict], site_workers: int
) -> int:
    """Process all hourly site extractions for a single year."""
    logger = get_logger(__name__)
    eligible_sites = _filter_sites_for_year(year, sites)
    logger.info(f"Processing {len(eligible_sites)} eligible sites concurrently for hourly data in {year}")

    year_total_rows = 0

    with ThreadPoolExecutor(max_workers=site_workers) as executor:
        futures = [
            executor.submit(
                _process_parameter_for_year,
                str(site['name']),
                str(site['station_id']),
                str(site['monitor_name']),
                str(site['channel_id']),
                year,
                str(site.get('group_store', 'unknown')),
                "hourly",
            )
            for site in eligible_sites
        ]

        for future in futures:
            _, _, _, hourly_rows, succeeded = future.result()
            if succeeded:
                year_total_rows += hourly_rows

    logger.info(f"Completed hourly processing for {year}: {year_total_rows} total rows.")
    return year_total_rows


def _process_daily_year(
    year: str, sites: list[dict], site_workers: int
) -> int:
    """Process all daily site extractions for a single year."""
    logger = get_logger(__name__)
    eligible_sites = _filter_sites_for_year(year, sites)
    logger.info(f"Processing {len(eligible_sites)} eligible sites concurrently for daily data in {year}")

    year_total_rows = 0

    with ThreadPoolExecutor(max_workers=site_workers) as executor:
        futures = [
            executor.submit(
                _process_parameter_for_year,
                str(site['name']),
                str(site['station_id']),
                str(site['monitor_name']),
                str(site['channel_id']),
                year,
                str(site.get('group_store', 'unknown')),
                "daily",
            )
            for site in eligible_sites
        ]

        for future in futures:
            _, _, _, daily_rows, succeeded = future.result()
            if succeeded:
                year_total_rows += daily_rows

    logger.info(f"Completed daily processing for {year}: {year_total_rows} total rows.")
    return year_total_rows


def run_sample_service(
    years: list[str], sites: list[dict]
) -> None:
    """Run Envista sample data extraction concurrently by year and site."""
    logger = get_logger(__name__)

    _combined_sample_results.clear()

    print("\n" + "=" * 60)
    print("STARTING ENVISTA SAMPLE SERVICE")
    print("=" * 60)

    total_sample_rows = 0

    with ThreadPoolExecutor(max_workers=ENV_SAMPLE_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(
                _process_sample_year,
                year,
                sites,
                ENV_SAMPLE_SITE_WORKERS,
            )
            for year in years
        ]

        for future in futures:
            total_sample_rows += future.result()

    logger.info(f"Sample service complete: {total_sample_rows} total sample rows extracted.")

    config.ensure_dirs(ENV_SAMPLE_DIR)
    for (group_store, year), df in _combined_sample_results.items():
        if df.empty:
            logger.warning(f"Skipping {group_store} year {year}: DataFrame is empty")
            continue

        all_na_cols = df.columns[df.isna().all()].tolist()
        if len(all_na_cols) == len(df.columns):
            logger.warning(f"Skipping {group_store} year {year}: All columns contain only NA values")
            continue

        output_file = ENV_SAMPLE_DIR / f"env_sample_{group_store}_{year}.csv"
        write_csv(df, output_file)
        logger.info(f"Exported {len(df)} rows for {group_store} year {year} to {output_file}")

    print(f"\n[COMPLETE] SAMPLE SERVICE COMPLETE: {total_sample_rows} total sample rows extracted.\n")


def run_daily_service(
    years: list[str], sites: list[dict]
) -> None:
    """Run Envista daily data extraction concurrently by year and site."""
    logger = get_logger(__name__)

    _combined_daily_results.clear()

    print("\n" + "=" * 60)
    print("STARTING ENVISTA DAILY SERVICE")
    print("=" * 60)

    total_daily_rows = 0

    with ThreadPoolExecutor(max_workers=ENV_SAMPLE_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(
                _process_daily_year,
                year,
                sites,
                ENV_SAMPLE_SITE_WORKERS,
            )
            for year in years
        ]

        for future in futures:
            total_daily_rows += future.result()

    logger.info(f"Daily service complete: {total_daily_rows} total daily rows extracted.")

    config.ensure_dirs(ENV_DAILY_DIR)
    for (group_store, year), df in _combined_daily_results.items():
        if df.empty:
            logger.warning(f"Skipping {group_store} year {year} daily data: DataFrame is empty")
            continue

        all_na_cols = df.columns[df.isna().all()].tolist()
        if len(all_na_cols) == len(df.columns):
            logger.warning(f"Skipping {group_store} year {year} daily data: All columns contain only NA values")
            continue

        output_file = ENV_DAILY_DIR / f"env_daily_{group_store}_{year}.csv"
        write_csv(df, output_file)
        logger.info(f"Exported {len(df)} daily rows for {group_store} year {year} to {output_file}")

    print(f"\n[COMPLETE] DAILY SERVICE COMPLETE: {total_daily_rows} total daily rows extracted.\n")


def main(argv: list[str] | None = None) -> None:
    """Main entry point for Envista service pipeline."""
    logger = get_logger(__name__)
    requested_group_stores, requested_service = _parse_requested_filters(argv)

    log_level = "DEBUG" if ENV_TEST_MODE else "INFO"
    log_dir = ENV_MONITOR_DIR.parent
    setup_logging(level=log_level, log_file=str(log_dir / "logs" / "envista_service.log"))
    log_pipeline_start("Envista Service Pipeline")
    logger.info("=" * 60)
    logger.info("[START] ENVISTA PIPELINE EXECUTION STARTING")
    logger.info("=" * 60)
    logger.info(f"Requested Envista service: {requested_service}")
    if requested_group_stores:
        logger.info(f"Requested Envista group stores: {requested_group_stores}")
    else:
        logger.info("No Envista group store filter specified; retrieving all configured groups.")
    
    # Extract station data
    monitor_metadata = extract_envista_station_data()
    
    if monitor_metadata is not None:
        logger.info(f"Successfully extracted station data with {len(monitor_metadata)} records")
        logger.debug(f"Columns: {list(monitor_metadata.columns)}")
        
        # Export to CSV
        try:
            config.ensure_dirs(config.RAW_ENV_MONITORS)
            write_csv(monitor_metadata, ENV_MONITOR_DIR / "envista_stations_monitors.csv")
            logger.info(f"Exported station metadata to {ENV_MONITOR_DIR}")
        except Exception as e:
            log_error_with_context(e, "Failed to export Envista station metadata to CSV")
            sys.exit(1)
    else:
        logger.error("Failed to extract Envista station data")
        sys.exit(1)
    
    # Load configured monitor names and group_store mappings from the Envista dimension table
    envista_group_catalog = _load_envista_group_catalog()
    envista_group_catalog["monitor_name_norm"] = (
        envista_group_catalog["monitor_name"].astype(str).str.strip().str.casefold()
    )

    monitor_metadata = monitor_metadata.copy()
    monitor_metadata["monitor_name_norm"] = (
        monitor_metadata["monitor_name"].astype(str).str.strip().str.casefold()
    )

    selected_sites = monitor_metadata.merge(
        envista_group_catalog[["monitor_name_norm", "group_store"]],
        how="inner",
        on="monitor_name_norm",
    )

    if requested_group_stores:
        requested_norm = {value.casefold() for value in requested_group_stores}
        selected_sites = selected_sites[
            selected_sites["group_store"].astype(str).str.casefold().isin(requested_norm)
        ]
        logger.info(f"Applied group_store filter: {requested_group_stores}")

    monitored_sites = (
        selected_sites[['name', 'station_id', 'monitor_name', 'channel_id', 'mon_start_date', 'group_store']]
        .drop_duplicates()
        .to_dict('records')
    )
    logger.info(f"Found {len(monitored_sites)} unique Envista monitor sites from the catalog")

    if not monitored_sites:
        logger.warning(
            "No configured Envista monitor sites found in the catalog for the requested group_store filter"
        )
        log_pipeline_end("Envista Service Pipeline", success=False, reason="no_configured_sites")
        return

    # Generate years list
    start_year = BDATE.year
    end_year = EDATE.year
    years = [str(year) for year in range(start_year, end_year + 1)]

    if ENV_TEST_MODE and len(years) > 2:
        logger.info("TEST MODE: Limiting to 2 years")
        years = years[:2]

    if ENV_TEST_MODE and len(monitored_sites) > 3:
        logger.info("TEST MODE: Limiting to 3 sites")
        monitored_sites = monitored_sites[:3]

    logger.info(f"Processing {len(monitored_sites)} configured sites across {len(years)} years")

    try:
        if requested_service in {"hourly", "both"}:
            run_sample_service(years, monitored_sites)
        if requested_service in {"daily", "both"}:
            run_daily_service(years, monitored_sites)

        logger.info("=" * 60)
        logger.info("[COMPLETE] ENVISTA PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        log_pipeline_end("Envista Service Pipeline", success=True)

    except Exception as e:
        log_error_with_context(e, "Envista Service Pipeline", pipeline_stage="execution")
        logger.error(f"[ERROR] Pipeline execution failed: {e}")
        log_pipeline_end("Envista Service Pipeline", success=False, error=str(e))
        raise


if __name__ == "__main__":
    main()