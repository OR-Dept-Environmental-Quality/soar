"""Envista service pipeline for extracting station metadata and measurements.

Retrieves Envista station metadata, builds monitor information tables,
and processes measurement data using the Envista API with concurrent
site-year extraction.
"""

from __future__ import annotations

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
_combined_hourly_results: dict[str, pd.DataFrame] = {}  # Key format: "year"
_combined_daily_results: dict[str, pd.DataFrame] = {}  # Key format: "year"

if BDATE < date(2018, 7, 1): BDATE = date(2018, 7, 1)  # Envista data starts mid-2018


def _process_hourly_site_year(
    station_name: str, station_id: str, channel_name: str, channel_id: str, year: str
) -> tuple[str, str, str, int, bool]:
    """Extract hourly Envista data for one site and one calendar year."""
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    logger = get_logger(__name__)
    logger.debug(
        f"Extracting hourly data for {station_name}:{channel_name}, {station_id}:{channel_id} in {year}"
    )

    try:
        envista_data_hourly = get_envista_hourly(
            station_id=station_id,
            channel_id=channel_id,
            from_date=from_date,
            to_date=to_date,
        )

        if envista_data_hourly is None or envista_data_hourly.empty:
            logger.warning(
                f"No hourly data retrieved for {station_name}:{station_id}, {channel_name}:{channel_id} "
                f"in {year}"
            )
            return station_id, channel_id, year, 0, False

        all_na_cols = envista_data_hourly.columns[envista_data_hourly.isna().all()].tolist()
        if len(all_na_cols) == len(envista_data_hourly.columns):
            logger.warning(
                f"No data for {station_name}:{station_id}, {channel_name}:{channel_id} in {year}."
            )
            return station_id, channel_id, year, 0, False

        hourly_rows = len(envista_data_hourly)
        logger.info(
            f"Retrieved {hourly_rows} hourly records for {station_name}:{station_id}, "
            f"{channel_name}:{channel_id} in {year}."
        )

        with _data_lock:
            if year in _combined_hourly_results:
                _combined_hourly_results[year] = pd.concat(
                    [_combined_hourly_results[year], envista_data_hourly],
                    ignore_index=True,
                )
            else:
                _combined_hourly_results[year] = envista_data_hourly.copy()

        return station_id, channel_id, year, hourly_rows, True

    except Exception as e:
        logger.error(
            f"Error retrieving hourly data for {station_name}:{station_id}, {channel_name}:{channel_id} "
            f"in {year}: {e}",
            exc_info=True,
        )
        return station_id, channel_id, year, 0, False


def _process_daily_site_year(
    station_name: str, station_id: str, channel_name: str, channel_id: str, year: str
) -> tuple[str, str, str, int, bool]:
    """Extract daily averaged Envista data for one site and one calendar year."""
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    logger = get_logger(__name__)
    logger.debug(
        f"Extracting daily data for {station_name}:{channel_name}, {station_id}:{channel_id} in {year}"
    )

    try:
        envista_data_daily = get_envista_daily(
            station_id=station_id,
            channel_id=channel_id,
            from_date=from_date,
            to_date=to_date,
        )

        if envista_data_daily is None or envista_data_daily.empty:
            logger.warning(
                f"No daily data retrieved for {station_name}:{station_id}, {channel_name}:{channel_id} "
                f"in {year}"
            )
            return station_id, channel_id, year, 0, False

        all_na_cols = envista_data_daily.columns[envista_data_daily.isna().all()].tolist()
        if len(all_na_cols) == len(envista_data_daily.columns):
            logger.warning(
                f"No daily data for {station_name}:{station_id}, {channel_name}:{channel_id} in {year}."
            )
            return station_id, channel_id, year, 0, False

        daily_rows = len(envista_data_daily)
        logger.info(
            f"Retrieved {daily_rows} daily records for {station_name}:{station_id}, "
            f"{channel_name}:{channel_id} in {year}."
        )

        with _data_lock:
            if year in _combined_daily_results:
                _combined_daily_results[year] = pd.concat(
                    [_combined_daily_results[year], envista_data_daily],
                    ignore_index=True,
                )
            else:
                _combined_daily_results[year] = envista_data_daily.copy()

        return station_id, channel_id, year, daily_rows, True

    except Exception as e:
        logger.error(
            f"Error retrieving daily data for {station_name}:{station_id}, {channel_name}:{channel_id} "
            f"in {year}: {e}",
            exc_info=True,
        )
        return station_id, channel_id, year, 0, False


def _process_hourly_year(
    year: str, pm25_sites: list[dict], site_workers: int
) -> int:
    """Process all hourly site extractions for a single year."""
    logger = get_logger(__name__)
    logger.info(f"Processing {len(pm25_sites)} sites concurrently for hourly data in {year}")

    year_total_rows = 0

    with ThreadPoolExecutor(max_workers=site_workers) as executor:
        futures = [
            executor.submit(
                _process_hourly_site_year,
                str(site['name']),
                str(site['station_id']),
                str(site['monitor_name']),
                str(site['channel_id']),
                year,
            )
            for site in pm25_sites
        ]

        for future in futures:
            _, _, _, hourly_rows, succeeded = future.result()
            if succeeded:
                year_total_rows += hourly_rows

    logger.info(f"Completed hourly processing for {year}: {year_total_rows} total rows.")
    return year_total_rows


def _process_daily_year(
    year: str, pm25_sites: list[dict], site_workers: int
) -> int:
    """Process all daily site extractions for a single year."""
    logger = get_logger(__name__)
    logger.info(f"Processing {len(pm25_sites)} sites concurrently for daily data in {year}")

    year_total_rows = 0

    with ThreadPoolExecutor(max_workers=site_workers) as executor:
        futures = [
            executor.submit(
                _process_daily_site_year,
                str(site['name']),
                str(site['station_id']),
                str(site['monitor_name']),
                str(site['channel_id']),
                year,
            )
            for site in pm25_sites
        ]

        for future in futures:
            _, _, _, daily_rows, succeeded = future.result()
            if succeeded:
                year_total_rows += daily_rows

    logger.info(f"Completed daily processing for {year}: {year_total_rows} total rows.")
    return year_total_rows


def _process_sample_service(
    years: list[str], pm25_sites: list[dict]
) -> None:
    """Run hourly Envista sample data extraction concurrently by year and site."""
    logger = get_logger(__name__)

    _combined_hourly_results.clear()

    print("\n" + "=" * 60)
    print("STARTING ENVISTA HOURLY SAMPLE SERVICE (PM2.5 SensOR)")
    print("=" * 60)

    total_hourly_rows = 0

    with ThreadPoolExecutor(max_workers=ENV_SAMPLE_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(
                _process_hourly_year,
                year,
                pm25_sites,
                ENV_SAMPLE_SITE_WORKERS,
            )
            for year in years
        ]

        for future in futures:
            total_hourly_rows += future.result()

    logger.info(f"Hourly sample service complete: {total_hourly_rows} total hourly rows extracted.")

    config.ensure_dirs(ENV_SAMPLE_DIR)
    for year, df in _combined_hourly_results.items():
        if df.empty:
            logger.warning(f"Skipping year {year}: DataFrame is empty")
            continue

        all_na_cols = df.columns[df.isna().all()].tolist()
        if len(all_na_cols) == len(df.columns):
            logger.warning(f"Skipping year {year}: All columns contain only NA values")
            continue

        output_file = ENV_SAMPLE_DIR / f"env_hourly_pm25_{year}.csv"
        write_csv(df, output_file)
        logger.info(f"Exported {len(df)} rows for year {year} to {output_file}")

    print(f"\n[COMPLETE] HOURLY SAMPLE SERVICE COMPLETE: {total_hourly_rows} total hourly rows extracted.\n")


def _process_daily_service(
    years: list[str], pm25_sites: list[dict]
) -> None:
    """Run daily Envista sample data extraction concurrently by year and site."""
    logger = get_logger(__name__)

    _combined_daily_results.clear()

    print("\n" + "=" * 60)
    print("STARTING ENVISTA DAILY SAMPLE SERVICE (PM2.5 SensOR)")
    print("=" * 60)

    total_daily_rows = 0

    with ThreadPoolExecutor(max_workers=ENV_SAMPLE_YEAR_WORKERS) as executor:
        futures = [
            executor.submit(
                _process_daily_year,
                year,
                pm25_sites,
                ENV_SAMPLE_SITE_WORKERS,
            )
            for year in years
        ]

        for future in futures:
            total_daily_rows += future.result()

    logger.info(f"Daily sample service complete: {total_daily_rows} total daily rows extracted.")

    config.ensure_dirs(ENV_DAILY_DIR)
    for year, df in _combined_daily_results.items():
        if df.empty:
            logger.warning(f"Skipping year {year} daily data: DataFrame is empty")
            continue

        all_na_cols = df.columns[df.isna().all()].tolist()
        if len(all_na_cols) == len(df.columns):
            logger.warning(f"Skipping year {year} daily data: All columns contain only NA values")
            continue

        output_file = ENV_DAILY_DIR / f"env_daily_pm25_{year}.csv"
        write_csv(df, output_file)
        logger.info(f"Exported {len(df)} daily rows for year {year} to {output_file}")

    print(f"\n[COMPLETE] DAILY SAMPLE SERVICE COMPLETE: {total_daily_rows} total daily rows extracted.\n")


def main() -> None:
    """Main entry point for Envista service pipeline."""
    logger = get_logger(__name__)

    log_level = "DEBUG" if ENV_TEST_MODE else "INFO"
    log_dir = ENV_MONITOR_DIR.parent
    setup_logging(level=log_level, log_file=str(log_dir / "logs" / "envista_service.log"))
    log_pipeline_start("Envista Service Pipeline")
    logger.info("=" * 60)
    logger.info("[START] ENVISTA PIPELINE EXECUTION STARTING")
    logger.info("=" * 60)
    
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
    
    # Extract PM2.5 monitors paired with their station and channel IDs
    pm25_monitors = monitor_metadata[monitor_metadata['monitor_alias'] == "PM2.5 Est SensOR"]
    pm25_sites = pm25_monitors[['name', 'station_id', 'monitor_name', 'channel_id']].drop_duplicates().to_dict('records')
    logger.info(f"Found {len(pm25_sites)} unique PM2.5 SensOR sites")

    if not pm25_sites:
        logger.warning("No PM2.5 SensOR sites found")
        log_pipeline_end("Envista Service Pipeline", success=False, reason="no_pm25_sites")
        return

    # Generate years list
    start_year = BDATE.year
    end_year = EDATE.year
    years = [str(year) for year in range(start_year, end_year + 1)]

    if ENV_TEST_MODE and len(years) > 2:
        logger.info("TEST MODE: Limiting to 2 years")
        years = years[:2]

    if ENV_TEST_MODE and len(pm25_sites) > 3:
        logger.info("TEST MODE: Limiting to 3 sites")
        pm25_sites = pm25_sites[:3]

    logger.info(f"Processing {len(pm25_sites)} sites across {len(years)} years")

    try:
        # Run separate hourly and daily extraction services
        _process_sample_service(years, pm25_sites)
        _process_daily_service(years, pm25_sites)

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