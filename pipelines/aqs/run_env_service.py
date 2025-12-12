"""Envista service pipeline for extracting station metadata and measurements.

Retrieves Envista station metadata, builds monitor information tables,
and processes measurement data using the Envista API.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import after path is set
import config

from logging_config import (
     setup_logging, get_logger, log_pipeline_start, 
     log_error_with_context, log_data_processing, log_pipeline_end)
from loaders.filesystem import write_csv
from envista.monitors import build_envista_metadata, get_envista_stations

TEST_MODE = os.getenv("ENV_TEST_MODE", "").strip().lower() in {"1", "true", "yes", "on"}
ENV_MONITOR_DIR = config.RAW_ENV_MONITORS
ENV_SAMPLE_DIR = config.RAW_ENV_SAMPLE

def extract_envista_station_data() -> pd.DataFrame | None:
    """Extract station data from Envista API.

    Retrieves all stations and builds a comprehensive metadata table
    with monitor information for each station-monitor combination.

    Returns:
        DataFrame with station and monitor metadata, or None if extraction fails.
    """
    logger = get_logger(__name__)
    logger.info("Starting Envista station data extraction")
    logger.info(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not config.ENV_URL or not config.ENV_USER or not config.ENV_KEY:
        logger.info("Missing Envista credentials in configuration")
        return None

    try:
        logger.info("Fetching Envista station data...")
        stations_df = get_envista_stations()
        
        if stations_df.empty:
            logger.warning("No stations retrieved from Envista API")
            return None
        
        logger.info(f"Retrieved {len(stations_df)} stations from Envista")
        
        # Build metadata table with monitor information
        logger.info("Building Envista monitor metadata table...")
        monitor_metadata = build_envista_metadata(stations_df)
        
        if monitor_metadata.empty:
            logger.warning("No monitor metadata generated")
            return None
        
        logger.info(f"Generated metadata for {len(monitor_metadata)} station-monitor combinations")
        
        return monitor_metadata
    
    except Exception as e:
        logger.error(f"Error extracting Envista station data: {e}", exc_info=True)
        return None


def main() -> None:
    """Main entry point for Envista service pipeline."""
    logger = get_logger(__name__)

    log_level = "DEBUG" if TEST_MODE else "INFO"
    log_dir = ENV_MONITOR_DIR.parent
    setup_logging(level=log_level, log_file=str(log_dir / "envista_service.log"))
    log_pipeline_start("Envista Service Pipeline") 
    
    # Extract station data
    monitor_metadata = extract_envista_station_data()
    
    if monitor_metadata is not None:
        logger.info(f"Successfully extracted station data with {len(monitor_metadata)} records")
        logger.debug(f"Columns: {list(monitor_metadata.columns)}")
        logger.debug(f"\nFirst few rows:\n{monitor_metadata.head()}")
        
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
    log_pipeline_end("Envista Service Pipeline", success=True)


if __name__ == "__main__":
    main()