"""Envista service pipeline for extracting station metadata and measurements.

Retrieves Envista station metadata, builds monitor information tables,
and processes measurement data using the Envista API.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import after path is set
import config
from envista.monitors import build_envista_metadata, get_envista_stations

logger = logging.getLogger(__name__)


def extract_envista_station_data() -> pd.DataFrame | None:
    """Extract station data from Envista API.

    Retrieves all stations and builds a comprehensive metadata table
    with monitor information for each station-monitor combination.

    Returns:
        DataFrame with station and monitor metadata, or None if extraction fails.
    """
    if not config.ENVISTA_URL or not config.ENVISTA_USER or not config.ENVISTA_KEY:
        logger.error("Missing Envista credentials in configuration")
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting Envista service pipeline")
    
    # Extract station data
    monitor_metadata = extract_envista_station_data()
    
    if monitor_metadata is not None:
        logger.info(f"Successfully extracted station data with {len(monitor_metadata)} records")
        logger.debug(f"Columns: {list(monitor_metadata.columns)}")
        logger.debug(f"\nFirst few rows:\n{monitor_metadata.head()}")
        
        # Export to CSV
        try:
            config.ensure_dirs(config.RAW_ENV_MONITORS)
            csv_path = config.RAW_ENV_MONITORS / "envista_stations_metadata.csv"
            monitor_metadata.to_csv(csv_path, index=False)
            logger.info(f"Exported station metadata to {csv_path}")
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}", exc_info=True)
            sys.exit(1)
    else:
        logger.error("Failed to extract Envista station data")
        sys.exit(1)


if __name__ == "__main__":
    main()