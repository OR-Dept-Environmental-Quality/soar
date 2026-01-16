from __future__ import annotations
from datetime import datetime
import threading

import pandas as pd
import requests

from config import ENV_KEY, ENV_URL, ENV_USER
from logging_config import get_logger
from .. import _env_client

logger = get_logger(__name__)

# Thread-local storage for session management
_session_local = threading.local()


def _get_session() -> requests.Session:
    """Get or create a thread-local Envista API session."""
    session = getattr(_session_local, "session", None)
    if session is None:
        session = _env_client.make_session()
        _session_local.session = session
    return session

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

    if not ENV_URL or not ENV_USER or not ENV_KEY:
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

def get_envista_stations() -> pd.DataFrame:
    """Retrieve all stations from the Envista API.

    Fetches station metadata including monitors, regions, and location data
    from the Envista API using configured credentials. Uses centralized
    _env_client for rate limiting, retries, and circuit breaker.

    Returns:
        DataFrame with station information including monitors and region data.
        Returns empty DataFrame if request fails.
    """
    if not ENV_URL or not ENV_USER or not ENV_KEY:
        raise ValueError("Missing Envista credentials in configuration")

    query = f"{ENV_URL}v1/envista/stations"
    
    try:
        session = _get_session()
        stations = _env_client.fetch_json(session, query)
        
        if not stations:
            logger.warning("No stations retrieved from Envista API")
            return pd.DataFrame()
        
        # Convert to DataFrame
        stations_df = pd.json_normalize(stations)
        
        # Rename address column to census_classifier
        if 'address' in stations_df.columns:
            stations_df = stations_df.rename(columns={'address': 'census_classifier'})
        
        logger.debug(f"Retrieved {len(stations_df)} stations from Envista API")
        return stations_df
    
    except Exception as e:
        logger.error(f"Failed to retrieve Envista stations: {e}")
        return pd.DataFrame()

def build_envista_metadata(envista_stations: pd.DataFrame) -> pd.DataFrame:
    """Build a complete Envista monitor metadata table from station data.

    Expands the monitors list for each station into separate rows, with
    one row per station-monitor combination. Includes all available monitor
    fields from the API response.

    Args:
        envista_stations: DataFrame from get_envista_stations()

    Returns:
        DataFrame with one row per station-monitor combination.
    """
    monitor_rows = []
    
    for idx, station_info in envista_stations.iterrows():
        # Get station-level metadata (excluding monitors column)
        station_meta = station_info.drop('monitors', errors='ignore')
        
        # Get monitors for this station
        monitors = station_info.get('monitors', [])
        if not isinstance(monitors, list):
            monitors = [monitors] if monitors else []
        
        # Create a row for each monitor
        for monitor in monitors:
            # Start with station metadata
            monitor_dict = station_meta.to_dict()
            
            # Add all monitor fields from the API response
            monitor_dict.update({
                'channel_id': monitor.get('channelId', -9999),
                'monitor_name': monitor.get('name', 'none'),
                'monitor_alias': monitor.get('alias', 'none'),
                'monitor_active': monitor.get('active', False),
                'type_id': monitor.get('typeId', -9999),
                'pollutant_id': monitor.get('pollutantId', -9999),
                'units': monitor.get('units', 'none'),
                'unit_id': monitor.get('unitID', -9999),
                'description': monitor.get('description'),
                'map_view': monitor.get('mapView', False),
                'is_index': monitor.get('isIndex', False),
                'pollutant_category': monitor.get('PollutantCategory', -9999),
                'numeric_format': monitor.get('NumericFormat', 'none'),
                'low_range': monitor.get('LowRange'),
                'high_range': monitor.get('HighRange'),
                'state': monitor.get('state', -9999),
                'pct_valid': monitor.get('PctValid'),
                'monitor_title': monitor.get('MonitorTitle', 'none'),
                'mon_start_date': monitor.get('MON_StartDate'),
                'mon_end_date': monitor.get('MON_EndDate'),
            })
            monitor_rows.append(monitor_dict)
    
    monitor_data = pd.DataFrame(monitor_rows)
    
    # Apply column renaming for station fields
    rename_dict = {
        'shortName': 'site',
        'stationsTag': 'stations_tag',
        'stationId': 'station_id',
    }
    monitor_data = monitor_data.rename(columns=rename_dict)
    
    # Lowercase site names
    if 'site' in monitor_data.columns:
        monitor_data['site'] = monitor_data['site'].str.lower()
    
    # Convert all columns to lowercase
    monitor_data.columns = monitor_data.columns.str.lower()
    
    # Flatten any remaining list columns to strings
    for col in monitor_data.columns:
        if monitor_data[col].dtype == 'object':
            monitor_data[col] = monitor_data[col].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
            )
    
    return monitor_data
