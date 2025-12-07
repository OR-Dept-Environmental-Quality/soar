from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from config import ENVISTA_KEY, ENVISTA_URL, ENVISTA_USER

logger = logging.getLogger(__name__)

def get_envista_stations() -> pd.DataFrame:
    """Retrieve all stations from the Envista API.

    Fetches station metadata including monitors, regions, and location data
    from the Envista API using configured credentials.

    Returns:
        DataFrame with station information including monitors and region data.
        Returns empty DataFrame if request fails.

    Raises:
        requests.RequestException: If HTTP request fails
    """
    if not ENVISTA_URL or not ENVISTA_USER or not ENVISTA_KEY:
        raise ValueError("Missing Envista credentials in configuration")

    query = f"{ENVISTA_URL}v1/envista/stations"
    
    try:
        response = requests.get(
            query,
            auth=HTTPBasicAuth(ENVISTA_USER, ENVISTA_KEY),
            timeout=120
        )
        response.raise_for_status()
        stations = response.json()
        
        # Convert to DataFrame
        stations_df = pd.json_normalize(stations)
        
        # Rename address column to census_classifier
        if 'address' in stations_df.columns:
            stations_df = stations_df.rename(columns={'address': 'census_classifier'})
        
        return stations_df
    
    except requests.RequestException as e:
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