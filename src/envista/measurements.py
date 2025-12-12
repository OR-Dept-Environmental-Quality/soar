"""Envista API measurement data extraction module.

Provides functions to retrieve measurement data from the Envista API,
parse responses, and standardize data into a consistent format for
processing by downstream pipelines.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

from config import ENV_KEY, ENV_URL, ENV_USER

logger = logging.getLogger(__name__)

def get_envista_data_by_site(
    site_index: int,
    site_pollutant_meta: dict[str, Any]
) -> pd.DataFrame | None:
    """Retrieve Envista measurement data for a specific site and channel.

    Fetches hourly measurement data from a specific station's channel over
    a specified date range.

    Args:
        site_index: Index of the site in the metadata
        site_pollutant_meta: Dictionary with 'meta' key containing site metadata

    Returns:
        DataFrame with parsed measurements, or None if request fails or no data

    Raises:
        requests.RequestException: If HTTP request fails
    """
    if not ENV_URL or not ENV_USER or not ENV_KEY:
        raise ValueError("Missing Envista credentials in configuration")

    meta = site_pollutant_meta.get('meta', {})
    time_base = 60
    
    channel_id = meta.get('channel_id', [None])[site_index] if isinstance(meta.get('channel_id'), list) else meta.get('channel_id')
    station_id = meta.get('station_id', [None])[site_index] if isinstance(meta.get('station_id'), list) else meta.get('station_id')
    from_date = meta.get('from_date', [None])[site_index] if isinstance(meta.get('from_date'), list) else meta.get('from_date')
    to_date = meta.get('to_date', [None])[site_index] if isinstance(meta.get('to_date'), list) else meta.get('to_date')
    
    # Convert to date strings
    if isinstance(from_date, datetime):
        start_date = from_date.strftime('%Y-%m-%d')
    else:
        start_date = str(from_date)
    
    if isinstance(to_date, datetime):
        end_date = to_date.strftime('%Y-%m-%d')
    else:
        end_date = str(to_date)
    
    query = (
        f"{ENV_URL}v1/envista/stations/{station_id}/data/{channel_id}"
        f"?from={start_date}&to={end_date}&timebase={time_base}"
    )
    
    logger.info(f"Querying Envista: {query}")
    
    try:
        response = requests.get(
            query,
            auth=HTTPBasicAuth(ENV_USER, ENV_KEY),
            timeout=120
        )
        
        if response.status_code in (200, 204):
            if response.status_code == 204 or not response.text:
                logger.warning(f"No content returned (status {response.status_code})")
                return None
        else:
            logger.warning(f"Request failed with status {response.status_code}")
            return None
        
        envista_raw = parse_envista_api_response(response.json())
        
        if envista_raw is None or envista_raw.empty:
            logger.warning("Empty Envista response received")
            return None
        
        # Add metadata columns
        site = meta.get('site', [None])[site_index] if isinstance(meta.get('site'), list) else meta.get('site')
        envista_raw['site'] = site
        envista_raw['method_code'] = meta.get('aqs_method_code', [None])[site_index] if isinstance(meta.get('aqs_method_code'), list) else meta.get('aqs_method_code')
        envista_raw['by_date'] = meta.get('interval', [None])[site_index] if isinstance(meta.get('interval'), list) else meta.get('interval')
        envista_raw['parameter'] = meta.get('envista_name', [None])[site_index] if isinstance(meta.get('envista_name'), list) else meta.get('envista_name')
        envista_raw['units_of_measure'] = meta.get('units_envista', [None])[site_index] if isinstance(meta.get('units_envista'), list) else meta.get('units_envista')
        envista_raw['latitude'] = meta.get('latitude', [None])[site_index] if isinstance(meta.get('latitude'), list) else meta.get('latitude')
        envista_raw['longitude'] = meta.get('longitude', [None])[site_index] if isinstance(meta.get('longitude'), list) else meta.get('longitude')
        
        return envista_raw
    
    except requests.RequestException as e:
        logger.error(f"Request failed for site {station_id}: {e}")
        return None


def parse_envista_api_response(envista_response: dict[str, Any]) -> pd.DataFrame | None:
    """Parse Envista API JSON response into a flat DataFrame.

    Extracts measurement data from the nested JSON structure returned by
    the Envista API and flattens it into a tabular format.

    Args:
        envista_response: JSON response from Envista API

    Returns:
        DataFrame with columns: datetime, channel_id, value, status, valid, description
        Returns None if response has no data

    Raises:
        KeyError: If expected response structure is missing
    """
    data = envista_response.get('data', {})
    
    if not data or not data.get('datetime') or not data.get('channels'):
        logger.warning("No data found in Envista response")
        return None
    
    # Parse channels data
    rows = []
    for i, dt in enumerate(data.get('datetime', [])):
        if i < len(data.get('channels', [])):
            channel = data['channels'][i]
            rows.append({
                'datetime': dt,
                'channel_id': channel.get('id'),
                'value': channel.get('value'),
                'status': channel.get('status'),
                'valid': channel.get('valid'),
                'description': channel.get('description'),
            })
    
    if not rows:
        logger.warning("No rows parsed from Envista response")
        return None
    
    df = pd.DataFrame(rows)
    
    # Parse datetime field - remove timezone info and convert
    df['datetime'] = df['datetime'].str.replace('T', ' ').str.replace(r'-0[78]:00', '', regex=True)
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S', utc=True)
    
    # Convert value to numeric
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    return df


def standardize_envista_data(
    envista_data: pd.DataFrame,
    qualifier_codes: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Standardize Envista measurement data to consistent format.

    Applies time adjustments based on resolution, renames columns, adds
    metadata fields, and maps qualifier codes to standard codes.

    Args:
        envista_data: DataFrame from get_envista_data_by_site()
        qualifier_codes: DataFrame with columns 'envista_qualifier_id' and 'simple_qualifier'.
                        If None, qualifiers are not mapped.

    Returns:
        Standardized DataFrame ready for loading to staging layer
    """
    df = envista_data.copy()
    
    # Apply time offset adjustments based on resolution
    if 'by_date' in df.columns:
        offsets = {
            'five_min': timedelta(minutes=5),
            'hour': timedelta(hours=1),
            'day': timedelta(days=1),
        }
        
        for resolution, offset in offsets.items():
            mask = df['by_date'] == resolution
            df.loc[mask, 'datetime'] = df.loc[mask, 'datetime'] - offset
    
    # Select and rename columns
    df = df[['datetime', 'value', 'status', 'valid', 'parameter', 'method_code',
             'units_of_measure', 'latitude', 'longitude', 'site']].copy()
    
    df.columns = ['datetime', 'sample_measurement', 'qualifier', 'valid',
                  'parameter', 'method_code', 'units_of_measure',
                  'latitude', 'longitude', 'site']
    
    # Add standard columns
    df['simple_qual'] = df['qualifier']
    df['data_source'] = 'envista'
    df['poc'] = -9999
    df['sample_frequency'] = 'hourly'
    
    # Map qualifier codes if provided
    if qualifier_codes is not None and not qualifier_codes.empty:
        qualifier_map = dict(
            zip(qualifier_codes['envista_qualifier_id'],
                qualifier_codes['simple_qualifier'])
        )
        df['simple_qual'] = df['qualifier'].map(qualifier_map).fillna(df['qualifier'])
    
    # Apply type conversions
    df = df.astype({
        'datetime': 'datetime64[ns]',
        'sample_measurement': 'float64',
        'units_of_measure': 'str',
        'qualifier': 'str',
        'simple_qual': 'str',
        'data_source': 'str',
        'method_code': 'float64',
        'method_type': 'str',
        'poc': 'int64',
        'site': 'str',
        'latitude': 'float64',
        'longitude': 'float64',
        'sample_frequency': 'str',
    }, errors='ignore')
    
    # Reorder columns
    final_cols = ['datetime', 'sample_measurement', 'units_of_measure', 'qualifier',
                  'simple_qual', 'data_source', 'method_code', 'parameter',
                  'poc', 'site', 'latitude', 'longitude', 'sample_frequency']
    
    # Only select columns that exist
    final_cols = [col for col in final_cols if col in df.columns]
    
    return df[final_cols]

