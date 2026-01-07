"""Envista API measurement data extraction module.

Provides functions to retrieve measurement data from the Envista API,
parse responses, and standardize data into a consistent format for
processing by downstream pipelines.
"""

from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import Any

import pandas as pd
import requests

from config import ENV_KEY, ENV_URL, ENV_USER
from logging_config import get_logger
from . import _env_client

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

def get_envista_hourly(station_id: str, channel_id: str, from_date: str, to_date: str) -> pd.DataFrame | None:
    """Retrieve Envista measurement data for a specific site and channel.

    Fetches hourly measurement data from a specific station's channel over
    a specified date range. Uses centralized _env_client for rate limiting,
    retries, and circuit breaker.

    Args:
        station_id: Envista station ID
        channel_id: Envista channel ID
        from_date: Start date in ISO format (e.g., '2022-01-01')
        to_date: End date in ISO format (e.g., '2022-12-31')

    Returns:
        DataFrame with parsed measurements, or None if request fails or no data
    """
    if not ENV_URL or not ENV_USER or not ENV_KEY:
        raise ValueError("Missing Envista credentials in configuration")
    
    query = (
        f"{ENV_URL}v1/envista/stations/{station_id}/data/{channel_id}"
        f"?from={from_date}&to={to_date}&timebase=60&timeBeginning=True"
    )
    
    logger.debug(f"Fetching Envista data: station={station_id}, channel={channel_id}, "
                 f"from={from_date}, to={to_date}")
    
    try:
        session = _get_session()
        response = _env_client.fetch_json(session, query)
        
        if response is None:
            logger.warning(f"No content returned for station={station_id}, channel={channel_id}")
            return None
        
        # Convert response to DataFrame
        if isinstance(response, list):
            env_sample_df = pd.DataFrame(response)
        elif isinstance(response, dict):
            env_sample_df = pd.json_normalize(response)
        else:
            logger.warning(f"Unexpected response type: {type(response)}")
            return None
        
        if env_sample_df is None or env_sample_df.empty:
            logger.debug(f"Empty data for station={station_id}, channel={channel_id}")
            return None

        # Fully unnest the DataFrame to handle nested structures
        env_sample_df = _fully_unnest_dataframe(env_sample_df)
        
        # Validate: Skip if all values are NA for any column
        if env_sample_df['data_channels_value'].isna().all():
            logger.warning(
                f"Skipping data for station={station_id}, channel={channel_id}: "
                f"All values in 'data_channels_values' are NA"
            )
            return None
        
        logger.debug(f"Retrieved {len(env_sample_df)} records, shape={env_sample_df.shape}")
        return env_sample_df
    
    except Exception as e:
        logger.error(f"Error retrieving Envista data for station={station_id}, "
                     f"channel={channel_id}: {e}")
        return None

def get_envista_daily(station_id: str, channel_id: str, from_date: str, to_date: str) -> pd.DataFrame | None:
    """Retrieve Envista averaged data for a specific site and channel.

    Fetches daily averaged data from a specific station's channel over
    a specified date range. Uses centralized _env_client for rate limiting,
    retries, and circuit breaker.

    Args:
        station_id: Envista station ID
        channel_id: Envista channel ID
        from_date: Start date in ISO format (e.g., '2022-01-01')
        to_date: End date in ISO format (e.g., '2022-12-31')

    Returns:
        DataFrame with parsed averaged data, or None if request fails or no data
    """
    if not ENV_URL or not ENV_USER or not ENV_KEY:
        raise ValueError("Missing Envista credentials in configuration")
    
    query = (
        f"{ENV_URL}v1/envista/stations/{station_id}/Average"
        f"?from={from_date}&to={to_date}&timeBeginning=True"
        f"&fromTimebase=60&toTimebase=1440&percentValid=75&filterChannels={channel_id}"
    )
    
    logger.debug(f"Fetching Envista daily averaged data: station={station_id}, channel={channel_id}, "
                 f"from={from_date}, to={to_date}")
    
    try:
        session = _get_session()
        response = _env_client.fetch_json(session, query)
        
        if response is None:
            logger.warning(f"No content returned for station={station_id}, channel={channel_id}")
            return None
        
        # Convert response to DataFrame
        if isinstance(response, list):
            env_daily_df = pd.DataFrame(response)
        elif isinstance(response, dict):
            env_daily_df = pd.json_normalize(response)
        else:
            logger.warning(f"Unexpected response type: {type(response)}")
            return None
        
        if env_daily_df is None or env_daily_df.empty:
            logger.debug(f"Empty data for station={station_id}, channel={channel_id}")
            return None
        
        # Append station_id
        env_daily_df['stationId'] = station_id

        # Fully unnest the DataFrame to handle nested structures
        env_daily_df = _fully_unnest_dataframe(env_daily_df)
        
        # Validate: Skip if all values are NA for any column
        if env_daily_df['data_channels_value'].isna().all():
            logger.warning(
                f"Skipping daily averaged data for station={station_id}, channel={channel_id}: "
                f"All values in 'data_channels_values' are NA"
            )
            return None
        
        logger.debug(f"Retrieved {len(env_daily_df)} records, shape={env_daily_df.shape}")
        return env_daily_df
    
    except Exception as e:
        logger.error(f"Error retrieving Envista daily averaged data for station={station_id}, "
                     f"channel={channel_id}: {e}")
        return None

def _fully_unnest_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Fully unnest a DataFrame with nested lists and dictionaries.
    
    Recursively expands nested structures until all columns contain
    scalar values only.
    
    Args:
        df: DataFrame potentially containing nested lists/dicts
        
    Returns:
        Fully unnested DataFrame with only scalar values
    """
    # Identify columns that contain lists or dicts
    nested_cols = []
    for col in df.columns:
        sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
        if isinstance(sample_val, (list, dict)):
            nested_cols.append(col)
    
    if not nested_cols:
        # No nested structures, return as-is
        return df
    
    # Handle each nested column
    result_df = df.copy()
    
    for col in nested_cols:
        if result_df[col].dtype == 'object':
            # Check the type of nested data
            sample = result_df[col].dropna().iloc[0] if len(result_df[col].dropna()) > 0 else None
            
            if isinstance(sample, list):
                # Explode lists into separate rows
                result_df = result_df.explode(col, ignore_index=False)
            
            elif isinstance(sample, dict):
                # Flatten dictionaries into separate columns
                nested_data = result_df[col].apply(
                    lambda x: pd.Series(x) if isinstance(x, dict) else pd.Series()
                )
                # Rename nested columns with parent column prefix
                nested_data.columns = [f"{col}_{subcol}" for subcol in nested_data.columns]
                # Drop original column and concatenate flattened data
                result_df = result_df.drop(columns=[col])
                result_df = pd.concat([result_df, nested_data], axis=1)
    
    # Recursively unnest if there are still nested structures
    return _fully_unnest_dataframe(result_df)