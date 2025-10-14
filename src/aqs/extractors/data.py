"""AQS data extractors for sample, annual, and daily endpoints.

This module provides functions to fetch air quality data from EPA's AQS API:
- Sample data: Raw hourly/sub-daily measurements (sampleData/byState endpoint)
- Annual data: Annual statistical aggregates (annualData/byState endpoint)
- Daily data: Daily summary statistics (dailyData/byState endpoint)

All fetch functions preserve API response data exactly as returned - no fields are
added, modified, or removed. Write functions organize outputs by group_store and year.
"""
from __future__ import annotations

from datetime import date
from typing import Optional
import json
import os

import pandas as pd

from urllib.parse import urlencode

from aqs import _client
import config
from pathlib import Path
from loaders.filesystem import append_csv


def _sanitize_filename(name: str, max_len: int = 80) -> str:
    import re

    if not name:
        return "unknown"
    s = re.sub(r"\s+", "-", name)
    s = re.sub(r"[^A-Za-z0-9._-]", "", s)
    s = re.sub(r"[-_]{2,}", "-", s)
    s = re.sub(r"(^[^A-Za-z0-9]+)|([^A-Za-z0-9]+$)", "", s)
    if not s:
        return "unknown"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-_.")
    return s


def fetch_samples_by_state(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None):
    """Fetch sample data from AQS sampleData/byState endpoint, yielding results per year.
    
    Retrieves raw hourly/sub-daily air quality measurements for a single parameter
    across all monitoring sites in a state. Data is fetched year-by-year and yielded
    as (year, DataFrame) tuples for memory-efficient streaming processing.
    
    Args:
        parameter_code: AQS parameter code (e.g., "44201" for Ozone)
        bdate: Begin date (YYYY-MM-DD or datetime.date)
        edate: End date (YYYY-MM-DD or datetime.date)
        state_fips: State FIPS code as 2-digit string (e.g., "41" for Oregon)
        session: Optional requests.Session for connection pooling
    
    Yields:
        Tuple of (year_string, DataFrame) for each year in the date range.
        DataFrame contains all columns returned by AQS API without modification.
    
    API Endpoint:
        https://aqs.epa.gov/data/api/sampleData/byState
    """
    session = session or _client.make_session()
    for b, e in _client.build_year_chunks(bdate, edate):
        params = {
            "email": config.AQS_EMAIL or "",
            "key": config.AQS_KEY or "",
            "param": parameter_code,
            "bdate": b,
            "edate": e,
            "state": state_fips,
        }
        url = f"https://aqs.epa.gov/data/api/sampleData/byState?{urlencode(params)}"
        df = _client.fetch_df(session, url)
        year_token = b[:4]  # Extract year from YYYYMMDD format
        yield year_token, df


def fetch_samples_for_parameter(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None) -> pd.DataFrame:
    """Backward-compatible helper: fetch sample data for a parameter and return a single concatenated DataFrame.

    Historically callers requested a single DataFrame for a parameter (per-site). The consolidated
    `fetch_samples_by_state` yields year-by-year DataFrames for streaming. This helper consumes that
    generator and concatenates non-empty yearly frames into a single DataFrame, preserving all API
    fields without modification.

    Returns an empty DataFrame if no data was returned.
    """
    frames = []
    session = session or _client.make_session()
    for _year, df in fetch_samples_by_state(parameter_code, bdate, edate, state_fips, session=session):
        if df is None:
            continue
        if not df.empty:
            frames.append(df)
    if not frames:
        # Return an empty DataFrame with no columns
        return pd.DataFrame()
    # Concatenate and preserve original column order and types
    return pd.concat(frames, ignore_index=True)



def fetch_annual_by_state(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None):
    """Fetch annual aggregate data from AQS annualData/byState endpoint, yielding results per year.
    
    Retrieves annual statistical summaries (mean, max, percentiles, etc.) for a parameter
    across all monitoring sites in a state. Data is fetched year-by-year.
    
    Args:
        parameter_code: AQS parameter code (e.g., "44201" for Ozone)
        bdate: Begin date
        edate: End date
        state_fips: State FIPS code as 2-digit string
        session: Optional requests.Session for connection pooling
    
    Yields:
        Tuple of (year_string, DataFrame) for each year.
        DataFrame preserves all columns from AQS API response.
    
    API Endpoint:
        https://aqs.epa.gov/data/api/annualData/byState
    """
    session = session or _client.make_session()
    for b, e in _client.build_year_chunks(bdate, edate):
        params = {
            "email": config.AQS_EMAIL or "",
            "key": config.AQS_KEY or "",
            "param": parameter_code,
            "bdate": b,
            "edate": e,
            "state": state_fips,
        }
        url = f"https://aqs.epa.gov/data/api/annualData/byState?{urlencode(params)}"
        df = _client.fetch_df(session, url)
        year_token = b[:4]
        yield year_token, df


def fetch_daily_by_state(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None):
    """Fetch daily summary data from AQS dailyData/byState endpoint, yielding results per year.
    
    Retrieves daily statistical summaries (daily mean, max, AQI values, etc.) for a parameter
    across all monitoring sites in a state. Data is fetched year-by-year.
    
    Args:
        parameter_code: AQS parameter code (e.g., "44201" for Ozone)
        bdate: Begin date
        edate: End date
        state_fips: State FIPS code as 2-digit string
        session: Optional requests.Session for connection pooling
    
    Yields:
        Tuple of (year_string, DataFrame) for each year.
        DataFrame preserves all columns from AQS API response.
    
    API Endpoint:
        https://aqs.epa.gov/data/api/dailyData/byState
    """
    session = session or _client.make_session()
    for b, e in _client.build_year_chunks(bdate, edate):
        params = {
            "email": config.AQS_EMAIL or "",
            "key": config.AQS_KEY or "",
            "param": parameter_code,
            "bdate": b,
            "edate": e,
            "state": state_fips,
        }
        url = f"https://aqs.epa.gov/data/api/dailyData/byState?{urlencode(params)}"
        df = _client.fetch_df(session, url)
        year_token = b[:4]
        yield year_token, df


def write_annual_for_parameter(parameter_code: str, analyte_name: str, bdate: date, edate: date, state_fips: str, session=None, group_store: str = None) -> dict:
    """Fetch and write annual aggregate data for a parameter, organized by group_store and year.
    
    Extracts annual statistical data from AQS API and writes to CSV files organized by
    pollutant group (toxics, pm25, ozone, other) and year. API data is preserved exactly
    as returned without modification.
    
    Args:
        parameter_code: AQS parameter code (e.g., "44201")
        analyte_name: Human-readable parameter name (e.g., "Ozone")
        bdate: Begin date
        edate: End date
        state_fips: State FIPS code
        session: Optional requests.Session for connection pooling
        group_store: Pollutant group category (auto-detected if not provided)
    
    Returns:
        Dictionary with extraction results including row counts per year and status.
        Written to logs/annual_{parameter_code}_{group_store}_{timestamp}.json
    
    Output Files:
        raw/aqs/annual/aqs_annual_{group_store}_{year}.csv
        Multiple parameters with same group_store append to same file.
    """
    session = session or _client.make_session()
    
    # Look up group_store category from dimPollutant.csv if not provided
    if group_store is None:
        from utils import get_parameter_group
        group_store = get_parameter_group(parameter_code)
    
    results = {"parameter": parameter_code, "analyte_name": analyte_name, "group_store": group_store, "years": {}, "status": "ok"}
    logs_dir = Path(config.ROOT) / "raw" / "aqs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Create output directory (files go directly here, no year subdirectories)
        config.RAW_ANNUAL.mkdir(parents=True, exist_ok=True)
        
        # Fetch and write data year by year
        for year_token, df in fetch_annual_by_state(parameter_code, bdate, edate, state_fips, session=session):
            out_path = config.RAW_ANNUAL / f"aqs_annual_{group_store}_{year_token}.csv"
            if df is None or df.empty:
                results["years"][year_token] = {"rows": 0, "path": str(out_path)}
                continue
            # Append to existing file or create new with header
            # API response data written without modification
            append_csv(df, out_path)
            results["years"][year_token] = {"rows": len(df), "path": str(out_path)}
    except Exception as exc:
        results["status"] = "failed"
        results["error"] = str(exc)

    # Write audit log with extraction results
    from loaders.filesystem import atomic_write_json

    ts = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    audit_name = f"annual_{parameter_code}_{group_store}_{ts}.json"
    audit_path = logs_dir / audit_name
    atomic_write_json(audit_path, results)

    return results


def write_daily_for_parameter(parameter_code: str, analyte_name: str, bdate: date, edate: date, state_fips: str, session=None, group_store: str = None) -> dict:
    """Fetch and write daily summary data for a parameter, organized by group_store and year.
    
    Extracts daily statistical summaries (daily mean, max, AQI, etc.) from AQS API and
    writes to CSV files organized by pollutant group and year. API data is preserved
    exactly as returned without modification.
    
    Args:
        parameter_code: AQS parameter code (e.g., "44201")
        analyte_name: Human-readable parameter name (e.g., "Ozone")
        bdate: Begin date
        edate: End date
        state_fips: State FIPS code
        session: Optional requests.Session for connection pooling
        group_store: Pollutant group category (auto-detected if not provided)
    
    Returns:
        Dictionary with extraction results including row counts per year and status.
        Written to logs/daily_{parameter_code}_{group_store}_{timestamp}.json
    
    Output Files:
        raw/aqs/daily/aqs_daily_{group_store}_{year}.csv
        Multiple parameters with same group_store append to same file.
    """
    session = session or _client.make_session()
    
    # Look up group_store category from dimPollutant.csv if not provided
    if group_store is None:
        from utils import get_parameter_group
        group_store = get_parameter_group(parameter_code)
    
    results = {"parameter": parameter_code, "analyte_name": analyte_name, "group_store": group_store, "years": {}, "status": "ok"}
    logs_dir = Path(config.ROOT) / "raw" / "aqs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Create output directory (files go directly here, no year subdirectories)
        config.RAW_DAILY.mkdir(parents=True, exist_ok=True)
        
        for year_token, df in fetch_daily_by_state(parameter_code, bdate, edate, state_fips, session=session):
            out_path = config.RAW_DAILY / f"aqs_daily_{group_store}_{year_token}.csv"
            if df is None or df.empty:
                results["years"][year_token] = {"rows": 0, "path": str(out_path)}
                continue
            append_csv(df, out_path)
            results["years"][year_token] = {"rows": len(df), "path": str(out_path)}
    except Exception as exc:
        results["status"] = "failed"
        results["error"] = str(exc)

    from loaders.filesystem import atomic_write_json

    ts = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    audit_name = f"daily_{parameter_code}_{group_store}_{ts}.json"
    audit_path = logs_dir / audit_name
    atomic_write_json(audit_path, results)

    return results
