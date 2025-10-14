"""Consolidated data extractors for AQS endpoints (sampleData, annualData).

Provides consistent generator-style fetchers that yield (year_token, DataFrame)
for per-year endpoints and small writer helpers used by pipelines.
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
        year_token = b[:4]
        if df is None or df.empty:
            yield year_token, pd.DataFrame()
        else:
            df["parameter"] = parameter_code
            yield year_token, df


def fetch_samples_for_parameter(parameter_code: str, bdate: date, edate: date, state_fips: str) -> pd.DataFrame:
    # Lazy pyaqsapi import
    try:
        from pyaqsapi import bystate
    except Exception:
        raise

    monitors = bystate.monitors(parameter_code, bdate, edate, state_fips, return_header=True)
    if hasattr(monitors, "get_data"):
        md = monitors.get_data()
    else:
        md = monitors
    try:
        monitors_df = pd.DataFrame(md)
    except Exception:
        monitors_df = pd.DataFrame()

    if monitors_df.empty:
        return pd.DataFrame()

    session = _client.make_session()
    frames = []
    unique_sites = monitors_df[["state_code", "county_code", "site_number"]].drop_duplicates()
    for _, row in unique_sites.iterrows():
        state = str(row["state_code"]).zfill(2)
        county = str(row["county_code"]).zfill(3)
        site = str(row["site_number"]).zfill(3)
        for b, e in _client.build_year_chunks(bdate, edate):
            params = {
                "email": config.AQS_EMAIL or "",
                "key": config.AQS_KEY or "",
                "param": parameter_code,
                "bdate": b,
                "edate": e,
                "state": state,
                "county": county,
                "site": site,
            }
            url = f"https://aqs.epa.gov/data/api/sampleData/bySite?{urlencode(params)}"
            df = _client.fetch_df(session, url)
            if df is None or df.empty:
                continue
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["parameter"] = parameter_code
    return out


def fetch_annual_by_state(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None):
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
    """Fetch daily summary data by state for a parameter.
    
    Yields (year_token, DataFrame) tuples for each year in the date range.
    Uses the AQS dailyData/byState endpoint.
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
    """Write annual data for a parameter to group-based CSV files.
    
    Files are written as: raw/aqs/annual/{year}/aqs_annual_{group_store}_{year}.csv
    where group_store comes from dimPollutant.csv (toxics, pm25, ozone, etc.)
    """
    session = session or _client.make_session()
    
    # Use provided group_store or look it up
    if group_store is None:
        from utils import get_parameter_group
        group_store = get_parameter_group(parameter_code)
    
    results = {"parameter": parameter_code, "analyte_name": analyte_name, "group_store": group_store, "years": {}, "status": "ok"}
    logs_dir = Path(config.ROOT) / "raw" / "aqs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    try:
        for year_token, df in fetch_annual_by_state(parameter_code, bdate, edate, state_fips, session=session):
            year_dir = config.RAW_ANNUAL / year_token
            year_dir.mkdir(parents=True, exist_ok=True)
            out_path = year_dir / f"aqs_annual_{group_store}_{year_token}.csv"
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
    audit_name = f"annual_{parameter_code}_{group_store}_{ts}.json"
    audit_path = logs_dir / audit_name
    atomic_write_json(audit_path, results)

    return results


def write_daily_for_parameter(parameter_code: str, analyte_name: str, bdate: date, edate: date, state_fips: str, session=None, group_store: str = None) -> dict:
    """Write daily summary data for a parameter to group-based CSV files.
    
    Files are written as: raw/aqs/daily/{year}/aqs_daily_{group_store}_{year}.csv
    where group_store comes from dimPollutant.csv (toxics, pm25, ozone, etc.)
    """
    session = session or _client.make_session()
    
    # Use provided group_store or look it up
    if group_store is None:
        from utils import get_parameter_group
        group_store = get_parameter_group(parameter_code)
    
    results = {"parameter": parameter_code, "analyte_name": analyte_name, "group_store": group_store, "years": {}, "status": "ok"}
    logs_dir = Path(config.ROOT) / "raw" / "aqs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    try:
        for year_token, df in fetch_daily_by_state(parameter_code, bdate, edate, state_fips, session=session):
            year_dir = config.RAW_DAILY / year_token
            year_dir.mkdir(parents=True, exist_ok=True)
            out_path = year_dir / f"aqs_daily_{group_store}_{year_token}.csv"
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
