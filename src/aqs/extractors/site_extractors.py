"""Site and sample extractors that use the shared AQS HTTP client.

This module collects helpers for fetching monitor metadata and sample data
and is intended to replace the older `monitors.py` extractor which mixed
direct `requests` usage and pyaqsapi imports.
"""
from __future__ import annotations

from datetime import date
from typing import Iterable, List, Optional

import pandas as pd

from aqs import _client
import config


def _ensure_dataframe(payload: object) -> Optional[pd.DataFrame]:
    if payload is None:
        return None
    if isinstance(payload, pd.DataFrame):
        return payload if not payload.empty else None
    if hasattr(payload, "get_data"):
        return _ensure_dataframe(payload.get_data())
    if isinstance(payload, list):
        frames = [frame for frame in (_ensure_dataframe(item) for item in payload) if frame is not None and not frame.empty]
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)
    try:
        frame = pd.DataFrame(payload)
    except ValueError:
        return None
    return frame if not frame.empty else None


def fetch_monitors(parameter_codes: Iterable[str], bdate: date, edate: date, state_fips: str) -> pd.DataFrame:
    """Fetch monitor metadata for parameters; uses pyaqsapi for discovery.

    Returns a DataFrame or empty DataFrame when no monitors are found.
    """
    frames: list[pd.DataFrame] = []
    # Lazy import of pyaqsapi to avoid hard dependency in tests
    try:
        from pyaqsapi import bystate
    except Exception:
        raise

    for code in parameter_codes:
        response = bystate.monitors(code, bdate, edate, state_fips, return_header=True)
        frame = _ensure_dataframe(response)
        if frame is None:
            continue
        enriched = frame.copy()
        enriched["seed_parameter_code"] = code
        frames.append(enriched)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def fetch_aqs_response(api_url: str, session: Optional[_client.requests.Session] = None) -> pd.DataFrame:
    """Fetch an AQS URL using the shared client and return a DataFrame payload.

    session can be provided for reuse; otherwise a new one is created.
    """
    sess = session or _client.make_session()
    return _client.fetch_df(sess, api_url)


def build_aqs_requests(state_code: str, county_code: str, site_number: str, parameter_code: str, start_date: date | str, end_date: date | str) -> List[str]:
    """Return a list of AQS sampleData/bySite request URLs split by calendar year."""
    sdate = pd.to_datetime(start_date).date()
    edate = pd.to_datetime(end_date).date()
    years = range(sdate.year, edate.year + 1)

    urls: List[str] = []
    for i, y in enumerate(years):
        if i == 0:
            b = sdate.strftime("%Y%m%d")
        else:
            b = f"{y}0101"
        if i == len(years) - 1:
            e = edate.strftime("%Y%m%d")
        else:
            e = f"{y}1231"

        params = {
            "email": config.AQS_EMAIL or "",
            "key": config.AQS_KEY or "",
            "param": parameter_code,
            "bdate": b,
            "edate": e,
            "state": state_code,
            "county": county_code,
            "site": site_number,
        }
        from urllib.parse import urlencode

        url = f"https://aqs.epa.gov/data/api/sampleData/bySite?{urlencode(params)}"
        urls.append(url)

    return urls


def load_parameters_csv(path: str = "ops/parameters.csv") -> List[str]:
    df = pd.read_csv(path, dtype={"AQS_Parameter": str})
    if "AQS_Parameter" not in df.columns:
        raise KeyError("parameters.csv must contain an 'AQS_Parameter' column")
