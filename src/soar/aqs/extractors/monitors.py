"""Extractors for AQS monitor data.

This module contains the existing metadata extractor (`fetch_monitors`) plus
additional, sample-first helpers to fetch AQS `sampleData/bySite` results.

Design notes:
- The repository prefers lazy registration of AQS credentials via
  `src/soar/config.set_aqs_credentials()`. Sample/live HTTP helpers below will
  read `config.AQS_EMAIL` and `config.AQS_KEY` when building API URLs.
- The sample-data helpers use the same per-year splitting logic as the R
  reference implementation (one request per calendar year chunk).
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, Optional, List

import json
from urllib.parse import urlencode

import pandas as pd
import requests

from pyaqsapi import bystate
from soar import config


def _ensure_dataframe(payload: object) -> Optional[pd.DataFrame]:
    """Normalize pyaqsapi responses into a pandas DataFrame."""
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
    except ValueError:  # pragma: no cover - fall back when payload is not coercible
        return None
    return frame if not frame.empty else None


def fetch_monitors(parameter_codes: Iterable[str], bdate: date, edate: date, state_fips: str) -> pd.DataFrame:
    """Fetch monitor metadata for the provided parameters and time window.

    This is the existing metadata fetcher (keeps using `pyaqsapi.bystate.monitors`).
    """
    frames: list[pd.DataFrame] = []
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


def fetch_aqs_response(api_url: str) -> pd.DataFrame:
    """Fetch a raw AQS API URL and return a DataFrame of the data payload.

    The AQS JSON responses are structured as a two-element result where the
    second element is the data array (the R script used `[[2]]`). We mirror that
    behavior here.
    """
    resp = requests.get(api_url)
    resp.raise_for_status()
    parsed = resp.json()
    # AQS typically returns [header, data]; if data missing return empty frame
    data = parsed[1] if isinstance(parsed, list) and len(parsed) > 1 else []
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def build_aqs_requests(state_code: str, county_code: str, site_number: str, parameter_code: str, start_date: date | str, end_date: date | str) -> List[str]:
    """Return a list of AQS sampleData/bySite request URLs split by calendar year.

    start_date and end_date may be datetime.date or ISO strings. Returned
    URLs include the configured `config.AQS_EMAIL` and `config.AQS_KEY`.
    """
    # Normalize dates
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
        url = f"https://aqs.epa.gov/data/api/sampleData/bySite?{urlencode(params)}"
        urls.append(url)

    return urls


def fetch_samples_for_parameter(parameter_code: str, bdate: date, edate: date, state_fips: str) -> pd.DataFrame:
    """Fetch sample-level data for all sites that report the given parameter.

    Flow:
    1. Use `fetch_monitors` to obtain monitor metadata (sites) for the parameter.
    2. For each unique (state, county, site) build per-year URLs and fetch sample data.
    3. Concatenate results and add a `parameter` column with the provided code.

    This function is sample-first and uses the AQS REST endpoint (same as the
    R example). It is safe to call without credentials in tests because the HTTP
    calls are mockable. For large parameter lists consider streaming writes to
    disk instead of keeping all data in memory.
    """
    monitors = fetch_monitors([parameter_code], bdate, edate, state_fips)
    if monitors.empty:
        return pd.DataFrame()

    samples_frames: List[pd.DataFrame] = []
    # Expect monitors to contain state_code, county_code, site_number columns
    unique_sites = monitors[["state_code", "county_code", "site_number"]].drop_duplicates()
    for _, row in unique_sites.iterrows():
        state = str(row["state_code"]).zfill(2)
        county = str(row["county_code"]).zfill(3)
        site = str(row["site_number"]).zfill(3)
        urls = build_aqs_requests(state, county, site, parameter_code, bdate, edate)
        site_frames: List[pd.DataFrame] = []
        for url in urls:
            df = fetch_aqs_response(url)
            if df.empty:
                continue
            site_frames.append(df)
        if not site_frames:
            continue
        site_data = pd.concat(site_frames, ignore_index=True)
        site_data["parameter"] = parameter_code
        # keep a lightweight site id column similar to other transforms
        site_data["site_number"] = site
        samples_frames.append(site_data)

    if not samples_frames:
        return pd.DataFrame()
    return pd.concat(samples_frames, ignore_index=True)


def load_parameters_csv(path: str = "ops/parameters.csv") -> List[str]:
    """Load the `AQS_Parameter` column from the parameters CSV as strings.

    Returns a list of parameter codes suitable for looping in the pipeline.
    """
    df = pd.read_csv(path, dtype={"AQS_Parameter": str})
    if "AQS_Parameter" not in df.columns:
        raise KeyError("parameters.csv must contain an 'AQS_Parameter' column")
    return df["AQS_Parameter"].dropna().astype(str).tolist()

