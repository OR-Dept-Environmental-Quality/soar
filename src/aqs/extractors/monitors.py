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

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import List, Optional
from urllib.parse import urlencode

import pandas as pd
import requests

import config
from aqs import _client


def _add_site_code(df: pd.DataFrame) -> pd.DataFrame:
    """Add site_code column to monitor DataFrame.

    Creates site_code as: state_code (2 digits) + county_code (3 digits) + site_number (4 digits)
    For Oregon data, defaults state_code to 41 if missing/invalid.
    """
    df = df.copy()

    # Handle NaN and non-numeric values robustly
    df["state_code_num"] = pd.to_numeric(df["state_code"], errors="coerce")
    df["county_code_num"] = pd.to_numeric(df["county_code"], errors="coerce")
    df["site_number_num"] = pd.to_numeric(df["site_number"], errors="coerce")

    # Default to Oregon state code (41) if missing or invalid
    df["state_code_num"] = df["state_code_num"].fillna(41).astype(int)
    df["state_code_num"] = df["state_code_num"].where(df["state_code_num"] == 41, 41)

    # Fill missing county/site codes with 0
    df["county_code_num"] = df["county_code_num"].fillna(0).astype(int)
    df["site_number_num"] = df["site_number_num"].fillna(0).astype(int)

    df["site_code"] = (
        df["state_code_num"].astype(str).str.zfill(2)
        + df["county_code_num"].astype(str).str.zfill(3)
        + df["site_number_num"].astype(str).str.zfill(4)
    )

    # Clean up temporary columns
    df = df.drop(columns=["state_code_num", "county_code_num", "site_number_num"])

    return df


def fetch_aqs_response(api_url: str) -> pd.DataFrame:
    """Fetch a raw AQS API URL and return a DataFrame of the data payload.

    The AQS JSON responses are structured as a two-element result where the
    second element is the data array (the R script used `[[2]]`). We mirror that
    behavior here.
    """
    try:
        print(f"Fetching AQS data from: {api_url}")
        resp = requests.get(api_url)
        resp.raise_for_status()
        parsed = resp.json()
        # AQS typically returns [header, data]; if data missing return empty frame

        try:
            # Handle list format: [header, data]
            if isinstance(parsed, list) and len(parsed) > 1:
                data = parsed[1]
            # Handle dict format with 'result' key (common in EPA APIs)
            elif isinstance(parsed, dict) and "result" in parsed:
                data = parsed["result"]
            else:
                data = []
        except (KeyError, IndexError, TypeError) as e:
            print(f"⚠️  Error extracting data from API response: {type(e).__name__}: {e}")
            print(f"    Response type: {type(parsed)}, Response structure: {list(parsed.keys()) if isinstance(parsed, dict) else f'list of length {len(parsed)}' if isinstance(parsed, list) else 'unknown'}")
            return pd.DataFrame()
        
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        print(f"❌ API request failed: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"❌ JSON parsing error: {e}")
        return pd.DataFrame()


def build_aqs_requests(
    parameter_code_list: list[str],
    start_date: date | str,
    end_date: date | str,
) -> List[str]:
    """Return a list of AQS monitor request URLs split by calendar year.

    start_date and end_date may be datetime.date or ISO strings. Returned
    URLs include the configured `config.AQS_EMAIL` and `config.AQS_KEY`.
    """
    # Normalize dates
    sdate = pd.to_datetime(start_date).date()
    edate = pd.to_datetime(end_date).date()
    years = range(sdate.year, edate.year + 1)

    urls: List[str] = []
    for parameter_code in parameter_code_list:
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
                "state": "41"
            }
            url = f"https://aqs.epa.gov/data/api/monitors/byState?{urlencode(params)}"
            urls.append(url)

    return urls


def fetch_all_monitors_for_oregon(bdate: date, edate: date) -> pd.DataFrame:
    """Fetch all unique monitor metadata for Oregon (state 41) from 2005-2025.

    Iterates through each parameter to ensure complete dataset extraction,
    fetches monitors for each parameter across the full date range, concatenates results,
    and deduplicates by site_code to ensure one entry per unique monitor location,
    regardless of parameter coverage.
    """
    # Check circuit breaker
    if _client.circuit_is_open():
        raise RuntimeError("AQS circuit is open; cannot fetch monitors")

    # Hardcoded parameter codes
    parameter_codes = [
        "88101",
        "88502",
        "81102",
        "44201",
        "42101",
        "42602",
        "14129",
        "85129",
        "61101",
        "82128",
        "17141",
        "45201",
        "14115",
        "43502",
        "58103",
        "85102",
    ]

    print(
        f"📋 Processing {len(parameter_codes)} parameters for monitors from {bdate} to {edate}..."
    )
    urls = build_aqs_requests(parameter_codes,bdate, edate)

    # Fetch monitors for each parameter across the full date range (more efficient)
    all_monitors: List[pd.DataFrame] = []

    # Use ThreadPoolExecutor for concurrent fetching
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(fetch_aqs_response, url) for url in urls]
        for future in futures:
            df = future.result()
            if not df.empty:
                all_monitors.append(df)

    # Concatenate and deduplicate by site_code (one entry per monitor location)
    if not all_monitors:
        print("❌ No monitors found for any parameter")
        return pd.DataFrame()

    print("🔄 Concatenating and deduplicating monitor data...")
    combined = pd.concat(all_monitors, ignore_index=True)
    original_count = len(combined)

    # Create site_code efficiently
    combined = _add_site_code(combined)

    # Deduplicate by site_code (one entry per monitor location)
    combined = combined.drop_duplicates(subset=["site_code"])
    deduped_count = len(combined)

    print(
        f"✅ Deduplicated: {original_count} raw entries → {deduped_count} unique monitor locations (by site_code)"
    )
    return combined
