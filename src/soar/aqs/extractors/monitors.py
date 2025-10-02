"""Extractors for AQS monitor data."""
from __future__ import annotations

from datetime import date
from typing import Iterable, Optional

import pandas as pd
from pyaqsapi import bystate


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
    """Fetch monitor metadata for the provided parameters and time window."""
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

