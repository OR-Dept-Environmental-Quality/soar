"""Transformations for monitor datasets."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

_SITE_METADATA_COLUMNS = [
    "local_site_name",
    "latitude",
    "longitude",
    "datum",
    "elevation",
    "address",
    "state_name",
    "county_name",
    "city_name",
]


def _first_non_null(values: pd.Series) -> Any:
    """Return the first non-null value in a series, if any."""
    non_null = values.dropna()
    return non_null.iloc[0] if not non_null.empty else None


def _unique_sorted(values: Iterable[Any]) -> list[str]:
    """Return a sorted list of unique stringified values."""
    unique = {str(value) for value in values if pd.notna(value)}
    return sorted(unique)


def add_site_id(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive a zero-padded site identifier (state+county+site)."""
    scoped = frame.copy()
    scoped["state_code"] = scoped["state_code"].astype(str).str.zfill(2)
    scoped["county_code"] = scoped["county_code"].astype(str).str.zfill(3)
    scoped["site_number"] = scoped["site_number"].astype(str).str.zfill(4)
    scoped["site_id"] = scoped["state_code"] + scoped["county_code"] + scoped["site_number"]
    return scoped


def to_curated(frame: pd.DataFrame) -> pd.DataFrame:
    """Standardize raw monitor data into the curated layer schema."""
    if "site_id" not in frame.columns:
        return add_site_id(frame)
    return frame.copy()


def to_staged(curated: pd.DataFrame) -> pd.DataFrame:
    """Aggregate curated records down to one row per site with metadata."""
    if curated.empty:
        columns = ["site_id", "parameters_measured", *_SITE_METADATA_COLUMNS]
        return pd.DataFrame(columns=columns)
    grouped = curated.groupby("site_id", dropna=False)
    metadata_columns = [col for col in _SITE_METADATA_COLUMNS if col in curated.columns]
    if metadata_columns:
        metadata = grouped[metadata_columns].agg(_first_non_null)
    else:
        metadata = pd.DataFrame(index=grouped.size().index)
    params = grouped["parameter_code"].apply(_unique_sorted)
    staged = metadata.copy()
    staged["parameters_measured"] = params
    return staged.reset_index()
