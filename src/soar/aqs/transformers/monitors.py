"""Minimal monitor transformers used by the test-suite.

This module provides small, well-documented helpers that mirror the
transformations expected by the tests: create a zero-padded `site_id`,
reduce monitor rows to a curated shape, and produce a staged table with
one row per site listing sorted parameter codes.

The implementations are intentionally small and deterministic so they are
easy to test and safe to re-use in pipelines until a fuller transform
implementation is introduced.
"""
from __future__ import annotations

from typing import List

import pandas as pd


def add_site_id(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `frame` with a new `site_id` column.

    `site_id` is the concatenation of zero-padded state (2), county (3),
    and site (3) codes as strings (e.g. state=41, county=5, site=1 -> "410050001").
    """
    df = frame.copy()
    # normalize and zero-pad components to match repository conventions
    df["state_code"] = df["state_code"].astype(str).str.zfill(2)
    df["county_code"] = df["county_code"].astype(str).str.zfill(3)
    # site_number is padded to 4 for a 9-digit site_id (2+3+4)
    df["site_number"] = df["site_number"].astype(str).str.zfill(4)
    df["site_id"] = df["state_code"] + df["county_code"] + df["site_number"]
    return df


def to_curated(frame: pd.DataFrame) -> pd.DataFrame:
    """Produce a curated DataFrame with a small set of columns used downstream.

    The returned frame contains these columns (in order):
    - site_id
    - state_code
    - county_code
    - site_number
    - parameter_code
    """
    df = add_site_id(frame)
    curated = df[["site_id", "state_code", "county_code", "site_number", "parameter_code"]].copy()
    # ensure parameter codes are strings
    curated["parameter_code"] = curated["parameter_code"].astype(str)
    return curated


def to_staged(curated: pd.DataFrame) -> pd.DataFrame:
    """Aggregate curated rows into one staged row per site.

    The `parameters_measured` column is a list of unique parameter codes
    sorted in ascending numeric order where possible.
    """
    if curated.empty:
        return pd.DataFrame(columns=["site_id", "parameters_measured"])

    def _sorted_unique(xs: List[str]) -> List[str]:
        # sort numerically when values are digits, otherwise lexicographically
        try:
            return [str(x) for x in sorted({int(x) for x in xs})]
        except Exception:
            return sorted(set(xs))

    grouped = curated.groupby("site_id")["parameter_code"].apply(list).reset_index()
    grouped["parameters_measured"] = grouped["parameter_code"].apply(_sorted_unique)
    staged = grouped[["site_id", "parameters_measured"]].copy().reset_index(drop=True)
    return staged
