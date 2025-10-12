"""Compatibility wrapper for historical sample extractor API.

This module re-exports the new consolidated data extractors from
`soar.aqs.extractors.data` to preserve the previous import surface for
callers while allowing the codebase to move to the consolidated module.
"""
from __future__ import annotations

from datetime import date
from typing import Iterator, Tuple

import pandas as pd

from soar.aqs.extractors.data import (
    fetch_samples_by_state,
    fetch_samples_for_parameter,
    fetch_annual_by_state,
    fetch_samples_for_parameter as fetch_samples_per_site,
    write_annual_for_parameter,
)


def fetch_samples_dispatch(parameter_code: str, bdate: date, edate: date, state_fips: str, session=None):
    """Backward-compatible dispatcher kept for callers.

    Returns either a generator yielding (year, df) (by_state) or a DataFrame
    (per-site) consistent with the older interface.
    """
    from soar import config

    if config.SAMPLE_MODE == "by_state":
        return fetch_samples_by_state(parameter_code, bdate, edate, state_fips, session=session)
    return fetch_samples_for_parameter(parameter_code, bdate, edate, state_fips)


__all__ = [
    "fetch_samples_dispatch",
    "fetch_samples_by_state",
    "fetch_samples_for_parameter",
    "fetch_annual_by_state",
    "write_annual_for_parameter",
]
