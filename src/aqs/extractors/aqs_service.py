"""Compatibility wrapper for historical sample extractor API.

This module re-exports the consolidated data extractors from
`soar.aqs.extractors.measurements` to preserve the previous import surface.
"""

from __future__ import annotations

from datetime import date

from aqs.extractors.measurements import (
    fetch_annual_by_state,
    fetch_samples_by_state,
    fetch_samples_for_parameter,
    write_annual_for_parameter,
)


def fetch_samples_dispatch(
    parameter_code: str, bdate: date, edate: date, state_fips: str, session=None
):
    """Dispatch to appropriate sample fetcher based on configuration.

    Returns either a generator yielding (year, df) or a DataFrame
    consistent with the interface.
    """
    import config

    if config.SAMPLE_MODE == "by_state":
        return fetch_samples_by_state(
            parameter_code, bdate, edate, state_fips, session=session
        )
    return fetch_samples_for_parameter(parameter_code, bdate, edate, state_fips)


__all__ = [
    "fetch_samples_dispatch",
    "fetch_samples_by_state",
    "fetch_samples_for_parameter",
    "fetch_annual_by_state",
    "write_annual_for_parameter",
]
