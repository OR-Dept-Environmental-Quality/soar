"""Tests for monitor transformers."""

from __future__ import annotations

import pandas as pd

from aqs.transformers.monitors import transform_monitors


def _build_sample_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state_code": [41, 41],
            "county_code": [5, 5],
            "site_number": [1, 1],
            "parameter_code": ["88101", "42101"],
            "local_site_name": ["Portland Harbor", "Portland Harbor"],
            "latitude": [45.1, 45.1],
            "longitude": [-122.1, -122.1],
            "datum": ["NAD83", "NAD83"],
            "elevation": [120.0, 120.0],
            "address": ["123 Main St", "123 Main St"],
            "state_name": ["Oregon", "Oregon"],
            "county_name": ["Multnomah", "Multnomah"],
            "city_name": ["Portland", "Portland"],
            "open_date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-01")],
            "site_code": ["410050001", "410050001"],  # Add site_code field
        }
    )


def test_transform_monitors_selects_fields_and_deduplicates() -> None:
    frame = _build_sample_frame()
    transformed = transform_monitors(frame)

    # Should have deduplicated from 2 rows to 1 row (same site_code)
    assert len(transformed) == 1

    # Should contain expected fields
    expected_fields = [
        "open_date",
        "local_site_name",
        "latitude",
        "longitude",
        "datum",
        "elevation",
        "address",
        "state_name",
        "county_name",
        "city_name",
        "site_code",
    ]

    for field in expected_fields:
        assert field in transformed.columns


def test_transform_monitors_handles_empty_dataframe() -> None:
    empty_frame = pd.DataFrame()
    result = transform_monitors(empty_frame)
    assert result.empty
