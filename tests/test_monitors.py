"""Tests for monitor transformers."""
from __future__ import annotations

import pandas as pd
import pandera as pa

from soar.aqs.transformers.monitors import add_site_id, to_curated, to_staged


CURATED_SCHEMA = pa.DataFrameSchema(
    {
        "site_id": pa.Column(str, nullable=False),
        "state_code": pa.Column(str, nullable=False),
        "county_code": pa.Column(str, nullable=False),
        "site_number": pa.Column(str, nullable=False),
        "parameter_code": pa.Column(str, nullable=False),
    }
)

STAGED_SCHEMA = pa.DataFrameSchema(
    {
        "site_id": pa.Column(str, nullable=False),
        "parameters_measured": pa.Column(object, nullable=False),
    }
)


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
        }
    )


def test_add_site_id_zero_pads_and_concatenates() -> None:
    frame = _build_sample_frame()
    augmented = add_site_id(frame)
    assert augmented.loc[0, "site_id"] == "410050001"
    assert augmented.loc[1, "site_id"] == "410050001"


def test_to_staged_returns_one_row_per_site_with_sorted_parameters() -> None:
    frame = _build_sample_frame()
    curated = to_curated(frame)
    CURATED_SCHEMA.validate(curated)

    staged = to_staged(curated)
    STAGED_SCHEMA.validate(staged)

    assert len(staged) == 1
    assert staged.loc[0, "parameters_measured"] == ["42101", "88101"]
