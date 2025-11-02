"""Pandera schemas for monitors transforms."""

from __future__ import annotations

import pandera as pa
from pandera import Check, Column, DataFrameSchema

schema_curated = DataFrameSchema(
    {
        "site_id": Column(pa.String, Check.str_matches(r"^\d{9}$")),
        "state_code": Column(pa.String, Check.str_matches(r"^\d{2}$")),
        "county_code": Column(pa.String, Check.str_matches(r"^\d{3}$")),
        "site_number": Column(pa.String, Check.str_matches(r"^\d{3,4}$")),
        "parameter_code": Column(pa.String),
    }
)

schema_staged = DataFrameSchema(
    {
        "site_id": Column(pa.String, Check.str_matches(r"^\d{9}$")),
        "parameters_measured": Column(list),
    }
)
