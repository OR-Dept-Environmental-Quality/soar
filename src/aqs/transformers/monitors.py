"""Transformers for AQS monitor metadata.

This module provides functions to transform raw monitor metadata
into cleaned, deduplicated monitor records with standardized fields.
"""
from __future__ import annotations

import pandas as pd


def transform_monitors(raw_monitors: pd.DataFrame) -> pd.DataFrame:
    """Transform raw monitor metadata into cleaned monitor records.

    Selects specific fields, removes duplicates by site_code, and ensures
    one record per unique monitor location.

    Args:
        raw_monitors: Raw monitor DataFrame from AQS API

    Returns:
        Transformed DataFrame with selected fields and deduplicated by site_code
    """
    if raw_monitors.empty:
        return pd.DataFrame()

    # Define the fields to keep (in the order requested)
    fields_to_keep = [
        "open_date",
        "close_date",
        "concurred_exclusions",
        "dominant_source",
        "measurement_scale",
        "measurement_scale_def",
        "monitoring_objective",
        "last_method_code",
        "last_method_description",
        "last_method_begin_date",
        "naaqs_primary_monitor",
        "qa_primary_monitor",
        "monitor_type",
        "networks",
        "monitoring_agency_code",
        "monitoring_agency",
        "si_id",
        "latitude",
        "longitude",
        "datum",
        "lat_lon_accuracy",
        "elevation",
        "probe_height",
        "pl_probe_location",
        "local_site_name",
        "address",
        "state_name",
        "county_name",
        "city_name",
        "cbsa_code",
        "cbsa_name",
        "csa_code",
        "csa_name",
        "tribal_code",
        "tribe_name",
        "site_code"
    ]

    # Filter to only the fields that exist in the data
    available_fields = [field for field in fields_to_keep if field in raw_monitors.columns]
    if not available_fields:
        raise ValueError("None of the requested fields are present in the raw monitors data")

    # Select the available fields
    transformed = raw_monitors[available_fields].copy()

    # Remove duplicates by site_code, keeping the first occurrence
    if "site_code" in transformed.columns:
        original_count = len(transformed)
        transformed = transformed.drop_duplicates(subset=["site_code"])
        deduped_count = len(transformed)
        print(f"✅ Deduplicated monitors: {original_count} → {deduped_count} unique sites")
    else:
        print("⚠️  site_code field not found, skipping deduplication")

    return transformed