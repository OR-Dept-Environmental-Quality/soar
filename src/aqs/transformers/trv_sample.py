"""Transformers for sample toxics data to calculate TRV exceedances.

This module provides pure functions to transform raw toxics sample data
into TRV exceedance calculations. All functions take DataFrames as input
and return transformed DataFrames without side effects.
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


# Unit normalization aliases
UNIT_ALIASES: Dict[str, str] = {
    "micrograms/cubicmeter": "ug/m3",
    "microgrampercubmeter": "ug/m3",
    "microgramsperm3": "ug/m3",
    "µg/m3": "ug/m3",
    "ug/m3": "ug/m3",
    "nanograms/cubicmeter": "ng/m3",
    "nanogramsperm3": "ng/m3",
    "ng/m3": "ng/m3",
    "milligrams/cubicmeter": "mg/m3",
    "mg/m3": "mg/m3",
    "ppb": "ppb",
    "ppm": "ppm",
    "partsperbillion": "ppb",
    "partspermillion": "ppm",
}


def _normalize_unit(unit: str) -> str:
    """Normalize unit string to standard form."""
    if pd.isna(unit):
        return ""
    return UNIT_ALIASES.get(unit.lower().replace(" ", ""), "")


def _convert_to_ug_m3(value: float, unit_norm: str, mol_weight: float) -> float:
    """Convert measurement to µg/m³."""
    if pd.isna(value):
        return math.nan
    if unit_norm == "ug/m3" or unit_norm == "":
        return float(value)
    if unit_norm == "ng/m3":
        return float(value) / 1000.0
    if unit_norm == "mg/m3":
        return float(value) * 1000.0
    if unit_norm == "ppb":
        # Concentration (µg/m3) = molecular weight x concentration (ppb) ÷ 24.45
        return mol_weight * float(value) / 24.45
    # If unknown unit, return NaN
    return math.nan


def transform_toxics_trv(df: pd.DataFrame, dim_pollutant_path: str) -> pd.DataFrame:
    """Transform sample toxics data to include TRV exceedances.

    Args:
        df: Raw sample toxics DataFrame from AQS API.
        dim_pollutant_path: Path to dimPollutant.csv file.

    Returns:
        Transformed DataFrame with TRV and exceedance fields.
    """
    # Load dimPollutant and filter for toxics only
    dim_pollutant = pd.read_csv(dim_pollutant_path, dtype={"aqs_parameter": str})
    dim_trv = dim_pollutant[dim_pollutant["group_store"] == "toxics"]
    dim_trv = dim_trv.set_index("aqs_parameter")[["mol_weight_g_mol", "trv_cancer", "trv_noncancer", "trv_acute"]]

    # Normalize units
    df = df.copy()
    df["parameter_code"] = df["parameter_code"].astype(str)
    df["units_of_measurement_norm"] = df["units_of_measure"].apply(_normalize_unit)

    # Convert sample_measurement to ug/m3
    df = df.merge(dim_trv[["mol_weight_g_mol"]], left_on="parameter_code", right_index=True, how="left")
    df["sample_measurement_ug_m3"] = df.apply(
        lambda row: _convert_to_ug_m3(row["sample_measurement"], row["units_of_measurement_norm"], row["mol_weight_g_mol"]),
        axis=1
    )

    # Merge TRV values
    df = df.merge(dim_trv[["trv_cancer", "trv_noncancer", "trv_acute"]], left_on="parameter_code", right_index=True, how="left")

    # Calculate exceedances
    df["xtrv_cancer"] = df["sample_measurement_ug_m3"] / df["trv_cancer"]
    df["xtrv_noncancer"] = df["sample_measurement_ug_m3"] / df["trv_noncancer"]
    df["xtrv_acute"] = df["sample_measurement_ug_m3"] / df["trv_acute"]

    # Create site_code
    df["site_code"] = df["county_code"].astype(str) + df["site_number"].astype(str)

    # Select and order output columns
    output_columns = [
        "site_code", "parameter_code", "poc", "parameter", "date_local",
        "sample_measurement", "units_of_measure", "sample_measurement_ug_m3",
        "trv_cancer", "trv_noncancer", "trv_acute",
        "xtrv_cancer", "xtrv_noncancer", "xtrv_acute",
        "sample_duration", "sample_frequency", "detection_limit",
        "uncertainty", "qualifier", "method_type", "method", "method_code"
    ]

    # Ensure all columns exist (fill missing with NaN)
    for col in output_columns:
        if col not in df.columns:
            df[col] = math.nan

    return df[output_columns]