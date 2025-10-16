"""Transformers for annual toxics data to calculate TRV exceedances.

This module provides pure functions to transform raw annual toxics data
into TRV exceedance calculations. All functions take DataFrames as input
and return transformed DataFrames without side effects.
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


# Unit normalization aliases (same as sample)
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


def transform_toxics_annual_trv(df: pd.DataFrame, dim_pollutant_path: str) -> pd.DataFrame:
    """Transform annual toxics data to include TRV exceedances.

    Args:
        df: Raw annual toxics DataFrame from AQS API.
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

    # Merge mol_weight for conversions
    df = df.merge(dim_trv[["mol_weight_g_mol"]], left_on="parameter_code", right_index=True, how="left")

    # Convert relevant fields to ug/m3 for calculations
    df["arithmetic_mean_ug_m3"] = df.apply(
        lambda row: _convert_to_ug_m3(row["arithmetic_mean"], row["units_of_measurement_norm"], row["mol_weight_g_mol"]),
        axis=1
    )
    df["first_max_value_ug_m3"] = df.apply(
        lambda row: _convert_to_ug_m3(row["first_max_value"], row["units_of_measurement_norm"], row["mol_weight_g_mol"]),
        axis=1
    )
    df["second_max_value_ug_m3"] = df.apply(
        lambda row: _convert_to_ug_m3(row["second_max_value"], row["units_of_measurement_norm"], row["mol_weight_g_mol"]),
        axis=1
    )

    # Merge TRV values
    df = df.merge(dim_trv[["trv_cancer", "trv_noncancer", "trv_acute"]], left_on="parameter_code", right_index=True, how="left")

    # Calculate exceedances
    df["xtrv_cancer"] = df["arithmetic_mean_ug_m3"] / df["trv_cancer"]
    df["xtrv_noncancer"] = df["arithmetic_mean_ug_m3"] / df["trv_noncancer"]
    df["xtrv_acute_first"] = df["first_max_value_ug_m3"] / df["trv_acute"]
    df["xtrv_acute_second"] = df["second_max_value_ug_m3"] / df["trv_acute"]

    # Create site_code
    df["site_code"] = df["county_code"].astype(str) + df["site_number"].astype(str)

    # Select and order output columns
    output_columns = [
        "site_code", "parameter", "sample_duration", "parameter_code", "poc", "method", "year",
        "units_of_measurement", "observation_count", "observation_percent", "validity_indicator",
        "valid_day_count", "required_day_count", "exceptional_data_count", "null_observation_count",
        "primary_exceedance_count", "secondary_exceedance_count", "certification_indicator",
        "arithmetic_mean", "xtrv_cancer", "xtrv_noncancer", "standard_deviation",
        "first_max_value", "xtrv_acute_first", "first_max_datetime",
        "second_max_value", "xtrv_acute_second", "second_max_datetime",
        "third_max_value", "third_max_datetime", "fourth_max_value", "fourth_max_datetime",
        "first_max_nonoverlap_value", "first_max_n_o_datetime", "second_max_nonoverlap_value",
        "second_max_n_o_datetime", "ninety_ninth_percentile", "ninety_eighth_percentile",
        "ninety_fifth_percentile", "ninetieth_percentile", "seventy_fifth_percentile",
        "fiftieth_percentile", "tenth_percentile"
    ]

    # Ensure all columns exist (fill missing with NaN)
    for col in output_columns:
        if col not in df.columns:
            df[col] = math.nan

    return df[output_columns]