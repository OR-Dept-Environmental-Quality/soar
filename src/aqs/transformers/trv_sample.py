"""Transformers for sample toxics data to calculate TRV exceedances.

This module provides pure functions to transform raw toxics sample data
into TRV exceedance calculations. All functions take DataFrames as input
and return transformed DataFrames without side effects.
"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


# Unit normalization aliases (hardened)
UNIT_ALIASES: Dict[str, str] = {
    "micrograms/cubicmeter": "ug/m3",
    "microgrampercubmeter": "ug/m3",
    "microgramsperm3": "ug/m3",
    "ug/m3": "ug/m3",
    "µg/m3": "ug/m3",
    "ug/m^3": "ug/m3",
    "ug/m³": "ug/m3",
    "µg/m³": "ug/m3",

    "nanograms/cubicmeter": "ng/m3",
    "nanogramscubicmeter(25c)": "ng/m3",
    "nanograms/cubicmeter(25c)": "ng/m3",
    "nanogramscubicmeter(lc)": "ng/m3",
    "nanograms/cubicmeter(lc)": "ng/m3",
    "nanogramsperm3": "ng/m3",
    "ng/m3": "ng/m3",

    "milligrams/cubicmeter": "mg/m3",
    "mg/m3": "mg/m3",

    "ppb": "ppb",
    "ppbv": "ppb",
    "partsperbillion": "ppb",
    "partsperbillioncarbon": "ppbc",
    "partsperbillionvolume": "ppb",

    "ppm": "ppm",
    "ppmv": "ppm",
    "partspermillion": "ppm",
    "partspermillionvolume": "ppm",
}


def _normalize_unit(unit: str) -> str:
    """Normalize unit string to standard form."""
    if pd.isna(unit):
        return ""
    key = str(unit).lower().replace(" ", "").replace(",", "")
    return UNIT_ALIASES.get(key, "")


def _convert_to_ug_m3(value: float, unit_norm: str, mol_weight: float, carbon_atoms: float = None) -> float:
    """Convert measurement to µg/m³. Uses 24.45 L/mol at 25°C, 1 atm for gases."""
    """Convert measurement to µg/m³. Uses 24.45 L/mol at 25°C, 1 atm for gases."""
    if pd.isna(value):
        return math.nan
    v = float(value)

    if unit_norm in ("ug/m3", ""):
        return v
    if unit_norm == "ng/m3":
        return v / 1000.0
    if unit_norm == "mg/m3":
        return v * 1000.0
    if unit_norm == "ppb":
        # µg/m³ = ppb × MW / 24.45
        return (v * mol_weight) / 24.45 if pd.notna(mol_weight) else math.nan
    if unit_norm == "ppbc":
        # µg/m³ = ppbC × (MW / carbon_atoms) × (24.45 / 1000)
        if pd.notna(mol_weight) and pd.notna(carbon_atoms) and carbon_atoms > 0:
            return (v * mol_weight * 24.45) / (carbon_atoms * 1000.0)
        return math.nan
    if unit_norm == "ppm":
        # 1 ppm = 1000 ppb
        return (v * 1000.0 * mol_weight) / 24.45 if pd.notna(mol_weight) else math.nan

    # Unknown unit
    return math.nan


def _safe_div(n, d):
    """Divide with NaN/zero protection (vectorized for pandas Series)."""
    import numpy as np
    return np.where(pd.notna(n) & pd.notna(d) & (d != 0), n / d, np.nan)


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
    dim_trv = dim_pollutant[dim_pollutant["group_store"] == "toxics"].set_index("aqs_parameter")[
        ["mol_weight_g_mol", "carbon_atoms", "trv_cancer", "trv_noncancer", "trv_acute"]
    ]

    # Normalize units
    df = df.copy()
    df["parameter_code"] = df["parameter_code"].astype(str)
    df["units_of_measure_norm"] = df["units_of_measure"].apply(_normalize_unit)

    # Convert sample_measurement to ug/m3
    df = df.merge(
        dim_trv[["mol_weight_g_mol", "carbon_atoms"]],
        left_on="parameter_code",
        right_index=True,
        how="left",
    )
    df["sample_measurement_ug_m3"] = df.apply(
        lambda r: _convert_to_ug_m3(
            r["sample_measurement"],
            r["units_of_measure_norm"],
            r["mol_weight_g_mol"],
            r["carbon_atoms"],
        ),
        axis=1,
    )

    # Merge TRV values
    df = df.merge(
        dim_trv[["trv_cancer", "trv_noncancer", "trv_acute"]],
        left_on="parameter_code",
        right_index=True,
        how="left",
    )

    # Calculate exceedances (safe division)
    df["xtrv_cancer"] = _safe_div(df["sample_measurement_ug_m3"], df["trv_cancer"])
    df["xtrv_noncancer"] = _safe_div(df["sample_measurement_ug_m3"], df["trv_noncancer"])
    df["xtrv_acute"] = _safe_div(df["sample_measurement_ug_m3"], df["trv_acute"])

    # Create site_code: state_code (2 digits) + county_code (3 digits) + site_number (4 digits)
    # Handle NaN values and non-numeric values by converting to numeric first
    df["site_code"] = (
        pd.to_numeric(df["state_code"], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(2) +
        pd.to_numeric(df["county_code"], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(3) +
        pd.to_numeric(df["site_number"], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(4)
    )

    # Add converted concentration field (equal to converted value)
    df["ugm3_converted"] = df["sample_measurement_ug_m3"]

    # Select and order output columns (include converted value for QA)
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
