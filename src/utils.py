"""Utility functions for parameter metadata and group_store categorization.

Provides functions to load and query parameter metadata from ops/dimPollutant.csv,
which maps AQS parameter codes to pollutant groups (toxics, pm25, ozone, other).
This mapping is used to organize output files by pollutant category.
"""

from __future__ import annotations

from typing import Dict

import pandas as pd


def load_parameter_groups(csv_path: str = "ops/dimPollutant.csv") -> Dict[str, str]:
    """Load parameter code to group_store mapping from pollutant dimension table.

    Reads ops/dimPollutant.csv and creates a mapping from AQS parameter codes to
    their group_store category. This is used to organize output files by pollutant type.

    Args:
        csv_path: Path to dimPollutant.csv file (default: "ops/dimPollutant.csv")

    Returns:
        Dictionary mapping parameter code (string) to group_store value.
        Example: {"44201": "ozone", "45201": "toxics", "88101": "pm25"}

    Note:
        Parameters missing either aqs_parameter or group_store columns are excluded.
    """
    df = pd.read_csv(csv_path, dtype=str)

    # Filter to rows that have both aqs_parameter code and group_store category
    df = df[df["aqs_parameter"].notna() & df["group_store"].notna()]

    # Create mapping: aqs_parameter (code) -> group_store (category)
    mapping = df.set_index("aqs_parameter")["group_store"].to_dict()

    return mapping


def get_parameter_group(
    parameter_code: str, csv_path: str = "ops/dimPollutant.csv"
) -> str:
    """Look up the group_store category for an AQS parameter code.

    Queries the parameter-to-group mapping to determine which pollutant category
    a parameter belongs to. Used to determine output file naming.

    Args:
        parameter_code: AQS parameter code (e.g., "44201" for Ozone)
        csv_path: Path to dimPollutant.csv file (default: "ops/dimPollutant.csv")

    Returns:
        Group store category string: "toxics", "pm25", "ozone", "other", or "unknown"
        if the parameter is not found in the dimension table.

    Example:
        >>> get_parameter_group("44201")
        'ozone'
        >>> get_parameter_group("45201")
        'toxics'
    """
    mapping = load_parameter_groups(csv_path)
    return mapping.get(str(parameter_code), "unknown")


def get_toxics_parameters(csv_path: str = "ops/dimPollutant.csv") -> Dict[str, str]:
    """Get all parameters where group_store equals 'toxics'.

    Loads the pollutant dimension table and returns a dictionary of parameter codes
    and names for all toxics pollutants. Used to identify which parameters need
    qualifier data extraction.

    Args:
        csv_path: Path to dimPollutant.csv file (default: "ops/dimPollutant.csv")

    Returns:
        Dictionary mapping parameter code to parameter name for toxics pollutants.
        Example: {"45201": "Benzene", "43301": "1,3-Butadiene", ...}

    Note:
        Only includes parameters where group_store = "toxics" and both
        aqs_parameter and parameter_name columns are populated.
    """
    df = pd.read_csv(csv_path, dtype=str)

    # Filter to toxics parameters with valid codes and names
    toxics_df = df[
        (df["group_store"] == "toxics") &
        df["aqs_parameter"].notna() &
        df["analyte_name_deq"].notna()
    ]

    # Create mapping: aqs_parameter (code) -> analyte_name_deq
    mapping = toxics_df.set_index("aqs_parameter")["analyte_name_deq"].to_dict()

    return mapping
