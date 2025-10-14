"""Helpers for loading parameter metadata from ops/dimPollutant.csv."""
from __future__ import annotations

from pathlib import Path
from typing import Dict
import pandas as pd


def load_parameter_groups(csv_path: str = "ops/dimPollutant.csv") -> Dict[str, str]:
    """Load parameter code -> group_store mapping from dimPollutant.csv.
    
    Returns a dict mapping aqs_parameter (as string) to group_store value.
    Parameters without aqs_parameter or group_store are excluded.
    """
    df = pd.read_csv(csv_path, dtype=str)
    
    # Filter rows with both aqs_parameter and group_store
    df = df[df["aqs_parameter"].notna() & df["group_store"].notna()]
    
    # Create mapping: aqs_parameter -> group_store
    mapping = df.set_index("aqs_parameter")["group_store"].to_dict()
    
    return mapping


def get_parameter_group(parameter_code: str, csv_path: str = "ops/dimPollutant.csv") -> str:
    """Get the group_store value for a parameter code.
    
    Returns the group_store (toxics, pm25, ozone, other, etc.) or 'unknown' if not found.
    """
    mapping = load_parameter_groups(csv_path)
    return mapping.get(str(parameter_code), "unknown")
