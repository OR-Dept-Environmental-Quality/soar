"""Test script for toxics TRV transformer.

Loads dimTRV, creates a sample toxics DataFrame, applies the transformer,
and prints the result to verify correctness.
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
from soar.aqs.transformers.toxics import transform_toxics_trv


def main():
    # Path to dimTRV
    dim_trv_path = ROOT / "ops" / "dimTRV.csv"

    # Create sample toxics DataFrame (mock data)
    sample_data = {
        "parameter_code": ["17147", "17242", "43828"],  # Acenaphthene, Benzo(a)pyrene, Benzene
        "parameter": ["Acenaphthene", "Benzo(a)pyrene", "Benzene"],
        "poc": [1, 1, 1],
        "date_local": ["2020-01-01", "2020-01-01", "2020-01-01"],
        "sample_measurement": [0.1, 0.05, 5.0],  # µg/m³ for first two, ppb for benzene
        "units_of_measure": ["Micrograms/cubic meter (LC)", "Micrograms/cubic meter (LC)", "Parts per billion"],
        "county_code": ["041", "041", "041"],
        "site_number": ["0001", "0001", "0001"],
        "sample_duration": ["24 HOUR", "24 HOUR", "1 HOUR"],
        "sample_frequency": ["EVERY DAY", "EVERY DAY", "CONTINUOUS"],
        "detection_limit": [0.01, 0.01, 0.1],
        "uncertainty": [None, None, None],
        "qualifier": [None, None, None],
        "method_type": ["FEM", "FEM", "INSTRUMENTAL - NON-Integrated"],
        "method": ["MODIFIED TIM", "MODIFIED TIM", "INSTRUMENTAL - Gas Chromatography"],
        "method_code": ["001", "001", "002"],
    }
    df = pd.DataFrame(sample_data)

    print("Original sample data:")
    print(df.head())

    # Apply transformer
    transformed = transform_toxics_trv(df, str(dim_trv_path))

    print("\nTransformed data:")
    print(transformed.head())

    # Check if output columns are present
    expected_columns = [
        "site_code", "parameter_code", "poc", "parameter", "date_local",
        "sample_measurement", "units_of_measure",
        "trv_cancer", "trv_noncancer", "trv_acute",
        "xtrv_cancer", "xtrv_noncancer", "xtrv_acute",
        "sample_duration", "sample_frequency", "detection_limit",
        "uncertainty", "qualifier", "method_type", "method", "method_code"
    ]
    missing = [col for col in expected_columns if col not in transformed.columns]
    if missing:
        print(f"Missing columns: {missing}")
    else:
        print("All expected columns present.")

    # Save to test output
    output_path = ROOT / "test_trv_output_v2.csv"
    transformed.to_csv(output_path, index=False)
    print(f"Test output saved to {output_path}")


if __name__ == "__main__":
    main()