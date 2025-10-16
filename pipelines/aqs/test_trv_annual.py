"""Test script for annual toxics TRV transformer.

Loads dimTRV, creates a sample annual toxics DataFrame, applies the transformer,
and prints the result to verify correctness.
"""
from __future__ import annotations
import sys
from pathlib import Path

# Add src to path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
from soar.aqs.transformers.annual_toxics import transform_toxics_annual_trv


def main():
    # Path to dimTRV
    dim_trv_path = ROOT / "ops" / "dimTRV.csv"

    # Create sample annual toxics DataFrame (mock data)
    sample_data = {
        "parameter_code": ["17147", "17242"],  # Acenaphthene, Benzo(a)pyrene
        "parameter": ["Acenaphthene", "Benzo(a)pyrene"],
        "sample_duration": ["24 HOUR", "24 HOUR"],
        "poc": [1, 1],
        "method": ["MODIFIED TIM", "MODIFIED TIM"],
        "year": [2020, 2020],
        "units_of_measurement": ["Micrograms/cubic meter (LC)", "Micrograms/cubic meter (LC)"],
        "county_code": ["041", "041"],
        "site_number": ["0001", "0001"],
        "observation_count": [365, 365],
        "observation_percent": [100.0, 100.0],
        "validity_indicator": ["Y", "Y"],
        "valid_day_count": [365, 365],
        "required_day_count": [365, 365],
        "exceptional_data_count": [0, 0],
        "null_observation_count": [0, 0],
        "primary_exceedance_count": [0, 0],
        "secondary_exceedance_count": [0, 0],
        "certification_indicator": ["Certified", "Certified"],
        "arithmetic_mean": [0.1, 0.05],
        "standard_deviation": [0.01, 0.005],
        "first_max_value": [0.2, 0.1],
        "first_max_datetime": ["2020-01-01", "2020-01-01"],
        "second_max_value": [0.15, 0.08],
        "second_max_datetime": ["2020-01-02", "2020-01-02"],
        "third_max_value": [0.12, 0.06],
        "third_max_datetime": ["2020-01-03", "2020-01-03"],
        "fourth_max_value": [0.1, 0.05],
        "fourth_max_datetime": ["2020-01-04", "2020-01-04"],
        "first_max_nonoverlap_value": [0.18, 0.09],
        "first_max_n_o_datetime": ["2020-01-05", "2020-01-05"],
        "second_max_nonoverlap_value": [0.14, 0.07],
        "second_max_n_o_datetime": ["2020-01-06", "2020-01-06"],
        "ninety_ninth_percentile": [0.19, 0.095],
        "ninety_eighth_percentile": [0.18, 0.09],
        "ninety_fifth_percentile": [0.15, 0.075],
        "ninetieth_percentile": [0.13, 0.065],
        "seventy_fifth_percentile": [0.11, 0.055],
        "fiftieth_percentile": [0.1, 0.05],
        "tenth_percentile": [0.08, 0.04],
    }
    df = pd.DataFrame(sample_data)

    print("Original annual sample data:")
    print(df.head())

    # Apply transformer
    transformed = transform_toxics_annual_trv(df, str(dim_trv_path))

    print("\nTransformed data:")
    print(transformed.head())

    # Check if output columns are present
    expected_columns = [
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
    missing = [col for col in expected_columns if col not in transformed.columns]
    if missing:
        print(f"Missing columns: {missing}")
    else:
        print("All expected columns present.")

    # Save to test output
    output_path = ROOT / "test_trv_annual_output.csv"
    transformed.to_csv(output_path, index=False)
    print(f"Test output saved to {output_path}")


if __name__ == "__main__":
    main()