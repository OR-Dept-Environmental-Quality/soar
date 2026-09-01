""" Ventilation Index transformer.

Combines staged hourly mixing height (HRRR) and wind speed (AQS) fact tables into an hourly ventilation index (wind speed x mixing height).

Wind speed source data is in knots' converted to m/s for standard m^2/s ventilation index units"""

from __future__ import annotations

import pandas as pd

import config

_KNOTS_TO_MPS = 0.514444

def _load_mixing_height(year: int):
    path = config.ROOT / "staged" / "fct_mixing_height" / f"fct_mixing_height_{year}.csv"
    return pd.read_csv(path, dtype={"site_code": str})

def _load_wind_speed(year: int) -> pd.DataFrame:
    path = config.ROOT / "staged" / "fct_wind_speed" / f"fct_wind_speed_{year}.csv"
    df = pd.read_csv(path, dtype={"site_code": str})
    return df [["site_code", "date_local", "time_local", "sample_measurement"]].rename(
        columns={"sample_measurement": "wind_speed_knots"}
    )

_VENTILATION_BINS = [0, 235, 2350, 4700, float("inf")]
_VENTILATION_LABELS = ["Very Poor", "Poor", "Marginal", "Good"]

def _classify_ventilation(vi_m2_s: pd.Series) ->pd.Series:
    return pd.cut(vi_m2_s, bins=_VENTILATION_BINS, labels=_VENTILATION_LABELS, right=False)

def calculate_ventilation_index(year: int) -> pd.DataFrame:
    """Join hourly mixing height and wind speed to and compute ventilation index."""
    mixing_height = _load_mixing_height(year)
    wind_speed = _load_wind_speed(year)

    merged = mixing_height.merge(
        wind_speed, on=["site_code", "date_local", "time_local"], how="inner"
    )

    merged["wind_speed_ms"] = merged["wind_speed_knots"] * _KNOTS_TO_MPS
    merged["ventilation_index_m2_s"] = merged["wind_speed_ms"] * merged["mixing_height_m"]
    merged["ventilation_category"] = _classify_ventilation(merged["ventilation_index_m2_s"])

    return merged[[
        "site_code", "date_local", "time_local",
        "mixing_height_m", "wind_speed_ms", "ventilation_index_m2_s", "ventilation_category"
    ]]

def run_transform(start_year: int, end_year: int) -> None:
    out_dir = config.ROOT / "staged" / "fct_ventilation_index"
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in range(start_year, end_year +1):
        result = calculate_ventilation_index(year)
        if result.empty:
            print(f"{year}: no matching mixing height/win speed rows, skipping")
            continue
        out_path = out_dir / f"fct_ventilation_index_{year}.csv"
        result.to_csv(out_path, index=False)
        print(f"{year}: wrote {len(result)} hourly ventilation index rows")

if __name__ == "__main__":
    run_transform(2020,2025)