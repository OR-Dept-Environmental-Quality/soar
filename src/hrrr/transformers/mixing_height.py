""" HRRR Mixing Height Transformer.

Reads the raw, Oregon-cropped, PST-organized 24-hour band GeoTIFF day-files produced by src/hrrr/extractors/pull_raw_mixing_height.py
and computes a per-site mixing height value for each hour via inverse-distance-weighted (IDW) averaging of nearby grid cells, 

Reads only the already downloaded raw files, allows for re-running with different radius_m/power (to tune the IDW averaging)
without re-downloading the raw data.

Raw files are already PST labeled (band N = PST hour N-1) and cropped to Oregon + 100km buffer.

Output: transform/hrrr/mixing_height/mixing_height_{date}.csv, one row per (site_code, date_local, time_local) with mixing_height_m.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import rasterio
import rasterio.windows
from rasterio.warp import transform as warp_transform

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

_RADIUS_M = 50_000
_POWER = 2.0

import config

def _idw_at_site(dataset, band_idx: int, site_x: float, site_y: float, radius_m: float = 50_000, power: float = 2.0) -> float:
    """Inverse-distance-weighted average of every grid cell within radiu_m meters of (site_x, site_y), in the data sets own CRS coordinates."""
    row, col = dataset.index(site_x, site_y)

    res_x = abs(dataset.transform.a)
    res_y = abs(dataset.transform.e)
    cell_radius_row = int(radius_m / res_y) +1
    cell_radius_col = int(radius_m / res_x) +1

    row_start = max(0, row - cell_radius_row)
    row_stop = min(dataset.height, row + cell_radius_row + 1)
    col_start = max(0 ,col - cell_radius_col)
    col_stop = min(dataset.height, col + cell_radius_col + 1)

    window   = rasterio.windows.Window.from_slices((row_start, row_stop), (col_start, col_stop))
    band_window = dataset.read(band_idx, window=window).ravel()

    rows, cols = np.meshgrid(
        np.arange(row_start, row_stop), np.arange(col_start, col_stop), indexing = "ij"
    )
    xs, ys = rasterio.transform.xy(dataset.transform, rows.ravel(), cols.ravel())
    distances = np.sqrt((np.array(xs) - site_x)** 2 + (np.array(ys) - site_y) ** 2)

    mask = distances <= radius_m
    if not mask.any():
        return float(band_window.flat[np.argmin(distances)])

    weights = 1.0 / np.maximum(distances[mask], 1.0)** power
    return float(np.sum(weights*band_window[mask]) / np.sum(weights))

def transform_day(day: datetime, sites: pd.DataFrame, raw_dir: Path, out_dir: Path, radius_m: float = 50_000, power: float = 2.0) -> None:
    """Compute per-site hourly mixing height for one PST day from its raw 24-band GeoTIFF, skipping days whose outputs already exist."""
    day_str = day.strftime("%Y-%m-%d")
    raw_path = raw_dir / f"mixing_height_{day_str}.tiff"
    out_path = out_dir / f"mixing_height_{day_str}.csv"

    if out_path.exists():
        print(f"{day_str}: already exists, skipping")
        return
    if not raw_path.exists():
        print(f"{day_str}: raw file {raw_path} does not exist, skipping")
        return

    day_rows = []
    with rasterio.open(raw_path) as dataset:
        xs, xy = warp_transform(
            "EPSG:4326", dataset.crs, sites["longitude"].tolist(), sites["latitude"].tolist())

        for hour in range(24):
            band_idx = hour + 1
            values = [
                _idw_at_site(dataset, band_idx, x, y, radius_m=radius_m, power=power)
                for x, y in zip(xs, xy)
            ]
            hour_df = sites[["site_code"]].copy()
            hour_df["mixing_height_m"] = values
            hour_df["date_local"] = day_str
            hour_df["time_local"] = f"{hour:02d}:00"
            day_rows.append(hour_df)

    day_df = pd.concat(day_rows, ignore_index=True)
    day_df.to_csv(out_path, index=False)
    print(f"{day_str}: wrote {len(day_df)} rows to {out_path.name}")

def transform_years(start_year: int, end_year: int) -> None:
    """Run the transform for every day from start_year to end_year inclusive, skipping days whose outputs already exist."""
    raw_dir = config.ROOT / "raw" / "hrrr_mixing_height"
    out_dir = config.ROOT / "transform" / "hrrr_mixing_height"
    out_dir.mkdir(parents=True, exist_ok=True)

    sites = pd.read_csv(config.ROOT / "staged" / "dim_sites" / "dim_sites.csv", dtype={"site_code": str})
    sites = sites[["site_code", "latitude", "longitude"]].dropna()

    current = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    while current <= end:
        transform_day(current, sites, raw_dir, out_dir, radius_m = _RADIUS_M, power=_POWER)
        current += timedelta(days=1)
        