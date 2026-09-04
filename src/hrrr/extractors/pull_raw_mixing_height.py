""" HRRR mixing height extraction (PBL height) multi-year raw extraction.

Downloads and processes HRRR mixing height data for Oregons monitoring sites across a range of years,
using NOAA's .idx byte-range index to pull only the HPBL:surface field. .idx is a plain-text index NOAA publishes 
with the HRRR GRIB2 files. This allows us to find the byte at which HPBL:surface is stored and return just that byte
length, without unecessary information. 

Each site's value is a 50km inverse distance-weighted average of nearby grid cells, since the output is used the generalize air quality around
a given site. 

Resumable: Skips any day that has already been extracted Downloads within a day concurrently 
to cut overall run-time. This data extraction is very slow.

Output: transform/hrr_mixing_height/mixing_height_{date}.csv

#NOAA High-Resolution Rapid Refresh (HRRR) Model was accessed on {DATE} from https://registry.opendata.aws/noaa-hrrr-pds.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
import rasterio
import rasterio.windows
from rasterio.warp import transform as warp_transform

import config

_MIN_START_YEAR = 2014

_HRRR_URL_TEMPLATE = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{date_str}/conus/hrrr.t{hour_str}z.wrfsfcf00.grib2"

def _download_hrrr_subset(dt: datetime, out_dir: Path, search_pattern: str = "HPBL:surface")-> tuple[Path,int]:
    """Download only the HPBL:surface field from an HRRR file, using NOAA's .idx byte-range index instead of full grid file"""
    date_str = dt.strftime("%Y%m%d")
    hour_str = dt.strftime("%H")
    base_url = _HRRR_URL_TEMPLATE.format(date_str=date_str, hour_str=hour_str)
    idx_url = base_url + ".idx"

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"hrrr_{date_str}_{hour_str}_hpbl.grib2"

    if out_path.exists():
        return out_path, out_path.stat().st_size

    idx_response = requests.get(idx_url, timeout=60)
    idx_response.raise_for_status()
    lines = idx_response.text.strip().split("\n")

    start_byte = None
    end_byte = None 
    for i, line in enumerate(lines): 
        if search_pattern in line:
            start_byte = int(line.split(":")[1])
            if i + 1 < len(lines):
                end_byte = int(lines[i + 1].split(":")[1])-1
            break
    if start_byte is None:
        raise ValueError(f"Could not find '{search_pattern}' in index files")

    range_header = {"Range": f"bytes={start_byte}-{end_byte}" if end_byte else f"bytes={start_byte}-"}
    response = requests.get(base_url, headers=range_header, timeout=300)
    response.raise_for_status()
    out_path.write_bytes(response.content)

    return out_path, len(response.content)

def _find_hpbl_band(dataset: rasterio.DatasetReader)-> int:
    """ Find the band index for PLanetary Boundary Layer Height (HPBL) by inspecting each band's GRIB metadata tags."""
    for band_idx in range(1, dataset.count + 1):
        tags = dataset.tags(band_idx)
        short_name = tags.get("GRIB_SHORT_NAME", "")
        element = tags.get("GRIB_ELEMENT", "")
        if "HPBL" in element or "HPBL" in short_name:
            return band_idx
    raise ValueError("Could not find HPBL band in GRIB file")

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

def extract_hour(dt: datetime, sites: pd.DataFrame, grib_dir: Path, keep_raw: bool = False, radius_m: float = 50_000, power: float = 2.0) -> tuple[pd.DataFrame, int]:
    """ Extract mixing height for all sites for a single hour."""
    grib_path, n_bytes = _download_hrrr_subset(dt, grib_dir)

    try:
        with rasterio.open(grib_path) as dataset:
            band_idx = _find_hpbl_band(dataset)
            xs, ys, = warp_transform(
                "EPSG:4326", dataset.crs, sites["longitude"].tolist(), sites["latitude"].tolist()
            )
            values = [_idw_at_site(dataset, band_idx, x, y, radius_m=radius_m, power=power) for x, y in zip(xs, ys)]
    finally:
        if not keep_raw:
            grib_path.unlink(missing_ok=True)

    result = sites[["site_code"]].copy()
    result["mixing_height_m"] = values
    result["date_local"] = dt.strftime("%Y-%m-%d")
    result["time_local"] = dt.strftime("%H:00")
    return result, n_bytes

def run_day(day:datetime, sites: pd.DataFrame, grib_dir: Path, out_dir: Path, keep_raw: bool = False, max_workers: int = 8) -> None:
    """Extract all 24 hours for one day concurrently and write one CSV, skipping days that are already extracted."""
    day_str = day.strftime("%Y-%m-%d")
    out_path = out_dir / f"mixing_height_{day_str}.csv"
    if out_path.exists():
        return

    day_rows = []
    day_bytes =0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures ={
            executor.submit(
                extract_hour, day.replace(hour=h), sites, grib_dir, keep_raw=keep_raw
            ): h
            for h in range(24)
        }
        for future in as_completed(futures):
            hour = futures[future]
            try:
                result, n_bytes = future.result()
                day_rows.append(result)
                day_bytes += n_bytes
            except Exception as e :
                print (f" {day_str} hour {hour:02d}: FAILED ({e})")

    if day_rows:
        day_df = pd.concat(day_rows, ignore_index=True).sort_values("time_local")
        day_df.to_csv(out_path, index=False)
        print(f" {day_str}: wrote {len(day_df)} rows, {day_bytes:,} bytes")
    else:
        print(f"{day_str}: all hours failed, no file written")

def run_years(start_year: int, end_year: int, sites: pd.DataFrame, keep_raw: bool = False)-> None:
    """Run every day from start_year through end_year (inclusive)."""
    grib_dir = config.ROOT / "raw" / "hrrr_grib"
    out_dir = config.ROOT / "transform" / "hrrr_mixing_height"
    out_dir.mkdir(parents=True, exist_ok=True)

    current = datetime(start_year, 1,1)
    end = datetime(end_year,12,31)
    while current <= end:
        run_day(current, sites, grib_dir, out_dir, keep_raw=keep_raw)
        current += timedelta(days=1)

def run_extraction(start_year: int, end_year: int, keep_raw: bool = False) -> None:
    start_year = max(_MIN_START_YEAR, start_year)
    sites = pd.read_csv(
        config.ROOT / "staged" / "dim_sites" / "dim_sites.csv", dtype={"site_code": str}
    )
    sites = sites[["site_code", "latitude", "longitude"]].dropna()
    run_years(start_year, end_year, sites, keep_raw=keep_raw)