""" HRRR mixing height extraction (PBL height) multi-year raw extraction.

Downloads and processes HRRR mixing height data for Oregons monitoring sites across a range of years,
using NOAA's .idx byte-range index to pull only the HPBL:surface field. .idx is a plain-text index NOAA publishes 
with the HRRR GRIB2 files. This allows us to find the byte at which HPBL:surface is stored and return just that byte
length, without unecessary information. 

Each hours field is cropped to Oregon + 100km buffer to reduce memory usage and written as a single 24-band GeoTIFF (band N = hour N-1),
skipping days already downloaded. Used fixed PST (UTC-8) offset for consistency with other data in repository. Not DST aware.

Outout is organized by fixed PST (UTC-8, not DST-aware, to match other data in the repository): one 24-hour GeoTIFF per PST calendar day,
where N holds hour N-1's data. Raw HRRR archives are UTC indexed, so each PST hour is converted to its corresponding UTC hour for download.

Resumable: skips any days that have already been downloaded. If a day is partially downloaded, it will be overwritten with a new file containing all 24 hours.

Output: raw/hrrr_grib/mixing_height_{date}.tiff

NOAA HRRR data is available at https://registry.opendata.aws/noaa-hrrr-pds/ and https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{date}/conus/hrrr.t{hour}z.wrfsfcf00.grib2
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
from rasterio.io import MemoryFile

import config

_MIN_START_YEAR = 2014
_PST_OFFSET = timedelta(hours=8) #fixed offset for PST. Not DST aware, consistent with other data in repository. 
_OREGON_BOUNDS_WGS84 = (-124.6, 42.0, -116.6, 46.3) #west, south, east, north
_CROP_BUFFER_M = 100_000 #crop HRRR grid to Oregon + buffer to reduce memory usage

_HRRR_URL_TEMPLATE = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{date_str}/conus/hrrr.t{hour_str}z.wrfsfcf00.grib2"

def _crop_window(dataset: rasterio.DatasetReader) -> rasterio.windows.Window:
    """Return a rasterio window that crops the dataset to Oregon + buffer, in the dataset's own CRS."""
    west, south, east, north = _OREGON_BOUNDS_WGS84
    corner_lons = [west, east, west, east]
    corner_lats = [south, south, north, north]
    xs, ys = warp_transform("EPSG:4326", dataset.crs, corner_lons, corner_lats)

    min_x, max_x = min(xs) - _CROP_BUFFER_M, max(xs) + _CROP_BUFFER_M
    min_y, max_y = min(ys) - _CROP_BUFFER_M, max(ys) + _CROP_BUFFER_M

    row_start, col_start = dataset.index(min_x, max_y)
    row_stop, col_stop = dataset.index(max_x, min_y)

    row_start = max(0, row_start)
    col_start = max(0, col_start)
    row_stop = min(dataset.height, row_stop)
    col_stop = min(dataset.width, col_stop)

    return rasterio.windows.Window.from_slices((row_start, row_stop), (col_start, col_stop))

def _fetch_hour_array(dt: datetime, search_pattern: str = "HPBL:surface"):
    """Download HPBL:surface field for one hour via byte-range requests, crop it to Oregon + 100km buffer. Returns (cropped_array, 
    transform, crs, dtype) for the cropped array."""
    date_str = dt.strftime("%Y%m%d")
    hour_str = dt.strftime("%H")
    base_url = _HRRR_URL_TEMPLATE.format(date_str=date_str, hour_str=hour_str)
    idx_url = base_url + ".idx"

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

    with MemoryFile(response.content) as memfile:
        with memfile.open() as dataset:
            window = _crop_window(dataset)
            cropped = dataset.read(1, window=window)
            cropped_transform = dataset.window_transform(window)
            crs = dataset.crs
            dtype = dataset.dtypes[0]

    return cropped, cropped_transform, crs, dtype

def run_day(day:datetime, grib_dir: Path, max_workers: int = 8) -> None:
    """Download all 24 hours of HRRR mixing height data for one day and write them as a single 24-band GeoTIFF (band N = hour N-1),
    skipping days already downloaded. Used fixed PST (UTC-8) offset for consistency with other data in repository. Not DST aware."""
    day_str = day.strftime("%Y-%m-%d")
    out_path = grib_dir / f"mixing_height_{day_str}.tiff"
    if out_path.exists():
        return

    grib_dir.mkdir(parents=True, exist_ok=True)
    hour_arrays: dict[int, np.ndarray] = {}
    ref_transform = ref_crs = ref_dtype = None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures ={}
        for h in range(24):
            local_dt = datetime(day.year, day.month, day.day, h) #PST local time to be filled
            utc_dt = local_dt + _PST_OFFSET                      #Actual UTC time to be used for HRRR download
            futures[executor.submit(_fetch_hour_array, utc_dt)] = h #Convert to PST for naming, but use UTC for HRRR download
    
        for future in as_completed(futures):
            hour = futures[future]
            try:
                cropped, transform, crs, dtype = future.result()
                hour_arrays[hour] = cropped
                ref_transform, ref_crs, ref_dtype = transform, crs, dtype
            except Exception as e:
                print(f"{day_str} hour {hour:02d}: error downloading HRRR subset: {e}")

    if not hour_arrays:
        print(f"{day_str}: all hours failed, no file written")
        return

    height, width = next(iter(hour_arrays.values())).shape
    nodata = -9999.0
    profile = {
        "driver": "GTiff",
        "count": 24,
        "dtype": ref_dtype,
        "width": width,
        "height": height,
        "crs": ref_crs,
        "transform": ref_transform,
        "nodata": nodata,
        "compress": "lzw",
    }

    with rasterio.open(out_path, "w", **profile) as dst:
        for hour in range(24):
            band_data = hour_arrays.get(hour, np.full((height, width), nodata, dtype=ref_dtype))
            dst.write(band_data, hour + 1)
            dst.set_band_description(hour + 1, f"hour_{hour:02d}_pst")

    missing = 24 - len(hour_arrays)
    print(f"{day_str}: wrote {out_path} with {missing} missing hours filled with nodata")

def run_years(start_year: int, end_year: int)-> None:
    """Dowload all HRRR mixing height data for a range of years, skipping any days that have already been downloaded."""
    grib_dir = config.ROOT / "raw" / "hrrr_grib"
    current = datetime(start_year, 1,1)
    end = datetime(end_year,12,31)
    while current <= end:
        run_day(current, grib_dir)
        current += timedelta(days=1)

def run_extraction(start_year: int, end_year: int) -> None:
    start_year = max(_MIN_START_YEAR, start_year)
    run_years(start_year, end_year)