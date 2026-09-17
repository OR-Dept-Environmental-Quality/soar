import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import config
import pandas as pd
import rasterio
from rasterio.warp import transform as warp_transform
from hrrr.transformers.mixing_height import _idw_at_site

sites = pd.read_csv(config.ROOT / "staged" / "dim_sites" / "dim_sites.csv", dtype={"site_code": str})
sites = sites[["site_code", "latitude", "longitude"]].dropna()
sites = sites.iloc[0]
print(f"Testing IDW at site: {sites['site_code']} ({sites['latitude']}, {sites['longitude']})")

raw_path = config.ROOT /"raw" / "hrrr_grib" / "mixing_height_2023-01-01.tiff"
with rasterio.open(raw_path) as ds:
    x, y = warp_transform("EPSG:4326", ds.crs, [sites["longitude"]], [sites["latitude"]])
    x, y = x[0], y[0]
    row, col = ds.index(x,y)
    print(f"Projected x={x:.1f}, y={y:.1f} -> row={row}, col={col} (grid is {ds.height} rows x {ds.width} cols)")
    print(f"In bounds: {0 <= row < ds.height and 0 <= col < ds.width}")

    for hour, band_idx in [(0,1), (13,14)]:
        exact_pixel = ds.read(band_idx)[row,col]
        idw_value = _idw_at_site(ds, band_idx, x, y)
        band_mean = ds.read(band_idx).mean()
        print(f"hour={hour:02d} band={band_idx}: exact_pixel={exact_pixel:.1f} idw={idw_value:.1f} whole_grid_mean={band_mean:.1f}")