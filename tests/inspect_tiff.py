import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import config
import rasterio

path = config.ROOT / "raw" / "hrrr_grib" / "mixing_height_2023-01-01.tiff"
with rasterio.open(path) as ds:
    print(f"File: {path}")
    print(f"Size: {ds.width} x {ds.height} pixels, {ds.count} bands")
    print(f"Shape: {ds.shape}")
    print(f"CRS: {ds.crs}")
    print(f"Bounds: {ds.bounds}")
    print(f"Transform: {ds.transform}")

    for band in range(1, ds.count + 1):
        data = ds.read(band)
        desc = ds.descriptions[band - 1]
        valid = data[data != ds.nodata]
        if valid.size:
            print(f"Band {band:2d} ({desc}): min={valid.min():.2f}, max={valid.max():.2f}, mean={valid.mean():.2f}, std={valid.std():.2f}")
        else:
            print(f"Band {band:2d} ({desc}): no valid data (all nodata)")