"""Transform monitor data by adding region information via spatial join.

This module reads monitor location data and enriches it with region information
by performing a spatial join with a regions shapefile.
"""

from __future__ import annotations
from pathlib import Path

import geopandas as gpd
import pandas as pd

# Default CRS for all geospatial operations (WGS 84)
DEFAULT_CRS = "EPSG:4326"

def add_monitor_regions(root_path: Path, raw_monitors: pd.DataFrame) -> pd.DataFrame:

    print("Starting monitor region transformation")

    # Path to regions shapefile
    regions_shp_path = root_path / "ops" / "dimRegions.shp"

    # Input validation
    if not regions_shp_path.exists():
        raise FileNotFoundError(f"Regions shapefile not found at {regions_shp_path}")

    # Validate required columns
    required_cols = ["latitude", "longitude"]
    missing_cols = [col for col in required_cols if col not in raw_monitors.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Create GeoDataFrame from monitor coordinates

    graw_monitors_monitors = gpd.GeoDataFrame(
        raw_monitors,
        geometry=gpd.points_from_xy(raw_monitors.longitude, raw_monitors.latitude),
        crs="EPSG:4326",  # WGS 84
    )

    # Read regions shapefile
    print(f"Reading regions from {regions_shp_path}")
    graw_monitors_regions = gpd.read_file(regions_shp_path)

    # Ensure both GeoDataFrames use WGS 84
    if graw_monitors_monitors.crs is None or graw_monitors_monitors.crs != DEFAULT_CRS:
        graw_monitors_monitors.set_crs(DEFAULT_CRS, inplace=True)
        print(f"Set monitor CRS to {DEFAULT_CRS}")
    
    if graw_monitors_regions.crs is None:
        graw_monitors_regions.set_crs(DEFAULT_CRS, inplace=True)
        print(f"Set regions CRS to {DEFAULT_CRS}")
    elif graw_monitors_regions.crs != DEFAULT_CRS:
        print(f"Reprojecting regions from {graw_monitors_regions.crs} to {DEFAULT_CRS}")
        graw_monitors_regions = graw_monitors_regions.to_crs(DEFAULT_CRS)

    # Perform spatial join
    print("Performing spatial join")
    result = gpd.sjoin(
        graw_monitors_monitors,
        graw_monitors_regions[["geometry", "Region"]],  # Only keep needed columns
        how="left",
        predicate="within",
    )

    # Clean up the result
    result = (
        result
        .drop(columns=["geometry", "index_right"])  # Remove spatial columns
        .fillna({"Region": "Unknown"})  # Fill missing regions
    )

    return result