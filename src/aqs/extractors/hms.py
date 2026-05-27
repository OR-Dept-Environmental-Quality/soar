"""NOAA HMS smoke polygon extractor for Oregon monitoring sites.

Downloads HMS KML smoke polygon files from NOAA NESDIS for each date in the
wildfire season, performs a spatial point-in-polygon test for each monitoring
site, and writes a per-day smoke level record to staged/hms/hms_smoke_{year}.csv.

Output schema:
  site_code   (str)  — monitoring site identifier
  date_local  (str)  — ISO date (YYYY-MM-DD)
  smoke_level (str)  — "Light", "Medium", or "Heavy" (highest level wins)

Only smoke-affected site-dates are written; an absent row means no HMS smoke
was detected at that site on that date.

Year-level caching: if hms_smoke_{year}.csv already exists the year is skipped.
This allows incremental updates without re-downloading prior seasons.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

import config

logger = logging.getLogger(__name__)

# ── CONSTANTS ──────────────────────────────────────────────────────────────────

_HMS_KML_URL = (
    "https://satepsanone.nesdis.noaa.gov/pub/FIRE/web/HMS/Smoke_Polygons/KML"
    "/{year}/{month:02d}/hms_smoke{date_str}.kml"
)

# Map KML layer names (lowercased) to canonical labels.
# NOAA HMS KML files use "Smoke (Light)", "Smoke (Medium)", "Smoke (Heavy)".
# Legacy aliases ("smoke (low)", "smoke (high)") are kept for older archives.
_LAYER_MAP: dict[str, str] = {
    "light": "Light",
    "smoke (light)": "Light",
    "smoke (low)": "Light",
    "medium": "Medium",
    "smoke (medium)": "Medium",
    "heavy": "Heavy",
    "smoke (heavy)": "Heavy",
    "smoke (high)": "Heavy",
}

# Numeric priority for "highest level wins" logic
_SMOKE_PRIORITY: dict[str, int] = {"Light": 1, "Medium": 2, "Heavy": 3}
_PRIORITY_TO_LEVEL: dict[int, str] = {v: k for k, v in _SMOKE_PRIORITY.items()}

# Wildfire season bounds (month * 100 + day)
_SEASON_START_MMDD = 601   # June 1
_SEASON_END_MMDD = 1025    # October 25
_SEASON_EXCLUDE_MMDD: frozenset[int] = frozenset({704})  # July 4

# HTTP request timeout (seconds)
_REQUEST_TIMEOUT = 30

# ── OREGON BOUNDARY (module-level cache) ───────────────────────────────────────
_oregon_boundary: gpd.GeoDataFrame | None = None


def _get_oregon_boundary() -> gpd.GeoDataFrame:
    """Load and cache the dissolved Oregon state boundary in WGS84 (EPSG:4326)."""
    global _oregon_boundary
    if _oregon_boundary is None:
        shp_path = ROOT / config.REGIONS_SHP
        gdf = gpd.read_file(shp_path)
        _oregon_boundary = gdf.dissolve()[["geometry"]].to_crs(4326)
    return _oregon_boundary


# ── DATE HELPERS ───────────────────────────────────────────────────────────────

def _in_wildfire_season(d: date) -> bool:
    """Return True if *d* falls within the Oregon wildfire monitoring season."""
    mmdd = d.month * 100 + d.day
    return (
        _SEASON_START_MMDD <= mmdd <= _SEASON_END_MMDD
        and mmdd not in _SEASON_EXCLUDE_MMDD
    )


def _season_dates(year: int):
    """Yield all wildfire-season dates for *year* (Jun 1 – Oct 25, excl. Jul 4)."""
    current = date(year, 6, 1)
    end = date(year, 10, 25)
    while current <= end:
        if _in_wildfire_season(current):
            yield current
        current += timedelta(days=1)


# ── KML FETCH & PARSE ──────────────────────────────────────────────────────────

def _fetch_hms_kml(d: date) -> dict[str, gpd.GeoDataFrame]:
    """Download and parse an HMS KML file for date *d*.

    Returns a dict mapping canonical smoke level (e.g. "Light") to a GeoDataFrame
    of polygons for that level in WGS84.  Returns an empty dict when the file is
    unavailable or cannot be parsed.
    """
    url = _HMS_KML_URL.format(
        year=d.year,
        month=d.month,
        date_str=d.strftime("%Y%m%d"),
    )

    try:
        resp = requests.get(url, timeout=_REQUEST_TIMEOUT)
        if resp.status_code == 404:
            logger.debug("HMS KML not available for %s (HTTP 404)", d.isoformat())
            return {}
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("HMS download failed for %s: %s", d.isoformat(), exc)
        return {}

    with tempfile.NamedTemporaryFile(suffix=".kml", delete=False) as tmp:
        tmp.write(resp.content)
        tmp_path = tmp.name

    try:
        import pyogrio

        layers = pyogrio.list_layers(tmp_path)
        # list_layers returns an ndarray of [name, geometry_type] rows
        layer_names = [row[0] for row in layers]
        result: dict[str, gpd.GeoDataFrame] = {}
        for layer in layer_names:
            level = _LAYER_MAP.get(layer.lower().strip())
            if level is None:
                continue
            try:
                gdf = gpd.read_file(tmp_path, layer=layer)
            except Exception as exc:
                logger.debug("Skipping layer %r for %s: %s", layer, d.isoformat(), exc)
                continue
            if not gdf.empty:
                result[level] = gdf.to_crs(4326)
        return result

    except Exception as exc:
        logger.warning("HMS parse failed for %s: %s", d.isoformat(), exc)
        return {}
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ── SPATIAL JOIN ───────────────────────────────────────────────────────────────

def _point_in_smoke(
    sites_gdf: gpd.GeoDataFrame,
    smoke_layers: dict[str, gpd.GeoDataFrame],
) -> dict[str, str]:
    """Return a mapping of site_code → highest smoke_level.

    Iterates smoke layers in ascending priority order (Light → Medium → Heavy)
    so that a later (higher-priority) assignment overwrites a prior one.
    """
    site_levels: dict[str, int] = {}  # site_code → numeric priority

    for level in ("Light", "Medium", "Heavy"):
        if level not in smoke_layers:
            continue
        smoke_gdf = smoke_layers[level]
        try:
            joined = gpd.sjoin(
                sites_gdf,
                smoke_gdf[["geometry"]].reset_index(drop=True),
                how="inner",
                predicate="within",
            )
        except Exception as exc:
            logger.debug("sjoin failed for level %s: %s", level, exc)
            continue

        priority = _SMOKE_PRIORITY[level]
        for site_code in joined["site_code"]:
            # Overwrite only if this level is strictly higher priority
            if site_levels.get(site_code, 0) < priority:
                site_levels[site_code] = priority

    return {code: _PRIORITY_TO_LEVEL[p] for code, p in site_levels.items()}


# ── PUBLIC API ─────────────────────────────────────────────────────────────────

def stage_hms_year(year: int, staged_dir: Path) -> None:
    """Download and stage HMS smoke data for one wildfire season.

    Args:
        year:       Calendar year to stage (e.g. 2023).
        staged_dir: Root of the staged data lake (config.ROOT / "staged").

    Writes:
        staged_dir/hms/hms_smoke_{year}.csv
        Columns: site_code, date_local, smoke_level
        Only smoke-affected site-dates are written.

    Raises:
        FileNotFoundError: if dim_sites/dim_sites.csv does not exist.
    """
    output_dir = staged_dir / "hms"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"hms_smoke_{year}.csv"

    if output_path.exists():
        print(f"   ⏩ hms_smoke_{year}.csv already exists, skipping")
        return

    print(f"   📡 Fetching HMS smoke data for wildfire season {year}...")

    # Load monitoring site coordinates
    sites_path = staged_dir / "dim_sites" / "dim_sites.csv"
    if not sites_path.exists():
        raise FileNotFoundError(
            f"dim_sites not found at {sites_path}. "
            "Run the monitors pipeline first."
        )

    sites_df = pd.read_csv(sites_path, dtype={"site_code": str})
    sites_df = sites_df[["site_code", "latitude", "longitude"]].dropna(
        subset=["latitude", "longitude"]
    )
    sites_gdf = gpd.GeoDataFrame(
        sites_df,
        geometry=gpd.points_from_xy(sites_df["longitude"], sites_df["latitude"]),
        crs=4326,
    )

    # Clip sites to Oregon boundary (removes any out-of-state sites)
    oregon = _get_oregon_boundary()
    sites_gdf = gpd.clip(sites_gdf, oregon).reset_index(drop=True)

    if sites_gdf.empty:
        logger.warning("No Oregon sites found in dim_sites for year %s", year)
        pd.DataFrame(columns=["site_code", "date_local", "smoke_level"]).to_csv(
            output_path, index=False
        )
        return

    print(f"   🗺️  {len(sites_gdf)} Oregon monitoring sites loaded")

    rows: list[dict] = []
    dates = list(_season_dates(year))

    for i, d in enumerate(dates, 1):
        smoke_layers = _fetch_hms_kml(d)
        if not smoke_layers:
            continue

        # Fast bounding-box pre-filter to Oregon extent before sjoin.
        # Using cx[] is O(n) vs full clip which is O(n*m) on complex boundaries.
        oregon_bounds = oregon.total_bounds  # [minx, miny, maxx, maxy]
        filtered: dict[str, gpd.GeoDataFrame] = {}
        for level, gdf in smoke_layers.items():
            subset = gdf.cx[
                oregon_bounds[0]:oregon_bounds[2],
                oregon_bounds[1]:oregon_bounds[3],
            ]
            if not subset.empty:
                filtered[level] = subset

        if not filtered:
            continue

        site_smoke = _point_in_smoke(sites_gdf, filtered)
        for site_code, smoke_level in site_smoke.items():
            rows.append(
                {
                    "site_code": str(site_code),
                    "date_local": d.isoformat(),
                    "smoke_level": smoke_level,
                }
            )

        if i % 10 == 0 or i == len(dates):
            print(f"   ↳ {i}/{len(dates)} dates processed ({len(rows)} smoke records so far)")

    df = pd.DataFrame(rows).sort_values(["site_code", "date_local"]) if rows else pd.DataFrame(
        columns=["site_code", "date_local", "smoke_level"]
    )
    df.to_csv(output_path, index=False)

    print(f"   ✅ hms_smoke_{year}.csv → {len(df)} smoke site-date records")
