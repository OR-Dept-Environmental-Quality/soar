# HRRR Mixing Height and Ventilation Index Calculation

Extracts hourly mixing height (Planetary Boundary Layer Height) from NOAA's High-Resolution Rapid Refresh (HRRR) model for Oregon monitoring sites and combines it with AQS wind speed data to compute ventilation index.

This README covers the full pipeline:
- src/hrrr/extractors/pull_raw_mixing_height.py + pipelines/hrrr/run_raw_mixing_height.py
- src/hrrr/transformers/mixing_height.py + pipelines/hrrr/run_transform_mixing_height.py
- src/stage/consolidate_fct_mixing_height.py + pipelines/hrrr/run_fct_mixing_height.py
- src/aqs/transformers/ventilation_index.py + pipelines/aqs/run_fct_ventilation_index.py

## Prerequisites

Requires staged/dim_sites/dim_sites.csv (site code, lat, long) and staged/fct_wind_speed_{year}.csv to already exist.

## Usage

Run in order, from the project root:

### 1. Pull raw HRRR data (pipelines/hrrr/run_raw_mixing_height.py)
Downloads the full HPBL field for every hour via NOAA's .idx byte-range index (not the full grid file), crops it to Oregon plus a 100 km buffer, and writes one 24-band GeoTIFF per PST calendar day (band N = hour N-1 PST) to raw/hrrr_mixing_height/.

This step is resumable: if interrupted, rerunning skips any day already completed and picks up where it left off. This step is also slow (network-bound); it only needs to be rerun to pull more years of data.

### 2. Compute per-site mixing height (IDW) (pipelines/run_transform_mixing_height.py)
Reads the raw day files and computes a 50 km radius, inverse-distance-weighted average of nearby grid cells for each site hour.
Output: one CSV per day in transform/hrrr_mixing_height.

### 3. Stage the yearly mixing height files (pipelines/hrrr/run_fct_mixing_height.py)
Consolidates the daily files into one CSV per year:
staged/fct_mixing_height_{year}.csv. Prints a warning if a year's daily file count does not match the expected number of days for that year.

### 4. Compute ventilation index (pipelines/aqs/run_fct_ventilation_index.py)
Joins mixing height with AQS wind speed (converts knots to m/s) and computes hourly ventilation index with a category label.
Output: staged/fct_ventilation_index_{year}.csv

## Methodology Notes
- **Raw storage is spatially cropped, not full CONUS grid.** Each raw file covers Oregon plus a 100 km buffer rather than HRRR's full continental domain, cutting storage substantially while leaving enough margin that a larger IDW range or other analysis can be tested without needing to re-download.
- **Raw storage is fixed PST (UTC-8), not UTC.** HRRR's own archive is UTC-indexed, so each PST hour is converted to its corresponding UTC time before being requested from NOAA, and the resulting files are organized and labeled in PST.
- **Sites are linked by reprojecting their coordinates into HRRR's grid, not fixed lookup.** Each site's latitude and longitude come from staged/dim_sites/dim_sites.csv (in WGS84 format) and are reprojected into HRRR's Lambert Conformal Conic projection for every raw file read. That projected coordinate is used to locate the corresponding pixel in the grid, and the IDW window (nearby grid cells within the averaging radius) is built around that exact point. The resulting site_code is carried through unchanged into every downstream row.
- **Site values are spatially averaged, not point-sampled.** Each site's mixing height is a 50 km radius, inverse-distance-weighted (power = 2) average of nearby HRRR grid cells. This is meant to better generalize the ventilation index to a larger area.
- **Downloads use byte-range requests**, not full HRRR files. NOAA publishes an .idx index alongside each hourly grid file; this pipeline reads that index to find the HPBL field's byte range and downloads only that slice. This cuts the extraction from ~100 MB to ~3 MB per hour.
- **Ventilation Index** = wind speed (m/s) × mixing height (m), per the VCIS methodology (Ferguson et al.) with categorization labels from the University of Washington - Department of Atmospheric Sciences, which match those in the DEQ SOP for stagnation events.

Categories are:
    - Very Poor: 0-235
    - Poor: 235-2,350
    - Marginal: 2,350-4,700
    - Good: 4,700+

## Output Folders
| Folder | Contents |
| --- | --- |
| raw/hrrr_mixing_height/ | Cropped, PST-organized 24-band GeoTIFF day-files |
| transform/hrrr_mixing_height/ | Daily per-site mixing height CSVs |
| staged/fct_mixing_height_{year}.csv | Yearly consolidated mixing height |
| staged/fct_ventilation_index_{year}.csv | Yearly hourly ventilation index |

