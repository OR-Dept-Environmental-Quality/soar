# HRRR Mixing Height and Ventilation Index Calculation

Extracts hourly mixing height (Planetary Boundary Layer Height) from NOAA's High Resolution Rapid Refresh Model (HRRR) for Oregon monitoring sites, and combines it with AQS wind speed data to compute ventilation index.

This README covers the full pipeline:
- 'src/hrrr/extractors/pull_raw_mixing_height.py' + 'pipelines/hrrr/run_raw_mixing_height.py'
- 'src/stage/consolidate_mixing_height.py' + 'pipelines/hrrr/run_fct_mixing_height.py'
- 'src/aqs/transformers/ventilation_index.py' + 'pipelines/aqs/run_fct_ventilation_index.py'

## Prerequisites

Requires 'staged/dim_sites/dim_sites.csv' (site code, lat, long) and 'staged/fct_wind_speed_{year}.csv' to already exist

## Usage 

Run in order, from the project root:

### 1. Pull raw HRRR data
    You will be prompted for:
    - **Keep raw GRIB2 files?** -- 'y' keeps the downloaded grid files in 'raw/hrrr_grib' (~25 GB/year); 'n' (default) deletes each file after the required values are extracted 
    This step is resumable, if interrupted then rerunning skips any day already completed and picks up where it left off
    Output: one CSV per day in 'transform/hrrr_mixing_height/'

### 2. Stage the yearly mixing height files
    Consolidates the daily files into one CSV per year:
    'staged/fct_mixing_height_{year}.csv'. Prints a warning if a year's daily file count doesn't match the expected number of days for that year. 

### 3. Compute ventilation index
    Joins mixing height with AQS wind speed (converts knots to m/s) and computes hourly ventilation index, with a catergory label.
    Output: 'staged/fct_ventilation_index_{year}.csv'

## Methodology Notes
- **Site values are spatially averaged, not point-sampled.** Each site's mixing height is a 50km radius, inverse-distance-weighted (power=2) average of nearby HRRR grid cells. This is meant to better generalize the ventilation index to a larger area. 
- **Downloads use byte-range requests** not full HRRR files. NOAA publishes an '.idx' index alongside each hourly grid file; this pipeline reads that index to find the HPBL field's byte range and downloads only that slice. This cuts the extraction from ~100 MB to ~3 MB per hour. 
- **Ventilation Index** = wind speed(m/s) x mixing height(m), per the VCIS methodology (Ferguson et al.) with categorization labels from The University of Washington - Dept. of Atmospheric Sciences, which match those in the DEQ SOP for stagnation events. 
Categories are:
    - Very Poor: 0-235
    - Poor: 235-2,350
    - Marginal: 2,350-4,700
    - Good: 4,700+

## Output Folders
 | Folder | Contents |
 | --- | ---|
 | 'raw/hrrr_grib/' | Downloaded GRIB2 grid files (only if 'keep_raw' selected) |
 | 'transform/hrrr_mixing_height/' | Daily per-site mixing height CSVs |
 | 'staged/fct_mixing_height_{year}.csv' | Yearly consolidated mixing height |
 | 'staged/fct_ventilation_index_{year}.csv' | Yearly hourly ventilation index |

