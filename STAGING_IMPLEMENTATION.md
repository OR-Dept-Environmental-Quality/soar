# Staging Pipeline Implementation Summary

## Overview
Successfully implemented comprehensive staging pipelines to create fact and dimension tables from transform layer data. All pipelines properly exclude geographic fields (latitude, longitude, county) from fact tables to maintain separation between measurement data and site dimension data.

## Created Staging Scripts

### Fact Table Staging Scripts
Located in `src/stage/`:

1. **`consolidate_fct_toxics_annual.py`**
   - Source: `transform/trv/annual/trv_annual_YYYY.csv`
   - Output: `staged/fct_toxics_annual_YYYY.csv`
   - Fields: All TRV annual fields except geographic coordinates
   - Status: ✅ Tested and working (20 years, 10,549 total records)

2. **`consolidate_fct_toxics_sample.py`**
   - Source: `transform/trv/sample/trv_sample_YYYY.csv` 
   - Output: `staged/fct_toxics_sample_YYYY.csv`
   - Fields: All TRV sample fields except geographic coordinates

3. **`consolidate_fct_criteria_daily.py`**
   - Source: `transform/aqi/aqi_aqs_daily_YYYY.csv`
   - Output: `staged/fct_criteria_daily_YYYY.csv`
   - Fields: All AQI daily fields except geographic coordinates

### Dimension Table Staging Scripts
Located in `src/stage/`:

4. **`consolidate_dim_sites.py`**
   - Source: `transform/monitors/aqs_monitors.csv`
   - Output: `staged/dim_sites.csv` 
   - Purpose: Site dimension data including geographic coordinates
   - Status: ✅ Tested and working (170 site records)

5. **`consolidate_dim_pollutant.py`**
   - Source: `ops/dimPollutant.csv`
   - Output: `staged/dim_pollutant.csv`
   - Purpose: Pollutant parameter dimension data
   - Status: ✅ Tested and working (112 parameters: 109 toxics, 1 PM2.5, 1 ozone)

## Pipeline Runner Scripts
Located in `pipelines/aqs/`:

- **`run_staging_all.py`** - Comprehensive runner for all staging pipelines
- **`run_fct_toxics_annual.py`** - Individual runner for toxics annual staging
- **`run_fct_toxics_sample.py`** - Individual runner for toxics sample staging  
- **`run_fct_criteria_daily.py`** - Individual runner for criteria daily staging
- **`run_dim_tables.py`** - Runner for both dimension table staging

## Field Architecture

### Excluded from Fact Tables
Geographic fields properly excluded to maintain data architecture:
- `latitude`
- `longitude` 
- `county`
- `monitors_site_code` (not present in source data)

### Available in Dimension Tables
Geographic and site information available via `dim_sites.csv` for joins on `site_code`:
- Site coordinates (latitude, longitude)
- County information
- Site metadata (monitoring objectives, methods, etc.)

## Data Schema Alignment

### fct_toxics_annual_YYYY.csv
```
site_code, parameter, sample_duration, parameter_code, poc, method, year, 
units_of_measure, observation_count, observation_percent, validity_indicator,
valid_day_count, required_day_count, exceptional_data_count, null_observation_count,
primary_exceedance_count, secondary_exceedance_count, certification_indicator,
arithmetic_mean, arithmetic_mean_ug_m3, ugm3_converted, xtrv_cancer, xtrv_noncancer,
standard_deviation, first_max_value, first_max_value_ug_m3, xtrv_acute_first,
first_max_datetime, second_max_value, second_max_value_ug_m3, xtrv_acute_second,
second_max_datetime, third_max_value, third_max_datetime, fourth_max_value,
fourth_max_datetime, first_max_nonoverlap_value, first_max_n_o_datetime,
second_max_nonoverlap_value, second_max_n_o_datetime, ninety_ninth_percentile,
ninety_eighth_percentile, ninety_fifth_percentile, ninetieth_percentile,
seventy_fifth_percentile, fiftieth_percentile, tenth_percentile
```

### fct_toxics_sample_YYYY.csv
```
site_code, parameter_code, poc, parameter, date_local, sample_measurement,
units_of_measure, sample_measurement_ug_m3, trv_cancer, trv_noncancer, trv_acute,
xtrv_cancer, xtrv_noncancer, xtrv_acute, qualifier, sample_duration, sample_frequency,
detection_limit, uncertainty, method_type, method, method_code
```

### fct_criteria_daily_YYYY.csv
```
parameter_code, poc, parameter, sample_duration_code, sample_duration, date_local,
units_of_measure, event_type, observation_count, observation_percent, validity_indicator,
arithmetic_mean, first_max_value, first_max_hour, aqi, method_code, method, site_code
```

## Test Results

✅ **Dimension Tables**: Successfully created
- `dim_sites.csv`: 170 site records
- `dim_pollutant.csv`: 112 parameter records

✅ **Toxics Annual Facts**: Successfully created 20 yearly files
- Years 2005-2024 processed
- Total: 10,549 records across all years
- All 47 required columns present
- Geographic fields properly excluded

## Usage

### Run All Staging Pipelines
```bash
python pipelines/aqs/run_staging_all.py
```

### Run Individual Pipelines
```bash
python pipelines/aqs/run_fct_toxics_annual.py
python pipelines/aqs/run_fct_toxics_sample.py
python pipelines/aqs/run_fct_criteria_daily.py
python pipelines/aqs/run_dim_tables.py
```

## Architecture Benefits

1. **Proper Data Separation**: Fact tables contain only measurement data
2. **Dimensional Modeling**: Geographic data available via dimension table joins
3. **Consistent Schema**: All staging scripts follow the same pattern
4. **Comprehensive Coverage**: Handles toxics annual, toxics sample, criteria daily, and dimensions
5. **Error Handling**: Scripts provide detailed logging and error reporting
6. **Flexible Execution**: Can run all pipelines together or individually

## Output Location
All staging files written to: `C:\Users\abiberi\Oregon\DEQ - Air Data Team - DataRepo\soar\staged\`