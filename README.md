# SOAR Data Pipeline

Small, well-scoped Python pipelines that pull EPA Air Quality System (AQS) monitor data and write it into a local filesystem "data lake" (raw → transform/curated → staged). Pipelines live under `pipelines/` and reusable logic lives in `src/soar/`.

## What This Does

This repository provides automated data pipelines for extracting air quality monitoring data from the EPA's Air Quality System (AQS) API. The pipelines:

- **Extract** raw data from EPA AQS services (sample measurements, annual summaries, daily aggregates)
- **Transform** data into analytics-ready formats with calculated fields and data quality improvements
- **Load** data into a structured filesystem data lake organized by data freshness layers

Data flows through three layers:
- `raw/` - Direct API extracts with minimal processing
- `transform/` - Cleaned and enriched data with business logic applied
- `staged/` - Final analytics-ready datasets optimized for downstream tools

## Project Structure

```
soar_pipeline/
├── src/soar/                    # Core reusable modules
│   ├── __init__.py
│   ├── config.py               # Environment variables and paths
│   ├── utils.py                # Utility functions
│   ├── aqs/                    # AQS-specific modules
│   │   ├── __init__.py
│   │   ├── _client.py          # API client with retry logic
│   │   ├── extractors/         # Data extraction logic
│   │   │   ├── __init__.py
│   │   │   ├── monitors.py     # Monitor metadata extraction
│   │   │   ├── aqs_service.py  # Sample data extraction
│   │   │   └── data.py         # Annual/daily data extraction
│   │   └── transformers/       # Data transformation logic
│   │       ├── __init__.py
│   │       ├── monitors.py     # Monitor data transformations
│   │       ├── aqi_daily.py    # AQI daily transformations
│   │       ├── trv_sample.py   # Toxics TRV calculations (sample)
│   │       └── trv_annual.py   # Toxics TRV calculations (annual)
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── filesystem.py       # File I/O helpers
│   └── schemas/
│       ├── __init__.py
│       └── monitors.py         # Data validation schemas
├── pipelines/aqs/              # Executable pipeline scripts
│   ├── run_monitors.py         # Monitor metadata pipeline
│   ├── run_monitors_transform.py # Monitor transformation pipeline
│   ├── run_metadata.py         # Combined metadata pipeline
│   ├── run_aqs_service.py      # Full AQS data extraction pipeline
│   ├── run_aqi_daily_transform.py # AQI daily transformation pipeline
│   ├── run_pm25_only.py        # PM2.5 focused extraction
│   └── run_trv_only.py         # TRV transformation only
├── tests/                      # Unit tests
│   ├── __init__.py
│   ├── conftest.py            # Test configuration
│   ├── test_monitors.py       # Monitor pipeline tests
│   ├── test_client_retry.py   # API client tests
│   ├── test_extractors.py     # Extractor tests
│   └── test_transformers_schema.py # Schema validation tests
├── ops/                        # Operations and configuration
│   ├── requirements.txt       # Python dependencies
│   ├── parameters.csv         # Parameter mappings (if used)
│   └── dimPollutant.csv       # Pollutant dimension data
└── README.md                  # This file
```

## Quick Start

### 1. Environment Setup

**Create and activate a virtual environment:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Install dependencies:**
```powershell
pip install -r ops/requirements.txt
```

**Optional: Install development tools:**
```powershell
pip install pre-commit ruff black pytest
pre-commit install
```

### 2. Configuration

**Create a `.env` file** in the repository root with your EPA AQS API credentials:

```env
# Required: EPA AQS API Credentials
AQS_EMAIL=your-email@example.com
AQS_KEY=your-api-key-here

# Required: Data parameters
BDATE=2005-01-01
EDATE=2024-12-31
STATE_CODE=41

# Required: Local data lake path
DATAREPO_ROOT=C:\path\to\your\data\lake

# Optional: Processing parameters
START_YEAR=2005
END_YEAR=2024
```

**Note:** Get your AQS API credentials from [EPA's AQS API signup](https://aqs.epa.gov/aqsweb/documents/data_api.html).

### 3. Run the Pipelines

All pipelines should be run from the repository root with `PYTHONPATH=src` set.

#### Monitor Metadata Pipeline
Extracts monitor metadata (locations, parameters, etc.) for Oregon:
```powershell
set PYTHONPATH=src
python pipelines/aqs/run_monitors.py
```

#### Full AQS Data Pipeline
Extracts sample, annual, and daily data for all parameters:
```powershell
set PYTHONPATH=src
python pipelines/aqs/run_aqs_service.py
```

#### Monitor Transformation Pipeline
Transforms raw monitor data into site-level summaries:
```powershell
set PYTHONPATH=src
python pipelines/aqs/run_monitors_transform.py
```

#### AQI Daily Transformation Pipeline
Transforms AQI daily summary data:
```powershell
set PYTHONPATH=src
python pipelines/aqs/run_aqi_daily_transform.py
```

#### Specialized Pipelines
- **PM2.5 Only**: `python pipelines/aqs/run_pm25_only.py`
- **TRV Transformations Only**: `python pipelines/aqs/run_trv_only.py`

## Pipeline Details

### Data Flow Architecture

1. **Raw Layer** (`DATAREPO_ROOT/raw/`)
   - Direct API extracts with minimal processing
   - Files: `aqs_sample_*.csv`, `aqs_annual_*.csv`, `aqs_daily_*.csv`

2. **Transform Layer** (`DATAREPO_ROOT/transform/`)
   - Cleaned and enriched data
   - Business logic applied (TRV calculations, data validation)
   - Files: `trv_sample_*.csv`, `trv_annual_*.csv`, `aqi_aqs_daily_*.csv`

3. **Staged Layer** (`DATAREPO_ROOT/staged/`)
   - Final analytics-ready datasets
   - Optimized for downstream tools like Power BI

### Key Pipelines Explained

| Pipeline | Purpose | Input | Output | Run Time |
|----------|---------|-------|--------|----------|
| `run_monitors.py` | Extract monitor metadata | EPA AQS API | `raw/aqs_monitors_oregon.csv` | ~2 minutes |
| `run_aqs_service.py` | Full data extraction | EPA AQS API | Raw CSV files by year/parameter | Hours-days |
| `run_monitors_transform.py` | Monitor data cleanup | Raw monitors | `staged/monitors_site_level.csv` | ~1 minute |
| `run_aqi_daily_transform.py` | AQI daily processing | Raw daily data | `transform/aqi_aqs_daily_*.csv` | ~5 minutes |

### API Rate Limiting & Performance

- **Concurrent Processing**: Pipelines use ThreadPoolExecutor (4 workers) for parallel API calls
- **Year-by-Year Processing**: Large date ranges are split by year to avoid API timeouts
- **Circuit Breaker**: Built-in retry logic with exponential backoff for API failures
- **Expected Runtime**: Full extraction (2005-2024) can take 24+ hours depending on parameter count

## Development

### Code Quality

**Run all quality checks:**
```powershell
ruff check .
black --check .
pytest -q
```

**Auto-fix formatting:**
```powershell
black .
ruff check . --fix
```

### Testing

**Run unit tests:**
```powershell
pytest
```

**Run with coverage:**
```powershell
pytest --cov=src --cov-report=html
```

### Adding New Pipelines

1. Create pipeline script in `pipelines/aqs/run_<name>.py`
2. Follow the pattern: config setup → extraction → transformation → loading
3. Add imports after `sys.path` modification
4. Include docstring with usage instructions
5. Add unit tests in `tests/`

### Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `AQS_EMAIL` | Yes | EPA AQS API email | `user@example.com` |
| `AQS_KEY` | Yes | EPA AQS API key | `abc123def456` |
| `BDATE` | Yes | Start date for data extraction | `2005-01-01` |
| `EDATE` | Yes | End date for data extraction | `2024-12-31` |
| `STATE_CODE` | Yes | State FIPS code (Oregon = 41) | `41` |
| `DATAREPO_ROOT` | Yes | Local data lake root path | `C:\data\soar` |
| `START_YEAR` | No | Override start year | `2005` |
| `END_YEAR` | No | Override end year | `2024` |

## Troubleshooting

### Common Issues

**"Module not found" errors:**
- Ensure `PYTHONPATH=src` is set when running pipelines
- Activate your virtual environment

**API timeouts:**
- Reduce date ranges or run year-by-year
- Check your internet connection

**Permission errors:**
- Ensure write access to `DATAREPO_ROOT` path
- Check antivirus exclusions for data directories

**Memory issues:**
- Large datasets may require 8GB+ RAM
- Consider processing smaller date ranges

### Getting Help

- Check existing issues in the repository
- Review EPA AQS API documentation
- Verify your API credentials are active

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run quality checks: `ruff check . && black . && pytest`
5. Submit a pull request

## License

[Specify your license here]
