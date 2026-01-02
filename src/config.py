"""Configuration module for SOAR AQS data pipeline.

Loads environment variables, defines data lake paths, and provides utilities for
credential management and date policy enforcement. All paths point to the data lake
(DATAREPO_ROOT), not the code repository.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# EPA AQS API credentials
AQS_EMAIL = os.getenv("AQS_EMAIL")
# Accept either AQS_KEY or legacy AQS_Key from user-provided .env files
AQS_KEY = os.getenv("AQS_KEY") or os.getenv("AQS_Key")

# Envista API credentials (aliases accept either ENV_* or ENVISTA_* names)
ENV_USER = os.getenv("ENV_USER") or os.getenv("ENVISTA_USER")
ENV_KEY = os.getenv("ENV_KEY") or os.getenv("ENVISTA_KEY")
ENV_URL = os.getenv("ENV_URL") or os.getenv("ENVISTA_URL")

# State FIPS code (zero-padded to 2 digits)
STATE = (os.getenv("STATE_CODE") or "").zfill(2)

# Date range for data extraction
# Raw values from environment - use clamped_bdate() for policy-enforced dates
BDATE = date.fromisoformat(os.environ["BDATE"])
EDATE = date.fromisoformat(os.environ["EDATE"])

# Repository policy: no data extraction before 2005-01-01
# Use clamped_bdate() in pipelines to enforce this constraint
_MIN_BDATE = date(2005, 1, 1)
START_YEAR = BDATE.year
END_YEAR = EDATE.year

# Data lake root and layer paths
# All output written to DATAREPO_ROOT data lake, organized by layer and service
ROOT = Path(os.environ["DATAREPO_ROOT"]).expanduser()
RAW = ROOT / "raw" / "aqs" / "monitors"  # Monitors path
RAW_SAMPLE = ROOT / "raw" / "aqs" / "sample"  # Sample data (hourly/sub-daily)
RAW_DAILY = ROOT / "raw" / "aqs" / "daily"  # Daily summaries
RAW_ANNUAL = ROOT / "raw" / "aqs" / "annual"  # Annual aggregates
RAW_QUALIFIERS = ROOT / "raw" / "aqs" / "qualifiers"  # Qualifier data for toxics

# Backwards-compatible aliases introduced by Envista feature PR
# These map the new names used in PRs to the existing paths so old and
# new code can coexist until a full rename/migration is performed.
RAW_AQS_MONITORS = RAW
RAW_AQS_SAMPLE = RAW_SAMPLE
RAW_AQS_DAILY = RAW_DAILY
RAW_AQS_ANNUAL = RAW_ANNUAL
RAW_AQS_QUALIFIERS = RAW_QUALIFIERS

# Envista raw paths (new) — created as aliases; directories will be created
RAW_ENV_MONITORS = ROOT / "raw" / "envista" / "monitors"
RAW_ENV_SAMPLE = ROOT / "raw" / "envista" / "sample"
RAW_ENV_DAILY = ROOT / "raw" / "envista" / "daily"
TRANS = ROOT / "transform" / "aqs" / "monitors"  # Transformed/curated layer
STAGED = ROOT / "staged" / "aqs" / "monitors"  # Staged layer for analytics
CTL_DIR = ROOT / "raw" / "aqs" / "_ctl"  # Control files (circuit breaker health, etc.)

# Parameter definitions
PARAMS_CSV = Path("ops/parameters.csv")
REGIONS_SHP = Path("ops/dimRegions.shp")

# Sample extraction mode: "by_state" (default) or "by_site"
# by_state: Fetch all sites at once, memory-efficient streaming
# by_site: Fetch site-by-site, slower but more granular
SAMPLE_MODE = os.getenv("SAMPLE_MODE", "by_state")

# Fallback threshold: if by_state response exceeds this row count, fall back to by_site mode
SAMPLE_FALLBACK_ROW_THRESHOLD = int(
    os.getenv("SAMPLE_FALLBACK_ROW_THRESHOLD", "200000")
)

# HTTP and concurrency tuning for AQS clients
AQS_TIMEOUT = int(os.getenv("AQS_TIMEOUT", "120"))
AQS_RETRIES = int(os.getenv("AQS_RETRIES", "6"))
AQS_BACKOFF_FACTOR = float(os.getenv("AQS_BACKOFF_FACTOR", "1.5"))
AQS_RETRY_MAX_WAIT = int(os.getenv("AQS_RETRY_MAX_WAIT", "60"))
AQS_MIN_DELAY = float(os.getenv("AQS_MIN_DELAY", "0"))
AQS_MAX_RPS = int(os.getenv("AQS_MAX_RPS", "5"))
AQS_SAMPLE_YEAR_WORKERS = max(1, int(os.getenv("AQS_SAMPLE_YEAR_WORKERS", "3")))
AQS_SAMPLE_PARAM_WORKERS = max(1, int(os.getenv("AQS_SAMPLE_PARAM_WORKERS", "3")))
AQS_ANNUAL_YEAR_WORKERS = max(1, int(os.getenv("AQS_ANNUAL_YEAR_WORKERS", "3")))
AQS_DAILY_YEAR_WORKERS = max(1, int(os.getenv("AQS_DAILY_YEAR_WORKERS", "3")))


# Envista retry / circuit defaults
ENV_TIMEOUT = int(os.getenv("ENV_TIMEOUT", "120"))
ENV_RETRIES = int(os.getenv("ENV_RETRIES", "6"))
ENV_BACKOFF_FACTOR = float(os.getenv("ENV_BACKOFF_FACTOR", "1.5"))
ENV_RETRY_MAX_WAIT = int(os.getenv("ENV_RETRY_MAX_WAIT", "60"))
ENV_MIN_DELAY = float(os.getenv("ENV_MIN_DELAY", "0"))
ENV_MAX_RPS = int(os.getenv("ENV_MAX_RPS", "5"))
ENV_SAMPLE_YEAR_WORKERS = max(1, int(os.getenv("ENV_SAMPLE_YEAR_WORKERS", "3")))
ENV_SAMPLE_PARAM_WORKERS = max(1, int(os.getenv("ENV_SAMPLE_PARAM_WORKERS", "3")))

# Envista circuit breaker settings (explicitly exposed)
ENV_CIRCUIT_THRESHOLD = int(os.getenv("ENV_CIRCUIT_THRESHOLD", "5"))
ENV_CIRCUIT_COOLDOWN = int(os.getenv("ENV_CIRCUIT_COOLDOWN", "1800"))


def ensure_dirs(*paths: Path) -> None:
    """Create directory structures for data lake layers.

    Creates any missing directories in the provided paths. Used during pipeline
    initialization to ensure output directories exist before writing data.

    Args:
        *paths: One or more Path objects to create
    """
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def set_aqs_credentials() -> None:
    """Validate and register EPA AQS API credentials with pyaqsapi library.

    Performs input validation on credentials before registering them.
    Lazy-loads pyaqsapi dependency to avoid requiring network libraries at module
    import time. This allows tests and lightweight scripts to import config without
    pulling in requests and other heavy dependencies.

    Raises:
        ValueError: If AQS_EMAIL or AQS_KEY environment variables are missing or invalid
        ImportError: If pyaqsapi package is not installed
    """
    if not AQS_EMAIL or not AQS_KEY:
        raise ValueError("Missing AQS_EMAIL or AQS_KEY in environment")

    # Validate email format
    if "@" not in AQS_EMAIL or "." not in AQS_EMAIL:
        raise ValueError("AQS_EMAIL does not appear to be a valid email address")

    # Validate key format (AQS keys are typically alphanumeric, reasonable length)
    if len(AQS_KEY.strip()) < 10:
        raise ValueError("AQS_KEY appears to be invalid (too short)")

    # Lazy import: only load pyaqsapi when credentials are actually needed
    # This avoids forcing the dependency at module import time
    try:
        from pyaqsapi import aqs_credentials
    except Exception as exc:  # pragma: no cover - helpful runtime message
        raise ImportError(
            "The 'pyaqsapi' package is required to set AQS credentials. "
            "Install it into your environment (pip install pyaqsapi) and ensure its "
            "dependencies such as 'requests' are available."
        ) from exc

    aqs_credentials(AQS_EMAIL, AQS_KEY)


def clamped_bdate() -> date:
    """Return BDATE but clamped to the repository minimum _MIN_BDATE.

    Pipelines should call this helper when deciding which start date to use
    for AQS API requests so historical backfills won't request data earlier
    than 2005-01-01 even if the environment BDATE is set earlier.
    """
    return BDATE if BDATE >= _MIN_BDATE else _MIN_BDATE
