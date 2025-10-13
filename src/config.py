"""Core configuration for the soar package."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

AQS_EMAIL = os.getenv("AQS_EMAIL")
AQS_KEY = os.getenv("AQS_KEY")
STATE = (os.getenv("STATE_CODE") or "").zfill(2)

# Read raw BDATE/EDATE from environment. Keep the original values available but
# expose helpers that enforce repository policy (minimum start date 2005-01-01).
BDATE = date.fromisoformat(os.environ["BDATE"])
EDATE = date.fromisoformat(os.environ["EDATE"])

# Policy: do not allow BDATE earlier than 2005-01-01. Callers should use
# `clamped_bdate()` where they want the policy-enforced start date.
_MIN_BDATE = date(2005, 1, 1)
START_YEAR = BDATE.year
END_YEAR = EDATE.year
ROOT = Path(os.environ["DATAREPO_ROOT"]).expanduser()
RAW = ROOT / "raw" / "aqs" / "monitors"
RAW_SAMPLE = ROOT / "raw" / "aqs" / "sample"
TRANS = ROOT / "transform" / "aqs" / "monitors"
STAGED = ROOT / "staged" / "aqs" / "monitors"
RAW_ANNUAL = ROOT / "raw" / "aqs" / "annual"
CTL_DIR = ROOT / "raw" / "aqs" / "_ctl"
PARAMS_CSV = Path("ops/parameters.csv")
SAMPLE_MODE = os.getenv("SAMPLE_MODE", "by_state")
# When a by_state response is very large, a fallback to per-site mode could be used.
SAMPLE_FALLBACK_ROW_THRESHOLD = int(os.getenv("SAMPLE_FALLBACK_ROW_THRESHOLD", "200000"))


def ensure_dirs(*paths: Path) -> None:
    """Create any missing directories for the provided paths."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def set_aqs_credentials() -> None:
    """Validate the configured credentials and register them with pyaqsapi."""
    if not AQS_EMAIL or not AQS_KEY:
        raise ValueError("Missing AQS_EMAIL or AQS_KEY in environment")

    # Import pyaqsapi only when credentials are actually needed. This avoids
    # forcing the dependency (and transitive deps like 'requests') at import
    # time which makes tests and light-weight uses easier.
    try:
        from pyaqsapi import aqs_credentials
    except Exception as exc:  # pragma: no cover - helpful runtime message
        raise ImportError(
            "The 'pyaqsapi' package is required to set AQS credentials. "
            "Install it into your environment (pip install pyaqsapi) and ensure its "
            "dependencies such as 'requests' are available.") from exc

    aqs_credentials(AQS_EMAIL, AQS_KEY)


def clamped_bdate() -> date:
    """Return BDATE but clamped to the repository minimum _MIN_BDATE.

    Pipelines should call this helper when deciding which start date to use
    for AQS API requests so historical backfills won't request data earlier
    than 2005-01-01 even if the environment BDATE is set earlier.
    """
    return BDATE if BDATE >= _MIN_BDATE else _MIN_BDATE
