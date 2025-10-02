"""Core configuration for the soar package."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from pyaqsapi import aqs_credentials

load_dotenv()

AQS_EMAIL = os.getenv("AQS_EMAIL")
AQS_KEY = os.getenv("AQS_KEY")
STATE = (os.getenv("STATE_CODE") or "").zfill(2)
BDATE = date.fromisoformat(os.environ["BDATE"])
EDATE = date.fromisoformat(os.environ["EDATE"])
ROOT = Path(os.environ["DATAREPO_ROOT"]).expanduser()
RAW = ROOT / "raw" / "aqs" / "monitors"
TRANS = ROOT / "transform" / "aqs" / "monitors"
STAGED = ROOT / "staged" / "aqs" / "monitors"


def ensure_dirs(*paths: Path) -> None:
    """Create any missing directories for the provided paths."""
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def set_aqs_credentials() -> None:
    """Validate the configured credentials and register them with pyaqsapi."""
    if not AQS_EMAIL or not AQS_KEY:
        raise ValueError("Missing AQS_EMAIL or AQS_KEY in environment")
    aqs_credentials(AQS_EMAIL, AQS_KEY)
