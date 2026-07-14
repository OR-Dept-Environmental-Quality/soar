"""Pytest configuration for local package imports.

This prepends the repository `src` directory to sys.path so tests can import
the `soar` package without installing it into the environment.

Environment variable stubs are set before any src import so that config.py
(which requires BDATE, EDATE, and DATAREPO_ROOT at module load time) does not
raise KeyError when tests run without a real .env file.  Values already
present in the environment take precedence via os.environ.setdefault().
"""

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Provide safe defaults so config.py can be imported in unit-test contexts.
# These are only used when the variables are not already set in the environment.
os.environ.setdefault("BDATE", "2021-01-01")
os.environ.setdefault("EDATE", "2021-12-31")
os.environ.setdefault("DATAREPO_ROOT", tempfile.gettempdir())
os.environ.setdefault("STATE_CODE", "41")
