"""Pytest configuration for local package imports.

This prepends the repository `src` directory to sys.path so tests can import
the `soar` package without installing it into the environment.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
