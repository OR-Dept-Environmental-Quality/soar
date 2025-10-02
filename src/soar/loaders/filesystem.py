"""Filesystem loaders for data lake outputs."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to parquet, creating parent folders when needed."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(destination, index=False)


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV, creating parent folders when needed."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(destination, index=False)
