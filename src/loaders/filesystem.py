"""Filesystem loaders for data lake outputs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import json
import tempfile
import os


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


def append_csv(frame: pd.DataFrame, path: Path) -> None:
    """Append a DataFrame to a CSV file, creating parent folders and header as needed.

    This helper is intended for streaming writes where the full result for a
    parameter may be built incrementally (per-site or per-year) and we want to
    avoid holding everything in memory.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_header = not destination.exists()
    # pandas handles append mode; ensure consistent columns by writing header only
    # on first append call.
    frame.to_csv(destination, mode="a", header=write_header, index=False)


def atomic_write_text(path: Path | str, text: str, encoding: str = "utf-8") -> None:
    """Atomically write text to `path` by writing to a temp file then replacing.

    Ensures parent directories exist. Works across platforms by using os.replace.
    """
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # write to a temp file in the same directory to ensure replace is atomic
    dirpath = str(destination.parent)
    fd, tmp_path = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as fh:
            fh.write(text)
            if destination.exists():
                try:
                    destination.unlink()
                except FileNotFoundError:
                    pass
        try:
            os.replace(tmp_path, str(destination))
        except PermissionError:
            if destination.exists():
                try:
                    destination.unlink()
                except FileNotFoundError:
                    pass
            os.replace(tmp_path, str(destination))
        except Exception:
            os.rename(tmp_path, str(destination))
    finally:
        # if tmp_path still exists, try to remove it
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def atomic_write_json(path: Path | str, obj) -> None:
    """Atomically write JSON-serializable object to `path`.

    This helper uses atomic_write_text under the hood.
    """
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    atomic_write_text(path, text)
