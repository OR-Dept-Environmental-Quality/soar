"""Metadata pipeline orchestration for AQS services.

This runner replaces the old `monitors_run.py` naming and is intended to
orchestrate metadata-level extraction (monitors and other metadata-centric
endpoints) for AQS. It keeps the same runtime behavior and function signature
(`run()`) so existing callers can continue to use a simple `run()` entrypoint.
"""
from __future__ import annotations
import sys
from pathlib import Path

# When running this module directly (python -m pipelines.aqs.metadata_run)
# the repository's `src` directory may not be on sys.path. Prepend it so
# `import soar` works from the repo root.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from soar import config
from soar.aqs.extractors.site_extractors import fetch_monitors
from soar.loaders.filesystem import write_csv
import pandas as pd
from soar.aqs import _client
import json
from pathlib import Path
import re

RAW_PARQUET = config.RAW / "monitors_raw.parquet"
RAW_CSV = config.RAW / "monitors_raw.csv"
CURATED_PARQUET = config.TRANS / "monitors_curated.parquet"
CURATED_CSV = config.TRANS / "monitors_curated.csv"
STAGED_PARQUET = config.STAGED / "monitors_staged.parquet"
STAGED_CSV = config.STAGED / "monitors_staged.csv"


def run() -> None:
    """Execute the metadata (monitors) pipeline end-to-end."""
    config.ensure_dirs(config.RAW, config.TRANS, config.STAGED)
    config.set_aqs_credentials()

    # Short-circuit if AQS is currently marked unhealthy by the circuit-breaker
    if _client.circuit_is_open():
        from soar.loaders.filesystem import atomic_write_json

        metadata_dir = Path(__file__).resolve().parents[2] / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        degraded = {
            "status": "degraded",
            "reason": "AQS circuit is open; skipping heavy extraction",
        }
        atomic_write_json(metadata_dir / "run_manifest_degraded.json", degraded)
        print("AQS circuit is open — wrote degraded manifest and exiting")
        return

    # Check AQS availability and log to metadata folder
    metadata_dir = Path(__file__).resolve().parents[2] / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    session = _client.make_session()
    try:
        avail = _client.fetch_json(session, "https://aqs.epa.gov/data/api/metaData/isAvailable")
    except Exception as exc:
        (metadata_dir / "aqs_availability.json").write_text(json.dumps({"error": str(exc)}))
        raise
    else:
        (metadata_dir / "aqs_availability.json").write_text(json.dumps(avail))

    # Save fields for sampleData service
    try:
        fields = _client.fetch_json(session, f"https://aqs.epa.gov/data/api/metaData/fieldsByService?email={config.AQS_EMAIL}&key={config.AQS_KEY}&service=sampleData")
        (metadata_dir / "fields_sampleData.json").write_text(json.dumps(fields))
    except Exception:
        pass

    # Determine parameter list source. Use only metadata/aqsSiteParameters.csv
    # (the authoritative site-parameter mapping shipped with the repo). There
    # is intentionally NO fallback to ops/dimPollutant.csv; if the metadata
    # file is missing or malformed we write a short manifest and stop.
    params_path = Path(__file__).resolve().parents[2] / "metadata" / "aqsSiteParameters.csv"
    params: list[tuple[str, str]] = []
    if params_path.exists():
        dfp = pd.read_csv(params_path, dtype=str)
        if "aqs_parameter" in dfp.columns and "analyte_name" in dfp.columns:
            params = list(dfp[["aqs_parameter", "analyte_name"]].dropna().itertuples(index=False, name=None))
        else:
            # write an error manifest and stop
            from soar.loaders.filesystem import atomic_write_json

            metadata_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(metadata_dir / "aqsSiteParameters_error.json", {
                "error": "aqsSiteParameters.csv is missing required columns",
                "found_columns": list(dfp.columns),
            })
            raise KeyError("metadata/aqsSiteParameters.csv must contain 'aqs_parameter' and 'analyte_name' columns")
    else:
        # No fallback allowed: write a clear missing manifest and stop
        from soar.loaders.filesystem import atomic_write_json

        metadata_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(metadata_dir / "aqsSiteParameters_missing.json", {
            "error": "metadata/aqsSiteParameters.csv not found. No fallback allowed.",
        })
        raise FileNotFoundError("No parameter source found: expected metadata/aqsSiteParameters.csv (no fallback)")

    print(f"Fetching monitors for {len(params)} parameters and writing per-parameter CSVs to {config.RAW}")

    total_rows = 0
    for code, label in params:
        try:
            df = fetch_monitors([code], config.clamped_bdate(), config.EDATE, config.STATE)
        except Exception as exc:
            print(f"Failed to fetch monitors for {code} ({label}): {exc}")
            continue
        if df.empty:
            print(f"No monitors found for {code} ({label})")
            continue
        safe_label = _sanitize_filename(label)
        csv_path = config.RAW / f"monitors_{safe_label}.csv"
        write_csv(df, csv_path)
        total_rows += len(df)


def _sanitize_filename(name: str, max_len: int = 80) -> str:
    """Return a filesystem-safe token based on `name`.

    Matches the sanitizer in sample_run.py: normalize whitespace to dashes,
    remove unsafe chars, collapse separators, strip edges, and truncate.
    """
    if not name:
        return "unknown"
    s = re.sub(r"\s+", "-", name)
    s = re.sub(r"[^A-Za-z0-9._-]", "", s)
    s = re.sub(r"[-_]{2,}", "-", s)
    s = re.sub(r"(^[^A-Za-z0-9]+)|([^A-Za-z0-9]+$)", "", s)
    if not s:
        return "unknown"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-_.")
    return s

    # Also write a combined raw file for convenience
    print(f"Finished monitors fetch. Total monitor rows written (sum of per-parameter files): {total_rows}")


if __name__ == "__main__":
    run()
