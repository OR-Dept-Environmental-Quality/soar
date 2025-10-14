# Copilot Instructions for Soar Data Pipeline

## What this repo does
Small, well-scoped Python pipelines that pull EPA AQS monitor data and write it into a local filesystem "data lake" (raw -> transform/curated -> staged). Pipelines live under `pipelines/` and reusable logic lives in `src/soar/`.

## Quick architecture map
- `src/soar/config.py` — environment and paths (BDATE/EDATE, DATAREPO_ROOT, AQS credentials). Important: `set_aqs_credentials()` delays importing `pyaqsapi` until runtime.
- `src/soar/aqs/extractors/*.py` — fetchers that call external AQS services (network code should be isolated here).
Note: transformation logic is being redesigned; the previous `src/soar/aqs/transformers/` module has been removed. Pipelines currently write raw outputs only.
- `src/soar/loaders/filesystem.py` — single-purpose I/O helpers (write_csv/write_parquet) that create parent dirs.
- `pipelines/aqs/metadata_run.py` — orchestrator that wires config, extraction, transform, and loaders and writes outputs under `DATAREPO_ROOT`.

## Developer workflows and exact commands
- Create and activate a venv (Windows PowerShell):
  - `python -m venv .venv; .\\.venv\\Scripts\\Activate.ps1`
- Install dependencies (project root):
  - `pip install -r ops/requirements.txt`
- Run the metadata pipeline (from repo root):
  - `set PYTHONPATH=src; python pipelines/aqs/metadata_run.py` (PowerShell)
  - or `python -m pipelines.aqs.metadata_run` (module mode; the script inserts `src` onto sys.path)
- Run the AQS service pipeline (from repo root):
  - `set PYTHONPATH=src; python pipelines/aqs/aqs_service_run.py` (PowerShell)
  - or `python -m pipelines.aqs.aqs_service_run` (module mode)
- Run tests and linters (after installing requirements):
  - `ruff check .`
  - `black --check .`
  - `pytest -q`

## Concrete repository conventions agents should follow
- Environment variables are authoritative and parsed in `src/soar/config.py`:
  - Required: `BDATE`, `EDATE`, `DATAREPO_ROOT`
  - Credentials: `AQS_EMAIL`, `AQS_KEY` (used by `config.set_aqs_credentials()`)
  - Optional: `STATE_CODE` (config zero-pads to 2 digits)
- Avoid importing heavy network dependencies at module import time. The repo intentionally delays importing `pyaqsapi` until `config.set_aqs_credentials()` so unit tests and simple static analysis won't require network or requests.
- Prefer pure pandas DataFrame inputs/outputs for transformer functions so tests can construct in-memory frames (see `tests/test_monitors.py`).
- Write small helpers in `src/soar/loaders/filesystem.py` for any file writes — they already handle parent-dir creation.

## Integration and runtime notes
-- Runtime dependency for parquet: ensure `pyarrow` or `fastparquet` is installed when writing parquet files (parquet writing may be used by metadata pipelines). This is not declared in code; check `ops/requirements.txt`.
- `pyaqsapi` is only required when actually contacting the EPA AQS API. Tests operate on local DataFrames and do not call external services.

## Example code patterns to cite in PRs or edits
- To add a new extractor, mirror `src/soar/aqs/extractors/monitors.py` and add a pipeline entrypoint in `pipelines/aqs/` that:
  1. calls `config.ensure_dirs()`
  2. calls `config.set_aqs_credentials()` only if network access is needed
  3. runs fetch -> transform -> stage -> write
- Tests should construct DataFrames directly (see `tests/test_monitors.py:: _build_sample_frame`) and validate transformer output with `pandera` schemas where helpful.

## What agents must NOT assume
-- Do not assume `src` is on sys.path — pipelines either add it (see `metadata_run.py` or `aqs_service_run.py`) or callers must set `PYTHONPATH`.
- Do not assume `pyaqsapi` is installed; only import it at runtime if the change requires actual API calls.

---
**If anything in these instructions is unclear or missing, tell me which parts you'd like expanded (env examples, ops/requirements quick check, or common PR templates) and I'll iterate.**
