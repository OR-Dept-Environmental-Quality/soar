# Copilot Instructions for Soar Data Pipeline

## Overview
This repository implements modular Python pipelines for ingesting EPA AQS data into a local filesystem data lake, supporting downstream analytics (e.g., Power BI).

## Architecture
- **src/soar/**: Core library code. Organized by domain:
  - `config.py`: Configuration management.
  - `aqs/extractors/monitors.py`: Data extraction logic for EPA AQS monitors.
  - `aqs/transformers/monitors.py`: Data transformation logic.
  - `loaders/filesystem.py`: Handles writing data to the local filesystem.
- **pipelines/aqs/monitors_run.py**: Entry point for running the AQS pipeline. Orchestrates extraction, transformation, and loading.
- **tests/**: Contains unit tests (e.g., `test_monitors.py`).
- **ops/requirements.txt**: Python dependencies.

## Developer Workflow
- **Environment Setup**:
  - Use a virtual environment (`python -m venv .venv; .\.venv\Scripts\activate`).
  - Install dependencies: `pip install -r ops/requirements.txt`.
  - Create a `.env` file in the repo root with required keys (see README for example).
- **Running Pipelines**:
  - Set `PYTHONPATH=src` before running pipeline scripts.
  - Main pipeline: `python pipelines/aqs/monitors_run.py`.
- **Testing & Quality**:
  - Run checks: `ruff check .`, `black --check .`, `pytest -q`.
- **Data Output**:
  - Pipeline writes to `raw/`, `transform/`, and `staged/` under `DATAREPO_ROOT`.

## Project-Specific Patterns
- **Modular Design**: Extraction, transformation, and loading are separated by submodules for maintainability and extensibility.
- **Configuration**: All environment/configuration is managed via `.env` and `config.py`.
- **Explicit Data Flow**: Pipelines are orchestrated via scripts in `pipelines/`, not via CLI or API entrypoints.
- **Testing**: Tests are in `tests/` and should cover core logic in `src/soar/`.

## Integration Points
- **EPA AQS API**: Credentials and parameters are set via `.env`.
- **Filesystem**: All outputs are written to the local data lake structure defined by `DATAREPO_ROOT`.

## Example: Adding a New Data Source
1. Create new extractor/transformer modules under `src/soar/aqs/`.
2. Update or add pipeline scripts in `pipelines/`.
3. Add tests in `tests/`.

## References
- See `README.md` for quickstart and environment details.
- Key files: `src/soar/config.py`, `src/soar/aqs/extractors/monitors.py`, `src/soar/aqs/transformers/monitors.py`, `src/soar/loaders/filesystem.py`, `pipelines/aqs/monitors_run.py`.

---
**Feedback:** Please review and suggest edits for any unclear or missing sections.
