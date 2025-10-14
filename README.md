# Soar Data Pipeline

Modular Python pipelines for ingesting EPA AQS data into a local filesystem data lake that is ready for downstream analytics tools such as Power BI.

## Project Layout

```
soar/
  src/soar/
    config.py
    aqs/
      extractors/
        monitors.py
      transformers/
        monitors.py
    loaders/
      filesystem.py
   pipelines/aqs/metadata_run.py
  tests/test_monitors.py
  ops/requirements.txt
```

## Quickstart

1. **Create and activate a virtual environment**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   ```
2. **Install dependencies**
   ```powershell
   pip install -r ops/requirements.txt
   ```
   Optionally install development tooling:
   ```powershell
   pip install pre-commit
   pre-commit install
   ```
3. **Create a `.env` file** in the repository root with:
   ```env
   AQS_EMAIL=your-email
   AQS_KEY=your-api-key
   STATE_CODE=41
   BDATE=2019-01-01
   EDATE=2024-12-31
   DATAREPO_ROOT=C:\Users\abiberi\Oregon\DEQ - Air Data Team - DataRepo\soar
   ```
4. **Run the metadata (monitors) pipeline**
   ```powershell
   set PYTHONPATH=src
   python pipelines/aqs/metadata_run.py
   ```
   
5. **Run the sample-level pipeline**
   ```powershell
   set PYTHONPATH=src
   python pipelines/aqs/aqs_service_run.py
   ```
5. **Execute unit tests and quality checks**
   ```powershell
   ruff check .
   black --check .
   pytest -q
   ```

Additional files
- `.env.template` contains required environment keys and example values; copy to `.env` and fill in secrets.
- `.pre-commit-config.yaml` is provided; run `pre-commit install` to enable local checks.

Outputs write to the `raw/`, `transform/`, and `staged/` subdirectories under the configured `DATAREPO_ROOT` path. Re-run the pipeline to refresh the data as needed.
