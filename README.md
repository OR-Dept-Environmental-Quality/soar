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
  pipelines/aqs/monitors_run.py
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
3. **Create a `.env` file** in the repository root with:
   ```env
   AQS_EMAIL=your-email
   AQS_KEY=your-api-key
   STATE_CODE=41
   BDATE=2019-01-01
   EDATE=2024-12-31
   DATAREPO_ROOT=C:\Users\abiberi\Oregon\DEQ - Air Data Team - DataRepo\soar
   ```
4. **Run the monitors pipeline**
   ```powershell
   set PYTHONPATH=src
   python pipelines/aqs/monitors_run.py
   ```
5. **Execute unit tests and quality checks**
   ```powershell
   ruff check .
   black --check .
   pytest -q
   ```

Outputs write to the `raw/`, `transform/`, and `staged/` subdirectories under the configured `DATAREPO_ROOT` path. Re-run the pipeline to refresh the data as needed.
