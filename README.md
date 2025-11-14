# SOAR AQS Data Pipeline

This repository contains the SOAR (State of Oregon Air Resources) data pipeline for EPA AQS air quality data. This guide will help first-time users download and run the code.

## First Time Setup

### 1. Download the Code

If you're new to GitHub, here's how to get the code:

1. Click the green "Code" button at the top of the GitHub page
2. Copy the HTTPS URL (looks like `https://github.com/OR-Dept-Environmental-Quality/soar.git`)
3. Open PowerShell on your Windows computer
4. Navigate to where you want to store the code:
```powershell
# Example: create and navigate to a code folder in your Documents
cd ~\Documents
mkdir code
cd code

# Clone (download) the repository
git clone https://github.com/OR-Dept-Environmental-Quality/soar.git
cd soar
```

### 2. Install Python

1. Download Python 3.9 or newer (3.10/3.11 recommended) from [python.org](https://www.python.org/downloads/)
2. During installation:
   - ✅ Check "Add Python to PATH"
   - ✅ Check "Install pip"
3. Verify installation in PowerShell:
```powershell
python --version
pip --version
```

### 3. Set Up Python Environment

```powershell
# Create a virtual environment (keeps dependencies isolated)
python -m venv .venv

# Activate it (you'll need to do this each time you open a new terminal)
.\.venv\Scripts\Activate.ps1

# Update core tools
python -m pip install --upgrade pip setuptools wheel

# Install project dependencies
python -m pip install -r .\ops\requirements.txt
```

### 4. Configure Environment Variables

1. Make a copy of the template file and rename it to `.env`:
```powershell
# In PowerShell
Copy-Item env.template .env
```

2. Open `.env` in your text editor and update these required values:
   - BDATE, EDATE: Your desired date range for data extraction
   - DATAREPO_ROOT: Path where you want to store the data (create this folder)
   - STATE_CODE: Your state's FIPS code (41 for Oregon)
   - AQS_EMAIL, AQS_KEY: Your EPA AQS API credentials

The template includes optional performance tuning parameters (commented out by default). You can uncomment and adjust these if needed for your specific use case.

### 5. First-Time Test

Run these commands to verify your setup:

```powershell
# Make sure you're in the project directory with .venv activated
cd path\to\soar
.\.venv\Scripts\Activate.ps1

# Test that dependencies are installed
python -c "import requests, pandas, pyarrow; print('Dependencies OK')"

# Verify AQS credentials (if you have them)
python -c "from src import config; config.set_aqs_credentials(); print('AQS credentials OK')"

# Run the test suite
python -m pytest -v
```

## Running the Pipeline

Once everything is set up, you can run the pipeline components:

```powershell
# Fetch monitor metadata
python -m pipelines.aqs.run_metadata

# Get monitor data
python -m pipelines.aqs.run_monitors

# Get AQS service data
python -m pipelines.aqs.run_aqs_service
```

Or use the installed command-line tools (if you ran `pip install -e .`):
```powershell
soar-run-metadata
soar-run-monitors
soar-run-aqs-service
```

## Common Issues & Solutions

1. **"python not found"**
   - Check if Python is in your PATH
   - Try running `py` instead of `python`
   - Try reopening PowerShell

2. **Import errors after installation**
   - Make sure your virtual environment is activated (you should see `(.venv)` in your prompt)
   - Try reinstalling dependencies: `python -m pip install -r .\ops\requirements.txt`

3. **Environment variable errors**
   - Check that your `.env` file exists in the project root
   - Verify no quotes around values in `.env`
   - Make sure `python-dotenv` is installed

4. **AQS API errors**
   - Verify your AQS_EMAIL and AQS_KEY are correct
   - Check your internet connection
   - The API might have rate limits or be temporarily down

5. **Data folder errors**
   - Create the folder specified in DATAREPO_ROOT
   - Make sure you have write permissions to that location

## Next Steps

1. Read the pipeline module docstrings for detailed parameter info:
```powershell
python -c "import pipelines.aqs.run_monitors; help(pipelines.aqs.run_monitors)"
```

2. Explore the data structure under your DATAREPO_ROOT:
   - `raw/aqs/` - Raw data from AQS API
   - `transform/aqs/` - Processed data
   - `staged/aqs/` - Analytics-ready data

3. Check the AQS parameters file:
   - Open `ops/parameters.csv` to see available parameters

## Getting Help

- For code issues: Open a GitHub issue with error details
- For AQS API questions: Contact EPA AQS support
- For project questions: Email the Oregon DEQ Air Data Team at airdata@deq.state.or.us

## Contributing Back

If you make improvements:

1. Create a new branch for your changes:
```powershell
git checkout -b feature/my-improvement
```

2. Make and test your changes

3. Commit and push:
```powershell
git add .
git commit -m "Description of your changes"
git push -u origin feature/my-improvement
```

4. Open a Pull Request on GitHub