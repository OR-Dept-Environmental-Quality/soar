""" Wood smoke pollutants consolidation pipeline

Filters already staged toxics sample fact table down to the subset of toxics that are relevant to wood smoke, 
and consolidates multiple sources into a single deduplicated fact table.
These include Acrolein, Benzene, Formaldehyde, 1,3-Butadiene, Acetonitrile, Benzo(a)pyrene, Benzo(a)anthracene, Benzo(b)fluoranthene,
Benzo(k)fluoranthene, Chrysene, Dibenzo(a,h)anthracene, Indeno(1,2,3-cd)pyrene.
This covers smoke poluutants from both wood stoves and wildfires.

**Black carbon will be added once it is staged.**

Source: staged/fct_toxics_sample/fct_toxics_sample_{year}.csv (already transformed to include TRV exceedances)
Output: staged/fct_wood_smoke_toxics/fct_wood_smoke_toxics_{year}.csv

""" 
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config

#AQS parameter codes for wood smoke pollutants of interest
_WOOD_SMOKE_PARAM_CODES = [
    "43509", #Acrolein
    "45201", #Benzene
    "43502", #Formaldehyde
    "43218", #1,3-Butadiene
    "43702" #Acetonitrile
    "17242", #Benzo(a)pyrene 
    "17215", #Benzo(a)anthracene
    "17220", #Benzo(b)fluoranthene 
    "17223", #Benzo(k)fluoranthene
    "17208", #Chrysene 
    "17231", #Dibenzo(a,h)anthracene 
    "17243", #Indeno(1,2,3-cd)pyrene 

]


def consolidate_wood_smoke_toxics_for_year(year: str, staged_toxics_dir: Path) -> pd.DataFrame: 
    """Consolidate wood smoke toxics sample data for a specific year.

    Args:
        year: Year to process (e.g., 2023)
        staged_toxics_dir: Directory containing staged toxics sample fact tables
    Returns:
        pd.DataFrame: Consolidated wood smoke toxics sample data for the specified year
        """
    input_file = staged_toxics_dir / f"fct_toxics_sample_{year}.csv" #Path to the staged toxics sample fact table for the specified year
   
    if not input_file.exists(): #if the input file does not exist, print a message and return an empty DataFrame
        print(f"Staged toxics sample file not found for year {year}: {input_file}")
        return pd.DataFrame()
    
    try: 
        #dtype forces parameter code to stay text on read-back, preventing loss of leading zeros
        df = pd.read_csv(input_file, dtype={"parameter_code": str})
    except Exception as e:
        print(f"Error reading staged toxics sample file for year {year}: {e}")
        return pd.DataFrame()
    
    if df.empty:
        print(f"No data found in staged toxics sample file for year {year}: {input_file}")
        return pd.DataFrame()
    
    result = df[df["parameter_code"].isin(_WOOD_SMOKE_PARAM_CODES)].copy() #Filter the DataFrame to only include rows where the parameter_code is in the list of wood smoke toxics parameter codes

    print(f"Consolidated {len(result)} wood smoke toxics records for year {year} from {len(df)} total toxics records")
    return result

def run_consolidation() -> None:
    """Run the wood smoke toxics consolidation pipeline for all years in the configured range."""
    print("Starting Wood Smoke Toxics Consolidation Pipeline")
    staged_toxics_dir = config.ROOT / "staged" / "fct_toxics_sample" 
    if not staged_toxics_dir.exists():
        print(f"Staged toxics sample directory not found: {staged_toxics_dir}")
        print("Please run the toxics sample extraction pipeline first.")
        return
    output_dir = config.ROOT / "staged" / "fct_wood_smoke_toxics"
    output_dir.mkdir(parents=True, exist_ok=True)

    years_processed = 0
    total_records = 0

    for year in range(config.START_YEAR, config.END_YEAR + 1): #loop through each year in the configured range and consolidate wood smoke toxics for that year
        year_str = str(year) #Convert the year to a string
        print(f"\nConsolidating wood smoke toxics for year {year_str}...")

        result = consolidate_wood_smoke_toxics_for_year(year_str, staged_toxics_dir) 
        if result.empty:
            print(f"  No wood smoke toxics data for {year_str}, skipping")
            continue

        output_path = output_dir / f"fct_wood_smoke_toxics_{year_str}.csv"
        result.to_csv(output_path, index=False)
        print(f"  Wrote {len(result)} records to {output_path.name}")

        years_processed += 1
        total_records += len(result)

    print("\nWood Smoke Toxics consolidation complete!")
    print(f"Processed {years_processed} years with {total_records} total records")

if __name__ == "__main__":
    run_consolidation()
