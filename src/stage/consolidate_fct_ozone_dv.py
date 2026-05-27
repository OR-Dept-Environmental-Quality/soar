"""Ozone Design Value Consolidation.

Calculates 8-hour ozone design values per EPA 40 CFR Part 50 Appendix P.

Algorithm:
  1. Filter fct_criteria_daily to ozone (44201), 8-hour duration codes ('W', 'Z').
  2. Resolve event-type duplicates per (site, date, POC): keep row with highest
     first_max_value (naturally selects 'Events Included' records).
  3. Collapse across POCs: daily site max = max(first_max_value) per (site, date).
  4. Per (site, year): count valid days, tag exceptional-event days, extract
     1st-5th highest daily max, truncated to 3 decimal places per Appendix P.
  5. Rolling 3-year average of the 4th-highest value = design value, also
     truncated to 3 decimal places.
  6. Flag: meets_ozone_naaqs = (dv_3yr_avg_ppm <= 0.070).

Input:  staged/fct_criteria_daily/fct_criteria_daily_{year}.csv (2005-present)
Output: staged/fct_ozone_dv/fct_ozone_dv.csv  (all sites, all years, one file)
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config

# Ozone NAAQS standard (ppm)
OZONE_NAAQS_PPM = 0.070

# AQS parameter code for ozone
OZONE_PARAM = 44201

# Valid 8-hour averaging period codes ('W' = 8-hr running, 'Z' = 8-hr block)
OZONE_DURATION_CODES = {"W", "Z"}

# Earliest year with staged criteria daily data
DV_START_YEAR = 2005


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _truncate_3dp(val: float) -> float:
    """Truncate (do not round) to 3 decimal places, per Appendix P."""
    if pd.isna(val):
        return np.nan
    return int(val * 1000) / 1000


def _load_criteria_daily(staged_dir: Path, end_year: int) -> pd.DataFrame:
    """Load all fct_criteria_daily files from DV_START_YEAR through end_year."""
    frames = []
    for year in range(DV_START_YEAR, end_year + 1):
        f = staged_dir / f"fct_criteria_daily_{year}.csv"
        if not f.exists():
            print(f"   ⚠️  {f.name} not found, skipping")
            continue
        df = pd.read_csv(f)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Core calculation
# ---------------------------------------------------------------------------

def consolidate_ozone_dv(staged_dir: Path) -> pd.DataFrame:
    """Calculate ozone design values for all sites and years.

    Returns a DataFrame with one row per (site_code, year) containing
    annual top-5 daily maxima and the 3-year rolling design value.
    """
    print("   📂 Loading fct_criteria_daily data...")
    df = _load_criteria_daily(staged_dir, config.END_YEAR)
    if df.empty:
        print("   ❌ No criteria daily data found")
        return pd.DataFrame()

    print(f"   ✓ Loaded {len(df):,} total criteria daily records")

    # Filter to ozone 8-hour records
    df = df[
        (df["parameter_code"].astype(str) == str(OZONE_PARAM)) &
        (df["sample_duration_code"].isin(OZONE_DURATION_CODES))
    ].copy()

    if df.empty:
        print("   ❌ No ozone 8-hour records found")
        return pd.DataFrame()

    print(f"   ✓ {len(df):,} ozone 8-hour records after filter")

    # Parse / cast
    df["date_local"] = pd.to_datetime(df["date_local"])
    df["year"] = df["date_local"].dt.year
    df["first_max_value"] = pd.to_numeric(df["first_max_value"], errors="coerce")
    df["poc"] = pd.to_numeric(df["poc"], errors="coerce").fillna(0).astype(int)

    # ------------------------------------------------------------------ #
    # Exceptional-event flag BEFORE deduplication                         #
    # Count distinct dates per (site, year) that were affected by events. #
    # ------------------------------------------------------------------ #
    event_days = (
        df[df["event_type"] != "No Events"]
        .groupby(["site_code", "year"])["date_local"]
        .nunique()
        .reset_index()
        .rename(columns={"date_local": "exceptional_event_days"})
    )

    # ------------------------------------------------------------------ #
    # Resolve event-type duplicates                                        #
    # For a given (site, date, POC), multiple rows can exist for different #
    # event_type values.  Keep the row with the highest first_max_value — #
    # this naturally selects the 'Events Included' version of the data.   #
    # ------------------------------------------------------------------ #
    df = (
        df.sort_values("first_max_value", ascending=False)
        .drop_duplicates(subset=["site_code", "date_local", "poc"])
        .copy()
    )

    # Daily site maximum: collapse across POCs
    daily = (
        df.groupby(["site_code", "date_local", "year"], as_index=False)
        ["first_max_value"]
        .max()
        .rename(columns={"first_max_value": "daily_max_8hr_ppm"})
    )

    # ------------------------------------------------------------------ #
    # Annual statistics per (site, year)                                  #
    # ------------------------------------------------------------------ #
    def _annual_stats(grp: pd.DataFrame) -> pd.Series:
        vals = (
            grp["daily_max_8hr_ppm"]
            .dropna()
            .sort_values(ascending=False)
            .reset_index(drop=True)
        )
        n = len(vals)

        def _get(rank_0: int) -> float:
            """Return truncated value at 0-based rank, or NaN if not enough data."""
            return _truncate_3dp(float(vals.iloc[rank_0])) if rank_0 < n else np.nan

        return pd.Series(
            {
                "valid_days": n,
                "first_max_8hr_ppm": _get(0),
                "second_max_8hr_ppm": _get(1),
                "third_max_8hr_ppm": _get(2),
                "fourth_max_8hr_ppm": _get(3),
                "fifth_max_8hr_ppm": _get(4),
            }
        )

    annual = (
        daily.groupby(["site_code", "year"])
        .apply(_annual_stats, include_groups=False)
        .reset_index()
    )

    # Merge exceptional event day counts
    annual = annual.merge(event_days, on=["site_code", "year"], how="left")
    annual["exceptional_event_days"] = (
        annual["exceptional_event_days"].fillna(0).astype(int)
    )

    # ------------------------------------------------------------------ #
    # Rolling 3-year design value                                         #
    # DV = truncated mean of fourth_max for year-2, year-1, year.        #
    # Requires all 3 years to be present with non-null fourth_max.       #
    # Uses self-joins on (site_code, year) offsets to avoid groupby.     #
    # ------------------------------------------------------------------ #
    annual = annual.sort_values(["site_code", "year"]).reset_index(drop=True)

    _base = annual[["site_code", "year", "fourth_max_8hr_ppm"]]

    # Shift years so that a merge on (site_code, year) aligns year-1 and year-2
    _y1 = _base.rename(columns={"fourth_max_8hr_ppm": "_fourth_m1"})
    _y1 = _y1.assign(year=_y1["year"] + 1)  # y1.year+1 == current year

    _y2 = _base.rename(columns={"fourth_max_8hr_ppm": "_fourth_m2"})
    _y2 = _y2.assign(year=_y2["year"] + 2)  # y2.year+2 == current year

    annual = (
        annual
        .merge(_y1[["site_code", "year", "_fourth_m1"]], on=["site_code", "year"], how="left")
        .merge(_y2[["site_code", "year", "_fourth_m2"]], on=["site_code", "year"], how="left")
    )

    def _dv(row) -> float:
        vals = [row["fourth_max_8hr_ppm"], row["_fourth_m1"], row["_fourth_m2"]]
        if any(pd.isna(v) for v in vals):
            return np.nan
        return _truncate_3dp(float(np.mean(vals)))

    annual["dv_3yr_avg_ppm"] = annual.apply(_dv, axis=1)
    annual = annual.drop(columns=["_fourth_m1", "_fourth_m2"])

    # NAAQS compliance flag
    annual["meets_ozone_naaqs"] = annual["dv_3yr_avg_ppm"].apply(
        lambda v: bool(v <= OZONE_NAAQS_PPM) if not pd.isna(v) else pd.NA
    )

    # Final column order
    col_order = [
        "site_code",
        "year",
        "valid_days",
        "exceptional_event_days",
        "first_max_8hr_ppm",
        "second_max_8hr_ppm",
        "third_max_8hr_ppm",
        "fourth_max_8hr_ppm",
        "fifth_max_8hr_ppm",
        "dv_3yr_avg_ppm",
        "meets_ozone_naaqs",
    ]
    annual = annual[col_order].sort_values(["site_code", "year"]).reset_index(drop=True)

    print(
        f"   ✅ Calculated ozone DVs: {annual['site_code'].nunique()} sites, "
        f"{annual['year'].nunique()} years, {len(annual):,} rows"
    )
    return annual


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_consolidation() -> None:
    """Run the ozone design value consolidation pipeline."""
    print("🚀 Starting Ozone Design Value Consolidation Pipeline")

    staged_criteria_dir = config.ROOT / "staged" / "fct_criteria_daily"
    if not staged_criteria_dir.exists():
        print(f"❌ Staged criteria daily directory not found: {staged_criteria_dir}")
        print("   Please run the criteria daily staging pipeline first.")
        return

    output_dir = config.ROOT / "staged" / "fct_ozone_dv"
    output_dir.mkdir(parents=True, exist_ok=True)

    dv_df = consolidate_ozone_dv(staged_criteria_dir)
    if dv_df.empty:
        print("❌ No ozone DV data produced")
        return

    output_path = output_dir / "fct_ozone_dv.csv"
    dv_df.to_csv(output_path, index=False)
    print(f"✅ Wrote {len(dv_df):,} ozone DV records to {output_path}")
    print()
    print("📁 Output: staged/fct_ozone_dv/fct_ozone_dv.csv")
    print(f"   Standard: {OZONE_NAAQS_PPM} ppm (40 CFR Part 50 Appendix P)")


if __name__ == "__main__":
    run_consolidation()
