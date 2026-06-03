"""PM2.5 Design Value Consolidation.

Calculates PM2.5 annual and 24-hour design values per EPA 40 CFR Part 50
Appendix N.

Algorithm:
  1. Filter fct_criteria_daily to PM2.5 parameters (88101, 88502).
  2. Priority assignment:
       88101              → priority 1  (FRM/FEM)
       88502, POC != 99   → priority 2  (nephalometer)
       88502, POC == 99   → priority 3  (SensOR)
  3. Resolve event-type duplicates per (site, date, POC, param): keep row with
     highest arithmetic_mean (naturally selects 'Events Included' data).
  4. Priority dedup per (site, date): keep lowest priority, then highest mean.
  5. Wildfire flag per day: arithmetic_mean > 15.0 µg/m³, Jun 1–Oct 25,
     excluding Jul 4 (same logic as fct_aqi_daily pipeline).
  6. Truncate daily mean to 1 decimal place (Appendix N, §4.1).
  7. Per (site, year):
       - Quarterly means (Q1–Q4) and sample counts
       - Annual mean = mean of 4 quarterly means (Appendix N Eq 2)
           → NaN when fewer than 4 quarters have data
       - Creditable samples = count of valid daily values
       - 98th-percentile value using Table 1 rank lookup
       - wildfire_days, exceptional_event_days counts
  8. Rolling 3-year averages:
       dv_annual_3yr_ugm3 = round(mean of annual_mean, 1)
       dv_24hr_3yr_ugm3   = round(mean of p98_ugm3) to nearest integer
       → NaN when any of the 3 years is missing or has a null component.
  9. Flags: meets_annual_naaqs (≤ 9.0 µg/m³), meets_24hr_naaqs (≤ 35 µg/m³).

Input:  staged/fct_criteria_daily/fct_criteria_daily_{year}.csv (2005-present)
Output: staged/fct_pm25_dv/fct_pm25_dv.csv  (all sites, all years, one file)
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config

# PM2.5 NAAQS standards
PM25_ANNUAL_NAAQS_UGM3 = 9.0   # µg/m³
PM25_24HR_NAAQS_UGM3 = 35      # µg/m³

# AQS parameter codes for PM2.5
PM25_PARAMS = {88101, 88502}

# Earliest year with staged criteria daily data
DV_START_YEAR = 2005

# Appendix N Table 1: (max_creditable_samples, rank) pairs — upper bound inclusive
_TABLE1 = [
    (50, 1),
    (100, 2),
    (150, 3),
    (200, 4),
    (250, 5),
    (300, 6),
    (350, 7),
    (366, 8),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _p98_rank(n_creditable: int) -> int:
    """EPA Appendix N Table 1: creditable sample count → 98th-percentile rank.

    The rank is 1-based (i.e., take the nth-largest daily value).
    """
    for upper, rank in _TABLE1:
        if n_creditable <= upper:
            return rank
    return 8  # > 366 → still rank 8 (shouldn't happen in practice)


def _assign_priority(parameter_code: int, poc: int) -> int:
    """Return numeric priority for PM2.5 DV hierarchy.

    Priority 1 (FRM/FEM) > Priority 2 (nephalometer) > Priority 3 (Envista continuous).
    """
    if parameter_code == 88101:
        return 1
    if parameter_code == 88502 and poc != 99:
        return 2
    if parameter_code == 88502 and poc == 99:
        return 3  # 88502, POC=99 = Envista continuous → included as lowest priority
    return 999  # Unknown/invalid → excluded


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

def consolidate_pm25_dv(staged_dir: Path) -> pd.DataFrame:
    """Calculate PM2.5 design values for all sites and years.

    Returns a DataFrame with one row per (site_code, year).
    """
    print("   📂 Loading fct_criteria_daily data...")
    df = _load_criteria_daily(staged_dir, config.END_YEAR)
    if df.empty:
        print("   ❌ No criteria daily data found")
        return pd.DataFrame()

    print(f"   ✓ Loaded {len(df):,} total criteria daily records")

    # Filter to PM2.5 parameters
    df["parameter_code"] = pd.to_numeric(df["parameter_code"], errors="coerce")
    df = df[df["parameter_code"].isin(PM25_PARAMS)].copy()

    if df.empty:
        print("   ❌ No PM2.5 records found")
        return pd.DataFrame()

    print(f"   ✓ {len(df):,} PM2.5 records after filter")

    # Cast columns
    df["date_local"] = pd.to_datetime(df["date_local"])
    df["year"] = df["date_local"].dt.year
    df["arithmetic_mean"] = pd.to_numeric(df["arithmetic_mean"], errors="coerce")
    df["poc"] = pd.to_numeric(df["poc"], errors="coerce").astype(int)
    df["parameter_code"] = pd.to_numeric(df["parameter_code"], errors="coerce").astype(int)

    # Assign priority
    df["_priority"] = df.apply(
        lambda r: _assign_priority(int(r["parameter_code"]), int(r["poc"])), axis=1
    )
    # Drop invalid records (priority 999), but keep Envista (priority 3)
    df = df[df["_priority"] < 999].copy()

    # ------------------------------------------------------------------ #
    # Exceptional-event flag BEFORE deduplication                         #
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
    # For a given (site, date, POC, param), keep row with highest mean.   #
    # ------------------------------------------------------------------ #
    df = (
        df.sort_values("arithmetic_mean", ascending=False)
        .drop_duplicates(subset=["site_code", "date_local", "poc", "parameter_code"])
        .copy()
    )

    # ------------------------------------------------------------------ #
    # Priority dedup: one row per (site, date)                            #
    # ------------------------------------------------------------------ #
    df = df.sort_values(
        ["site_code", "date_local", "_priority", "arithmetic_mean"],
        ascending=[True, True, True, False],
    ).drop_duplicates(subset=["site_code", "date_local"])

    # ------------------------------------------------------------------ #
    # Wildfire flag (same logic as consolidate_aqi_daily.py)              #
    # PM2.5 > 15 µg/m³, Jun 1–Oct 25, excluding Jul 4                    #
    # ------------------------------------------------------------------ #
    _mmdd = df["date_local"].dt.month * 100 + df["date_local"].dt.day
    df["pm25_wildfire_tag"] = (
        (_mmdd >= 601) & (_mmdd <= 1025) & (_mmdd != 704) &
        (df["arithmetic_mean"] > 15.0)
    )

    # Truncate daily mean to 1 decimal place (Appendix N §4.1)
    df["daily_mean_trunc"] = np.floor(df["arithmetic_mean"] * 10) / 10

    df["quarter"] = df["date_local"].dt.quarter

    # ------------------------------------------------------------------ #
    # Annual statistics per (site, year)                                  #
    # ------------------------------------------------------------------ #
    def _annual_stats(grp: pd.DataFrame) -> pd.Series:
        n = len(grp)

        # Quarterly means and sample counts
        q_means = {}
        q_counts = {}
        for q in [1, 2, 3, 4]:
            qvals = grp.loc[grp["quarter"] == q, "daily_mean_trunc"].dropna()
            q_means[q] = float(qvals.mean()) if len(qvals) > 0 else np.nan
            q_counts[q] = len(qvals)

        # Annual mean = mean of quarterly means, only when all 4 quarters present
        q_mean_list = [q_means[q] for q in [1, 2, 3, 4]]
        annual_mean = (
            float(np.mean(q_mean_list))
            if all(not np.isnan(v) for v in q_mean_list)
            else np.nan
        )

        # 98th-percentile value (Table 1 rank lookup)
        creditable = n
        rank = _p98_rank(creditable)
        sorted_vals = (
            grp["daily_mean_trunc"]
            .dropna()
            .sort_values(ascending=False)
            .reset_index(drop=True)
        )
        p98 = float(sorted_vals.iloc[rank - 1]) if len(sorted_vals) >= rank else np.nan

        wildfire_days = int(grp["pm25_wildfire_tag"].sum())

        return pd.Series(
            {
                "q1_mean": q_means[1],
                "q2_mean": q_means[2],
                "q3_mean": q_means[3],
                "q4_mean": q_means[4],
                "q1_samples": q_counts[1],
                "q2_samples": q_counts[2],
                "q3_samples": q_counts[3],
                "q4_samples": q_counts[4],
                "annual_mean_ugm3": annual_mean,
                "creditable_samples": creditable,
                "p98_ugm3": p98,
                "wildfire_days": wildfire_days,
            }
        )

    annual = (
        df.groupby(["site_code", "year"])
        .apply(_annual_stats, include_groups=False)
        .reset_index()
    )

    # Merge exceptional event day counts
    annual = annual.merge(event_days, on=["site_code", "year"], how="left")
    annual["exceptional_event_days"] = (
        annual["exceptional_event_days"].fillna(0).astype(int)
    )

    # ------------------------------------------------------------------ #
    # Rolling 3-year design values                                        #
    # Requires all 3 consecutive years present with non-null components.  #
    # Uses self-joins on (site_code, year) offsets to avoid groupby.     #
    # ------------------------------------------------------------------ #
    annual = annual.sort_values(["site_code", "year"]).reset_index(drop=True)

    _base_ann = annual[["site_code", "year", "annual_mean_ugm3"]]
    _base_p98 = annual[["site_code", "year", "p98_ugm3"]]

    # Shift years so merge on (site_code, year) aligns year-1 and year-2
    _ann_m1 = _base_ann.rename(columns={"annual_mean_ugm3": "_ann_m1"}).assign(year=lambda d: d["year"] + 1)
    _ann_m2 = _base_ann.rename(columns={"annual_mean_ugm3": "_ann_m2"}).assign(year=lambda d: d["year"] + 2)
    _p98_m1 = _base_p98.rename(columns={"p98_ugm3": "_p98_m1"}).assign(year=lambda d: d["year"] + 1)
    _p98_m2 = _base_p98.rename(columns={"p98_ugm3": "_p98_m2"}).assign(year=lambda d: d["year"] + 2)

    annual = (
        annual
        .merge(_ann_m1[["site_code", "year", "_ann_m1"]], on=["site_code", "year"], how="left")
        .merge(_ann_m2[["site_code", "year", "_ann_m2"]], on=["site_code", "year"], how="left")
        .merge(_p98_m1[["site_code", "year", "_p98_m1"]], on=["site_code", "year"], how="left")
        .merge(_p98_m2[["site_code", "year", "_p98_m2"]], on=["site_code", "year"], how="left")
    )

    def _dv_annual(row) -> float:
        vals = [row["annual_mean_ugm3"], row["_ann_m1"], row["_ann_m2"]]
        if any(pd.isna(v) for v in vals):
            return np.nan
        return math.floor(float(np.mean(vals)) * 10 + 0.5) / 10

    def _dv_24hr(row) -> float:
        vals = [row["p98_ugm3"], row["_p98_m1"], row["_p98_m2"]]
        if any(pd.isna(v) for v in vals):
            return np.nan
        return float(math.floor(float(np.mean(vals)) + 0.5))

    annual["dv_annual_3yr_ugm3"] = annual.apply(_dv_annual, axis=1)
    annual["dv_24hr_3yr_ugm3"] = annual.apply(_dv_24hr, axis=1)
    annual = annual.drop(columns=["_ann_m1", "_ann_m2", "_p98_m1", "_p98_m2"])

    # NAAQS compliance flags
    annual["meets_annual_naaqs"] = annual["dv_annual_3yr_ugm3"].apply(
        lambda v: bool(v <= PM25_ANNUAL_NAAQS_UGM3) if not pd.isna(v) else pd.NA
    )
    annual["meets_24hr_naaqs"] = annual["dv_24hr_3yr_ugm3"].apply(
        lambda v: bool(v <= PM25_24HR_NAAQS_UGM3) if not pd.isna(v) else pd.NA
    )

    # Final column order
    col_order = [
        "site_code",
        "year",
        "q1_mean",
        "q2_mean",
        "q3_mean",
        "q4_mean",
        "q1_samples",
        "q2_samples",
        "q3_samples",
        "q4_samples",
        "annual_mean_ugm3",
        "creditable_samples",
        "p98_ugm3",
        "wildfire_days",
        "exceptional_event_days",
        "dv_annual_3yr_ugm3",
        "dv_24hr_3yr_ugm3",
        "meets_annual_naaqs",
        "meets_24hr_naaqs",
    ]
    annual = annual[col_order].sort_values(["site_code", "year"]).reset_index(drop=True)

    print(
        f"   ✅ Calculated PM2.5 DVs: {annual['site_code'].nunique()} sites, "
        f"{annual['year'].nunique()} years, {len(annual):,} rows"
    )
    return annual


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_consolidation() -> None:
    """Run the PM2.5 design value consolidation pipeline."""
    print("🚀 Starting PM2.5 Design Value Consolidation Pipeline")

    staged_criteria_dir = config.ROOT / "staged" / "fct_criteria_daily"
    if not staged_criteria_dir.exists():
        print(f"❌ Staged criteria daily directory not found: {staged_criteria_dir}")
        print("   Please run the criteria daily staging pipeline first.")
        return

    output_dir = config.ROOT / "staged" / "fct_pm25_dv"
    output_dir.mkdir(parents=True, exist_ok=True)

    dv_df = consolidate_pm25_dv(staged_criteria_dir)
    if dv_df.empty:
        print("❌ No PM2.5 DV data produced")
        return

    output_path = output_dir / "fct_pm25_dv.csv"
    dv_df.to_csv(output_path, index=False)
    print(f"✅ Wrote {len(dv_df):,} PM2.5 DV records to {output_path}")
    print()
    print("📁 Output: staged/fct_pm25_dv/fct_pm25_dv.csv")
    print(
        f"   Standards: annual ≤ {PM25_ANNUAL_NAAQS_UGM3} µg/m³, "
        f"24-hr ≤ {PM25_24HR_NAAQS_UGM3} µg/m³ (40 CFR Part 50 Appendix N)"
    )


if __name__ == "__main__":
    run_consolidation()
