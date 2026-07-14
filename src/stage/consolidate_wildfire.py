"""Wildfire smoke staging script.

Produces two staged tables:
  stg_wildfire_aqi_daily.csv      — one row per site per date (all years, all calendar days)
  stg_wildfire_annual_summary.csv — one row per site per year (wildfire season only)

Inputs (staged layer):
  fct_aqi_daily/fct_aqi_daily_{year}.csv         PM2.5/ozone AQI, hierarchy-resolved
  fct_criteria_daily/fct_criteria_daily_{year}.csv  raw PM2.5 concentrations (AQS only)
  dim_sites/dim_sites.csv                          site metadata with region

PM2.5 concentration is retrieved by joining fct_criteria_daily on site_code + date_local + poc,
where poc matches the hierarchy-resolved pm25_poc already stored in fct_aqi_daily. POC=99
(Envista) rows are excluded from the criteria join (AQS-only per current pipeline scope).

Wildfire season: June 1 – October 25, excluding July 4 (fireworks artifact).
Heuristic threshold: arithmetic_mean > 15.0 µg/m³ (DEQ methodology).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
ALL_YEARS = range(2005, 2026)
SEASON_START_MMDD = 601   # June 1  (month * 100 + day)
SEASON_END_MMDD   = 1025  # October 25
SEASON_EXCLUDE_MMDD = [704]  # July 4 — fireworks artifact
WF_PM25_THRESHOLD = 15.0  # µg/m³  DEQ heuristic wildfire day threshold
EPOCH_YEARS = [2015, 2018, 2025]  # Reference years for monitor network epoch flags

# PM2.5 AQI category breakpoints — 2024 EPA standard; labels match dimAQI.csv
_PM25_AQI_BINS = [0, 50, 100, 150, 200, 300, 999]
_PM25_AQI_LABELS = [
    "GOOD",
    "MODERATE",
    "UNHEALTHY FOR SENSITIVE",
    "UNHEALTHY",
    "VERY UNHEALTHY",
    "HAZARDOUS",
]
# ───────────────────────────────────────────────────────────────────────────────


def _pm25_aqi_category(series: pd.Series) -> pd.Series:
    """Map a PM2.5 AQI Series to category labels (null-safe)."""
    return pd.cut(
        series,
        bins=_PM25_AQI_BINS,
        labels=_PM25_AQI_LABELS,
        right=True,
        include_lowest=True,
    ).astype(object).where(series.notna(), other=None)


def _wildfire_season_mask(dates: pd.Series) -> pd.Series:
    """Return boolean mask: True if date falls within wildfire season.
    
    Before 2020: July 1 – Sept 30 (0701–0930)
    2020 and after: June 1 – Oct 25 (0601–1025)
    """
    mmdd = dates.dt.month * 100 + dates.dt.day
    year = dates.dt.year
    excluded = mmdd.isin(SEASON_EXCLUDE_MMDD)
    
    # Apply different date windows based on year
    before_2020 = (mmdd >= 701) & (mmdd <= 930)
    from_2020 = (mmdd >= 601) & (mmdd <= 1025)
    in_window = (year < 2020) & before_2020 | (year >= 2020) & from_2020
    
    return in_window & ~excluded


# ── LOADERS ────────────────────────────────────────────────────────────────────

def load_fct_aqi_daily(staged_dir: Path) -> pd.DataFrame:
    """Load all fct_aqi_daily files and concatenate into one DataFrame."""
    aqi_dir = staged_dir / "fct_aqi_daily"
    files = sorted(aqi_dir.glob("fct_aqi_daily_*.csv"))
    if not files:
        raise FileNotFoundError(f"No fct_aqi_daily files found in {aqi_dir}")

    dfs = [pd.read_csv(f, parse_dates=["date_local"]) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    # Ensure site_code is string and poc is numeric (nullable)
    df["site_code"] = df["site_code"].astype(str)
    df["pm25_poc"] = pd.to_numeric(df["pm25_poc"], errors="coerce")

    print(f"   ✅ fct_aqi_daily: {len(df):,} rows ({len(files)} files)")
    return df


def load_fct_criteria_pm25(staged_dir: Path) -> pd.DataFrame:
    """Load PM2.5 rows from fct_criteria_daily (AQS only — excludes POC=99/Envista)."""
    crit_dir = staged_dir / "fct_criteria_daily"
    files = sorted(crit_dir.glob("fct_criteria_daily_*.csv"))
    if not files:
        raise FileNotFoundError(f"No fct_criteria_daily files found in {crit_dir}")

    keep_cols = ["site_code", "date_local", "poc", "arithmetic_mean"]
    dfs = []
    for f in files:
        df = pd.read_csv(f, parse_dates=["date_local"])
        mask = df["parameter_code"].isin([88101, 88502]) & (df["poc"] != 99)
        dfs.append(df.loc[mask, keep_cols])

    df = pd.concat(dfs, ignore_index=True)
    df["site_code"] = df["site_code"].astype(str)
    df["poc"] = pd.to_numeric(df["poc"], errors="coerce")

    print(f"   ✅ fct_criteria_daily (PM2.5, AQS only): {len(df):,} rows ({len(files)} files)")
    return df


def load_dim_sites(staged_dir: Path) -> pd.DataFrame:
    """Load dim_sites, compute epoch flags, normalize column names."""
    sites_path = staged_dir / "dim_sites" / "dim_sites.csv"
    if not sites_path.exists():
        raise FileNotFoundError(f"dim_sites not found: {sites_path}")

    df = pd.read_csv(sites_path, parse_dates=["open_date", "close_date"])
    df["site_code"] = df["site_code"].astype(str)

    # Compute epoch flags: site was open at any point during the reference year
    for yr in EPOCH_YEARS:
        yr_start = pd.Timestamp(f"{yr}-01-01")
        yr_end   = pd.Timestamp(f"{yr}-12-31")
        open_ok  = df["open_date"].isna()  | (df["open_date"]  <= yr_end)
        close_ok = df["close_date"].isna() | (df["close_date"] >= yr_start)
        df[f"epoch_{yr}_active"] = open_ok & close_ok

    # Normalize Region → region (spatial join produces capital R)
    if "Region" in df.columns:
        df = df.rename(columns={"Region": "region"})

    meta_cols = (
        ["site_code", "local_site_name", "county_name", "region", "latitude", "longitude"]
        + [f"epoch_{yr}_active" for yr in EPOCH_YEARS]
    )
    available = [c for c in meta_cols if c in df.columns]
    df = df[available].copy()

    print(f"   ✅ dim_sites: {len(df):,} sites")
    return df


# ── BUILD TABLES ───────────────────────────────────────────────────────────────

def build_daily_table(
    aqi: pd.DataFrame,
    criteria: pd.DataFrame,
    sites: pd.DataFrame,
) -> pd.DataFrame:
    """Build stg_wildfire_aqi_daily: one row per site per date."""

    # Join PM2.5 concentration via the hierarchy-resolved POC in fct_aqi_daily
    daily = aqi.merge(
        criteria.rename(columns={"arithmetic_mean": "pm25_concentration_mean", "poc": "_poc_criteria"}),
        left_on=["site_code", "date_local", "pm25_poc"],
        right_on=["site_code", "date_local", "_poc_criteria"],
        how="left",
    ).drop(columns=["_poc_criteria"], errors="ignore")
    daily = daily.dropna(subset=["pm25_concentration_mean"])

    # Date features
    daily["year"]       = daily["date_local"].dt.year
    daily["month"]      = daily["date_local"].dt.month
    daily["day_of_year"] = daily["date_local"].dt.day_of_year

    # Wildfire season flag (vectorized)
    daily["is_wildfire_season"] = _wildfire_season_mask(daily["date_local"])

    # PM2.5 AQI category derived from pm25_aqi using 2024 EPA breakpoints
    daily["pm25_aqi_category"] = _pm25_aqi_category(daily["pm25_aqi"])

    # Wildfire flags
    daily["is_wf_heuristic"] = (
        daily["is_wildfire_season"]
        & daily["pm25_concentration_mean"].notna()
        & (daily["pm25_concentration_mean"] > WF_PM25_THRESHOLD)
    )
    event_type_normalized = daily["event_type"].fillna("No Events")
    daily["is_wf_event_type"]       = event_type_normalized != "No Events"
    daily["is_concurred_exclusion"] = event_type_normalized == "Concurred Events Excluded"

    # Join site metadata
    daily = daily.merge(sites, on="site_code", how="left")

    # Final column order
    epoch_cols = [f"epoch_{yr}_active" for yr in EPOCH_YEARS]
    final_cols = [
        "site_code", "date_local", "year", "month", "day_of_year",
        "is_wildfire_season",
        "pm25_aqi", "pm25_aqi_category",
        "pm25_concentration_mean",
        "pm25_poc", "pm25_validity_indicator",
        "event_type",
        "is_wf_heuristic", "is_wf_event_type", "is_concurred_exclusion",
        *epoch_cols,
        "local_site_name", "county_name", "region", "latitude", "longitude",
    ]
    available = [c for c in final_cols if c in daily.columns]
    return daily[available].reset_index(drop=True)


def build_annual_summary(daily: pd.DataFrame) -> pd.DataFrame:
    """Build stg_wildfire_annual_summary: one row per site per year (wildfire season only)."""
    season = daily[daily["is_wildfire_season"] & daily["pm25_aqi"].notna()].copy()

    if season.empty:
        print("   ⚠️  No wildfire-season PM2.5 data found — annual summary will be empty")
        return pd.DataFrame()

    grp = season.groupby(["site_code", "year"])

    summary = grp.agg(
        n_days_sampled           =("pm25_aqi", "count"),
        n_days_ge_usg            =("pm25_aqi", lambda x: (x >= 101).sum()),
        n_days_unhealthy         =("pm25_aqi", lambda x: ((x >= 151) & (x <= 200)).sum()),
        n_days_very_unhealthy    =("pm25_aqi", lambda x: ((x >= 201) & (x <= 300)).sum()),
        n_days_hazardous         =("pm25_aqi", lambda x: (x >= 301).sum()),
        n_days_wf_heuristic      =("is_wf_heuristic", "sum"),
        n_days_wf_event_type     =("is_wf_event_type", "sum"),
        n_days_concurred_exclusion=("is_concurred_exclusion", "sum"),
        max_pm25_aqi             =("pm25_aqi", "max"),
    ).reset_index()

    summary["pct_days_ge_usg"] = (
        summary["n_days_ge_usg"] / summary["n_days_sampled"]
    ).round(4)

    # Date of peak PM2.5 AQI per site-year (first occurrence if tied)
    peak = (
        season.sort_values(["site_code", "year", "pm25_aqi"], ascending=[True, True, False])
        .groupby(["site_code", "year"])
        .first()[["date_local"]]
        .rename(columns={"date_local": "max_pm25_aqi_date"})
        .reset_index()
    )
    summary = summary.merge(peak, on=["site_code", "year"], how="left")

    # Statewide count of sites with PM2.5 data per season-year (denominator context)
    sites_per_year = (
        season.groupby("year")["site_code"]
        .nunique()
        .rename("n_sites_with_data")
        .reset_index()
    )
    summary = summary.merge(sites_per_year, on="year", how="left")

    # Join static site metadata (one row per site)
    epoch_cols = [f"epoch_{yr}_active" for yr in EPOCH_YEARS]
    meta_cols  = ["site_code", "local_site_name", "county_name", "region", *epoch_cols]
    meta = daily[[c for c in meta_cols if c in daily.columns]].drop_duplicates("site_code")
    summary = summary.merge(meta, on="site_code", how="left")

    # Final column order
    ordered = [
        "site_code", "year",
        "local_site_name", "county_name", "region",
        *epoch_cols,
        "n_days_sampled",
        "n_days_ge_usg", "n_days_unhealthy", "n_days_very_unhealthy", "n_days_hazardous",
        "pct_days_ge_usg",
        "n_days_wf_heuristic", "n_days_wf_event_type", "n_days_concurred_exclusion",
        "max_pm25_aqi", "max_pm25_aqi_date",
        "n_sites_with_data",
    ]
    available = [c for c in ordered if c in summary.columns]
    return summary[available].sort_values(["year", "site_code"]).reset_index(drop=True)


# ── RUNNER ─────────────────────────────────────────────────────────────────────

def run_consolidation() -> None:
    """Run the wildfire smoke staging pipeline."""
    print("🚀 Starting Wildfire Staging Pipeline")

    staged_dir = config.ROOT / "staged"
    output_dir = staged_dir / "wildfire"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load inputs
    print("\n📖 Loading inputs...")
    aqi      = load_fct_aqi_daily(staged_dir)
    criteria = load_fct_criteria_pm25(staged_dir)
    sites    = load_dim_sites(staged_dir)

    # Build daily table
    print("\n🔄 Building stg_wildfire_aqi_daily...")
    daily = build_daily_table(aqi, criteria, sites)
    n_sites = daily["site_code"].nunique()
    n_years = daily["year"].nunique() if "year" in daily.columns else "?"
    print(f"   📊 {len(daily):,} rows — {n_sites} sites, {n_years} years")

    # Build annual summary
    print("\n🔄 Building stg_wildfire_annual_summary...")
    annual = build_annual_summary(daily)
    print(f"   📊 {len(annual):,} site-year rows")

    # Write outputs
    daily_path  = output_dir / "stg_wildfire_aqi_daily.csv"
    annual_path = output_dir / "stg_wildfire_annual_summary.csv"
    daily.to_csv(daily_path, index=False)
    annual.to_csv(annual_path, index=False)

    print(f"\n✅ stg_wildfire_aqi_daily      → {daily_path}")
    print(f"✅ stg_wildfire_annual_summary → {annual_path}")
    print("\n🎉 Wildfire staging complete!")


if __name__ == "__main__":
    run_consolidation()
