""" Stage raw air quality advisory logs into fct_air_aquality_advisories.

Source: {ADVISORY_SOURCE_DIR}/{year}/{year}.advisory.tracking.csv

Output: stages/fct_air_quality_advisories/fct_air_quality_advisories_{year}.csv

"""

from __future__ import annotations

import re
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import config

_STANDARD_COLUMNS = [
    'start_date','end_date','pollutant_source', 'county',
    'issued_advisory', 'fire_names','comments'
]

_OUTPUT_COLUMNS = ['site_code'] + _STANDARD_COLUMNS

_COLUMN_MAP_BASE = {
    'Start Date': 'start_date',
    'End Date': 'end_date',
    'Pollutant Source': 'pollutant_source',
    'Issued Advisory': 'issued_advisory',
    'Name of Fire (State)': 'fire_names',
    'Comments': 'comments'
}

_COUNTY_COLUMN_CANDIDATES = ["Impacted", "Impacted Area", "Impacted Areas"]

_COLUMN_MAP_2022 = {
    'Call Date': 'start_date',
    'Impacted Areas': 'county',
    'Advisory/Watch Duration': 'comments',
    'Fire': 'fire_names'
}

_YES_NO_COLS = ['issued_advisory']
_DATE_COLS = ['start_date', 'end_date']

_SINGLE_DATE_PATTERN = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
_DATE_RANGE_PATTERN = re.compile(r"^\d{1,2}/\d{1,2}(?:/\d{2,4})?\s*-\s*(\d{1,2}/\d{1,2}/\d{2,4})$")

def _parse_2022_end_date(raw:str) -> str | None:
    """Return an end date parse from a 2022 Advisory/Watch Duration value, if it strictly is a date or a "date- date"range.
    Anything else returns None. """

    raw = str(raw).strip()
    if _SINGLE_DATE_PATTERN.match(raw):
        return raw
    m = _DATE_RANGE_PATTERN.match(raw)
    return m.group(1) if m else None

_DIRECTION_PATTERN = re.compile(r"^(N|S|E|W|North|South|East|West|Northern|Southern|Eastern|Western)\s+(.+)$", re.IGNORECASE)
_DIRECTION_NORMALIZE = {
    "n": "North", "s": "South", "e": "East", "w": "West",
    "north": "North", "south": "South", "east": "East", "west": "West",
    "northern": "North", "southern":"South", "eastern":"East", "western": "West"
}
_TRAILING_PAREN_PATTERN = re.compile(r"\s*\([^)]*\)\s*$")

def _find_county_column(columns) -> str | None:
    for candidate in _COUNTY_COLUMN_CANDIDATES:
        if candidate in columns:
            return candidate
    return None

def _find_source_file(year_dir: Path, year: str) -> Path | None:
    csv_path = year_dir / f"{year}.advisory.tracking.csv"
    if csv_path.exists():
        return csv_path

    xlsx_path = year_dir / f"{year}.advisory.tracking.xlsx"
    if xlsx_path.exists():
        return xlsx_path

    return None

def _read_source(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".xlsx":
        return pd.read_excel(path)
    return pd.read_csv(path)

_AND_SPLIT_PATTERN = re.compile(r"\s+and\s+", re.IGNORECASE)

def _split_top_level_commas(raw: str) -> list[str]:
    """Split a string on commas and the word "and", ignoring commas inside parentheses."""
    tokens = []
    depth = 0
    current = []
    i = 0
    n = len(raw)
    while i < n:
        ch=raw[i]
        if ch == "(":
            depth +=1 
            current.append(ch)
            i += 1 
        elif ch == ")":
            depth = max(0, depth - 1)
            current.append(ch)
            i += 1
        elif ch == "," and depth == 0:
            tokens.append("".join(current))
            current = []
            i += 1
        elif depth == 0 and (m := _AND_SPLIT_PATTERN.match(raw, i)):
            tokens.append("".join(current))
            current = []
            i = m.end()
        else:
            current.append(ch)
            i += 1
    if current:
        tokens.append("".join(current))
    return [t.strip() for t in tokens if t.strip()]

_TRALING_COUNTY_WORD_PATTERN = re.compile(r"\s+counties?$", re.IGNORECASE)

def _parse_county_field(raw: str) -> tuple[str, str | None]:
    """ Split single county token into (base_county, direction).
    Direction is None for plain county name. 
    """
    raw = str(raw).strip()
    m = _DIRECTION_PATTERN.match(raw)
    if not m:
        base = raw
        direction = None
    else:
        direction_raw, base = m.groups()
        direction = _DIRECTION_NORMALIZE[direction_raw.lower()]
    base = _TRAILING_PAREN_PATTERN.sub("", base).strip()
    base = _TRALING_COUNTY_WORD_PATTERN.sub(" ", base).strip()
    return base, direction

def load_dim_sites() -> pd.DataFrame:
    sites_path = config.ROOT / "staged" / "dim_sites" / "dim_sites.csv"
    if not sites_path.exists():
        raise FileNotFoundError(f"dim_sites not found: {sites_path}")

    df = pd.read_csv(sites_path, dtype={"site_code": str})
    keep_cols = [c for c in ['site_code', 'local_site_name', 'county_name', 'Region', 'city_name'] if c in df.columns]
    return df[keep_cols].copy()

_REGION_ALIASES = {
    'portland vancouver' : 'portland metro',
}

_PORTLAND_EXCLUSION_WORDS = [
    'downwind','upwind','near','toward','towards','from','away'
]

_PORTLAND_MAX_LEN = 25

def _is_portland(value:str) -> bool:
    """True if a value is short and simple enough to safely treat as reffering to the Portland Matro area."""
    lowered = value.casefold()
    if "portland" not in lowered:
        return False
    if len(value)> _PORTLAND_MAX_LEN:
        return False
    return not any(word in lowered for word in _PORTLAND_EXCLUSION_WORDS)

def _match_leading_county(base_county: str, sites: pd.DataFrame)-> pd.DataFrame:
    """Last-resort match for narrative sentances that open with county name"""
    lowered = base_county.casefold()
    for county_name in sites["county_name"].dropna().unique():
        prefix = county_name.casefold()
        if lowered == prefix:
            continue
        if lowered.startswith(prefix + " ") or lowered.startswith(prefix + ")"):
            return sites[sites["county_name"].str.casefold() == prefix]
    return sites.iloc[0:0]

_CITY_ALIASES = {
    "salem_city": ["Salem"],
    "Salem-Silverton": ["Salem", "Silverton"],
    "Eugene_pm10_maint_area": ["Eugene"],
    "burns_city": ["Burns"],
    "klamathfalls_city": ["Klamath Falls"],
    "KlamathFalls_pm10_maint_area": ["Klamath Falls"],
    "MedfordAshland_pm10_maint_area": ["Medford", "Ashland"],
}

def _match_city_alias(base_county: str, sites: pd.DataFrame)-> pd.DataFrame:
    cities = _CITY_ALIASES.get(base_county)
    if not cities or "city_name" not in sites.columns:
        return sites.iloc[0:0]
    lowered_cities = {c.strip().casefold() for c in cities}
    return sites[sites["city_name"].str.strip().str.casefold().isin(lowered_cities)]

_COMMENT_DIRECTION = re.compile(
    r"\b(N|S|E|W|North|South|East|West|Northern|Southern|Eastern|Western)\s+([A-Z][a-zA-Z]+)\b",
    re.IGNORECASE,
)

def _find_directional_comment(comments, known_counties: set) -> list[tuple[str, str]]:
    """ Scan comments to find directional information if available. Lokks for "[Direction] [County]" where County matches county in DimSites.""" 
    if not isinstance(comments, str):
        return[]
    found = []
    for match in _COMMENT_DIRECTION.finditer(comments):
        direction_raw, county_raw = match.groups()
        direction = _DIRECTION_NORMALIZE.get(direction_raw.casefold())
        if not direction:
            continue
        for county in known_counties:
            if county.casefold() == county_raw.casefold():
                found.append((direction, county))
                break
    return found

def _find_city_comment(comments, known_cities: set) -> list[str]:
    """ Scan comments to find cities."""
    if not isinstance(comments, str):
        return[]
    found = []
    for piece in comments.split(","):
        piece = piece.strip()
        for city in known_cities:
            if city.casefold() == piece.casefold():
                found.append(city)
                break
    return found

def _normalize_region(value: str) -> str:
    normalized = value.casefold().replace("-", " ").replace("_"," ")
    normalized = re.sub(r"\s+"," ", normalized).strip()
    if normalized.endswith(" area"):
        normalized = normalized[: -len("area")].strip()
    return _REGION_ALIASES.get(normalized, normalized)


def load_direction_crosswalk() -> pd.DataFrame:
    path = ROOT / 'ops' / 'advisory_direction_crosswalk.csv'
    if not path.exists():
        print(f"Advisory direction crosswalk not found: {path}")
        return pd.DataFrame(columns=['county_name', 'direction', 'local_site_name'])
    return pd.read_csv(path)

def attach_site_codes(df: pd.DataFrame, sites: pd.DataFrame, crosswalk: pd.DataFrame) -> pd.DataFrame:
    unresolved_directional = set() 
    unmatched_counties = set()
    blank_county_rows = 0
    out_rows = []

    for row in df.to_dict("records"):
            county_raw = row['county']
            if not isinstance(county_raw, str) or not county_raw.strip():
                blank_county_rows += 1
                continue

            site_to_county: dict[str, str] = {}

            for token in _split_top_level_commas(county_raw):
                base_county, direction = _parse_county_field(token)

                if direction is None:
                    matches = sites[sites['county_name'].str.casefold() == base_county.casefold()] # checks for a county level match
                    if matches.empty and "city_name" in sites.columns:                             # checks for a city level match
                        matches = sites[sites["city_name"].str.casefold() == base_county.casefold()]
                    if matches.empty:
                        matches = _match_city_alias(base_county, sites)                             # checks for city alias match 
                    if matches.empty and "Region" in sites.columns:                                 # checks for a region level match 
                        normalized = _normalize_region(base_county)
                        region_norm = sites['Region'].fillna("").apply(_normalize_region)
                        matches = sites[region_norm == normalized]
                    if matches.empty and "Region"  in sites.columns and _is_portland(base_county):  # checks for Portland metro 
                        region_norm = sites["Region"].fillna("").apply(_normalize_region)
                        matches = sites[region_norm == "portland metro"]
                    if matches.empty:                                                                  # checks for county in sentance strings
                        matches = _match_leading_county(base_county, sites)
                    if matches.empty:
                        unmatched_counties.add(base_county)
                        continue
                    display_direction = None
                else:
                    cw_matches = crosswalk[                                                #If none of the above, moves onto the crosswalk for directional 
                        (crosswalk['county'].str.casefold() == base_county.casefold())
                        & (crosswalk['direction'].str.casefold() == direction.casefold())
                    ]
                    if cw_matches.empty:
                        unresolved_directional.add((direction, base_county))
                        continue
                    matches = sites[sites['local_site_name'].isin(cw_matches['local_site_name'])]
                    if matches.empty:
                        unresolved_directional.add((direction, base_county))
                        continue
                    display_direction = direction

                for site_code, county_name in zip(matches['site_code'], matches['county_name']):
                    display_county = f'{display_direction} {county_name}' if display_direction else county_name
                    site_to_county.setdefault(site_code, display_county)

                known_counties = set(sites["county_name"].dropna().unique())
                comment_directions = _find_directional_comment(row.get("comments"), known_counties)
                directional_counties_used = {county for _, county in comment_directions}

                for county in directional_counties_used:
                    site_to_county = {
                        sc: dc for sc, dc in site_to_county.items() if dc.casefold() != county.casefold()
                    }

                for direction, county in comment_directions:
                    cw_matches = crosswalk[
                        (crosswalk["county"].str.casefold() == county.casefold())
                        & (crosswalk["direction"].str.casefold() == direction.casefold())
                    ]
                    if cw_matches.empty:
                        unresolved_directional.add((direction, county))
                        continue
                    
                    narrowed_sites = sites[sites["local_site_name"].isin(cw_matches["local_site_name"])]
                    for site_code, county_name in zip(narrowed_sites["site_code"], narrowed_sites["county_name"]):
                        site_to_county[site_code] = f"{direction} {county_name}"

                known_cities = set(sites["city_name"].dropna().unique())
                comment_cities = _find_city_comment(row.get("comments"), known_cities)
                if comment_cities:
                    narrowed_sites = sites[sites["city_name"].isin(comment_cities)]
                    narrowed_counties = set(narrowed_sites["county_name"].str.casefold())
                    site_to_county = {
                        sc: dc for sc, dc in site_to_county.items()
                        if not any(dc.casefold().endswith(nc) for nc in narrowed_counties)
                    }
                    for site_code, county_name in zip(narrowed_sites["site_code"], narrowed_sites["county_name"]):
                        site_to_county[site_code] = county_name

            for site_code, county_name in site_to_county.items():
                out_rows.append({**row, 'site_code': site_code, 'county': county_name})

    if unresolved_directional:
        print("Directional county entries with no crosswalk match (left unresolved):")
        for direction, county in sorted(unresolved_directional):
            print(f"  {direction} {county}")

    if unmatched_counties:
        print("Plain counties with no match in dim_sites (left unresolved):")
        for county in sorted(unmatched_counties):
            print(f"  {county}")

    if blank_county_rows:
        print(f"{blank_county_rows} advisory rows had blank or missing county values and were skipped")

    if not out_rows:
        return pd.DataFrame(columns=_OUTPUT_COLUMNS)

    return pd.DataFrame(out_rows)[_OUTPUT_COLUMNS]

def _values_match(a, b) -> bool:
    """ Exact match comparison, used for merging later on. Only merges if certain columns match exactly."""
    if pd.isna(a) and pd.isna(b): 
        return True
    return a == b 

def merge_continuous_advisories(df: pd.DataFrame) -> pd.DataFrame:
    """ Merge advisory rows for the same site_code and advisory type whose date ranges touch or overlap. Comments are combined with extact-duplicate items removed."""
    df= df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])

    merged_rows = []
    group_cols = ["site_code", "pollutant_source"]
    for _, group in df.groupby(group_cols, sort=False):
        group = group.sort_values("start_date").reset_index(drop=True)
        current = group.iloc[0].to_dict()
        current_end = current["end_date"]

        for _, row in group.iloc[1:].iterrows():
            dates_touch = row["start_date"] <= current_end
            comments_match = _values_match(row.get("comments"), current.get("comments"))
            fire_names_match  = _values_match(row.get("fire_names"), current.get("fire_names"))

            if dates_touch and comments_match and fire_names_match:
                current_end = max(current_end, row["end_date"])
                current["end_date"] = current_end
            else:
                merged_rows.append(current)
                current = row.to_dict()
                current_end = current["end_date"]

        merged_rows.append(current)

    return pd.DataFrame(merged_rows)

def consolidate_advisories_for_year(year: str, source_dir: Path) -> pd.DataFrame:
    """ Read and clean the raw advisory tracking data for a specific year, returning a DataFrame with the consolidated data.
    
    Args:
        year: Year to process (e.g., 2023)
        source_dir: ADVISORY_SOURCE_DIR
    Returns:
        pd.DataFrame: Consolidated advisory data for the specified year, reindex onto the same standard column set regardless of source-year format. 
        Not yet exploded on site_code.
    """
    year_dir = source_dir / year
    input_file = _find_source_file(year_dir, year)
    if input_file is None:
        print(f"No advisory tracking file found for year {year} in {year_dir}")
        return pd.DataFrame()

    try:
        df = _read_source(input_file)
    except Exception as e:
        print(f"Error reading advisory tracking file for year {year}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"No data found in advisory tracking file for year {year}: {input_file}")
        return pd.DataFrame()

    if year == "2022":
        column_map = _COLUMN_MAP_2022
        missing = [c for c in column_map if c not in df.columns]
        if missing:
            print(f"Missing expected columns in 2022 advisory tracking file: {missing}")
            return pd.DataFrame()

    else:
        county_col = _find_county_column(df.columns)
        if county_col is None:
            print(f"Advisory tracking file for year {year} has no recognized county column"
                  f"(looked for {_COUNTY_COLUMN_CANDIDATES}): {list(df.columns)}")
            return pd.DataFrame()

        missing = [c for c in _COLUMN_MAP_BASE if c not in df.columns]
        if missing:
            print(f"Advisory tracking file for year {year} has missing expected columns: {missing}")
            return pd.DataFrame()

        column_map = {**_COLUMN_MAP_BASE, county_col: 'county'}

    df = df.rename(columns=column_map)[list(column_map.values())].copy()
    df = df.reindex(columns=_STANDARD_COLUMNS)

    if year == "2022":
        df['county'] = df['county'].astype(str).str.strip()
        df['end_date'] = df['comments'].apply(_parse_2022_end_date)
    else:
        df['county'] = df['county'].str.rstrip(",").str.strip()

    for col in _DATE_COLS:
        if df[col].notna().any():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date 

    for col in _YES_NO_COLS:
        df[col] = df[col].astype(str).str.strip().str.upper().eq("YES")

    print(f"Consolidated {len(df)} advisory records for year {year} from {len(df)} total advisory records")
    return df

def run_consolidation() -> None:
    """Run the advisory consolidation pipeline for all years in the configured range."""
    print("Starting Advisory Consolidation Pipeline")

    if not config.ADVISORY_SOURCE_DIR:
        print("ADVISORY_SOURCE_DIR is not set in config.py. Please set it to the directory containing the raw advisory tracking files.")
        return

    source_dir = Path(config.ADVISORY_SOURCE_DIR)
    if not source_dir.exists():
        print(f"ADVISORY_SOURCE_DIR does not exist: {source_dir}")
        return  

    try:
        sites = load_dim_sites()
    except FileNotFoundError as e:
        print(e)
        print("Please run the dim_sites consolidation pipeline first.")
        return
    crosswalk = load_direction_crosswalk()

    output_dir = config.ROOT / "staged" / "fct_air_quality_advisories"
    output_dir.mkdir(parents=True, exist_ok=True)

    years_dirs = sorted(
        p for p in source_dir.iterdir() if p.is_dir() and p.name.isdigit() and len(p.name) == 4
    )
    if not years_dirs:
        print(f"No year subfolders found in ADVISORY_SOURCE_DIR: {source_dir}")
        return

    years_processed = 0
    total_records = 0

    for year_dir in years_dirs:
        year = year_dir.name
        print(f"\nConsolidating advisories for year {year}...")

        result = consolidate_advisories_for_year(year, source_dir)
        if result.empty:
            print(f"  No advisory data for {year}, skipping")
            continue

        if year == "2022":
            print("  2022 county values are not reliably splittable per county -"
                  "skipping site_code attachment for 2022 advisories")
            continue

        result = attach_site_codes(result, sites, crosswalk)
        result = merge_continuous_advisories(result)
        if result.empty:
            print(f"  No advisories could be matched to site codes for {year}, skipping")
            continue

        output_path = output_dir / f"fct_air_quality_advisories_{year}.csv"
        result.to_csv(output_path, index=False)
        print(f"  Wrote {len(result)} records to {output_path.name}")

        years_processed += 1
        total_records += len(result)

    print(f"Finished Advisory Consolidation Pipeline. Processed {years_processed} years with a total of {total_records} records.")

if __name__ == "__main__":
    run_consolidation()