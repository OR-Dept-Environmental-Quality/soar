"""Pipeline orchestration for AQS monitors."""
from __future__ import annotations

from soar import config
from soar.aqs.extractors.monitors import fetch_monitors
from soar.aqs.transformers.monitors import add_site_id, to_curated, to_staged
from soar.loaders.filesystem import write_csv, write_parquet

RAW_PARQUET = config.RAW / "monitors_raw.parquet"
RAW_CSV = config.RAW / "monitors_raw.csv"
CURATED_PARQUET = config.TRANS / "monitors_curated.parquet"
CURATED_CSV = config.TRANS / "monitors_curated.csv"
STAGED_PARQUET = config.STAGED / "monitors_staged.parquet"
STAGED_CSV = config.STAGED / "monitors_staged.csv"


def run() -> None:
    """Execute the monitors pipeline end-to-end."""
    config.ensure_dirs(config.RAW, config.TRANS, config.STAGED)
    config.set_aqs_credentials()

    raw = fetch_monitors(["88101"], config.BDATE, config.EDATE, config.STATE)
    raw_with_ids = add_site_id(raw) if not raw.empty else raw
    curated = to_curated(raw_with_ids) if not raw.empty else raw_with_ids
    staged = to_staged(curated)

    write_parquet(raw_with_ids, RAW_PARQUET)
    write_csv(raw_with_ids, RAW_CSV)
    write_parquet(curated, CURATED_PARQUET)
    write_csv(curated, CURATED_CSV)
    write_parquet(staged, STAGED_PARQUET)
    write_csv(staged, STAGED_CSV)

    print(
        " | ".join(
            [
                f"raw={len(raw_with_ids)}",
                f"curated={len(curated)}",
                f"staged={len(staged)}",
            ]
        )
    )


if __name__ == "__main__":
    run()
