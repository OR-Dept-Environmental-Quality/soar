import pandas as pd
from aqs.transformers.monitors import add_site_id, to_curated, to_staged
from schemas.monitors import schema_curated, schema_staged


def _build_sample_frame():
    return pd.DataFrame([
        {"state_code": "41", "county_code": "5", "site_number": "1", "parameter_code": "44201"},
        {"state_code": "41", "county_code": "5", "site_number": "2", "parameter_code": "42602"},
        {"state_code": "6", "county_code": "75", "site_number": "10", "parameter_code": "42101"},
    ])


def test_transformers_and_schemas():
    df = _build_sample_frame()
    df2 = add_site_id(df)
    curated = to_curated(df2)
    # validate curated schema
    validated = schema_curated.validate(curated)
    assert not validated.empty
    staged = to_staged(validated)
    validated2 = schema_staged.validate(staged)
    assert not validated2.empty
