import pandas as pd
from aqs.transformers.monitors import transform_monitors


def _build_sample_frame():
    return pd.DataFrame(
        [
            {
                "state_code": "41",
                "county_code": "5",
                "site_number": "1",
                "parameter_code": "44201",
                "site_code": "410050001",
            },
            {
                "state_code": "41",
                "county_code": "5",
                "site_number": "2",
                "parameter_code": "42602",
                "site_code": "410050002",
            },
            {
                "state_code": "6",
                "county_code": "75",
                "site_number": "10",
                "parameter_code": "42101",
                "site_code": "060750010",
            },
        ]
    )


def test_transform_monitors_processes_data():
    df = _build_sample_frame()
    result = transform_monitors(df)

    # Should have 3 unique sites (no deduplication in this test data)
    assert len(result) == 3

    # Should contain site_code column
    assert "site_code" in result.columns
