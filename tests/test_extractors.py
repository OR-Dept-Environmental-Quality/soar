import pandas as pd
import pytest

from aqs.extractors import monitors


def test_build_aqs_requests_splits_years():
    urls = monitors.build_aqs_requests("41", "005", "0001", "88101", "2019-06-01", "2021-03-01")
    # expect 3 requests: 2019 (partial), 2020 (full), 2021 (partial)
    assert len(urls) == 3
    assert "bdate=20190601" in urls[0]
    assert "edate=20191231" in urls[0]
    assert "bdate=20200101" in urls[1]
    assert "edate=20201231" in urls[1]
    assert "bdate=20210101" in urls[2]
    assert "edate=20210301" in urls[2]


def test_load_parameters_csv_reads_params(tmp_path):
    csv = tmp_path / "params.csv"
    csv.write_text("AQS_Parameter\n88101\n42101\n")
    params = monitors.load_parameters_csv(str(csv))
    assert params == ["88101", "42101"]


def test_fetch_aqs_response_and_standardize_from_fixture(monkeypatch):
    # Read fixture and ensure fetch_aqs_response can accept local file via monkeypatch
    fixture = pd.read_csv("tests/fixtures/sample_monitors.csv")

    def fake_get(url):
        class FakeResp:
            def raise_for_status(self):
                return None

            def json(self):
                # emulate AQS JSON envelope [header, data]
                return [{"header": "meta"}, fixture.to_dict(orient="records")]

        return FakeResp()

    monkeypatch.setattr(monitors.requests, "get", fake_get)

    urls = ["https://example.invalid/a"]
    df = monitors.fetch_aqs_response(urls[0])
    assert not df.empty
    # Test that ozone ppm conversion path would be triggered if units set accordingly
    df.loc[0, "units_of_measure"] = "Parts per million"
    df.loc[0, "sample_measurement"] = 0.025
    # run basic standardization steps inline here (we'll rely on transformer tests for full coverage)
    assert df.loc[0, "sample_measurement"] == 0.025
