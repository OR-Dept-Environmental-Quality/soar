from datetime import datetime

import pytest
import requests

from aqs import _client


class DummyResp:
    def __init__(self, status_code=500, headers=None, content=b"{}"):
        self.status_code = status_code
        self.headers = headers or {}
        self._content = content

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise requests.exceptions.HTTPError(response=self)

    def json(self):
        return {"status": "error"}


class DummySession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, url, timeout=None):
        if not self._responses:
            return DummyResp(status_code=200, content=b"[]")
        return self._responses.pop(0)


def test_circuit_opens_after_consecutive_server_errors(tmp_path, monkeypatch):
    # ensure a clean health file location
    monkeypatch.setattr(
        _client, "_health_path", lambda: str(tmp_path / "aqs_health.json")
    )

    # responses: 500, 500, 500, 500, 500 (threshold default 5)
    responses = [DummyResp(500) for _ in range(6)]
    session = DummySession(responses)

    # first call should raise after retries and increment failures
    with pytest.raises(Exception):
        _client.fetch_json(session, "https://example.invalid/api")

    # circuit should now be open
    assert _client.circuit_is_open() is True

    # subsequent call should raise early due to open circuit
    with pytest.raises(RuntimeError):
        _client.fetch_json(session, "https://example.invalid/api")


def test_reset_circuit_allows_requests(tmp_path, monkeypatch):
    monkeypatch.setattr(
        _client, "_health_path", lambda: str(tmp_path / "aqs_health.json")
    )
    # write a health file with high consecutive_failures and recent opened_at
    now = datetime.utcnow().isoformat()
    _client._write_health({"consecutive_failures": 10, "opened_at": now})
    assert _client.circuit_is_open() is True

    # reset
    _client._reset_circuit()
    assert _client.circuit_is_open() is False
