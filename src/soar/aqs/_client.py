"""Shared AQS HTTP client and helpers.

Provides a requests.Session configured with retries and a simple global
rate-limiter to comply with AQS API guidance. Also includes helpers to build
calendar-year chunks used by several AQS services.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Iterator, Tuple
import json

import pandas as pd
import requests
from urllib.parse import parse_qs, urlparse
from email.utils import parsedate_to_datetime

from soar import config

# Simple global rate limiter state
_last_request_time = 0.0
_rate_lock = Lock()
_min_delay_seconds = float(int(config.__dict__.get('AQS_MIN_DELAY', 5)))

# Retry/backoff configuration (read from env or use defaults)
_AQS_RETRIES = int(config.__dict__.get('AQS_RETRIES', 4))
_BACKOFF_FACTOR = float(config.__dict__.get('AQS_BACKOFF_FACTOR', 1.0))
_RETRY_MAX_WAIT = int(config.__dict__.get('AQS_RETRY_MAX_WAIT', 60))

# Circuit-breaker configuration
_CIRCUIT_THRESHOLD = int(config.__dict__.get('AQS_CIRCUIT_THRESHOLD', 5))
_CIRCUIT_COOLDOWN = int(config.__dict__.get('AQS_CIRCUIT_COOLDOWN', 1800))  # seconds


def _sleep_if_needed() -> None:
    """Enforce a global minimum delay between requests."""
    global _last_request_time
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        wait = _min_delay_seconds - elapsed
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.time()


def _health_path() -> str:
    config.CTL_DIR.mkdir(parents=True, exist_ok=True)
    return str(config.CTL_DIR / "aqs_health.json")


def _read_health() -> dict:
    path = _health_path()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {"consecutive_failures": 0, "opened_at": None}


def _write_health(state: dict) -> None:
    from soar.loaders.filesystem import atomic_write_json

    path = _health_path()
    atomic_write_json(path, state)


def _open_circuit() -> None:
    state = _read_health()
    state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
    state["opened_at"] = datetime.utcnow().isoformat()
    _write_health(state)


def _reset_circuit() -> None:
    state = _read_health()
    if state.get("consecutive_failures", 0) == 0 and not state.get("opened_at"):
        return
    _write_health({"consecutive_failures": 0, "opened_at": None})


def circuit_is_open() -> bool:
    state = _read_health()
    opened_at = state.get("opened_at")
    failures = state.get("consecutive_failures", 0)
    if not opened_at:
        return False
    try:
        opened = datetime.fromisoformat(opened_at)
    except Exception:
        return False
    if failures < _CIRCUIT_THRESHOLD:
        return False
    # if still within cooldown window, circuit remains open
    if datetime.utcnow() < opened + timedelta(seconds=_CIRCUIT_COOLDOWN):
        return True
    # cooldown expired — allow a probe (caller should attempt a single check)
    return False


def make_session(timeout: int = 30) -> requests.Session:
    """Create a requests.Session and wrap requests with rate limiting.

    We intentionally avoid the urllib3 Retry adapter here because we implement
    a Retry-After-aware retry loop in `fetch_json` which allows honoring
    service-provided Retry-After headers and a file-backed circuit breaker.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "soar-pipeline/1.0"})
    session.request = _wrap_request_with_rate(session.request)
    session.timeout = timeout
    return session


def _wrap_request_with_rate(func):
    def wrapped(method, url, *args, **kwargs):
        _sleep_if_needed()
        return func(method, url, *args, **kwargs)

    return wrapped


def _parse_retry_after(resp) -> int | None:
    header = resp.headers.get("Retry-After")
    if not header:
        return None
    try:
        # If it's an integer number of seconds
        return int(header)
    except Exception:
        try:
            # If it's an HTTP date
            t = parsedate_to_datetime(header)
            return max(0, int((t - datetime.utcnow()).total_seconds()))
        except Exception:
            return None


def _sleep_backoff(attempt: int, retry_after: int | None = None) -> None:
    if retry_after is not None:
        wait = min(retry_after, _RETRY_MAX_WAIT)
    else:
        # exponential backoff with jitter
        base = _BACKOFF_FACTOR * (2 ** attempt)
        jitter = base * 0.1
        wait = min(_RETRY_MAX_WAIT, base + (jitter * (2 * (time.time() % 1) - 1)))
        if wait < 0:
            wait = 0
    time.sleep(wait)


def fetch_json(session: requests.Session, url: str) -> dict:
    """Fetch JSON with Retry-After and circuit-breaker awareness.

    Raises on HTTP errors. On repeated server errors this will open the
    circuit (persisted) to avoid hammering AQS.
    """
    # If circuit is currently open, raise early to let callers fallback/abort
    if circuit_is_open():
        raise RuntimeError("AQS circuit is open; skipping external requests")

    last_exc = None
    for attempt in range(_AQS_RETRIES + 1):
        try:
            resp = session.get(url, timeout=getattr(session, "timeout", 30))
            # if service tells us to slow down, honor it
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                _sleep_backoff(attempt, retry_after=retry_after)
                last_exc = requests.exceptions.RetryError("429 Too Many Requests")
                continue
            resp.raise_for_status()
            # success -> reset circuit
            _reset_circuit()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            # server-side 5xx errors should increment failure counter
            status = getattr(exc.response, "status_code", None) if hasattr(exc, "response") else None
            if status and 500 <= status < 600:
                _open_circuit()
            # allow backoff and retry
            retry_after = None
            try:
                retry_after = _parse_retry_after(exc.response) if hasattr(exc, "response") and exc.response is not None else None
            except Exception:
                retry_after = None
            if attempt < _AQS_RETRIES:
                _sleep_backoff(attempt, retry_after=retry_after)
                continue
            break
    # all retries exhausted
    raise last_exc


def fetch_df(session: requests.Session, url: str) -> pd.DataFrame:
    js = fetch_json(session, url)

    data = []
    if isinstance(js, list):
        # Common pattern: [metadata, data]
        if len(js) > 1 and isinstance(js[1], list):
            data = js[1]
        elif js and isinstance(js[0], list):
            data = js[0]
    elif isinstance(js, dict):
        for key in ("Data", "data", "Results", "results", "rows"):
            value = js.get(key)
            if isinstance(value, list):
                data = value
                break

    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def build_year_chunks(start: date, end: date) -> Iterator[Tuple[str, str]]:
    """Yield (bdate, edate) strings for each calendar-year chunk between start and end.

    Returns strings in YYYYMMDD format.
    """
    s = pd.to_datetime(start).date()
    e = pd.to_datetime(end).date()
    for year in range(s.year, e.year + 1):
        if year == s.year:
            b = s.strftime("%Y%m%d")
        else:
            b = f"{year}0101"
        if year == e.year:
            ed = e.strftime("%Y%m%d")
        else:
            ed = f"{year}1231"
        yield b, ed
