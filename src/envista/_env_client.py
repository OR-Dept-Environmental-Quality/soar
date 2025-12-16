"""Shared Envista HTTP client and helpers.

Provides a requests.Session configured with retries and a simple global
rate-limiter to comply with Envista API guidance. Also includes helpers for
building date ranges used by Envista services.
"""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from threading import Lock
from typing import Optional

import requests

import config

# Simple global rate limiter state
_last_request_time = 0.0
_rate_lock = Lock()
_request_timestamps: deque[float] = deque()
_min_delay_seconds = float(getattr(config, "ENV_MIN_DELAY", 0.1))
_max_requests_per_second = int(getattr(config, "ENV_MAX_RPS", 5))

# Session timeout storage (keyed by session id)
_session_timeouts: dict = {}

# Retry/backoff configuration (read from env or use defaults)
_ENV_RETRIES = int(getattr(config, "ENV_RETRIES", 4))
_BACKOFF_FACTOR = float(getattr(config, "ENV_BACKOFF_FACTOR", 1.5))
_RETRY_MAX_WAIT = int(getattr(config, "ENV_RETRY_MAX_WAIT", 60))
_DEFAULT_TIMEOUT = int(getattr(config, "ENV_TIMEOUT", 120))

# Circuit-breaker configuration
_CIRCUIT_THRESHOLD = int(config.__dict__.get("ENV_CIRCUIT_THRESHOLD", 5))
_CIRCUIT_COOLDOWN = int(config.__dict__.get("ENV_CIRCUIT_COOLDOWN", 1800))  # seconds


def _sleep_if_needed() -> None:
    """Enforce request pacing to honor Envista rate limits while allowing concurrency."""
    global _last_request_time

    if _max_requests_per_second <= 0:
        return

    with _rate_lock:
        now = time.monotonic()

        # Enforce optional minimum delay between successive requests
        if _min_delay_seconds > 0:
            elapsed = now - _last_request_time
            if elapsed < _min_delay_seconds:
                time.sleep(_min_delay_seconds - elapsed)
                now = time.monotonic()

        # Drop timestamps older than one-second window
        window_start = now - 1.0
        while _request_timestamps and _request_timestamps[0] < window_start:
            _request_timestamps.popleft()

        # If we're at capacity, wait until the earliest request expires
        if len(_request_timestamps) >= _max_requests_per_second:
            earliest = _request_timestamps[0]
            wait = 1.0 - (now - earliest)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
                window_start = now - 1.0
                while _request_timestamps and _request_timestamps[0] < window_start:
                    _request_timestamps.popleft()

        _request_timestamps.append(now)
        _last_request_time = now


def _health_path() -> str:
    from pathlib import Path
    ctl_dir = getattr(config, "CTL_DIR", Path.home() / ".soar" / "ctl")
    ctl_dir.mkdir(parents=True, exist_ok=True)
    return str(ctl_dir / "env_health.json")


def _read_health() -> dict:
    path = _health_path()
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {"consecutive_failures": 0, "opened_at": None}


def _write_health(state: dict) -> None:
    from loaders.filesystem import atomic_write_json

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
    """Check if Envista circuit breaker is currently open."""
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


def make_session(timeout: Optional[int] = None) -> requests.Session:
    """Create a requests.Session configured for Envista API with rate limiting.

    We intentionally avoid the urllib3 Retry adapter here because we implement
    a Retry-After-aware retry loop in `fetch_json` which allows honoring
    service-provided Retry-After headers and a file-backed circuit breaker.

    Args:
        timeout: Optional request timeout in seconds. Defaults to ENV_TIMEOUT config.

    Returns:
        Configured requests.Session instance.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "soar-pipeline/1.0"})
    # Store timeout in module-level dictionary to avoid Pylance type errors
    _session_timeouts[id(session)] = timeout if timeout is not None else _DEFAULT_TIMEOUT
    return session


def _parse_retry_after(resp) -> Optional[int]:
    """Parse Retry-After header from response."""
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


def _sleep_backoff(attempt: int, retry_after: Optional[int] = None) -> None:
    """Sleep with exponential backoff and optional Retry-After header."""
    if retry_after is not None:
        wait = min(retry_after, _RETRY_MAX_WAIT)
    else:
        # exponential backoff with jitter
        base = _BACKOFF_FACTOR * (2**attempt)
        jitter = base * 0.1
        wait = min(_RETRY_MAX_WAIT, base + (jitter * (2 * (time.time() % 1) - 1)))
        if wait < 0:
            wait = 0
    time.sleep(wait)


def fetch_json(session: requests.Session, url: str) -> dict:
    """Fetch JSON with Retry-After and circuit-breaker awareness.

    Implements retry logic with exponential backoff and respects Retry-After headers.
    Opens circuit breaker on repeated server errors (5xx).

    Args:
        session: Configured requests.Session instance
        url: URL to fetch

    Returns:
        Parsed JSON response as dictionary

    Raises:
        RuntimeError: If circuit breaker is open
        requests.exceptions.RequestException: If request fails after retries
    """
    # If circuit is currently open, raise early to let callers fallback/abort
    if circuit_is_open():
        raise RuntimeError("Envista circuit is open; skipping external requests")

    last_exc = None
    for attempt in range(_ENV_RETRIES + 1):
        try:
            _sleep_if_needed()
            resp = session.get(
                url, timeout=_session_timeouts.get(id(session), _DEFAULT_TIMEOUT)
            )
            # if service tells us to slow down, honor it
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                _sleep_backoff(attempt, retry_after=retry_after)
                last_exc = requests.exceptions.ConnectionError("429 Too Many Requests")
                continue
            resp.raise_for_status()
            # success -> reset circuit
            _reset_circuit()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            # server-side 5xx errors should increment failure counter
            status = (
                getattr(exc.response, "status_code", None)
                if hasattr(exc, "response")
                else None
            )
            if status and 500 <= status < 600:
                _open_circuit()
            # allow backoff and retry
            retry_after = None
            try:
                retry_after = (
                    _parse_retry_after(exc.response)
                    if hasattr(exc, "response") and exc.response is not None
                    else None
                )
            except Exception:
                retry_after = None
            if attempt < _ENV_RETRIES:
                _sleep_backoff(attempt, retry_after=retry_after)
                continue
            break
    # all retries exhausted
    if last_exc is not None:
        raise last_exc
    else:
        raise RuntimeError(f"Failed to fetch {url} after {_ENV_RETRIES + 1} attempts")
