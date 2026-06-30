"""
Process-wide token bucket + 429 retry for Google API write calls.

Why: Google Docs / Sheets / Slides cap mutating calls at ~60/minute per
user per project. Bursting past that returns HTTP 429
("Quota exceeded for quota metric 'Write requests'..."). Before this
module, a long-running `gw docs ...` sequence (or anything that loops
batchUpdate) would crash mid-flight and force a manual sleep.

Design: monkey-patch `googleapiclient.http.HttpRequest.execute` at
process start. The wrapper:

  1. Token-bucket throttles writes to <= MAX_WRITES per WINDOW seconds
     (defaults: 50 / 60 to leave headroom under Google's 60/min cap).
     Reads (GET) pass through untouched.
  2. On 429 (or 403 with "rateLimit"/"userRateLimit" reason), sleeps
     `Retry-After` (if present) or exponential backoff starting at 5s,
     up to MAX_BACKOFF=60s, for up to MAX_ATTEMPTS=5 tries.
  3. Logs to stderr so the operator sees progress.

The patch is idempotent — calling install() twice is a no-op.
"""

from __future__ import annotations

import os
import random
import sys
import threading
import time
from collections import deque
from typing import Deque

from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest

# ---------------------------------------------------------------------------
# Tunables (env-overridable for emergencies)
# ---------------------------------------------------------------------------

MAX_WRITES = int(os.environ.get("GW_THROTTLE_MAX_WRITES", "50"))
WINDOW_SECS = float(os.environ.get("GW_THROTTLE_WINDOW", "60"))
MAX_ATTEMPTS = int(os.environ.get("GW_THROTTLE_MAX_ATTEMPTS", "5"))
INITIAL_BACKOFF = float(os.environ.get("GW_THROTTLE_INITIAL_BACKOFF", "5"))
MAX_BACKOFF = float(os.environ.get("GW_THROTTLE_MAX_BACKOFF", "60"))

# Methods we consider writes. Everything else is a read and bypasses the
# token bucket (still benefits from the 429 retry layer).
_WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_recent_writes: Deque[float] = deque()


def _wait_for_slot() -> None:
    """Block until we're allowed another write under the rolling window."""
    while True:
        with _lock:
            now = time.monotonic()
            # Drop timestamps that fell out of the window
            while _recent_writes and now - _recent_writes[0] >= WINDOW_SECS:
                _recent_writes.popleft()
            if len(_recent_writes) < MAX_WRITES:
                _recent_writes.append(now)
                return
            # Sleep until the oldest in-window write ages out
            sleep_for = WINDOW_SECS - (now - _recent_writes[0]) + 0.05
        if sleep_for > 0:
            print(
                f"[gw] throttle: {len(_recent_writes)}/{MAX_WRITES} writes "
                f"in last {WINDOW_SECS:.0f}s, sleeping {sleep_for:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(sleep_for)


# ---------------------------------------------------------------------------
# 429 detection + retry
# ---------------------------------------------------------------------------


def _is_rate_limit_error(err: HttpError) -> bool:
    status = getattr(getattr(err, "resp", None), "status", None)
    if status == 429:
        return True
    if status == 403:
        # google-api-python-client surfaces userRateLimitExceeded as 403
        msg = str(err).lower()
        if "ratelimit" in msg or "userratelimit" in msg or "quota" in msg:
            return True
    return False


def _retry_after_seconds(err: HttpError, attempt: int) -> float:
    resp = getattr(err, "resp", None)
    if resp is not None:
        ra = resp.get("retry-after") if hasattr(resp, "get") else None
        if ra:
            try:
                return float(ra)
            except (TypeError, ValueError):
                pass
    # Exponential backoff with jitter
    delay = min(INITIAL_BACKOFF * (2 ** (attempt - 1)), MAX_BACKOFF)
    return delay + random.uniform(0, 1)


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

_INSTALLED = False
_original_execute = None


def install() -> None:
    """Monkey-patch HttpRequest.execute. Idempotent."""
    global _INSTALLED, _original_execute
    if _INSTALLED:
        return
    _original_execute = HttpRequest.execute

    def patched_execute(self, http=None, num_retries=0):
        method = (getattr(self, "method", "GET") or "GET").upper()
        is_write = method in _WRITE_METHODS
        if is_write:
            _wait_for_slot()

        attempt = 0
        while True:
            attempt += 1
            try:
                return _original_execute(self, http=http, num_retries=num_retries)
            except HttpError as e:
                if not _is_rate_limit_error(e) or attempt >= MAX_ATTEMPTS:
                    raise
                delay = _retry_after_seconds(e, attempt)
                print(
                    f"[gw] rate-limited (HTTP {e.resp.status}), "
                    f"attempt {attempt}/{MAX_ATTEMPTS}, waiting {delay:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(delay)

    HttpRequest.execute = patched_execute  # type: ignore[assignment]
    _INSTALLED = True
