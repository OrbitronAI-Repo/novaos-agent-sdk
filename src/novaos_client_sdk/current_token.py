"""Read the run's newest execution token at call time.

A pod is handed one token, on the call that starts its work. A run that outlives
it keeps calling the platform with that token unless it asks for a newer one. The
auth-proxy sidecar keeps the newest verified token per run and serves it on a
loopback port; ``NOVAOS_TOKEN_ENDPOINT`` points at it (for example
``http://127.0.0.1:8002``). The proxy serves it only to a caller that proves it
holds the run, so the request carries the token in hand as its bearer. Without
that variable nothing changes: the token the caller already holds is used.

The token is never logged, and any failure to reach the proxy falls back to the
token in hand.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable

TOKEN_ENDPOINT_ENV = "NOVAOS_TOKEN_ENDPOINT"

# Ask for a newer token once less than this share of the lifetime remains.
RENEW_BELOW_FRACTION = 1 / 3

_FETCH_TIMEOUT_S = 0.25


def _lifetime(token: str) -> tuple[float, float] | None:
    """``(issued_at, expires_at)`` from the payload, or None if unreadable.

    The signature is not checked here: the proxy verified the token before
    holding it, and this only decides when to ask again.
    """
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return float(claims["iat"]), float(claims["exp"])
    except (IndexError, ValueError, KeyError, TypeError):
        return None


class CurrentTokenSource:
    """Resolves the newest token for a run, falling back to the one in hand."""

    def __init__(
        self,
        endpoint: str | None = None,
        *,
        fetch: Callable[[str, str], str | None] | None = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._endpoint = (endpoint or "").rstrip("/")
        self._fetch = fetch or self._http_fetch
        self._now = now
        self._newest: dict[str, str] = {}
        self._lock = threading.Lock()

    def current(self, run_id: str, in_hand: str) -> str:
        if not self._endpoint or not run_id:
            return in_hand

        with self._lock:
            best = self._newest.get(run_id, in_hand)
            if _expiry(in_hand) > _expiry(best):
                best = in_hand
            if not self._due(best):
                return best
            try:
                fetched = self._fetch(run_id, best)
            except Exception:
                return best
            if fetched and _expiry(fetched) > _expiry(best):
                self._newest[run_id] = fetched
                return fetched
            return best

    def _due(self, token: str) -> bool:
        lifetime = _lifetime(token)
        if lifetime is None:
            return True
        issued_at, expires_at = lifetime
        return expires_at - self._now() < (expires_at - issued_at) * RENEW_BELOW_FRACTION

    def _http_fetch(self, run_id: str, proof: str) -> str | None:
        url = f"{self._endpoint}/token?{urllib.parse.urlencode({'run_id': run_id})}"
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {proof}"})
        try:
            with urllib.request.urlopen(request, timeout=_FETCH_TIMEOUT_S) as resp:  # noqa: S310
                token = json.load(resp).get("token")
        except (urllib.error.URLError, OSError, ValueError):
            return None
        return token if isinstance(token, str) and token else None


def _expiry(token: str) -> float:
    lifetime = _lifetime(token)
    return lifetime[1] if lifetime else float("-inf")


_default: CurrentTokenSource | None = None
_default_endpoint: str | None = None
_default_lock = threading.Lock()


def default_source() -> CurrentTokenSource:
    """The process-wide source, built from ``NOVAOS_TOKEN_ENDPOINT``."""
    global _default, _default_endpoint
    endpoint = os.environ.get(TOKEN_ENDPOINT_ENV, "")
    with _default_lock:
        if _default is None or endpoint != _default_endpoint:
            _default = CurrentTokenSource(endpoint)
            _default_endpoint = endpoint
        return _default


def reset_default_source() -> None:
    """Drop the process-wide source. For tests."""
    global _default, _default_endpoint
    with _default_lock:
        _default, _default_endpoint = None, None
