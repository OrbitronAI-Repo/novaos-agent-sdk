"""Shared resilience primitives for NovaOS client SDK.

Provides :class:`CircuitBreaker` and :func:`backoff_delay` so both
:class:`~novaos_client_sdk.memory.MemoryClient` and
:class:`~novaos_client_sdk.tools.KnowledgeTool` share the same battle-tested
retry/circuit-breaker machinery without duplication.

Design notes
------------
* ``CircuitBreaker`` is a pure, dependency-free implementation of the classic
  three-state automaton: CLOSED → OPEN → HALF_OPEN → CLOSED.
* ``backoff_delay`` computes ``base * 2^(attempt-1)`` capped at ``max_s`` with
  optional ±50 % multiplicative jitter (on by default).
* ``RETRYABLE_HTTP_STATUSES`` lists the HTTP status codes that indicate a
  transient server-side failure and are safe to retry (5xx gateway errors +
  429 rate-limit).  Client-side 4xx errors are *not* retryable.
"""

from __future__ import annotations

import logging
import random
import time

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: HTTP status codes that represent transient failures worth retrying.
#: 4xx codes (except 429) indicate client-side mistakes and are excluded.
RETRYABLE_HTTP_STATUSES: frozenset[int] = frozenset({429, 502, 503, 504})


# ---------------------------------------------------------------------------
# Backoff helper
# ---------------------------------------------------------------------------


def backoff_delay(
    attempt: int,
    base_s: float = 1.0,
    max_s: float = 16.0,
    *,
    jitter: bool = True,
) -> float:
    """Return the sleep duration (seconds) for *attempt* (1-based).

    Uses truncated binary exponential backoff::

        delay = min(base_s * 2^(attempt-1), max_s)

    With ``jitter=True`` (the default) the result is multiplied by a random
    factor in ``[0.5, 1.0]`` so concurrent callers spread their retries.

    Args:
        attempt: Current attempt number, 1-based.  Attempt 1 → ``base_s``,
            attempt 2 → ``2 * base_s``, etc.
        base_s: Base delay in seconds (default 1.0).
        max_s: Maximum delay cap in seconds (default 16.0).
        jitter: When ``True`` (default) apply multiplicative jitter in [0.5, 1.0].

    Returns:
        Sleep duration in seconds.
    """
    delay = min(base_s * (2.0 ** (attempt - 1)), max_s)
    if jitter:
        delay *= 0.5 + random.random() * 0.5
    return delay


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class CircuitBreaker:
    """Three-state circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED.

    Protects downstream services from thundering-herd after repeated failures:

    * **CLOSED** — all requests flow normally.
    * **OPEN** — requests are rejected immediately (fail-fast) for
      ``reset_timeout_s`` seconds after ``failure_threshold`` consecutive
      failures.
    * **HALF_OPEN** — one probe request is allowed; success closes the breaker,
      failure re-opens it and resets the timer.

    This class was previously duplicated as ``_CircuitBreaker`` in
    ``memory.py``.  Both :class:`~novaos_client_sdk.memory.MemoryClient` and
    :class:`~novaos_client_sdk.tools.KnowledgeTool` now import it from here.

    Args:
        failure_threshold: Number of consecutive failures that trip the breaker
            to OPEN (default 5).
        reset_timeout_s: Seconds to wait in OPEN state before allowing a
            half-open probe (default 30.0).

    Example::

        cb = CircuitBreaker(failure_threshold=3, reset_timeout_s=10)
        if cb.allow_request():
            try:
                result = await do_work()
                cb.record_success()
            except TransientError:
                cb.record_failure()
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout_s: float = 30.0,
    ) -> None:
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout_s
        self._failures = 0
        self._last_failure: float = 0.0
        self._state = "closed"

    # ------------------------------------------------------------------
    # State machine
    # ------------------------------------------------------------------

    def allow_request(self) -> bool:
        """Return ``True`` if a request should be allowed through."""
        if self._state == "closed":
            return True
        if self._state == "open":
            if time.monotonic() - self._last_failure >= self._reset_timeout:
                self._state = "half_open"
                return True
            return False
        # half_open: allow exactly one probe request
        return True

    def record_success(self) -> None:
        """Signal that the most recent request succeeded.  Closes the breaker."""
        self._state = "closed"
        self._failures = 0

    def record_failure(self) -> None:
        """Signal that the most recent request failed.  May trip the breaker."""
        self._failures += 1
        self._last_failure = time.monotonic()
        if self._failures >= self._threshold:
            if self._state != "open":
                _logger.warning("CircuitBreaker: tripped OPEN after %d failures.", self._failures)
            self._state = "open"

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def reset_timeout(self) -> float:
        """Seconds before a half-open probe is allowed after the breaker opens."""
        return self._reset_timeout

    @property
    def state(self) -> str:
        """Current state: ``"closed"``, ``"open"``, or ``"half_open"``."""
        return self._state
