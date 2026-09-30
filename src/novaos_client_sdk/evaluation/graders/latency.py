"""Grader that scores total execution latency against a threshold."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score

DEFAULT_MAX_LATENCY_MS = 10_000


class LatencyGrader(Grader):
    """Scores execution latency: 1.0 if within budget, linear decay to 0 at 2x budget."""

    def __init__(self, max_latency_ms: float = DEFAULT_MAX_LATENCY_MS):
        self._max_ms = max_latency_ms

    def grade(self, transcript: dict[str, Any]) -> Score:
        timing = transcript.get("timing") or {}
        total_ms = float(timing.get("total_ms") or 0.0)

        if total_ms <= 0:
            return Score(name="latency", value=1.0, comment="No timing data")

        if total_ms <= self._max_ms:
            value = 1.0
        elif total_ms >= self._max_ms * 2:
            value = 0.0
        else:
            value = 1.0 - (total_ms - self._max_ms) / self._max_ms

        return Score(
            name="latency",
            value=round(value, 4),
            comment=f"{total_ms:.0f}ms (budget: {self._max_ms:.0f}ms)",
        )
