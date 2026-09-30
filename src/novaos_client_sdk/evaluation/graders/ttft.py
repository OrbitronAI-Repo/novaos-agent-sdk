"""Grader that scores time-to-first-token for streaming responses."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score

DEFAULT_MAX_TTFT_MS = 2_000


class TTFTGrader(Grader):
    """Scores TTFT: 1.0 if within budget, linear decay to 0 at 2x budget."""

    def __init__(self, max_ttft_ms: float = DEFAULT_MAX_TTFT_MS):
        self._max_ms = max_ttft_ms

    def grade(self, transcript: dict[str, Any]) -> Score:
        timing = transcript.get("timing") or {}
        ttft_ms = float(timing.get("first_token_ms") or 0.0)

        if ttft_ms <= 0:
            return Score(name="ttft", value=1.0, comment="No TTFT data (non-streaming)")

        if ttft_ms <= self._max_ms:
            value = 1.0
        elif ttft_ms >= self._max_ms * 2:
            value = 0.0
        else:
            value = 1.0 - (ttft_ms - self._max_ms) / self._max_ms

        return Score(
            name="ttft",
            value=round(value, 4),
            comment=f"{ttft_ms:.0f}ms (budget: {self._max_ms:.0f}ms)",
        )
