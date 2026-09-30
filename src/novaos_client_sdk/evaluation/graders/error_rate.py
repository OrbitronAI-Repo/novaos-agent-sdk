"""Grader that scores based on the number of errors during execution."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score


class ErrorRateGrader(Grader):
    """Scores 1.0 for zero errors, penalizing 0.25 per non-recoverable error (floor 0)."""

    PENALTY_PER_ERROR = 0.25

    def grade(self, transcript: dict[str, Any]) -> Score:
        errors = transcript.get("errors") or []
        if not isinstance(errors, list):
            errors = []
        if not errors:
            return Score(name="error_rate", value=1.0, comment="No errors")

        non_recoverable = sum(
            1 for e in errors if isinstance(e, dict) and not e.get("recoverable", False)
        )
        penalty = non_recoverable * self.PENALTY_PER_ERROR
        value = max(0.0, 1.0 - penalty)

        return Score(
            name="error_rate",
            value=round(value, 4),
            comment=f"{non_recoverable} non-recoverable error(s) out of {len(errors)} total",
        )
