"""Grader that validates expected tool calls were made."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score


class ToolCallExpectedGrader(Grader):
    """Checks that a specified set of expected tools were actually invoked.

    Pass ``expected_tools`` in transcript's ``custom_metadata`` to activate.
    Score is the Jaccard overlap between expected and actual tool names.
    """

    def grade(self, transcript: dict[str, Any]) -> Score:
        metadata = transcript.get("custom_metadata") or {}
        raw_expected = metadata.get("expected_tools", [])
        if isinstance(raw_expected, str):
            expected = [raw_expected]
        elif isinstance(raw_expected, (list, tuple)):
            expected = list(raw_expected)
        else:
            expected = []

        if not expected:
            return Score(
                name="tool_expected",
                value=1.0,
                comment="No expected tools specified",
            )

        raw_calls = transcript.get("tool_calls") or []
        actual_names = {tc.get("tool_name", "") for tc in raw_calls if isinstance(tc, dict)}
        expected_set = set(expected)

        intersection = expected_set & actual_names
        union = expected_set | actual_names
        jaccard = len(intersection) / len(union) if union else 1.0

        missing = expected_set - actual_names
        extra = actual_names - expected_set
        parts = []
        if missing:
            parts.append(f"missing: {sorted(missing)}")
        if extra:
            parts.append(f"unexpected: {sorted(extra)}")

        return Score(
            name="tool_expected",
            value=round(jaccard, 4),
            comment="; ".join(parts) if parts else "All expected tools called",
        )
