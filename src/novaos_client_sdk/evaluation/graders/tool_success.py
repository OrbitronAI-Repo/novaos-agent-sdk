"""Grader that scores MCP tool call success rate."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score


class ToolCallSuccessGrader(Grader):
    """Scores the proportion of tool calls that succeeded (``ToolResponse.success``)."""

    def grade(self, transcript: dict[str, Any]) -> Score:
        tool_calls = transcript.get("tool_calls") or []
        if not isinstance(tool_calls, list):
            tool_calls = []
        if not tool_calls:
            return Score(name="tool_call_success", value=1.0, comment="No tool calls made")

        successes = sum(1 for tc in tool_calls if isinstance(tc, dict) and tc.get("success", False))
        rate = successes / len(tool_calls)
        return Score(
            name="tool_call_success",
            value=round(rate, 4),
            comment=f"{successes}/{len(tool_calls)} tool calls succeeded",
        )
