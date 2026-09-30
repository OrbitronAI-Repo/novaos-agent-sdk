"""Grader that scores LLM cost against a budget.

When the LLM Gateway reports ``cost_usd`` on the transcript, that value is used
directly.  When ``cost_usd`` is 0 (e.g. local development, or a provider that
doesn't report cost), the grader falls back to a **static pricing table**
(ported from Galaxy) that estimates cost from ``prompt_tokens``, ``completion_tokens``,
and ``model``.
"""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score

DEFAULT_MAX_COST_USD = 0.10

# $/1M tokens — kept in sync with major provider pricing pages.
FALLBACK_PRICING: dict[str, dict[str, float]] = {
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
    "gpt-4": {"input": 30.00, "output": 60.00},
    "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
    "claude-3-5-sonnet": {"input": 3.00, "output": 15.00},
    "claude-3-5-haiku": {"input": 0.80, "output": 4.00},
    "claude-3-opus": {"input": 15.00, "output": 75.00},
    "claude-3-haiku": {"input": 0.25, "output": 1.25},
    "claude-4-sonnet": {"input": 3.00, "output": 15.00},
    "default": {"input": 5.00, "output": 15.00},
}


def _estimate_cost(tokens: dict[str, Any]) -> float:
    """Estimate cost from token counts and model name using the fallback table."""
    model = str(tokens.get("model", "default")).lower()
    input_tokens = int(tokens.get("prompt_tokens") or 0)
    output_tokens = int(tokens.get("completion_tokens") or 0)
    if input_tokens == 0 and output_tokens == 0:
        return 0.0

    if model in FALLBACK_PRICING:
        pricing = FALLBACK_PRICING[model]
    else:
        pricing = FALLBACK_PRICING["default"]
        for prefix, p in sorted(FALLBACK_PRICING.items(), key=lambda kv: len(kv[0]), reverse=True):
            if prefix != "default" and prefix in model:
                pricing = p
                break

    return (input_tokens * pricing["input"] / 1_000_000) + (
        output_tokens * pricing["output"] / 1_000_000
    )


class CostGrader(Grader):
    """Scores cost: 1.0 if within budget, linear decay to 0 at 2x budget.

    Uses ``cost_usd`` from the transcript when available (reported by the LLM
    Gateway); falls back to a static pricing table otherwise.
    """

    def __init__(self, max_cost_usd: float = DEFAULT_MAX_COST_USD):
        self._max_usd = max_cost_usd

    def grade(self, transcript: dict[str, Any]) -> Score:
        tokens = transcript.get("tokens") or {}
        cost = float(tokens.get("cost_usd") or 0.0)

        if cost <= 0:
            cost = _estimate_cost(tokens)

        if cost <= 0:
            return Score(name="cost", value=1.0, comment="No cost data")

        if cost <= self._max_usd:
            value = 1.0
        elif cost >= self._max_usd * 2:
            value = 0.0
        else:
            value = 1.0 - (cost - self._max_usd) / self._max_usd

        return Score(
            name="cost",
            value=round(value, 4),
            comment=f"${cost:.6f} (budget: ${self._max_usd:.4f})",
        )
