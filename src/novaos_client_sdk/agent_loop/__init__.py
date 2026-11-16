"""A bounded agent loop runner, built on LangGraph.

``AgentLoop`` runs a model and its tools within set limits, ends every run with one stop reason
(``done``, ``limit_reached``, ``stalled``, ``cancelled``, ``failed``) and pauses for a person
with ``input-required``. It needs LangGraph: ``pip install 'novaos-agent-sdk[agent-loop]'``.
The limits, the stall guard, the key builder and the result types import without it.
"""

from __future__ import annotations

from typing import Any

from .check import (
    Check,
    GatewayJudge,
    InRunCheck,
    JudgeClient,
    JudgeRejected,
    JudgeReply,
    Verdict,
    build_judge_messages,
    output_schema,
    parse_verdict,
)
from .context import (
    ContextLayer,
    DefaultReusePolicy,
    ExtractiveSummarizer,
    MemoryClientReader,
    MemoryEntry,
    MemoryReader,
    ReusePolicy,
    Summarizer,
)
from .guards import StallGuard, idempotency_key
from .limits import PLATFORM_CAPS, LimitName, LoopLimits, PlatformCaps, Usage, limit_reached
from .ports import ModelClient, ModelTurn, ToolCall, ToolClient, ToolSpec
from .result import LoopResult, StopReason

__all__ = [
    "Check",
    "GatewayJudge",
    "InRunCheck",
    "JudgeClient",
    "JudgeRejected",
    "JudgeReply",
    "Verdict",
    "build_judge_messages",
    "output_schema",
    "parse_verdict",
    "PLATFORM_CAPS",
    "AgentLoop",
    "ContextLayer",
    "DefaultReusePolicy",
    "ExtractiveSummarizer",
    "LimitName",
    "LoopLimits",
    "LoopResult",
    "LoopState",
    "MemoryClientReader",
    "MemoryEntry",
    "MemoryReader",
    "ModelClient",
    "ModelTurn",
    "PlatformCaps",
    "ReusePolicy",
    "StallGuard",
    "StopReason",
    "Summarizer",
    "ToolCall",
    "ToolClient",
    "ToolSpec",
    "Usage",
    "build_default_graph",
    "idempotency_key",
    "limit_reached",
]


def __getattr__(name: str) -> Any:
    # The runner imports LangGraph, which the rest of this package must not require.
    if name in {"AgentLoop", "LoopState", "build_default_graph"}:
        try:
            from . import runner
        except ImportError as exc:
            raise ImportError(
                f"{name} needs LangGraph: pip install 'novaos-agent-sdk[agent-loop]'"
            ) from exc
        return getattr(runner, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
