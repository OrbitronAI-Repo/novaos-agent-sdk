"""
Evaluation data model for NovaOS agents.

Defines the canonical Transcript dataclass that captures a complete agent execution record,
plus supporting types (Score, ContextItem, ToolCall, TokenUsage, TimingMetrics, ErrorRecord).
Adapted from the Agent_Evaluation_Guide.md gold standard for the NovaOS ACP execution model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ScoreDataType(StrEnum):
    NUMERIC = "NUMERIC"
    BOOLEAN = "BOOLEAN"
    CATEGORICAL = "CATEGORICAL"


@dataclass
class Score:
    """A single evaluation score produced by a grader."""

    name: str
    value: float | None = None
    comment: str = ""
    data_type: ScoreDataType = ScoreDataType.NUMERIC


@dataclass
class ContextItem:
    """A retrieved context chunk (RAG) used during agent execution."""

    content: str = ""
    source: str = ""
    score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolCall:
    """Record of a single MCP tool invocation.

    .. admonition:: ``arguments`` and ``result`` are recorded unmasked

       Connector calls routinely pass customer data as tool arguments (e.g. a
       ClickUp task body, an email address in a Slack message). Every governed
       tool call — including :meth:`~novaos_client_sdk.tools.ConnectorTool.call`
       — records one of these **automatically**, via
       ``_BaseToolBinding._record_eval``, whenever an ``EvaluationContext`` is
       active; there is no opt-out short of not having one active. The manual
       :meth:`~novaos_client_sdk.evaluation.context.EvaluationContext.track_tool`
       / ``add_tool_call`` helpers go through the exact same unmasked path for
       hand-instrumented (non-MCP-gateway) calls. This is a **separate,
       unrelated mechanism** from
       ``AgentBuilder(mask_output_pii=True)``, which only masks the ACP
       response (``SkillResult.output``/``.metadata``/``.error``) and has no
       effect on this transcript. The default posture today is: whatever a
       skill passes as tool arguments/result ends up in the eval transcript in
       the clear, and flows wherever that transcript flows (see
       :mod:`novaos_client_sdk.evaluation.context`'s module docstring and
       ``docs/NovaOS_Agent_Evaluation_Guide.md`` for where that is).
    """

    tool_name: str = ""
    arguments: dict[str, Any] = field(default_factory=dict)
    result: Any = None
    success: bool = True
    error: str = ""
    latency_ms: float = 0.0
    timestamp: str = ""


@dataclass
class TokenUsage:
    """Token consumption for a single LLM call or aggregated across an execution."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    model: str = ""


@dataclass
class TimingMetrics:
    """Timing breakdown for an agent execution."""

    total_ms: float = 0.0
    first_token_ms: float = 0.0
    llm_ms: float = 0.0
    tool_ms: float = 0.0
    retrieval_ms: float = 0.0
    overhead_ms: float = 0.0


@dataclass
class ErrorRecord:
    """A single error that occurred during execution."""

    error_type: str = ""
    message: str = ""
    timestamp: str = ""
    recoverable: bool = False


@dataclass
class Transcript:
    """Complete record of a NovaOS agent execution -- foundation for all evaluation.

    Captures identity, ACP context, prompt versioning, model info, I/O, RAG contexts,
    MCP tool calls, token usage, timing, errors, experiment metadata, and custom tags.
    Graders receive this (as a dict via ``to_dict()``) to produce Scores.
    """

    # Identity
    trace_id: str = ""
    run_id: str = ""
    agent_name: str = ""
    agent_version: str = "0.0.0"

    # ACP context (from AgentRuntimeContext)
    thread_id: str = ""
    session_id: str | None = None
    user_id: str | None = None
    tenant_id: str | None = None
    instance_id: str | None = None

    # Environment
    environment: str = "development"

    # Skill
    skill_name: str = ""

    # Prompt versioning
    prompt_template_id: str | None = None
    prompt_template_version: str | None = None
    prompt_template_hash: str | None = None
    system_prompt: str | None = None
    prompt_variables: dict[str, Any] | None = None

    # Model info
    model_provider: str | None = None
    model_name: str | None = None
    model_parameters: dict[str, Any] | None = None

    # Input / Output
    user_input: str = ""
    output: str = ""

    # RAG contexts
    contexts: list[ContextItem] = field(default_factory=list)

    # MCP tool calls
    tool_calls: list[ToolCall] = field(default_factory=list)

    # LLM calls (raw records for multi-turn or multi-model)
    llm_calls: list[dict[str, Any]] = field(default_factory=list)

    # Token usage (aggregated)
    tokens: TokenUsage = field(default_factory=TokenUsage)

    # Timing
    timing: TimingMetrics = field(default_factory=TimingMetrics)

    # Errors
    errors: list[ErrorRecord] = field(default_factory=list)

    # A/B testing / experiments
    experiment_id: str | None = None
    variant_id: str | None = None
    feature_flags: dict[str, Any] | None = None

    # Custom metadata
    custom_metadata: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a flat dict for graders and Langfuse metadata."""
        return {
            "trace_id": self.trace_id,
            "run_id": self.run_id,
            "agent_name": self.agent_name,
            "agent_version": self.agent_version,
            "thread_id": self.thread_id,
            "session_id": self.session_id,
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "instance_id": self.instance_id,
            "environment": self.environment,
            "skill_name": self.skill_name,
            "prompt_template_id": self.prompt_template_id,
            "prompt_template_version": self.prompt_template_version,
            "prompt_template_hash": self.prompt_template_hash,
            "system_prompt": self.system_prompt,
            "prompt_variables": self.prompt_variables,
            "model_provider": self.model_provider,
            "model_name": self.model_name,
            "model_parameters": self.model_parameters,
            "user_input": self.user_input,
            "output": self.output,
            "contexts": [
                {
                    "content": c.content,
                    "source": c.source,
                    "score": c.score,
                    "metadata": c.metadata,
                }
                for c in self.contexts
            ],
            "tool_calls": [
                {
                    "tool_name": t.tool_name,
                    "arguments": t.arguments,
                    "result": t.result,
                    "success": t.success,
                    "error": t.error,
                    "latency_ms": t.latency_ms,
                    "timestamp": t.timestamp,
                }
                for t in self.tool_calls
            ],
            "llm_calls": self.llm_calls,
            "tokens": {
                "prompt_tokens": self.tokens.prompt_tokens,
                "completion_tokens": self.tokens.completion_tokens,
                "total_tokens": self.tokens.total_tokens,
                "cost_usd": self.tokens.cost_usd,
                "model": self.tokens.model,
            },
            "timing": {
                "total_ms": self.timing.total_ms,
                "first_token_ms": self.timing.first_token_ms,
                "llm_ms": self.timing.llm_ms,
                "tool_ms": self.timing.tool_ms,
                "retrieval_ms": self.timing.retrieval_ms,
                "overhead_ms": self.timing.overhead_ms,
            },
            "errors": [
                {
                    "error_type": e.error_type,
                    "message": e.message,
                    "timestamp": e.timestamp,
                    "recoverable": e.recoverable,
                }
                for e in self.errors
            ],
            "experiment_id": self.experiment_id,
            "variant_id": self.variant_id,
            "feature_flags": self.feature_flags,
            "custom_metadata": self.custom_metadata,
            "tags": self.tags,
        }
