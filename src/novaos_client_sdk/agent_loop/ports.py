"""What the loop depends on: a model client, a tool client and the tools it may call."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from .limits import Usage


@dataclass(frozen=True)
class ToolSpec:
    """A tool the model may call. A tool is a write unless it says it is read-only.

    ``repeatable`` marks a read-only tool that is expected to repeat with the same arguments,
    such as a status check; the stall guard lets it through, within the step limit.
    """

    name: str
    read_only: bool = False
    repeatable: bool = False
    description: str = ""
    parameters: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.repeatable and not self.read_only:
            raise ValueError(f"tool {self.name!r}: only a read-only tool can be repeatable")


@dataclass(frozen=True)
class ToolCall:
    name: str
    args: dict[str, Any] = field(default_factory=dict)
    id: str = ""


@dataclass(frozen=True)
class ModelTurn:
    """One model reply: a final answer, tool calls, or a request for input from a person.

    ``revises_plan`` counts against the plan-revision limit.
    """

    content: str | None = None
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage = field(default_factory=Usage)
    model: str = ""
    revises_plan: bool = False
    input_request: Any = None


class ModelClient(Protocol):
    async def complete(
        self, messages: list[dict[str, Any]], tools: Sequence[ToolSpec]
    ) -> ModelTurn: ...


class ToolClient(Protocol):
    async def call(
        self,
        name: str,
        args: dict[str, Any],
        *,
        task_id: str,
        idempotency_key: str | None,
        timeout_s: float,
    ) -> Any: ...
