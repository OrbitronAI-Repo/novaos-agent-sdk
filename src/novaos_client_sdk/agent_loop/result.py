"""Stop reasons and the result of a run, and how they map onto a skill result."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal, cast

from ..agent_types import SkillResult
from .limits import LimitName, Usage


class StopReason(StrEnum):
    """Why a run stopped. Closed: every run that stops has exactly one of these."""

    DONE = "done"
    LIMIT_REACHED = "limit_reached"
    STALLED = "stalled"
    CANCELLED = "cancelled"
    FAILED = "failed"


def approval_fields(request: Any) -> tuple[str, str, str] | None:
    """``(question, call_id, tool_name)`` when a pause request has all three, else None."""
    if not isinstance(request, dict):
        return None
    question, call_id, tool_name = (request.get(k) for k in ("question", "call_id", "tool_name"))
    if all(isinstance(v, str) and v for v in (question, call_id, tool_name)):
        return cast("tuple[str, str, str]", (question, call_id, tool_name))
    return None


#: The ``action_id`` values the platform treats as approving a write (``approve``, ``confirm``, ``retry``).
APPROVE_ACTION_IDS = frozenset({"approve", "confirm", "retry"})


def parse_decision(message: Any) -> bool | None:
    """True or False when ``message`` is the platform's decision on an approval, else None.

    The platform sends a decision as JSON text, ``{"type": "hitl_response", "action_id": ...,
    "approved": ...}``, or a client sends ``{"hitl_response": {"action_id": ...}}``. Free text is
    never a decision. When ``action_id`` and ``approved`` are both present, both must approve.
    """
    if isinstance(message, list):
        for item in reversed(message):
            content = item.get("content") if isinstance(item, dict) else item
            decision = parse_decision(content)
            if decision is not None:
                return decision
        return None
    record = message
    if isinstance(message, str):
        try:
            record = json.loads(message)
        except ValueError:
            return None
    if not isinstance(record, dict):
        return None
    if record.get("type") != "hitl_response":
        record = record.get("hitl_response")
        if not isinstance(record, dict):
            return None
    action, approved = record.get("action_id"), record.get("approved")
    votes = []
    if isinstance(action, str):
        votes.append(action in APPROVE_ACTION_IDS)
    if isinstance(approved, bool):
        votes.append(approved)
    return all(votes) if votes else None


def approval_request(
    request: Any, held_writes: Sequence[dict[str, Any]]
) -> tuple[str, str, str] | None:
    """``(question, call_id, tool_name)`` when a pause asks to approve a write the turn holds.

    A pause is an approval only if it names a write call the model issued in the same turn: the
    platform binds the resume to that call, and a model's invented id or tool name would ask the
    person to approve a write that is never made. The platform resumes an approval on the same
    run, so the saved state is found; a plain question starts a new run.
    """
    fields = approval_fields(request)
    if fields is None:
        return None
    _, call_id, tool_name = fields
    held = any(c.get("id") == call_id and c.get("name") == tool_name for c in held_writes)
    return fields if held else None


@dataclass(frozen=True)
class LoopResult:
    """A stopped run (``stop_reason`` set) or a paused one (``input_request`` set).

    A pause for a person is not a stop: ``stop_reason`` is None and ``state`` is
    ``"input-required"``.
    """

    stop_reason: StopReason | None
    usage: Usage
    output: Any = None
    limit: LimitName | None = None
    error: str | None = None
    input_request: Any = None
    messages: list[dict[str, Any]] = field(default_factory=list)
    summary: str | None = None
    #: What the in-run check recorded for this run's answer, when the loop has a check.
    in_run_check: dict[str, Any] | None = None
    #: ``(question, call_id, tool_name)`` when this pause asks to approve a write the turn holds.
    approval: tuple[str, str, str] | None = None

    @property
    def paused(self) -> bool:
        return self.stop_reason is None

    @property
    def state(self) -> Literal["input-required", "stopped"]:
        return "input-required" if self.paused else "stopped"

    def to_skill_result(self) -> SkillResult:
        # The summary of older turns goes back with the result, for whoever stores it.
        extra: dict[str, Any] = {"summary": self.summary} if self.summary else {}
        if self.stop_reason is None:
            approval = self.approval
            output = approval[0] if approval else self.input_request
            return SkillResult(output=output, status="WAITING_USER", metadata=extra)
        metadata: dict[str, Any] = {"stop_reason": self.stop_reason.value, **extra}
        if self.in_run_check is not None:
            metadata["in_run_check"] = self.in_run_check
        if self.limit is not None:
            metadata["limit"] = self.limit.value
        if self.stop_reason is StopReason.DONE:
            return SkillResult(output=self.output, metadata=metadata)
        detail = self.error or (
            f"limit reached: {self.limit.value}" if self.limit else self.stop_reason.value
        )
        return SkillResult(output=self.output, status="FAILED", error=detail, metadata=metadata)
