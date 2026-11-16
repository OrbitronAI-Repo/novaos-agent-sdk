"""Loop limits, their platform caps and the usage they are checked against.

Limits come only from the constructor. Nothing here reads the environment: a limit an agent
could raise through a pod variable would not be a limit.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from enum import StrEnum
from typing import Any


class LimitName(StrEnum):
    STEPS = "steps"
    TOOL_CALLS = "tool_calls"
    TOKENS = "tokens"
    WALL_TIME = "wall_time"
    PLAN_REVISIONS = "plan_revisions"


@dataclass(frozen=True)
class PlatformCaps:
    """The highest value an agent may set for each limit."""

    max_steps: int = 50
    max_tool_calls: int = 100
    max_tokens: int = 500_000
    max_wall_time_s: float = 1_800.0
    max_plan_revisions: int = 10


PLATFORM_CAPS = PlatformCaps()


@dataclass(frozen=True)
class LoopLimits:
    """Per-run limits. A limit is reached when its counter equals the maximum, so the next unit
    of work would exceed it. Wall time counts from the start of each ``run`` or ``resume``."""

    max_steps: int = 8
    max_tool_calls: int = 12
    max_tokens: int = 60_000
    max_wall_time_s: float = 240.0
    max_plan_revisions: int = 3

    def __post_init__(self) -> None:
        for f in fields(self):
            value = getattr(self, f.name)
            cap = getattr(PLATFORM_CAPS, f.name)
            floor = 0 if f.name == "max_plan_revisions" else 1e-9
            if value < floor:
                raise ValueError(f"{f.name} must be positive, got {value}")
            if value > cap:
                raise ValueError(f"{f.name}={value} is above the platform cap of {cap}")


@dataclass
class Usage:
    """What a run has used so far. ``steps`` counts model calls."""

    steps: int = 0
    tool_calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    plan_revisions: int = 0
    tokens_by_model: dict[str, int] = field(default_factory=dict)

    @property
    def tokens(self) -> int:
        return self.tokens_in + self.tokens_out

    def add_model_call(self, model: str, tokens_in: int, tokens_out: int) -> None:
        self.steps += 1
        self.tokens_in += tokens_in
        self.tokens_out += tokens_out
        name = model or "unknown"
        self.tokens_by_model[name] = self.tokens_by_model.get(name, 0) + tokens_in + tokens_out

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "tokens": self.tokens}

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> Usage:
        data = dict(data or {})
        data.pop("tokens", None)
        return cls(**data)


def limit_reached(
    limits: LoopLimits, usage: Usage, time_left_s: float, *, count_steps: bool
) -> LimitName | None:
    """The first limit reached, in the order time, tokens, tool calls, steps.

    ``count_steps`` is false before a tool call: the step that asked for the call has already
    been counted, so its calls may still run.
    """
    if time_left_s <= 0:
        return LimitName.WALL_TIME
    if usage.tokens >= limits.max_tokens:
        return LimitName.TOKENS
    if usage.tool_calls >= limits.max_tool_calls:
        return LimitName.TOOL_CALLS
    if count_steps and usage.steps >= limits.max_steps:
        return LimitName.STEPS
    return None
