"""When a saved tool result may be used again."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

DAY_S = 24 * 60 * 60
AUTHORITATIVE_FIELDS = frozenset(
    {"rating", "verdict", "email", "phone", "address", "date_of_birth", "national_id"}
)


@dataclass(frozen=True)
class Evidence:
    """A read-only tool result the run kept, and when it was fetched (epoch seconds)."""

    tool: str
    args: dict[str, Any]
    content: Any
    fetched_at: float


class ReusePolicy(Protocol):
    def refresh_reason(self, evidence: Evidence, now: float, *, resumed: bool) -> str | None:
        """Why to fetch ``evidence`` again, or None to use it as saved."""
        ...


def _keys(value: Any) -> set[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return set()
    if isinstance(value, dict):
        return set(value) | {k for v in value.values() for k in _keys(v)}
    if isinstance(value, list):
        return {k for v in value for k in _keys(v)}
    return set()


@dataclass(frozen=True)
class DefaultReusePolicy:
    """Evidence is fetched again when it is older than ``max_age_s``, and, after a pause, when it
    holds an authoritative field. Such a field is never trusted from a checkpoint."""

    max_age_s: float = DAY_S
    authoritative_fields: frozenset[str] = AUTHORITATIVE_FIELDS

    def refresh_reason(self, evidence: Evidence, now: float, *, resumed: bool) -> str | None:
        if now - evidence.fetched_at > self.max_age_s:
            return "stale"
        if resumed and _keys(evidence.content) & self.authoritative_fields:
            return "authoritative"
        return None
