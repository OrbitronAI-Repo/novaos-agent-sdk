"""The identity of the run whose skill is executing in the current task.

``AgentBuilder`` binds it for the length of each skill, inside the skill's own
task, so anything the skill does — directly or through a library — can find out
which run it belongs to without being handed ``ctx``: log lines written through
any logger (:class:`~novaos_client_sdk.logging.RunContextFilter`) and outbound
calls that need the run's ``traceparent`` (:class:`~novaos_client_sdk.mcp.MCPClient`).
Outside a skill there is no run, and :func:`current_run` returns ``None``.
"""

from __future__ import annotations

import base64
import json
import re
from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True)
class RunContext:
    """The ids that tie a signal to one run. Empty strings for ids the platform did not send."""

    run_id: str = ""
    trace_id: str = ""
    agent_id: str = ""
    tenant_id: str = ""
    instance_id: str = ""
    traceparent: str = ""
    task_id: str = ""
    plan_id: str = ""


# The shape the platform's own spans accept for an id: short and opaque, so a value that is
# really text never becomes a span attribute.
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_:-]{0,63}")


def plan_id_from_token(token: str) -> str:
    """The ``plan_id`` claim of an execution token, or ``""`` when there is none.

    A run that no plan started has no such claim, and a token that cannot be read or a claim
    that is not shaped like an id gives the same answer. The signature is not checked: the
    auth-proxy sidecar verified the token before the agent saw it, and this only labels spans.
    """
    try:
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        value = claims.get("plan_id")
    except (IndexError, ValueError, AttributeError):
        return ""
    return value if isinstance(value, str) and _ID.fullmatch(value) else ""


def run_span_attributes(*, task: bool = False) -> dict[str, str]:
    """Ids of the current run for a span: the plan, and with ``task`` the task. Ids only.

    Anything the run does not have is left out rather than set empty.
    """
    run = current_run()
    if run is None:
        return {}
    attributes = {"plan_id": run.plan_id}
    if task:
        attributes["novaos.task_id"] = run.task_id
    return {key: value for key, value in attributes.items() if value}


_current_run: ContextVar[RunContext | None] = ContextVar("novaos_current_run", default=None)


def current_run() -> RunContext | None:
    """The run whose skill is executing in this task, or ``None`` outside a skill."""
    return _current_run.get()


def bind_run(run: RunContext) -> Token[RunContext | None]:
    """Make ``run`` the current run; pass the returned token to :func:`reset_run`."""
    return _current_run.set(run)


def reset_run(token: Token[RunContext | None]) -> None:
    """Restore the run that was current before the matching :func:`bind_run`."""
    _current_run.reset(token)
