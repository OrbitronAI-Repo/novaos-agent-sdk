"""A2A ``TaskPushNotificationConfig`` types for the NovaOS client SDK.

Push notification is the A2A mechanism for learning that a task finished
without holding a stream open or polling: a caller registers a webhook URL
against a task, and the agent (or the gateway fronting it) posts to it when
the task's status changes.  The gateway already serves the four JSON-RPC
methods this module's types travel over; this is the client half that was
missing.

**Flat, not nested — heed F20.**  The ADR's own description of
``TaskPushNotificationConfig`` was v0.3-shaped: v0.3 wrapped ``url`` and
``token`` in a nested ``pushNotificationConfig`` object alongside an outer
``taskId``.  v1.0 **merged** that nested object's fields directly onto
``TaskPushNotificationConfig`` — there is no nested config any more — and
**pluralised** the list method (``ListTaskPushNotificationConfigs``, not
``ListTaskPushNotificationConfig``).  :class:`TaskPushNotificationConfig`
below models the v1.0, flat shape; ``tests/test_a2a_v1_conformance.py`` pins
that the v0.3 nested shape is rejected, the same guard F18 established for the
message envelope.

**Wire format.**  A2A v1.0 defines this message in ``specification/a2a.proto``
and JSON-RPC carries its proto3 JSON mapping: ``lowerCamelCase`` field names.
Reads accept the JSON-RPC binding's ``camelCase`` keys and the gRPC binding's
``snake_case`` equivalents, and never raise — the same tolerant-read rule the
rest of the A2A type family follows: a config a caller reads back
off the wire is untrusted input, and a parser that raises on one bad field
leaves the caller unable to read the fields that *did* survive.

**``token`` and ``authentication.credentials`` are secrets.**  Ownership of
each is the webhook receiver's problem, but this SDK holds them in memory and
must never let them reach a log line.  ``mask_pii_in_dict``'s key-aware
backstop (``pii.py``) treats any key whose name contains ``token`` or
``credential`` as sensitive regardless of its content — added by this ticket
specifically so a caller who logs ``config.to_dict()`` via
``extra={...}`` does not leak either value; ``tests/test_pii_in_logging.py``
covers it.  Nothing in this module logs on its own — the risk is a caller
handing the dict to :func:`novaos_client_sdk.logging.get_agent_logger`, not
anything this module does — so there is no logging code here to audit, only
the wire types the masking has to recognise by key name.
"""

from dataclasses import dataclass, field
from typing import Any

from .a2a_types import as_dict, as_str

__all__ = [
    "AuthenticationInfo",
    "TaskPushNotificationConfig",
    "TaskPushNotificationConfigList",
]


@dataclass
class AuthenticationInfo:
    """A2A ``AuthenticationInfo`` — how a receiver should authenticate a push.

    ``credentials`` is a secret (a bearer token, a shared secret, …) the
    receiving webhook uses to verify the call came from the agent it
    registered with.  It travels in the clear on this object exactly as A2A
    defines it — this SDK does not seal it the way ``a2a_sealed_envelope``
    seals NovaOS's own secret delivery — so it must never be logged; see the
    module docstring.
    """

    scheme: str = ""
    credentials: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"scheme": self.scheme}
        if self.credentials:
            out["credentials"] = self.credentials
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AuthenticationInfo":
        """Create from the A2A JSON wire shape.

        Args:
            data: An A2A ``AuthenticationInfo`` JSON object.  Typed ``Any``
                because this is public API over untrusted wire data.
        """
        obj = as_dict(data)
        return cls(
            scheme=as_str(obj.get("scheme")),
            credentials=as_str(obj.get("credentials")) or None,
        )


@dataclass
class TaskPushNotificationConfig:
    """A2A ``TaskPushNotificationConfig`` (spec v1.0.1) — a task's webhook registration.

    **Flat (F20)**: ``url`` and ``token`` are fields on this object directly,
    not on a nested ``pushNotificationConfig`` — see the module docstring.
    ``id`` identifies *this* registration, distinct from ``task_id``: a task
    may have more than one config registered against it (v1.0 §3.1.7), which
    is the whole reason ``ListTaskPushNotificationConfigs`` exists rather than
    the caller simply remembering the one it set.

    ``token`` is a secret the agent should echo back on every push so the
    receiver can verify the call is not forged; ``authentication`` covers the
    receiver-side check going the other way.  Both must never reach a log
    line — see the module docstring.
    """

    task_id: str = ""
    id: str = ""
    url: str = ""
    token: str | None = None
    authentication: AuthenticationInfo | None = None
    tenant: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase).

        ``taskId``, ``id`` and ``url`` are always emitted, matching how the
        rest of the A2A type family (:class:`novaos_client_sdk.a2a_task.Task`,
        for one) always emits its own REQUIRED-shaped identifier fields.
        """
        out: dict[str, Any] = {"taskId": self.task_id, "id": self.id, "url": self.url}
        if self.token:
            out["token"] = self.token
        if self.authentication is not None:
            out["authentication"] = self.authentication.to_dict()
        if self.tenant:
            out["tenant"] = self.tenant
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskPushNotificationConfig":
        """Create from the A2A JSON wire shape.

        Args:
            data: An A2A ``TaskPushNotificationConfig`` JSON object.  Typed
                ``Any`` because this is public API over untrusted wire data.
        """
        obj = as_dict(data)
        raw_auth = obj.get("authentication")
        return cls(
            task_id=as_str(obj.get("taskId", obj.get("task_id"))),
            id=as_str(obj.get("id")),
            url=as_str(obj.get("url")),
            token=as_str(obj.get("token")) or None,
            authentication=(
                AuthenticationInfo.from_dict(raw_auth) if isinstance(raw_auth, dict) else None
            ),
            tenant=as_str(obj.get("tenant")) or None,
        )


@dataclass
class TaskPushNotificationConfigList:
    """A2A ``ListTaskPushNotificationConfigsResponse`` (spec v1.0.1) — one page.

    Mirrors :class:`novaos_client_sdk.a2a_task.TaskList`'s pagination contract
    exactly, per the ticket's own instruction to reuse that pattern rather
    than invent a second one: ``next_page_token`` is the whole of it — a
    non-empty value means another page exists and is fetched by passing it
    back as ``page_token``.
    """

    configs: list[TaskPushNotificationConfig] = field(default_factory=list)
    next_page_token: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"configs": [c.to_dict() for c in self.configs]}
        if self.next_page_token:
            out["nextPageToken"] = self.next_page_token
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskPushNotificationConfigList":
        """Create from the A2A JSON wire shape.

        Args:
            data: A ``ListTaskPushNotificationConfigsResponse`` JSON object.
                Typed ``Any`` because this is public API over untrusted wire
                data.
        """
        obj = as_dict(data)
        configs = obj.get("configs")
        return cls(
            configs=(
                [TaskPushNotificationConfig.from_dict(c) for c in configs if isinstance(c, dict)]
                if isinstance(configs, list)
                else []
            ),
            next_page_token=as_str(obj.get("nextPageToken", obj.get("next_page_token"))) or None,
        )
