"""A2A ``Task`` types for the NovaOS client SDK.

Companion to :mod:`novaos_client_sdk.a2a_types`, which owns the ``Message`` /
``Part`` family.  These types implement the **task-centric** half of the **A2A
protocol specification v1.0.1** (`a2aproject/A2A`, tag ``v1.0.1``,
``specification/a2a.proto``) — the objects an agent *returns* rather than the
ones it receives.

Why a separate module rather than more of ``a2a_types``: the message family and
the task family are used by different halves of the SDK (the builder emits
tasks; the client consumes messages), and keeping them apart means neither
side's work has to move through the other's file.

**Wire format.**  A2A v1.0 defines its data structures in protobuf, and every
JSON binding carries the **proto3 JSON mapping** of them: ``lowerCamelCase``
field names, and enums serialized as their full proto value names.  That is why
:class:`TaskState`'s values are the ``TASK_STATE_*`` strings and not v0.3's
lowercase ``"completed"`` / ``"input-required"`` spellings — a v1.0 peer reads
the enum value name off the wire.

**Both directions, since the client transport landed.**  This module was emit-only while ``AgentBuilder``
was its only user: an agent produces tasks, and parsing one off the wire was left
to the work that had a transport to feed it.  :class:`~novaos_client_sdk.client.Client`
is that transport, so ``from_dict`` now exists on both classes here and
:class:`TaskList` models what ``ListTasks`` answers.

**Reads are coerced, not trusted**, exactly as
:meth:`novaos_client_sdk.a2a_types.A2AMessage.from_dict` is and for the same
reason: a ``Task`` arrives from a remote agent, so every field in it
is untrusted input.  An unparseable ``state`` degrades to
:attr:`TaskState.UNSPECIFIED` rather than raising, because a client that cannot
read one field of a task should still be able to read its id — and because a
future spec revision adding a state must not turn every response into an
exception.

Design notes: the state mapping and the spec-version
pin are documented in the migration's architecture decision record.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final

from .a2a_artifact import Artifact
from .a2a_types import A2AMessage, as_dict, as_int, as_str

__all__ = [
    "INTERRUPT_TASK_STATES",
    "TERMINAL_TASK_STATES",
    "Task",
    "TaskList",
    "TaskState",
    "TaskStatus",
    "task_state_from_wire",
    "utc_timestamp",
]


class TaskState(StrEnum):
    """A2A ``TaskState`` (spec v1.0.1).

    Values are the proto enum's value names, which is what proto3 JSON emits.
    The ACP statuses these replace (ADR F2):

    ==========================  ==========================
    ACP                         A2A
    ==========================  ==========================
    ``created``                 :attr:`SUBMITTED`
    ``in-progress``             :attr:`WORKING`
    ``completed``               :attr:`COMPLETED`
    ``failed``                  :attr:`FAILED`
    ``awaiting_approval``       :attr:`INPUT_REQUIRED`
    ==========================  ==========================

    ``awaiting_approval`` was a custom NovaOS field on an ACP run; under A2A the
    pause is a first-class protocol state.
    """

    UNSPECIFIED = "TASK_STATE_UNSPECIFIED"
    SUBMITTED = "TASK_STATE_SUBMITTED"
    WORKING = "TASK_STATE_WORKING"
    COMPLETED = "TASK_STATE_COMPLETED"
    FAILED = "TASK_STATE_FAILED"
    CANCELED = "TASK_STATE_CANCELED"
    INPUT_REQUIRED = "TASK_STATE_INPUT_REQUIRED"
    REJECTED = "TASK_STATE_REJECTED"
    AUTH_REQUIRED = "TASK_STATE_AUTH_REQUIRED"


#: States after which a task will emit no further updates.  A caller streaming a
#: task stops when it sees one of these; a stream that closes *without* one is a
#: transport failure, not a completed run — the distinction this design turns on.
TERMINAL_TASK_STATES: Final[frozenset[TaskState]] = frozenset(
    {
        TaskState.COMPLETED,
        TaskState.FAILED,
        TaskState.CANCELED,
        TaskState.REJECTED,
    }
)

#: States where the task has paused for the caller and *will* continue once the
#: caller supplies something.  Deliberately not part of
#: :data:`TERMINAL_TASK_STATES`: treating :attr:`TaskState.INPUT_REQUIRED` as
#: terminal is the misclassification this design exists to prevent.
INTERRUPT_TASK_STATES: Final[frozenset[TaskState]] = frozenset(
    {
        TaskState.INPUT_REQUIRED,
        TaskState.AUTH_REQUIRED,
    }
)


def task_state_from_wire(value: Any) -> TaskState:
    """Read a ``TaskState`` off the wire, degrading unknown values rather than raising.

    A response comes from a remote agent, so ``state`` can be a spelling this
    release has never seen — a future spec revision's new state, or simply
    garbage.  Neither justifies an exception out of a parser whose caller is
    trying to find out what happened to its task, so anything unrecognized reads
    as :attr:`TaskState.UNSPECIFIED`, which already means "no usable state".

    v0.3's lowercase spellings (``"completed"``, ``"input-required"``) are
    **not** accepted.  Tolerating them here would let a v0.3 peer look like it
    was working while every other part of the exchange failed, which is the
    looks-compliant-but-is-not shape ADR F18 found; a v0.3 pod is rejected at the
    version header long before its task states matter.
    """
    try:
        return TaskState(value)
    except ValueError:
        return TaskState.UNSPECIFIED


def utc_timestamp() -> str:
    """Return an RFC 3339 / ISO 8601 UTC timestamp.

    A2A's ``TaskStatus.timestamp`` is a protobuf ``Timestamp``, which proto3
    JSON renders as an RFC 3339 string in UTC with a ``Z`` suffix.
    ``datetime.isoformat()`` writes ``+00:00`` for UTC, so the offset is
    normalized here rather than at each call site.

    **Always fixed-width, including the fractional seconds** — ``strftime``'s
    ``%f`` is always exactly six digits, unlike ``isoformat()``, which omits
    the fraction entirely when the microsecond happens to be zero. That
    omission is not merely cosmetic: two RFC 3339 UTC-Z timestamps of
    *consistent* width compare correctly with a plain string comparison, but
    ``"...T00:00:00Z"`` (no fraction) sorts **after**
    ``"...T00:00:00.500000Z"`` lexicographically — ``"Z" > "."`` in ASCII —
    even though the first instant is earlier. ``ListTasks``'
    ``statusTimestampAfter`` filter in ``agent_builder.py`` relies on exactly
    this format being comparable as a plain string, which a timestamp that
    ever dropped its fraction would silently violate.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@dataclass
class TaskStatus:
    """A2A ``TaskStatus`` (spec v1.0.1) — a task's state plus optional detail.

    ``message`` is how an agent says *why* a task reached this state.  A
    terminal state without one leaves the caller with a finished run and no
    explanation, which is the failure mode recorded in KB-15 — so
    :class:`~novaos_client_sdk.agent_builder.AgentBuilder` always attaches one
    when it fails a task.
    """

    state: TaskState
    message: A2AMessage | None = None
    timestamp: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"state": str(self.state)}
        if self.message is not None:
            out["message"] = self.message.to_dict()
        if self.timestamp is not None:
            out["timestamp"] = self.timestamp
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskStatus":
        """Create from the A2A JSON wire shape.

        Args:
            data: An A2A ``TaskStatus`` JSON object.  Typed ``Any`` because this
                is public API over untrusted wire data.
        """
        obj = as_dict(data)
        raw_message = obj.get("message")
        return cls(
            state=task_state_from_wire(obj.get("state")),
            message=A2AMessage.from_dict(raw_message) if isinstance(raw_message, dict) else None,
            timestamp=as_str(obj.get("timestamp")) or None,
        )


@dataclass
class Task:
    """A2A ``Task`` (spec v1.0.1) — the unit of work a ``SendMessage`` creates.

    ``artifacts`` holds the task's outputs, modelled as
    :class:`novaos_client_sdk.a2a_artifact.Artifact`.  It is what replaces ACP's
    flat, unstructured ``Run.output``.

    **``status.message`` is not redundant with it.**  The two answer different
    questions and A2A keeps both: the artifact is *what the task produced*, the
    status message is *what the agent has to say about reaching this state*.  A
    terminal task always carries the latter — KB-15's failure mode is a finished
    run with no explanation — while a task that failed before producing anything
    legitimately carries no artifacts at all.
    """

    id: str
    status: TaskStatus
    context_id: str | None = None
    artifacts: list[Artifact] = field(default_factory=list)
    history: list[A2AMessage] = field(default_factory=list)
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"id": self.id, "status": self.status.to_dict()}
        if self.context_id is not None:
            out["contextId"] = self.context_id
        if self.artifacts:
            out["artifacts"] = [a.to_dict() for a in self.artifacts]
        if self.history:
            out["history"] = [m.to_dict() for m in self.history]
        if self.metadata:
            out["metadata"] = self.metadata
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "Task":
        """Create from the A2A JSON wire shape.

        Accepts the JSON-RPC binding's ``camelCase`` keys and the gRPC binding's
        ``snake_case`` equivalents, matching
        :meth:`novaos_client_sdk.a2a_types.A2AMessage.from_dict`.

        A task with no readable ``status`` still parses, carrying
        :attr:`TaskState.UNSPECIFIED`.  The alternative — raising — would leave a
        caller unable to learn even the task's id from a response it did get,
        and ``id`` is the field that lets it retry with ``GetTask``.

        Args:
            data: An A2A ``Task`` JSON object.  Typed ``Any`` because this is
                public API over untrusted wire data.
        """
        obj = as_dict(data)
        artifacts = obj.get("artifacts")
        history = obj.get("history")
        return cls(
            id=as_str(obj.get("id")),
            status=TaskStatus.from_dict(obj.get("status")),
            context_id=as_str(obj.get("contextId", obj.get("context_id"))) or None,
            # Non-object members are dropped rather than degraded, the same way
            # `A2AMessage.from_dict` treats a part that was never an object.
            artifacts=(
                [Artifact.from_dict(a) for a in artifacts if isinstance(a, dict)]
                if isinstance(artifacts, list)
                else []
            ),
            history=(
                [A2AMessage.from_dict(m) for m in history if isinstance(m, dict)]
                if isinstance(history, list)
                else []
            ),
            metadata=as_dict(obj.get("metadata")) or None,
        )

    @property
    def is_terminal(self) -> bool:
        """Has this task finished for good?

        ``True`` only for :data:`TERMINAL_TASK_STATES`.  A task paused on the
        caller (:data:`INTERRUPT_TASK_STATES`) is **not** terminal even though
        its stream closes — treating it as one is the misclassification this design
        exists to prevent, and it is why :attr:`is_interrupted` is separate
        rather than folded in here.
        """
        return self.status.state in TERMINAL_TASK_STATES

    @property
    def is_interrupted(self) -> bool:
        """Has this task paused waiting for the caller to supply something?"""
        return self.status.state in INTERRUPT_TASK_STATES


@dataclass
class TaskList:
    """A2A ``ListTasksResponse`` (spec v1.0.1) — one page of an agent's tasks.

    ``ListTasks`` had **no SDK representation at all** before this class: ADR
    finding F13 caught that it was missing from the migration's original client
    method list, so nothing modelled either the request or its answer.  It is the
    one A2A operation with no ACP counterpart whatsoever — ACP could not
    enumerate an agent's runs — which is why there is nothing here to map from.

    ``next_page_token`` is the whole of the pagination contract: a non-empty
    value means another page exists and is fetched by passing it back as
    ``page_token``.  ``total_size`` is advisory — the spec does not require an
    agent to know its total — so a caller paginates on the token and treats the
    count as a display hint, never as a loop bound.
    """

    tasks: list[Task] = field(default_factory=list)
    next_page_token: str | None = None
    page_size: int | None = None
    total_size: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"tasks": [t.to_dict() for t in self.tasks]}
        if self.next_page_token:
            out["nextPageToken"] = self.next_page_token
        if self.page_size is not None:
            out["pageSize"] = self.page_size
        if self.total_size is not None:
            out["totalSize"] = self.total_size
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskList":
        """Create from the A2A JSON wire shape.

        Args:
            data: An A2A ``ListTasksResponse`` JSON object.  Typed ``Any``
                because this is public API over untrusted wire data.
        """
        obj = as_dict(data)
        tasks = obj.get("tasks")
        return cls(
            tasks=(
                [Task.from_dict(t) for t in tasks if isinstance(t, dict)]
                if isinstance(tasks, list)
                else []
            ),
            next_page_token=as_str(obj.get("nextPageToken", obj.get("next_page_token"))) or None,
            page_size=as_int(obj.get("pageSize", obj.get("page_size"))),
            total_size=as_int(obj.get("totalSize", obj.get("total_size"))),
        )
