"""A2A streaming frames for the NovaOS client SDK.

Fourth module of the A2A type family, after :mod:`novaos_client_sdk.a2a_types`
(``Message`` / ``Part``), :mod:`novaos_client_sdk.a2a_task`
(``Task``) and :mod:`novaos_client_sdk.a2a_artifact` (``Artifact``).
This one models what arrives *over time* rather than what arrives
at the end: the frames ``SendStreamingMessage`` and ``SubscribeToTask`` deliver
over SSE.

It exists now because the SDK finally added the transport that carries these
frames.  ``a2a_artifact`` deliberately left ``TaskArtifactUpdateEvent``
unmodelled for exactly that reason — modelling a streaming frame before there
was a stream would have been a stub to migrate off — and noted that the work
which did add it would be "adding an envelope, not a model", which is what this
module is: :class:`~novaos_client_sdk.a2a_artifact.Artifact` and
:class:`~novaos_client_sdk.a2a_task.TaskStatus` do the work, and the classes here
say which task they belong to.

**``StreamResponse`` is a ``oneof``** (spec v1.0.1, ``a2a.proto``) over ``task``,
``message``, ``statusUpdate`` and ``artifactUpdate``, so — as everywhere else in
proto3 JSON — *the populated key is the discriminator*.  A2A **v0.3** tagged each
frame with ``kind: "status-update"`` and carried a ``final`` boolean; v1.0 has
neither.

**There is no ``final`` flag, and that is the point of this design.**  Because v1.0
removed it, a caller cannot ask a frame whether the stream is over — it has to
read the *task state*.  A stream that closes without a terminal or interrupt
state is a transport failure, not a completed run, and reporting it as success is
the bug this terminality rule exists to prevent.  :func:`is_last_frame` is where that rule
lives on this side so no consumer has to re-derive it.

**Parity with the TypeScript caller.**  The platform's A2A wire contract parses
these same frames for the Temporal worker and the gateway's pod-caller.  Both
accept a frame either bare
(``{"statusUpdate": …}``) or wrapped in a JSON-RPC success envelope, for the same
reason: ``a2a-sdk`` emits the envelope form and a hand-rolled pod server should
not be silently unreadable.

Design notes: the A2A spec version pin, the wire
format, and this terminality rule are documented in the migration's architecture decision record.
"""

from dataclasses import dataclass
from typing import Any

from .a2a_artifact import Artifact
from .a2a_task import (
    INTERRUPT_TASK_STATES,
    TERMINAL_TASK_STATES,
    Task,
    TaskStatus,
)
from .a2a_types import A2AMessage, as_bool, as_dict, as_str

__all__ = [
    "STREAM_FRAME_KEYS",
    "StreamFrame",
    "TaskArtifactUpdateEvent",
    "TaskStatusUpdateEvent",
    "is_last_frame",
    "stream_frame_from_dict",
]


@dataclass
class TaskStatusUpdateEvent:
    """A2A ``TaskStatusUpdateEvent`` (spec v1.0.1) — a task changed state.

    ``task_id`` is carried on the frame rather than inferred from the request
    because a single ``SubscribeToTask`` stream is scoped to one task but a
    ``SendStreamingMessage`` one need not be: the spec lets an agent report on a
    task it created mid-stream.  Reading the id off the frame is therefore the
    only correlation that is always right.
    """

    status: TaskStatus
    task_id: str | None = None
    context_id: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"status": self.status.to_dict()}
        if self.task_id is not None:
            out["taskId"] = self.task_id
        if self.context_id is not None:
            out["contextId"] = self.context_id
        if self.metadata:
            out["metadata"] = self.metadata
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskStatusUpdateEvent":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        return cls(
            status=TaskStatus.from_dict(obj.get("status")),
            task_id=as_str(obj.get("taskId", obj.get("task_id"))) or None,
            context_id=as_str(obj.get("contextId", obj.get("context_id"))) or None,
            metadata=as_dict(obj.get("metadata")) or None,
        )


@dataclass
class TaskArtifactUpdateEvent:
    """A2A ``TaskArtifactUpdateEvent`` (spec v1.0.1) — a task produced output.

    **``append`` is how A2A expresses artifact versioning**, and it is why
    :class:`~novaos_client_sdk.a2a_artifact.Artifact` has no version field of its
    own: a later frame carrying the same ``artifactId`` either extends the
    earlier content (``append=True``, a chunk of one streamed artifact) or
    replaces it wholesale (``append=False``, a new revision).
    :meth:`novaos_client_sdk.a2a_artifact.Artifact.revised` already implements
    both, so :meth:`apply_to` is a two-line dispatch rather than new semantics.

    ``last_chunk`` says this artifact is complete.  It says nothing about the
    *task* — an agent can finish one artifact and keep working — so it must not
    be used as a stream-terminality signal.  That is :func:`is_last_frame`'s job,
    reading the task state, per the stream-terminality rule.
    """

    artifact: Artifact
    task_id: str | None = None
    context_id: str | None = None
    append: bool = False
    last_chunk: bool = False
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"artifact": self.artifact.to_dict()}
        if self.task_id is not None:
            out["taskId"] = self.task_id
        if self.context_id is not None:
            out["contextId"] = self.context_id
        if self.append:
            out["append"] = True
        if self.last_chunk:
            out["lastChunk"] = True
        if self.metadata:
            out["metadata"] = self.metadata
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "TaskArtifactUpdateEvent":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        return cls(
            artifact=Artifact.from_dict(obj.get("artifact")),
            task_id=as_str(obj.get("taskId", obj.get("task_id"))) or None,
            context_id=as_str(obj.get("contextId", obj.get("context_id"))) or None,
            append=as_bool(obj.get("append")),
            last_chunk=as_bool(obj.get("lastChunk", obj.get("last_chunk"))),
            metadata=as_dict(obj.get("metadata")) or None,
        )

    def apply_to(self, artifacts: list[Artifact]) -> list[Artifact]:
        """Fold this update into a list of artifacts, keyed by ``artifact_id``.

        Returns a new list; the input is left alone.  An update for an id not
        yet seen appends the artifact — which is what the first frame of every
        artifact is — and an update for a known id revises it, extending or
        replacing its parts according to :attr:`append`.

        This is the accumulation a streaming caller would otherwise write by
        hand at every call site, and getting it wrong is how a token-by-token
        artifact ends up either truncated to its last chunk or duplicated.
        """
        out = list(artifacts)
        for index, existing in enumerate(out):
            if existing.artifact_id == self.artifact.artifact_id:
                out[index] = existing.revised(self.artifact.parts, append=self.append)
                return out
        out.append(self.artifact)
        return out


#: The ``StreamResponse`` oneof's members, in the order
#: :func:`stream_frame_from_dict` probes them.  ``statusUpdate`` and
#: ``artifactUpdate`` come first because they are what a live stream is mostly
#: made of; ``task`` and ``message`` are the snapshot/no-task cases.
STREAM_FRAME_KEYS: tuple[str, ...] = ("statusUpdate", "artifactUpdate", "task", "message")


#: One decoded ``StreamResponse``.  A closed union over the oneof's four members
#: — which member arrived *is* the frame's kind, so a consumer dispatches with
#: ``isinstance`` rather than on a tag the wire no longer carries.
StreamFrame = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Task | A2AMessage


def stream_frame_from_dict(data: Any) -> StreamFrame | None:
    """Decode one ``StreamResponse`` payload, or ``None`` if it is not one.

    Accepts the frame bare or wrapped in a JSON-RPC success envelope, matching
    the TypeScript caller's ``parseStreamFrame`` — ``a2a-sdk`` emits the wrapped
    form, and a hand-rolled pod server that emits the bare one should still be
    readable.

    **Returns ``None`` rather than raising** on anything unrecognizable.  One
    undecodable frame must not abort a run that is otherwise progressing, and
    the caller's stream-close classification (:func:`is_last_frame`) is
    what decides whether the run actually succeeded.  A JSON-RPC *error*
    envelope is not handled here — it is a failed call rather than a frame, and
    :mod:`novaos_client_sdk.client` raises it as an
    :class:`~novaos_client_sdk.exceptions.A2AProtocolError` before reaching this
    function.

    Args:
        data: One already-JSON-decoded SSE ``data:`` payload.

    Returns:
        The frame's populated member, or ``None``.
    """
    if not isinstance(data, dict):
        return None

    # A wrapped frame carries the StreamResponse under `result`; a bare frame is
    # the StreamResponse.  Checked by shape rather than by the presence of
    # "jsonrpc", so a server that omits the version key is still readable.
    body = data
    result = data.get("result")
    if isinstance(result, dict) and any(key in result for key in STREAM_FRAME_KEYS):
        body = result

    if "statusUpdate" in body:
        return TaskStatusUpdateEvent.from_dict(body["statusUpdate"])
    if "artifactUpdate" in body:
        return TaskArtifactUpdateEvent.from_dict(body["artifactUpdate"])
    if "task" in body:
        return Task.from_dict(body["task"])
    if "message" in body:
        return A2AMessage.from_dict(as_dict(body["message"]))
    return None


def is_last_frame(frame: StreamFrame) -> bool:
    """Does this frame carry a task state after which no more frames arrive?

    True for both terminal states (the task is finished) and interrupt states
    (the task has paused for the caller) — the SSE stream closes on either, a
    fact confirmed by an A2A maintainer on
    `a2aproject/A2A#1503 <https://github.com/a2aproject/A2A/issues/1503>`_.

    **This is not "did the task succeed".**  The two questions are deliberately
    separate, because conflating them is a real bug: a stream that ends on
    ``TASK_STATE_INPUT_REQUIRED`` is over *and* unfinished, and a run whose
    stream simply closes with no such frame at all failed in transport and must
    not be reported as complete.  Callers ask this to know when to stop reading,
    and ask the task's own state to know what happened.

    Args:
        frame: A decoded stream frame.

    Returns:
        ``True`` when the frame's state is terminal or an interrupt.
    """
    if isinstance(frame, TaskStatusUpdateEvent):
        state = frame.status.state
    elif isinstance(frame, Task):
        state = frame.status.state
    else:
        # An artifact update says nothing about the task's lifecycle, and a bare
        # Message is the agent answering without creating a task at all — the
        # stream ends because there is nothing else to send, which the transport
        # observes directly rather than inferring from a state.
        return False

    return state in TERMINAL_TASK_STATES or state in INTERRUPT_TASK_STATES
