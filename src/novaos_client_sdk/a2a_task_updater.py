"""``TaskUpdater`` — a skill's way to report progress mid-execution.

Before this module, a skill handler was silent from the moment
:class:`~novaos_client_sdk.agent_builder.AgentBuilder` invoked it until the
moment its ``SkillResult`` came back: there was no way to say "still working",
attach an artifact early, or explain a pause, short of returning. This is the
user-visible half of that capability — the piece a skill author actually touches — and its
shape deliberately mirrors the reference A2A SDK's ``TaskUpdater``: one method
per lifecycle transition, so a skill's state
changes read the same way they would against any other A2A server SDK.

**This does not replace the ``SkillResult`` return contract.**
:class:`~novaos_client_sdk.agent_builder.AgentBuilder` still decides a task's
*terminal* outcome from the handler's return value by default, exactly as it
did before this class existed — that is what keeps an agent that never
touches this class behaving identically. A skill still has
to return a ``SkillResult`` regardless (it is the handler's declared return
type), so the ordinary way to close a task in a given terminal state —
including :attr:`~novaos_client_sdk.a2a_task.TaskState.REJECTED`
— is to return one whose ``status`` maps there, not to also call
:meth:`TaskUpdater.complete`, :meth:`TaskUpdater.failed`,
:meth:`TaskUpdater.reject` or :meth:`TaskUpdater.cancel` directly.

A skill *may* still call one of those directly if it has its own reason to
(most concretely: it wants to close the task before it has finished building
the ``SkillResult`` it must still return). Doing so is safe and takes effect:
:meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task` catches
the :class:`RuntimeError` its own later terminal call raises against an
already-sealed updater, and treats the skill's direct
call as authoritative rather than overwriting it with wherever the returned
``SkillResult.status`` maps. See that method's docstring for the full
precedence reasoning. What a skill calls mid-execution for a *non-terminal*
update is :meth:`start_work`, :meth:`update_status` (with
:attr:`~novaos_client_sdk.a2a_task.TaskState.WORKING`), :meth:`add_artifact`,
:meth:`requires_input` and :meth:`requires_auth` — none of which this class
treats as terminal.

**Interrupts are not terminal.**
:attr:`~novaos_client_sdk.a2a_task.TaskState.INPUT_REQUIRED` and
:attr:`~novaos_client_sdk.a2a_task.TaskState.AUTH_REQUIRED` are deliberately
excluded from the states that seal a task against further updates — treating
either as terminal is the exact misclassification this design exists to prevent, and
here it would additionally mean a paused task could never be moved again once
:meth:`requires_input` or :meth:`requires_auth` was called.
"""

import asyncio
from typing import Any
from uuid import uuid4

from .a2a_artifact import Artifact
from .a2a_event_queue import EventQueue
from .a2a_events import TaskArtifactUpdateEvent, TaskStatusUpdateEvent
from .a2a_task import (
    TERMINAL_TASK_STATES,
    Task,
    TaskState,
    TaskStatus,
    utc_timestamp,
)
from .a2a_task_store import TaskStore
from .a2a_types import A2AMessage, Part, Role

__all__ = ["TaskUpdater"]


class TaskUpdater:
    """Publishes one task's lifecycle transitions to its store and event queue.

    Every method here does the same two things: writes the change into
    ``store`` so a later ``GetTask`` sees it, and enqueues the
    corresponding event onto ``event_queue`` so a live ``SubscribeToTask``
    would see it as it happens. A skill never touches either
    directly — this class is the entire surface.
    """

    def __init__(
        self,
        store: TaskStore,
        event_queue: EventQueue,
        task_id: str,
        context_id: str,
    ) -> None:
        """Initialize the updater.

        Args:
            store: Where transitions are persisted.
            event_queue: Where transitions are published live.
            task_id: The A2A ``Task.id`` this updater owns.
            context_id: The A2A ``contextId`` grouping this task with related ones.
        """
        self._store = store
        self.event_queue = event_queue
        self.task_id = task_id
        self.context_id = context_id or None
        self._lock = asyncio.Lock()
        self._terminal_reached = False
        self._revoked = False

    def revoke(self) -> None:
        """Turn every later write through this updater into a silent no-op.

        Called by :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._supersede_live_execution`
        when a newer request has taken over this updater's task id: the
        execution holding this instance has been preempted, and whatever it
        still tries to publish — a progress update, an artifact, a terminal
        state — would land on the task the *newer* execution now owns. The
        preempted execution already checks its cancellation token before
        publishing, but that check and the write are separated by ``await``s
        (:meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task`
        reads the task, attaches artifacts, then writes the status), and a
        supersede can land in between. Revocation closes that
        window at the only place it can be closed: inside this instance, re-
        checked under :attr:`_lock` immediately before each store write.

        Silent rather than raising: unlike a second terminal call, which is
        the skill's own mistake and gets a :class:`RuntimeError`, being
        superseded is nothing the skill did — and by then its ``Task`` has
        been cancelled anyway. One-way, like the terminal guard.
        """
        self._revoked = True

    @property
    def is_revoked(self) -> bool:
        """Whether :meth:`revoke` has been called — see it for what that means."""
        return self._revoked

    @property
    def is_sealed(self) -> bool:
        """Will this updater accept no further writes?

        ``True`` once a terminal method has succeeded through *this* instance,
        **or** once :meth:`revoke` has been called on it — in
        both cases a caller deciding whether to attempt a write should not.
        The terminal half of that, in detail:

        ``True`` once :meth:`complete`, :meth:`failed`, :meth:`reject`,
        :meth:`cancel`, or :meth:`update_status` called directly with a
        :data:`~novaos_client_sdk.a2a_task.TERMINAL_TASK_STATES` member has
        written through this exact instance. From that point on, every
        further call through :meth:`update_status` (including its named
        terminal wrappers) raises :class:`RuntimeError` rather than silently
        doing nothing — this property lets a caller that wants to know
        *before* attempting a write check first, rather than catching that
        exception (see :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task`,
        which does both: checks this to decide whether to attempt anything
        at all, and still catches the exception as a narrower backstop).

        Only reflects writes made through this instance, not the store —
        see :meth:`_set_status`'s own docstring for how a *different*
        updater sealing the same task (a concurrent ``CancelTask``, most
        concretely) is handled instead.
        """
        return self._terminal_reached or self._revoked

    async def _current_task(self) -> Task:
        """The task this updater owns, creating a bare skeleton if none is stored yet.

        The skeleton path exists for a :class:`TaskUpdater` used standalone
        (tests, or a future caller that never pre-seeds the store) — within
        :class:`~novaos_client_sdk.agent_builder.AgentBuilder` itself the store
        always already holds the task, seeded with its inbound message history
        before the updater is ever constructed, so this never discards history
        in practice.
        """
        task = await self._store.get(self.task_id)
        if task is None:
            task = Task(
                id=self.task_id,
                context_id=self.context_id,
                status=TaskStatus(state=TaskState.UNSPECIFIED),
            )
        return task

    async def update_status(
        self,
        state: TaskState,
        message: A2AMessage | None = None,
    ) -> None:
        """Move the task to ``state`` and publish a ``TaskStatusUpdateEvent``.

        Args:
            state: The task's new state.
            message: The agent's account of reaching this state, if any.

        Raises:
            RuntimeError: The task already reached a
                :data:`~novaos_client_sdk.a2a_task.TERMINAL_TASK_STATES` state
                through this updater. A terminal task does not change state
                again; the interrupt states are exempt from this — see the
                module docstring.
        """
        async with self._lock:
            if self._revoked:
                return
            if self._terminal_reached:
                raise RuntimeError(f"Task {self.task_id} is already in a terminal state.")
            await self._set_status(state, message)

    async def _force_update_status(
        self,
        state: TaskState,
        message: A2AMessage | None = None,
    ) -> None:
        """Move the task to ``state``, bypassing the terminal-state guard.

        Not for a skill's use: :meth:`update_status` (and its named wrappers)
        are what a skill calls, and its guard exists to catch a skill's own
        mistake — calling a terminal method more than once.

        **Not currently called anywhere in this SDK.**
        :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task`
        used to call this when a skill had already sealed the task itself
        through a direct ``ctx.task_updater`` call, so the harness's own,
        ``SkillResult``-derived decision would win over the skill's. This
        was found to be the mechanism behind a real bug — it made a
        skill's deliberate ``ctx.task_updater.reject(...)`` call unrecoverable,
        since any ``SkillResult`` the skill then returned would overwrite it —
        and reversed the precedence: ``_finalize_task`` now treats a skill's
        own direct terminal call as authoritative and leaves it alone rather
        than calling this method at all. See that method's docstring for the
        full reasoning.

        This method remains as a low-level, independently tested capability
        of this class — bypassing the terminal guard is occasionally exactly
        what a caller means to do — for whichever future caller has an
        actual reason to override an already-sealed task, which
        ``_finalize_task`` no longer is.
        """
        async with self._lock:
            await self._set_status(state, message, force=True)

    async def _set_status(
        self, state: TaskState, message: A2AMessage | None, *, force: bool = False
    ) -> None:
        if state in TERMINAL_TASK_STATES:
            self._terminal_reached = True

        task = await self._current_task()

        # `self._terminal_reached` only guards repeat writes
        # through *this* instance. A concurrent `CancelTask` seals the task
        # through its own, separate `TaskUpdater` — this instance's guard
        # knows nothing about that — so a write already in flight (most
        # concretely, `start_work()`'s own WORKING write racing a `CancelTask`
        # that landed first) could otherwise silently downgrade an
        # already-terminal store state back to something transient. Once the
        # *store itself* says terminal, only `_force_update_status` (the
        # harness overriding a skill's own premature self-close, a different,
        # same-instance scenario entirely) may write over it.
        if not force and task.status.state in TERMINAL_TASK_STATES:
            return

        # Re-checked *after* the `await` above, immediately before
        # the write — `revoke()` may have landed while `_current_task()` was
        # suspended, in which case `task` is now the superseding execution's
        # and must not be touched. See `revoke()`.
        if self._revoked:
            return

        task.status = TaskStatus(state=state, message=message, timestamp=utc_timestamp())
        await self._store.save(task)
        await self.event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                status=task.status, task_id=self.task_id, context_id=self.context_id
            )
        )

    async def submit(self, message: A2AMessage | None = None) -> None:
        """Mark the task submitted (acknowledged, not yet started)."""
        await self.update_status(TaskState.SUBMITTED, message)

    async def start_work(self, message: A2AMessage | None = None) -> None:
        """Mark the task as actively running."""
        await self.update_status(TaskState.WORKING, message)

    async def complete(self, message: A2AMessage | None = None) -> None:
        """Mark the task completed. Terminal."""
        await self.update_status(TaskState.COMPLETED, message)

    async def failed(self, message: A2AMessage | None = None) -> None:
        """Mark the task failed. Terminal."""
        await self.update_status(TaskState.FAILED, message)

    async def reject(self, message: A2AMessage | None = None) -> None:
        """Mark the task rejected — it was never going to work. Terminal."""
        await self.update_status(TaskState.REJECTED, message)

    async def cancel(self, message: A2AMessage | None = None) -> None:
        """Mark the task canceled. Terminal."""
        await self.update_status(TaskState.CANCELED, message)

    async def requires_input(self, message: A2AMessage | None = None) -> None:
        """Pause the task pending caller input. Not terminal — interrupts never are."""
        await self.update_status(TaskState.INPUT_REQUIRED, message)

    async def requires_auth(self, message: A2AMessage | None = None) -> None:
        """Pause the task pending caller authorization. Not terminal — interrupts never are."""
        await self.update_status(TaskState.AUTH_REQUIRED, message)

    async def add_artifact(
        self,
        parts: list[Part],
        *,
        artifact_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
        append: bool = False,
        last_chunk: bool = False,
    ) -> None:
        """Attach or extend one output artifact and publish a ``TaskArtifactUpdateEvent``.

        Args:
            parts: The artifact chunk's content.
            artifact_id: The artifact's identity. A new one is generated when
                omitted; pass the same id across calls to revise or extend one
                artifact instead of producing a second, unrelated one.
            name: Optional human-readable name.
            description: Optional human-readable description.
            metadata: Optional artifact metadata.
            append: When ``True``, extends the existing artifact of this id
                instead of replacing its content — see
                :meth:`novaos_client_sdk.a2a_artifact.Artifact.revised`.
            last_chunk: Whether this chunk completes the artifact.
        """
        async with self._lock:
            artifact = Artifact(
                parts=parts,
                artifact_id=artifact_id or str(uuid4()),
                name=name,
                description=description,
                metadata=metadata,
            )
            event = TaskArtifactUpdateEvent(
                artifact=artifact,
                task_id=self.task_id,
                context_id=self.context_id,
                append=append,
                last_chunk=last_chunk,
            )
            task = await self._current_task()
            # Same re-check as `_set_status`, for the same reason
            # — `add_artifact` has no terminal guard of its own, so this is
            # its *only* guard against writing onto a superseded task.
            if self._revoked:
                return
            task.artifacts = event.apply_to(task.artifacts)
            await self._store.save(task)
            await self.event_queue.enqueue_event(event)

    def new_agent_message(
        self,
        parts: list[Part],
        metadata: dict[str, Any] | None = None,
    ) -> A2AMessage:
        """Build (but do not publish) a new agent message for this task.

        A convenience for a skill that wants to pass a status message to
        :meth:`update_status` or one of its named wrappers without
        re-threading ``task_id``/``context_id`` by hand.
        """
        return A2AMessage(
            parts=parts,
            role=Role.AGENT,
            task_id=self.task_id,
            context_id=self.context_id,
            metadata=metadata,
        )
