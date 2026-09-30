"""``Task`` persistence for the NovaOS client SDK.

Fifth module of the A2A type family, and the first that is not a wire type.
:mod:`novaos_client_sdk.a2a_task` models what a ``Task`` *is*; this module
models where one *lives* between the moment :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
creates it and the moment ``GetTask``, ``CancelTask``, ``ListTasks`` or
``SubscribeToTask`` needs to find it again.

**Why this had no reason to exist before now.** Earlier in the A2A migration,
every call built a ``Task`` and handed it straight back on the same call stack
that created it — there was never a second call that needed to find the same
object again. ``SendMessage`` still works that way for a caller that never
touches the store (see :class:`~novaos_client_sdk.agent_builder.AgentBuilder`'s
acceptance criterion that its response is byte-identical either way); what
changes is that the task now also has somewhere to be looked up afterward.

**Scope, per the ADR this ticket implements (F9/F15).** NovaOS pods are not
internet-reachable and the Temporal worker drives execution; the gateway owns
the authoritative run record the Agent Log Panel and the Loki pipeline key on.
So an **in-process** store is the right default — a pod's lifetime is one
execution in the common case — and a durable, cross-pod store is explicitly
**not** what this module is for: duplicating the gateway's run record would
create a second, competing source of truth for run status. The reference A2A
SDK's DB-backed store, its Alembic migrations, and ``a2a_db_cli`` are
deliberately not ported here; a client SDK does not take on a database
dependency to answer ``GetTask``.
"""

import asyncio
from abc import ABC, abstractmethod
from collections import OrderedDict

from .a2a_task import Task

__all__ = ["DEFAULT_MAX_TASKS", "InMemoryTaskStore", "TaskStore"]

#: Default cap for :class:`InMemoryTaskStore`. Sized for the fleet-mode pod a
#: long-lived worker keeps warm across many runs, not for a store meant to
#: retain history — see the class docstring for the eviction policy this
#: bounds.
DEFAULT_MAX_TASKS = 1000


class TaskStore(ABC):
    """Where an :class:`~novaos_client_sdk.a2a_task.Task` is kept once it exists.

    Three operations, matching what :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
    and the ``GetTask`` / ``CancelTask`` / ``ListTasks``
    handlers actually do to a task: write it, read it back by id, and remove it.
    A subclass backing this with something other than memory — a Redis instance
    shared by a fleet of pods, say — implements the same three methods and
    :class:`~novaos_client_sdk.agent_builder.AgentBuilder` does not need to know
    the difference; that substitutability is the entire reason this is an ABC
    rather than :class:`InMemoryTaskStore` being the only option.
    """

    @abstractmethod
    async def save(self, task: Task) -> None:
        """Persist ``task``, replacing any earlier task with the same id."""

    @abstractmethod
    async def get(self, task_id: str) -> Task | None:
        """Return the task stored under ``task_id``, or ``None`` if there is none."""

    @abstractmethod
    async def delete(self, task_id: str) -> None:
        """Remove the task stored under ``task_id``. A no-op if there is none."""

    @abstractmethod
    async def list_tasks(self) -> list[Task]:
        """Return every task currently held, as a snapshot list.

        Added once there was a reason to enumerate a store rather than only
        address one task by id: ``ListTasks`` has to see everything before it
        can filter and paginate any of it. :class:`InMemoryTaskStore` already
        holds everything this needs, so extending the ABC now is a
        deliberate, anticipated step — the same way :meth:`save` /
        :meth:`get` / :meth:`delete` were sized for what this store's callers
        actually do. It is not a breaking change to any real implementer:
        this SDK ships exactly one concrete :class:`TaskStore`
        (:class:`InMemoryTaskStore`) and there is no other known
        implementation to break.

        No filtering or pagination happens here — that is entirely the
        JSON-RPC handler's job (:meth:`~novaos_client_sdk.agent_builder.AgentBuilder`'s
        ``ListTasks`` dispatch), matching the same store/handler division of
        labour every other operation in this class already draws: the store
        answers "what do you have", the handler decides "what did the caller
        ask for".
        """


class InMemoryTaskStore(TaskStore):
    """A :class:`TaskStore` backed by a dict in this process's memory.

    The default for :class:`~novaos_client_sdk.agent_builder.AgentBuilder`, and
    per the ADR (F9/F15) the *only* store this SDK ships: a pod is not
    internet-reachable and the gateway already owns the durable run record, so
    there is nothing for a second, cross-pod-durable implementation to do here
    that would not compete with that record.

    **Eviction policy.** Bounded by ``max_tasks`` (default
    :data:`DEFAULT_MAX_TASKS`), tracked in least-recently-saved order. Every
    :meth:`save` moves that task to the most-recently-saved end; once the
    store holds more than ``max_tasks`` entries, the least-recently-saved one
    is evicted — on the theory that a task nobody has updated in a while is
    also the task nobody is still polling. This is a straight LRU-by-write
    policy, not a TTL: there is no background sweep, so an idle pod holds no
    timer and eviction only ever happens as a side effect of a new
    :meth:`save`. A pod whose lifetime is one execution (the common case)
    never approaches the cap; it exists for the fleet-mode pod that stays
    warm across many runs and must not retain every task it has ever handled.
    """

    def __init__(self, max_tasks: int = DEFAULT_MAX_TASKS) -> None:
        """Initialize the store.

        Args:
            max_tasks: The eviction cap described above. Must be positive.
        """
        if max_tasks <= 0:
            raise ValueError("max_tasks must be greater than 0")
        self._max_tasks = max_tasks
        self._tasks: OrderedDict[str, Task] = OrderedDict()
        self._lock = asyncio.Lock()

    async def save(self, task: Task) -> None:
        """Persist ``task``, evicting the least-recently-saved entry if over capacity."""
        async with self._lock:
            self._tasks[task.id] = task
            self._tasks.move_to_end(task.id)
            if len(self._tasks) > self._max_tasks:
                self._tasks.popitem(last=False)

    async def get(self, task_id: str) -> Task | None:
        """Return the task stored under ``task_id``, or ``None`` if there is none.

        Does **not** count as a "save" for eviction-ordering purposes — reading
        a task does not protect it from eviction, only updating one does. A
        long-poller that only ever calls :meth:`get` on a stalled task gets no
        special protection from the cap.
        """
        async with self._lock:
            return self._tasks.get(task_id)

    async def delete(self, task_id: str) -> None:
        """Remove the task stored under ``task_id``. A no-op if there is none."""
        async with self._lock:
            self._tasks.pop(task_id, None)

    async def list_tasks(self) -> list[Task]:
        """Return every task currently held, in this store's own (insertion/LRU) order.

        A snapshot list, not a live view: the caller gets the tasks as they
        stood at the moment of the call, and a later ``save``/``delete``
        elsewhere does not retroactively change what was already returned.
        """
        async with self._lock:
            return list(self._tasks.values())
