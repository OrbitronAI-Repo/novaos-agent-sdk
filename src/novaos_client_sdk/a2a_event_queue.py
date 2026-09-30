"""The transport a task's lifecycle events travel over.

:mod:`novaos_client_sdk.a2a_events` models the *frames*
(:class:`~novaos_client_sdk.a2a_events.TaskStatusUpdateEvent`,
:class:`~novaos_client_sdk.a2a_events.TaskArtifactUpdateEvent`) an in-flight
task produces; until this module, nothing in the SDK ever produced one — they
were parsed off the wire (by :class:`~novaos_client_sdk.client.Client`) but
never emitted, because emitting one requires something to hold a task's
in-flight events between the moment they happen and the moment a consumer
reads them. That something is :class:`EventQueue`.

**Deliberately unbounded.** The reference A2A SDK's event queue is bounded,
because it is always paired with a consumer draining it concurrently — a
streaming request handler, or a background task started for exactly that
purpose. ``AgentBuilder`` has no such consumer yet: ``SubscribeToTask``, the
JSON-RPC method that would drain one live, landed one wave after this
one. A bounded queue with nothing to drain it would turn a skill that reports
enough progress through :class:`~novaos_client_sdk.a2a_task_updater.TaskUpdater`
into a ``SendMessage`` call that deadlocks on a full queue, which is worse than
the silent-progress problem this machinery exists to fix. So this queue never
blocks a producer; ``SubscribeToTask`` is what gives it a reader.
"""

import asyncio

from .a2a_events import TaskArtifactUpdateEvent, TaskStatusUpdateEvent

__all__ = ["TaskEvent", "EventQueue"]

#: The two frame types a task's lifecycle produces. Deliberately excludes
#: ``Task`` and ``A2AMessage`` — the other two members of
#: :data:`novaos_client_sdk.a2a_events.StreamFrame`'s oneof — because those are
#: whole-result snapshots a *stream reader* may see, not incremental events a
#: task *producer* enqueues.
TaskEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent


class EventQueue:
    """A single task's in-flight event stream, buffered in memory.

    One instance per task execution — :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
    creates a fresh queue for every ``SendMessage`` call, so two concurrent
    executions for different tasks never share one, and there is no cross-task
    state here to interfere between them.
    """

    def __init__(self, maxsize: int = 0) -> None:
        """Initialize the queue.

        Args:
            maxsize: Forwarded to the underlying ``asyncio.Queue``. ``0``
                (the default) means unbounded — see the module docstring for
                why an unconsumed queue must never apply backpressure today.
        """
        self._queue: asyncio.Queue[TaskEvent] = asyncio.Queue(maxsize=maxsize)
        self._closed = False
        # Independent per-subscriber queues for :meth:`subscribe`, added
        # later. ``_queue`` above is this class's original, single primary
        # consumer — :meth:`dequeue_event`'s contract and every pre-existing test
        # exercising it are unchanged. A subscriber queue is additive: each
        # call to :meth:`subscribe` registers a second, independent queue
        # that also receives every event :meth:`enqueue_event` publishes and
        # a closing sentinel once this instance closes, so N concurrent
        # ``SubscribeToTask`` calls on the same task each see the whole
        # stream instead of racing one shared ``asyncio.Queue.get()`` for
        # every event — a single queue delivers each item to exactly one
        # caller, so two callers of :meth:`dequeue_event` on the same
        # instance would silently split the stream between them, each
        # missing whatever the other happened to dequeue first, and only one
        # of them ever seeing the terminal frame.
        self._subscribers: list[asyncio.Queue[TaskEvent | None]] = []

    async def enqueue_event(self, event: TaskEvent) -> None:
        """Push ``event`` onto the queue, and fan it out to every subscriber.

        Silently dropped once the queue is :meth:`close`-d, matching how a
        closed SSE connection drops frames rather than erroring the producer —
        the task itself has already reached its outcome by the time anything
        closes this queue, so a straggling event has nowhere useful to go.
        """
        if self._closed:
            return
        await self._queue.put(event)
        for subscriber in self._subscribers:
            await subscriber.put(event)

    async def dequeue_event(self) -> TaskEvent:
        """Wait for and return the next event, from this instance's one primary queue.

        Unaffected by :meth:`subscribe` — this method's own queue is
        separate from every subscriber's, so a caller here still sees every
        event exactly once, exactly as before subscriber queues existed. Used by tests that
        verify what :class:`~novaos_client_sdk.a2a_task_updater.TaskUpdater`
        enqueued, and by any single-consumer caller that has no need for
        :meth:`subscribe`'s multi-consumer guarantee.

        **Can block forever if the producer abandons this task without ever
        enqueuing a terminal event and without ever calling** :meth:`close`
        **either** — this method itself has no way to distinguish "nothing
        has happened yet" from "nothing will ever happen again", unlike
        :meth:`subscribe`'s queue, which additionally receives a closing
        sentinel. A caller that must not hang forever on an abandoned
        producer should use :meth:`subscribe` instead.
        """
        return await self._queue.get()

    def subscribe(self) -> asyncio.Queue[TaskEvent | None]:
        """Register a new, independent subscriber queue.

        Every event this instance is given from here on (via
        :meth:`enqueue_event`) is also delivered to the returned queue, and a
        single ``None`` sentinel is delivered to it once :meth:`close` runs —
        the only way a consumer reading from it can tell "the producer is
        done, and this is the last one" apart from "the producer enqueued
        another real event". A queue returned here is never shared with
        another caller of this method — call it once per subscriber, e.g.
        once per ``SubscribeToTask`` request.

        A subscriber that joins mid-stream sees only what is enqueued from
        this point on, never a replay of what came before — the same
        no-replay guarantee A2A's own spec gives a reconnecting
        ``SubscribeToTask`` caller (spec §3.7), which is why
        :class:`~novaos_client_sdk.agent_builder.AgentBuilder`'s own use of
        this method always yields the task's current snapshot from the
        store first, before ever reading from the queue this returns.

        If this instance is already closed, the returned queue already has
        its sentinel queued, so a caller reading from it once returns
        immediately with ``None`` rather than hanging.
        """
        subscriber: asyncio.Queue[TaskEvent | None] = asyncio.Queue()
        if self._closed:
            subscriber.put_nowait(None)
        else:
            self._subscribers.append(subscriber)
        return subscriber

    def empty(self) -> bool:
        """Whether the queue currently holds no undelivered events."""
        return self._queue.empty()

    async def close(self) -> None:
        """Mark the queue closed and notify every subscriber it is done.

        Future :meth:`enqueue_event` calls become no-ops, as before. Also
        puts a single ``None`` onto every queue :meth:`subscribe` has handed
        out, waking a subscriber blocked waiting for the next event instead
        of leaving it to wait forever — a producer can reach this point
        without ever having enqueued a terminal event at all (an abandoned
        stream, a task canceled before it finished), and a subscriber must
        not assume "closed" implies "already saw a terminal frame".
        Idempotent: a second call is a no-op, so it never queues a
        second sentinel onto an already-notified subscriber.

        Does not drain or clear whatever is already buffered on the primary
        queue :meth:`dequeue_event` reads — a consumer that was already
        reading may still finish doing so.
        """
        if self._closed:
            return
        self._closed = True
        for subscriber in self._subscribers:
            await subscriber.put(None)

    @property
    def is_closed(self) -> bool:
        return self._closed
