"""When saves go out: one at a time, in order, and never behind a write.

This is the only place that knows the save rules; the transport knows nothing of them and the
LangGraph saver only calls through. Every accepted save sits in one ordered list until the
service confirms it, so a save that failed while the service was down is sent again, in order,
before anything that names it as a parent.

* ``ASYNC`` saves go out in the background. A second save first waits for the one in flight,
  so at most one is outstanding.
* ``SYNC`` saves, ``flush`` and ``close`` are barriers: they wait until everything accepted is
  stored. While the service is unavailable a barrier waits on capacity (reported through a
  callback, never with state in it) and gives up with ``CheckpointUnavailable`` after the
  bounded wait.
* ``EXIT`` saves are held in order and stored at the next barrier.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .errors import CheckpointError, CheckpointUnavailable
from .models import Durability, PendingWrite, SaveReceipt, SaveRequest

__all__ = ["SaveScheduler"]

logger = logging.getLogger(__name__)

T = TypeVar("T")

_MAX_WAIT_STEP_S = 10.0


class SaveScheduler:
    def __init__(
        self,
        store: Callable[[SaveRequest], Awaitable[SaveReceipt]],
        *,
        capacity_wait_s: float = 300.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        on_waiting_on_capacity: Callable[[float], None] | None = None,
    ) -> None:
        self._store = store
        self._capacity_wait_s = capacity_wait_s
        self._sleep = sleep
        self._clock = clock
        self._on_waiting = on_waiting_on_capacity
        self._unstored: list[SaveRequest] = []
        self._held: list[SaveRequest] = []
        self._inflight: asyncio.Task[None] | None = None
        self._fatal: CheckpointError | None = None
        self._lock = asyncio.Lock()  # only one request goes out at a time
        self._wanted: set[tuple[str, str]] = set()  # saves whose caller waits for the receipt
        self._receipts: dict[tuple[str, str], SaveReceipt] = {}

    async def submit(self, request: SaveRequest, durability: Durability) -> SaveReceipt:
        await self.settle()
        if durability is Durability.EXIT:
            self._held.append(request)
            return SaveReceipt(request.checkpoint_id, None, None, stored=False)
        self._unstored.append(request)
        if durability is Durability.ASYNC:
            self._inflight = asyncio.ensure_future(self._drain())
            return SaveReceipt(request.checkpoint_id, None, None, stored=False)
        key = (request.namespace, request.checkpoint_id)
        self._wanted.add(key)
        try:
            await self.barrier(self._drain)
            return self._receipts[key]
        finally:
            self._wanted.discard(key)
            self._receipts.pop(key, None)

    async def flush(self) -> None:
        await self.settle()
        self._unstored.extend(self._held)
        self._held.clear()
        await self.barrier(self._drain)

    async def settle(self) -> None:
        """Wait for the save in flight. An outage leaves its request queued; anything else is fatal."""
        inflight, self._inflight = self._inflight, None
        if inflight is not None:
            try:
                await inflight
            except CheckpointUnavailable:
                logger.warning("a background checkpoint save failed; it will be sent again")
            except CheckpointError as exc:
                self._fatal = exc
        if self._fatal is not None:
            raise self._fatal

    def attach_writes(self, checkpoint_id: str, namespace: str, writes: list[PendingWrite]) -> bool:
        """Add pending writes to a save that is held for the end of the run."""
        for i, held in enumerate(self._held):
            if held.checkpoint_id == checkpoint_id and held.namespace == namespace:
                merged = {(w.node_task_id, w.index): w for w in held.pending_writes}
                merged.update({(w.node_task_id, w.index): w for w in writes})
                self._held[i] = dataclasses.replace(held, pending_writes=list(merged.values()))
                return True
        return False

    async def barrier(self, call: Callable[[], Awaitable[T]]) -> T:
        """Run ``call`` and, while the service is unavailable, wait on capacity and try again."""
        started = self._clock()
        step = 1.0
        while True:
            try:
                return await call()
            except CheckpointUnavailable:
                elapsed = self._clock() - started
                if elapsed >= self._capacity_wait_s:
                    raise
                if self._on_waiting is not None:
                    self._on_waiting(elapsed)
                await self._sleep(min(step, _MAX_WAIT_STEP_S, self._capacity_wait_s - elapsed))
                step *= 2

    async def _drain(self) -> None:
        async with self._lock:
            while self._unstored:
                head = self._unstored[0]
                receipt = await self._store(head)
                self._unstored.pop(0)
                key = (head.namespace, head.checkpoint_id)
                if key in self._wanted:
                    self._receipts[key] = receipt
