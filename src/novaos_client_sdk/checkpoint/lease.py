"""The resume lease: taken when a runner starts or resumes, renewed while the run is live."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from .errors import CheckpointError, LeaseConflict, LeaseLost, TaskClosed
from .transport import CheckpointTransport

__all__ = ["LeaseKeeper"]

logger = logging.getLogger(__name__)


class LeaseKeeper:
    def __init__(
        self,
        transport: CheckpointTransport,
        holder: str,
        *,
        ttl_s: int = 60,
        renew_s: float = 20.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._transport = transport
        self._holder = holder
        self._ttl = ttl_s
        self._renew = renew_s
        self._sleep = sleep
        self._epoch: int | None = None
        self._task_id: str | None = None
        self._renewer: asyncio.Task[None] | None = None
        self._lost: CheckpointError | None = None

    @property
    def ttl_s(self) -> int:
        return self._ttl

    async def take(self, task_id: str) -> int:
        """Take the lease now (a start or a resume) and keep renewing it."""
        lease = await self._transport.take_lease(task_id, self._holder, self._ttl)
        self._epoch, self._task_id, self._lost = lease.epoch, task_id, None
        if self._renewer is None or self._renewer.done():
            self._renewer = asyncio.ensure_future(self._renew_loop())
        return lease.epoch

    async def ensure(self, task_id: str) -> int:
        if self._epoch is None or self._task_id != task_id:
            return await self.take(task_id)
        return self.epoch

    @property
    def epoch(self) -> int:
        """The epoch every save repeats. Raises at once once the lease is known to be lost."""
        if self._lost is not None:
            raise self._lost
        assert self._epoch is not None
        return self._epoch

    async def stop(self) -> None:
        renewer, self._renewer = self._renewer, None
        if renewer is not None:
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)

    async def _renew_loop(self) -> None:
        while True:
            await self._sleep(self._renew)
            assert self._task_id is not None
            try:
                lease = await self._transport.take_lease(self._task_id, self._holder, self._ttl)
            except (LeaseLost, LeaseConflict, TaskClosed) as exc:
                self._lost = LeaseLost(str(exc)) if not isinstance(exc, TaskClosed) else exc
                return
            except CheckpointError:
                logger.warning("resume lease renewal failed; will try again")
                continue
            self._epoch = lease.epoch
