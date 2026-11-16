"""The save/resume interface and its implementation over a transport.

Agents hold a :class:`SaveResume`. They never see the transport, the scheduler or the lease.
The task of every call is the execution token's run; a ``task_id`` argument that names
anything else is refused (see :mod:`novaos_client_sdk.checkpoint.identity`).
"""

from __future__ import annotations

import asyncio
import builtins
import dataclasses
import inspect
import logging
import os
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from importlib import metadata as importlib_metadata
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from .committed_writes import CommittedWritesClient, CommittedWritesSource
from .errors import (
    CheckpointError,
    CheckpointNotFound,
    CheckpointRequestError,
    DurabilityNotAllowed,
    UnsupportedCheckpointFormat,
)
from .identity import require_task
from .lease import LeaseKeeper
from .models import (
    ApprovalRequest,
    Checkpoint,
    CheckpointSummary,
    CheckpointVersions,
    CloseReceipt,
    CommittedWrite,
    ContinuationAnswer,
    Durability,
    InputRequired,
    PendingWrite,
    SaveReceipt,
    SaveRequest,
)
from .scheduler import SaveScheduler
from .transport import CheckpointTransport, HttpCheckpointTransport

if TYPE_CHECKING:
    from novaos_client_sdk.context import ExecutionContext

__all__ = ["CheckpointSaveResume", "SaveResume", "write_after_save"]

logger = logging.getLogger(__name__)

CHECKPOINT_SERVICE_URL_ENV = "CHECKPOINT_SERVICE_URL"

_TOKEN_SHAPE = re.compile(rb"eyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}")


@runtime_checkable
class SaveResume(Protocol):
    async def save(
        self,
        task_id: str,
        checkpoint_id: str,
        parent_id: str | None,
        state: bytes,
        pending_writes: list[PendingWrite] = [],  # noqa: B006 - protocol default, never mutated
        *,
        namespace: str = "",
        metadata: Mapping[str, Any] | None = None,
        durability: Durability = Durability.SYNC,
    ) -> SaveReceipt: ...

    async def put_pending_writes(
        self,
        task_id: str,
        checkpoint_id: str,
        writes: list[PendingWrite],
        *,
        namespace: str = "",
    ) -> None: ...

    async def load(
        self,
        task_id: str,
        checkpoint_id: str | Literal["latest"] = "latest",
        *,
        namespace: str = "",
    ) -> Checkpoint | None: ...

    async def list(
        self,
        task_id: str,
        *,
        namespace: str | None = None,
        before: str | None = None,
        limit: int = 50,
    ) -> list[CheckpointSummary]: ...

    async def pause(self, task_id: str, request: ApprovalRequest) -> InputRequired: ...

    async def resume(self, task_id: str, answer: ContinuationAnswer) -> Checkpoint: ...

    async def committed_writes(self, task_id: str) -> builtins.list[CommittedWrite]: ...

    async def flush(self) -> None: ...

    async def close(self, task_id: str, reason: str = "completed") -> CloseReceipt: ...


def _sdk_version() -> str | None:
    try:
        return importlib_metadata.version("novaos-agent-sdk")
    except importlib_metadata.PackageNotFoundError:
        return None


class CheckpointSaveResume:
    """:class:`SaveResume` over a :class:`~novaos_client_sdk.checkpoint.transport.CheckpointTransport`."""

    def __init__(
        self,
        transport: CheckpointTransport,
        token_provider: Callable[[], str],
        *,
        holder: str | None = None,
        versions: CheckpointVersions | None = None,
        has_write_tools: bool = False,
        committed_writes: (
            CommittedWritesSource | Callable[[str], Awaitable[list[CommittedWrite]]] | None
        ) = None,
        lease_ttl_s: int = 60,
        lease_renew_s: float = 20.0,
        capacity_wait_s: float = 300.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        on_waiting_on_capacity: Callable[[float], None] | None = None,
    ) -> None:
        self._transport = transport
        self._token = token_provider
        self._versions = versions or CheckpointVersions(format_version=1)
        if self._versions.sdk_version is None:
            self._versions = dataclasses.replace(self._versions, sdk_version=_sdk_version())
        fmt = self._versions.format_version
        self._accepted = (fmt, fmt - 1)
        self._has_write_tools = has_write_tools
        if committed_writes is not None and not callable(committed_writes):
            committed_writes = committed_writes.committed_writes
        self._committed = committed_writes
        self._lease = LeaseKeeper(
            transport,
            holder or os.environ.get("HOSTNAME") or f"sdk/{uuid.uuid4().hex[:8]}",
            ttl_s=lease_ttl_s,
            renew_s=lease_renew_s,
            sleep=sleep,
        )
        self._scheduler = SaveScheduler(
            self._send,
            capacity_wait_s=capacity_wait_s,
            sleep=sleep,
            clock=clock,
            on_waiting_on_capacity=on_waiting_on_capacity,
        )
        self._task: str | None = None

    @property
    def lease_ttl_s(self) -> int:
        """How long a resume lease lives without a renewal."""
        return self._lease.ttl_s

    def __repr__(self) -> str:
        return f"CheckpointSaveResume(task={self._task!r})"

    @classmethod
    def from_context(
        cls,
        ctx: ExecutionContext,
        *,
        base_url: str | None = None,
        gateway_url: str | None = None,
        **kwargs: Any,
    ) -> CheckpointSaveResume:
        """Build from the run's execution context.

        The service URL is ``base_url`` or the ``CHECKPOINT_SERVICE_URL`` environment variable; the tool gateway used by
        ``committed_writes`` is ``gateway_url`` or ``MCP_GATEWAY_URL`` (without one,
        ``committed_writes`` raises). The token is read from the context on every request, so a
        re-minted token is used as soon as the platform has sent it.
        """
        url = base_url or os.environ.get(CHECKPOINT_SERVICE_URL_ENV, "")
        if not url:
            raise CheckpointError(
                f"no checkpoint service URL: pass base_url or set {CHECKPOINT_SERVICE_URL_ENV}"
            )
        token = ctx.require_token
        gateway = gateway_url or os.environ.get("MCP_GATEWAY_URL", "")
        committed = CommittedWritesClient(gateway, token) if gateway else None
        return cls(
            HttpCheckpointTransport(url, token),
            token,
            committed_writes=kwargs.pop("committed_writes", committed),
            **kwargs,
        )

    # -- identity and guards ------------------------------------------------

    def _for(self, task_id: str) -> str:
        self._task = require_task(self._token(), task_id)
        return self._task

    def _refuse_secrets(self, *parts: bytes) -> None:
        token = self._token().encode()
        for part in parts:
            if token in part or _TOKEN_SHAPE.search(part):
                raise CheckpointRequestError(
                    "a token-shaped value must not be stored in a checkpoint"
                )

    def _check_format(self, cp: Checkpoint) -> Checkpoint:
        if cp.versions.format_version not in self._accepted:
            raise UnsupportedCheckpointFormat.for_versions(
                cp.versions.format_version, self._accepted
            )
        return cp

    async def _send(self, request: SaveRequest) -> SaveReceipt:
        # The lease is taken here, inside the scheduler's waits, so an outage during the take
        # is waited out like an outage during the save.
        assert self._task is not None
        epoch = await self._lease.ensure(self._task)
        return await self._transport.put_checkpoint(
            self._task, dataclasses.replace(request, lease_epoch=epoch)
        )

    # -- the interface ------------------------------------------------------

    async def save(
        self,
        task_id: str,
        checkpoint_id: str,
        parent_id: str | None,
        state: bytes,
        pending_writes: list[PendingWrite] | None = None,
        *,
        namespace: str = "",
        metadata: Mapping[str, Any] | None = None,
        durability: Durability = Durability.SYNC,
    ) -> SaveReceipt:
        self._for(task_id)
        if durability is Durability.EXIT and self._has_write_tools:
            raise DurabilityNotAllowed("Durability.EXIT is not allowed for tasks with write tools")
        writes = list(pending_writes or [])
        self._refuse_secrets(state, repr(dict(metadata or {})).encode(), *(w.value for w in writes))
        request = SaveRequest(
            checkpoint_id=checkpoint_id,
            parent_id=parent_id,
            namespace=namespace,
            lease_epoch=0,
            durability=durability.value,
            state=state,
            metadata=dict(metadata or {}),
            versions=self._versions,
            pending_writes=writes,
        )
        return await self._scheduler.submit(request, durability)

    async def acquire(self, task_id: str) -> int:
        """Take the task's resume lease now, before anything runs; ``LeaseConflict`` if it is held.

        A run that continues a task should call this first, so it does not run a write while
        another holder may still be running the same task.
        """
        return await self._lease.ensure(self._for(task_id))

    async def put_pending_writes(
        self,
        task_id: str,
        checkpoint_id: str,
        writes: list[PendingWrite],
        *,
        namespace: str = "",
    ) -> None:
        task = self._for(task_id)
        self._refuse_secrets(*(w.value for w in writes))
        if self._scheduler.attach_writes(checkpoint_id, namespace, writes):
            return
        await self._scheduler.settle()

        async def put() -> int:
            epoch = await self._lease.ensure(task)
            return await self._transport.put_pending_writes(
                task, checkpoint_id, namespace, epoch, writes
            )

        await self._scheduler.barrier(put)

    async def load(
        self,
        task_id: str,
        checkpoint_id: str | Literal["latest"] = "latest",
        *,
        namespace: str = "",
    ) -> Checkpoint | None:
        task = self._for(task_id)
        await self._scheduler.settle()
        cp = await self._transport.get_checkpoint(task, checkpoint_id, namespace)
        return self._check_format(cp) if cp else None

    async def list(
        self,
        task_id: str,
        *,
        namespace: str | None = None,
        before: str | None = None,
        limit: int = 50,
    ) -> list[CheckpointSummary]:
        task = self._for(task_id)
        await self._scheduler.settle()
        return await self._transport.list_checkpoints(task, namespace, before, limit)

    async def pause(self, task_id: str, request: ApprovalRequest) -> InputRequired:
        """Store everything outstanding, then return ``input-required``. Opens no approval."""
        self._for(task_id)
        await self.flush()
        return InputRequired(request=request)

    async def resume(self, task_id: str, answer: ContinuationAnswer) -> Checkpoint:
        """Take the resume lease and load the latest root checkpoint.

        The caller then reads :meth:`committed_writes` before any write and skips what is there.
        """
        task = self._for(task_id)
        await self._lease.take(task)
        cp = await self._transport.get_checkpoint(task, "latest", "")
        if cp is None:
            raise CheckpointNotFound("nothing is stored for this task")
        return self._check_format(cp)

    async def committed_writes(self, task_id: str) -> builtins.list[CommittedWrite]:
        self._for(task_id)
        if self._committed is None:
            raise CheckpointError("no tool gateway client is configured for committed writes")
        return await self._committed(task_id)

    async def flush(self) -> None:
        await self._scheduler.flush()

    async def close(self, task_id: str, reason: str = "completed") -> CloseReceipt:
        """Final save already stored; close the task with this runner's epoch.

        A ``LeaseLost`` here is raised: a runner that lost the task must not report it ended.
        """
        task = self._for(task_id)
        await self.flush()
        epoch = self._lease.epoch
        try:
            return await self._scheduler.barrier(
                lambda: self._transport.close_task(task, reason, epoch)
            )
        finally:
            await self._lease.stop()

    async def aclose(self) -> None:
        """Stop renewing the lease. Does not close the task."""
        await self._lease.stop()


async def write_after_save[T](
    save_resume: SaveResume,
    task_id: str,
    checkpoint_id: str,
    parent_id: str | None,
    state: bytes,
    write: Callable[[], T | Awaitable[T]],
    *,
    namespace: str = "",
    metadata: Mapping[str, Any] | None = None,
    pending_writes: list[PendingWrite] | None = None,
) -> T:
    """Flush, store a checkpoint with ``SYNC``, and only then run ``write``.

    If the checkpoint cannot be stored the write does not happen and the error is raised.
    """
    await save_resume.flush()
    await save_resume.save(
        task_id,
        checkpoint_id,
        parent_id,
        state,
        pending_writes or [],
        namespace=namespace,
        metadata=metadata,
        durability=Durability.SYNC,
    )
    result = write()
    if inspect.isawaitable(result):
        return await result
    return result
