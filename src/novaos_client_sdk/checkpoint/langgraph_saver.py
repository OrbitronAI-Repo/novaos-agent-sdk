"""LangGraph's checkpointer on the save/resume interface.

Needs LangGraph (``pip install novaos-agent-sdk[agent-loop]``); the interface in
:mod:`novaos_client_sdk.checkpoint` does not. The mapping:

=============================  ==========================================
LangGraph                      Interface
=============================  ==========================================
``configurable.thread_id``     the task, which must be the token's run
``configurable.checkpoint_ns`` ``namespace``
``configurable.checkpoint_id`` ``checkpoint_id`` (LangGraph's ids as given)
``put`` / ``aput``             ``save`` (parent from the incoming config)
``put_writes``                 ``put_pending_writes``
``get_tuple``                  ``load``
``list``                       ``list``, then one ``load`` per summary
=============================  ==========================================

``state`` is LangGraph's own serialization of the checkpoint (type tag, then bytes), so the
service never sees Python objects. LangGraph hands ``put`` only the channel values that
changed; the saver merges them into the parent's so that every stored checkpoint is complete
and a load needs one read. Each save therefore carries all channel values, not just the changed
ones (no blob deduplication yet). ``metadata`` is the checkpoint metadata as JSON. Values
that JSON cannot hold are stored as their string form, which only affects ``list`` filters.

``delete_thread`` is not supported: the end of a task is ``close`` on the interface.

Every call must be for the token's run, so ``thread_id`` has to be the run id. The sync
methods run on the event loop that built the saver and must be called from another thread.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections import OrderedDict
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any, TypeVar

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from .errors import CheckpointNotFound
from .models import Durability, PendingWrite
from .save_resume import SaveResume

__all__ = ["NovaCheckpointSaver"]

T = TypeVar("T")

_SEP = b"\x00"
_PAGE = 200
_CACHE_SIZE = 16
_SAVE_WAIT_S = 30.0


class NovaCheckpointSaver(BaseCheckpointSaver[str]):
    def __init__(
        self,
        save_resume: SaveResume,
        *,
        durability: Durability = Durability.SYNC,
        serde: SerializerProtocol | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self._sr = save_resume
        self._durability = durability
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None  # type: ignore[assignment]
        # LangGraph sends the writes for a checkpoint before, or while, that checkpoint is saved.
        # The service refuses writes for a checkpoint it has not stored, so such a write waits
        # for the save: ``_gates`` has one future per save not yet finished, ``_finished`` the
        # most recent saves that have.
        self._gates: dict[tuple[str, str, str], asyncio.Future[None]] = {}
        self._finished: OrderedDict[tuple[str, str, str], None] = OrderedDict()
        # (task, namespace, checkpoint id) -> (channel versions, channel values), newest last
        self._full: OrderedDict[tuple[str, str, str], tuple[dict[str, Any], dict[str, Any]]] = (
            OrderedDict()
        )

    def _gate(self, key: tuple[str, str, str]) -> asyncio.Future[None]:
        if key not in self._gates:
            self._gates[key] = asyncio.get_running_loop().create_future()
        return self._gates[key]

    def _remember(self, task_id: str, ns: str, checkpoint: Checkpoint) -> None:
        key = (task_id, ns, checkpoint["id"])
        self._full[key] = (dict(checkpoint["channel_versions"]), dict(checkpoint["channel_values"]))
        self._full.move_to_end(key)
        while len(self._full) > _CACHE_SIZE:
            self._full.popitem(last=False)

    async def _complete(
        self, task_id: str, ns: str, parent_id: str | None, checkpoint: Checkpoint
    ) -> Checkpoint:
        """Merge the changed channel values into the parent's, so the stored copy is whole."""
        parent: tuple[dict[str, Any], dict[str, Any]] | None = None
        if parent_id:
            parent = self._full.get((task_id, ns, parent_id))
            if parent is None:
                loaded = await self._sr.load(task_id, parent_id, namespace=ns)
                if loaded is not None:
                    cp = self._unpack(loaded.state)
                    parent = (dict(cp["channel_versions"]), dict(cp["channel_values"]))
        values = dict(checkpoint["channel_values"])
        if parent is not None:
            for channel, version in checkpoint["channel_versions"].items():
                if (
                    channel not in values
                    and parent[0].get(channel) == version
                    and channel in parent[1]
                ):
                    values[channel] = parent[1][channel]
        return {**checkpoint, "channel_values": values}

    # -- encoding -----------------------------------------------------------

    def _pack(self, value: Any) -> bytes:
        kind, data = self.serde.dumps_typed(value)
        return kind.encode() + _SEP + data

    def _unpack(self, blob: bytes) -> Any:
        kind, _, data = blob.partition(_SEP)
        return self.serde.loads_typed((kind.decode(), data))

    @staticmethod
    def _meta_out(metadata: CheckpointMetadata) -> dict[str, Any]:
        out: dict[str, Any] = json.loads(json.dumps(dict(metadata), default=str))
        return out

    @staticmethod
    def _ids(config: RunnableConfig) -> tuple[str, str, str | None]:
        conf = config["configurable"]
        return conf["thread_id"], conf.get("checkpoint_ns", ""), get_checkpoint_id(config)

    def _tuple(self, task_id: str, cp: Any) -> CheckpointTuple:
        base = {"thread_id": task_id, "checkpoint_ns": cp.namespace}
        return CheckpointTuple(
            config={"configurable": {**base, "checkpoint_id": cp.checkpoint_id}},
            checkpoint=self._unpack(cp.state),
            metadata=cp.metadata,
            parent_config=(
                {"configurable": {**base, "checkpoint_id": cp.parent_id}} if cp.parent_id else None
            ),
            pending_writes=[
                (w.node_task_id, w.channel, self._unpack(w.value)) for w in cp.pending_writes
            ],
        )

    # -- async API ----------------------------------------------------------

    @property
    def lease_ttl_s(self) -> int | None:
        """How long the task's resume lease lives without a renewal, if the interface has one."""
        return getattr(self._sr, "lease_ttl_s", None)

    async def aacquire(self, task_id: str) -> None:
        """Take the task's resume lease now, if the save/resume interface has one to take."""
        acquire = getattr(self._sr, "acquire", None)
        if acquire is not None:
            await acquire(task_id)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        task_id, ns, checkpoint_id = self._ids(config)
        cp = await self._sr.load(task_id, checkpoint_id or "latest", namespace=ns)
        if cp is None:
            return None
        found = self._tuple(task_id, cp)
        self._remember(task_id, cp.namespace, found.checkpoint)
        return found

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        if config is None:
            return
        task_id = config["configurable"]["thread_id"]
        ns = config["configurable"].get("checkpoint_ns")
        cursor = get_checkpoint_id(before) if before else None
        yielded = 0
        while limit is None or yielded < limit:
            page = await self._sr.list(task_id, namespace=ns, before=cursor, limit=_PAGE)
            if not page:
                return
            for summary in page:
                cursor = summary.checkpoint_id
                if filter and not all(summary.metadata.get(k) == v for k, v in filter.items()):
                    continue
                cp = await self._sr.load(
                    task_id, summary.checkpoint_id, namespace=summary.namespace
                )
                if cp is None:
                    continue
                yield self._tuple(task_id, cp)
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            if len(page) < _PAGE:
                return

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        task_id, ns, parent_id = self._ids(config)
        key = (task_id, ns, checkpoint["id"])
        gate = self._gate(key)
        try:
            whole = await self._complete(task_id, ns, parent_id, checkpoint)
            self._remember(task_id, ns, whole)
            await self._sr.save(
                task_id,
                checkpoint["id"],
                parent_id,
                self._pack(whole),
                namespace=ns,
                metadata=self._meta_out(metadata),
                durability=self._durability,
            )
        finally:
            self._gates.pop(key, None)
            self._finished[key] = None
            while len(self._finished) > _CACHE_SIZE * 4:
                self._finished.popitem(last=False)
            if not gate.done():
                gate.set_result(None)
        return {
            "configurable": {
                "thread_id": task_id,
                "checkpoint_ns": ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id, ns, checkpoint_id = self._ids(config)
        assert checkpoint_id is not None
        rows = [
            PendingWrite(task_id, WRITES_IDX_MAP.get(channel, idx), channel, self._pack(value))
            for idx, (channel, value) in enumerate(writes)
        ]
        key = (thread_id, ns, checkpoint_id)
        gate = self._gates.get(key)
        if gate is not None:
            await asyncio.shield(gate)
        try:
            await self._sr.put_pending_writes(thread_id, checkpoint_id, rows, namespace=ns)
        except CheckpointNotFound:
            # Not stored yet: its save has not started. Wait for it, then send once more.
            if key not in self._finished:
                await asyncio.wait_for(asyncio.shield(self._gate(key)), _SAVE_WAIT_S)
            await self._sr.put_pending_writes(thread_id, checkpoint_id, rows, namespace=ns)

    async def adelete_thread(self, thread_id: str) -> None:
        raise NotImplementedError("a task ends with close on the save/resume interface")

    # -- sync API: runs the async one on the loop that built the saver ------------------------

    def _run(self, coro: Any) -> Any:
        if self._loop is None:
            raise RuntimeError("build the saver inside a running event loop to use the sync API")
        try:
            if asyncio.get_running_loop() is self._loop:
                coro.close()
                raise RuntimeError(
                    "the sync API cannot run on the saver's own event loop; use the async API"
                )
        except RuntimeError as exc:
            if "sync API" in str(exc):
                raise
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self._run(self.aget_tuple(config))  # type: ignore[no-any-return]

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        async def collect() -> list[CheckpointTuple]:
            return [t async for t in self.alist(config, filter=filter, before=before, limit=limit)]

        yield from self._run(collect())

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self._run(self.aput(config, checkpoint, metadata, new_versions))  # type: ignore[no-any-return]

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self._run(self.aput_writes(config, writes, task_id, task_path))

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Monotonic string versions, as LangGraph's in-memory saver makes them."""
        if current is None:
            number = 0
        elif isinstance(current, int):
            number = current
        else:
            number = int(current.split(".")[0])
        return f"{number + 1:032}.{random.random():016}"

    def delete_thread(self, thread_id: str) -> None:
        raise NotImplementedError("a task ends with close on the save/resume interface")
