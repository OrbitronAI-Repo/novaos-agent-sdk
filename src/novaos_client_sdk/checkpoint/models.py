"""Value types of the save/resume interface.

Plain frozen dataclasses with no behaviour. The wire spelling (base64, ISO times) is the
transport's concern; these hold bytes and datetimes.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

__all__ = [
    "ApprovalRequest",
    "Checkpoint",
    "CheckpointSummary",
    "CheckpointVersions",
    "CloseReceipt",
    "CommittedWrite",
    "ContinuationAnswer",
    "Durability",
    "InputRequired",
    "Lease",
    "PendingWrite",
    "SaveReceipt",
    "SaveRequest",
]


class Durability(StrEnum):
    """How long a ``save`` waits."""

    SYNC = "sync"  # wait until stored
    ASYNC = "async"  # store in the background; at most one save in flight
    EXIT = "exit"  # store only when the run stops; not allowed for tasks with write tools


@dataclass(frozen=True)
class CheckpointVersions:
    """What wrote a checkpoint. ``format_version`` says how ``state`` is laid out."""

    format_version: int
    agent_version: str | None = None
    agent_loop_version: str | None = None
    sdk_version: str | None = None
    persona_version: str | None = None
    rubric_version: str | None = None
    serializer: str | None = None

    def to_wire(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_wire(cls, data: Mapping[str, Any]) -> CheckpointVersions:
        known = {k: data.get(k) for k in cls.__dataclass_fields__ if k != "format_version"}
        return cls(format_version=data["format_version"], **known)


@dataclass(frozen=True)
class PendingWrite:
    """A loop framework's channel update for a step that has not finished.

    ``node_task_id`` is the framework's id for a node run inside a step, not the platform task.
    """

    node_task_id: str
    index: int
    channel: str
    value: bytes


@dataclass(frozen=True)
class Checkpoint:
    task_id: str
    namespace: str
    checkpoint_id: str
    parent_id: str | None
    state: bytes
    metadata: dict[str, Any]
    versions: CheckpointVersions
    pending_writes: list[PendingWrite]
    created_at: datetime


@dataclass(frozen=True)
class CheckpointSummary:
    """A checkpoint without its state, as ``list`` returns it."""

    task_id: str
    namespace: str
    checkpoint_id: str
    parent_id: str | None
    metadata: dict[str, Any]
    versions: CheckpointVersions
    bytes: int
    created_at: datetime


@dataclass(frozen=True)
class SaveReceipt:
    """What a save returns. ``stored`` is False for an ``ASYNC`` save that has not finished."""

    checkpoint_id: str
    stored_at: datetime | None
    bytes: int | None
    stored: bool


@dataclass(frozen=True)
class SaveRequest:
    """One save as the transport sends it. ``lease_epoch`` is the runner's, repeated on every save."""

    checkpoint_id: str
    parent_id: str | None
    namespace: str
    lease_epoch: int
    durability: str
    state: bytes
    metadata: Mapping[str, Any]
    versions: CheckpointVersions
    pending_writes: list[PendingWrite] = field(default_factory=list)


@dataclass(frozen=True)
class Lease:
    epoch: int
    expires_at: datetime


@dataclass(frozen=True)
class CloseReceipt:
    reason: str
    closed_at: datetime
    retention_ends_at: datetime


@dataclass(frozen=True)
class ApprovalRequest:
    """What the pod asks a person. The pod only asks; the worker opens and records the approval."""

    summary: str
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InputRequired:
    """The A2A ``input-required`` state, with the request that caused it."""

    request: ApprovalRequest
    state: str = "input-required"


@dataclass(frozen=True)
class ContinuationAnswer:
    """The answer that comes with a continuation."""

    value: Any


@dataclass(frozen=True)
class CommittedWrite:
    """A write the tool gateway recorded as completed. References only, never payloads."""

    tool: str
    business_key: str | None
    idempotency_key: str
    committed_at: datetime | None
    result_ref: str
