"""Save and resume for agent tasks: state that outlives the pod.

.. admonition:: Architecture: named exception, as for memory

   The checkpoint service is called **directly** by this package, not through the MCP Gateway.
   Like the memory service, it is a deliberate, documented exception to the rule that agents
   reach internal services through the gateway only. The reasons are the same: a policy hop
   on every step without a policy to apply, and the latency of a save on the critical path of
   the run. The path stays measured, and the exception can be removed later by changing the
   base URL, because the HTTP API is the same behind either route.

   Authorization is the run's execution token as the bearer, validated by the service. The
   tenant is the token's ``tenant_id`` claim and the task is its ``run_id`` claim, which stays
   the same when the platform re-mints the token. The gateway's own ``task_id`` claim changes
   on every mint and is never used. Nothing in this package takes a tenant, user or task
   from an argument that the token does not already say.

The interface (:class:`SaveResume`) needs no extra. Only :class:`NovaCheckpointSaver`, the
LangGraph adapter, needs LangGraph: ``pip install novaos-agent-sdk[agent-loop]``.
"""

from __future__ import annotations

from typing import Any

from .committed_writes import CommittedWritesClient, CommittedWritesSource
from .errors import (
    CheckpointAuthError,
    CheckpointChainError,
    CheckpointError,
    CheckpointIdentityError,
    CheckpointNotFound,
    CheckpointRequestError,
    CheckpointTooLarge,
    CheckpointUnavailable,
    DurabilityNotAllowed,
    LeaseConflict,
    LeaseLost,
    TaskClosed,
    UnsupportedCheckpointFormat,
)
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
    Lease,
    PendingWrite,
    SaveReceipt,
    SaveRequest,
)
from .save_resume import CheckpointSaveResume, SaveResume, write_after_save
from .transport import CheckpointTransport, HttpCheckpointTransport, RetryPolicy

__all__ = [
    "ApprovalRequest",
    "Checkpoint",
    "CheckpointAuthError",
    "CheckpointChainError",
    "CheckpointError",
    "CheckpointIdentityError",
    "CheckpointNotFound",
    "CheckpointRequestError",
    "CheckpointSaveResume",
    "CheckpointSummary",
    "CheckpointTooLarge",
    "CheckpointTransport",
    "CheckpointUnavailable",
    "CheckpointVersions",
    "CloseReceipt",
    "CommittedWrite",
    "CommittedWritesClient",
    "CommittedWritesSource",
    "ContinuationAnswer",
    "Durability",
    "DurabilityNotAllowed",
    "HttpCheckpointTransport",
    "InputRequired",
    "Lease",
    "LeaseConflict",
    "LeaseLost",
    "NovaCheckpointSaver",
    "PendingWrite",
    "RetryPolicy",
    "SaveReceipt",
    "SaveRequest",
    "SaveResume",
    "TaskClosed",
    "UnsupportedCheckpointFormat",
    "write_after_save",
]


def __getattr__(name: str) -> Any:
    # The saver imports LangGraph, which the interface must not require.
    if name == "NovaCheckpointSaver":
        try:
            from .langgraph_saver import NovaCheckpointSaver
        except ImportError as exc:
            raise ImportError(
                "NovaCheckpointSaver needs LangGraph: pip install 'novaos-agent-sdk[agent-loop]'"
            ) from exc
        return NovaCheckpointSaver
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
