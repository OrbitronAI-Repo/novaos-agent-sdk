"""Exceptions raised by the save/resume interface, one per row of the service's error table."""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.exceptions import NovaOSError

__all__ = [
    "CheckpointAuthError",
    "CheckpointChainError",
    "CheckpointError",
    "CheckpointIdentityError",
    "CheckpointNotFound",
    "CheckpointRequestError",
    "CheckpointTooLarge",
    "CheckpointUnavailable",
    "DurabilityNotAllowed",
    "LeaseConflict",
    "LeaseLost",
    "TaskClosed",
    "UnsupportedCheckpointFormat",
]


class CheckpointError(NovaOSError):
    """Base class for everything the save/resume interface raises."""


class CheckpointAuthError(CheckpointError):
    """The token is missing, invalid, expired, lacks a run, or lacks the scope."""


class CheckpointIdentityError(CheckpointError):
    """A call named a task other than the token's run.

    Tenant, user and task come from the execution token; an argument that disagrees is a bug in
    the caller, never a way to reach another task.
    """


class CheckpointRequestError(CheckpointError):
    """The service refused the request as malformed."""


class CheckpointChainError(CheckpointError):
    """The parent is not in this task and namespace, or the id exists with a different body."""


class CheckpointNotFound(CheckpointError):
    """There is nothing stored to resume from."""


class LeaseConflict(CheckpointError):
    """Another runner holds a live resume lease. This runner stops without writing."""


class LeaseLost(CheckpointError):
    """The runner's epoch is old or missing. It stops at once; it never writes again."""


class TaskClosed(CheckpointError):
    """The task was closed; saves, pending writes and lease calls no longer work."""


class CheckpointTooLarge(CheckpointError):
    """A per-save, per-blob, per-task or checkpoint-count limit was passed."""


class CheckpointUnavailable(CheckpointError):
    """The service could not be reached or is overloaded, after the retries."""


class DurabilityNotAllowed(CheckpointError, ValueError):
    """``Durability.EXIT`` was asked for by a task that has write tools."""


class UnsupportedCheckpointFormat(CheckpointError):
    """A stored checkpoint is in a format this reader does not read.

    ``found`` is the stored ``format_version``; ``supported`` lists the versions this reader
    accepts (its own and the one before). The reader never guesses.
    """

    def __init__(
        self,
        message: str,
        *,
        found: int | None = None,
        supported: tuple[int, ...] = (),
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, retry_after=retry_after, details=details)
        self.found = found
        self.supported = supported

    @classmethod
    def for_versions(
        cls, found: int | None, supported: tuple[int, ...]
    ) -> UnsupportedCheckpointFormat:
        return cls(
            f"checkpoint format {found} is not readable here; this reader reads {list(supported)}",
            found=found,
            supported=supported,
        )
