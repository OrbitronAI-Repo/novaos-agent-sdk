"""How the save/resume interface reaches the checkpoint service.

``CheckpointTransport`` is what :class:`~novaos_client_sdk.checkpoint.CheckpointSaveResume`
depends on. :class:`HttpCheckpointTransport` is the one implementation: the stub and the real
service answer the same API, so tests switch between them by changing the base URL.

The transport reads the token from a provider on every request, so a re-minted token is used
as soon as it exists, and it never logs or stores it. Tenant and task are not parameters: the
service takes the tenant from the token, and the path's task must be the token's run.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol

import httpx

from .errors import (
    CheckpointAuthError,
    CheckpointChainError,
    CheckpointError,
    CheckpointNotFound,
    CheckpointRequestError,
    CheckpointTooLarge,
    CheckpointUnavailable,
    LeaseConflict,
    LeaseLost,
    TaskClosed,
    UnsupportedCheckpointFormat,
)
from .models import (
    Checkpoint,
    CheckpointSummary,
    CheckpointVersions,
    CloseReceipt,
    Lease,
    PendingWrite,
    SaveReceipt,
    SaveRequest,
)

__all__ = ["CheckpointTransport", "HttpCheckpointTransport", "RetryPolicy"]

_GZIP_ABOVE_BYTES = 1024
_MAX_RETRY_AFTER_S = 30.0


class CheckpointTransport(Protocol):
    async def take_lease(self, task_id: str, holder: str, ttl_seconds: int) -> Lease: ...

    async def put_checkpoint(self, task_id: str, request: SaveRequest) -> SaveReceipt: ...

    async def put_pending_writes(
        self,
        task_id: str,
        checkpoint_id: str,
        namespace: str,
        lease_epoch: int,
        writes: list[PendingWrite],
    ) -> int: ...

    async def get_checkpoint(
        self, task_id: str, checkpoint_id: str | Literal["latest"], namespace: str
    ) -> Checkpoint | None: ...

    async def list_checkpoints(
        self, task_id: str, namespace: str | None, before: str | None, limit: int
    ) -> list[CheckpointSummary]: ...

    async def close_task(self, task_id: str, reason: str, lease_epoch: int) -> CloseReceipt: ...


@dataclass(frozen=True)
class RetryPolicy:
    """Backoff for ``rate_limited`` and for an unreachable or overloaded service."""

    attempts: int = 4
    base_s: float = 0.25
    max_s: float = 4.0
    sleep: Callable[[float], Awaitable[None]] = field(default=asyncio.sleep, repr=False)


_BY_CODE: dict[str, type[CheckpointError]] = {
    "unauthenticated": CheckpointAuthError,
    "forbidden": CheckpointAuthError,
    "invalid_request": CheckpointRequestError,
    "parent_not_found": CheckpointChainError,
    "checkpoint_conflict": CheckpointChainError,
    "lease_conflict": LeaseConflict,
    "lease_lost": LeaseLost,
    "task_closed": TaskClosed,
    "state_too_large": CheckpointTooLarge,
    "task_quota_exceeded": CheckpointTooLarge,
}


def error_from_response(response: httpx.Response) -> CheckpointError:
    """Map a service error body to its exception. Only ids and codes are kept, never bodies."""
    code = ""
    message = f"checkpoint service answered {response.status_code}"
    try:
        err = response.json()["error"]
        code = str(err.get("code", ""))
        message = f"{code}: {err.get('message', '')}".strip(": ")
    except (ValueError, KeyError, TypeError, AttributeError):
        pass
    if code == "unsupported_format_version" or response.status_code == 422:
        return UnsupportedCheckpointFormat(message)
    if code in _BY_CODE:
        return _BY_CODE[code](message)
    if code in ("rate_limited", "service_unavailable") or response.status_code >= 500:
        return CheckpointUnavailable(message)
    if response.status_code == 401:
        return CheckpointAuthError(message)
    return CheckpointError(message)


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _write_to_wire(w: PendingWrite) -> dict[str, Any]:
    return {
        "node_task_id": w.node_task_id,
        "index": w.index,
        "channel": w.channel,
        "value": _b64(w.value),
    }


def _write_from_wire(w: dict[str, Any]) -> PendingWrite:
    return PendingWrite(
        node_task_id=w["node_task_id"],
        index=int(w["index"]),
        channel=w["channel"],
        value=base64.b64decode(w["value"]),
    )


class HttpCheckpointTransport:
    """The checkpoint service's HTTP API (base path ``/v1``)."""

    def __init__(
        self,
        base_url: str,
        token_provider: Callable[[], str],
        *,
        client: httpx.AsyncClient | None = None,
        retry: RetryPolicy | None = None,
        timeout_s: float = 30.0,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._token = token_provider
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._retry = retry or RetryPolicy()

    def __repr__(self) -> str:
        return f"HttpCheckpointTransport(base_url={self._base!r})"

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _send(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
    ) -> httpx.Response:
        headers = {"Authorization": f"Bearer {self._token()}"}
        content: bytes | None = None
        if body is not None:
            content = json.dumps(body, separators=(",", ":")).encode()
            headers["Content-Type"] = "application/json"
            if len(content) > _GZIP_ABOVE_BYTES:
                content = gzip.compress(content)
                headers["Content-Encoding"] = "gzip"
        last: CheckpointError | None = None
        for attempt in range(1, self._retry.attempts + 1):
            delay = min(self._retry.base_s * 2 ** (attempt - 1), self._retry.max_s)
            try:
                response = await self._client.request(
                    method, f"{self._base}{path}", content=content, headers=headers, params=params
                )
            except httpx.HTTPError:
                last = CheckpointUnavailable("the checkpoint service could not be reached")
            else:
                if response.status_code < 400:
                    return response
                err = error_from_response(response)
                if not isinstance(err, CheckpointUnavailable):
                    if response.status_code == 404:
                        raise CheckpointNotFound("checkpoint_not_found") from None
                    raise err
                last = err
                after = response.headers.get("Retry-After")
                if after and after.isdigit():
                    delay = min(float(after), _MAX_RETRY_AFTER_S)
            if attempt < self._retry.attempts:
                await self._retry.sleep(delay)
        assert last is not None
        raise last

    async def take_lease(self, task_id: str, holder: str, ttl_seconds: int) -> Lease:
        r = await self._send(
            "POST",
            f"/v1/tasks/{task_id}/lease",
            body={"holder": holder, "ttl_seconds": ttl_seconds},
        )
        data = r.json()
        return Lease(epoch=int(data["lease_epoch"]), expires_at=_time(data["expires_at"]))  # type: ignore[arg-type]

    async def put_checkpoint(self, task_id: str, request: SaveRequest) -> SaveReceipt:
        body: dict[str, Any] = {
            "namespace": request.namespace,
            "parent_id": request.parent_id,
            "lease_epoch": request.lease_epoch,
            "durability": request.durability,
            "state": _b64(request.state),
            "metadata": dict(request.metadata),
            "versions": request.versions.to_wire(),
        }
        if request.pending_writes:
            body["pending_writes"] = [_write_to_wire(w) for w in request.pending_writes]
        r = await self._send(
            "PUT", f"/v1/tasks/{task_id}/checkpoints/{request.checkpoint_id}", body=body
        )
        data = r.json()
        return SaveReceipt(
            checkpoint_id=data["checkpoint_id"],
            stored_at=_time(data.get("stored_at")),
            bytes=data.get("bytes"),
            stored=True,
        )

    async def put_pending_writes(
        self,
        task_id: str,
        checkpoint_id: str,
        namespace: str,
        lease_epoch: int,
        writes: list[PendingWrite],
    ) -> int:
        r = await self._send(
            "PUT",
            f"/v1/tasks/{task_id}/checkpoints/{checkpoint_id}/pending-writes",
            body={
                "namespace": namespace,
                "lease_epoch": lease_epoch,
                "writes": [_write_to_wire(w) for w in writes],
            },
        )
        return int(r.json().get("count", len(writes)))

    async def get_checkpoint(
        self, task_id: str, checkpoint_id: str | Literal["latest"], namespace: str
    ) -> Checkpoint | None:
        try:
            r = await self._send(
                "GET",
                f"/v1/tasks/{task_id}/checkpoints/{checkpoint_id}",
                params={"namespace": namespace},
            )
        except CheckpointNotFound:
            return None
        d = r.json()
        return Checkpoint(
            task_id=task_id,
            namespace=d.get("namespace", namespace),
            checkpoint_id=d["checkpoint_id"],
            parent_id=d.get("parent_id"),
            state=base64.b64decode(d["state"]),
            metadata=d.get("metadata") or {},
            versions=CheckpointVersions.from_wire(d["versions"]),
            pending_writes=[_write_from_wire(w) for w in d.get("pending_writes", [])],
            created_at=_time(d["created_at"]),  # type: ignore[arg-type]
        )

    async def list_checkpoints(
        self, task_id: str, namespace: str | None, before: str | None, limit: int
    ) -> list[CheckpointSummary]:
        params = {"limit": str(min(limit, 200))}
        if namespace is not None:
            params["namespace"] = namespace
        if before is not None:
            params["before"] = before
        try:
            r = await self._send("GET", f"/v1/tasks/{task_id}/checkpoints", params=params)
        except CheckpointNotFound:
            return []
        return [
            CheckpointSummary(
                task_id=task_id,
                namespace=d.get("namespace", ""),
                checkpoint_id=d["checkpoint_id"],
                parent_id=d.get("parent_id"),
                metadata=d.get("metadata") or {},
                versions=CheckpointVersions.from_wire(d["versions"]),
                bytes=int(d.get("bytes", 0)),
                created_at=_time(d["created_at"]),  # type: ignore[arg-type]
            )
            for d in r.json()["checkpoints"]
        ]

    async def close_task(self, task_id: str, reason: str, lease_epoch: int) -> CloseReceipt:
        r = await self._send(
            "POST",
            f"/v1/tasks/{task_id}/close",
            body={"reason": reason, "lease_epoch": lease_epoch},
        )
        d = r.json()
        return CloseReceipt(
            reason=d["reason"],
            closed_at=_time(d["closed_at"]),  # type: ignore[arg-type]
            retention_ends_at=_time(d["retention_ends_at"]),  # type: ignore[arg-type]
        )
