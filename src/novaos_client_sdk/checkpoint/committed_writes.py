"""Client for the tool gateway's record of completed writes.

Kept apart from the checkpoint transport: it talks to a different service, and an agent that
never writes never needs it. The gateway takes tenant and run from the token, so the URL
carries no task.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from typing import Protocol

import httpx

from .errors import CheckpointError, CheckpointUnavailable
from .identity import require_task
from .models import CommittedWrite
from .transport import error_from_response

__all__ = ["COMMITTED_WRITES_PATH", "CommittedWritesClient", "CommittedWritesSource"]

COMMITTED_WRITES_PATH = "/mcp/v1/committed-writes"


class CommittedWritesSource(Protocol):
    async def committed_writes(self, task_id: str) -> list[CommittedWrite]: ...


class CommittedWritesClient:
    def __init__(
        self,
        base_url: str | None,
        token_provider: Callable[[], str],
        *,
        client: httpx.AsyncClient | None = None,
        timeout_s: float = 15.0,
    ) -> None:
        url = base_url or os.environ.get("MCP_GATEWAY_URL", "")
        if not url:
            raise CheckpointError("no tool gateway URL: pass base_url or set MCP_GATEWAY_URL")
        self._base = url.rstrip("/")
        self._token = token_provider
        self._client = client or httpx.AsyncClient(timeout=timeout_s)

    def __repr__(self) -> str:
        return f"CommittedWritesClient(base_url={self._base!r})"

    async def aclose(self) -> None:
        await self._client.aclose()

    async def committed_writes(self, task_id: str) -> list[CommittedWrite]:
        token = self._token()
        require_task(token, task_id)
        try:
            response = await self._client.get(
                f"{self._base}{COMMITTED_WRITES_PATH}",
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError as exc:
            raise CheckpointUnavailable("the tool gateway could not be reached") from exc
        if response.status_code >= 400:
            raise error_from_response(response)
        return [
            CommittedWrite(
                tool=row["tool"],
                business_key=row.get("business_key"),
                idempotency_key=row["idempotency_key"],
                committed_at=(
                    datetime.fromisoformat(row["committed_at"].replace("Z", "+00:00"))
                    if row.get("committed_at")
                    else None
                ),
                result_ref=row["result_ref"],
            )
            for row in response.json()["committed_writes"]
        ]
