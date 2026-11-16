"""Reading memory. The loop only reads: the interface has no write."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class MemoryEntry:
    id: str
    version: int
    scope: str
    content: str

    def ref(self) -> dict[str, Any]:
        """What a checkpoint may keep of this entry: where it came from, never what it said."""
        return {"id": self.id, "version": self.version, "scope": self.scope}


class MemoryReader(Protocol):
    async def read(self, query: str) -> list[MemoryEntry]:
        """The best entries for ``query``, best first, read live."""
        ...


class MemoryClientReader:
    """Reads through the SDK memory client, with the budget the platform put in the run context."""

    def __init__(self, client: Any, *, ctx: Any = None, top_k: int = 10) -> None:
        self._client = client
        self._ctx = ctx
        self._top_k = top_k
        # The memory the run may use, in tokens; the layer trims recalled memory to it.
        self.token_budget: int | None = (getattr(ctx, "memory_budget", None) or {}).get(
            "token_budget"
        )
        # What the platform fetched for this turn before the skill started, used for the first
        # read (if it is whole and was about the same question) so one turn does not read memory
        # twice.
        namespace = getattr(ctx, "memory", None)
        self._prefetched = getattr(namespace, "context", None)
        self._prefetched_for = getattr(namespace, "context_query", None)

    async def read(self, query: str) -> list[MemoryEntry]:
        result, self._prefetched = self._prefetched, None
        # A partial result missed a source, and one asked about other text (after a pause it is
        # the human's answer as the gateway wraps it) ranks memory for the wrong question.
        if result is None or result.partial or self._prefetched_for != query:
            budget = getattr(self._ctx, "memory_budget", None) or {}
            result = await self._client.assemble_context(
                query,
                top_k=self._top_k,
                token_budget=budget.get("token_budget"),
                latency_budget_ms=budget.get("latency_budget_ms"),
                ctx=self._ctx,
            )
        return [
            MemoryEntry(item.memory_id, item.version, item.scope, item.content)
            for item in [*result.durable_memories, *result.agent_memories]
        ]
