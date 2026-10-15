"""Memory Service client for NovaOS agents.

Provides a typed async client for the NovaOS Memory Service MCP endpoint.

.. admonition:: Architecture — Named I2 Exception

   The Memory Service is called **directly** by this client (not via the MCP
   Gateway). This is a deliberate, documented exception to platform invariant
   I2 ("single governed egress; agents call internal services through the MCP
   Gateway only — never directly").

   **Design rationale for the direct path:**

   1. *Policy-hop cost without benefit* — Routing through the MCP Gateway
      would add a policy evaluation hop (OPA + rate limits) without those
      controls applying to the Memory Service's own endpoints. The gateway
      would proxy the ``/api/v1/mcp`` dispatch envelope opaquely with no
      additional enforcement.
   2. *Performance* — Durable-memory lookups sit on the critical path of
      every agent turn. An unnecessary gateway hop adds p99 latency for all
      agent invocations.
   3. *Protocol ownership* — Memory Service predates the MCP Gateway's
      role as the general egress point. Moving it behind the gateway is
      tracked as a long-term architectural goal, not a near-term requirement.

   **Authorization model (reconciliation with OPA policy):**

   - The caller must pass ``headers=ctx.to_auth_headers()``, which forwards
     ``Authorization: Bearer <execution_token>`` to the Memory Service.
   - The Memory Service validates the same RS256 execution-token shape
     (issuer, expiry, core claims including ``tenant_id``) as the MCP Gateway
     does. **MCP Gateway OPA evaluation and gateway rate limits are not in
     this path.**
   - Scope write-restrictions (R1: agents cannot write to ``user`` scope;
     R2: ``tenant`` scope is admin-only) are enforced by the Memory Service's
     own policy layer.
   - Tenant isolation — all reads and writes include ``tenant_id`` from the
     token claims, enforced at the Memory Service data layer.
   - Memory Service may also enforce memory IAM scopes from the token's
     ``scopes`` claim (``memory:read:*`` / ``memory:write:*``) via its own
     policy layer, separately from MCP Gateway OPA.

   **Known policy gap** — MCP Gateway-managed controls (per-agent capability
   allow-lists, audit enrichment, gateway rate limits) are **not** applied to
   Memory Service requests. A policy change in the gateway does not
   automatically restrict memory access. Because execution tokens are
   short-lived, **gateway policy changes do not affect an already-minted token
   until expiry or run termination.** Operator runbooks must explicitly revoke
   the agent's execution token to enforce restrictions immediately.

   **Canonical memory access** — ``MemoryClient`` is the sole supported path
   for durable semantic memory via direct HTTP to the Memory Service.

   This exception is formally recorded in the platform's architecture
   documentation, §I2.

The client wraps the ``POST /api/v1/mcp`` dispatch endpoint, providing typed methods
for each MCP tool: ``query``, ``write``, ``forget``, ``hint``, and
``assemble_context``.

Example:
    ```python
    from novaos_client_sdk.context import ExecutionContext
    from novaos_client_sdk.memory import MemoryClient

    ctx = ExecutionContext.from_headers(request.headers)

    async with MemoryClient(headers=ctx.to_auth_headers()) as mem:
        # Write agent-scoped operational knowledge
        result = await mem.write(
            scope="agent",
            content="Reporting API requires ISO 8601 date ranges",
            tags=["api", "reporting"],
        )
        print(result.memory_id)

        # Submit a hint for user-memory consolidation
        hint_result = await mem.hint(
            conversation_id="conv-123",
            hint="User expressed preference for dark mode",
        )
        print(hint_result.hint_id)  # queued, not committed

        # Query memories
        query_result = await mem.query(scope="user", query="ui preferences")
        for item in query_result.items:
            print(f"{item.memory_id}: {item.content}")

        # Assemble context for agent reasoning
        context = await mem.assemble_context(query="current task context")
        print(f"Got {len(context.durable_memories)} durable memories")
    ```
"""

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from novaos_client_sdk._http import httpx
from novaos_client_sdk._http_error import status_error_details, status_error_message
from novaos_client_sdk._resilience import CircuitBreaker as _CircuitBreaker
from novaos_client_sdk._resilience import backoff_delay
from novaos_client_sdk.exceptions import (
    NovaOSConnectionError,
    NovaOSError,
    NovaOSServerError,
    NovaOSValidationError,
)
from novaos_client_sdk.tracing import inject_trace_headers, traced_tool_call

if TYPE_CHECKING:
    from novaos_client_sdk.agent_context import AgentExecutionContext

logger = logging.getLogger(__name__)

MEMORY_SERVICE_URL_ENV = "MEMORY_SERVICE_URL"

_WRITE_ORDINAL_ATTR = "_novaos_memory_write_ordinal"


def _next_write_ordinal(ctx: Any) -> int:
    """Next per-execution ordinal for a ctx-derived write idempotency key.

    Stored on ``ctx`` itself rather than on the ``MemoryClient`` instance:
    callers typically construct a fresh ``MemoryClient`` per call (e.g. via
    ``ctx.memory.client()`` or a new ``async with MemoryClient(...)`` block
    for each write), so a client-instance counter would restart at 1 every
    time and every write in one execution would collide on the same key.
    Attaching the counter to ``ctx`` — stable for the lifetime of one
    execution — keeps it shared across however many client instances end up
    calling :meth:`MemoryClient.write` for that execution.
    """
    ordinal = getattr(ctx, _WRITE_ORDINAL_ATTR, 0) + 1
    try:
        setattr(ctx, _WRITE_ORDINAL_ATTR, ordinal)
    except AttributeError:
        pass  # ctx doesn't support attribute assignment; ordinal is still usable, just not persisted
    return ordinal


# ---------------------------------------------------------------------------
# Response dataclasses
# ---------------------------------------------------------------------------


MemoryType = Literal["semantic", "episodic", "procedural"]
"""Optional kind of a memory. Opt-in on write and on query/assemble filters."""

MEMORY_TYPES: tuple[str, ...] = ("semantic", "episodic", "procedural")


def _check_memory_type(memory_type: str | None) -> str | None:
    """Reject an unknown ``memory_type`` before any request is made."""
    if memory_type is not None and memory_type not in MEMORY_TYPES:
        raise ValueError(
            f"memory_type must be one of {', '.join(MEMORY_TYPES)}; got {memory_type!r}"
        )
    return memory_type


def _recall_filters(
    filters: dict[str, Any] | None,
    memory_type: str | None,
    conversation_id: str | None,
) -> dict[str, Any] | None:
    """Merge the ``memory_type``/``conversation_id`` shortcuts into a filters dict.

    Returns ``None`` when there is nothing to send, so a call that sets neither
    shortcut (and passes no ``filters``) builds the same payload as before.
    """
    merged: dict[str, Any] = dict(filters) if filters else {}
    if _check_memory_type(memory_type) is not None:
        merged["memory_type"] = memory_type
    if conversation_id is not None:
        merged["conversation_id"] = conversation_id
    return merged or None


@dataclass
class MemoryMetadata:
    """Metadata attached to a memory item."""

    tags: list[str] = field(default_factory=list)
    source: str | None = None
    conversation_id: str | None = None
    confidence: float | None = None
    salience: float | None = None
    memory_type: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_accessed_at: str | None = None
    custom: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MemoryMetadata":
        return cls(
            tags=data.get("tags", []),
            source=data.get("source"),
            conversation_id=data.get("conversation_id"),
            confidence=data.get("confidence"),
            salience=data.get("salience"),
            memory_type=data.get("memory_type"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            last_accessed_at=data.get("last_accessed_at"),
            custom=data.get("custom"),
        )


@dataclass
class MemoryItem:
    """A single memory entry returned by query or assemble.

    Attributes:
        memory_id: Unique identifier for this memory.
        scope: Memory scope (user, agent, run, tenant, platform).
        content: The memory content text.
        version: Version counter for this memory.
        metadata: Associated metadata (tags, source, timestamps).
        score: Similarity to the query text (0 to 1) for durable scopes (user,
            agent, tenant). Always ``None`` for ``run`` and ``task`` scope: those
            are plain key-value reads, never similarity-ranked.
    """

    memory_id: str
    scope: str
    content: str
    version: int = 1
    metadata: MemoryMetadata = field(default_factory=MemoryMetadata)
    score: float | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MemoryItem":
        meta_data = data.get("metadata", {})
        metadata = (
            MemoryMetadata.from_dict(meta_data) if isinstance(meta_data, dict) else MemoryMetadata()
        )
        return cls(
            memory_id=data["memory_id"],
            scope=data.get("scope", ""),
            content=data.get("content", ""),
            version=data.get("version", 1),
            metadata=metadata,
            score=data.get("score"),
        )


@dataclass
class QueryResult:
    """Result of a ``memory.query`` call.

    Attributes:
        items: List of matching memory items.
        retrieval_id: Opaque ID for ExplainRetrieval lookups.
        partial: True if some stores failed and results are incomplete.
        stores_queried: Which backing stores were consulted.
    """

    items: list[MemoryItem] = field(default_factory=list)
    retrieval_id: str = ""
    partial: bool = False
    stores_queried: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryResult":
        return cls(
            items=[MemoryItem.from_dict(i) for i in data.get("items", [])],
            retrieval_id=data.get("retrieval_id", ""),
            partial=data.get("partial", False),
            stores_queried=data.get("stores_queried", []),
        )


@dataclass
class WriteResult:
    """Result of a ``memory.write`` call.

    Attributes:
        memory_id: ID of the created/updated memory. For ``agent``-scope writes
            gated by the AgentWriteGate, this is a ``prop_``-prefixed proposal
            ID, not a durable memory ID. Use
            :meth:`MemoryClient.poll_proposal` to resolve the proposal to its
            committed ``memory_id`` once the gate decides.
        scope: Scope the memory was written to.
        write_event_id: Opaque event ID for audit trail.
        created_at: ISO-8601 timestamp of creation.
        durable: Whether the write is already persisted. ``False`` when
            ``memory_id`` carries a ``prop_`` prefix, meaning the write is a
            proposal pending AgentWriteGate approval. When ``False``, callers
            should use :meth:`MemoryClient.poll_proposal` to obtain the
            committed row's real ``memory_id``.
    """

    memory_id: str = ""
    scope: str = ""
    write_event_id: str = ""
    created_at: str = ""
    durable: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WriteResult":
        memory_id = data.get("memory_id", "")
        return cls(
            memory_id=memory_id,
            scope=data.get("scope", ""),
            write_event_id=data.get("write_event_id", ""),
            created_at=data.get("created_at", ""),
            durable=not memory_id.startswith("prop_"),
        )


@dataclass
class ProposalStatus:
    """Status of an agent-scope write proposal returned by the write-gate.

    Attributes:
        proposal_id: The ``prop_``-prefixed proposal identifier.
        status: Gate decision — ``"committed"``, ``"pending"``, or
            ``"rejected"``.
        reason: Rejection reason when ``status`` is ``"rejected"``
            (e.g. ``"pii_detected"``, ``"secrets_detected"``), otherwise
            ``None``.
        memory_id: The durable memory ID assigned after commit, or ``None``
            while the proposal is still pending or was rejected.
    """

    proposal_id: str = ""
    status: str = ""
    reason: str | None = None
    memory_id: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ProposalStatus":
        return cls(
            proposal_id=d.get("proposal_id", ""),
            status=d.get("status", ""),
            reason=d.get("reason"),
            memory_id=d.get("memory_id"),
        )


@dataclass
class UpdateResult:
    """Result of a ``memory.update`` call.

    Attributes:
        memory_id: ID of the updated memory.
        scope: Scope of the memory (always ``agent``).
        version: The memory's new version. Pass it as ``version`` on the next
            update; a stale version is rejected with HTTP 409.
        update_event_id: Opaque event ID for audit trail.
        updated_at: ISO-8601 timestamp of the update.
    """

    memory_id: str = ""
    scope: str = ""
    version: int = 0
    update_event_id: str = ""
    updated_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UpdateResult":
        return cls(
            memory_id=data.get("memory_id", ""),
            scope=data.get("scope", ""),
            version=data.get("version", 0),
            update_event_id=data.get("update_event_id", ""),
            updated_at=data.get("updated_at", ""),
        )


@dataclass
class ForgetResult:
    """Result of a ``memory.forget`` call.

    Attributes:
        deleted_count: Number of memories deleted.
        delete_event_id: Opaque event ID for audit trail.
    """

    deleted_count: int = 0
    delete_event_id: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ForgetResult":
        return cls(
            deleted_count=data.get("deleted_count", 0),
            delete_event_id=data.get("delete_event_id", ""),
        )


@dataclass
class HintStatus:
    """Outcome of a consolidation hint this agent submitted.

    Attributes:
        hint_id: ID returned by ``hint()``.
        status: ``pending``, ``consolidated``, ``dead_lettered`` or ``expired``.
        submitted_at: When the service accepted the hint (ISO 8601), if known.
        resolved_at: When consolidation resolved it (ISO 8601); ``None`` while pending.
    """

    hint_id: str = ""
    status: str = "pending"
    submitted_at: str | None = None
    resolved_at: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HintStatus":
        return cls(
            hint_id=data.get("hint_id", ""),
            status=data.get("status", "pending"),
            submitted_at=data.get("submitted_at"),
            resolved_at=data.get("resolved_at"),
        )


@dataclass
class HintResult:
    """Result of a ``memory.hint`` call.

    Attributes:
        hint_id: ID of the enqueued consolidation hint.
        status: Acceptance status (typically "accepted").
        message: Human-readable status message.
    """

    hint_id: str = ""
    status: str = "accepted"
    message: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HintResult":
        return cls(
            hint_id=data.get("hint_id", ""),
            status=data.get("status", "accepted"),
            message=data.get("message", ""),
        )


@dataclass
class LiveMessage:
    """A live conversation message from the Conversation Store.

    Attributes:
        message_id: Unique message identifier.
        role: Message role (user, assistant, system).
        content: Message text content.
        timestamp: ISO-8601 timestamp.
    """

    message_id: str = ""
    role: str = ""
    content: str = ""
    timestamp: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LiveMessage":
        return cls(
            message_id=data.get("message_id", ""),
            role=data.get("role", ""),
            content=data.get("content", ""),
            timestamp=data.get("timestamp", ""),
        )


@dataclass
class AssembleResult:
    """Result of a ``memory.assemble`` call.

    Combines memories from multiple scopes into a single context bundle
    for agent reasoning.

    Attributes:
        durable_memories: User-scoped durable memories.
        live_messages: Recent conversation messages.
        agent_memories: Agent-scoped durable memories.
        run_data: Ephemeral run-scoped data (dict or None).
        sources_consulted: Which sources were queried.
        sources_skipped: Which sources were skipped (missing capabilities/IDs).
        partial: True if some sources failed.
        retrieval_id: Opaque ID for ExplainRetrieval lookups.
    """

    durable_memories: list[MemoryItem] = field(default_factory=list)
    live_messages: list[LiveMessage] = field(default_factory=list)
    agent_memories: list[MemoryItem] = field(default_factory=list)
    run_data: dict[str, Any] | None = None
    sources_consulted: list[str] = field(default_factory=list)
    sources_skipped: list[str] = field(default_factory=list)
    partial: bool = False
    retrieval_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AssembleResult":
        return cls(
            durable_memories=[MemoryItem.from_dict(i) for i in data.get("durable_memories", [])],
            live_messages=[LiveMessage.from_dict(m) for m in data.get("live_messages", [])],
            agent_memories=[MemoryItem.from_dict(i) for i in data.get("agent_memories", [])],
            run_data=data.get("run_data"),
            sources_consulted=data.get("sources_consulted", []),
            sources_skipped=data.get("sources_skipped", []),
            partial=data.get("partial", False),
            retrieval_id=data.get("retrieval_id"),
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class MemoryClient:
    """Async client for the NovaOS Memory Service.

    Communicates with the Memory Service's ``/mcp`` dispatch endpoint using
    the MCP tool protocol (tool name + payload + context).

    Includes built-in retry with exponential backoff for transient errors
    (429, 502, 503, 504) and a circuit breaker for fail-fast when the
    service is down.

    Args:
        base_url: Memory Service URL. If None, reads from ``MEMORY_SERVICE_URL``
            env var.
        headers: Additional HTTP headers (typically includes ``Authorization``
            from ``ExecutionContext.to_auth_headers()``).
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum total attempts for retryable errors (default: 3).
            Always issues at least one attempt, even when set to ``0`` --
            ``0`` means "no retries", not "no attempts": the first (and
            only) attempt's real error is raised on failure instead of
            retrying.
        circuit_breaker_threshold: Failures before circuit opens (default: 5).
        circuit_breaker_reset_s: Seconds before half-open probe (default: 30).
        client: Optional pre-configured httpx.AsyncClient to use instead of
            constructing one internally. Useful for tests and for callers
            who want to share a connection pool or attach custom
            transports/interceptors. If provided, MemoryClient will NOT
            close it on __aexit__ — the caller retains ownership of its
            lifecycle.

    Example:
        ```python
        ctx = ExecutionContext.from_headers(request.headers)

        async with MemoryClient(headers=ctx.to_auth_headers()) as mem:
            result = await mem.query(scope="user", query="preferences")
            for item in result.items:
                print(item.content)
        ```
    """

    def __init__(
        self,
        base_url: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_reset_s: float = 30.0,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if base_url is None:
            base_url = os.getenv(MEMORY_SERVICE_URL_ENV)
            if not base_url:
                raise NovaOSError(
                    f"Memory Service URL not provided and '{MEMORY_SERVICE_URL_ENV}' "
                    "environment variable is not set."
                )
        self._base_url = base_url.rstrip("/")
        self._headers = headers.copy() if headers else {}
        self._timeout = timeout
        self._max_retries = max_retries
        self._cb = _CircuitBreaker(circuit_breaker_threshold, circuit_breaker_reset_s)
        self._client: httpx.AsyncClient | None = None
        self._injected_client = client
        self._owns_client: bool = False

    async def __aenter__(self) -> "MemoryClient":
        """Async context manager entry."""
        if self._injected_client is not None:
            self._client = self._injected_client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._headers,
            )
            self._owns_client = True
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self._client and self._owns_client:
            await self._client.aclose()
        self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if not self._client:
            raise NovaOSError(
                "MemoryClient is not initialized. "
                "Use async context manager or call __aenter__() first."
            )
        return self._client

    async def _send_mcp(
        self,
        tool: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Send an MCP tool call with retry and circuit breaker.

        Retries up to ``max_retries`` times for transient errors (429, 502, 503,
        504, connection errors). Non-retryable errors are raised immediately.
        Always issues at least one attempt: ``max_retries=0`` means "issue the
        one attempt and never retry it", not "issue no attempt at all".

        Raises:
            NovaOSConnectionError: If connection to Memory Service fails after retries.
            NovaOSServerError: If the response contains errors after retries.
        """
        if not self._cb.allow_request():
            raise NovaOSConnectionError(
                "Memory Service circuit breaker is OPEN — failing fast. "
                f"Will probe again in {self._cb.reset_timeout}s.",
            )

        last_error: NovaOSError | None = None
        # `max(self._max_retries, 1)`: a `range(1, self._max_retries + 1)` bound
        # directly on `self._max_retries` makes `max_retries=0` an empty range --
        # zero HTTP attempts, followed by `raise last_error` with `last_error`
        # still `None`. Every other value already made `max_retries` total
        # attempts (not `max_retries` retries beyond the first), so this only
        # changes the `0` case.
        total_attempts = max(self._max_retries, 1)

        for attempt in range(1, total_attempts + 1):
            try:
                result = await self._send_mcp_once(
                    tool,
                    payload,
                    idempotency_key=idempotency_key,
                    request_id=request_id,
                    trace_id=trace_id,
                )
                self._cb.record_success()
                return result
            except NovaOSError as e:
                last_error = e
                if not e.is_retryable or attempt == total_attempts:
                    self._cb.record_failure()
                    raise

                delay = e.retry_after if e.retry_after else backoff_delay(attempt)
                logger.warning(
                    "Memory Service request failed (attempt %d/%d, retrying in %.1fs): %s",
                    attempt,
                    total_attempts,
                    delay,
                    e.message,
                )
                await asyncio.sleep(delay)

        self._cb.record_failure()
        raise last_error  # type: ignore[misc]

    async def _send_mcp_once(
        self,
        tool: str,
        payload: dict[str, Any],
        *,
        idempotency_key: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Single attempt to send an MCP tool call."""
        client = self._ensure_client()

        context: dict[str, Any] = {}
        if request_id:
            context["request_id"] = request_id
        if trace_id:
            context["trace_id"] = trace_id
        if idempotency_key:
            context["idempotency_key"] = idempotency_key

        body: dict[str, Any] = {
            "tool": tool,
            "payload": payload,
            "context": context,
        }

        with traced_tool_call(tool, payload):
            try:
                # Merge auth headers explicitly rather than relying on the client's own
                # default headers — an injected client (see `client=` on __init__) may not
                # have been constructed with self._headers baked in, and silently dropping
                # them would send an unauthenticated request instead of failing loudly.
                trace_headers = inject_trace_headers({})
                request_headers = {**self._headers, **trace_headers}
                response = await client.post("/api/v1/mcp", json=body, headers=request_headers)
                response.raise_for_status()
            except httpx.ConnectError as e:
                raise NovaOSConnectionError(
                    f"Failed to connect to Memory Service: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.TimeoutException as e:
                raise NovaOSConnectionError(
                    f"Memory Service request timed out: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.HTTPStatusError as e:
                retry_after = None
                if e.response.status_code == 429:
                    ra_header = e.response.headers.get("Retry-After")
                    if ra_header:
                        try:
                            retry_after = float(ra_header)
                        except ValueError:
                            pass
                raise NovaOSServerError(
                    status_error_message("Memory Service", e),
                    status_code=e.response.status_code,
                    retry_after=retry_after,
                    details=status_error_details(e),
                ) from e

            envelope = response.json()

            if envelope.get("errors"):
                errors = envelope["errors"]
                first = errors[0] if errors else {}
                raise NovaOSServerError(
                    f"Memory Service error: {first.get('title', 'Unknown')} — {first.get('detail', '')}",
                    status_code=first.get("status"),
                    details={"errors": errors},
                )

            return envelope.get("data", {})

    async def _get_json(
        self,
        path: str,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """HTTP GET with the same retry / circuit-breaker logic as ``_send_mcp``.

        Used for REST endpoints that don't go through the MCP dispatch envelope
        (e.g. ``/api/v1/memory/proposals/{id}``).
        """
        if not self._cb.allow_request():
            raise NovaOSConnectionError(
                "Memory Service circuit breaker is OPEN — failing fast. "
                f"Will probe again in {self._cb.reset_timeout}s.",
            )

        last_error: NovaOSError | None = None
        total_attempts = max(self._max_retries, 1)

        for attempt in range(1, total_attempts + 1):
            try:
                result = await self._get_json_once(
                    path,
                    request_id=request_id,
                    trace_id=trace_id,
                )
                self._cb.record_success()
                return result
            except NovaOSError as e:
                last_error = e
                if not e.is_retryable or attempt == total_attempts:
                    self._cb.record_failure()
                    raise

                delay = e.retry_after if e.retry_after else backoff_delay(attempt)
                logger.warning(
                    "Memory Service GET %s failed (attempt %d/%d, retrying in %.1fs): %s",
                    path,
                    attempt,
                    total_attempts,
                    delay,
                    e.message,
                )
                await asyncio.sleep(delay)

        self._cb.record_failure()
        raise last_error  # type: ignore[misc]

    async def _get_json_once(
        self,
        path: str,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Single-attempt HTTP GET returning the ``data`` envelope."""
        client = self._ensure_client()

        trace_headers = inject_trace_headers({})
        request_headers = {**self._headers, **trace_headers}
        if request_id:
            request_headers["X-Request-Id"] = request_id
        if trace_id:
            request_headers["X-Trace-Id"] = trace_id

        try:
            response = await client.get(path, headers=request_headers)
            response.raise_for_status()
        except httpx.ConnectError as e:
            raise NovaOSConnectionError(
                f"Failed to connect to Memory Service: {e}",
                details={"original_error": type(e).__name__},
            ) from e
        except httpx.TimeoutException as e:
            raise NovaOSConnectionError(
                f"Memory Service request timed out: {e}",
                details={"original_error": type(e).__name__},
            ) from e
        except httpx.HTTPStatusError as e:
            retry_after = None
            if e.response.status_code == 429:
                ra_header = e.response.headers.get("Retry-After")
                if ra_header:
                    try:
                        retry_after = float(ra_header)
                    except ValueError:
                        pass
            raise NovaOSServerError(
                status_error_message("Memory Service", e),
                status_code=e.response.status_code,
                retry_after=retry_after,
                details=status_error_details(e),
            ) from e

        envelope = response.json()

        if envelope.get("errors"):
            errors = envelope["errors"]
            first = errors[0] if errors else {}
            raise NovaOSServerError(
                f"Memory Service error: {first.get('title', 'Unknown')} — {first.get('detail', '')}",
                status_code=first.get("status"),
                details={"errors": errors},
            )

        return envelope.get("data", {})

    # ------------------------------------------------------------------
    # Public API — one method per MCP tool
    # ------------------------------------------------------------------

    async def query(
        self,
        scope: str,
        query: str,
        *,
        top_k: int = 10,
        filters: dict[str, Any] | None = None,
        memory_type: MemoryType | None = None,
        conversation_id: str | None = None,
        token_budget: int | None = None,
        latency_budget_ms: int | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> QueryResult:
        """Query memories by scope and semantic similarity.

        Args:
            scope: Memory scope to query (``user``, ``agent``, ``run``, ``tenant``).
            query: Natural language query for semantic search.
            top_k: Maximum number of results (1-100, default 10).
            filters: Optional filters (``tags``, ``after``, ``before``,
                ``classification``).
            memory_type: Optional. Only return memories written with this type
                (``semantic``, ``episodic`` or ``procedural``). Applied before
                ``top_k``, so every match up to ``top_k`` comes back. Memories
                written without a type never match. Shortcut for
                ``filters["memory_type"]``.
            conversation_id: Optional. Recall durable memories written under this
                conversation id, ranked by similarity to ``query``. Applies to
                durable scopes only (``user``, ``agent``, ``tenant``); ignored for
                ``run``/``task``. Shortcut for ``filters["conversation_id"]``.
            token_budget: Optional token budget for the result set.
            latency_budget_ms: Optional latency budget in milliseconds.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            QueryResult with matching items and retrieval metadata.
        """
        budget: dict[str, Any] = {"top_k": top_k}
        if token_budget is not None:
            budget["token_budget"] = token_budget
        if latency_budget_ms is not None:
            budget["latency_budget_ms"] = latency_budget_ms
        payload: dict[str, Any] = {
            "scope": scope,
            "query": query,
            "budget": budget,
        }
        recall_filters = _recall_filters(filters, memory_type, conversation_id)
        if recall_filters:
            payload["filters"] = recall_filters

        data = await self._send_mcp(
            "memory.query",
            payload,
            request_id=request_id,
            trace_id=trace_id,
        )
        return QueryResult.from_dict(data)

    async def write(
        self,
        scope: str,
        content: str,
        *,
        tags: list[str] | None = None,
        source: str | None = None,
        confidence: float | None = None,
        custom: dict[str, Any] | None = None,
        memory_type: MemoryType | None = None,
        ttl_seconds: int | None = None,
        classification: str | None = None,
        idempotency_key: str | None = None,
        ctx: "AgentExecutionContext | None" = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> WriteResult:
        """Write a new memory to ``agent`` or ``run`` scope.

        **Scope restrictions (platform policy):**

        - ``agent`` — allowed; writes to the caller's own agent memory.
        - ``run`` — allowed; writes ephemeral data scoped to the current run.
        - ``user`` — **blocked** for agent principals (policy R1). Use
          :meth:`hint` to submit observations for the consolidation pipeline,
          which decides what to persist to user memory.
        - ``tenant`` — **blocked** for agent principals (admin-only).

        Args:
            scope: Memory scope (``agent`` or ``run`` for agents).
            content: Memory content text (max 32 KB).
            tags: Optional tags for categorization (max 20, each max 64 chars).
            source: Optional source identifier.
            confidence: Optional confidence score (0.0-1.0).
            custom: Optional custom metadata dict.
            memory_type: Optional kind of the memory (``semantic``, ``episodic``
                or ``procedural``). Omitted: nothing extra is stored and the
                memory matches no ``memory_type`` query filter.
            ttl_seconds: Optional lifetime of the memory in seconds (the service
                enforces its own minimum and per-scope maximum and rejects a value
                outside them). Omitted: the scope's default lifetime applies.
            classification: Optional classification label (``inferred``,
                ``explicit``, ``system``, ``confidential``). Omitted: the
                service default applies.
            idempotency_key: Optional key for write idempotency (24h TTL). If
                omitted and ``ctx`` is given, derived from
                ``ctx.idempotency_key`` plus a per-execution ordinal (tracked
                on ``ctx``, so it survives across separate ``MemoryClient``
                instances) so repeated writes within the same execution get
                distinct, stable keys instead of colliding on one dedup key.
            ctx: The calling agent's execution context, used to derive
                ``idempotency_key`` when it isn't passed explicitly.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            WriteResult with the new memory ID and event metadata.

        Raises:
            NovaOSServerError: If scope is ``user`` or ``tenant`` for agent
                principals (HTTP 422).
        """
        metadata: dict[str, Any] = {}
        if tags is not None:
            metadata["tags"] = tags
        if source is not None:
            metadata["source"] = source
        if confidence is not None:
            metadata["confidence"] = confidence
        if custom is not None:
            metadata["custom"] = custom
        if _check_memory_type(memory_type) is not None:
            metadata["memory_type"] = memory_type

        payload: dict[str, Any] = {
            "scope": scope,
            "content": content,
        }
        if metadata:
            payload["metadata"] = metadata
        if ttl_seconds is not None:
            payload["ttl_seconds"] = ttl_seconds
        if classification is not None:
            payload["classification"] = classification

        if idempotency_key is None:
            ctx_key = getattr(ctx, "idempotency_key", None) if ctx is not None else None
            if ctx_key:
                idempotency_key = f"{ctx_key}:{_next_write_ordinal(ctx)}"
            else:
                idempotency_key = uuid.uuid4().hex

        data = await self._send_mcp(
            "memory.write",
            payload,
            idempotency_key=idempotency_key,
            request_id=request_id,
            trace_id=trace_id,
        )
        return WriteResult.from_dict(data)

    async def poll_proposal(
        self,
        proposal_id: str,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> ProposalStatus:
        """Check the status of an agent-scope write proposal.

        After :meth:`write` returns a ``WriteResult`` with ``durable=False``
        (``prop_``-prefixed ``memory_id``), call this method to learn whether
        the AgentWriteGate has committed, rejected, or is still evaluating
        the proposal.

        Args:
            proposal_id: The ``prop_``-prefixed proposal ID from
                ``WriteResult.memory_id``.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            ProposalStatus with the gate decision and, when committed, the
            durable ``memory_id``.

        Raises:
            NovaOSServerError: If the proposal is not found (HTTP 404) or
                the service returns an error.
        """
        data = await self._get_json(
            f"/api/v1/memory/proposals/{proposal_id}",
            request_id=request_id,
            trace_id=trace_id,
        )
        return ProposalStatus.from_dict(data)

    async def update(
        self,
        scope: str,
        memory_id: str,
        version: int,
        *,
        content: str | None = None,
        tags: list[str] | None = None,
        confidence: float | None = None,
        custom: dict[str, Any] | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> UpdateResult:
        """Update one of the agent's own memories in place.

        Applies immediately (no proposal gate), so the next :meth:`query` returns
        the new content. Only ``agent``-scope memories can be updated; ``user``
        and ``tenant`` are refused for agent principals, and ``run``/``task``
        memories are ephemeral — :meth:`forget` and write a new one instead.
        New ``content`` is screened for PII and secrets and rejected on a hit.

        Args:
            scope: Memory scope — ``agent``.
            memory_id: ID of the committed memory (the ``memory_id`` on a
                :class:`MemoryItem` from :meth:`query`, not a ``prop_`` proposal ID).
            version: The memory's ``version`` as last read from :meth:`query`.
                Used as an optimistic lock: if the memory changed since, the call
                fails with HTTP 409 — re-read and retry.
            content: Optional new content (max 32 KB).
            tags: Optional replacement tags (max 20, each max 64 chars).
            confidence: Optional new confidence score (0.0-1.0).
            custom: Optional replacement custom metadata dict.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            UpdateResult with the memory's new version.

        Raises:
            NovaOSValidationError: If none of ``content``, ``tags``,
                ``confidence`` or ``custom`` is given.
            NovaOSServerError: HTTP 409 on a stale ``version``; 404 if the
                memory is not found; 403 if it belongs to another agent;
                422 for a scope that cannot be updated or content rejected by
                the PII/secret screen.
        """
        metadata: dict[str, Any] = {}
        if tags is not None:
            metadata["tags"] = tags
        if confidence is not None:
            metadata["confidence"] = confidence
        if custom is not None:
            metadata["custom"] = custom
        if content is None and not metadata:
            raise NovaOSValidationError(
                "update needs at least one of content, tags, confidence or custom"
            )

        payload: dict[str, Any] = {
            "scope": scope,
            "memory_id": memory_id,
            "version": version,
        }
        if content is not None:
            payload["content"] = content
        if metadata:
            payload["metadata"] = metadata

        data = await self._send_mcp(
            "memory.update",
            payload,
            request_id=request_id,
            trace_id=trace_id,
        )
        return UpdateResult.from_dict(data)

    async def forget(
        self,
        scope: str,
        memory_id: str,
        *,
        reason: str = "agent-requested",
        idempotency_key: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> ForgetResult:
        """Delete a specific memory by ID.

        Args:
            scope: Memory scope (``user``, ``agent``, ``run``).
            memory_id: ID of the memory to delete.
            reason: Human-readable reason for deletion (1-500 chars). Stored
                in the audit trail. Defaults to ``"agent-requested"``.
            idempotency_key: Optional key for delete idempotency (24h TTL).
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            ForgetResult with the delete count and event ID.

        Raises:
            NovaOSValidationError: If ``reason`` is empty or exceeds 500 characters.
            NovaOSServerError: If the memory is not found (HTTP 404).
        """
        if not reason or len(reason) > 500:
            raise NovaOSValidationError("reason must be 1-500 characters")

        payload: dict[str, Any] = {
            "scope": scope,
            "selector": {"memory_id": memory_id},
            "reason": reason,
        }

        if idempotency_key is None:
            idempotency_key = uuid.uuid4().hex

        data = await self._send_mcp(
            "memory.forget",
            payload,
            idempotency_key=idempotency_key,
            request_id=request_id,
            trace_id=trace_id,
        )
        return ForgetResult.from_dict(data)

    async def hint(
        self,
        hint: str,
        *,
        conversation_id: str | None = None,
        ctx: "AgentExecutionContext | None" = None,
        priority: str = "normal",
        tags: list[str] | None = None,
        context: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> HintResult:
        """Submit a consolidation hint for background fact extraction.

        Hints are fire-and-forget — acceptance does not guarantee persistence.

        Args:
            hint: Natural language hint about what to extract (max 8 KB).
            conversation_id: ID of the conversation to consolidate from. If
                omitted, derived from ``ctx.context_id``.
            ctx: The calling agent's execution context, used to derive
                ``conversation_id`` when it isn't passed explicitly.
            priority: Hint priority (``low``, ``normal``, ``high``).
            tags: Optional tags for the resulting memories.
            context: Optional additional context for the consolidation worker.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            HintResult with the hint ID and acceptance status.

        Raises:
            NovaOSValidationError: If ``conversation_id`` is omitted and
                ``ctx`` doesn't supply one either.
        """
        if conversation_id is None and ctx is not None:
            conversation_id = getattr(ctx, "context_id", None) or None
        if not conversation_id:
            raise NovaOSValidationError(
                "conversation_id is required — pass it explicitly or supply "
                "a ctx whose context_id is set."
            )

        payload: dict[str, Any] = {
            "conversation_id": conversation_id,
            "hint": hint,
            "priority": priority,
        }

        hint_metadata: dict[str, Any] = {}
        if tags is not None:
            hint_metadata["tags"] = tags
        if context is not None:
            hint_metadata["context"] = context
        if hint_metadata:
            payload["metadata"] = hint_metadata

        data = await self._send_mcp(
            "memory.hint",
            payload,
            request_id=request_id,
            trace_id=trace_id,
        )
        return HintResult.from_dict(data)

    async def hint_status(
        self,
        hint_id: str,
        *,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> HintStatus:
        """Read the outcome of a hint this agent submitted.

        ``hint()`` only says the hint was accepted. This says whether
        consolidation stored it: ``pending`` until the cycle resolves it, then
        ``consolidated`` or ``dead_lettered``; ``expired`` if it stayed pending
        for over an hour. State is kept for 24 hours.

        Raises:
            NovaOSServerError: HTTP 404 if the hint is unknown, has aged out, or was
                submitted by a different agent.
        """
        data = await self._send_mcp(
            "memory.hint_status",
            {"hint_id": hint_id},
            request_id=request_id,
            trace_id=trace_id,
        )
        return HintStatus.from_dict(data)

    async def assemble_context(
        self,
        query: str,
        *,
        top_k: int = 10,
        live_message_limit: int = 20,
        token_budget: int | None = None,
        latency_budget_ms: int | None = None,
        memory_type: MemoryType | None = None,
        conversation_id: str | None = None,
        ctx: "AgentExecutionContext | None" = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> AssembleResult:
        """Assemble a unified context bundle from all available memory scopes.

        Queries durable user memories, agent memories, live conversation
        messages, and run data in parallel. Gracefully degrades if individual
        sources fail (sets ``partial=True``).

        Args:
            query: Natural language query for semantic ranking.
            top_k: Max durable memories per scope (1-100, default 10).
            live_message_limit: Max live messages to include (1-100, default 20).
            token_budget: Overall token budget for the assembled bundle, if the
                caller has one — e.g. from ``ctx.memory_budget``. ``None``
                leaves the server's own default in place.
            latency_budget_ms: Per-turn latency budget in milliseconds, if the
                caller has one. ``None`` leaves the server's own default in
                place.
            memory_type: Optional. Only include durable memories written with
                this type (``semantic``, ``episodic`` or ``procedural``); also
                narrows run and task data. Omitted: no type filtering.
            conversation_id: Optional. Restrict durable recall to memories written
                under this conversation id (similarity-ranked). Opt-in; omitted:
                recall spans all conversations as before.
            ctx: The calling agent's execution context. Supplies
                ``request_id``/``trace_id`` from ``ctx.run_id``/``ctx.trace_id``
                when those aren't passed explicitly.
            request_id: Optional request ID for tracing.
            trace_id: Optional trace ID for correlation.

        Returns:
            AssembleResult with memories from all scopes and availability metadata.

        Note:
            The Memory Service scopes live messages to the caller's
            conversation from its own token claims, not from anything in this
            call's payload. ``conversation_id`` here is a different thing: an
            opt-in *recall filter* over durable memories written under that
            conversation id, not the conversation the live messages come from.
        """
        budget: dict[str, Any] = {
            "top_k": top_k,
            "live_message_limit": live_message_limit,
        }
        if token_budget is not None:
            budget["token_budget"] = token_budget
        if latency_budget_ms is not None:
            budget["latency_budget_ms"] = latency_budget_ms

        payload: dict[str, Any] = {
            "query": query,
            "budget": budget,
        }
        recall_filters = _recall_filters(None, memory_type, conversation_id)
        if recall_filters:
            payload["filters"] = recall_filters

        data = await self._send_mcp(
            "memory.assemble",
            payload,
            request_id=request_id or (getattr(ctx, "run_id", None) or None),
            trace_id=trace_id or (getattr(ctx, "trace_id", None) or None),
        )
        return AssembleResult.from_dict(data)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
