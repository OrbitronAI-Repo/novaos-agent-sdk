"""
Platform tool bindings for the NovaOS Agent SDK.

Each tool binding makes governed MCP Gateway calls with auto-injected
auth tokens and tenant context. In sandbox mode (NOVAOS_ENV=sandbox),
calls are routed to local mock services instead.

Tools:
  memory, knowledge, agents, hitl, files, notify, http, browser, code

Ported from NovaOs/packages/agent-sdk/python/novaos_sdk/tools.py
Adapted to use httpx (already a dependency of novaos-client-sdk).
"""

import asyncio
import dataclasses
import json
import logging
import os
import time
import uuid
import warnings
from collections.abc import Callable
from typing import Any, Literal

from novaos_client_sdk import config as _config
from novaos_client_sdk._http import httpx
from novaos_client_sdk._resilience import (
    RETRYABLE_HTTP_STATUSES as _RETRYABLE_HTTP_STATUSES,
)
from novaos_client_sdk._resilience import (
    CircuitBreaker,
)
from novaos_client_sdk._resilience import (
    backoff_delay as _backoff_delay,
)
from novaos_client_sdk.hitl_effect import MCP_TOOLS_CALL_WRITE, mcp_tool_action_class
from novaos_client_sdk.tracing import get_traceparent, inject_trace_headers, traced_tool_call

from .agent_types import ToolResponse

_logger = logging.getLogger(__name__)

_HAS_EVAL = False
try:
    from novaos_client_sdk.evaluation.context import get_eval_context

    _HAS_EVAL = True
except ImportError:
    pass

_DEFAULT_TOOL_CALL_TIMEOUT_S = 128  # matches config.get_default_tool_timeout_s()'s built-in default
_INGEST_TOOL_TIMEOUT = 120  # only for the ingest submission itself
_SANDBOX_ENV_VAR = "NOVAOS_ENV"
_SANDBOX_URL_VAR = "NOVAOS_SANDBOX_URL"
_DEFAULT_SANDBOX_URL = "http://localhost:4000"


def _timeout_from_ms(timeout_ms: int | None, margin_s: int = 8) -> int:
    """Convert tool-declared timeout milliseconds to HTTP timeout seconds."""
    if timeout_ms is None:
        return _config.get_default_tool_timeout_s()
    return timeout_ms // 1000 + margin_s


def _extract_error_detail(response: "httpx.Response") -> str | None:
    """Best-effort extraction of a diagnostic message from an error response body.

    Gateway-fronted services don't share one error shape — connector-host's
    feature-flag gate returns ``{"error": "SERVICE_DISABLED", "message": "..."}``,
    its auth/entitlement errors return ``{"error_category": ..., "errors": [...]}``,
    others use a bare ``{"detail": "..."}``. Try the common shapes in order and
    fall back to the raw response text so a caller gets more than a bare status
    code, regardless of which service is on the other end.
    """
    try:
        body = response.json()
    except ValueError:
        text = response.text.strip()
        return text[:500] if text else None

    if not isinstance(body, dict):
        return None

    error = body.get("error")
    if isinstance(error, str):
        message = body.get("message")
        return f"{error}: {message}" if message else error
    if isinstance(error, dict):
        return error.get("message") or error.get("code")

    errors = body.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            return first.get("message") or first.get("code") or str(first)
        return str(first)

    for key in ("detail", "message"):
        value = body.get(key)
        if isinstance(value, str) and value:
            return value

    return None


def _unwrap_mcp_content(data: Any) -> dict[str, Any]:
    """Unwrap an MCP content envelope to the actual tool result dict.

    The RAG MCP server serialises tool output as::

        {"content": [{"type": "text", "text": "<json-stringified-result>"}]}

    This helper extracts and JSON-decodes the inner payload so callers can
    access fields like ``status`` or ``job_id`` directly.  If *data* is
    already a plain dict (e.g. a non-RAG response or a test mock), it is
    returned unchanged.
    """
    if not isinstance(data, dict):
        return {}
    content = data.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict) and first.get("type") == "text":
            try:
                parsed = json.loads(first["text"])
                if isinstance(parsed, dict):
                    return parsed
            except (json.JSONDecodeError, KeyError):
                pass
    return data


def _connector_fqn(connector_id: str, operation: str, tier: str = "standard") -> str:
    """Build the MCP tool name connector-host's catalog registers for a given tier.

    Mirrors how the platform's own catalog builds and parses this same name and
    must stay in lockstep with it: ``standard`` is the plain 4-segment name;
    ``licensed``/``ext`` insert themselves as an extra segment right after
    ``novaos.connector``.
    """
    if tier == "standard":
        return f"novaos.connector.{connector_id}.{operation}"
    return f"novaos.connector.{tier}.{connector_id}.{operation}"


# Terminal statuses for RAG ingestion jobs (JobStatus enum in nova-os-rag).
_RAG_TERMINAL_STATUSES = frozenset({"completed", "failed", "dead_letter", "duplicate"})

# submit_error is terminal at the SDK layer (the job was never queued on the backend).
_BATCH_TERMINAL_STATUSES = _RAG_TERMINAL_STATUSES | {"submit_error"}

# Terminal states for a GraphRAG build (nova-os-rag's graph_status field).
# Unlike ingestion jobs there is no dead_letter/duplicate equivalent — a graph
# build is either still in flight ("none"/"pending"/"running") or done.
_GRAPH_BUILD_TERMINAL_STATUSES = frozenset({"completed", "failed"})


@dataclasses.dataclass
class IngestItem:
    """A single document to submit in a batch ingestion request.

    Attributes:
        content: Raw document text to ingest.
        metadata: Optional key-value metadata passed through to the backend
            (e.g. ``collection_id``, ``filename``, ``source_url``).
    """

    content: str
    metadata: dict[str, Any] | None = None


@dataclasses.dataclass
class IngestJobStatus:
    """Per-item status within a :class:`BatchIngestResponse`.

    Attributes:
        index: Zero-based position of this item in the original
            :meth:`KnowledgeTool.ingest_batch` call.
        job_id: Backend job UUID, or ``None`` when submission itself failed.
        status: One of ``"pending"``, ``"running"``, ``"completed"``,
            ``"failed"``, ``"dead_letter"``, ``"duplicate"``,
            ``"submit_error"``, or ``"unknown"``.
        error: Human-readable error message when *status* is ``"failed"``,
            ``"dead_letter"``, or ``"submit_error"``; ``None`` otherwise.
    """

    index: int
    job_id: str | None
    status: str
    error: str | None = None


@dataclasses.dataclass
class BatchIngestResponse:
    """Aggregate response from :meth:`KnowledgeTool.ingest_batch`.

    Attributes:
        batch_id: Client-generated UUID correlating all items in this batch.
        total: Number of items originally submitted.
        items: Per-item statuses in input order.  Poll or pass to
            :meth:`KnowledgeTool.poll_batch_until_complete` for completion.
    """

    batch_id: str
    total: int
    items: list[IngestJobStatus]


class _BaseToolBinding:
    """Base class for all tool bindings."""

    def __init__(
        self,
        mcp_gateway_url: str,
        token: str,
        tenant_id: str,
        *,
        default_timeout_s: int | None = None,
        gateway_timeout_s: int | None = None,
    ):
        self._mcp_gateway_url = mcp_gateway_url
        self._token = token
        self._tenant_id = tenant_id
        # See docs/timeout-hierarchy.md. default_timeout_s resolves from
        # NOVAOS_TOOL_TIMEOUT_S (or the built-in 128s) when not given explicitly.
        # gateway_timeout_s is opt-in validation — there is no single platform-wide
        # Gateway ceiling to assume, so it's off (None) unless the caller sets it.
        self._default_timeout_s = (
            default_timeout_s
            if default_timeout_s is not None
            else _config.get_default_tool_timeout_s()
        )
        self._gateway_timeout_s = gateway_timeout_s
        self._is_sandbox = os.environ.get(_SANDBOX_ENV_VAR) == "sandbox"
        self._sandbox_url = os.environ.get(_SANDBOX_URL_VAR, _DEFAULT_SANDBOX_URL)
        self._entity_refs: list[str] = []
        # Per-request W3C traceparent injected by the Temporal worker for this run.
        # Takes priority over TRACEPARENT env-var (which is stale in fleet mode where
        # the same pod handles sequential runs without restart). (P4-fleet F16)
        self._per_request_traceparent: str = ""
        # Current run's id, set by AgentExecutionContext so tool-call spans carry
        # it as an attribute (novaos.run_id) for correlation with the platform's
        # own run records — see `traced_tool_call`.
        self._per_request_run_id: str = ""
        # Set by AgentExecutionContext for a streaming request. `None`
        # — the default, and what every non-streaming call keeps — means "emit
        # nothing", so a skill invoked outside a streaming request behaves exactly
        # as it did before this hook existed.
        self._event_sink: Callable[[str, dict[str, Any]], None] | None = None

    def set_entity_refs(self, refs: list[str]) -> None:
        self._entity_refs = list(refs)

    def set_event_sink(self, sink: Callable[[str, dict[str, Any]], None] | None) -> None:
        """Wire this binding's tool calls into a live event stream.

        Called by :class:`~novaos_client_sdk.agent_context.AgentExecutionContext`
        when handling a ``SendStreamingMessage`` request, so every governed tool
        call this binding makes emits a ``tool.call`` / ``tool.result`` pair the
        caller can publish as it happens rather than only at the end of the run.
        See :meth:`_call_tool` for where the sink is actually invoked.
        """
        self._event_sink = sink

    def set_traceparent(self, traceparent: str) -> None:
        """Set a per-request W3C traceparent that takes priority over TRACEPARENT env var.

        Call this from AgentExecutionContext once the inbound A2A request traceparent
        is known so that outbound MCP tool calls carry the correct trace context in
        fleet mode where ``TRACEPARENT`` env var is set at pod startup and becomes
        stale across sequential runs (P4-fleet F16).
        """
        self._per_request_traceparent = traceparent

    def set_run_id(self, run_id: str) -> None:
        """Set the current run's id so tool-call spans carry it as an attribute.

        Call this from AgentExecutionContext once the inbound request's run_id
        is known, mirroring :meth:`set_traceparent`.
        """
        self._per_request_run_id = run_id

    def _emit_event(self, event_type: str, payload: dict[str, Any]) -> None:
        """Push one event to :attr:`_event_sink`, if a streaming request set one."""
        if self._event_sink is not None:
            self._event_sink(event_type, payload)

    async def _call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        """Make a governed MCP tool call with auto-injected auth.

        Thin wrapper around :meth:`_call_tool_impl` that brackets it with
        ``tool.call`` / ``tool.result`` on :attr:`_event_sink` — a
        no-op when no sink is set, which is every non-streaming request, so this
        is a pure addition for every existing caller. Emitting here rather than
        inside the implementation means exactly one ``tool.result`` per logical
        call regardless of which internal branch produced it, including the one
        that raised — a live tool timeline needs to know a call finished (and
        how) even when it failed.
        """
        self._emit_event("tool.call", {"tool_name": tool_name, "arguments": arguments})
        resp = await self._call_tool_impl(tool_name, arguments, timeout_s=timeout_s)
        self._emit_event(
            "tool.result",
            {
                "tool_name": tool_name,
                "result": resp.data if resp.success else None,
                "success": resp.success,
                "error": resp.error,
            },
        )
        return resp

    async def _call_tool_impl(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        """Make a governed MCP tool call with auto-injected auth.

        Automatically records the call in the active EvaluationContext (if any).
        ``timeout_s`` overrides this binding's ``default_timeout_s`` (see
        docs/timeout-hierarchy.md) for calls that are expected to take longer
        (e.g. document ingestion).
        """
        base_url = self._sandbox_url if self._is_sandbox else self._mcp_gateway_url
        url = f"{base_url}/mcp/v1/tools/call"

        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "X-Tenant-ID": self._tenant_id,
        }

        if self._entity_refs:
            headers["X-NovaOS-Entity-Refs"] = json.dumps(self._entity_refs)

        effective_timeout = timeout_s if timeout_s is not None else self._default_timeout_s
        _config.validate_tool_timeout(effective_timeout, self._gateway_timeout_s)
        start = time.perf_counter()
        with traced_tool_call(tool_name, arguments, run_id=self._per_request_run_id) as span:
            # Inject W3C traceparent from the active span (or env fallback) so the
            # MCP Gateway can link child HTTP spans to this tool span.
            inject_trace_headers(headers)
            # Per-request traceparent ALWAYS wins over whatever inject_trace_headers set
            # (which may be a stale TRACEPARENT env var from pod startup in fleet mode).
            if self._per_request_traceparent:
                headers["traceparent"] = self._per_request_traceparent
            elif "traceparent" not in headers:
                tp = get_traceparent()
                if tp:
                    headers["traceparent"] = tp

            try:
                async with httpx.AsyncClient(timeout=effective_timeout) as client:
                    response = await client.post(url, json=body, headers=headers)
                elapsed_ms = (time.perf_counter() - start) * 1000
                if not response.is_success:
                    detail = _extract_error_detail(response)
                    error_msg = (
                        f"HTTP {response.status_code}: {detail}"
                        if detail
                        else f"HTTP {response.status_code}"
                    )
                    resp = ToolResponse(success=False, error=error_msg)
                    if span is not None:
                        from opentelemetry.trace import StatusCode

                        span.set_status(StatusCode.ERROR, error_msg)
                    self._record_eval(tool_name, arguments, resp, elapsed_ms)
                    return resp
                result = response.json()
                if "error" in result:
                    error_msg = result["error"].get("message", "Unknown error")
                    resp = ToolResponse(success=False, error=error_msg)
                    if span is not None:
                        from opentelemetry.trace import StatusCode

                        span.set_status(StatusCode.ERROR, error_msg)
                    self._record_eval(tool_name, arguments, resp, elapsed_ms)
                    return resp
                resp = ToolResponse(success=True, data=result.get("result"))
                self._record_eval(tool_name, arguments, resp, elapsed_ms)
                return resp
            except Exception as e:
                elapsed_ms = (time.perf_counter() - start) * 1000
                error_msg = str(e)
                resp = ToolResponse(success=False, error=error_msg)
                if span is not None:
                    from opentelemetry.trace import StatusCode

                    span.set_status(StatusCode.ERROR, error_msg)
                self._record_eval(tool_name, arguments, resp, elapsed_ms)
                return resp

    async def _call_tool_with_retry(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        timeout_s: int | None = None,
        max_retries: int = 3,
        base_delay_s: float = 1.0,
        circuit_breaker: "CircuitBreaker | None" = None,
    ) -> ToolResponse:
        """Like :meth:`_call_tool` but with exponential backoff and circuit breaker.

        Thin event-emitting wrapper around :meth:`_call_tool_with_retry_impl`,
        for the same reason and with the same one-pair-per-logical-call contract
        as :meth:`_call_tool`'s wrapper — a caller sees one ``tool.call`` /
        ``tool.result`` per call to this method, not one per retry attempt.

        Args:
            tool_name: MCP tool name (e.g. ``"novaos.knowledge.ingest"``).
            arguments: Tool arguments dict.
            timeout_s: Per-attempt HTTP timeout in seconds.  Defaults to this
                binding's ``default_timeout_s`` (see docs/timeout-hierarchy.md).
            max_retries: Maximum number of attempts (default 3 — i.e. initial
                call + 2 retries).
            base_delay_s: Base backoff delay in seconds (default 1.0).
            circuit_breaker: Optional :class:`~novaos_client_sdk._resilience.CircuitBreaker`
                instance shared across calls to the same service endpoint.

        Returns:
            :class:`ToolResponse` — same contract as :meth:`_call_tool`.
        """
        self._emit_event("tool.call", {"tool_name": tool_name, "arguments": arguments})
        resp = await self._call_tool_with_retry_impl(
            tool_name,
            arguments,
            timeout_s=timeout_s,
            max_retries=max_retries,
            base_delay_s=base_delay_s,
            circuit_breaker=circuit_breaker,
        )
        self._emit_event(
            "tool.result",
            {
                "tool_name": tool_name,
                "result": resp.data if resp.success else None,
                "success": resp.success,
                "error": resp.error,
            },
        )
        return resp

    async def _call_tool_with_retry_impl(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        timeout_s: int | None = None,
        max_retries: int = 3,
        base_delay_s: float = 1.0,
        circuit_breaker: "CircuitBreaker | None" = None,
    ) -> ToolResponse:
        """Retry/circuit-breaker implementation behind :meth:`_call_tool_with_retry`.

        Retries up to *max_retries* times for transient failures:

        * Network errors (timeout, connection reset)
        * HTTP 429, 502, 503, 504

        Non-retryable errors (HTTP 4xx other than 429, MCP-level semantic errors)
        are returned immediately as ``ToolResponse(success=False)``.

        A *circuit_breaker* (if provided) short-circuits further attempts when the
        service has seen too many consecutive failures, preventing thundering-herd
        load on a degraded backend.
        """
        # Fast-fail if circuit breaker is open.
        if circuit_breaker is not None and not circuit_breaker.allow_request():
            return ToolResponse(
                success=False,
                error=(
                    "Circuit breaker OPEN — MCP Gateway temporarily unavailable. "
                    f"Will probe again in {circuit_breaker.reset_timeout:.0f}s."
                ),
            )

        base_url = self._sandbox_url if self._is_sandbox else self._mcp_gateway_url
        url = f"{base_url}/mcp/v1/tools/call"
        body: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        effective_timeout = timeout_s if timeout_s is not None else self._default_timeout_s
        _config.validate_tool_timeout(effective_timeout, self._gateway_timeout_s)
        overall_start = time.perf_counter()
        last_error: str = "unknown error"

        with traced_tool_call(tool_name, arguments, run_id=self._per_request_run_id) as span:
            for attempt in range(1, max_retries + 1):
                # Build fresh headers each attempt so trace headers are current.
                headers: dict[str, str] = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._token}",
                    "X-Tenant-ID": self._tenant_id,
                }
                if self._entity_refs:
                    headers["X-NovaOS-Entity-Refs"] = json.dumps(self._entity_refs)
                inject_trace_headers(headers)
                # Per-request traceparent ALWAYS wins over whatever inject_trace_headers set
                # (which may be a stale TRACEPARENT env var from pod startup in fleet mode).
                if self._per_request_traceparent:
                    headers["traceparent"] = self._per_request_traceparent
                elif "traceparent" not in headers:
                    tp = get_traceparent()
                    if tp:
                        headers["traceparent"] = tp

                attempt_start = time.perf_counter()
                try:
                    async with httpx.AsyncClient(timeout=effective_timeout) as client:
                        response = await client.post(url, json=body, headers=headers)

                    # --- Retryable HTTP status codes ---------------------------------
                    if response.status_code in _RETRYABLE_HTTP_STATUSES:
                        last_error = f"HTTP {response.status_code}: {response.reason_phrase}"
                        if attempt < max_retries:
                            # Honour server-sent Retry-After when present.
                            try:
                                ra = float(response.headers.get("Retry-After", 0))
                                delay = ra if ra > 0 else _backoff_delay(attempt, base_delay_s)
                            except (ValueError, TypeError):
                                delay = _backoff_delay(attempt, base_delay_s)
                            _logger.warning(
                                "KnowledgeTool %s: attempt %d/%d → %s; retrying in %.1fs",
                                tool_name,
                                attempt,
                                max_retries,
                                last_error,
                                delay,
                            )
                            await asyncio.sleep(delay)
                            continue
                        # All retries exhausted — record the final failure.
                        if circuit_breaker is not None:
                            circuit_breaker.record_failure()
                        resp = ToolResponse(success=False, error=last_error)
                        if span is not None:
                            from opentelemetry.trace import StatusCode

                            span.set_status(StatusCode.ERROR, last_error)
                        self._record_eval(
                            tool_name,
                            arguments,
                            resp,
                            (time.perf_counter() - overall_start) * 1000,
                        )
                        return resp

                    # --- Non-retryable HTTP error (4xx/5xx not in retryable set) ----
                    if not response.is_success:
                        last_error = f"HTTP {response.status_code}: {response.reason_phrase}"
                        if circuit_breaker is not None:
                            circuit_breaker.record_failure()
                        resp = ToolResponse(success=False, error=last_error)
                        if span is not None:
                            from opentelemetry.trace import StatusCode

                            span.set_status(StatusCode.ERROR, last_error)
                        self._record_eval(
                            tool_name,
                            arguments,
                            resp,
                            (time.perf_counter() - overall_start) * 1000,
                        )
                        return resp

                    # --- Parse result ------------------------------------------------
                    result = response.json()
                    if "error" in result:
                        # MCP-level semantic error — not retryable.
                        error_msg = result["error"].get("message", "Unknown error")
                        resp = ToolResponse(success=False, error=error_msg)
                        if circuit_breaker is not None:
                            circuit_breaker.record_failure()
                        if span is not None:
                            from opentelemetry.trace import StatusCode

                            span.set_status(StatusCode.ERROR, error_msg)
                        self._record_eval(
                            tool_name,
                            arguments,
                            resp,
                            (time.perf_counter() - overall_start) * 1000,
                        )
                        return resp

                    # --- Success -----------------------------------------------------
                    if circuit_breaker is not None:
                        circuit_breaker.record_success()
                    resp = ToolResponse(success=True, data=result.get("result"))
                    self._record_eval(
                        tool_name,
                        arguments,
                        resp,
                        (time.perf_counter() - overall_start) * 1000,
                    )
                    return resp

                except (httpx.TimeoutException, httpx.ConnectError) as e:
                    # Network-level transient failure — retryable.
                    last_error = str(e)
                    if attempt < max_retries:
                        delay = _backoff_delay(attempt, base_delay_s)
                        _logger.warning(
                            "KnowledgeTool %s: attempt %d/%d network error (%s); retrying in %.1fs",
                            tool_name,
                            attempt,
                            max_retries,
                            type(e).__name__,
                            delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    # All retries exhausted — record the final failure.
                    if circuit_breaker is not None:
                        circuit_breaker.record_failure()
                    resp = ToolResponse(success=False, error=last_error)
                    if span is not None:
                        from opentelemetry.trace import StatusCode

                        span.set_status(StatusCode.ERROR, last_error)
                    self._record_eval(
                        tool_name,
                        arguments,
                        resp,
                        (time.perf_counter() - overall_start) * 1000,
                    )
                    return resp

                except Exception as e:
                    # Unexpected error — do not retry.
                    elapsed_ms = (time.perf_counter() - attempt_start) * 1000
                    error_msg = str(e)
                    resp = ToolResponse(success=False, error=error_msg)
                    if span is not None:
                        from opentelemetry.trace import StatusCode

                        span.set_status(StatusCode.ERROR, error_msg)
                    self._record_eval(tool_name, arguments, resp, elapsed_ms)
                    return resp

        # Unreachable — all paths inside the loop return; satisfies type checker.
        return ToolResponse(success=False, error=last_error)  # pragma: no cover

    async def call_ungoverned_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        """Invoke an arbitrary MCP tool FQN not covered by any first-class
        binding on :class:`ToolBindings` (``memory``, ``knowledge``, ``agents``,
        ``hitl``, ``files``, ``notify``, ``http``, ``browser``, ``code``,
        ``connectors``).

        ⚠️ **This bypasses the SDK's governed tool surface.** Every one of the
        methods above exists specifically so a caller only ever reaches tools
        the platform has explicitly typed and documented. This method exists
        because ``_call_tool``/``_call_tool_with_retry`` were previously only
        reachable as "private" methods with a leading underscore — which is
        not an access control in Python, just a convention, and at least one
        real agent has already called them directly to invoke a tool FQN with
        no first-class binding. Making that capability a named, public,
        logged method doesn't prevent the bypass (nothing in Python can), but
        it does two things a private method reachable-by-convention cannot:
        make the intent explicit and unmissable in a code review, and leave
        an audit trail every time it's actually used.

        The platform's own OPA policy may still deny the call outright (e.g.
        ``"Tool access denied: <fqn> (category mismatch)"``) — reaching this
        method does not guarantee the Gateway will honor it.

        Uses the plain (non-retrying) call path, matching most tool bindings
        here — an arbitrary tool's idempotency can't be assumed. If you need
        retry/circuit-breaker behavior for a specific arbitrary tool, call
        ``_call_tool_with_retry`` directly and take on that judgment call
        yourself.

        If you find yourself reaching for this repeatedly for the same tool,
        that's a signal the SDK should grow a first-class binding for it —
        please file that gap rather than normalizing this path.
        """
        _logger.warning(
            "call_ungoverned_tool(%r): bypassing the governed ToolBindings surface. "
            "This call may still be denied by platform policy.",
            tool_name,
        )
        return await self._call_tool(tool_name, arguments, timeout_s=timeout_s)

    @staticmethod
    def _record_eval(
        tool_name: str,
        arguments: dict[str, Any],
        response: ToolResponse,
        latency_ms: float,
    ) -> None:
        """Record tool call in the active EvaluationContext if one exists."""
        if not _HAS_EVAL:
            return
        ctx = get_eval_context()
        if ctx.is_active:
            ctx.add_tool_call(
                tool_name=tool_name,
                arguments=arguments,
                result=response.data if response.success else None,
                success=response.success,
                error=response.error or "",
                latency_ms=latency_ms,
            )
            ctx.add_tool_latency(latency_ms)


class KnowledgeTool(_BaseToolBinding):
    """RAG search and document ingestion.

    All network operations use exponential backoff with jitter and a shared
    circuit breaker so transient MCP Gateway failures are handled automatically.

    Resilience defaults:

    * **Max retries:** 3 (initial attempt + 2 retries)
    * **Base backoff:** 1.0 s → 2.0 s → 4.0 s (±50 % jitter applied)
    * **Retryable statuses:** HTTP 429, 502, 503, 504 and network timeouts
    * **Circuit breaker:** trips after 5 consecutive failures, resets after 30 s
    * **Timeout:** ``default_timeout_s`` (128 s by default, see
      docs/timeout-hierarchy.md) for search / status / graph operations;
      120 s for ingest submissions specifically

    These defaults match the ``MemoryClient`` retry policy and can be customised
    via constructor arguments.
    """

    #: Default retry count for all KnowledgeTool operations.
    DEFAULT_MAX_RETRIES: int = 3
    #: Default base backoff delay in seconds.
    DEFAULT_BASE_DELAY_S: float = 1.0

    def __init__(
        self,
        mcp_gateway_url: str,
        token: str,
        tenant_id: str,
        *,
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay_s: float = DEFAULT_BASE_DELAY_S,
        circuit_breaker_threshold: int = 5,
        circuit_breaker_reset_s: float = 30.0,
        default_timeout_s: int | None = None,
        gateway_timeout_s: int | None = None,
    ) -> None:
        """Initialise KnowledgeTool with resilience settings.

        Args:
            mcp_gateway_url: MCP Gateway base URL.
            token: Bearer token for the ``Authorization`` header.
            tenant_id: Tenant UUID for the ``X-Tenant-ID`` header.
            max_retries: Maximum number of attempts per operation (default 3).
            base_delay_s: Base delay for exponential backoff (default 1.0 s).
            circuit_breaker_threshold: Consecutive failures before the circuit
                opens (default 5).
            circuit_breaker_reset_s: Seconds before a half-open probe is allowed
                after the circuit opens (default 30.0).
            default_timeout_s: Default per-call timeout in seconds. Defaults to
                ``NOVAOS_TOOL_TIMEOUT_S`` (or 128s) — see docs/timeout-hierarchy.md.
            gateway_timeout_s: Optional opt-in validation floor — raises
                ``ValueError`` if an effective timeout is shorter than it (the
                SDK should wait at least as long as the Gateway does). ``None``
                (default) disables validation.
        """
        super().__init__(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self._max_retries = max_retries
        self._base_delay_s = base_delay_s
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            reset_timeout_s=circuit_breaker_reset_s,
        )

    async def search(
        self,
        query: str,
        limit: int = 5,
        collection: str | None = None,
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        args: dict[str, Any] = {"query": query, "limit": limit}
        if collection:
            args["collection"] = collection
        return await self._call_tool_with_retry(
            "novaos.knowledge.search",
            args,
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )

    async def ingest(
        self, content: str, metadata: dict | None = None, *, timeout_s: int | None = None
    ) -> ToolResponse:
        """Submit a document for ingestion.

        Returns a :class:`ToolResponse` whose ``data`` contains ``job_id`` —
        the async job identifier you can pass to :meth:`get_job_status` or
        :meth:`poll_until_complete`.

        Defaults to a 120-second timeout because ingestion is an async
        operation whose HTTP acknowledgement may itself take a few seconds;
        pass ``timeout_s`` to override for larger documents. The RAG MCP
        content envelope is unwrapped so callers can access
        ``resp.data["job_id"]`` directly.

        Retries on 5xx / timeout (not 4xx) with exponential backoff.
        """
        resp = await self._call_tool_with_retry(
            "novaos.knowledge.ingest",
            {"content": content, "metadata": metadata or {}},
            timeout_s=timeout_s if timeout_s is not None else _INGEST_TOOL_TIMEOUT,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    async def get_job_status(self, job_id: str, *, timeout_s: int | None = None) -> ToolResponse:
        """Retrieve the current status of an ingestion job.

        Returns a :class:`ToolResponse` whose ``data`` contains at minimum
        ``status`` (one of ``"pending"``, ``"running"``, ``"completed"``,
        ``"failed"``, ``"dead_letter"``) and optionally ``error`` on failure.

        Uses this binding's ``default_timeout_s`` (128s by default) unless
        ``timeout_s`` overrides it — status polls should normally be fast. The
        RAG MCP content envelope is unwrapped so callers can access
        ``resp.data["status"]`` directly.

        Retries on 5xx / timeout with exponential backoff.
        """
        resp = await self._call_tool_with_retry(
            "novaos.novaos-rag.get_job_status",
            {"job_id": job_id},
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    async def poll_until_complete(
        self,
        job_id: str,
        *,
        poll_interval_s: float = 2.0,
        max_wait_s: float = 300.0,
    ) -> ToolResponse:
        """Block (async) until the ingestion job reaches a terminal state.

        Terminal states: ``"completed"``, ``"failed"``, ``"dead_letter"``,
        ``"duplicate"``.

        Returns the final :class:`ToolResponse` from :meth:`get_job_status`
        (``data`` is already unwrapped — access ``resp.data["status"]``
        directly).  Raises :class:`TimeoutError` if ``max_wait_s`` elapses
        without a terminal status.
        """
        deadline = time.perf_counter() + max_wait_s
        while True:
            resp = await self.get_job_status(job_id)
            if not resp.success:
                return resp
            status = (resp.data or {}).get("status", "")
            if status in _RAG_TERMINAL_STATUSES:
                return resp
            if time.perf_counter() >= deadline:
                raise TimeoutError(
                    f"Ingestion job {job_id!r} did not complete within {max_wait_s}s "
                    f"(last status: {status!r})"
                )
            await asyncio.sleep(poll_interval_s)

    # ------------------------------------------------------------------
    # GraphRAG (entity graph search) — build_graph / graph_status /
    # graph_query ("local") / global_query ("global").
    #
    # FQN naming: these use "novaos.novaos-rag.<action>", matching
    # get_job_status above and the platform registry's scope mapping — NOT
    # "novaos.knowledge.<action>". The knowledge-tool-alias
    # shim in mcp-gateway only translates novaos.knowledge.search/ingest to
    # novaos.novaos-rag.*; it has no entry for these four, so the previous
    # "novaos.knowledge.*" literals here always failed with "tool not found."
    # ------------------------------------------------------------------

    async def build_graph(
        self, collection_id: str, *, timeout_s: int | None = None
    ) -> ToolResponse:
        """Kick off (or re-trigger) GraphRAG indexing for a collection.

        Fire-and-forget: the backend flips the collection's graph status to
        ``"pending"`` and returns immediately — unlike :meth:`ingest`, there is
        no ``job_id`` to poll. Use :meth:`get_graph_status` (or
        :meth:`poll_graph_build_until_complete`) on the same ``collection_id``
        to observe the ``pending -> running -> completed|failed`` transition.

        Returns a :class:`ToolResponse` whose ``data`` mirrors the backend's
        immediate ack (``status``, ``entity_count``, ``relationship_count``,
        ``community_count`` — all zero at this point, since building hasn't
        started yet).
        """
        resp = await self._call_tool_with_retry(
            "novaos.novaos-rag.build_graph",
            {"collection_id": collection_id},
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    async def get_graph_status(
        self, collection_id: str, *, timeout_s: int | None = None
    ) -> ToolResponse:
        """Retrieve the current GraphRAG build status for a collection.

        Returns a :class:`ToolResponse` whose ``data`` contains
        ``graph_status`` (one of ``"none"``, ``"pending"``, ``"running"``,
        ``"completed"``, ``"failed"``), ``graph_error``, and aggregate counts
        (``graph_entity_count``, ``graph_relationship_count``,
        ``graph_community_count``).
        """
        resp = await self._call_tool_with_retry(
            "novaos.novaos-rag.graph_status",
            {"collection_id": collection_id},
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    async def poll_graph_build_until_complete(
        self,
        collection_id: str,
        *,
        poll_interval_s: float = 2.0,
        max_wait_s: float = 300.0,
    ) -> ToolResponse:
        """Block (async) until a :meth:`build_graph` run reaches a terminal state.

        Unlike :meth:`poll_until_complete` (which polls by ``job_id``), there is
        no job identifier for a graph build — this re-polls
        :meth:`get_graph_status` on the same ``collection_id`` instead.

        Terminal states: ``"completed"``, ``"failed"``.

        Returns the final :class:`ToolResponse` from :meth:`get_graph_status`.
        Raises :class:`TimeoutError` if ``max_wait_s`` elapses without a
        terminal status.
        """
        deadline = time.perf_counter() + max_wait_s
        while True:
            resp = await self.get_graph_status(collection_id)
            if not resp.success:
                return resp
            status = (resp.data or {}).get("graph_status", "")
            if status in _GRAPH_BUILD_TERMINAL_STATUSES:
                return resp
            if time.perf_counter() >= deadline:
                raise TimeoutError(
                    f"Graph build for collection {collection_id!r} did not complete "
                    f"within {max_wait_s}s (last status: {status!r})"
                )
            await asyncio.sleep(poll_interval_s)

    async def query_graph_local(
        self, question: str, collection_id: str, *, timeout_s: int | None = None
    ) -> ToolResponse:
        """Entity-centric, multi-hop graph search ("local" search mode).

        Requires the graph to already be built — check
        :meth:`get_graph_status` shows ``graph_status == "completed"`` first.
        If the graph isn't ready, the backend returns a degraded response
        (``resp.data["degraded"] is True``) prompting a :meth:`build_graph`
        call rather than an error.

        Returns a :class:`ToolResponse` whose ``data`` contains ``answer``,
        ``citations``, ``confidence``, ``degraded``, and
        ``metadata.search_mode == "graph_local"``.
        """
        resp = await self._call_tool_with_retry(
            "novaos.novaos-rag.graph_query",
            {"question": question, "collection_id": collection_id},
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    async def query_graph_global(
        self, question: str, collection_id: str, *, timeout_s: int | None = None
    ) -> ToolResponse:
        """Theme/community-report synthesis across the graph ("global" search mode).

        Same preconditions and response shape as :meth:`query_graph_local`,
        except ``metadata.search_mode == "graph_global"``. Use this for
        broad, corpus-wide questions ("what are the main themes...") rather
        than entity-specific lookups.
        """
        resp = await self._call_tool_with_retry(
            "novaos.novaos-rag.global_query",
            {"question": question, "collection_id": collection_id},
            timeout_s=timeout_s,
            max_retries=self._max_retries,
            base_delay_s=self._base_delay_s,
            circuit_breaker=self._circuit_breaker,
        )
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        return resp

    #: Maximum concurrent submissions in :meth:`ingest_batch`.
    BATCH_CONCURRENCY_LIMIT: int = 10

    async def ingest_batch(
        self,
        items: list[IngestItem],
        *,
        collection: str | None = None,
    ) -> BatchIngestResponse:
        """Submit multiple documents for ingestion with bounded concurrency.

        Items are submitted with up to :attr:`BATCH_CONCURRENCY_LIMIT` (10)
        concurrent requests to avoid overwhelming the MCP Gateway.  Each
        receives its own backend ``job_id``.  Items that fail to submit
        (network error, content too large, etc.) are recorded with
        ``status="submit_error"`` — the other items continue unaffected.

        The returned :class:`BatchIngestResponse` mirrors the input order.
        Pass it to :meth:`poll_batch_until_complete` to await all jobs, or
        iterate ``resp.items`` and call :meth:`poll_until_complete` per
        ``job_id`` for finer-grained control.

        Args:
            items: Documents to ingest.
            collection: Optional collection name / ID passed into each
                item's metadata (overridden by any ``collection`` key already
                present in ``item.metadata``).

        Returns:
            :class:`BatchIngestResponse` with per-item :class:`IngestJobStatus`
            entries in input order.
        """
        batch_id = str(uuid.uuid4())
        semaphore = asyncio.Semaphore(self.BATCH_CONCURRENCY_LIMIT)

        async def _submit_one(idx: int, item: IngestItem) -> IngestJobStatus:
            async with semaphore:
                meta: dict[str, Any] = dict(item.metadata or {})
                if collection and "collection" not in meta:
                    meta["collection"] = collection
                resp = await self.ingest(item.content, meta)
                if resp.success:
                    data = resp.data or {}
                    job_id: str | None = data.get("job_id")
                    if job_id is None:
                        return IngestJobStatus(
                            index=idx,
                            job_id=None,
                            status="submit_error",
                            error="backend returned success with no job_id",
                        )
                    return IngestJobStatus(
                        index=idx,
                        job_id=job_id,
                        status=data.get("status", "pending"),
                    )
                return IngestJobStatus(
                    index=idx,
                    job_id=None,
                    status="submit_error",
                    error=resp.error,
                )

        results = await asyncio.gather(*(_submit_one(i, item) for i, item in enumerate(items)))
        return BatchIngestResponse(batch_id=batch_id, total=len(items), items=list(results))

    async def get_ingest_status(self, job_id: str) -> IngestJobStatus:
        """Return the current status of a single ingestion job as a typed object.

        Typed convenience wrapper around :meth:`get_job_status`.  Prefer
        :meth:`get_job_status` when you need the raw :class:`ToolResponse`
        envelope; use this method when you want a concrete
        :class:`IngestJobStatus` for programmatic status checks.

        Args:
            job_id: The job UUID returned by :meth:`ingest` or found in a
                :class:`BatchIngestResponse`.

        Returns:
            :class:`IngestJobStatus` reflecting the latest backend state.
            ``index`` is set to ``0`` — it carries no meaning for single-job
            queries; use :class:`BatchIngestResponse` for index-aware tracking.
        """
        resp = await self.get_job_status(job_id)
        if not resp.success:
            return IngestJobStatus(index=0, job_id=job_id, status="unknown", error=resp.error)
        data = resp.data or {}
        return IngestJobStatus(
            index=0,
            job_id=job_id,
            status=data.get("status", "unknown"),
            error=data.get("error"),
        )

    async def poll_batch_until_complete(
        self,
        batch: BatchIngestResponse,
        *,
        poll_interval_s: float = 2.0,
        max_wait_s: float = 600.0,
    ) -> BatchIngestResponse:
        """Poll all jobs in a batch until every job reaches a terminal state.

        Polls successfully-submitted jobs (``job_id`` is not ``None``) in
        parallel on each tick.  Items with ``status="submit_error"`` are
        already terminal and are preserved unchanged.

        Returns an updated :class:`BatchIngestResponse` with final per-item
        statuses in input order.  Raises :class:`TimeoutError` if
        ``max_wait_s`` elapses before all jobs complete.

        Args:
            batch: Response from a prior :meth:`ingest_batch` call.
            poll_interval_s: Seconds to wait between polling rounds.
            max_wait_s: Maximum total wait before :class:`TimeoutError` is
                raised.  Default 600 s (10 minutes) — increase for very large
                documents.
        """
        # Build a mutable index keyed by job_id for O(1) updates.
        by_job_id: dict[str, IngestJobStatus] = {}
        for item in batch.items:
            if item.job_id:
                by_job_id[item.job_id] = item

        # Defensive: items with no job_id that aren't already terminal can never be
        # polled. Upgrade them to "submit_error" so the loop can reach completion.
        untracked_items: list[IngestJobStatus] = []
        for item in batch.items:
            if not item.job_id and item.status not in _BATCH_TERMINAL_STATUSES:
                untracked_items.append(
                    IngestJobStatus(
                        index=item.index,
                        job_id=None,
                        status="submit_error",
                        error=item.error or "no job_id — item cannot be polled",
                    )
                )
            else:
                untracked_items.append(item)

        # Track consecutive poll failures per job — after 3 consecutive "unknown"
        # results we mark the job as terminal to avoid hanging until timeout.
        _MAX_CONSECUTIVE_UNKNOWN = 3
        unknown_counts: dict[str, int] = {}

        deadline = time.perf_counter() + max_wait_s
        while True:
            pending = [s for s in by_job_id.values() if s.status not in _BATCH_TERMINAL_STATUSES]
            if not pending:
                break
            if time.perf_counter() >= deadline:
                pending_ids = [s.job_id for s in pending]
                raise TimeoutError(
                    f"Batch {batch.batch_id!r}: {len(pending)} job(s) did not complete within "
                    f"{max_wait_s}s — still pending: {pending_ids!r}"
                )
            updates = await asyncio.gather(
                *(self.get_ingest_status(s.job_id) for s in pending if s.job_id),
                return_exceptions=True,
            )
            for upd in updates:
                if isinstance(upd, asyncio.CancelledError):
                    raise upd
                if isinstance(upd, BaseException):
                    _logger.warning(
                        "poll_batch: ignoring exception from get_ingest_status: %s",
                        upd,
                    )
                    continue
                if isinstance(upd, IngestJobStatus) and upd.job_id:
                    old = by_job_id[upd.job_id]
                    if upd.status == "unknown":
                        unknown_counts[upd.job_id] = unknown_counts.get(upd.job_id, 0) + 1
                        if unknown_counts[upd.job_id] >= _MAX_CONSECUTIVE_UNKNOWN:
                            by_job_id[upd.job_id] = IngestJobStatus(
                                index=old.index,
                                job_id=upd.job_id,
                                status="failed",
                                error=upd.error or "poll failed after 3 consecutive errors",
                            )
                            continue
                    else:
                        unknown_counts.pop(upd.job_id, None)
                    by_job_id[upd.job_id] = IngestJobStatus(
                        index=old.index,
                        job_id=upd.job_id,
                        status=upd.status,
                        error=upd.error,
                    )
            await asyncio.sleep(poll_interval_s)

        updated_items = [
            (
                by_job_id[item.job_id]
                if item.job_id and item.job_id in by_job_id
                else next((u for u in untracked_items if u.index == item.index), item)
            )
            for item in batch.items
        ]
        return BatchIngestResponse(batch_id=batch.batch_id, total=batch.total, items=updated_items)


class AgentsTool(_BaseToolBinding):
    """Agent-to-agent communication (governed, token-chained)."""

    async def call(self, agent_id: str, input: str, timeout_ms: int = 300000) -> ToolResponse:
        return await self._call_tool(
            "novaos.agents.call",
            {"agent_id": agent_id, "input": input, "timeout_ms": timeout_ms},
            timeout_s=_timeout_from_ms(timeout_ms),
        )

    async def discover(self, query: str | None = None) -> ToolResponse:
        args: dict[str, Any] = {}
        if query:
            args["query"] = query
        return await self._call_tool("novaos.agents.discover", args)


_HITL_REQUEST_INPUT_UNAVAILABLE = (
    "novaos.hitl.request_input is unavailable: no hitl-service backend exists. "
    "Do not use ctx.tools.hitl.request_input for human input gates."
)
_hitl_request_input_warned = False


def _warn_hitl_request_input_unavailable() -> None:
    """Emit a once-only DeprecationWarning for the unimplemented request_input tool."""
    global _hitl_request_input_warned
    if _hitl_request_input_warned:
        return
    _hitl_request_input_warned = True
    warnings.warn(_HITL_REQUEST_INPUT_UNAVAILABLE, DeprecationWarning, stacklevel=3)


class HITLTool(_BaseToolBinding):
    """Human-in-the-loop approval requests."""

    async def request_approval(
        self, prompt: str, options: list[str] | None = None, timeout_ms: int = 300000
    ) -> ToolResponse:
        args: dict[str, Any] = {"prompt": prompt, "timeout_ms": timeout_ms}
        if options:
            args["options"] = options
        return await self._call_tool(
            "novaos.hitl.request_approval",
            args,
            timeout_s=_timeout_from_ms(timeout_ms),
        )

    async def request_input(self, prompt: str, input_type: str = "text") -> ToolResponse:
        """Unavailable — no hitl-service backend exists.

        Fails closed without calling the gateway, matching the platform's
        agent-sdk behavior for the same tool.
        """
        del prompt, input_type
        _warn_hitl_request_input_unavailable()
        return ToolResponse(success=False, error=_HITL_REQUEST_INPUT_UNAVAILABLE)

    def requires_write_hitl(self, tool_fqn: str) -> bool:
        """Local, synchronous classification of whether a write-class MCP
        tool call is likely to hit Gate A's ``enforceMcpHitl`` (-32042).

        No network call — this calls the exact same ``mcpToolActionClass``
        classifier Gate A itself uses (mirrored from the
        platform's own implementation), so it cannot drift from the
        platform's actual enforcement decision the way
        a separately-maintained mint-time hint could. Not a guarantee (OPA
        policy can still override per tenant), but lets a skill decide up
        front whether to build a pause before calling ``tool_fqn``, instead
        of guessing or hand-rolling a fail-then-recover loop. Mirrors
        ``HITLTool.requiresWriteHitl`` in the TypeScript agent SDK.
        """
        return mcp_tool_action_class(tool_fqn) == MCP_TOOLS_CALL_WRITE


class FilesTool(_BaseToolBinding):
    """Scoped file storage."""

    async def upload(
        self,
        filename: str,
        content: str,
        content_type: str = "text/plain",
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        return await self._call_tool(
            "novaos.files.upload",
            {"filename": filename, "content": content, "content_type": content_type},
            timeout_s=timeout_s,
        )

    async def download(self, file_id: str, *, timeout_s: int | None = None) -> ToolResponse:
        return await self._call_tool(
            "novaos.files.download", {"file_id": file_id}, timeout_s=timeout_s
        )

    async def list(
        self, prefix: str | None = None, *, timeout_s: int | None = None
    ) -> ToolResponse:
        args: dict[str, Any] = {}
        if prefix:
            args["prefix"] = prefix
        return await self._call_tool("novaos.files.list", args, timeout_s=timeout_s)


class MemoryTool(_BaseToolBinding):
    """Durable memory storage and retrieval (user/agent/tenant/run/task scope)."""

    async def write(
        self,
        scope: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        args: dict[str, Any] = {"scope": scope, "content": content}
        if metadata:
            args["metadata"] = metadata
        return await self._call_tool("novaos.memory.write", args, timeout_s=timeout_s)

    async def query(
        self,
        scope: str,
        query: str,
        top_k: int = 10,
        filters: dict[str, Any] | None = None,
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        # Memory Service rejects unknown top-level arguments; the result limit
        # is only accepted inside `budget`.
        args: dict[str, Any] = {"scope": scope, "query": query, "budget": {"top_k": top_k}}
        if filters:
            args["filters"] = filters
        return await self._call_tool("novaos.memory.query", args, timeout_s=timeout_s)


class NotifyTool(_BaseToolBinding):
    """Multi-channel notifications."""

    async def send(
        self,
        channel: str,
        recipient: str,
        message: str,
        template: str | None = None,
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        args: dict[str, Any] = {"channel": channel, "recipient": recipient, "message": message}
        if template:
            args["template"] = template
        return await self._call_tool("novaos.notify.send", args, timeout_s=timeout_s)


class HTTPTool(_BaseToolBinding):
    """Governed outbound HTTP client (domain-allowlisted)."""

    async def request(
        self,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        body: str | None = None,
        *,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        args: dict[str, Any] = {"url": url, "method": method}
        if headers:
            args["headers"] = headers
        if body:
            args["body"] = body
        return await self._call_tool("novaos.http.request", args, timeout_s=timeout_s)


class BrowserTool(_BaseToolBinding):
    """Browser automation."""

    async def navigate(self, url: str, *, timeout_s: int | None = None) -> ToolResponse:
        return await self._call_tool("novaos.browser.navigate", {"url": url}, timeout_s=timeout_s)

    async def screenshot(self, *, timeout_s: int | None = None) -> ToolResponse:
        return await self._call_tool("novaos.browser.screenshot", {}, timeout_s=timeout_s)

    async def extract(self, selector: str, *, timeout_s: int | None = None) -> ToolResponse:
        return await self._call_tool(
            "novaos.browser.extract", {"selector": selector}, timeout_s=timeout_s
        )


class CodeTool(_BaseToolBinding):
    """Sandboxed code execution."""

    async def execute(
        self, code: str, language: str = "python", timeout_ms: int = 30000
    ) -> ToolResponse:
        return await self._call_tool(
            "novaos.code.execute",
            {"code": code, "language": language, "timeout_ms": timeout_ms},
            timeout_s=_timeout_from_ms(timeout_ms),
        )


class ConnectorTool(_BaseToolBinding):
    """Generic connector-host invocation — the governed request/response path
    to third-party integrations (ClickUp, Slack, Gmail, ...) that don't have a
    bespoke tool binding of their own.

    Routed through the MCP Gateway like every other tool binding here (single
    governed egress), the same as ``memory``/``knowledge``/etc. — NOT a direct
    HTTP client to connector-host itself. connector-host scopes access via JWT
    scopes (``connector:invoke`` plus a per-connector/operation scope) and is
    disabled by default per environment; going through the Gateway means the
    agent never needs to hold or mint that token itself.

    Uses the plain (non-retrying) :meth:`_BaseToolBinding._call_tool`, matching
    every tool binding here except ``knowledge`` — connector operations are
    frequently non-idempotent (e.g. "create-task", "send-message"), so this
    class does not default to automatic retries the way ``KnowledgeTool``'s
    read/idempotent-ish operations do. Callers who know a specific operation is
    safe to retry should implement that themselves.

    The MCP tool name and argument shape below are **not** a free choice — they
    must match how ``connector-host``'s catalog registers tools (confirmed by
    reading the catalog builder and Gateway protocol code directly): the
    registered tool name is
    ``novaos.connector.{connector_id}.{operation}`` for the standard tier, and
    one segment longer for the other two tiers connector-host's catalog
    recognizes — ``novaos.connector.licensed.{connector_id}.{operation}`` and
    ``novaos.connector.ext.{connector_id}.{operation}`` — selected via the
    ``tier`` argument (see :meth:`call`; cross-reference the platform's own
    catalog builder, which must stay in lockstep with
    :func:`_connector_fqn` below). The MCP Gateway does the actual tier
    routing and scope enforcement server-side — this class only builds the
    right tool name; a caller lacking the licensed/ext scope still correctly
    gets a scope error back from the Gateway. This is independent of
    connector-host's separate ``data_tier``/``max_tier`` entitlement gate
    (``raw``/``information``/``knowledge``), which is enforced server-side
    regardless of which MCP tool name was called.
    """

    #: Client-side cap on the JSON-encoded size of ``params``, applied before
    #: any network call. Generous for real connector payloads (task bodies,
    #: message text) — this exists to fail fast on a caller mistake (e.g. an
    #: accidentally-embedded file or a runaway loop) rather than to constrain
    #: legitimate use.
    MAX_PARAMS_BYTES: int = 262_144  # 256 KiB

    #: The three tool-name tiers connector-host's catalog registers. Catalog-backed
    #: resolution of "which tier is this connector actually on" (a `tier="auto"`
    #: that lists MCP tools and picks the match) is a possible future follow-up,
    #: not implemented here — this SDK has no catalog access, only the naming
    #: convention, so the caller must know and pass the tier.
    _TIERS: tuple[str, ...] = ("standard", "licensed", "ext")

    async def call(
        self,
        connector_id: str,
        operation: str,
        params: dict[str, Any] | None = None,
        *,
        tier: Literal["standard", "licensed", "ext"] = "standard",
        params_schema: dict[str, Any] | None = None,
        timeout_s: int | None = None,
    ) -> ToolResponse:
        """Invoke ``operation`` on ``connector_id`` with ``params``.

        Calls the MCP tool ``novaos.connector.{connector_id}.{operation}`` (or,
        for ``tier="licensed"``/``tier="ext"``, the same name with that tier
        inserted as an extra segment — see :func:`_connector_fqn`) with
        ``params`` as its arguments — the MCP Gateway forwards them verbatim as
        the body of connector-host's own
        ``POST /api/v1/connectors/{connector_id}/{operation}``. On success,
        ``resp.data`` mirrors connector-host's response envelope:
        ``{"data": ..., "provenance": {...}, "meta": {...}}``.

        ``tier`` defaults to ``"standard"``, so every existing caller is
        unaffected. Pass ``tier="licensed"`` or ``tier="ext"`` for a connector
        registered on one of those tiers; an invalid ``tier`` value fails
        client-side with no network call. The Gateway — not this SDK —
        enforces whether the caller's token actually carries the scope for a
        licensed/ext connector.

        A connector-host environment that's disabled (``CONNECTOR_HOST_ENABLED=false``)
        surfaces as a failure whose ``resp.error`` names ``SERVICE_DISABLED`` —
        check for that specifically if you need to distinguish "not enabled in
        this environment" from a genuine connector failure. A standard-tier
        call that fails tool-not-found gets its error message augmented with a
        hint to try ``tier="licensed"``/``tier="ext"``, since that is the most
        common reason a connector-host tool name isn't found.

        Before any network call, ``params`` is checked for JSON-serializability
        and capped at :attr:`MAX_PARAMS_BYTES` — this SDK has no per-connector
        schema to validate structure against, so it can only guard against
        gross caller mistakes, not malformed-but-small payloads. If you *do*
        know the expected shape for this ``connector_id``/``operation`` pair,
        pass it as ``params_schema`` (a JSON Schema) to validate ``params``
        against it client-side, via the same :func:`validate_against_schema`
        used for Ex10 structured extraction — this requires the optional
        ``[schema]`` extra only if you actually pass ``params_schema``.

        .. admonition:: ``params`` is recorded unmasked in the eval transcript, if one is active

           This method calls :meth:`_BaseToolBinding._call_tool`, which
           automatically records every call — ``params`` and the response
           data — into the active ``EvaluationContext`` via ``_record_eval``,
           with no masking and no way to opt out short of not having an
           active evaluation context. Connector params routinely carry
           customer data (a ClickUp task body, an email address). This is
           unrelated to ``AgentBuilder(mask_output_pii=True)``, which only
           masks the outbound A2A task, not the eval transcript. See
           :class:`novaos_client_sdk.evaluation.models.ToolCall` for the full
           default posture.
        """
        if not isinstance(connector_id, str) or not connector_id.strip():
            return ToolResponse(success=False, error="connector_id must be a non-empty string")
        if not isinstance(operation, str) or not operation.strip():
            return ToolResponse(success=False, error="operation must be a non-empty string")
        if tier not in self._TIERS:
            return ToolResponse(
                success=False,
                error=f"tier must be one of {self._TIERS!r}, got {tier!r}",
            )

        params = params or {}
        try:
            encoded_size = len(json.dumps(params))
        except (TypeError, ValueError) as exc:
            return ToolResponse(success=False, error=f"params is not JSON-serializable: {exc}")
        if encoded_size > self.MAX_PARAMS_BYTES:
            return ToolResponse(
                success=False,
                error=(
                    f"params payload is {encoded_size} bytes, exceeding the "
                    f"{self.MAX_PARAMS_BYTES}-byte client-side cap"
                ),
            )

        if params_schema is not None:
            from novaos_client_sdk.schema_validation import validate_against_schema

            schema_errors = validate_against_schema(params, params_schema)
            if schema_errors:
                return ToolResponse(success=False, error="; ".join(schema_errors))

        fqn = _connector_fqn(connector_id, operation, tier)
        resp = await self._call_tool(fqn, params, timeout_s=timeout_s)
        if resp.success:
            return ToolResponse(success=True, data=_unwrap_mcp_content(resp.data))
        if (
            tier == "standard"
            and resp.error
            # Match the MCP Gateway's own "Tool '<fqn>' not found" specifically
            # (it echoes the exact tool name we sent) rather than a bare "not
            # found" substring, which also matches a connector resource miss
            # (e.g. "task not found"), a JSON-RPC "Method not found", or a
            # transport-level DNS failure — none of which are a tier problem.
            and fqn in resp.error
            and "not found" in resp.error.lower()
        ):
            return ToolResponse(
                success=False,
                error=(
                    f"{resp.error} (no standard-tier tool for {connector_id}.{operation}; "
                    'if this is a licensed/ext connector, pass tier="licensed" (or "ext"))'
                ),
            )
        return resp


class ToolBindings:
    """Container for all platform tool bindings. Access via ctx.tools.knowledge, ctx.tools.agents, etc."""

    def __init__(
        self,
        mcp_gateway_url: str,
        token: str,
        tenant_id: str,
        *,
        default_timeout_s: int | None = None,
        gateway_timeout_s: int | None = None,
    ):
        """
        Args:
            mcp_gateway_url: MCP Gateway base URL.
            token: Bearer token for the ``Authorization`` header.
            tenant_id: Tenant UUID for the ``X-Tenant-ID`` header.
            default_timeout_s: Default per-tool-call timeout in seconds,
                applied to every binding unless a call overrides it with its
                own ``timeout_s``. Defaults to ``NOVAOS_TOOL_TIMEOUT_S`` (or
                128s) — see docs/timeout-hierarchy.md.
            gateway_timeout_s: Optional opt-in validation floor applied to
                every binding — raises ``ValueError`` if an effective timeout
                is shorter than it (the SDK should wait at least as long as
                the Gateway does). ``None`` (default) disables validation,
                since there is no single platform-wide Gateway timeout to assume.
        """
        self.knowledge = KnowledgeTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.agents = AgentsTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.hitl = HITLTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.files = FilesTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.memory = MemoryTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.notify = NotifyTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.http = HTTPTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.browser = BrowserTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.code = CodeTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )
        self.connectors = ConnectorTool(
            mcp_gateway_url,
            token,
            tenant_id,
            default_timeout_s=default_timeout_s,
            gateway_timeout_s=gateway_timeout_s,
        )

    def set_entity_refs(self, refs: list[str]) -> None:
        for tool in (
            self.knowledge,
            self.agents,
            self.hitl,
            self.files,
            self.memory,
            self.notify,
            self.http,
            self.browser,
            self.code,
            self.connectors,
        ):
            tool.set_entity_refs(refs)

    def set_event_sink(self, sink: Callable[[str, dict[str, Any]], None] | None) -> None:
        """Wire every binding's tool calls into a live event stream.

        Called by :class:`~novaos_client_sdk.agent_context.AgentExecutionContext`
        for a ``SendStreamingMessage`` request, so ``ctx.tools.knowledge.search(...)``
        and its siblings emit ``tool.call`` / ``tool.result`` as the skill runs.
        ``None`` (the default every non-streaming request keeps) turns emission
        back off — see :meth:`_BaseToolBinding._emit_event`.
        """
        for tool in (
            self.knowledge,
            self.agents,
            self.hitl,
            self.files,
            self.memory,
            self.notify,
            self.http,
            self.browser,
            self.code,
            self.connectors,
        ):
            tool.set_event_sink(sink)

    def set_traceparent(self, traceparent: str) -> None:
        """Set a per-request W3C traceparent on all tool bindings (P4-fleet F16).

        In fleet mode the same pod handles sequential runs, so ``TRACEPARENT`` env
        var is stale after the first run.  Call this once per request with the
        inbound ``traceparent`` header value so all outbound MCP calls carry the
        correct trace context for the current run.
        """
        for tool in (
            self.knowledge,
            self.agents,
            self.hitl,
            self.files,
            self.memory,
            self.notify,
            self.http,
            self.browser,
            self.code,
            self.connectors,
        ):
            tool.set_traceparent(traceparent)

    def set_run_id(self, run_id: str) -> None:
        """Set the current run's id on all tool bindings.

        Called once per request by ``AgentExecutionContext`` so every tool-call
        span this binding creates carries ``novaos.run_id`` as an attribute,
        matching the run in the platform's own run records.
        """
        for tool in (
            self.knowledge,
            self.agents,
            self.hitl,
            self.files,
            self.memory,
            self.notify,
            self.http,
            self.browser,
            self.code,
            self.connectors,
        ):
            tool.set_run_id(run_id)
