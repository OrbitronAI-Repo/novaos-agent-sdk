"""Client for the NovaOS Gateway's run-observability endpoints.

Wraps two confirmed Gateway REST endpoints for inspecting and controlling a
live agent run:

* **EventIndex** — ``GET /api/v1/service/runs/:runId/events`` (service-account
  callers) or ``GET /api/v1/runs/:runId/events`` (user callers): history of a
  run's events.
* **DebugSession** — ``POST /api/v1/runs/:runId/debug/:action``: pause / resume /
  step / abort control of a live run, part of the platform's "Agent
  Debugger Platform".

.. admonition:: Direct Gateway REST client, not an MCP-routed tool

   Like :class:`~novaos_client_sdk.memory.MemoryClient`, this talks directly
   to platform HTTP endpoints rather than going through the MCP Gateway's
   ``/mcp/v1/tools/call`` JSON-RPC envelope used by
   :class:`~novaos_client_sdk.tools.ToolBindings` — because these ARE Gateway
   endpoints, not tools registered in the MCP tool registry.

.. admonition:: Auth requirements differ per endpoint — read before use

   The platform serves run events on two routes. The service route
   (``/api/v1/service/runs/:runId/events``) is guarded by service-account
   bearer auth plus a revocation check (``saAuthPreHandler`` +
   ``assertSaNotRevoked``) and answers 403 to a user token. The user route
   (``/api/v1/runs/:runId/events``) serves a user's own runs.
   ``get_run_events`` picks between them via its ``route`` argument; the
   default ``"auto"`` tries the service route and falls back to the user
   route on a 403. ``send_debug_control`` uses a more general bearer-auth
   guard.

Endpoint contract: the events route and the debug-control
route are both shipped with full test coverage as part of the platform's
"Agent Debugger Platform". The ``EventIndex``/``DebugSession`` models
backing these live in the platform's shared schema. ``EventIndex`` stores
``trace_id``/``span_id``, but older platform builds do not return them on
either events route, so :attr:`RunEvent.trace_id` / ``span_id`` may be
``None``. ``sequence`` is returned by the user route on every build, and by
the service route only on newer builds.

⚠️ Both routes are registered on the Gateway *without* an ``/api/v1``
prefix in the literal route-definition call — the prefix is applied
separately, where the route module is mounted
(``server.register(debugControlRoutes, {prefix: "/api/v1", ...})`` /
``server.register(serviceRunsRoutes, {prefix: "/api/v1", ...})``). Reading
only the route definition, as an earlier pass at this client did for the
debug-control path (caught in code review), silently produces the wrong
path. Verify against the ``server.register(...)`` call site, not just the
route definition, before trusting any route string pulled from this
codebase.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Literal

from novaos_client_sdk._http import httpx
from novaos_client_sdk._http_error import status_error_details, status_error_message
from novaos_client_sdk._resilience import CircuitBreaker as _CircuitBreaker
from novaos_client_sdk._resilience import backoff_delay
from novaos_client_sdk.exceptions import (
    NovaOSConnectionError,
    NovaOSError,
    NovaOSServerError,
)
from novaos_client_sdk.tracing import inject_trace_headers, traced_tool_call

logger = logging.getLogger(__name__)

NOVAOS_GATEWAY_URL_ENV = "NOVAOS_GATEWAY_URL"

DebugAction = Literal["pause", "resume", "step", "abort"]
EventsRoute = Literal["auto", "user", "service"]

_SERVICE_EVENTS_PATH = "/api/v1/service/runs/{run_id}/events"
_USER_EVENTS_PATH = "/api/v1/runs/{run_id}/events"


# ---------------------------------------------------------------------------
# Response dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RunEvent:
    """A single event from a run's event history.

    Attributes:
        event_id: Unique identifier for this event. Empty on the user route,
            which does not return one.
        type: Event type (e.g. tool call, status transition).
        data: Event payload — shape varies by ``type``.
        timestamp: ISO-8601 timestamp of the event.
        sequence: Per-run ordering number, or ``None`` when the route does
            not return it (the service route).
        trace_id: Trace ID the platform stored for this event, if any.
        span_id: Span ID the platform stored for this event, if any.
    """

    event_id: str = ""
    type: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""
    sequence: int | None = None
    trace_id: str | None = None
    span_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunEvent:
        # The service route calls the payload ``data``; the user route calls it ``payload``.
        payload = data.get("data")
        if payload is None:
            payload = data.get("payload")
        return cls(
            event_id=data.get("event_id", ""),
            type=data.get("type", ""),
            data=payload if payload is not None else {},
            timestamp=data.get("timestamp", ""),
            sequence=data.get("sequence"),
            trace_id=data.get("trace_id"),
            span_id=data.get("span_id"),
        )


@dataclass
class RunEventsPage:
    """One page of :meth:`ObservabilityClient.get_run_events` results.

    Attributes:
        events: Events in this page, in server-returned order.
        next_cursor: Opaque cursor for the next page, or ``None`` if this is
            the last page. Pass as ``after=`` to :meth:`get_run_events` to
            continue.
        request_id: Request ID for tracing/support correlation.
    """

    events: list[RunEvent] = field(default_factory=list)
    next_cursor: str | None = None
    request_id: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunEventsPage:
        meta = data.get("meta", {}) or {}
        cursor = meta.get("cursor", {}) or {}
        return cls(
            events=[RunEvent.from_dict(e) for e in data.get("data", [])],
            next_cursor=cursor.get("next"),
            request_id=meta.get("request_id", ""),
        )

    @classmethod
    def from_user_route_dict(cls, data: dict[str, Any]) -> RunEventsPage:
        """Parse the user route's ``{"events": [...]}`` body.

        That route has no cursor envelope, so ``next_cursor`` is the last
        returned event's ``sequence`` (the same cursor format the service route
        uses). It is ``None`` only once a page comes back empty, so a
        ``while page.next_cursor`` loop ends with one empty request.
        """
        events = [RunEvent.from_dict(e) for e in data.get("events", [])]
        last_sequence = events[-1].sequence if events else None
        return cls(
            events=events,
            next_cursor=str(last_sequence) if last_sequence is not None else None,
        )


@dataclass
class DebugControlResult:
    """Result of a :meth:`ObservabilityClient.send_debug_control` call.

    Attributes:
        session_id: The DebugSession's ID.
        run_id: The run this session controls.
        breakpoint_state: State after the action was applied.
        action: The action that was applied (echoed back).
    """

    session_id: str = ""
    run_id: str = ""
    breakpoint_state: str = ""
    action: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DebugControlResult:
        inner = data.get("data", data)
        return cls(
            session_id=inner.get("session_id", ""),
            run_id=inner.get("run_id", ""),
            breakpoint_state=inner.get("breakpoint_state", ""),
            action=inner.get("action", ""),
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class ObservabilityClient:
    """Async client for the NovaOS Gateway's run-observability endpoints.

    Args:
        base_url: Gateway base URL. If ``None``, reads from
            ``NOVAOS_GATEWAY_URL`` env var.
        headers: Additional HTTP headers — typically
            ``{"Authorization": "Bearer <token>"}``. A user token or a
            service-account token both work with ``get_run_events``; see the
            module docstring.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum retry attempts for :meth:`get_run_events`
            (default: 3). :meth:`send_debug_control` is never auto-retried —
            see its docstring.
        circuit_breaker_threshold: Failures before circuit opens (default: 5).
        circuit_breaker_reset_s: Seconds before half-open probe (default: 30).
        client: Optional pre-configured ``httpx.AsyncClient``. If provided,
            this class will NOT close it on ``__aexit__`` — the caller
            retains ownership of its lifecycle.

    Example:
        ```python
        async with ObservabilityClient(
            base_url="https://gateway.internal",
            headers={"Authorization": f"Bearer {service_account_token}"},
        ) as obs:
            page = await obs.get_run_events(run_id)
            for event in page.events:
                print(event.type, event.timestamp)
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
            base_url = os.getenv(NOVAOS_GATEWAY_URL_ENV)
            if not base_url:
                raise NovaOSError(
                    f"Gateway URL not provided and '{NOVAOS_GATEWAY_URL_ENV}' "
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
        self._events_route: Literal["user", "service"] | None = None

    async def __aenter__(self) -> ObservabilityClient:
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
                "ObservabilityClient is not initialized. "
                "Use async context manager or call __aenter__() first."
            )
        return self._client

    # ------------------------------------------------------------------
    # EventIndex — GET /api/v1/service/runs/:runId/events (service account)
    #              GET /api/v1/runs/:runId/events          (user)
    # ------------------------------------------------------------------

    async def get_run_events(
        self,
        run_id: str,
        *,
        after: str | None = None,
        limit: int | None = None,
        route: EventsRoute = "auto",
    ) -> RunEventsPage:
        """Fetch one page of a run's event history.

        Retries on transient failures (429/502/503/504, network errors) with
        jittered backoff — this is a read-only GET, safe to retry.

        Args:
            run_id: The run to fetch events for.
            after: Opaque cursor from a prior :class:`RunEventsPage`'s
                ``next_cursor``, to continue pagination.
            limit: Maximum events to return in this page (server-defined
                default/max apply if omitted). Only the service route honours
                it; the user route uses a fixed server-side page size.
            route: ``"service"`` for the service-account route, ``"user"`` for
                the user route (a user token reading its own run), or
                ``"auto"`` (default) to try the service route and fall back
                to the user route on a 403. The route that worked is
                remembered for later ``"auto"`` calls on this client.

        Returns:
            One page of events plus a cursor for the next page.
        """
        if route == "auto":
            if self._events_route is not None:
                route = self._events_route
            else:
                try:
                    page = await self._get_events_page(run_id, after, limit, "service")
                except NovaOSServerError as e:
                    if e.status_code != 403:
                        raise
                    page = await self._get_events_page(run_id, after, limit, "user")
                    self._events_route = "user"
                else:
                    self._events_route = "service"
                return page
        return await self._get_events_page(run_id, after, limit, route)

    async def _get_events_page(
        self,
        run_id: str,
        after: str | None,
        limit: int | None,
        route: Literal["user", "service"],
    ) -> RunEventsPage:
        params: dict[str, str] = {}
        if after:
            params["after"] = after
        if route == "service":
            if limit is not None:
                params["limit"] = str(limit)
            data = await self._get_with_retry(_SERVICE_EVENTS_PATH.format(run_id=run_id), params)
            return RunEventsPage.from_dict(data)
        data = await self._get_with_retry(_USER_EVENTS_PATH.format(run_id=run_id), params)
        return RunEventsPage.from_user_route_dict(data)

    async def _get_with_retry(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        """GET with retry + circuit breaker (mirrors MemoryClient._send_mcp)."""
        if not self._cb.allow_request():
            raise NovaOSConnectionError(
                "Gateway observability circuit breaker is OPEN — failing fast. "
                f"Will probe again in {self._cb.reset_timeout}s."
            )

        last_error: NovaOSError | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                result = await self._get_once(path, params)
                self._cb.record_success()
                return result
            except NovaOSError as e:
                last_error = e
                if not e.is_retryable or attempt == self._max_retries:
                    self._cb.record_failure()
                    raise

                delay = e.retry_after if e.retry_after else backoff_delay(attempt)
                logger.warning(
                    "Gateway observability request failed (attempt %d/%d, retrying in %.1fs): %s",
                    attempt,
                    self._max_retries,
                    delay,
                    e.message,
                )
                await asyncio.sleep(delay)

        self._cb.record_failure()
        raise last_error  # type: ignore[misc]

    async def _get_once(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        client = self._ensure_client()
        with traced_tool_call("observability.get_run_events", params):
            request_headers = {**self._headers, **inject_trace_headers({})}
            try:
                response = await client.get(path, params=params, headers=request_headers)
                response.raise_for_status()
            except httpx.ConnectError as e:
                raise NovaOSConnectionError(
                    f"Failed to connect to Gateway: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.TimeoutException as e:
                raise NovaOSConnectionError(
                    f"Gateway events request timed out: {e}",
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
                    status_error_message("Gateway", e),
                    status_code=e.response.status_code,
                    retry_after=retry_after,
                    details=status_error_details(e),
                ) from e

            result: dict[str, Any] = response.json()
            return result

    # ------------------------------------------------------------------
    # DebugSession — POST /api/v1/runs/:runId/debug/:action
    # ------------------------------------------------------------------

    async def send_debug_control(
        self,
        run_id: str,
        action: DebugAction,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> DebugControlResult:
        """Send a debug-control action to a live run's DebugSession.

        Not retried automatically, unlike :meth:`get_run_events`: a debug
        action such as ``"step"`` is not guaranteed idempotent, so retrying
        blindly on an ambiguous network failure (e.g. a timeout where the
        server may have already applied the action) risks double-advancing
        the run. If you know a specific failure happened strictly before the
        server received the request, it's safe to retry yourself.

        Args:
            run_id: The run whose DebugSession to control.
            action: One of ``"pause"``, ``"resume"``, ``"step"``, ``"abort"``.
            metadata: Optional metadata to attach to the control action.

        Returns:
            The resulting session state after the action was applied.
        """
        client = self._ensure_client()
        body: dict[str, Any] = {}
        if metadata:
            body["metadata"] = metadata

        with traced_tool_call("observability.send_debug_control", {"action": action}):
            request_headers = {**self._headers, **inject_trace_headers({})}
            try:
                response = await client.post(
                    f"/api/v1/runs/{run_id}/debug/{action}", json=body, headers=request_headers
                )
                response.raise_for_status()
            except httpx.ConnectError as e:
                raise NovaOSConnectionError(
                    f"Failed to connect to Gateway: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.TimeoutException as e:
                raise NovaOSConnectionError(
                    f"Gateway debug-control request timed out: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.HTTPStatusError as e:
                raise NovaOSServerError(
                    status_error_message("Gateway", e, context=" for debug control"),
                    status_code=e.response.status_code,
                    details=status_error_details(e),
                ) from e

            return DebugControlResult.from_dict(response.json())
