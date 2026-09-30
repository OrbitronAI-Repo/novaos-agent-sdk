"""Client for the NovaOS agent-ops Heartbeat Schedule endpoints.

Lets a caller schedule proactive ("heartbeat") executions of an agent
instance — wiring a real Temporal Schedule that fires ``agentTaskWorkflow``
on a cron cadence, instead of the agent faking recurrence with a
self-hosted ``POST /trigger/scheduled`` endpoint.

.. admonition:: Direct agent-ops REST client, not an MCP-routed tool

   Like :class:`~novaos_client_sdk.memory.MemoryClient` and
   :class:`~novaos_client_sdk.observability.ObservabilityClient`, this talks
   directly to a platform HTTP service rather than going through the MCP
   Gateway's ``/mcp/v1/tools/call`` JSON-RPC envelope — because these are
   agent-ops control-plane routes, not tools registered in the MCP tool
   registry.

.. admonition:: Auth — IAM bearer token, not a service-account header

   Every agent-ops route is guarded by IAM-issued JWT bearer auth
   (tokens validated against ``iamServiceIssuer`` /
   ``iamServiceJwksUri`` / ``iamServiceAudience``), not the service-account
   convention some Gateway routes use. The SDK does not obtain, refresh, or
   manage this token — the caller supplies it via ``headers`` (e.g.
   ``{"Authorization": f"Bearer {token}"}"``), matching every other client
   in this SDK.

Endpoint contract: registered via
``server.register(heartbeatRoutes, {prefix: "/api/v1/agent-ops", ...})``.
Base URL env var (``NOVAOS_AGENT_OPS_URL``) matches the existing
``galaxy-cli`` convention, not a name invented for this client.

.. admonition:: Why all four operations retry, unlike DebugSession's actions

   :class:`~novaos_client_sdk.observability.ObservabilityClient`'s
   ``send_debug_control`` deliberately never auto-retries, because an action
   like ``"step"`` is not guaranteed idempotent server-side. Every operation
   here IS safe to retry on an ambiguous network failure, by construction on
   the platform side: :meth:`HeartbeatClient.create_config` hits a DB unique
   constraint on ``instance_id`` (a retried create after a successful-but-
   unacknowledged first attempt gets a decisive 409, never a second
   schedule), and :meth:`HeartbeatClient.pause_config` /
   :meth:`HeartbeatClient.resume_config` no-op server-side when the target
   status is already reached. So this client always retries on connection
   errors and 429/502/503/504, the same policy
   :meth:`~novaos_client_sdk.observability.ObservabilityClient.get_run_events`
   uses for its read-only GET.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any

from novaos_client_sdk._http import httpx
from novaos_client_sdk._resilience import CircuitBreaker as _CircuitBreaker
from novaos_client_sdk._resilience import backoff_delay
from novaos_client_sdk.exceptions import (
    NovaOSConnectionError,
    NovaOSError,
    NovaOSServerError,
)
from novaos_client_sdk.tracing import inject_trace_headers, traced_tool_call

logger = logging.getLogger(__name__)

NOVAOS_AGENT_OPS_URL_ENV = "NOVAOS_AGENT_OPS_URL"


# ---------------------------------------------------------------------------
# Response dataclasses — one per distinct wire shape (ISP: no artificial
# shared type spanning create/list/pause-resume's genuinely different bodies)
# ---------------------------------------------------------------------------


@dataclass
class HeartbeatBudget:
    """Daily spend/usage cap for a heartbeat config's scheduled runs."""

    max_daily_runs: int = 0
    max_daily_tokens: int = 0
    max_daily_cost_usd: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_daily_runs": self.max_daily_runs,
            "max_daily_tokens": self.max_daily_tokens,
            "max_daily_cost_usd": self.max_daily_cost_usd,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatBudget:
        return cls(
            max_daily_runs=data.get("max_daily_runs", 0),
            max_daily_tokens=data.get("max_daily_tokens", 0),
            max_daily_cost_usd=data.get("max_daily_cost_usd", 0.0),
        )


@dataclass
class HeartbeatSchedule:
    """One Temporal-backed schedule under a :class:`HeartbeatConfig`."""

    schedule_id: str = ""
    temporal_schedule_id: str = ""
    status: str = ""
    cron_expression: str | None = None
    timezone: str = "UTC"
    next_run_at: str | None = None
    last_run_at: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatSchedule:
        return cls(
            schedule_id=data.get("schedule_id", ""),
            temporal_schedule_id=data.get("temporal_schedule_id", ""),
            status=data.get("status", ""),
            cron_expression=data.get("cron_expression"),
            timezone=data.get("timezone", "UTC"),
            next_run_at=data.get("next_run_at"),
            last_run_at=data.get("last_run_at"),
        )


@dataclass
class HeartbeatConfig:
    """A heartbeat config as returned by :meth:`HeartbeatClient.list_configs`."""

    config_id: str = ""
    instance_id: str = ""
    status: str = ""
    triggers: list[dict[str, Any]] = field(default_factory=list)
    budget: HeartbeatBudget | None = None
    schedules: list[HeartbeatSchedule] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatConfig:
        budget_data = data.get("budget")
        return cls(
            config_id=data.get("config_id", ""),
            instance_id=data.get("instance_id", ""),
            status=data.get("status", ""),
            triggers=data.get("triggers", []),
            budget=HeartbeatBudget.from_dict(budget_data) if budget_data else None,
            schedules=[HeartbeatSchedule.from_dict(s) for s in data.get("schedules", [])],
        )


@dataclass
class HeartbeatConfigCreated:
    """Result of :meth:`HeartbeatClient.create_config`."""

    config_id: str = ""
    instance_id: str = ""
    status: str = ""
    temporal_schedule_id: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatConfigCreated:
        inner = data.get("data", data)
        return cls(
            config_id=inner.get("config_id", ""),
            instance_id=inner.get("instance_id", ""),
            status=inner.get("status", ""),
            temporal_schedule_id=inner.get("temporal_schedule_id", ""),
        )


@dataclass
class HeartbeatConfigStatus:
    """Result of :meth:`HeartbeatClient.pause_config` / :meth:`resume_config`."""

    config_id: str = ""
    instance_id: str = ""
    status: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatConfigStatus:
        inner = data.get("data", data)
        return cls(
            config_id=inner.get("config_id", ""),
            instance_id=inner.get("instance_id", ""),
            status=inner.get("status", ""),
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class HeartbeatClient:
    """Async client for the NovaOS agent-ops Heartbeat Schedule endpoints.

    Args:
        base_url: agent-ops base URL. If ``None``, reads from
            ``NOVAOS_AGENT_OPS_URL`` env var.
        headers: Additional HTTP headers — must include
            ``{"Authorization": "Bearer <iam-token>"}``. See the module
            docstring on auth: this is an IAM-issued token, not a
            service-account header.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum retry attempts for transient failures
            (default: 3). See the module docstring for why every method
            here retries, unlike ``ObservabilityClient.send_debug_control``.
        circuit_breaker_threshold: Failures before circuit opens (default: 5).
        circuit_breaker_reset_s: Seconds before half-open probe (default: 30).
        client: Optional pre-configured ``httpx.AsyncClient``. If provided,
            this class will NOT close it on ``__aexit__`` — the caller
            retains ownership of its lifecycle.

    Example:
        ```python
        async with HeartbeatClient(
            base_url="https://agent-ops.internal",
            headers={"Authorization": f"Bearer {iam_token}"},
        ) as hb:
            created = await hb.create_config(
                agent_id, instance_id,
                cron_expression="0 * * * *",
                agent_image="us-docker.pkg.dev/.../joke",
                agent_image_tag="1.1.10",
                task_content="Check in on any pending tasks.",
                budget=HeartbeatBudget(max_daily_runs=24, max_daily_tokens=100_000, max_daily_cost_usd=5),
            )
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
            base_url = os.getenv(NOVAOS_AGENT_OPS_URL_ENV)
            if not base_url:
                raise NovaOSError(
                    f"agent-ops URL not provided and '{NOVAOS_AGENT_OPS_URL_ENV}' "
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

    async def __aenter__(self) -> HeartbeatClient:
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
                "HeartbeatClient is not initialized. "
                "Use async context manager or call __aenter__() first."
            )
        return self._client

    @staticmethod
    def _config_path(agent_id: str, instance_id: str) -> str:
        return f"/api/v1/agent-ops/agents/{agent_id}/instances/{instance_id}/heartbeat/configs"

    # ------------------------------------------------------------------
    # POST .../heartbeat/configs
    # ------------------------------------------------------------------

    async def create_config(
        self,
        agent_id: str,
        instance_id: str,
        *,
        cron_expression: str,
        agent_image: str,
        agent_image_tag: str,
        task_content: str,
        budget: HeartbeatBudget | dict[str, Any],
        timezone: str = "UTC",
    ) -> HeartbeatConfigCreated:
        """Create a heartbeat config — schedules ``agentTaskWorkflow`` on ``cron_expression``.

        MVP scope (platform-side): exactly one cron trigger per instance.
        A second call for the same instance gets a 409 (mapped to
        :class:`~novaos_client_sdk.exceptions.NovaOSServerError` with
        ``status_code=409``), not a duplicate schedule.

        Args:
            agent_id: The agent this instance belongs to.
            instance_id: The instance to schedule heartbeats for.
            cron_expression: Standard cron expression (e.g. ``"0 * * * *"``).
            agent_image: OCI image ref to run on each scheduled fire.
            agent_image_tag: Tag for ``agent_image``.
            task_content: Message content sent to the agent on each fire.
            budget: Daily run/token/cost caps — required by the platform,
                no default is invented here (an unlimited default would be
                a real cost-control gap).
            timezone: IANA timezone for the cron schedule (default "UTC").

        Returns:
            The created config's ID and its underlying Temporal schedule ID.
        """
        budget_dict = budget.to_dict() if isinstance(budget, HeartbeatBudget) else budget
        body = {
            "cron_expression": cron_expression,
            "timezone": timezone,
            "agent_image": agent_image,
            "agent_image_tag": agent_image_tag,
            "task_content": task_content,
            "budget": budget_dict,
        }
        data = await self._send_with_retry(
            "POST", self._config_path(agent_id, instance_id), json_body=body
        )
        return HeartbeatConfigCreated.from_dict(data)

    # ------------------------------------------------------------------
    # GET .../heartbeat/configs
    # ------------------------------------------------------------------

    async def list_configs(self, agent_id: str, instance_id: str) -> list[HeartbeatConfig]:
        """List heartbeat configs for an instance (0 or 1, per the MVP's unique-instance constraint)."""
        data = await self._send_with_retry("GET", self._config_path(agent_id, instance_id))
        inner = data.get("data", data)
        return [HeartbeatConfig.from_dict(c) for c in inner.get("configs", [])]

    # ------------------------------------------------------------------
    # POST .../configs/:configId/pause | /resume
    # ------------------------------------------------------------------

    async def pause_config(
        self, agent_id: str, instance_id: str, config_id: str
    ) -> HeartbeatConfigStatus:
        """Pause a heartbeat config's schedule(s). Idempotent — pausing an already-paused config no-ops."""
        path = f"{self._config_path(agent_id, instance_id)}/{config_id}/pause"
        data = await self._send_with_retry("POST", path)
        return HeartbeatConfigStatus.from_dict(data)

    async def resume_config(
        self, agent_id: str, instance_id: str, config_id: str
    ) -> HeartbeatConfigStatus:
        """Resume a paused heartbeat config's schedule(s). Idempotent — resuming an already-active config no-ops."""
        path = f"{self._config_path(agent_id, instance_id)}/{config_id}/resume"
        data = await self._send_with_retry("POST", path)
        return HeartbeatConfigStatus.from_dict(data)

    # ------------------------------------------------------------------
    # Shared HTTP + retry + circuit-breaker plumbing
    # ------------------------------------------------------------------

    async def _send_with_retry(
        self, method: str, path: str, json_body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Send with retry + circuit breaker (mirrors ObservabilityClient._get_with_retry).

        Safe to retry for all four operations — see the module docstring.
        """
        if not self._cb.allow_request():
            raise NovaOSConnectionError(
                "agent-ops heartbeat circuit breaker is OPEN — failing fast. "
                f"Will probe again in {self._cb.reset_timeout}s."
            )

        last_error: NovaOSError | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                result = await self._send_once(method, path, json_body)
                self._cb.record_success()
                return result
            except NovaOSError as e:
                last_error = e
                if not e.is_retryable or attempt == self._max_retries:
                    self._cb.record_failure()
                    raise

                delay = e.retry_after if e.retry_after else backoff_delay(attempt)
                logger.warning(
                    "agent-ops heartbeat request failed (attempt %d/%d, retrying in %.1fs): %s",
                    attempt,
                    self._max_retries,
                    delay,
                    e.message,
                )
                await asyncio.sleep(delay)

        self._cb.record_failure()
        raise last_error  # type: ignore[misc]

    async def _send_once(
        self, method: str, path: str, json_body: dict[str, Any] | None
    ) -> dict[str, Any]:
        client = self._ensure_client()
        with traced_tool_call(f"heartbeat.{method.lower()}", {"path": path}):
            request_headers = {**self._headers, **inject_trace_headers({})}
            try:
                response = await client.request(
                    method, path, json=json_body, headers=request_headers
                )
                response.raise_for_status()
            except httpx.ConnectError as e:
                raise NovaOSConnectionError(
                    f"Failed to connect to agent-ops: {e}",
                    details={"original_error": type(e).__name__},
                ) from e
            except httpx.TimeoutException as e:
                raise NovaOSConnectionError(
                    f"agent-ops heartbeat request timed out: {e}",
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
                    f"agent-ops returned HTTP {e.response.status_code}: {e}",
                    status_code=e.response.status_code,
                    retry_after=retry_after,
                    details={"original_error": type(e).__name__},
                ) from e

            result: dict[str, Any] = response.json()
            return result
