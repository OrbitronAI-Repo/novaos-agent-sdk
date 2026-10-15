"""Catalog-backed agent discovery for callers outside the cluster.

Agent-DNS is the cluster-internal registry and is not reachable from outside it.
Its records also carry no capabilities, so :meth:`AgentDNSClient.find_agents` cannot
match on what an agent does.  The marketplace's public catalog is the external
counterpart: ``GET /api/agents`` lists published agents, each with its A2A Agent
Card, and filters on the card's skill ids.  :class:`AgentCatalogClient` wraps that
route.

A skill id is the card's ``skills[].id`` — the same name an agent's manifest
declares as a capability — so ``skills=["conformance"]`` finds agents that declare
that capability.

The catalog describes agents; it does not route to them.  To call a discovered
agent, address it through the gateway (:meth:`CatalogAgent.gateway_url`), which is
also where its card is served (:meth:`CatalogAgent.card_url`).  The pod URL a card
may carry is an in-cluster address and is never a way in from outside.

Example::

    async with AgentCatalogClient("https://marketplace.example.com") as catalog:
        async for agent in catalog.find_agents(skills=["conformance"]):
            print(agent.name, agent.skill_ids)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Literal

from novaos_client_sdk._http import httpx
from novaos_client_sdk.a2a_agent_card import AgentCard
from novaos_client_sdk.discovery import _parsed_card, _retry_after
from novaos_client_sdk.exceptions import (
    NovaOSAuthenticationError,
    NovaOSConnectionError,
    NovaOSError,
    NovaOSRateLimitError,
    NovaOSServerError,
    NovaOSValidationError,
)

__all__ = ["AgentCatalogClient", "CatalogAgent", "SkillMatch"]

logger = logging.getLogger(__name__)

#: ``all``: an agent must declare every requested skill; ``any``: at least one.
SkillMatch = Literal["all", "any"]

#: The catalog's page size ceiling.
MAX_PAGE_SIZE = 100


@dataclass(frozen=True)
class CatalogAgent:
    """One published agent as the public catalog lists it."""

    agent_id: str
    name: str
    version: str
    publisher: str = ""
    description: str | None = None
    protocol: str = ""
    status: str = ""
    release_channel: str = ""
    #: The agent's Agent Card, or ``None`` when the row has none or it is not a card.
    agent_card: AgentCard | None = None
    #: Skill ids read from the raw card, so they survive a card that fails to parse fully.
    skill_ids: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def has_skill(self, skill_id: str) -> bool:
        """Whether the agent's card declares ``skill_id``."""
        return skill_id in self.skill_ids

    def gateway_url(self, gateway_url: str) -> str:
        """The agent's address on the gateway; the card sits beneath it."""
        return f"{gateway_url.rstrip('/')}/a2a/v1/agents/{self.agent_id}"

    def card_url(self, gateway_url: str) -> str:
        """Where the gateway serves this agent's public Agent Card."""
        return f"{self.gateway_url(gateway_url)}/.well-known/agent-card.json"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CatalogAgent:
        """Build from one catalog row.  Tolerant: a sparse row is a publisher's problem."""
        raw_card = data.get("agent_card")
        card: AgentCard | None = None
        skill_ids: tuple[str, ...] = ()
        if isinstance(raw_card, dict):
            skills = raw_card.get("skills")
            if isinstance(skills, list):
                skill_ids = tuple(
                    str(skill["id"])
                    for skill in skills
                    if isinstance(skill, dict) and skill.get("id")
                )
            try:
                card = _parsed_card(raw_card, str(data.get("agent_id", "")))
            except NovaOSError:
                card = None
        return cls(
            agent_id=str(data.get("agent_id", "")),
            name=str(data.get("name", "")),
            version=str(data.get("version", "")),
            publisher=str(data.get("publisher") or ""),
            description=data.get("description"),
            protocol=str(data.get("protocol") or ""),
            status=str(data.get("status") or ""),
            release_channel=str(data.get("release_channel") or ""),
            agent_card=card,
            skill_ids=skill_ids,
            raw=data,
        )


class AgentCatalogClient:
    """Client for the marketplace's public agent catalog."""

    def __init__(
        self,
        base_url: str,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        page_size: int = MAX_PAGE_SIZE,
    ) -> None:
        """Initialize the catalog client.

        Args:
            base_url: Origin of the marketplace (e.g. ``https://marketplace.example.com``).
            timeout: Request timeout in seconds.
            headers: Additional HTTP headers to include in requests.
            page_size: Rows requested per page, 1 to 100.

        """
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise NovaOSValidationError(
                f"page_size must be between 1 and {MAX_PAGE_SIZE}, got {page_size}",
                field="page_size",
                reason="out_of_range",
            )
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._headers = headers.copy() if headers else {}
        self._page_size = page_size
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> AgentCatalogClient:
        """Async context manager entry."""
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._headers,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if not self._client:
            raise NovaOSError(
                "Client is not initialized. Use async context manager or call __aenter__() first."
            )
        return self._client

    @staticmethod
    def _map_status_error(error: httpx.HTTPStatusError) -> NovaOSError:
        status = error.response.status_code
        details: dict[str, Any] = {"status_code": status}
        if status in (401, 403):
            return NovaOSAuthenticationError(
                f"Agent catalog refused the request ({status})", details=details
            )
        if status == 429:
            return NovaOSRateLimitError(
                "Agent catalog rate limit exceeded",
                retry_after=_retry_after(error.response),
                details=details,
            )
        if status >= 500:
            return NovaOSServerError(
                f"Agent catalog failed: {status}", status_code=status, details=details
            )
        return NovaOSValidationError(
            f"Agent catalog rejected the request: {status}",
            field="catalog_query",
            reason="http_error",
            details=details,
        )

    async def _get_page(
        self, params: list[tuple[str, str | int | float | bool | None]]
    ) -> tuple[list[dict[str, Any]], str | None]:
        client = self._ensure_client()
        try:
            response = await client.get("/api/agents", params=params)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPStatusError as e:
            raise self._map_status_error(e) from e
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise NovaOSConnectionError(
                f"Failed to reach the agent catalog at {self._base_url}: {e}"
            ) from e
        except ValueError as e:
            raise NovaOSValidationError(
                "Agent catalog returned a response that is not JSON",
                field="catalog_response",
                reason="not_json",
            ) from e

        data = payload.get("data") if isinstance(payload, dict) else None
        rows = data.get("agents") if isinstance(data, dict) else None
        if not isinstance(data, dict) or not isinstance(rows, list):
            raise NovaOSValidationError(
                "Agent catalog response has no data.agents list",
                field="catalog_response",
                reason="unexpected_shape",
            )
        next_cursor = data.get("next_cursor")
        return [row for row in rows if isinstance(row, dict)], (
            str(next_cursor) if next_cursor else None
        )

    async def find_agents(
        self,
        skills: list[str] | None = None,
        skill_match: SkillMatch = "all",
        protocol: Literal["A2A", "ACP", "REST"] | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CatalogAgent]:
        """Find published agents, optionally by the skills their Agent Card declares.

        Args:
            skills: Skill ids (``agent_card.skills[].id``) to filter on.  Empty or
                ``None`` lists every published agent.
            skill_match: ``all`` (default) requires every skill, ``any`` at least one.
            protocol: Restrict to one protocol.
            limit: Stop after this many agents.

        Yields:
            CatalogAgent, in the catalog's order, each at most once.

        Raises:
            NovaOSValidationError: For an unknown ``skill_match`` or a request the catalog rejects.
            NovaOSRateLimitError: When the catalog rate limits the caller.
            NovaOSServerError: When the catalog fails.
            NovaOSConnectionError: When the catalog cannot be reached.

        """
        if skill_match not in ("all", "any"):
            raise NovaOSValidationError(
                f"skill_match must be 'all' or 'any', got {skill_match!r}",
                field="skill_match",
                reason="unknown_value",
            )
        base_params: list[tuple[str, str | int | float | bool | None]] = [
            ("limit", str(self._page_size))
        ]
        base_params += [("skill", skill) for skill in (skills or []) if skill.strip()]
        if skills:
            base_params.append(("skill_match", skill_match))
        if protocol:
            base_params.append(("protocol", protocol))

        seen: set[str] = set()
        cursor: str | None = None
        yielded = 0
        while True:
            params = base_params + ([("cursor", cursor)] if cursor else [])
            rows, next_cursor = await self._get_page(params)
            fresh = 0
            for row in rows:
                agent = CatalogAgent.from_dict(row)
                if not agent.agent_id or agent.agent_id in seen:
                    continue
                seen.add(agent.agent_id)
                fresh += 1
                yield agent
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            if not next_cursor:
                return
            if fresh == 0 or next_cursor == cursor:
                # A cursor that does not advance would repeat the same page forever.
                logger.warning(
                    "Agent catalog cursor did not advance; stopping after %d agent(s)", yielded
                )
                return
            cursor = next_cursor
