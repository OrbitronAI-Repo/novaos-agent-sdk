"""DNS client for agent discovery across the network."""

from __future__ import annotations

import ipaddress
import logging
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

from novaos_client_sdk._http import httpx
from novaos_client_sdk._idna import wire_hostname as _wire_hostname
from novaos_client_sdk._idna import wire_suffix as _wire_suffix
from novaos_client_sdk._ssrf import is_blocked_ip
from novaos_client_sdk.client import Client
from novaos_client_sdk.exceptions import (
    NovaOSDNSAgentNotFoundError,
    NovaOSDNSConnectionError,
    NovaOSDNSError,
    NovaOSDNSRateLimitError,
    NovaOSError,
)
from novaos_client_sdk.models import Capability
from novaos_client_sdk.utils import match_capabilities, normalize_capability_name

logger = logging.getLogger(__name__)


def _hostname_matches_suffix(hostname: str, suffix: str) -> bool:
    """Check hostname against a suffix with proper dot-boundary enforcement.

    Prevents ``evil-example.com`` from matching suffix ``.example.com``.
    """
    if suffix.startswith("."):
        return hostname.endswith(suffix) or hostname == suffix[1:]
    return hostname == suffix or hostname.endswith("." + suffix)


def validate_agent_url(
    url: str,
    allowed_suffixes: list[str] | None = None,
    *,
    allow_private_ips: bool = False,
) -> None:
    """Validate an agent URL hostname against suffix allowlist and blocked IP ranges.

    Args:
        url: Agent base URL. Ordinarily carries a scheme
            (``https://agent.example.com``), but a gRPC interface's URL
            may be a bare ``host:port`` authority with no scheme
            at all — the conventional shape of a gRPC target. ``urlparse``
            alone misreads that as *scheme* ``host`` with no hostname
            (``urlparse("agent.example.com:50051").hostname is None``), which
            would make this guard silently validate nothing rather than
            reject it; see the scheme-less retry below.
        allowed_suffixes: When provided and non-empty, hostname must end with
            at least one suffix.
        allow_private_ips: When True, skip the private/loopback/link-local/
            reserved IP-range block below (scheme parsing and the suffix
            check above still run). Intended only for a URL sourced from a
            trusted internal registry the caller explicitly configured — an
            Agent-DNS record, not attacker-influenceable metadata such as a
            fetched Agent Card. See ``AgentDNSRecord.to_client``'s
            ``trust_url`` for the caller-facing distinction; defaults to
            False here so every direct caller of this function stays
            guarded unless it opts in.

    Raises:
        NovaOSError: If the URL cannot be parsed at all (e.g. an unterminated
            IPv6 literal or an out-of-range port), if hostname fails the
            suffix check, or if it resolves to a blocked IP. ``urlparse``
            itself raises a bare ``ValueError`` for some malformed input
            (caller-controlled callers should not have to know that as a
            separate case from every other rejection this function makes),
            so that is caught here and re-raised as ``NovaOSError`` too.
    """
    try:
        parsed = urlparse(url)
        if not parsed.hostname and "://" not in url:
            # No recognizable scheme at all — retry as a bare authority
            # (`//host:port`), which `urlparse` parses correctly regardless of
            # whether a scheme precedes it.
            parsed = urlparse(f"//{url}")
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise NovaOSError(f"Malformed URL {url!r}: {exc}") from exc
    if not hostname:
        raise NovaOSError(f"Invalid URL format: {url}")
    hostname = _wire_hostname(hostname)

    if allowed_suffixes:
        valid_suffixes = [_wire_suffix(s) for s in allowed_suffixes if s]
        if valid_suffixes and not any(
            _hostname_matches_suffix(hostname, suffix) for suffix in valid_suffixes
        ):
            raise NovaOSError(f"Host '{hostname}' not in allowed suffixes")

    try:
        addr_infos = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise NovaOSError(
            f"Cannot resolve host '{hostname}' — blocking to prevent SSRF bypass"
        ) from exc

    for info in addr_infos:
        sockaddr = info[4]
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if not allow_private_ips and is_blocked_ip(ip):
            raise NovaOSError(f"Resolved IP {ip_str} for host '{hostname}' is in a blocked range")


@dataclass
class AgentDNSRecord:
    """Agent DNS record with connection details."""

    id: str
    name: str
    url: str
    description: str | None = None
    metadata: dict[str, Any] | None = None
    capabilities: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    status: str = "active"

    def __post_init__(self) -> None:
        """Validate DNS record."""
        if not self.name:
            raise ValueError("Agent name is required")
        if not self.url:
            raise ValueError("Agent URL is required")
        # Validate URL format
        if not self.url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid URL format: {self.url}")

    @property
    def is_available(self) -> bool:
        """Check if agent is available."""
        return self.status == "active"

    def has_capability(self, capability_name: str, fuzzy: bool = False) -> bool:
        """Check if agent has specific capability.

        Args:
            capability_name: Capability name to check
            fuzzy: If True, use fuzzy matching (threshold 0.8)

        Returns:
            True if agent has the capability
        """
        if not capability_name:
            return False

        # Check in capabilities list (simple string list)
        normalized_required = normalize_capability_name(capability_name)
        for cap in self.capabilities:
            if normalize_capability_name(cap) == normalized_required:
                return True
            if fuzzy and match_capabilities(capability_name, [cap], threshold=0.8):
                return True

        # Check in metadata.capabilities (list of dicts with name/description)
        if self.metadata and "capabilities" in self.metadata:
            metadata_caps = self.metadata["capabilities"]
            if isinstance(metadata_caps, list):
                for cap_data in metadata_caps:
                    if isinstance(cap_data, dict):
                        cap_name = cap_data.get("name", "")
                        if normalize_capability_name(cap_name) == normalized_required:
                            return True
                        if fuzzy and match_capabilities(capability_name, [cap_name], threshold=0.8):
                            return True
                    elif isinstance(cap_data, str):
                        if normalize_capability_name(cap_data) == normalized_required:
                            return True
                        if fuzzy and match_capabilities(capability_name, [cap_data], threshold=0.8):
                            return True

        return False

    def get_capabilities(self) -> list[Capability]:
        """Get parsed capabilities from metadata.

        Returns:
            List of Capability objects parsed from metadata.capabilities
        """
        capabilities: list[Capability] = []

        # Parse from metadata.capabilities if available
        if self.metadata and "capabilities" in self.metadata:
            metadata_caps = self.metadata["capabilities"]
            if isinstance(metadata_caps, list):
                for cap_data in metadata_caps:
                    if isinstance(cap_data, dict):
                        try:
                            capabilities.append(Capability.from_dict(cap_data))
                        except (KeyError, ValueError, TypeError):
                            # Skip malformed capability data
                            logger.warning(
                                f"Failed to parse capability from metadata for agent {self.name}: {cap_data}"
                            )
                    elif isinstance(cap_data, str):
                        # Convert string to Capability object
                        capabilities.append(Capability(name=cap_data))

        # Also add capabilities from the simple list if not already included
        for cap_name in self.capabilities:
            # Check if already in parsed capabilities
            if not any(
                normalize_capability_name(c.name) == normalize_capability_name(cap_name)
                for c in capabilities
            ):
                capabilities.append(Capability(name=cap_name))

        return capabilities

    @property
    def parsed_capabilities(self) -> list[Capability]:
        """Get parsed capabilities from metadata (property alias for get_capabilities)."""
        return self.get_capabilities()

    def get_domains(self) -> list[str]:
        """Get domains from record field and metadata, merged.

        Returns:
            List of domain strings, merging record.domains field and metadata.domains
        """
        domains_set: set[str] = set()

        # Add domains from field
        for domain in self.domains:
            if domain:
                domains_set.add(str(domain))

        # Add domains from metadata
        if self.metadata and "domains" in self.metadata:
            metadata_domains = self.metadata["domains"]
            if isinstance(metadata_domains, list):
                for domain in metadata_domains:
                    if domain:
                        domains_set.add(str(domain))

        return list(domains_set)

    def get_tags(self) -> list[str]:
        """Get tags from record field and metadata, merged.

        Returns:
            List of tag strings, merging record.tags field and metadata.tags
        """
        tags_set: set[str] = set()

        # Add tags from field
        for tag in self.tags:
            if tag:
                tags_set.add(str(tag))

        # Add tags from metadata
        if self.metadata and "tags" in self.metadata:
            metadata_tags = self.metadata["tags"]
            if isinstance(metadata_tags, list):
                for tag in metadata_tags:
                    if tag:
                        tags_set.add(str(tag))

        return list(tags_set)

    def to_client(
        self,
        *,
        allowed_host_suffixes: list[str] | None = None,
        trust_url: bool = False,
        **client_kwargs: Any,
    ) -> Client:
        """Create ACP client from DNS record.

        **Trust is opt-in, not the default.** This URL comes from the
        Agent-DNS registry the caller explicitly configured and queried by
        name — a platform-auth-gated internal source, not
        attacker-influenceable metadata like a fetched Agent Card (see
        ``AgentDiscoveryService.to_client``, whose guard stays on
        unconditionally for exactly that reason). That distinction is why
        ``trust_url`` exists at all — but the sanctioned way to reach a
        NovaOS agent pod is gateway-mediated (the auth-proxy sidecar on
        ``:8000`` validates the run's RS256 execution token); a direct
        peer-to-peer dial to an agent's raw `*.example.internal` address is
        off that golden path (EXPL-EXEC-008) regardless of whether the
        private-IP-range guard lets the connection attempt through. Shipping
        this opt-in as the default would make the SDK quietly steer callers
        onto the off-golden-path route; leaving it off keeps the mechanism
        available for a caller who knowingly needs a direct dial (e.g. a
        deliberately unmediated internal tool) without making that the path
        of least resistance. Whether/how a gateway-mediated connect helper is
        added is a separate, later decision — not made by this flag.

        Args:
            allowed_host_suffixes: Optional hostname suffix allowlist passed to
                :func:`validate_agent_url` before client construction. This is
                always enforced, regardless of ``trust_url`` — it narrows, it
                never widens.
            trust_url: When True, skip the private/loopback/link-local/
                reserved IP-range block for this URL. Defaults to False — see
                "Trust is opt-in, not the default" above.
            **client_kwargs: Additional arguments to pass to Client constructor
                (e.g., timeout, headers, auth)

        Returns:
            Client instance configured with this agent's URL
        """
        validate_agent_url(self.url, allowed_host_suffixes, allow_private_ips=trust_url)
        return Client(
            base_url=self.url, guard_connect_ips=True, allow_private_ips=trust_url, **client_kwargs
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "url": self.url,
            "description": self.description,
            "metadata": self.metadata or {},
            "capabilities": self.capabilities,
            "domains": self.domains,
            "tags": self.tags,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentDNSRecord:
        """Create from dictionary."""
        # Defensive handling for id field
        record_id = data.get("id")
        if not record_id:
            # Try fallback fields
            record_id = data.get("name") or data.get("_id")
            if not record_id:
                available_keys = list(data.keys())
                error_msg = (
                    f"Missing required field 'id' in DNS record data. "
                    f"Available keys: {available_keys}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

        # Defensive handling for url field
        url = data.get("url")
        if not url:
            # Try alternative field names
            url = (
                data.get("agent_url")
                or data.get("endpoint")
                or data.get("base_url")
                or data.get("service_url")
            )
            if not url:
                available_keys = list(data.keys())
                error_msg = (
                    f"Missing required field 'url' in DNS record data. "
                    f"Available keys: {available_keys}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

        created_at = None
        updated_at = None
        if data.get("created_at"):
            if isinstance(data["created_at"], str):
                created_at = datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))
            elif isinstance(data["created_at"], datetime):
                created_at = data["created_at"]
        if data.get("updated_at"):
            if isinstance(data["updated_at"], str):
                updated_at = datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00"))
            elif isinstance(data["updated_at"], datetime):
                updated_at = data["updated_at"]

        return cls(
            id=record_id,
            name=data["name"],
            url=url,
            description=data.get("description"),
            metadata=data.get("metadata"),
            capabilities=data.get("capabilities", []),
            domains=data.get("domains", []),
            tags=data.get("tags", []),
            created_at=created_at,
            updated_at=updated_at,
            status=data.get("status", "active"),
        )


class AgentDNSClient:
    """Client for discovering agents via DNS service."""

    def __init__(
        self,
        base_url: str,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        allowed_host_suffixes: list[str] | None = None,
        trust_dns_urls: bool = False,
    ) -> None:
        """Initialize DNS client.

        Args:
            base_url: Base URL of the DNS service (e.g., "http://agent-dns.example.internal:8000")
            timeout: Request timeout in seconds
            headers: Additional HTTP headers to include in requests
            allowed_host_suffixes: Optional hostname suffix allowlist for agent URLs
                returned by DNS lookups when creating clients from records.
            trust_dns_urls: Forwarded as ``trust_url`` to
                :meth:`AgentDNSRecord.to_client` when :meth:`get_agent_client`
                builds a client from a looked-up record. Defaults to False —
                see that method's "Trust is opt-in, not the default" for why:
                the sanctioned way to reach a NovaOS agent is gateway-mediated,
                not a direct dial, so this SDK does not default to enabling a
                direct one. Pass True only when this client's caller
                knowingly needs a direct dial to whatever Agent-DNS returns.

        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._headers = headers.copy() if headers else {}
        self._allowed_host_suffixes = allowed_host_suffixes
        self._trust_dns_urls = trust_dns_urls
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> AgentDNSClient:
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
        """Ensure HTTP client is initialized."""
        if not self._client:
            raise NovaOSError(
                "Client is not initialized. Use async context manager or call __aenter__() first."
            )
        return self._client

    def _map_http_error(self, error: Exception, agent_name: str | None = None) -> NovaOSDNSError:
        """Map HTTP errors to DNS exceptions.

        Args:
            error: HTTP error from httpx
            agent_name: Optional agent name for context

        Returns:
            Mapped DNS exception
        """
        error_str = str(error)
        error_type = type(error).__name__

        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code
            if status_code == 404:
                return NovaOSDNSAgentNotFoundError(
                    f"Agent not found in DNS service: {error_str}",
                    agent_name=agent_name,
                    details={"status_code": status_code, "original_error": error_type},
                )
            # 502/503/504 mean the DNS service is unreachable or overloaded —
            # transient, so they map to the retryable connection error. 500 is
            # deliberately excluded, matching NovaOSServerError's
            # _RETRYABLE_STATUS_CODES: an Internal Server Error can be a
            # deterministic failure that retrying will only reproduce.
            if status_code in (502, 503, 504):
                return NovaOSDNSConnectionError(
                    f"DNS service error: {error_str}",
                    details={"status_code": status_code, "original_error": error_type},
                )
            if status_code == 500:
                return NovaOSDNSError(
                    f"DNS service error: {error_str}",
                    details={"status_code": status_code, "original_error": error_type},
                )
            if status_code == 429:
                retry_after = None
                if "Retry-After" in error.response.headers:
                    try:
                        retry_after = float(error.response.headers["Retry-After"])
                    except ValueError:
                        pass
                return NovaOSDNSRateLimitError(
                    f"DNS service rate limit exceeded: {error_str}",
                    retry_after=retry_after,
                    details={"status_code": status_code, "original_error": error_type},
                )

        if isinstance(error, httpx.ConnectError | httpx.TimeoutException):
            return NovaOSDNSConnectionError(
                f"Failed to connect to DNS service: {error_str}",
                details={"original_error": error_type},
            )

        return NovaOSDNSError(
            f"DNS service error: {error_str}",
            details={"original_error": error_type},
        )

    async def get_agent(self, name: str) -> AgentDNSRecord | None:
        """Get agent by name from DNS service.

        Args:
            name: Agent name

        Returns:
            AgentDNSRecord if found, None otherwise

        Raises:
            NovaOSDNSConnectionError: If connection to DNS service fails
            NovaOSDNSError: For other DNS service errors
        """
        client = self._ensure_client()

        try:
            response = await client.get(f"/agents/{name}")
            response.raise_for_status()
            data = response.json()
            return AgentDNSRecord.from_dict(data)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise self._map_http_error(e, agent_name=name) from e
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise self._map_http_error(e, agent_name=name) from e
        except Exception as e:
            raise self._map_http_error(e, agent_name=name) from e

    async def list_agents(self) -> AsyncIterator[AgentDNSRecord]:
        """List all agents from DNS service.

        Yields:
            AgentDNSRecord objects

        Raises:
            NovaOSDNSConnectionError: If connection to DNS service fails
            NovaOSDNSError: For other DNS service errors
        """
        client = self._ensure_client()

        try:
            response = await client.get("/agents")
            response.raise_for_status()
            data = response.json()
            # Handle both list and dict with 'agents' key
            agents_data = data if isinstance(data, list) else data.get("agents", [])
            for agent_data in agents_data:
                yield AgentDNSRecord.from_dict(agent_data)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise self._map_http_error(e) from e
        except Exception as e:
            raise self._map_http_error(e) from e

    async def search_agents(self, query: str) -> AsyncIterator[AgentDNSRecord]:
        """Search agents by query string.

        Args:
            query: Search query string

        Yields:
            AgentDNSRecord objects matching the query

        Raises:
            NovaOSDNSConnectionError: If connection to DNS service fails
            NovaOSDNSError: For other DNS service errors
        """
        client = self._ensure_client()

        try:
            # Try POST first (if service supports it), fallback to GET
            try:
                response = await client.post("/agents/search", json={"query": query})
                response.raise_for_status()
            except httpx.HTTPStatusError:
                # Fallback to GET with query parameter
                response = await client.get("/agents/search", params={"q": query})
                response.raise_for_status()

            data = response.json()
            agents_data = data if isinstance(data, list) else data.get("agents", [])
            for agent_data in agents_data:
                yield AgentDNSRecord.from_dict(agent_data)
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise self._map_http_error(e) from e
        except Exception as e:
            raise self._map_http_error(e) from e

    def _matches_text_filter(self, value: str, filter_text: str) -> bool:
        """Check if a value matches a text filter (case-insensitive partial match).

        Args:
            value: Value to check
            filter_text: Filter text to match against

        Returns:
            True if value contains filter_text (case-insensitive)
        """
        return filter_text.lower() in value.lower()

    def _agent_matches_filters(
        self,
        agent: AgentDNSRecord,
        *,
        name: str | None = None,
        domain: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        capability_list: list[str] | None = None,
        require_all_capabilities: bool = True,
        fuzzy_match: bool = False,
    ) -> bool:
        """Check if an agent matches all provided filters client-side.

        Args:
            agent: Agent record to check
            name: Filter by agent name (partial match)
            domain: Filter by domain
            tag: Filter by tag
            status: Filter by status
            capability_list: Filter by capability names
            require_all_capabilities: If True, agent must have all capabilities
            fuzzy_match: If True, use fuzzy matching for capabilities

        Returns:
            True if agent matches all provided filters
        """
        # Name filter (case-insensitive partial match)
        if name and not self._matches_text_filter(agent.name, name):
            return False

        # Status filter (exact match, case-insensitive)
        if status and agent.status.lower() != status.lower():
            return False

        # Domain filter (check agent's domains list)
        if domain:
            agent_domains = agent.get_domains()
            if not any(self._matches_text_filter(d, domain) for d in agent_domains):
                return False

        # Tag filter (check agent's tags list)
        if tag:
            agent_tags = agent.get_tags()
            if not any(self._matches_text_filter(t, tag) for t in agent_tags):
                return False

        # Capability filtering
        if capability_list:
            if len(capability_list) > 1:
                matches = [agent.has_capability(cap, fuzzy=fuzzy_match) for cap in capability_list]
                if require_all_capabilities and not all(matches):
                    return False
                if not require_all_capabilities and not any(matches):
                    return False
            else:
                if not agent.has_capability(capability_list[0], fuzzy=fuzzy_match):
                    return False

        return True

    async def get_agent_client(
        self,
        name: str,
        **client_kwargs: Any,
    ) -> Client:
        """Discover an agent by name and return a connected :class:`Client`.

        Uses the ``allowed_host_suffixes`` configured on this DNS client
        instance for URL validation before constructing the ACP client.

        Args:
            name: Agent name to look up.
            **client_kwargs: Forwarded to :meth:`AgentDNSRecord.to_client`
                (e.g. ``timeout``, ``headers``, ``auth``).

        Returns:
            A :class:`Client` configured with the agent's URL.

        Raises:
            NovaOSDNSAgentNotFoundError: If the agent is not found.
            NovaOSDNSConnectionError: On DNS service connectivity issues.
        """
        record = await self.get_agent(name)
        if not record:
            raise NovaOSDNSAgentNotFoundError(
                f"Agent '{name}' not found in DNS service", agent_name=name
            )
        return record.to_client(
            allowed_host_suffixes=self._allowed_host_suffixes,
            trust_url=self._trust_dns_urls,
            **client_kwargs,
        )

    async def find_agents(
        self,
        name: str | None = None,
        capability: str | None = None,
        capabilities: list[str] | None = None,
        require_all_capabilities: bool = True,
        fuzzy_match: bool = False,
        domain: str | None = None,
        tag: str | None = None,
        status: str | None = None,
    ) -> AsyncIterator[AgentDNSRecord]:
        """Find agents by criteria with enhanced capability filtering.

        When text-based filters (name, domain, tag) are provided, the method routes
        through ``POST /agents/search`` for server-side pre-filtering, then applies
        all remaining filters client-side. When only capability filters are provided,
        it fetches all agents via ``GET /agents`` and filters client-side.

        Args:
            name: Filter by agent name (partial match)
            capability: Filter by single capability name (deprecated, use capabilities)
            capabilities: Filter by multiple capability names
            require_all_capabilities: If True, agent must have all capabilities (AND logic).
                                     If False, agent needs at least one (OR logic)
            fuzzy_match: If True, use fuzzy matching for capabilities
            domain: Filter by domain
            tag: Filter by tag
            status: Filter by status (e.g., "active", "inactive")

        Yields:
            AgentDNSRecord objects matching the criteria

        Raises:
            NovaOSDNSConnectionError: If connection to DNS service fails
            NovaOSDNSError: For other DNS service errors
        """
        client = self._ensure_client()

        # Build capability list (support both old and new API)
        capability_list: list[str] = []
        if capabilities:
            capability_list = capabilities
        elif capability:
            capability_list = [capability]

        # Determine the best query term for server-side search.
        # The Agent DNS POST /agents/search accepts a single query string that
        # searches across name, description, tags, capabilities, and domains.
        # GET /agents only supports skip/take pagination — no filtering params.
        search_query = name or domain or tag
        has_text_filters = search_query is not None

        try:
            if has_text_filters:
                # Route through /agents/search for server-side pre-filtering
                try:
                    response = await client.post("/agents/search", json={"query": search_query})
                    response.raise_for_status()
                except httpx.HTTPStatusError:
                    # Fallback to GET with query parameter
                    response = await client.get("/agents/search", params={"q": search_query})
                    response.raise_for_status()
            else:
                # No text filters — fetch all and filter client-side
                response = await client.get("/agents")
                response.raise_for_status()

            data = response.json()
            agents_data = data if isinstance(data, list) else data.get("agents", [])

            # Apply all filters client-side for correctness
            for agent_data in agents_data:
                agent = AgentDNSRecord.from_dict(agent_data)

                if not self._agent_matches_filters(
                    agent,
                    name=name,
                    domain=domain,
                    tag=tag,
                    status=status,
                    capability_list=capability_list,
                    require_all_capabilities=require_all_capabilities,
                    fuzzy_match=fuzzy_match,
                ):
                    continue

                yield agent
        except (httpx.ConnectError, httpx.TimeoutException) as e:
            raise self._map_http_error(e) from e
        except Exception as e:
            raise self._map_http_error(e) from e


@asynccontextmanager
async def discover_and_connect(
    agent: str | AgentDNSRecord,
    dns_base_url: str,
    dns_timeout: float | None = None,
    dns_headers: dict[str, str] | None = None,
    allowed_host_suffixes: list[str] | None = None,
    trust_dns_urls: bool = False,
    **client_kwargs: Any,
) -> AsyncIterator[Client]:
    """Discover agent via DNS and create a connected client.

    This is a convenience function that combines DNS discovery with client creation.
    It automatically handles cleanup when exiting the context.

    Args:
        agent: Agent name (str) or AgentDNSRecord object. If an AgentDNSRecord is provided,
            it will be used directly without DNS lookup.
        dns_base_url: Base URL of the DNS service
        dns_timeout: Timeout for DNS service requests
        dns_headers: Headers for DNS service requests
        allowed_host_suffixes: Optional hostname suffix allowlist for the discovered
            agent URL before client construction.
        trust_dns_urls: Forwarded as ``trust_url``/``trust_dns_urls`` to
            :meth:`AgentDNSRecord.to_client` (or :class:`AgentDNSClient`, for
            a name lookup). Defaults to False — see
            ``AgentDNSRecord.to_client``'s "Trust is opt-in, not the default"
            for why: the golden path to a NovaOS agent is gateway-mediated,
            not a direct dial to whatever Agent-DNS returns, so this
            convenience function does not default to enabling one. Pass True
            only when the caller knowingly needs a direct dial.
        **client_kwargs: Additional arguments to pass to Client constructor
            (e.g., timeout, headers, auth)

    Yields:
        Client instance connected to the discovered agent

    Raises:
        NovaOSDNSAgentNotFoundError: If agent is not found in DNS service
        NovaOSDNSConnectionError: If connection to DNS service fails
        NovaOSDNSError: For other DNS service errors

    Example:
        ```python
        async with discover_and_connect(
            agent="joke-agent",
            dns_base_url="http://agent-dns.example.internal:8000"
        ) as client:
            task = await client.send_message("Hello", skill_id="joke")
            print(task.artifacts[0].text if task.artifacts else task.status.state)
        ```

        Discovery is what supplies the agent's URL, and under A2A that URL *is*
        the agent — the returned client addresses it and nothing else, so no
        call inside the block names an agent again.
    """
    if isinstance(agent, AgentDNSRecord):
        client = agent.to_client(
            allowed_host_suffixes=allowed_host_suffixes,
            trust_url=trust_dns_urls,
            **client_kwargs,
        )
    else:
        async with AgentDNSClient(
            base_url=dns_base_url,
            timeout=dns_timeout,
            headers=dns_headers,
            allowed_host_suffixes=allowed_host_suffixes,
            trust_dns_urls=trust_dns_urls,
        ) as dns_client:
            client = await dns_client.get_agent_client(agent, **client_kwargs)
    async with client:
        yield client
