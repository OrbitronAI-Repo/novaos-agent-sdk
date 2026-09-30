"""Platform execution context utilities for NovaOS agents.

Provides helpers to extract the execution context injected by the novaos-auth-proxy
sidecar and to build outbound headers for agent-to-agent or agent-to-MCP calls.

The auth-proxy sidecar validates RS256 execution tokens and injects the following
headers into every request that reaches the agent container:

- ``X-NovaOS-User-Id``
- ``X-NovaOS-Tenant-Id``
- ``X-NovaOS-Run-Id``
- ``X-NovaOS-Agent-Id``
- ``X-NovaOS-Instance-Id``
- ``X-NovaOS-Execution-Token`` (the original Bearer token, for outbound calls)

Example:
    ```python
    from novaos_client_sdk.context import ExecutionContext

    # Inside an A2A agent handler
    ctx = ExecutionContext.from_headers(request.headers)
    print(ctx.user_id, ctx.run_id)

    # Build outbound headers for calling another agent or MCP Gateway
    outbound = ctx.to_auth_headers()
    # outbound == {"Authorization": "Bearer <token>"}
    ```
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from novaos_client_sdk.memory import AssembleResult, MemoryClient

NOVAOS_EXECUTION_TOKEN_ENV = "NOVAOS_EXECUTION_TOKEN"

# Header name constants
HEADER_USER_ID = "X-NovaOS-User-Id"
HEADER_TENANT_ID = "X-NovaOS-Tenant-Id"
HEADER_RUN_ID = "X-NovaOS-Run-Id"
HEADER_AGENT_ID = "X-NovaOS-Agent-Id"
HEADER_INSTANCE_ID = "X-NovaOS-Instance-Id"
HEADER_EXECUTION_TOKEN = "X-NovaOS-Execution-Token"


def _get_header(headers: Any, name: str) -> str | None:
    """Get a header value, supporting both dict and framework-specific header objects.

    Handles case-insensitive lookup for dict-like objects and falls back to
    ``get()`` for framework headers (e.g., Starlette, Flask).

    Args:
        headers: Header mapping (dict, starlette.Headers, etc.)
        name: Header name to look up.

    Returns:
        Header value or None if not present.
    """
    # Try direct dict access first
    if isinstance(headers, dict):
        # Case-insensitive lookup for plain dicts
        name_lower = name.lower()
        for key, value in headers.items():
            if key.lower() == name_lower:
                return str(value) if value is not None else None
        return None

    # Framework headers (Starlette, Flask, etc.) typically support .get()
    if hasattr(headers, "get"):
        result = headers.get(name)
        return str(result) if result is not None else None

    return None


@dataclass
class ExecutionContext:
    """Typed representation of the NovaOS execution context.

    Populated from the ``X-NovaOS-*`` headers that the auth-proxy sidecar injects
    after validating the RS256 execution token.

    Attributes:
        user_id: The authenticated user ID (from token ``user_id`` claim).
        tenant_id: The tenant/organization ID (from token ``tenant_id`` claim).
        run_id: The current execution run ID (from token ``run_id`` claim).
        agent_id: The target agent ID (from token ``agent_id`` claim).
        instance_id: The agent instance/pod ID (from token ``instance_id`` claim).
        execution_token: The raw RS256 execution token for outbound calls.
    """

    user_id: str | None = None
    tenant_id: str | None = None
    run_id: str | None = None
    agent_id: str | None = None
    instance_id: str | None = None
    execution_token: str | None = None

    @classmethod
    def from_headers(cls, headers: Any) -> ExecutionContext:
        """Extract execution context from request headers.

        Works with plain dicts, Starlette ``Headers``, Flask ``EnvironHeaders``,
        or any object that supports ``get(name)`` or dict-style iteration.

        If ``X-NovaOS-Execution-Token`` is absent from the headers (e.g. the
        agent is running outside the auth-proxy sidecar), the method falls back
        to the ``NOVAOS_EXECUTION_TOKEN`` environment variable before returning
        ``None``.  This allows local-dev and test-harness usage without the
        auth-proxy while keeping production behaviour unchanged.

        Args:
            headers: Request headers mapping.

        Returns:
            ExecutionContext populated from the ``X-NovaOS-*`` headers.

        Example:
            ```python
            # FastAPI / Starlette
            @app.post("/runs")
            async def handle_run(request: Request):
                ctx = ExecutionContext.from_headers(request.headers)
                print(f"User: {ctx.user_id}, Run: {ctx.run_id}")
            ```
        """
        token = _get_header(headers, HEADER_EXECUTION_TOKEN) or os.environ.get(
            NOVAOS_EXECUTION_TOKEN_ENV
        )
        return cls(
            user_id=_get_header(headers, HEADER_USER_ID),
            tenant_id=_get_header(headers, HEADER_TENANT_ID),
            run_id=_get_header(headers, HEADER_RUN_ID),
            agent_id=_get_header(headers, HEADER_AGENT_ID),
            instance_id=_get_header(headers, HEADER_INSTANCE_ID),
            execution_token=token,
        )

    def require_token(self) -> str:
        """Return the execution token, raising if it is absent.

        Use this instead of reading ``execution_token`` directly whenever an
        authenticated outbound call is about to be made.  It surfaces the
        missing-token problem at the earliest possible point with an actionable
        error message rather than failing later with a cryptic 401.

        Returns:
            The non-empty execution token string.

        Raises:
            MissingExecutionTokenError: When ``execution_token`` is ``None`` or
                empty.
        """
        from novaos_client_sdk.exceptions import MissingExecutionTokenError

        if not self.execution_token:
            raise MissingExecutionTokenError()
        return self.execution_token

    def to_auth_headers(self) -> dict[str, str]:
        """Build outbound ``Authorization`` header from the execution token.

        Use this when your agent needs to call another agent (via A2A) or
        the MCP Gateway. The target's auth-proxy will validate the token.

        Returns:
            Dict with ``Authorization: Bearer <token>`` if a token is available,
            otherwise an empty dict.

        Example:
            ```python
            ctx = ExecutionContext.from_headers(request.headers)
            headers = ctx.to_auth_headers()
            # Pass headers to SDK client or httpx request
            async with discover_and_connect(
                agent="other-agent",
                dns_base_url=dns_url,
                headers=headers,
            ) as client:
                task = await client.send_message(text)
            ```
        """
        if self.execution_token:
            return {"Authorization": f"Bearer {self.execution_token}"}
        return {}

    @property
    def memory(self) -> MemoryNamespace:
        """Access the memory namespace for this execution context.

        Returns a MemoryNamespace that lazily initializes a MemoryClient
        using the execution token from this context.

        Example:
            ```python
            ctx = ExecutionContext.from_headers(request.headers)
            ns = ctx.memory
            # ns.context is pre-fetched context (set by Agent Runtime, or None)
            # For client operations, use MemoryClient directly
            ```
        """
        if not hasattr(self, "_memory_ns") or self._memory_ns is None:
            self._memory_ns = MemoryNamespace(self)
        return self._memory_ns  # type: ignore[return-value]

    def to_context_headers(self) -> dict[str, str]:
        """Build a complete set of ``X-NovaOS-*`` headers for outbound propagation.

        Useful when you need to forward the full execution context (not just
        the token) to a downstream service.

        Returns:
            Dict of ``X-NovaOS-*`` headers with non-None values.
        """
        headers: dict[str, str] = {}
        if self.user_id:
            headers[HEADER_USER_ID] = self.user_id
        if self.tenant_id:
            headers[HEADER_TENANT_ID] = self.tenant_id
        if self.run_id:
            headers[HEADER_RUN_ID] = self.run_id
        if self.agent_id:
            headers[HEADER_AGENT_ID] = self.agent_id
        if self.instance_id:
            headers[HEADER_INSTANCE_ID] = self.instance_id
        if self.execution_token:
            headers[HEADER_EXECUTION_TOKEN] = self.execution_token
        return headers


class MemoryNamespace:
    """Memory operations namespace attached to an ExecutionContext.

    Provides a ``context`` property for pre-fetched memory (set by Agent Runtime
    before the agent handler runs) and a ``client()`` factory for creating
    a MemoryClient bound to this execution context.
    """

    def __init__(self, ctx: ExecutionContext) -> None:
        self._ctx = ctx
        self._prefetched: AssembleResult | None = None

    @property
    def context(self) -> AssembleResult | None:
        """Pre-fetched context assembled by Agent Runtime before agent code runs.

        Returns None if memory pre-fetch is disabled or the Agent Runtime did
        not inject context.
        """
        return self._prefetched

    @context.setter
    def context(self, value: AssembleResult | None) -> None:
        self._prefetched = value

    def client(self, **kwargs: Any) -> MemoryClient:
        """Create a MemoryClient bound to this execution context.

        The client inherits the execution token for authentication.

        Returns:
            MemoryClient configured with auth headers from this context.

        Raises:
            MissingExecutionTokenError: When no execution token is available.
        """
        from novaos_client_sdk.memory import MemoryClient

        self._ctx.require_token()
        return MemoryClient(
            headers=self._ctx.to_auth_headers(),
            **kwargs,
        )
