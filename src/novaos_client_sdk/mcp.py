"""MCP Gateway client for tool discovery and execution.

Provides a lightweight client for communicating with the NovaOS MCP Gateway
using JSON-RPC 2.0 over HTTP. The MCP Gateway acts as a proxy between agents
and MCP servers, handling session management and tool routing.

Protocol reference:
- ``tools/list`` — Discover available tools (optionally filtered by query)
- ``tools/call`` — Execute a tool by qualified name

The MCP Gateway uses ``Mcp-Session-Id`` headers for session affinity with
downstream MCP servers (5-minute TTL).

Example:
    ```python
    from novaos_client_sdk.context import ExecutionContext
    from novaos_client_sdk.mcp import MCPClient

    ctx = ExecutionContext.from_headers(request.headers)

    async with MCPClient(
        base_url=os.environ["MCP_GATEWAY_URL"],
        headers=ctx.to_auth_headers(),
    ) as mcp:
        tools = await mcp.tools_list(query="affirmations")
        result = await mcp.tools_call(
            "novaos.affirmations.generate_affirmation",
            arguments={"theme": "motivation"},
        )
        print(result.content)
    ```
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Any

from novaos_client_sdk._http import httpx
from novaos_client_sdk.exceptions import (
    NovaOSConnectionError,
    NovaOSError,
    NovaOSServerError,
)

logger = logging.getLogger(__name__)

# JSON-RPC 2.0 version constant
_JSONRPC_VERSION = "2.0"

# Default MCP Gateway env var (set in agent pod spec)
MCP_GATEWAY_URL_ENV = "MCP_GATEWAY_URL"


@dataclass
class MCPTool:
    """Descriptor for an MCP tool returned by ``tools/list``.

    Attributes:
        name: Fully qualified tool name (e.g., ``novaos.affirmations.generate_affirmation``).
        description: Human-readable description of what the tool does.
        input_schema: JSON Schema describing the tool's expected input parameters.
    """

    name: str
    description: str | None = None
    input_schema: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MCPTool":
        """Create from a JSON-RPC result entry."""
        return cls(
            name=data["name"],
            description=data.get("description"),
            input_schema=data.get("inputSchema") or data.get("input_schema"),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "inputSchema": self.input_schema,
        }


@dataclass
class MCPToolResult:
    """Result of an MCP ``tools/call`` invocation.

    Attributes:
        content: The tool's response content (list of content blocks or a single value).
        is_error: Whether the tool execution resulted in an error.
        metadata: Additional metadata from the response.
    """

    content: list[dict[str, Any]] = field(default_factory=list)
    is_error: bool = False
    metadata: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MCPToolResult":
        """Create from a JSON-RPC result."""
        content = data.get("content", [])
        if not isinstance(content, list):
            content = [{"type": "text", "text": str(content)}]
        return cls(
            content=content,
            is_error=data.get("isError", False),
            metadata={k: v for k, v in data.items() if k not in ("content", "isError")},
        )

    @property
    def text(self) -> str | None:
        """Extract the first text content block, if any."""
        for block in self.content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text")
        return None


class MCPClient:
    """Async client for the NovaOS MCP Gateway.

    Communicates via JSON-RPC 2.0 over HTTP. Manages ``Mcp-Session-Id``
    for session affinity with downstream MCP servers.

    Args:
        base_url: MCP Gateway URL. If None, reads from ``MCP_GATEWAY_URL`` env var.
        headers: Additional HTTP headers (typically includes ``Authorization``).
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> None:
        if base_url is None:
            base_url = os.getenv(MCP_GATEWAY_URL_ENV)
            if not base_url:
                raise NovaOSError(
                    f"MCP Gateway URL not provided and '{MCP_GATEWAY_URL_ENV}' "
                    "environment variable is not set."
                )
        self._base_url = base_url.rstrip("/")
        self._headers = headers.copy() if headers else {}
        self._timeout = timeout
        self._session_id: str | None = None
        self._client: httpx.AsyncClient | None = None
        self._request_id = 0

    async def __aenter__(self) -> "MCPClient":
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
        """Ensure the HTTP client is initialized."""
        if not self._client:
            raise NovaOSError(
                "MCPClient is not initialized. Use async context manager or call __aenter__() first."
            )
        return self._client

    def _next_request_id(self) -> int:
        """Generate the next JSON-RPC request ID."""
        self._request_id += 1
        return self._request_id

    def _build_request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Build a JSON-RPC 2.0 request envelope.

        Args:
            method: JSON-RPC method name (e.g., ``tools/list``).
            params: Optional parameters dict.

        Returns:
            JSON-RPC 2.0 request dict.
        """
        request: dict[str, Any] = {
            "jsonrpc": _JSONRPC_VERSION,
            "id": self._next_request_id(),
            "method": method,
        }
        if params is not None:
            request["params"] = params
        return request

    def _build_headers(self) -> dict[str, str]:
        """Build request headers including session ID if available."""
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    def _update_session(self, response: httpx.Response) -> None:
        """Track the session ID from response headers."""
        session_id = response.headers.get("Mcp-Session-Id")
        if session_id:
            self._session_id = session_id

    async def _send_rpc(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send a JSON-RPC 2.0 request and return the result.

        Args:
            method: JSON-RPC method name.
            params: Optional parameters.

        Returns:
            The ``result`` field from the JSON-RPC response.

        Raises:
            NovaOSConnectionError: If connection to the MCP Gateway fails.
            NovaOSServerError: If the JSON-RPC response contains an error.
            NovaOSError: For other errors.
        """
        client = self._ensure_client()
        request_body = self._build_request(method, params)
        extra_headers = self._build_headers()

        try:
            response = await client.post("/", json=request_body, headers=extra_headers)
            response.raise_for_status()
            self._update_session(response)
        except httpx.ConnectError as e:
            raise NovaOSConnectionError(
                f"Failed to connect to MCP Gateway: {e}",
                details={"original_error": type(e).__name__},
            ) from e
        except httpx.TimeoutException as e:
            raise NovaOSConnectionError(
                f"MCP Gateway request timed out: {e}",
                details={"original_error": type(e).__name__},
            ) from e
        except httpx.HTTPStatusError as e:
            raise NovaOSServerError(
                f"MCP Gateway returned HTTP {e.response.status_code}: {e}",
                status_code=e.response.status_code,
                details={"original_error": type(e).__name__},
            ) from e

        data = response.json()

        # Handle JSON-RPC error response
        if "error" in data:
            error = data["error"]
            code = error.get("code", -1)
            message = error.get("message", "Unknown JSON-RPC error")
            raise NovaOSServerError(
                f"MCP Gateway JSON-RPC error ({code}): {message}",
                details={"jsonrpc_error_code": code, "jsonrpc_error_data": error.get("data")},
            )

        return data.get("result")

    async def tools_list(self, query: str | None = None) -> list[MCPTool]:
        """Discover available tools from the MCP Gateway.

        Args:
            query: Optional query string to filter tools by name or description.

        Returns:
            List of available MCPTool descriptors.

        Raises:
            NovaOSConnectionError: If connection to the MCP Gateway fails.
            NovaOSServerError: If the gateway returns an error.

        Example:
            ```python
            async with MCPClient(headers=ctx.to_auth_headers()) as mcp:
                tools = await mcp.tools_list(query="affirmations")
                for tool in tools:
                    print(f"{tool.name}: {tool.description}")
            ```
        """
        params: dict[str, Any] = {}
        if query is not None:
            params["query"] = query

        result = await self._send_rpc("tools/list", params or None)

        # Result should be {"tools": [...]} per MCP spec
        tools_data = result if isinstance(result, list) else result.get("tools", [])
        return [MCPTool.from_dict(t) for t in tools_data]

    async def tools_call(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
    ) -> MCPToolResult:
        """Execute a tool via the MCP Gateway.

        Args:
            tool_name: Fully qualified tool name
                (e.g., ``novaos.affirmations.generate_affirmation``).
            arguments: Tool input arguments matching the tool's input schema.

        Returns:
            MCPToolResult with the tool's response.

        Raises:
            NovaOSConnectionError: If connection to the MCP Gateway fails.
            NovaOSServerError: If the gateway or tool returns an error.

        Example:
            ```python
            async with MCPClient(headers=ctx.to_auth_headers()) as mcp:
                result = await mcp.tools_call(
                    "novaos.affirmations.generate_affirmation",
                    arguments={"theme": "motivation"},
                )
                print(result.text)
            ```
        """
        params: dict[str, Any] = {"name": tool_name}
        if arguments is not None:
            params["arguments"] = arguments

        result = await self._send_rpc("tools/call", params)

        return MCPToolResult.from_dict(result if isinstance(result, dict) else {"content": result})

    @property
    def session_id(self) -> str | None:
        """The current MCP session ID, if established."""
        return self._session_id
