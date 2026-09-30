"""Abstract transport interface shared by every A2A protocol binding.

Extracted from what :class:`novaos_client_sdk._a2a.A2ATransport` already did
once a second and third binding (HTTP+JSON, gRPC) were added that need to
satisfy the same shape.

**Deliberately not the reference ``a2a-sdk``'s ``ClientTransport``.**  That ABC
has thirteen methods, one per A2A operation, because the reference ``Client``
dispatches by calling a named method on the transport.  This SDK's
:class:`~novaos_client_sdk.client.Client` already dispatches by JSON-RPC method
name (the ``METHOD_*`` constants in :mod:`novaos_client_sdk.a2a_jsonrpc`)
through a generic ``call``/``stream`` pair — that is exactly what
:class:`~novaos_client_sdk._a2a.A2ATransport` already implemented, so this
module extracts that shape rather than redesigning around the
reference SDK's.  A REST or gRPC transport translates the method name into
whatever its own wire shape needs (an HTTP verb and path, or a stub RPC)
internally; nothing above the transport needs to know which.

**Each transport owns its own connection lifecycle.**  Unlike the reference
SDK, where a transport's ``__aenter__`` is a no-op because the caller injects
an already-open ``httpx.AsyncClient``/``grpc.aio.Channel``, every transport
here opens its own connection on ``__aenter__`` and tears it down in
``close()`` — matching what :class:`~novaos_client_sdk._a2a.A2ATransport`
already did.  ``__aenter__`` is therefore abstract; ``__aexit__`` is not, since
"close whatever was opened" is the same one line for every binding.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, Final, NoReturn, Self

from novaos_client_sdk.exceptions import error_from_json_rpc

__all__ = ["A2A_REASON_TO_JSONRPC_CODE", "ClientTransport", "raise_for_jsonrpc_error"]

#: A2A error *reason* string -> JSON-RPC-style numeric code.
#:
#: Shared by every non-JSON-RPC transport: REST reports a structured domain
#: error as a ``google.rpc.Status`` body with an ``ErrorInfo`` detail carrying
#: a ``reason``; gRPC reports the same ``ErrorInfo`` on the trailing status.
#: Both translate that ``reason`` through this table to the numeric code
#: :func:`raise_for_jsonrpc_error` (via
#: :func:`~novaos_client_sdk.exceptions.error_from_json_rpc`) already knows how
#: to turn into the right exception — so a domain error raises the identical
#: exception type regardless of which binding carried it.
#:
#: Built from A2A's own error-mapping reference (the spec's per-error HTTP
#: status / gRPC status / reason table), not imported from any SDK, vendor
#: included — this is protocol-level knowledge, not vendor-specific code.
A2A_REASON_TO_JSONRPC_CODE: Final[dict[str, int]] = {
    "TASK_NOT_FOUND": -32001,
    "TASK_NOT_CANCELABLE": -32002,
    "PUSH_NOTIFICATION_NOT_SUPPORTED": -32003,
    "UNSUPPORTED_OPERATION": -32004,
    "CONTENT_TYPE_NOT_SUPPORTED": -32005,
    "INVALID_AGENT_RESPONSE": -32006,
    "EXTENDED_AGENT_CARD_NOT_CONFIGURED": -32007,
    "EXTENSION_SUPPORT_REQUIRED": -32008,
    "VERSION_NOT_SUPPORTED": -32009,
    "INVALID_PARAMS": -32602,
    "INVALID_REQUEST": -32600,
    "METHOD_NOT_FOUND": -32601,
    "INTERNAL_ERROR": -32603,
}


def raise_for_jsonrpc_error(error: dict[str, Any], task_id: str | None) -> NoReturn:
    """Turn a JSON-RPC-shaped ``error`` object into the right exception and raise it.

    Shared by every transport, not only the JSON-RPC one: A2A's error *codes*
    are transport-independent (spec's error-mapping reference), so a REST or
    gRPC transport that received a structured domain error translates its own
    wire shape — ``google.rpc.Status`` details for REST, an ``ErrorInfo`` on the
    gRPC status for gRPC — into this same ``{code, message, data}`` shape
    before calling this, and the exception a caller catches is identical
    regardless of which transport actually ran the call.

    ``error_from_json_rpc`` picks the class from *both* layers: A2A's own
    codes become :class:`~novaos_client_sdk.exceptions.A2AProtocolError`
    subclasses (so ``-32001`` arrives as ``TaskNotFoundError``), while the
    standard JSON-RPC codes become platform errors. An unrecognized code still
    arrives carrying the code it came with.
    """
    raise error_from_json_rpc(error, task_id=task_id)


class ClientTransport(ABC):
    """One connection to one A2A agent, over one protocol binding.

    Every A2A operation is dispatched through :meth:`call` (request/response)
    or :meth:`stream` (server-streaming) by JSON-RPC method name — see this
    module's docstring for why that is the shape rather than the reference
    SDK's one-method-per-operation ABC.  A concrete transport owns exactly the
    binding-specific plumbing: how ``method``/``params`` becomes a request on
    the wire, how a response becomes the bare result value, and how
    ``A2A-Version``/``A2A-Extensions`` negotiation is carried (an HTTP header
    for JSON-RPC/REST, gRPC metadata for gRPC).
    """

    @abstractmethod
    async def __aenter__(self) -> Self:
        """Enter the transport's context, opening its connection."""

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the transport's context, closing its connection."""
        await self.close()

    @property
    @abstractmethod
    def is_open(self) -> bool:
        """Is this transport inside its context manager?"""

    @abstractmethod
    async def call(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make one non-streaming call and return its bare result value.

        Args:
            method: An A2A v1.0 method name (PascalCase, e.g. ``"GetTask"`` —
                see the ``METHOD_*`` constants in
                :mod:`novaos_client_sdk.a2a_jsonrpc`), translated internally to
                whatever this binding's wire shape needs.
            params: The method's request object, proto3-JSON-shaped
                (``lowerCamelCase`` keys) regardless of binding.
            task_id: Task this call concerns, if any — carried onto a raised
                :class:`~novaos_client_sdk.exceptions.A2AProtocolError` so a
                caller handling, say, ``TaskNotFoundError`` knows which task
                was not found without re-reading the request.
            headers: Extra request-scoped headers for this call only, merged
                over the transport's own — e.g. a credential a
                :class:`~novaos_client_sdk.interceptors.ClientCallInterceptor`
                resolved fresh for this call.  On gRPC these become metadata
                tuples rather than HTTP headers; see the concrete transport.

        Returns:
            The result value, in the same proto3-JSON dict shape a JSON-RPC
            ``result`` would carry — so ``Client``'s ``Task.from_dict`` and
            friends need not know which transport ran the call.

        Raises:
            A2AProtocolError: If the agent answered with a structured A2A
                error, regardless of binding.
        """

    @abstractmethod
    def stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[Any]:
        """Make one streaming call and yield each decoded frame.

        Args: as :meth:`call`.

        Yields:
            Each frame's already-decoded payload, in the same shape
            :meth:`call` returns its result in.

        Raises:
            A2AProtocolError: If the agent sent a structured A2A error,
                whether in the initial response or mid-stream.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the transport's connection."""
