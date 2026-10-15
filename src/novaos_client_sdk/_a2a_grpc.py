"""A2A gRPC transport for :class:`novaos_client_sdk.client.Client`.

Behind the optional ``grpc`` extra (``pip install
novaos-client-sdk[grpc]``).  Unlike JSON-RPC and HTTP+JSON — both plain JSON,
which is why this SDK owns its own transport for them rather than depending on
``a2a-sdk`` (see ``_a2a.py``'s module docstring for that ADR) — gRPC's wire
format literally *is* protobuf, so some compiled ``.proto`` is unavoidable.
Rather than vendor and independently regenerate the identical schema
``a2a-sdk`` already publishes and versions, this module imports its
pre-generated ``a2a.types.a2a_pb2`` / ``a2a_pb2_grpc`` — the compiled message
and stub classes only, **never** ``a2a.client.*`` (its ``ClientTransport``,
``ClientFactory`` or ``Client``).  This transport's method dispatch, header
negotiation, SSRF integration and error mapping are all NovaOS's own code,
exactly as the ADR requires for the other two bindings — only the schema
artifact itself is borrowed, because there is no alternative source for it
that isn't this SDK re-deriving and re-maintaining the same ``.proto`` a2a-sdk
already tracks against the spec.

**Field names round-trip for free.**  This SDK's ``params`` dicts are already
proto3-JSON-shaped (``historyLength``, ``taskId``, …), which
``google.protobuf.json_format.ParseDict``/``MessageToDict`` read and write
directly — confirmed field-by-field against ``a2a.types.a2a_pb2.pyi`` at
implementation time (e.g. ``GetTaskRequest.__slots__ == ("tenant", "id",
"history_length")``), not assumed.

**Negotiation over gRPC metadata, not HTTP headers.**  ``A2A-Version`` and
``A2A-Extensions`` are HTTP headers on the other two bindings; gRPC has no
headers, only metadata (a list of lower-cased ``(key, value)`` tuples) — see
:meth:`GrpcTransport._build_metadata`.

**Errors.**  A structured domain error arrives as a ``google.rpc.ErrorInfo``
unpacked from the trailing gRPC status, translated through
:data:`novaos_client_sdk._transport.A2A_REASON_TO_JSONRPC_CODE` into the same
numeric code space JSON-RPC/REST use, so the same exception type comes out
regardless of binding.  A bare transport-level failure (the peer unreachable,
a deadline exceeded, …) carries no such structured detail; this transport lets
``grpc.aio.AioRpcError`` propagate for those, and
:meth:`Client._map_exception <novaos_client_sdk.client.Client._map_exception>`
maps it by ``grpc.StatusCode`` the same way it already maps
``httpx.HTTPStatusError`` by HTTP status — the layer beneath the protocol,
which is exactly what that method already owns.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any, Final

from novaos_client_sdk._idna import wire_hostname
from novaos_client_sdk._ssrf import resolve_checked
from novaos_client_sdk._transport import (
    A2A_REASON_TO_JSONRPC_CODE,
    ClientTransport,
    raise_for_jsonrpc_error,
)
from novaos_client_sdk.a2a_jsonrpc import (
    METHOD_CANCEL_TASK,
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_GET_TASK,
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS,
    METHOD_LIST_TASKS,
    METHOD_SEND_MESSAGE,
    METHOD_SEND_STREAMING_MESSAGE,
    METHOD_SUBSCRIBE_TO_TASK,
)
from novaos_client_sdk.a2a_types import A2A_PROTOCOL_VERSION, A2A_VERSION_HEADER
from novaos_client_sdk.exceptions import NovaOSError

__all__ = ["GrpcChannelFactory", "GrpcTransport"]

try:
    import grpc
    from a2a.types import a2a_pb2, a2a_pb2_grpc
    from google.protobuf.json_format import MessageToDict, ParseDict
    from google.protobuf.message import Message as ProtoMessage
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise NovaOSError(
        "GrpcTransport requires the 'grpc' extra: pip install 'novaos-client-sdk[grpc]'"
    ) from exc

try:
    from google.rpc import error_details_pb2
    from grpc_status import rpc_status
except ImportError:  # pragma: no cover - grpcio-status ships with the extra
    error_details_pb2 = None
    rpc_status = None

#: A caller-supplied factory building a channel for a gRPC target
#: (``host:port``, no scheme).  The default when unset is a sensible
#: insecure/secure choice based on the interface URL's scheme — see
#: :meth:`GrpcTransport.__aenter__`.  Set on
#: :attr:`~novaos_client_sdk.client.ClientConfig.grpc_channel_factory` for
#: custom credentials, interceptors, or channel options.
GrpcChannelFactory = Callable[[str], "grpc.aio.Channel"]

#: ``METHOD_*`` -> ``(stub RPC attribute name, request message class, is_streaming)``.
#: Confirmed against ``a2a.types.a2a_pb2_grpc.A2AServiceStub.__init__`` and
#: ``a2a_pb2.pyi`` at implementation time rather than assumed from the
#: JSON-RPC method names alone.
_ROUTES: Final[dict[str, tuple[str, type[ProtoMessage], bool]]] = {
    METHOD_SEND_MESSAGE: ("SendMessage", a2a_pb2.SendMessageRequest, False),
    METHOD_SEND_STREAMING_MESSAGE: ("SendStreamingMessage", a2a_pb2.SendMessageRequest, True),
    METHOD_GET_TASK: ("GetTask", a2a_pb2.GetTaskRequest, False),
    METHOD_LIST_TASKS: ("ListTasks", a2a_pb2.ListTasksRequest, False),
    METHOD_CANCEL_TASK: ("CancelTask", a2a_pb2.CancelTaskRequest, False),
    METHOD_SUBSCRIBE_TO_TASK: ("SubscribeToTask", a2a_pb2.SubscribeToTaskRequest, True),
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG: (
        "CreateTaskPushNotificationConfig",
        a2a_pb2.TaskPushNotificationConfig,
        False,
    ),
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG: (
        "GetTaskPushNotificationConfig",
        a2a_pb2.GetTaskPushNotificationConfigRequest,
        False,
    ),
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS: (
        "ListTaskPushNotificationConfigs",
        a2a_pb2.ListTaskPushNotificationConfigsRequest,
        False,
    ),
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG: (
        "DeleteTaskPushNotificationConfig",
        a2a_pb2.DeleteTaskPushNotificationConfigRequest,
        False,
    ),
}


def _route(method: str) -> tuple[str, type[ProtoMessage], bool]:
    try:
        return _ROUTES[method]
    except KeyError:
        raise NovaOSError(f"GrpcTransport has no route for A2A method {method!r}.") from None


def _split_host_port(target: str) -> tuple[str, str]:
    """Split ``host[:port]`` (``[v6]:port`` too) into ``(host, port)``; port may be ``""``."""
    if target.startswith("["):
        host, _, rest = target[1:].partition("]")
        return host, rest.lstrip(":")
    host, sep, port = target.rpartition(":")
    if sep and port.isdigit():
        return host, port
    return target, ""


def _parse_target(base_url: str) -> tuple[str, bool]:
    """Split a gRPC interface URL into ``(target, is_secure)``.

    A gRPC target is conventionally bare ``host:port`` — no scheme — but this
    SDK's ``AgentInterface.url`` may carry one (``grpc://``/``grpcs://``, or
    even ``http://``/``https://`` for an agent card that reuses the same
    scheme convention as its other interfaces). Any of those are accepted;
    a target with no scheme at all is treated as insecure, matching typical
    internal/dev gRPC usage.
    """
    if "://" not in base_url:
        return _wire_authority(base_url.rstrip("/")), False
    scheme, _, rest = base_url.partition("://")
    return _wire_authority(rest.rstrip("/")), scheme in ("https", "grpcs")


def _wire_authority(authority: str) -> str:
    """Rewrite the host of a ``host[:port]`` target into its IDNA wire form.

    gRPC's resolver does not encode a Unicode host itself, so it would never
    look up the name that :func:`~novaos_client_sdk.dns.validate_agent_url`
    checked. Handing it the same ASCII form keeps the validated and the dialled
    host identical.
    """
    if authority.startswith("["):  # bracketed IPv6 literal
        return authority
    host, sep, port = authority.rpartition(":")
    if not sep or not port.isdigit():
        host, sep, port = authority, "", ""
    return wire_hostname(host) + sep + port


def _error_info_from_status(
    exc: grpc.aio.AioRpcError,
) -> tuple[str | None, str, dict[str, Any] | None]:
    """Unpack a structured ``ErrorInfo`` off *exc*'s trailing status, if any.

    Returns ``(reason, message, metadata)``. ``reason`` is ``None`` when the
    status carries no such detail — a bare transport failure, which the
    caller re-raises as-is rather than fabricating a code that was never
    actually reported.
    """
    fallback_message = exc.details() or str(exc)
    if rpc_status is None or error_details_pb2 is None:
        return None, fallback_message, None
    try:
        status = rpc_status.from_call(exc)
    except Exception:
        return None, fallback_message, None
    if status is None:
        return None, fallback_message, None
    for detail in status.details:
        info = error_details_pb2.ErrorInfo()
        if detail.Is(info.DESCRIPTOR):
            detail.Unpack(info)
            return info.reason, status.message or fallback_message, dict(info.metadata)
    return None, status.message or fallback_message, None


def _raise_for_grpc_error(exc: grpc.aio.AioRpcError, task_id: str | None) -> None:
    """Raise the right exception for a failed gRPC call, or re-raise *exc*.

    A structured ``ErrorInfo`` reason maps to the same exception a JSON-RPC or
    REST peer reporting the identical domain error would raise. Anything else
    — the peer unreachable, a deadline exceeded, an unstructured internal
    error — is a transport-level failure, not a protocol one, so it is left
    for :meth:`Client._map_exception
    <novaos_client_sdk.client.Client._map_exception>` to classify, matching
    how that method already owns "HTTP statuses and socket failures" for the
    other two bindings.
    """
    reason, message, metadata = _error_info_from_status(exc)
    code = A2A_REASON_TO_JSONRPC_CODE.get(reason) if reason else None
    if code is not None:
        raise_for_jsonrpc_error({"code": code, "message": message, "data": metadata}, task_id)
    raise exc


class GrpcTransport(ClientTransport):
    """One gRPC channel to one A2A agent.

    Builds its own ``grpc.aio.Channel`` on :meth:`__aenter__` unless a
    ``channel_factory`` is supplied — matching how
    :class:`~novaos_client_sdk._a2a.A2ATransport` self-constructs its
    ``httpx.AsyncClient`` rather than requiring one be injected.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        channel_factory: GrpcChannelFactory | None = None,
        guard_connect_ips: bool = False,
        allow_private_ips: bool = False,
    ) -> None:
        """Initialize the transport.

        Args:
            base_url: The agent's gRPC interface URL or bare ``host:port``
                target — see :func:`_parse_target`.
            timeout: Request timeout in seconds, applied to non-streaming
                calls only — matching the other two transports, a stream is
                expected to stay open for as long as the task runs.
            headers: Additional metadata entries, lower-cased and merged into
                every call — see :meth:`_build_metadata`.
            channel_factory: Builds the channel for a given target, for
                custom credentials/interceptors/options. Defaults to a
                secure or insecure channel based on the target's scheme.
            guard_connect_ips: Resolve the host when the channel opens, refuse
                a private/loopback/link-local/reserved address, and dial that
                checked address, keeping the original host as the TLS server
                name and authority. Not applied when ``channel_factory`` builds
                the channel — the SDK cannot pin a channel it does not create.
            allow_private_ips: With ``guard_connect_ips``, skip the range check.
        """
        self._target, self._secure = _parse_target(base_url)
        self._timeout = timeout
        self._base_headers = {A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION, **(headers or {})}
        self._channel_factory = channel_factory
        self._guard_connect_ips = guard_connect_ips
        self._allow_private_ips = allow_private_ips
        self._channel: grpc.aio.Channel | None = None
        self._stub: a2a_pb2_grpc.A2AServiceStub | None = None

    async def __aenter__(self) -> GrpcTransport:
        """Open the underlying gRPC channel."""
        if self._channel_factory is not None:
            self._channel = self._channel_factory(self._target)
        else:
            target, options = await self._pinned_target()
            if self._secure:
                self._channel = grpc.aio.secure_channel(
                    target, grpc.ssl_channel_credentials(), options=options
                )
            else:
                self._channel = grpc.aio.insecure_channel(target, options=options)
        self._stub = a2a_pb2_grpc.A2AServiceStub(self._channel)
        return self

    async def _pinned_target(self) -> tuple[str, list[tuple[str, str]] | None]:
        """The channel target, pinned to a range-checked address when guarded.

        The original host stays as the TLS server name and the HTTP/2
        authority, so certificate verification still checks the name the
        caller asked for, not the address.
        """
        if not self._guard_connect_ips:
            return self._target, None
        host, port = _split_host_port(self._target)
        address = await resolve_checked(
            host,
            int(port) if port else (443 if self._secure else 80),
            allow_private_ips=self._allow_private_ips,
        )
        pinned = f"[{address}]" if ":" in address else address
        if port:
            pinned = f"{pinned}:{port}"
        options = [("grpc.default_authority", self._target)]
        if self._secure:
            options.append(("grpc.ssl_target_name_override", host))
        return pinned, options

    async def close(self) -> None:
        """Close the underlying gRPC channel."""
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
            self._stub = None

    @property
    def is_open(self) -> bool:
        """Is this transport inside its context manager?"""
        return self._channel is not None

    def _require_stub(self) -> a2a_pb2_grpc.A2AServiceStub:
        if self._stub is None:
            raise RuntimeError("GrpcTransport used outside its context manager.")
        return self._stub

    def _build_metadata(self, headers: dict[str, str] | None) -> list[tuple[str, str]]:
        """Build this call's gRPC metadata — the ``A2A-Version``/``A2A-Extensions``
        negotiation mechanism on this binding, where HTTP headers don't exist.

        Every entry is rebuilt fresh per call (unlike the HTTP transports,
        which set ``A2A-Version`` once as a client-level default): gRPC
        metadata is call-scoped, not connection-scoped. Per-call ``headers``
        (e.g. from a
        :class:`~novaos_client_sdk.interceptors.ClientCallInterceptor`)
        override the transport's own for the same key, matching the HTTP
        transports' merge-with-override semantics — gRPC has no such
        automatic merge, so it is done here, keyed case-insensitively via
        lower-casing (gRPC metadata convention).
        """
        merged: dict[str, str] = {k.lower(): v for k, v in self._base_headers.items()}
        merged.update((k.lower(), v) for k, v in (headers or {}).items())
        return list(merged.items())

    async def call(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make one unary gRPC call and return its bare result value."""
        stub = self._require_stub()
        rpc_name, request_cls, streaming = _route(method)
        if streaming:
            raise NovaOSError(f"{method} is a streaming method; use stream() instead.")

        request = ParseDict(params, request_cls(), ignore_unknown_fields=True)
        rpc = getattr(stub, rpc_name)
        try:
            response = await rpc(
                request,
                metadata=self._build_metadata(headers),
                timeout=self._timeout,
            )
        except grpc.aio.AioRpcError as exc:
            _raise_for_grpc_error(exc, task_id)
        return MessageToDict(response)

    async def stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[Any]:
        """Make one server-streaming gRPC call and yield each decoded response."""
        stub = self._require_stub()
        rpc_name, request_cls, streaming = _route(method)
        if not streaming:
            raise NovaOSError(f"{method} is not a streaming method; use call() instead.")

        request = ParseDict(params, request_cls(), ignore_unknown_fields=True)
        rpc = getattr(stub, rpc_name)
        call = rpc(request, metadata=self._build_metadata(headers))
        try:
            async for response in call:
                yield MessageToDict(response)
        except grpc.aio.AioRpcError as exc:
            _raise_for_grpc_error(exc, task_id)
