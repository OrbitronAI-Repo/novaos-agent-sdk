"""A2A HTTP+JSON (REST) transport for :class:`novaos_client_sdk.client.Client`.

A2A v1.0's second JSON-shaped binding, alongside JSON-RPC
(``_a2a.py``): the same request/response bodies, but addressed by Google
AIP-style REST paths (``GET /tasks/{id}``, ``POST /message:send``) rather than
one endpoint dispatched by a JSON-RPC ``method`` field.  Exists so this SDK can
call a conformant A2A agent that only advertises this binding — which, per the
protocol's own design, is a legitimate agent this client should be able to
reach, not a gap.

**Paths verified against the reference implementation.**  The A2A spec defines
this binding in terms of the same proto service as gRPC (§"HTTP JSON/REST
Transport Binding"), rendered through ``google.api.http`` annotations. Rather
than re-deriving them from the spec's binary options, the verb/path table
below was confirmed against the reference ``a2a-sdk``'s own
``a2a/client/transports/rest.py``, itself conformance-tested against a real
server in ``tests/test_a2a_v1_conformance.py`` — which this ticket extends
with a REST peer.

**Path-variable field names came from this SDK's own params, not a guess.**
The task/config id keys a path pulls out of ``params`` (``"id"``, ``"taskId"``)
are exactly what ``client.py`` already puts there for every operation — e.g.
``Client.get_task_push_notification_config``'s
``params = {"id": config_id, "taskId": task_id}``.

**Content type.**  Spec v1.0.1 prefers ``application/a2a+json`` on this
binding; the JSON-RPC transport's ``application/json`` is unaffected and stays
as it was — see ``_a2a.py``.

**Errors.**  A2A errors on this binding arrive as an HTTP status plus a
``google.rpc.Status``-shaped JSON body wrapped under an ``"error"`` key
(``{"error": {"code", "message", "details"}}`` — confirmed against a real
server, not assumed), not a JSON-RPC ``error`` object.  A ``details[]`` entry
carrying a structured
``ErrorInfo`` (``@type: type.googleapis.com/google.rpc.ErrorInfo``) is
translated through :data:`novaos_client_sdk._transport.A2A_REASON_TO_JSONRPC_CODE`
into the same numeric code space JSON-RPC uses, so the same exception type
comes out regardless of binding.  A response with no such structured detail —
a third-party peer that only sets an HTTP status — falls back to
``response.raise_for_status()``, which :meth:`Client._map_exception
<novaos_client_sdk.client.Client._map_exception>` already maps by status code.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from typing import Any, Final

from novaos_client_sdk._a2a import SSE_CONTENT_TYPE, iter_sse_data, sse_data
from novaos_client_sdk._http import httpx
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

__all__ = ["A2A_JSON_CONTENT_TYPE", "RestTransport"]

#: Content type spec v1.0.1 prefers for the HTTP+JSON binding (§"HTTP JSON/REST
#: Transport Binding").  Distinct from the JSON-RPC transport's plain
#: ``application/json``, which is unaffected by this preference.
A2A_JSON_CONTENT_TYPE: Final = "application/a2a+json"

#: ``@type`` value identifying a structured ``ErrorInfo`` detail in a
#: ``google.rpc.Status`` error body.
_ERROR_INFO_TYPE: Final = "type.googleapis.com/google.rpc.ErrorInfo"

#: One entry per ``METHOD_*`` this SDK's ``Client`` calls: HTTP verb and a
#: function splitting ``params`` into ``(path, remaining_params)`` — path
#: variables are removed from what's left, which becomes the JSON body (POST)
#: or query string (GET).  Verified against the reference REST transport; see
#: this module's docstring.
_PathBuilder = Callable[[dict[str, Any]], tuple[str, dict[str, Any]]]


def _no_path_vars(path: str) -> _PathBuilder:
    def build(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        return path, dict(params)

    return build


def _task_path(suffix: str = "") -> _PathBuilder:
    def build(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        remaining = dict(params)
        task_id = remaining.pop("id")
        return f"/tasks/{task_id}{suffix}", remaining

    return build


def _push_config_collection_path(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    remaining = dict(params)
    task_id = remaining.pop("taskId")
    return f"/tasks/{task_id}/pushNotificationConfigs", remaining


def _push_config_item_path(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    remaining = dict(params)
    task_id = remaining.pop("taskId")
    config_id = remaining.pop("id")
    return f"/tasks/{task_id}/pushNotificationConfigs/{config_id}", remaining


#: ``METHOD_*`` -> ``(verb, path_builder, is_streaming)``.
_ROUTES: Final[dict[str, tuple[str, _PathBuilder, bool]]] = {
    METHOD_SEND_MESSAGE: ("POST", _no_path_vars("/message:send"), False),
    METHOD_SEND_STREAMING_MESSAGE: ("POST", _no_path_vars("/message:stream"), True),
    METHOD_GET_TASK: ("GET", _task_path(), False),
    METHOD_LIST_TASKS: ("GET", _no_path_vars("/tasks"), False),
    METHOD_CANCEL_TASK: ("POST", _task_path(":cancel"), False),
    METHOD_SUBSCRIBE_TO_TASK: ("POST", _task_path(":subscribe"), True),
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG: ("POST", _push_config_collection_path, False),
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG: ("GET", _push_config_item_path, False),
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS: ("GET", _push_config_collection_path, False),
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG: ("DELETE", _push_config_item_path, False),
}


def _route(method: str) -> tuple[str, _PathBuilder, bool]:
    try:
        return _ROUTES[method]
    except KeyError:
        raise NovaOSError(f"RestTransport has no route for A2A method {method!r}.") from None


def _error_dict_from_body(body: Any) -> dict[str, Any] | None:
    """Read a JSON-RPC-shaped error dict out of a REST error body.

    A conformant peer wraps the ``google.rpc.Status`` object under an
    ``"error"`` key (``{"error": {"code", "message", "details"}}``) — confirmed
    against the reference ``a2a-sdk``'s own REST server, not assumed from the
    ``google.rpc.Status`` proto definition alone, which does not itself dictate
    this envelope. A body with no ``"error"`` key is read as the status object
    directly, tolerating a peer that omits the wrapper.

    Returns ``None`` when the body carries no structured ``ErrorInfo`` detail
    this SDK recognizes — the caller falls back to the HTTP status in that
    case, rather than fabricating a code that was never actually reported.
    """
    if not isinstance(body, dict):
        return None
    status = body.get("error", body)
    if not isinstance(status, dict):
        return None
    details = status.get("details")
    if not isinstance(details, list):
        return None
    for detail in details:
        if not isinstance(detail, dict) or detail.get("@type") != _ERROR_INFO_TYPE:
            continue
        reason = detail.get("reason")
        if not isinstance(reason, str):
            continue
        code = A2A_REASON_TO_JSONRPC_CODE.get(reason)
        if code is None:
            continue
        return {
            "code": code,
            "message": str(status.get("message") or reason),
            "data": detail.get("metadata"),
        }
    return None


class RestTransport(ClientTransport):
    """One HTTP connection to one A2A agent, speaking the HTTP+JSON binding.

    Mirrors :class:`~novaos_client_sdk._a2a.A2ATransport`'s shape (own
    ``httpx.AsyncClient`` for the transport's lifetime, the same
    ``A2A-Version`` construction-time header, the same SSE machinery for the
    two streaming methods) — the difference is entirely in how ``method`` maps
    onto a request; see this module's docstring.
    """

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Initialize the transport.

        Args:
            base_url: Base URL of the agent's HTTP+JSON interface.
            timeout: Request timeout in seconds, applied to non-streaming
                calls — see :meth:`stream`.
            headers: Additional headers, including any the auth provider
                supplied.
        """
        self._base_url = base_url
        self._timeout = timeout
        self._headers = {
            "Content-Type": A2A_JSON_CONTENT_TYPE,
            "Accept": A2A_JSON_CONTENT_TYPE,
            A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION,
            **(headers or {}),
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> RestTransport:
        """Open the underlying HTTP client."""
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._headers,
        )
        return self

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def is_open(self) -> bool:
        """Is this transport inside its context manager?"""
        return self._client is not None

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("RestTransport used outside its context manager.")
        return self._client

    async def _raise_for_response_error(
        self, response: httpx.Response, task_id: str | None
    ) -> None:
        """Raise the right exception for a non-2xx response, or return.

        Tries the structured ``ErrorInfo`` path first; falls back to a plain
        ``httpx.HTTPStatusError`` (via ``raise_for_status``) when the body
        carries nothing this SDK can map to a specific A2A error, which
        :meth:`Client._map_exception <novaos_client_sdk.client.Client._map_exception>`
        already handles by HTTP status.
        """
        if response.status_code < 400:
            return
        try:
            body = response.json()
        except ValueError:
            body = None
        error = _error_dict_from_body(body)
        if error is not None:
            raise_for_jsonrpc_error(error, task_id)
        response.raise_for_status()

    async def call(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make one non-streaming REST call and return its bare result value."""
        client = self._require_client()
        verb, build_path, streaming = _route(method)
        if streaming:
            raise NovaOSError(f"{method} is a streaming method; use stream() instead.")

        path, body = build_path(params)
        # GET/DELETE carry any remaining fields (e.g. `tenant`) as query
        # params — neither verb has a body on this binding; POST carries them
        # as the JSON body.
        response = await client.request(
            verb,
            path,
            params=body if verb in ("GET", "DELETE") else None,
            json=body if verb == "POST" and body else None,
            headers=headers,
        )
        await self._raise_for_response_error(response, task_id)
        if not response.content:
            return {}
        return response.json()

    async def stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[Any]:
        """Make one streaming REST call and yield each decoded SSE frame.

        No JSON-RPC envelope exists on this binding, so unlike
        :meth:`A2ATransport.stream <novaos_client_sdk._a2a.A2ATransport.stream>`
        a frame is never checked for an embedded ``error`` object — a domain
        error on this binding is instead a non-2xx response to the initial
        request, handled the same way :meth:`call` handles one.
        """
        client = self._require_client()
        verb, build_path, streaming = _route(method)
        if not streaming:
            raise NovaOSError(f"{method} is not a streaming method; use call() instead.")

        path, body = build_path(params)
        async with client.stream(
            verb,
            path,
            json=body if body else None,
            headers={**(headers or {}), "Accept": SSE_CONTENT_TYPE},
            timeout=httpx.Timeout(self._timeout, read=None),
        ) as response:
            if response.status_code >= 400:
                await response.aread()
                await self._raise_for_response_error(response, task_id)

            content_type = response.headers.get("content-type", "")
            if SSE_CONTENT_TYPE not in content_type:
                body_text = (await response.aread()).decode(
                    response.encoding or "utf-8", errors="replace"
                )
                for payload in sse_data(body_text) or [body_text.strip()]:
                    decoded = self._decode(payload)
                    if decoded is not None:
                        yield decoded
                return

            async for payload in iter_sse_data(response):
                decoded = self._decode(payload)
                if decoded is not None:
                    yield decoded

    @staticmethod
    def _decode(payload: str) -> Any:
        """Decode one SSE frame's data, or ``None`` if it is not valid JSON.

        Mirrors :meth:`A2ATransport._decode
        <novaos_client_sdk._a2a.A2ATransport._decode>`'s tolerance: one corrupt
        frame must not abort a run that is otherwise progressing.
        """
        try:
            return json.loads(payload)
        except ValueError:
            return None
