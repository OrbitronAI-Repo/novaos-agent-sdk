"""JSON-RPC 2.0 framing for A2A's primary transport binding.

A2A v1.0.1 ships three transport bindings; **JSON-RPC 2.0 over HTTPS is the
primary one**, and it is what NovaOS agent pods speak.  This module owns the
envelope, in both directions, kept separate from
:mod:`novaos_client_sdk.a2a_types` (the data model) and
:mod:`novaos_client_sdk.agent_builder` (the dispatch).

**Both directions, deliberately in one module.**  :func:`parse_request` /
:func:`success_response` / :func:`error_response` are the server half, used by
``AgentBuilder``; :func:`request_envelope` / :func:`parse_response` are the
client half, used by :class:`novaos_client_sdk.client.Client`.  They
live together because they are two halves of one contract — the ``jsonrpc``
literal, the batch-rejection rule and the id-echo rule have to agree, and a
second hand-rolled copy on the client side is precisely how they would stop
agreeing.

**Method names are PascalCase in v1.0.1.**  The spec's method-mapping reference
(§5.3) names the operations ``SendMessage``, ``GetTask``, ``CancelTask`` and so
on.  A2A **v0.3** used slash-separated names (``message/send``, ``tasks/get``);
those are not v1.0 names, and a v1.0 peer answers ``-32601 Method not found``
for them.  This matters because the ADR's own F2 concept-mapping table quotes
the v0.3 spelling — see the note in that ticket's PR.

**Error codes here are the JSON-RPC *transport* errors only** (``-32700`` …
``-32603``).  They mean "this was not a well-formed call", and are deliberately
not modelled as exceptions: A2A's *own* nine protocol errors (``-32001`` …
``-32009``) are what
:class:`novaos_client_sdk.exceptions.A2AProtocolError` covers, and conflating
the two would put "the JSON was malformed" in the same hierarchy as "that task
id is unknown".  Use :data:`novaos_client_sdk.exceptions.A2A_ERROR_TYPES` for
the latter.
"""

import json
from dataclasses import dataclass
from typing import Any, Final

__all__ = [
    "CONFIG_RETURN_IMMEDIATELY",
    "DEFAULT_MAX_BODY_BYTES",
    "INTERNAL_ERROR",
    "INVALID_PARAMS",
    "INVALID_REQUEST",
    "JSONRPC_VERSION",
    "METHOD_CANCEL_TASK",
    "METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_UNAVAILABLE_ERROR_CODES",
    "METHOD_GET_TASK",
    "METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_LIST_TASKS",
    "METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS",
    "METHOD_NOT_FOUND",
    "METHOD_SEND_MESSAGE",
    "METHOD_SEND_STREAMING_MESSAGE",
    "METHOD_SUBSCRIBE_TO_TASK",
    "PARSE_ERROR",
    "SEND_MESSAGE_RESULT_MESSAGE",
    "SEND_MESSAGE_RESULT_TASK",
    "A2A_JSONRPC_PATH",
    "JsonRpcRequest",
    "decode_request_body",
    "error_response",
    "parse_request",
    "parse_response",
    "request_envelope",
    "send_message_result",
    "success_response",
]

#: The only ``jsonrpc`` value JSON-RPC 2.0 permits.
JSONRPC_VERSION: Final = "2.0"

#: Where an A2A pod **server** mounts its JSON-RPC route, relative to its own
#: root — a documentation/parity constant, not a path this SDK's own client
#: transport joins onto a caller's ``base_url`` (see
#: :class:`~novaos_client_sdk._a2a.A2ATransport`'s constructor note:
#: doing that silently mutated a gateway-shaped ``base_url`` that already has a
#: path, turning a valid route into a 404).
#:
#: The pod root, not ACP's ``/runs``.  Both kinds of NovaOS pod put it there — a
#: Galaxy-scaffolded pod calls ``create_jsonrpc_routes(handler, "/")`` and an
#: ``AgentBuilder`` pod's route handler is mounted at the same place — and the
#: auth-proxy sidecar forwards the path through unchanged, so an execution token
#: is still validated ahead of the agent container.
#:
#: ⚠️ Must match ``A2A_JSONRPC_PATH`` in
#: the platform's A2A wire contract, which is the same constant for the
#: TypeScript callers.
A2A_JSONRPC_PATH: Final = "/"

# ── A2A v1.0.1 method names (spec §5.3, "Method Mapping Reference") ──────────
METHOD_SEND_MESSAGE: Final = "SendMessage"
METHOD_SEND_STREAMING_MESSAGE: Final = "SendStreamingMessage"
METHOD_GET_TASK: Final = "GetTask"
METHOD_LIST_TASKS: Final = "ListTasks"
METHOD_CANCEL_TASK: Final = "CancelTask"
METHOD_SUBSCRIBE_TO_TASK: Final = "SubscribeToTask"

# Push notification config methods.  The server half of all
# four landed earlier; this is what the client side calls.  Note
# `ListTaskPushNotificationConfigs` is **plural** — v0.3 named it singular,
# and F20 caught that the ADR's own description still carried that spelling.
METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG: Final = "CreateTaskPushNotificationConfig"
METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG: Final = "GetTaskPushNotificationConfig"
METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS: Final = "ListTaskPushNotificationConfigs"
METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG: Final = "DeleteTaskPushNotificationConfig"

# ── SendMessageResponse (spec v1.0.1) ───────────────────────────────────────
#
# `SendMessage` does NOT return a bare Task.  v1.0 defines the result as a
# `SendMessageResponse`, which is a protobuf `oneof` over two payloads, and
# proto3 JSON renders a oneof as whichever member is set.  So the JSON-RPC
# `result` is `{"task": {...}}` or `{"message": {...}}`, never the object
# itself.  Returning the Task bare is A2A **v0.3**'s shape — the same class of
# drift as ADR finding F18, and a strict v1.0 peer rejects it.
SEND_MESSAGE_RESULT_TASK: Final = "task"
SEND_MESSAGE_RESULT_MESSAGE: Final = "message"

# `SendMessageConfiguration.returnImmediately` (spec v1.0.1).  Note this
# replaced v0.3's `configuration.blocking` and **the sense is inverted**: true
# means "do not wait", where `blocking: true` meant "do wait".  The default
# flipped with it, so a caller omitting the field is asking the server to block
# until the task is terminal or interrupted.
CONFIG_RETURN_IMMEDIATELY: Final = "returnImmediately"

# ── Standard JSON-RPC 2.0 transport errors ──────────────────────────────────
PARSE_ERROR: Final = -32700
INVALID_REQUEST: Final = -32600
METHOD_NOT_FOUND: Final = -32601
INVALID_PARAMS: Final = -32602
INTERNAL_ERROR: Final = -32603

#: Default cap on a raw request body :func:`decode_request_body` will attempt to
#: decode, in bytes.  Matches ``A2A_BODY_LIMIT`` in
#: the execution gateway's A2A route handler — the gateway already enforces this
#: figure ahead of every pod, so a pod checking the same number rejects an
#: oversize body the same way rather than spending a full ``json.loads`` on
#: megabytes of a request the gateway would have refused anyway.  A caller with
#: its own limit (e.g. a WSGI/ASGI server config) may pass a different value.
DEFAULT_MAX_BODY_BYTES: Final = 5 * 1024 * 1024

#: Codes meaning "this peer does not implement the method", as opposed to "the
#: call failed".
#:
#: Two codes for one condition because two kinds of pod answer it differently: an
#: ``AgentBuilder`` pod implements ``SendMessage`` only and reports A2A's own
#: ``-32004`` (``UnsupportedOperationError``), while a hand-rolled server that
#: simply does not route the method reports JSON-RPC's ``-32601``.  Neither is a
#: run failure — a caller degrades to ``SendMessage`` or to a ``GetTask`` poll.
#:
#: The two land in *different branches* of the exception hierarchy, deliberately
#: (``-32004`` is an :class:`~novaos_client_sdk.exceptions.A2AProtocolError`,
#: ``-32601`` a platform error), so branch on the code —
#: :func:`~novaos_client_sdk.exceptions.jsonrpc_error_code` reads it off either —
#: rather than on the exception type.
#:
#: ⚠️ Must match ``METHOD_UNAVAILABLE_ERROR_CODES`` in
#: the platform's A2A wire contract, which is the same set for the TypeScript
#: callers.  ``-32004`` is spelled out here rather than imported from
#: ``exceptions`` to keep this module free of that dependency; a test pins it to
#: :attr:`~novaos_client_sdk.exceptions.UnsupportedOperationError.error_code`.
METHOD_UNAVAILABLE_ERROR_CODES: Final[frozenset[int]] = frozenset({METHOD_NOT_FOUND, -32004})


@dataclass(frozen=True)
class JsonRpcRequest:
    """One well-formed JSON-RPC 2.0 request.

    ``id`` is typed loosely on purpose: JSON-RPC allows a string, a number, or
    ``null``, and a server must echo back whatever it was given rather than
    normalizing it.
    """

    method: str
    params: dict[str, Any]
    id: Any = None


def success_response(request_id: Any, result: Any) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 success response.

    Args:
        request_id: The ``id`` from the request, echoed back verbatim
        result: The method's result value

    Returns:
        A JSON-RPC 2.0 response object.
    """
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}


def error_response(
    request_id: Any,
    code: int,
    message: str,
    data: Any = None,
) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 error response.

    Args:
        request_id: The ``id`` from the request, or ``None`` when the request was
            unparseable enough that no id could be read
        code: JSON-RPC or A2A error code
        message: Short human-readable description
        data: Optional additional detail.  Omitted entirely when ``None``, since
            JSON-RPC treats the member's *presence* as meaningful.

    Returns:
        A JSON-RPC 2.0 response object.
    """
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "error": error}


def send_message_result(task: dict[str, Any] | None = None, message: Any = None) -> dict[str, Any]:
    """Wrap a Task or a Message as A2A's ``SendMessageResponse`` oneof.

    Exactly one of the two must be supplied — a protobuf oneof has exactly one
    member set, and emitting both would be rejected by a strict parser.

    Args:
        task: A serialized ``Task``
        message: A serialized ``Message``, for an agent replying without
            creating a task at all

    Returns:
        The ``result`` value for a ``SendMessage`` JSON-RPC response.

    Raises:
        ValueError: If both or neither payload is supplied.
    """
    if (task is None) == (message is None):
        raise ValueError("SendMessageResponse is a oneof: supply exactly one of task or message.")
    if task is not None:
        return {SEND_MESSAGE_RESULT_TASK: task}
    return {SEND_MESSAGE_RESULT_MESSAGE: message}


def decode_request_body(
    raw: bytes | str,
    *,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
) -> tuple[Any, dict[str, Any] | None]:
    """Decode a raw HTTP request body into a JSON value.

    This is the step a caller must take *before* :func:`parse_request` when it
    only has bytes off the wire, not an already-decoded object.  Kept separate
    from ``parse_request`` because the two can fail for different reasons that
    JSON-RPC 2.0 treats differently: a body that isn't valid JSON at all is
    ``PARSE_ERROR`` (``-32700``), while a body that decodes to valid JSON but
    isn't a well-formed JSON-RPC envelope is ``INVALID_REQUEST`` (``-32600``,
    handled by ``parse_request``). Collapsing the two into one error code is
    exactly the gap this function closes: a hand-rolled route that calls
    ``request.json()`` and lets the framework's decode exception propagate
    surfaces an HTTP 500 for the first case instead of a JSON-RPC error for
    either.

    Returns ``(value, error_response)`` with exactly one of the two non-None,
    matching ``parse_request``'s contract so the two compose directly.

    Args:
        raw: The undecoded request body, as ``bytes`` (e.g. from an ASGI
            ``await request.body()``) or ``str``.
        max_body_bytes: Reject bodies larger than this before attempting to
            decode them. Defaults to :data:`DEFAULT_MAX_BODY_BYTES`.

    Returns:
        ``(decoded_value, None)`` on success, else ``(None, response_dict)``.
        ``response_dict["id"]`` is always ``None`` per JSON-RPC 2.0 §5.1: the
        body could not be parsed, so no request id is knowable.
    """
    body_bytes = raw if isinstance(raw, bytes | bytearray) else raw.encode("utf-8")

    if len(body_bytes) > max_body_bytes:
        return None, error_response(
            None,
            PARSE_ERROR,
            f"Request body of {len(body_bytes)} bytes exceeds the " f"{max_body_bytes}-byte limit.",
        )

    try:
        decoded = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        return None, error_response(
            None,
            PARSE_ERROR,
            f"Request body is not valid JSON: {exc}",
        )

    return decoded, None


def parse_request(body: Any) -> tuple[JsonRpcRequest | None, dict[str, Any] | None]:
    """Validate a decoded JSON body as a JSON-RPC 2.0 request.

    Returns ``(request, error_response)`` with exactly one of the two non-None,
    so a caller dispatches on whichever it got rather than on a raised
    exception — the envelope is the transport's business, and a malformed one is
    an ordinary response to send, not an exceptional condition.

    ``id`` is read before anything else is validated, so an otherwise-invalid
    request still gets its id echoed back and the caller can correlate the
    failure.  Batch requests (a JSON array at the top level) are rejected as
    ``INVALID_REQUEST``: A2A defines no batched operation, and silently
    processing only the first element would lose calls.

    Args:
        body: An already-JSON-decoded request body

    Returns:
        ``(JsonRpcRequest, None)`` when valid, else ``(None, response_dict)``.
    """
    if not isinstance(body, dict):
        return None, error_response(
            None,
            INVALID_REQUEST,
            f"Request must be a JSON-RPC 2.0 object, got {type(body).__name__}.",
        )

    # Read the id first so even a rejected request can be correlated.  A
    # non-scalar id is not a usable correlator, so it degrades to None rather
    # than being echoed back as a dict.
    raw_id = body.get("id")
    request_id = raw_id if isinstance(raw_id, str | int | float) else None

    if body.get("jsonrpc") != JSONRPC_VERSION:
        return None, error_response(
            request_id,
            INVALID_REQUEST,
            f'Expected "jsonrpc": "{JSONRPC_VERSION}", got {body.get("jsonrpc")!r}.',
        )

    method = body.get("method")
    if not isinstance(method, str) or not method:
        return None, error_response(
            request_id,
            INVALID_REQUEST,
            'Request is missing a non-empty string "method".',
        )

    raw_params = body.get("params")
    if raw_params is None:
        params: dict[str, Any] = {}
    elif isinstance(raw_params, dict):
        params = raw_params
    else:
        # JSON-RPC also allows positional (array) params, but every A2A
        # operation takes a named-field request object, so an array here is a
        # caller bug rather than a shape to support.
        return None, error_response(
            request_id,
            INVALID_PARAMS,
            f'"params" must be an object for A2A methods, got {type(raw_params).__name__}.',
        )

    return JsonRpcRequest(method=method, params=params, id=request_id), None


def request_envelope(method: str, params: dict[str, Any], request_id: str) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 request for an A2A method call.

    ``request_id`` is required rather than generated here so the caller keeps
    the value it will correlate the response against — and so this module stays
    free of ``uuid4()``, which would make every envelope untestable without
    patching.

    Args:
        method: One of the ``METHOD_*`` constants above.  A2A v1.0 names are
            PascalCase; v0.3's ``message/send`` earns a ``-32601`` from a v1.0
            peer.
        params: The method's request object.  Always a named-field object —
            JSON-RPC's positional (array) form is not used by any A2A operation.
        request_id: Correlation id, echoed back verbatim by the peer.

    Returns:
        A JSON-RPC 2.0 request object, ready to serialize.
    """
    return {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "method": method,
        "params": params,
    }


def parse_response(body: Any) -> tuple[Any, dict[str, Any] | None]:
    """Split a JSON-RPC 2.0 response into ``(result, error)``.

    Exactly one of the two is meaningful, and the *error* is what the caller
    must check first: JSON-RPC allows a response to carry ``"result": null``
    legitimately, so "result is None" does not mean "this failed".

    Returns the error as a plain dict rather than raising, mirroring
    :func:`parse_request`'s shape and for the same reason — the envelope is the
    transport's business, and which exception an A2A error code deserves is
    :func:`novaos_client_sdk.exceptions.a2a_error_from_json_rpc`'s decision, not
    this module's.

    **A bare, un-enveloped body is accepted as a result.**  A hand-rolled pod
    server that answers with the payload alone should not be silently
    unreadable — the same tolerance
    :func:`novaos_client_sdk.a2a_events.stream_frame_from_dict` extends to bare
    stream frames, and the same one the platform's TypeScript caller
    extends in ``parseTaskResponse``.

    Args:
        body: An already-JSON-decoded response body.

    Returns:
        ``(result, None)`` on success, ``(None, error_object)`` on failure.

    Raises:
        ValueError: If ``body`` is not a JSON object at all — a response that is
            a list or a scalar is not something a caller can degrade past.
    """
    if not isinstance(body, dict):
        raise ValueError(f"JSON-RPC response must be a JSON object, got {type(body).__name__}.")

    error = body.get("error")
    if isinstance(error, dict):
        return None, error

    if "result" in body:
        return body["result"], None

    # Neither member present: treat the whole body as the result, so a server
    # answering with the bare payload is still readable.  An empty object is
    # deliberately included — that is a valid, if uninformative, result.
    return body, None
