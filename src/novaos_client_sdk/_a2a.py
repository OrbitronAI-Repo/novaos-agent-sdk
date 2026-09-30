"""A2A JSON-RPC transport for :class:`novaos_client_sdk.client.Client`.

Replaces ``_acp.py``, which held the SDK's last import of the archived
``acp-sdk`` package and existed only to give ``run_sync`` / ``run_stream`` a
transport.  The A2A migration deleted those methods, so the module went with them, and
the ``acp-sdk`` dependency with it.

**Why NovaOS's own transport rather than ``a2a-sdk``'s client.**  The vendor SDK
is a genuine option — ``pyproject.toml`` recorded ``a2a-sdk>=1.1,<2`` as the
package to reach for *if* a vendor A2A client were needed — and it was weighed
against this and not taken, for three reasons:

1. **There is almost nothing left to vendor.**  The wire model already lives in
   this SDK (``a2a_types``, ``a2a_task``, ``a2a_artifact``, ``a2a_events``) and
   the envelope in ``a2a_jsonrpc``, all of it written against spec v1.0.1 and
   *verified against the real ``a2a-sdk``* by ``tests/test_a2a_v1_conformance.py``.
   What a vendor client would add over that is an HTTP POST and an SSE reader —
   the two things below.
2. **It would reintroduce a coupling this SDK already removed.**  ``a2a-sdk``'s client
   speaks in generated protobuf types, so every call would need a conversion
   layer at the boundary, and any leak of those types into a return value is the
   DIP violation the migration's design plan exists to prevent.
3. **``a2a-sdk`` stays a test dependency, which is where it is most useful.**  As
   a conformance peer it *checks* NovaOS's wire format; as a runtime dependency
   it would hide the wire format instead, and months of emitting v0.3 JSON
   while claiming v1.0 is what "hidden wire format" cost last time.

**What this module owns:** the HTTP call, the SSE read loop, and the JSON-RPC
error-to-exception mapping.  It owns no A2A semantics: which method to call,
what to put in ``params``, and what a returned ``Task`` means are all
``client.py``'s.

Design notes: the spec-version pin and the wire-format
history are documented in the migration's architecture decision record.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, Final
from uuid import uuid4

from novaos_client_sdk._http import httpx
from novaos_client_sdk._transport import ClientTransport, raise_for_jsonrpc_error
from novaos_client_sdk.a2a_jsonrpc import (
    parse_response,
    request_envelope,
)
from novaos_client_sdk.a2a_types import A2A_PROTOCOL_VERSION, A2A_VERSION_HEADER
from novaos_client_sdk.exceptions import NovaOSError

__all__ = [
    "SSE_ACCEPT",
    "SSE_CONTENT_TYPE",
    "A2ATransport",
    "iter_sse_data",
    "sse_data",
    "sse_data_line",
]

#: ``Accept`` value for the two streaming methods.  A2A's JSON-RPC binding
#: carries ``SendStreamingMessage`` and ``SubscribeToTask`` over Server-Sent
#: Events, so the same endpoint answers either a single JSON body or an SSE
#: stream depending on what was asked for.
SSE_ACCEPT: str = "text/event-stream"

#: Substring identifying an SSE response body.  Matched rather than compared,
#: since a server is free to send ``text/event-stream; charset=utf-8``.
SSE_CONTENT_TYPE: str = "text/event-stream"

#: Sentinel for "this frame was not JSON", distinct from a decoded ``null``.
_UNDECODABLE: Final = object()


class _SSEFramer:
    """The SSE line grammar, shared by the streaming and buffered readers.

    Held as a tiny state machine rather than written twice because the two
    readers must agree: a non-SSE response is re-read as SSE when it turns out
    not to be JSON either (see :meth:`A2ATransport.stream`), and a second,
    subtly different copy of the framing rules is how one of those paths would
    quietly start losing events.
    """

    def __init__(self) -> None:
        self._data_lines: list[str] = []

    def feed(self, raw_line: str) -> str | None:
        """Consume one line; return a completed event's data, if it completed one.

        Implements the parts of the grammar A2A actually uses (WHATWG HTML
        §9.2.6): an event ends at a **blank line**, and only then is its data
        dispatched — so a payload split across several ``data:`` lines is
        rejoined with a newline rather than emitted as several truncated JSON
        fragments.  A leading space after the colon is syntax, not value.  A
        line starting with ``:`` is a comment, which is how servers and proxies
        send keep-alives.  ``event:`` / ``id:`` / ``retry:`` are ignored: A2A's
        stream carries one kind of payload and identifies its content by the
        ``StreamResponse`` oneof inside, so reading the event name would be
        reading v0.3's ``kind`` discriminator by another route.
        """
        line = raw_line.rstrip("\r")

        if not line:
            return self.flush()

        if line.startswith(":"):
            return None

        field, _, value = line.partition(":")
        if field != "data":
            return None

        self._data_lines.append(value[1:] if value.startswith(" ") else value)
        return None

    def flush(self) -> str | None:
        """Return any buffered event's data and reset.

        Called at end of stream as well as on a blank line, since a server that
        closes the connection right after its last frame is ordinary rather than
        malformed.
        """
        if not self._data_lines:
            return None
        payload = "\n".join(self._data_lines)
        self._data_lines = []
        return payload


def sse_data(body: str) -> list[str]:
    """Read every event's ``data`` payload out of an already-buffered SSE body.

    The buffered counterpart of :func:`iter_sse_data`, for the case where the
    response had to be read whole before its format was known.
    """
    framer = _SSEFramer()
    payloads = [p for line in body.splitlines() if (p := framer.feed(line)) is not None]
    trailing = framer.flush()
    if trailing is not None:
        payloads.append(trailing)
    return payloads


def sse_data_line(payload: dict[str, Any]) -> bytes:
    """Encode one JSON-RPC frame as a single SSE event.

    The server-side counterpart of :class:`_SSEFramer`: it emits exactly what
    :meth:`_SSEFramer.feed` expects to read back — one ``data:`` line per JSON
    payload, terminated by the blank line the WHATWG grammar (§9.2.6) requires
    to dispatch an event. No ``event:``/``id:``/``retry:`` fields, mirroring
    :meth:`_SSEFramer.feed`'s choice to ignore them on the read side: A2A
    identifies a frame's content by the ``StreamResponse`` oneof inside the
    payload, not by an SSE-level tag.

    ``payload`` is serialized whole on one line — never containing a literal
    newline that would need re-splitting across several ``data:`` lines — since
    ``json.dumps`` escapes ``\\n`` inside string values by default.

    Used by :meth:`novaos_client_sdk.agent_builder.AgentBuilder.handle_a2a_streaming_request`
    to produce the bytes an ASGI ``StreamingResponse`` writes to the wire.
    """
    return f"data: {json.dumps(payload)}\n\n".encode()


async def iter_sse_data(response: httpx.Response) -> AsyncIterator[str]:
    """Yield the ``data`` payload of each event in a streaming SSE response.

    The framing rules live in :class:`_SSEFramer`; see :meth:`_SSEFramer.feed`
    for which parts of the grammar are honoured and why.

    Args:
        response: An open streaming ``httpx`` response.

    Yields:
        Each event's raw ``data`` payload, unparsed.
    """
    framer = _SSEFramer()

    async for raw_line in response.aiter_lines():
        payload = framer.feed(raw_line)
        if payload is not None:
            yield payload

    trailing = framer.flush()
    if trailing is not None:
        yield trailing


class A2ATransport(ClientTransport):
    """One HTTP connection to one A2A agent, speaking JSON-RPC 2.0.

    Holds an ``httpx.AsyncClient`` for the lifetime of the enclosing
    :class:`novaos_client_sdk.client.Client` context, so repeated calls to the
    same agent reuse the connection.  Satisfies
    :class:`~novaos_client_sdk._transport.ClientTransport`, alongside the
    HTTP+JSON and gRPC transports this SDK later added.

    **The ``A2A-Version`` header is attached here, not at the call sites.**
    A2A v1.0 §7 makes it mandatory and defines an *absent* header as an explicit
    request for v0.3, which every kind of NovaOS pod answers ``-32009``
    (for scaffolded pods and for ``AgentBuilder`` ones alike).  Putting
    it on the transport means no method can be added later that forgets it — the
    failure would otherwise surface as a version error from the pod rather than
    as anything obviously missing at the caller.
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
            base_url: Base URL of the A2A agent.  Under A2A the URL *is* the
                agent's address — there is no per-request agent name, the way
                ACP's ``POST /runs`` carried one.
            timeout: Request timeout in seconds, applied to non-streaming calls.
                Streaming calls deliberately do not carry it; see :meth:`stream`.
            headers: Additional headers, including any the auth provider
                supplied.
        """
        # Kept verbatim, and posted to directly by `call()`/`stream()` rather
        # than merged onto the `httpx.AsyncClient` below. `httpx.AsyncClient`
        # silently appends a trailing slash to any `base_url` with a path
        # component — harmless for a bare pod host (`http://agent:8000`), the
        # only shape this transport's own test suite exercises, but wrong for
        # NovaOS's real gateway-multiplexed address
        # (`{gateway}/a2a/v1/agents/{agentId}`), where it turns a valid route
        # into one the gateway 404s. "Under A2A the URL *is*
        # the agent's address" — this transport must send exactly the URL it
        # was given, never a silently mutated one.
        self._base_url = base_url
        self._timeout = timeout
        self._headers = {
            "Content-Type": "application/json",
            A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION,
            **(headers or {}),
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> A2ATransport:
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
        """Return the open HTTP client, or raise if there is none."""
        if self._client is None:
            raise RuntimeError("A2ATransport used outside its context manager.")
        return self._client

    # Kept as a same-named static method (rather than calling the shared
    # `_transport.raise_for_jsonrpc_error` at each call site directly) so this
    # class's diff against its earlier shape stays minimal; it is a thin
    # forward to the now-shared implementation every transport uses.
    _raise_for_jsonrpc_error = staticmethod(raise_for_jsonrpc_error)

    async def call(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make one non-streaming JSON-RPC call and return its ``result``.

        Args:
            method: An A2A v1.0 method name (PascalCase).
            params: The method's request object.
            task_id: Task this call concerns, if any.  Carried onto a raised
                :class:`~novaos_client_sdk.exceptions.A2AProtocolError` so a
                caller handling, say, ``TaskNotFoundError`` knows which task was
                not found without re-reading the request.
            headers: Extra headers for this call only, on top of the client's
                own — e.g. a credential
                :class:`~novaos_client_sdk.interceptors.ClientCallInterceptor`
                resolved fresh for this call.  ``httpx`` merges these with the
                client-level headers set at construction, with a same-named
                entry here winning, so a per-call ``Authorization`` overrides
                the one baked in at ``Client`` construction rather than being
                dropped silently.

        Returns:
            The JSON-RPC ``result`` value.

        Raises:
            A2AProtocolError: If the agent answered a JSON-RPC ``error``.
            httpx.HTTPStatusError: If the agent answered a non-2xx status.
        """
        client = self._require_client()
        # Posted to the *original* base_url string, not the relative
        # `A2A_JSONRPC_PATH` — see the note on `self._base_url` in `__init__`
        # for why: `httpx.AsyncClient` silently appends a trailing slash to any
        # `base_url` with a path component, and merging `"/"` onto that just
        # re-emits the mutated URL. An absolute URL passed to `.post()` bypasses
        # base_url merging entirely and is sent exactly as given (verified: it
        # carries no trailing slash `base_url` did not already have).
        response = await client.post(
            self._base_url,
            json=request_envelope(method, params, str(uuid4())),
            headers=headers,
        )
        response.raise_for_status()

        result, error = parse_response(response.json())
        if error is not None:
            self._raise_for_jsonrpc_error(error, task_id)
        return result

    async def stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[Any]:
        """Make one streaming JSON-RPC call and yield each decoded SSE frame.

        **No read timeout is applied.**  The transport's configured timeout
        bounds a request that should answer promptly; a stream is expected to
        stay open for as long as the task runs, which can be hours, so applying
        the same bound would cancel healthy long runs.  Connect and write
        timeouts are kept — those still describe a peer that is not answering.

        A frame that decodes to something unrecognizable is yielded as-is and
        left for :func:`novaos_client_sdk.a2a_events.stream_frame_from_dict` to
        drop; a frame that is not valid JSON at all is skipped here, since one
        corrupt frame must not abort a run that is otherwise progressing.  What
        *is* fatal is a JSON-RPC ``error`` frame: that is the call failing, not a
        frame within it.

        **A 200 that is not an event stream is still an answer, and is read as
        one.**  A streaming method can be declined with an ordinary JSON-RPC
        body rather than an SSE stream — which is exactly what an
        ``AgentBuilder`` pod does when it answers ``SendStreamingMessage`` with
        ``-32004``, and what a hand-rolled server does with ``-32601``.
        Insisting on ``data:`` framing there would turn the documented "fall
        back to ``send_message``" path into an empty iterator, and an empty
        iterator is indistinguishable from a task that finished without saying
        anything.  So a non-SSE body is buffered and parsed as a single JSON-RPC
        response: an ``error`` raises, and a ``result`` is yielded as the
        stream's one and only frame.  If it turns out not to be JSON either, the
        buffered text is re-read as SSE, covering a server that streams
        correctly but forgets its ``Content-Type``.

        Args:
            method: An A2A v1.0 streaming method name.
            params: The method's request object.
            task_id: Task this call concerns, if any.
            headers: Extra headers for this call only — see :meth:`call`.
                Merged under the mandatory ``Accept`` header rather than over
                it: this method's own choice of ``Accept`` is the one detail a
                caller-supplied header must not be allowed to override, since
                that header is what tells the transport whether to read the
                response as SSE at all.

        Yields:
            Each SSE event's already-JSON-decoded payload.

        Raises:
            A2AProtocolError: If the agent sent a JSON-RPC ``error``, whether in
                the response body or mid-stream.
            httpx.HTTPStatusError: If the agent answered a non-2xx status.
        """
        client = self._require_client()
        # Absolute URL, not the relative `A2A_JSONRPC_PATH` — same reasoning as
        # `call()` above.
        async with client.stream(
            "POST",
            self._base_url,
            json=request_envelope(method, params, str(uuid4())),
            headers={**(headers or {}), "Accept": SSE_ACCEPT},
            timeout=httpx.Timeout(self._timeout, read=None),
        ) as response:
            # The body must be read before the status can be reported: `stream`
            # yields the response with its content unread, and httpx refuses to
            # expose `.text` on an unread streaming response.
            if response.status_code >= 400:
                await response.aread()
                response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if SSE_CONTENT_TYPE not in content_type:
                for payload in await self._non_sse_payloads(response):
                    yield self._decoded_frame(payload, task_id)
                return

            async for payload in iter_sse_data(response):
                decoded = self._decode(payload)
                if decoded is _UNDECODABLE:
                    continue
                yield self._checked(decoded, task_id)

    @staticmethod
    def _decode(payload: str) -> Any:
        """Decode one frame, or return :data:`_UNDECODABLE`.

        A sentinel rather than ``None`` because ``null`` is a payload a server
        may legitimately send, and skipping it silently for the wrong reason is
        the kind of thing that only shows up as a missing event much later.
        """
        try:
            return json.loads(payload)
        except ValueError:
            return _UNDECODABLE

    def _checked(self, decoded: Any, task_id: str | None) -> Any:
        """Raise if this frame is a JSON-RPC error, else return it unchanged."""
        if isinstance(decoded, dict) and isinstance(decoded.get("error"), dict):
            self._raise_for_jsonrpc_error(decoded["error"], task_id)
        return decoded

    def _decoded_frame(self, payload: str, task_id: str | None) -> Any:
        """Decode one payload from a non-SSE body, raising on an error object.

        Unlike the SSE path this does *not* tolerate undecodable content: there
        is only one payload here, so skipping it would leave the caller with the
        empty iterator this whole branch exists to prevent.
        """
        decoded = self._decode(payload)
        if decoded is _UNDECODABLE:
            raise NovaOSError(
                "Agent answered a streaming call with a body that is neither "
                "Server-Sent Events nor JSON."
            )
        return self._checked(decoded, task_id)

    @staticmethod
    async def _non_sse_payloads(response: httpx.Response) -> list[str]:
        """Buffer a non-event-stream body and return the payloads to decode.

        One payload for the ordinary case — the whole body is a single JSON-RPC
        response — and the SSE events for the case where the body *was* a
        stream and only the ``Content-Type`` was wrong.  The two are told apart
        by trying JSON first, since ``data:``-framed text is never valid JSON.
        """
        body = (await response.aread()).decode(response.encoding or "utf-8", errors="replace")

        stripped = body.strip()
        if stripped.startswith(("{", "[")):
            return [stripped]

        events = sse_data(body)
        return events if events else [stripped]
