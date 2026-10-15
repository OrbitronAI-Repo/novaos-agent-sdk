"""Execution Gateway client for end-user applications.

Provides a client for driving agent execution through the NovaOS Execution Gateway.
This is intended for external / end-user applications (e.g., a marketplace frontend),
**not** for agents calling other agents — use :class:`~novaos_client_sdk.client.Client`,
which speaks A2A v1.0 JSON-RPC, for that.

The Execution Gateway exposes:

- ``POST /api/v1/execute`` — Start a conversation / send a message to an agent.
- ``GET /api/v1/events/{runId}`` — Subscribe to real-time SSE token streaming.
- ``GET /api/v1/streaming/health`` — Check if real-time streaming is available.

Authentication uses NovaOS IAM tokens (OIDC ``Bearer`` tokens).

Why this module is not part of the A2A migration
------------------------------------------------

An assessment of this surface during the ACP→A2A migration found it
**protocol-agnostic**: it needs no A2A migration, and it never needed an ACP
one.

``Client`` addresses an agent *directly* and therefore speaks whatever wire
protocol that agent serves — which is why it was rewritten end-to-end as part
of that migration.
``GatewayClient`` addresses the *NovaOS platform*, whose REST contract is
expressed entirely in platform nouns: an ``instance_id`` to run, a ``run_id`` to
follow, and a run lifecycle of ``QUEUED``/``RUNNING``/``WAITING_USER``/
``COMPLETED``/``FAILED``/``CANCELLED``.  None of those are ACP or A2A concepts.
The pod-facing protocol is chosen and spoken by the Temporal worker, several
hops below this client, and swapping it changes nothing on this wire.

The one thing the migration *does* reach is the payload of the ``hitl`` event:
the platform's human-in-the-loop pause is now backed by A2A's first-class
``TASK_STATE_INPUT_REQUIRED`` rather than ACP's custom ``await_request`` field
(ADR F4).  This client passes that payload through opaquely, so it is unaffected
either way.

Terminal events and KB-15
-------------------------

The one place the assessment did find a defect was terminal-event handling, and
it is the same bug class the migration exists to avoid repeating.
``docs/knowledge-base/kb-15-marketplace-execution-stuck-acp-failed.md`` describes
a run that failed while the UI waited forever, because the failure arrived in a
shape the consumer did not recognize as an ending.  :meth:`GatewayClient.events`
had exactly that shape: it stopped only on ``done`` and ``error``, so a run whose
terminal signal arrives as ``status`` with a terminal ``payload.status`` — which
is how the platform records a failure to the event log, and therefore how every
replayed run reports its ending — neither ended the iteration nor surfaced the
failure.

:attr:`StreamEvent.is_terminal` and :attr:`StreamEvent.is_failure` are now the
contract, and :meth:`GatewayClient.events` stops on the former.  Consumers should
branch on those properties rather than on ``event.type`` string equality, so a
future event type cannot reintroduce the same silence.

Example:
    ```python
    from novaos_client_sdk.auth import BearerTokenAuth
    from novaos_client_sdk.gateway import GatewayClient
    from novaos_client_sdk.models import Message, MessagePart

    auth = BearerTokenAuth(token=novaos_iam_token)

    async with GatewayClient(
        base_url="http://execution-gateway:8081",
        auth=auth,
    ) as gw:
        # Start execution
        result = await gw.execute(instance_id="<uuid>", input_text="Hello!")

        # Stream events
        async for event in gw.events(result.run_id):
            if event.type == "chunk":
                print(event.content, end="", flush=True)
            elif event.type == "done":
                print()
                break
            elif event.type == "error":
                print(f"Error: {event.content}")
                break
    ```
"""

import asyncio
import base64
import json
import logging
import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from novaos_client_sdk._http import httpx
from novaos_client_sdk._resilience import backoff_delay
from novaos_client_sdk.auth import AuthProvider
from novaos_client_sdk.exceptions import (
    NovaOSAuthenticationError,
    NovaOSConnectionError,
    NovaOSError,
    NovaOSServerError,
)

logger = logging.getLogger(__name__)

#: Run lifecycle statuses after which the platform emits nothing further.
#:
#: These are NovaOS *run* statuses, not A2A ``TaskState`` values, and the two are
#: deliberately not collapsed: a run is a platform record with its own lifecycle,
#: while a task belongs to whichever agent served the request.  They correlate
#: and do not correspond one-for-one — ``WAITING_USER`` is a live run but a
#: paused task, and one run can span several tasks across a HITL exchange.
#:
#: ``WAITING_USER`` is deliberately absent.  It is a pause, not an ending: the
#: run continues once the user answers, and treating it as terminal would hang up
#: on every HITL exchange.  This mirrors the A2A layer's own reason for excluding
#: ``TASK_STATE_INPUT_REQUIRED`` from
#: :data:`~novaos_client_sdk.a2a_task.TERMINAL_TASK_STATES`.
TERMINAL_RUN_STATUSES: Final[frozenset[str]] = frozenset({"COMPLETED", "FAILED", "CANCELLED"})

#: Every run lifecycle status the platform emits, terminal or not.
#:
#: Used to tell a lifecycle status apart from an agent's free-text progress
#: payload, which the gateway forwards under the same ``status`` event type.
_RUN_STATUSES: Final[frozenset[str]] = TERMINAL_RUN_STATUSES | frozenset(
    {"QUEUED", "RUNNING", "WAITING_USER"}
)


def _run_status(payload: Any) -> str | None:
    """Read a run lifecycle status out of an event payload, if it names one.

    Returns ``None`` for anything that is not one of :data:`TERMINAL_RUN_STATUSES`
    or a known non-terminal run status.  That narrowness is the point: an agent's
    own progress payload may legitimately contain a ``status`` key holding free
    text, and mistaking it for a lifecycle status would end the stream early.
    """
    if not isinstance(payload, dict):
        return None
    value = payload.get("status")
    if isinstance(value, str) and value in _RUN_STATUSES:
        return value
    return None


@dataclass
class StreamEvent:
    """A single SSE event from the Execution Gateway streaming endpoint.

    Attributes:
        type: Event type — ``"chunk"``, ``"done"``, ``"error"``, ``"status"``,
            ``"hitl"``, or any other type the gateway forwards (see
            :meth:`from_sse_data`).
        content: Event payload (text chunk, or error message).
        timestamp: Event timestamp as a string (milliseconds since epoch).
        status: For a ``"status"`` event, the run lifecycle status it reports —
            ``None`` for every other event type.  This is what makes a failure
            that arrives as a status update visible instead of silent; see
            :attr:`is_failure`.
        id: The SSE ``id:`` line delivered with this event, if any — the
            resume cursor a reconnect sends back as ``Last-Event-ID`` (see
            :meth:`GatewayClient.events`).  This is **not** the JSON body's
            ``sequence`` field: on a Service Runs stream those are two
            different counters that happen to share the same plain-integer
            shape — sending ``sequence`` back as ``Last-Event-ID`` can
            silently resume from the wrong place. Use
            ``id`` for reconnects; treat ``sequence`` as ordering metadata only.
    """

    type: str
    content: str | None = None
    timestamp: str | None = None
    status: str | None = None
    id: str | None = None

    @property
    def is_terminal(self) -> bool:
        """Whether no further events will arrive for this run.

        Branch on this rather than on ``event.type == "done"``.  A run reaches
        its end in more than one shape: ``done`` closes a live stream, ``error``
        aborts one, and a ``status`` event carrying a terminal run status is how
        the *event log* records an ending — which is what a client replaying a
        finished run sees, and what a failure detected after the agent already
        returned looks like.  Recognizing only the first two is the KB-15 defect
        (see the module docstring).

        Two things this deliberately does not do.  It does not treat
        ``WAITING_USER`` as terminal, because a HITL pause is not an ending.  And
        it does not treat an unrecognized ``status`` value as terminal, because
        the gateway multiplexes agent-authored progress updates onto the same
        event type; matching the platform's exact vocabulary is what keeps an
        agent's own progress text from ending the stream early.
        """
        if self.type in ("done", "error"):
            return True
        return self.type == "status" and self.status in TERMINAL_RUN_STATUSES

    @property
    def is_failure(self) -> bool:
        """Whether this event reports that the run did **not** succeed.

        The KB-15 invariant a consumer needs: a terminal non-success state must
        never be mistaken for success.  A stream can end in failure without ever
        producing an ``error`` event — the platform records a late-detected
        failure as ``status``/``FAILED`` — so a consumer that watches only for
        ``error`` will read that ending as a normal completion.
        """
        if self.type == "error":
            return True
        return self.type == "status" and self.status == "FAILED"

    @classmethod
    def from_sse_data(cls, event_type: str, data: str) -> "StreamEvent":
        """Parse an SSE ``data:`` line into a StreamEvent.

        The Execution Gateway sends all events as ``event: message`` with a
        JSON body containing ``{type, payload, run_id, sequence}``.  The actual
        event semantics are determined by the JSON ``type`` field:

        - ``message.delta`` → content chunk (maps to ``"chunk"``)
        - ``message.final`` → the turn's complete agent message (replay only)
        - ``done``          → stream complete
        - ``error``         → stream failed
        - ``status``        → run lifecycle status, **or** agent progress
        - ``hitl``          → human-in-the-loop prompt

        ``status`` carries two different things under one type.  The platform
        publishes it with a ``status`` field naming a run lifecycle state; an
        agent publishes it with a ``message`` field describing what it is doing.
        Both are surfaced — :attr:`StreamEvent.status` is populated only when the
        payload actually names one, which is what lets :attr:`is_terminal`
        distinguish "this run ended" from "still working".

        Any other type is passed through with its own name rather than being
        dropped, so a gateway-side addition reaches the caller instead of
        vanishing.

        Legacy format (``event: chunk|done|error``) is also supported for
        backward compatibility.

        Args:
            event_type: The SSE ``event:`` field value.
            data: The SSE ``data:`` field value (JSON string).

        Returns:
            Parsed StreamEvent.
        """
        try:
            parsed = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            parsed = {}

        # Gateway unified format: event_type is always "message", real type in JSON.
        if event_type == "message":
            inner_type = parsed.get("type", "")
            payload = parsed.get("payload") or {}

            if inner_type == "message.delta":
                return cls(
                    type="chunk",
                    content=payload.get("content"),
                    timestamp=parsed.get("ts"),
                )
            elif inner_type == "done":
                return cls(type="done", content=None, timestamp=parsed.get("ts"))
            elif inner_type == "error":
                return cls(
                    type="error",
                    content=payload.get("message") or payload.get("error"),
                    timestamp=parsed.get("ts"),
                )
            else:
                return cls(
                    type=inner_type or "unknown",
                    content=payload.get("message") or payload.get("content"),
                    timestamp=parsed.get("ts"),
                    # Only a `status` event reports a run status. A `hitl` or
                    # `tool.result` payload could carry a `status` key meaning
                    # something else entirely, and reading it here would put a
                    # value in a field documented never to hold one.
                    status=_run_status(payload) if inner_type == "status" else None,
                )

        # Legacy SSE format: event type is the semantic type directly.
        content: str | None = None
        if event_type == "chunk":
            content = parsed.get("content")
        elif event_type == "error":
            content = parsed.get("message")

        return cls(
            type=event_type,
            content=content,
            timestamp=parsed.get("ts"),
            status=_run_status(parsed) if event_type == "status" else None,
        )


#: RFC 3986 URI scheme: a letter followed by letters, digits, ``+``, ``-`` or ``.``.
_URI_SCHEME: Final[re.Pattern[str]] = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*")


@dataclass(frozen=True)
class Attachment:
    """A file attached to an execution request (url or data).

    Mirrors the Execution Gateway's ``Attachment`` schema.  Exactly one of
    ``url`` or ``data`` must be given; the fields are validated on construction,
    so an invalid attachment raises before any request is sent.

    Attributes:
        type: MIME type or logical attachment type.
        url: Location to fetch the file (an absolute URI).
        data: Raw file content. Required if no ``url``; mutually exclusive with it.  Base64-encoded by
            :meth:`to_dict`, so pass the file's bytes (``bytes``, ``bytearray`` or
            ``memoryview``), not an encoded string.
            Left out of ``repr`` so a large file does not flood logs.
        name: Plain filename; no path separators or traversal.

    Raises:
        NovaOSError: If a field is invalid.
    """

    type: str
    url: str | None = None
    data: bytes | bytearray | memoryview | None = field(default=None, repr=False)
    name: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.type, str) or not self.type:
            raise NovaOSError("Attachment type must be a non-empty string")
        # `data` is checked against None, not truthiness: b"" is a valid empty file.
        if not self.url and self.data is None:
            raise NovaOSError("Attachment requires either url or data")
        if self.url and self.data is not None:
            raise NovaOSError("Attachment accepts only one of url or data, not both")
        if self.url is not None and not _is_uri(self.url):
            raise NovaOSError(f"Attachment url is not a valid URI: {self.url!r}")
        if self.data is not None and not isinstance(self.data, (bytes, bytearray, memoryview)):
            raise NovaOSError(
                f"Attachment data must be bytes, bytearray or memoryview, "
                f"got {type(self.data).__name__}"
            )
        if self.name is not None and not _is_plain_filename(self.name):
            raise NovaOSError(
                f"Attachment name must be a plain filename without path "
                f"separators or traversal: {self.name!r}"
            )

    def to_dict(self) -> dict[str, str]:
        """Serialize to the request body shape, omitting unset fields.

        ``data`` is sent base64-encoded, as the gateway expects.
        """
        body = {"type": self.type}
        if self.url is not None:
            body["url"] = self.url
        if self.data is not None:
            body["data"] = base64.b64encode(self.data).decode("ascii")
        if self.name is not None:
            body["name"] = self.name
        return body


def _is_uri(value: Any) -> bool:
    """Whether ``value`` is an absolute URI (a scheme followed by a non-empty rest)."""
    if not isinstance(value, str) or any(c.isspace() for c in value):
        return False
    scheme, sep, rest = value.partition(":")
    return bool(sep and rest and _URI_SCHEME.fullmatch(scheme))


def _is_plain_filename(value: Any) -> bool:
    """Whether ``value`` is a bare filename that cannot name another directory."""
    if not isinstance(value, str) or not value or value in (".", ".."):
        return False
    return not any(c in value for c in ("/", "\\", "\0"))


@dataclass
class ExecuteResult:
    """Result of a ``POST /api/v1/execute`` call.

    Attributes:
        run_id: The execution run ID (used for SSE event subscription).
        status: Execution status (e.g., ``"started"``, ``"completed"``).
        output: Agent output data if execution completed synchronously.
        raw: The full response body for advanced inspection.
    """

    run_id: str
    status: str | None = None
    output: Any | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecuteResult":
        """Create from the gateway response body."""
        return cls(
            run_id=data.get("runId") or data.get("run_id", ""),
            status=data.get("status"),
            output=data.get("output"),
            raw=data,
        )


class GatewayClient:
    """Async client for the NovaOS Execution Gateway.

    Intended for end-user applications that drive agent execution through the
    platform's gateway layer (with NovaOS IAM authentication).

    Args:
        base_url: Execution Gateway URL (e.g., ``http://execution-gateway:8081``).
        auth: Authentication provider (typically ``BearerTokenAuth`` with a NovaOS IAM token).
        timeout: Request timeout in seconds.
        headers: Additional HTTP headers.
    """

    def __init__(
        self,
        base_url: str,
        auth: AuthProvider | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth = auth
        self._timeout = timeout
        self._headers = headers.copy() if headers else {}
        self._client: httpx.AsyncClient | None = None

    def _build_headers(self) -> dict[str, str]:
        """Build request headers including authentication."""
        request_headers = self._headers.copy()
        if self._auth and hasattr(self._auth, "get_headers"):
            auth_headers = self._auth.get_headers()
            if auth_headers:
                request_headers.update(auth_headers)
        return request_headers

    async def __aenter__(self) -> "GatewayClient":
        """Async context manager entry."""
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._build_headers(),
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
                "GatewayClient is not initialized. "
                "Use async context manager or call __aenter__() first."
            )
        return self._client

    def _map_http_error(self, error: Exception) -> NovaOSError:
        """Map HTTP errors to SDK exception hierarchy."""
        error_str = str(error)
        error_type = type(error).__name__

        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code
            if status_code == 401:
                return NovaOSAuthenticationError(
                    f"Gateway authentication failed: {error_str}",
                    details={"status_code": status_code, "original_error": error_type},
                )
            if status_code == 403:
                return NovaOSAuthenticationError(
                    f"Gateway authorization failed: {error_str}",
                    details={"status_code": status_code, "original_error": error_type},
                )
            if status_code in (500, 502, 503, 504):
                return NovaOSServerError(
                    f"Gateway server error: {error_str}",
                    status_code=status_code,
                    details={"original_error": error_type},
                )
            return NovaOSError(
                f"Gateway error ({status_code}): {error_str}",
                details={"status_code": status_code, "original_error": error_type},
            )

        if isinstance(error, httpx.TransportError):
            # Covers ConnectError and TimeoutException (already handled before this
            # fix) as well as ReadError, WriteError, CloseError, and Local/Remote
            # ProtocolError — the actual exceptions httpx raises for a mid-stream
            # SSE drop. Narrower checks here silently made `events_with_reconnect`
            # treat the ordinary way a live stream disconnects as a permanent,
            # non-retryable failure.
            return NovaOSConnectionError(
                f"Failed to connect to Execution Gateway: {error_str}",
                details={"original_error": error_type},
            )

        return NovaOSError(
            f"Gateway client error: {error_str}",
            details={"original_error": error_type},
        )

    async def execute(
        self,
        instance_id: str,
        input_text: str,
        *,
        thread_id: str | None = None,
        conversation_id: str | None = None,
        attachments: Sequence[Attachment] | None = None,
    ) -> ExecuteResult:
        """Start agent execution via the Execution Gateway.

        Args:
            instance_id: UUID of the installed agent instance to execute.
            input_text: Plain-text input for the agent (required, max 100,000 chars).
            thread_id: Optional thread UUID for conversation continuity.
            conversation_id: Optional conversation UUID for grouping runs.
            attachments: Optional files to send with the request, each given
                by ``url`` or inline ``data`` bytes.  Omitted from the request
                when ``None`` or empty.

        Returns:
            ExecuteResult with the ``run_id`` for event subscription.

        Raises:
            NovaOSAuthenticationError: If authentication or authorization fails.
            NovaOSConnectionError: If connection to the gateway fails.
            NovaOSServerError: If the gateway returns a server error.
            NovaOSError: If instance_id is missing, or an element of
                ``attachments`` is not an :class:`Attachment`.
        """
        client = self._ensure_client()

        if not instance_id:
            raise NovaOSError("instance_id is required")

        body: dict[str, Any] = {"instance_id": instance_id, "input": input_text}
        if thread_id:
            body["thread_id"] = thread_id
        if conversation_id:
            body["conversation_id"] = conversation_id
        if attachments:
            for attachment in attachments:
                if not isinstance(attachment, Attachment):
                    raise NovaOSError(
                        f"attachments must contain Attachment objects, "
                        f"got {type(attachment).__name__}"
                    )
            body["attachments"] = [attachment.to_dict() for attachment in attachments]

        try:
            response = await client.post("/api/v1/execute", json=body)
            response.raise_for_status()
            return ExecuteResult.from_dict(response.json())
        except Exception as e:
            raise self._map_http_error(e) from e

    async def events(
        self, run_id: str, *, last_event_id: str | None = None
    ) -> AsyncIterator[StreamEvent]:
        """Subscribe to real-time SSE events for a running execution.

        Connects to ``GET /api/v1/events/{runId}`` and yields events as they
        arrive.  Iteration ends once an event reports the run has finished — see
        :attr:`StreamEvent.is_terminal`, which covers the ``status`` shape a
        replayed or late-failing run ends in as well as the ``done``/``error``
        sentinels a live stream ends in.

        The terminating event is always yielded before iteration stops, so a
        caller sees how the run ended.  Check :attr:`StreamEvent.is_failure` on
        it: a run can end in failure without ever producing an ``error`` event,
        and treating "the loop finished" as "the run succeeded" is precisely the
        KB-15 defect this module's docstring describes.

        Reconnecting after a disconnect: pass the ``id`` of the last
        :class:`StreamEvent` this method yielded as
        ``last_event_id`` to resume without a gap or a duplicate. The gateway
        resumes strictly after that id — never send the JSON body's
        ``sequence`` field here; see :attr:`StreamEvent.id`. Most callers should
        prefer :meth:`events_with_reconnect`, which does this automatically.

        Args:
            run_id: The execution run ID (from :meth:`execute`).
            last_event_id: Resume cursor from a previous connection's last
                yielded :attr:`StreamEvent.id`. Sent as the ``Last-Event-ID``
                SSE header. Omit for a fresh subscription.

        Yields:
            StreamEvent objects.  ``chunk`` carries streamed text; ``done``,
            ``error`` and a terminal ``status`` end the run; ``hitl``,
            ``connector.oauth_required``, ``tool.write_confirmation_required``,
            ``tool.call``/``tool.result`` and any future type are passed through.

        Raises:
            NovaOSAuthenticationError: If authentication fails.
            NovaOSConnectionError: If connection to the gateway fails.
            NovaOSServerError: If the gateway returns a server error.
        """
        client = self._ensure_client()

        headers = {"Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        try:
            async with client.stream(
                "GET",
                f"/api/v1/events/{run_id}",
                headers=headers,
            ) as response:
                response.raise_for_status()

                event_type = ""
                event_id: str | None = None
                async for line in response.aiter_lines():
                    line = line.strip()
                    if not line:
                        # Empty line = end of event
                        continue
                    if line.startswith("id:"):
                        # Per the SSE spec this persists across events until
                        # explicitly changed — it is not reset below.
                        event_id = line[len("id:") :].strip()
                    elif line.startswith("event:"):
                        event_type = line[len("event:") :].strip()
                    elif line.startswith("data:"):
                        data = line[len("data:") :].strip()
                        if event_type:
                            event = StreamEvent.from_sse_data(event_type, data)
                            event.id = event_id
                            yield event
                            if event.is_terminal:
                                return
                            event_type = ""

        except Exception as e:
            raise self._map_http_error(e) from e

    async def events_with_reconnect(
        self,
        run_id: str,
        *,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> AsyncIterator[StreamEvent]:
        """Subscribe to SSE events, transparently reconnecting on disconnect.

        Wraps :meth:`events`, tracking the last :attr:`StreamEvent.id` seen and
        passing it back as ``last_event_id`` on reconnect — the gateway
        resumes exactly where the dropped connection left off, so every event
        is yielded exactly once across any number of reconnects, with no gap
        and no duplicate.

        Only retryable failures (:attr:`NovaOSError.is_retryable`, e.g. a
        dropped connection or a 502/503/504) trigger a reconnect; an
        authentication failure or another non-retryable error propagates
        immediately. Reconnect attempts use :func:`_resilience.backoff_delay`
        (exponential backoff from ``initial_backoff`` seconds up to
        ``max_backoff``, with its default ±50% jitter so many clients
        reconnecting after the same gateway blip do not retry in lockstep),
        and the attempt count resets whenever an event is received —
        ``max_retries`` bounds *consecutive* failed reconnect attempts with no
        event received between them, not the total number of reconnects over a
        long-lived stream.

        Stops when a :attr:`StreamEvent.is_terminal` event is yielded, same as
        :meth:`events`.

        Args:
            run_id: The execution run ID (from :meth:`execute`).
            max_retries: Maximum consecutive reconnect attempts with no
                intervening event before giving up and raising.
            initial_backoff: Seconds to wait before the first reconnect.
            max_backoff: Upper bound on the backoff delay.

        Yields:
            StreamEvent objects, exactly once each, in order.

        Raises:
            NovaOSAuthenticationError: If authentication fails.
            NovaOSConnectionError: If reconnect attempts are exhausted.
            NovaOSServerError: If the gateway returns a non-retryable server error.
        """
        last_event_id: str | None = None
        consecutive_failures = 0

        while True:
            received_event = False
            try:
                async for event in self.events(run_id, last_event_id=last_event_id):
                    received_event = True
                    consecutive_failures = 0
                    if event.id:
                        last_event_id = event.id
                    yield event
                    if event.is_terminal:
                        return
                # The connection closed without a terminal event — treat like
                # any other drop and reconnect from the last id seen.
            except NovaOSError as e:
                if not e.is_retryable:
                    raise
                if received_event:
                    consecutive_failures = 0

            consecutive_failures += 1
            if consecutive_failures > max_retries:
                raise NovaOSConnectionError(
                    f"SSE stream for run {run_id} did not yield a terminal event after "
                    f"{max_retries} consecutive reconnect attempts",
                    details={"run_id": run_id, "last_event_id": last_event_id},
                )
            await asyncio.sleep(
                backoff_delay(consecutive_failures, base_s=initial_backoff, max_s=max_backoff)
            )

    async def streaming_health(self) -> bool:
        """Check if real-time streaming is available (Redis configured).

        Returns:
            True if the streaming subsystem is healthy.
        """
        client = self._ensure_client()

        try:
            response = await client.get("/api/v1/streaming/health")
            response.raise_for_status()
            return True
        except Exception:
            return False
