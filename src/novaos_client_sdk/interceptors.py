"""Client-side call interception seam.

**Why this exists.** :class:`novaos_client_sdk.client.Client` used to have exactly
one place a caller could act on a request or a response: the
:class:`~novaos_client_sdk.auth.AuthProvider` passed to the constructor, read
once, at construction (see ``Client.__init__``'s docstring). That is enough for
a static credential, but not for one that needs to change mid-session — a
client-credentials OAuth2 token expires, and there is no hook to refresh it
short of throwing away the ``Client`` (and its connection pool) and building a
new one. This module is that hook, generalized: any cross-cutting behaviour a
caller wants to run around every request — not just auth — gets one seam
instead of one bespoke constructor argument per concern.

Mirrors the reference ``a2a-python`` client (``src/a2a/client/interceptors.py``,
``src/a2a/client/client.py``'s ``ClientCallContext``), trimmed to what this
SDK's JSON-RPC-shaped client needs: the reference's interceptor sees a typed
protobuf request object, where this one sees the ``(method, params)`` pair
:class:`~novaos_client_sdk.client.Client` already builds for the wire.

**One shared execution path, not ten hand-rolled ones.**
:meth:`novaos_client_sdk.client.Client._execute` and
:meth:`~novaos_client_sdk.client.Client._execute_stream` are the only two
places that walk the interceptor list — every operation the client exposes
routes through one of the two, so a new operation cannot forget to run
interceptors and two operations cannot silently disagree on when ``before``
runs relative to ``after``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from novaos_client_sdk.a2a_agent_card import AgentCard

__all__ = ["AfterArgs", "BeforeArgs", "ClientCallContext", "ClientCallInterceptor"]


@dataclass
class ClientCallContext:
    """Per-call state threaded through every interceptor for one client call.

    One instance covers one call to :class:`~novaos_client_sdk.client.Client` —
    including, for a streaming call, every frame of it — so an interceptor can
    stash something in :attr:`state` in ``before`` and read it back in
    ``after``. Passing ``None`` (the default on every ``Client`` method) is a
    normal, fully supported call: a fresh :class:`ClientCallContext` is created
    for it, exactly as empty as one the caller built themselves.
    """

    state: dict[str, Any] = field(default_factory=dict)
    """Mutable scratch space, private to this one call. An interceptor that
    needs to correlate ``before`` and ``after`` — a request timestamp, a
    session id looked up once — keeps it here rather than as instance state on
    the interceptor itself, which would leak across concurrent calls on the
    same :class:`~novaos_client_sdk.client.Client`."""

    timeout: float | None = None
    """Per-call timeout override. Unread by the client today — carried here so
    an interceptor or a future ticket has somewhere to put one without another
    signature change."""

    service_parameters: dict[str, Any] | None = None
    """Free-form parameters a caller wants visible to every interceptor for
    this call, distinct from :attr:`state`: this is set once by the caller
    before the call starts, where ``state`` is for interceptors to read and
    write among themselves."""


@dataclass
class BeforeArgs:
    """What :meth:`ClientCallInterceptor.before` sees, ahead of one call.

    ``http_kwargs`` is mutated in place, not returned: several interceptors run
    in sequence over the same call, and each one adding to a shared dict is how
    a second interceptor's headers can sit alongside a first's without either
    having to know the other ran. Today the only key the transport reads is
    ``"headers"`` (see :meth:`novaos_client_sdk._a2a.A2ATransport.call`), kept
    as a generic ``dict[str, Any]`` rather than a bare ``headers=`` field so a
    later interceptor (a per-call timeout override, say) has somewhere to put
    it without another signature change here.
    """

    method: str
    """The A2A JSON-RPC method about to be called, e.g. ``"SendMessage"``."""

    params: dict[str, Any]
    """The request's ``params`` object, already built by the ``Client``
    method — read-only in practice: mutating it here would change what the
    agent sees without the interceptor's own record (``after``) reflecting
    that, so an interceptor that needs to change the request is a bigger
    change than this ticket, not a use of this field."""

    http_kwargs: dict[str, Any]
    """Extra keyword arguments to merge into the transport call. Mutate this in
    place to add to it."""

    card: AgentCard | None
    """The agent's Agent Card, if the :class:`~novaos_client_sdk.client.Client`
    was constructed with one — see
    :attr:`novaos_client_sdk.client.Client.card`. ``None`` under exactly the
    same conditions it is ``None`` there."""

    context: ClientCallContext
    """This call's context — the caller's own if one was passed to the
    ``Client`` method, else a fresh one created for this call alone."""


@dataclass
class AfterArgs:
    """What :meth:`ClientCallInterceptor.after` sees, once one call — or, for a
    streaming call, one frame of it — has answered.

    Exactly one of ``result`` / ``error`` is meaningful for a given call:
    ``error`` is ``None`` on success and the exception that was raised
    otherwise, already mapped through
    :meth:`novaos_client_sdk.client.Client._map_exception` where it came from
    below the protocol layer — so an interceptor sees a
    :class:`~novaos_client_sdk.exceptions.NovaOSAuthenticationError` for a 401,
    not a bare ``httpx.HTTPStatusError``, matching what a caller catching the
    same call directly would see.
    """

    method: str
    """As :attr:`BeforeArgs.method`."""

    params: dict[str, Any]
    """As :attr:`BeforeArgs.params`."""

    result: Any
    """The call's raw JSON-RPC result, or one decoded stream frame for a
    streaming call. ``None`` when ``error`` is set."""

    error: Exception | None
    """The exception the call raised, or ``None`` on success."""

    card: AgentCard | None
    """As :attr:`BeforeArgs.card`."""

    context: ClientCallContext
    """As :attr:`BeforeArgs.context`."""


class ClientCallInterceptor:
    """Cross-cutting behaviour that runs around every ``Client`` call.

    Both methods default to a no-op, so an interceptor that only cares about
    one side of a call — :class:`~novaos_client_sdk.auth_interceptor.AuthInterceptor`
    implements ``before`` only — does not have to write an empty override for
    the other. Neither method returns a value: ``before`` acts by mutating
    ``args.http_kwargs``, and ``after`` is a notification, not a filter — it
    cannot change what the caller already received.

    Example:
        ```python
        class RequestLogger(ClientCallInterceptor):
            async def before(self, args: BeforeArgs) -> None:
                print(f"-> {args.method}")

            async def after(self, args: AfterArgs) -> None:
                print(f"<- {args.method}: {'error' if args.error else 'ok'}")
        ```
    """

    async def before(self, args: BeforeArgs) -> None:
        """Run ahead of one call. Override to inspect or add to
        ``args.http_kwargs`` — see :class:`BeforeArgs`.

        Raising here aborts the call before any transport I/O has started: see
        :meth:`novaos_client_sdk.client.Client._execute`.
        """
        return None

    async def after(self, args: AfterArgs) -> None:
        """Run once one call — or one streamed frame — has answered. Override
        to observe ``args.result`` / ``args.error`` — see :class:`AfterArgs`.
        """
        return None
