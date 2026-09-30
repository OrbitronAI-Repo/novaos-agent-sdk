"""Client for calling agents over the A2A protocol (spec v1.0.1).

This module used to wrap
BeeAI's ``acp_sdk.client.Client`` and expose ``run_sync`` / ``run_stream``; it
now speaks A2A v1.0 JSON-RPC 2.0 directly, through
:class:`novaos_client_sdk._a2a.A2ATransport`.

**``run_sync`` and ``run_stream`` are gone outright — there are no aliases.**
Per the migration's scope decision (2026-08-26, Ashu Gupta, CAIO) NovaOS does not
stay backward compatible with ACP: no dual-protocol serving, no coexistence
period, no deprecated-but-functional names.  The replacements are not one-for-one
renames either, because A2A's model is task-centric where ACP's was run-centric:

=====================  ================================================
ACP                    A2A
=====================  ================================================
``run_sync``           :meth:`Client.send_message`
``run_stream``         :meth:`Client.send_streaming_message`
``run_safe``           :meth:`Client.send_message_safe`
*(no equivalent)*      :meth:`Client.get_task`
*(no equivalent)*      :meth:`Client.subscribe_to_task`
*(no equivalent)*      :meth:`Client.cancel_task`
*(no equivalent)*      :meth:`Client.list_tasks`
=====================  ================================================

The four bottom rows are the capability A2A adds: an ACP run was fire-and-read,
with no way to re-query it, reconnect to it, stop it, or enumerate what an agent
is working on.  ``list_tasks`` in particular was missing from the migration's
original method list entirely until ADR finding **F13** caught it.

**There is no ``agent=`` parameter any more, and that is the protocol, not a
simplification.**  ACP's ``POST /runs`` carried an agent name because one ACP
server fronted many agents.  A2A has no such field on any operation: an agent
*is* a URL, advertised by its own Agent Card at
``/.well-known/agent-card.json``.  So a :class:`Client` addresses one agent, and
calling a different one means constructing a different client.  Which *skill*
within that agent runs is a separate question, answered by ``skill_id`` — see
:meth:`Client.send_message`.

**Return types are A2A's, not a NovaOS envelope.**  ``run_sync`` returned a
``Run``; ``send_message`` returns the :class:`~novaos_client_sdk.a2a_task.Task`
the agent created (or, in the case the spec allows, a bare
:class:`~novaos_client_sdk.a2a_types.A2AMessage`).  ``Run`` was ACP's result
envelope and had no producer left once ``run_sync`` went, so it was removed
rather than left as a shape nothing fills — a ``Task`` carries everything it did
(``artifacts``, a status) plus the id the four new methods need.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass, field, replace
from typing import Any

from novaos_client_sdk._a2a import A2ATransport
from novaos_client_sdk._http import httpx
from novaos_client_sdk._transport import ClientTransport
from novaos_client_sdk.a2a_agent_card import (
    PROTOCOL_BINDING_GRPC,
    PROTOCOL_BINDING_HTTP_JSON,
    PROTOCOL_BINDING_JSONRPC,
    AgentCard,
)
from novaos_client_sdk.a2a_artifact import Artifact
from novaos_client_sdk.a2a_events import (
    StreamFrame,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
    stream_frame_from_dict,
)
from novaos_client_sdk.a2a_extensions import A2AExtension, ExtensionRegistry
from novaos_client_sdk.a2a_jsonrpc import (
    CONFIG_RETURN_IMMEDIATELY,
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
    SEND_MESSAGE_RESULT_MESSAGE,
    SEND_MESSAGE_RESULT_TASK,
)
from novaos_client_sdk.a2a_push_notification import (
    TaskPushNotificationConfig,
    TaskPushNotificationConfigList,
)
from novaos_client_sdk.a2a_task import (
    INTERRUPT_TASK_STATES,
    TERMINAL_TASK_STATES,
    Task,
    TaskList,
    TaskState,
)
from novaos_client_sdk.a2a_types import A2AMessage, Role, TextPart, as_dict
from novaos_client_sdk.auth import AuthProvider
from novaos_client_sdk.exceptions import (
    InvalidAgentResponseError,
    NovaOSAgentNotFoundError,
    NovaOSAuthenticationError,
    NovaOSConnectionError,
    NovaOSError,
    NovaOSRateLimitError,
    NovaOSServerError,
    NovaOSValidationError,
)
from novaos_client_sdk.interceptors import (
    AfterArgs,
    BeforeArgs,
    ClientCallContext,
    ClientCallInterceptor,
)
from novaos_client_sdk.models import Message

__all__ = ["SKILL_ID_METADATA_KEY", "Client", "ClientConfig", "MessageInput"]

#: Message-metadata key selecting which skill an agent should run.
#:
#: ⚠️ **Shared literal across four implementations** — this client, this SDK's
#: own :data:`novaos_client_sdk.agent_builder.SKILL_ID_METADATA_KEY`, the Galaxy
#: agent scaffold's ``SkillRegistry`` (``templates/server/skills.py``), and
#: the platform's A2A wire contract.  A2A has no
#: dedicated "which skill" field on ``SendMessage``, so every implementation has
#: to agree on where the selector lives or a caller would need to know which kind
#: of pod it is calling.  **Do not rename one side alone.**
#:
#: Declared here rather than imported from ``agent_builder`` on purpose, matching
#: how ``message_builder`` and ``agent_builder`` already duplicate
#: ``NOVAOS_METADATA_CONTENT_TYPE``: importing it would make every client pull in
#: the whole agent-server module — OTel, evaluation, the skill registry — to read
#: one string, and point the dependency from the calling side at the serving
#: side.  ``tests/test_client.py`` asserts the two are identical, so the
#: duplication cannot drift silently.
SKILL_ID_METADATA_KEY = "skillId"

#: Delay between ``GetTask`` calls in :meth:`Client.send_streaming_message`'s
#: polling fallback (``ClientConfig.polling``).  Not part of ``ClientConfig``
#: itself — the change that introduced polling did not ask for a
#: per-client override, so this stays a module constant until one does.
_POLL_INTERVAL_SECONDS = 2.0

#: What a caller may hand to :meth:`Client.send_message` as its message.
MessageInput = A2AMessage | Message | str


@dataclass
class ClientConfig:
    """Per-client defaults, layered underneath each call's own keyword arguments.

    Mirrors the reference ``a2a-python`` ``ClientConfig`` (``src/a2a/client/client.py``),
    trimmed to the fields this SDK consumes.  ``httpx_client`` is not carried:
    every transport here self-constructs its own ``httpx.AsyncClient`` /
    ``grpc.aio.Channel`` on ``__aenter__`` rather than requiring one be
    injected (matching :class:`~novaos_client_sdk._a2a.A2ATransport`'s
    pre-existing shape), so there is nothing to inject. ``grpc_channel_factory``,
    ``supported_protocol_bindings`` and ``use_client_preference`` were left out
    until the transports and selection logic that need them were built;
    they now have been.

    :meth:`Client._apply_config` is the seam these fields act through, rather
    than each reaching into :meth:`Client._send_params` independently.
    ``accepted_output_modes`` and ``push_notification_config`` are both read
    through it; ``streaming`` and ``polling`` are read by the streaming
    fallback, but not through this seam — see
    :meth:`Client._send_streaming_message_fallback`. Tenant propagation
    does not go through here either: a request's tenant is resolved
    once, from the card and the ``tenant=`` constructor argument, not from a
    per-call ``ClientConfig``, so it is applied by
    :meth:`Client._apply_tenant` instead.
    """

    streaming: bool = True
    """Whether this client prefers a streaming response, where the agent's
    capabilities allow a choice."""

    polling: bool = False
    """Whether this client prefers to poll for task updates rather than hold
    the connection open."""

    accepted_output_modes: list[str] = field(default_factory=list)
    """Output MIME types this client accepts by default, sent as
    ``configuration.acceptedOutputModes`` unless a call overrides it with its
    own ``accepted_output_modes=``. Empty means "no preference stated", and an
    empty list emits no key at all — see :meth:`Client.send_message`."""

    push_notification_config: TaskPushNotificationConfig | None = None
    """Push notification registration to apply by default, sent as
    ``configuration.taskPushNotificationConfig`` unless a call overrides it
    with its own ``task_push_notification_config=``. ``None`` (the default)
    means "no default" and emits no key — see :meth:`Client.send_message`.
    Matches the reference ``a2a-python`` ``ClientConfig.push_notification_config``
    and the precedence its ``base_client.py`` applies: an explicit per-call
    value, including an explicit ``None``, is indistinguishable from "not
    passed" on this field — there is no way to positionally say "explicitly no
    config" the way an explicit ``[]`` says it for
    :attr:`accepted_output_modes` — so a caller relying on the config default
    for most calls and wanting to suppress it for one still sends its own
    registration or a task without a config; there is no third state to ask
    for "none, overriding the client's default"."""

    supported_protocol_bindings: list[str] = field(default_factory=list)
    """Protocol bindings this client is willing to speak, most preferred
    first. Empty (the default) means JSON-RPC only —
    :data:`~novaos_client_sdk.a2a_agent_card.PROTOCOL_BINDING_JSONRPC` — which
    is exactly what this client spoke before the other two bindings were added,
    so an unset config changes nothing. Consumed by
    :func:`~novaos_client_sdk.a2a_agent_card.select_interface`, not by
    ``Client`` directly — see
    :meth:`~novaos_client_sdk.discovery.AgentDiscoveryService.to_client`."""

    use_client_preference: bool = False
    """Whether binding selection prefers *this client's* ordering
    (:attr:`supported_protocol_bindings`) over the agent card's own
    (``AgentCard.supported_interfaces`` order). ``False`` (the default) is
    server preference: the first interface the card lists whose binding this
    client also supports wins, so a caller with no opinion gets whatever the
    agent recommends. Matches the reference ``a2a-python`` client's identical
    flag and precedence."""

    grpc_channel_factory: Callable[[str], Any] | None = None
    """Builds the ``grpc.aio.Channel`` for a gRPC interface's target, for
    custom credentials, interceptors, or channel options. ``None`` (the
    default) is a sensible secure/insecure channel chosen from the target's
    scheme — see
    :meth:`~novaos_client_sdk._a2a_grpc.GrpcTransport.__aenter__`. Typed
    ``Callable[[str], Any]`` rather than ``Callable[[str], grpc.aio.Channel]``
    so importing this module never requires the optional ``grpc`` extra to be
    installed."""


class Client:
    """Client for calling one A2A agent.

    The client is an async context manager; the HTTP connection is opened on
    entry and reused for every call made inside it.

    Example:
        ```python
        from novaos_client_sdk.client import Client

        async with Client(base_url="http://my-agent:8000") as client:
            task = await client.send_message("Hello", skill_id="chat")
            print(task.artifacts[0].text if task.artifacts else task.status.state)
        ```
    """

    def __init__(
        self,
        base_url: str,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
        auth: AuthProvider | None = None,
        extensions: Iterable[A2AExtension] | None = None,
        *,
        card: AgentCard | None = None,
        config: ClientConfig | None = None,
        tenant: str | None = None,
        interceptors: list[ClientCallInterceptor] | None = None,
        protocol_binding: str = PROTOCOL_BINDING_JSONRPC,
    ) -> None:
        """Initialize the Client.

        Args:
            base_url: Base URL of the A2A agent (e.g., ``"http://localhost:8000"``).
                Under A2A the URL identifies the agent; there is no per-call
                agent name.
            timeout: Request timeout in seconds.  Applied to the non-streaming
                calls only — see :meth:`send_streaming_message`.
            headers: Additional HTTP headers to include in requests.
            auth: Authentication provider for requests.
            extensions: A2A protocol extensions to activate on every call.
                Their URIs travel in the ``A2A-Extensions``
                header and whatever data each one publishes travels in the
                request ``metadata`` map under its own URI.  Activating an
                extension the agent does not declare is harmless — a conformant
                agent ignores it — but
                :meth:`ExtensionRegistry.negotiate_card` will say so in advance
                if the agent's card has already been fetched.
            card: The agent's :class:`~novaos_client_sdk.a2a_agent_card.AgentCard`,
                if one has already been fetched — :meth:`AgentDiscoveryService.to_client
                <novaos_client_sdk.discovery.AgentDiscoveryService.to_client>` passes
                the card it cached rather than re-fetching it.  ``None`` is a
                normal, fully supported client: a ``Client`` built from nothing
                but a URL never depended on discovery and still does not.
            config: Per-client defaults layered under each call's own keyword
                arguments.  ``None`` behaves exactly like ``ClientConfig()``.
            tenant: Explicit ``tenant`` to send on every request (A2A v1.0.1,
                ``AgentInterface.tenant`` — routing for an agent that
                multiplexes several tenants behind one endpoint).  When
                ``None`` (the default), the tenant is read instead from
                ``card``: the interface whose ``url`` matches ``base_url``, or
                failing that the JSON-RPC interface, whichever declares one.
                Pass this explicitly when constructing a ``Client`` directly
                from a URL with no ``card`` — that client has nothing to read a
                tenant from otherwise — or to override what the card declares.
                An explicit value always wins over the card's.
            interceptors: Cross-cutting behaviour run around
                every call, in list order, through :meth:`_execute` /
                :meth:`_execute_stream` — see
                :class:`~novaos_client_sdk.interceptors.ClientCallInterceptor`.
                ``None`` (the default) runs none, producing the exact same
                request this client made before interceptors existed; passing
                one is how a caller wires in, for example,
                :class:`~novaos_client_sdk.auth_interceptor.AuthInterceptor`
                for a credential that can refresh mid-session, which the
                constructor's own ``auth=`` cannot do — see this class's
                ``__init__`` comment on ``_transport_headers`` below.
            protocol_binding: Which A2A binding this client speaks at
                ``base_url`` — one of
                :data:`~novaos_client_sdk.a2a_agent_card.PROTOCOL_BINDING_JSONRPC`
                (the default, preserving this client's original JSON-RPC-only behavior),
                :data:`~novaos_client_sdk.a2a_agent_card.PROTOCOL_BINDING_HTTP_JSON`
                or :data:`~novaos_client_sdk.a2a_agent_card.PROTOCOL_BINDING_GRPC`.
                :meth:`~novaos_client_sdk.discovery.AgentDiscoveryService.to_client`
                sets this from whichever interface
                :func:`~novaos_client_sdk.a2a_agent_card.select_interface`
                chose; a ``Client`` built directly from a URL defaults to
                JSON-RPC, same as always.

        Raises:
            ValueError: If two extensions claim the same URI, or one declares
                no URI at all.
        """
        self._base_url = base_url
        self._timeout = timeout
        self._headers = headers.copy() if headers else {}
        self._auth = auth
        self._extensions = ExtensionRegistry(extensions or ())
        self._card = card
        self._config = config
        self._tenant = tenant if tenant is not None else self._tenant_from_card(card, base_url)
        self._interceptors: list[ClientCallInterceptor] = list(interceptors) if interceptors else []
        self._protocol_binding = protocol_binding

        # Auth headers are read once, here — unchanged from every version of
        # this client before interceptors existed: a plain `auth=` provider still behaves
        # exactly as it always has, including a provider that would need a
        # fresh Client to hand out a refreshed token. The `interceptors=`
        # escape hatch above adds this instead: an `AuthInterceptor` resolves its
        # headers per call, through `_execute` / `_execute_stream`, not here —
        # so a caller who needs a credential that changes mid-session reaches
        # for that instead of this constructor-time `auth=`, rather than this
        # comment's rule being rewritten out from under them.
        request_headers = self._headers.copy()
        if auth and hasattr(auth, "get_headers"):
            auth_headers = auth.get_headers()
            if auth_headers:
                request_headers.update(auth_headers)

        # The extension header is applied *after* the caller's own, and
        # deliberately wins: `extensions=` is the supported way to activate one,
        # and a hand-written `A2A-Extensions` header that silently overrode it
        # would leave the request activating one set of extensions while
        # publishing another set's metadata.
        request_headers.update(self._extensions.request_headers())

        self._transport: ClientTransport | None = None
        self._transport_headers = request_headers

    @property
    def base_url(self) -> str:
        """Get the base URL of the A2A agent."""
        return self._base_url

    @property
    def timeout(self) -> float | None:
        """Get the request timeout in seconds."""
        return self._timeout

    @property
    def headers(self) -> dict[str, str] | None:
        """Get additional HTTP headers."""
        return self._headers.copy() if self._headers else None

    @property
    def auth(self) -> AuthProvider | None:
        """Get the authentication provider."""
        return self._auth

    @property
    def extensions(self) -> ExtensionRegistry:
        """The A2A protocol extensions this client activates on every call."""
        return self._extensions

    @property
    def card(self) -> AgentCard | None:
        """The agent's Agent Card, if this client was constructed with one.

        ``None`` for a client built from nothing but a URL — discovery is a
        convenience this client can do without, never a precondition.
        """
        return self._card

    @property
    def config(self) -> ClientConfig | None:
        """This client's ``ClientConfig``, if it was constructed with one."""
        return self._config

    @property
    def tenant(self) -> str | None:
        """The ``tenant`` sent on every request, or ``None`` if none applies.

        Either the explicit ``tenant=`` constructor argument, or — failing
        that — the tenant declared by the card's selected interface.  See
        :meth:`__init__`.
        """
        return self._tenant

    @property
    def interceptors(self) -> list[ClientCallInterceptor]:
        """The interceptors this client runs around every call, in call order.

        A copy: mutating the returned list does not change what this client
        runs, the same way :attr:`headers` returns a copy rather than the
        live dict.
        """
        return list(self._interceptors)

    @property
    def protocol_binding(self) -> str:
        """Which A2A binding this client speaks at :attr:`base_url`."""
        return self._protocol_binding

    def _build_transport(self) -> ClientTransport:
        """Construct (but do not open) this client's transport for :attr:`protocol_binding`.

        A plain ``if``/``elif`` rather than a module-level registry dict so
        that ``GrpcTransport`` — behind the optional ``grpc`` extra — is
        imported lazily, only when a caller actually asks for the gRPC
        binding.  A dict built at import time would have to import every
        transport unconditionally to populate it, which is exactly what the
        extra exists to avoid: a plain ``pip install novaos-client-sdk`` must
        never touch ``grpc``.
        """
        if self._protocol_binding == PROTOCOL_BINDING_HTTP_JSON:
            from novaos_client_sdk._a2a_rest import RestTransport

            return RestTransport(
                self._base_url, timeout=self._timeout, headers=self._transport_headers
            )
        if self._protocol_binding == PROTOCOL_BINDING_GRPC:
            from novaos_client_sdk._a2a_grpc import GrpcTransport

            channel_factory = (
                self._config.grpc_channel_factory if self._config is not None else None
            )
            return GrpcTransport(
                self._base_url,
                timeout=self._timeout,
                headers=self._transport_headers,
                channel_factory=channel_factory,
            )
        return A2ATransport(self._base_url, timeout=self._timeout, headers=self._transport_headers)

    async def __aenter__(self) -> Client:
        """Async context manager entry."""
        self._transport = self._build_transport()
        await self._transport.__aenter__()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self._transport:
            await self._transport.__aexit__(exc_type, exc_val, exc_tb)
            self._transport = None

    # ------------------------------------------------------------------
    # Request construction and transport plumbing
    # ------------------------------------------------------------------

    def _require_transport(self) -> ClientTransport:
        """Return the open transport, or raise if the client was never entered."""
        if not self._transport:
            raise NovaOSError(
                "Client is not initialized. Use async context manager or call __aenter__() first."
            )
        return self._transport

    @staticmethod
    def _tenant_from_card(card: AgentCard | None, base_url: str) -> str | None:
        """The tenant declared by *card*'s selected interface, or ``None``.

        "Selected" means the interface this client is actually addressed at —
        the card can list several with different tenants, so the tenant is a
        property of one ``AgentInterface``, not of the card as a whole.
        Matched by URL first, since that is what
        :meth:`~novaos_client_sdk.discovery.AgentDiscoveryService.to_client`
        addresses this client at; a card built and passed in by hand with no
        interface at that exact URL falls back to the JSON-RPC interface, the
        only binding this SDK's transport speaks.
        """
        if card is None:
            return None
        interface = next((i for i in card.supported_interfaces if i.url == base_url), None)
        if interface is None:
            interface = card.interface()
        return interface.tenant if interface else None

    @staticmethod
    def _as_a2a_message(message: MessageInput) -> A2AMessage:
        """Coerce whatever the caller supplied into an A2A ``Message``.

        Three input shapes are accepted, in decreasing order of how much the
        caller has already decided:

        * an :class:`~novaos_client_sdk.a2a_types.A2AMessage` — what
          :class:`~novaos_client_sdk.message_builder.MessageBuilder` produces —
          is used unchanged, **including its role**.  Overriding the role here
          would silently break :meth:`Client.send_hitl_response`, whose message
          is deliberately authored as ``ROLE_USER``.
        * a :class:`~novaos_client_sdk.models.Message`, the SDK's small
          convenience dataclass, is flattened part by part.  Its
          ``content_type`` becomes each text part's real ``mediaType`` field —
          A2A v1.0 has one, so ACP's trick of recording it in metadata is gone.
        * a bare ``str``, for the common case of sending prose.

        A message the caller did not author as an ``A2AMessage`` is given
        ``ROLE_USER``: this client is the calling side of the conversation, and
        A2A distinguishes the two roles on the wire.
        """
        if isinstance(message, A2AMessage):
            return message

        if isinstance(message, str):
            return A2AMessage(parts=[TextPart(text=message)], role=Role.USER)

        return A2AMessage(
            parts=[
                TextPart(
                    text=part.content,
                    media_type=part.content_type,
                    metadata=part.metadata,
                )
                for part in message.parts
            ],
            role=Role.USER,
            metadata=message.metadata,
        )

    def _send_params(
        self,
        message: MessageInput,
        *,
        skill_id: str | None,
        context_id: str | None,
        task_id: str | None,
        metadata: dict[str, Any] | None,
        return_immediately: bool,
        history_length: int | None,
        accepted_output_modes: list[str] | None,
        task_push_notification_config: TaskPushNotificationConfig | None,
    ) -> dict[str, Any]:
        """Build the ``params`` object shared by both ``SendMessage`` variants."""
        a2a_message = self._as_a2a_message(message)

        # Correlators and the skill selector are applied to a *copy* of the
        # caller's message rather than mutated onto it: a caller re-sending one
        # MessageBuilder result to two agents must not find the first call's
        # task id stuck to it.
        overrides: dict[str, Any] = {}
        if context_id is not None:
            overrides["context_id"] = context_id
        if task_id is not None:
            overrides["task_id"] = task_id
        if skill_id is not None:
            overrides["metadata"] = {
                **as_dict(a2a_message.metadata),
                SKILL_ID_METADATA_KEY: skill_id,
            }
        if overrides:
            a2a_message = replace(a2a_message, **overrides)

        configuration: dict[str, Any] = {CONFIG_RETURN_IMMEDIATELY: return_immediately}
        if history_length is not None:
            configuration["historyLength"] = history_length
        self._apply_config(
            configuration,
            accepted_output_modes=accepted_output_modes,
            task_push_notification_config=task_push_notification_config,
        )

        # Extension data goes in the request metadata map under each extension's
        # own URI — A2A's data plane for extensions (ADR F7/G1) — and is written
        # *after* the caller's metadata, so a caller cannot accidentally shadow
        # an activated extension's payload with a key of the same name.  The URI
        # namespace belongs to the extension; a caller with something to say to
        # one says it by configuring the extension.
        request_metadata: dict[str, Any] = dict(metadata) if metadata else {}
        request_metadata.update(self._extensions.request_metadata())

        params: dict[str, Any] = {
            "message": a2a_message.to_dict(),
            "configuration": configuration,
        }
        if request_metadata:
            params["metadata"] = request_metadata
        self._apply_tenant(params)
        return params

    def _apply_tenant(self, params: dict[str, Any]) -> None:
        """Attach this client's resolved tenant to *params*, in place.

        Called by every A2A operation this client sends, not only the ones
        that share :meth:`_send_params` — ``GetTask``, ``ListTasks``,
        ``CancelTask`` and ``SubscribeToTask`` each build their own ``params``
        and must call this too, since A2A v1.0.1 makes ``tenant`` a field on
        every one of them, not only ``SendMessage``.  A no-op when no tenant
        was resolved: an absent ``tenant`` key is what proto3 JSON reads back
        as "none", and emitting an empty string would be a different, wrong
        value rather than the same absence (acceptance criterion 1).

        This is a separate hook from :meth:`_apply_config` rather than one
        more thing that method does: ``tenant`` is a top-level request field,
        not part of ``configuration``, and it is resolved once at
        construction rather than read off a per-call :class:`ClientConfig`.
        """
        if self._tenant:
            params["tenant"] = self._tenant

    def _apply_config(
        self,
        configuration: dict[str, Any],
        *,
        accepted_output_modes: list[str] | None = None,
        task_push_notification_config: TaskPushNotificationConfig | None = None,
    ) -> None:
        """Extend point for :class:`ClientConfig`-driven request shaping.

        Called once per :meth:`_send_params` build, on the ``configuration``
        object before it is attached to the request. This is the seam that
        resolves ``acceptedOutputModes``; ``taskPushNotificationConfig`` is
        resolved the same way rather than through a separate call site reading
        ``self._config.push_notification_config`` on its own. Neither tenant
        propagation nor the streaming fallback extend this
        method: tenant is a top-level request field resolved once at
        construction — see :meth:`_apply_tenant` — and whether to call
        ``SendStreamingMessage`` at all is decided before any
        ``configuration`` object exists — see
        :meth:`Client._should_stream_message`, read at the top of
        :meth:`send_streaming_message` — so it is a control-flow branch
        around :meth:`_send_params`, not a value written into what
        :meth:`_send_params` builds.

        Args:
            configuration: The in-progress ``configuration`` object, mutated
                in place.
            accepted_output_modes: The per-call ``accepted_output_modes``
                argument from :meth:`send_message` /
                :meth:`send_streaming_message`, or ``None`` if the caller did
                not pass one.
            task_push_notification_config: The per-call
                ``task_push_notification_config`` argument from
                :meth:`send_message` / :meth:`send_streaming_message`, or
                ``None`` if the caller did not pass one.

        Precedence, matching the reference ``a2a-python``
        ``_apply_client_config``: an explicit per-call value — including an
        explicit empty list for ``accepted_output_modes`` — wins outright and
        is never merged with the :class:`ClientConfig` default; only when the
        caller passes nothing at all (``None``) does the client's configured
        default apply. Either way, an absent result omits the key rather than
        emitting an explicit empty/null — proto3 JSON drops an empty repeated
        field or an unset message field, and the wire has no way to
        distinguish "unset" from "explicitly nothing" for either.

        ``task_push_notification_config`` has no "explicit empty" state the
        way a list does — see :attr:`ClientConfig.push_notification_config`
        for what that means for a caller that wants to suppress the client's
        default on one call.
        """
        modes = (
            accepted_output_modes
            if accepted_output_modes is not None
            else (self._config.accepted_output_modes if self._config is not None else None)
        )
        if modes:
            configuration["acceptedOutputModes"] = list(modes)

        push_config = (
            task_push_notification_config
            if task_push_notification_config is not None
            else (self._config.push_notification_config if self._config is not None else None)
        )
        if push_config is not None:
            configuration["taskPushNotificationConfig"] = push_config.to_dict()

    @staticmethod
    def _send_message_result(result: Any) -> Task | A2AMessage:
        """Unwrap A2A's ``SendMessageResponse`` oneof.

        The result is ``{"task": …}`` or ``{"message": …}`` — never the object
        bare, which is v0.3's shape.  A bare ``Task`` is nonetheless
        accepted on the read side, for the same reason
        :func:`novaos_client_sdk.a2a_jsonrpc.parse_response` accepts an
        un-enveloped body: a hand-rolled pod server should not be silently
        unreadable.

        That tolerance is narrowed, not unconditional.  ``Task.from_dict``
        never raises — it is the wire parser, and a malformed nested field must
        not abort a read that is otherwise fine — so a response that
        is neither enveloped nor a real bare task (``{}``, a list, an object with
        unrelated keys) would otherwise come back as a ``Task`` with an empty
        ``id`` and ``TASK_STATE_UNSPECIFIED``, indistinguishable from a real one.
        The policy check belongs here, one layer above the parser: only a bare
        fallback that produced an ``id`` is accepted as the hand-rolled-pod case
        this fallback exists for; one that did not is reported as what it is —
        a response with neither a task nor a message.
        """
        obj = as_dict(result)
        if SEND_MESSAGE_RESULT_TASK in obj:
            return Task.from_dict(obj[SEND_MESSAGE_RESULT_TASK])
        if SEND_MESSAGE_RESULT_MESSAGE in obj:
            return A2AMessage.from_dict(as_dict(obj[SEND_MESSAGE_RESULT_MESSAGE]))
        task = Task.from_dict(obj)
        if not task.id:
            raise InvalidAgentResponseError(
                "SendMessage response has neither `task` nor `message`, and no bare task id",
                details={"response": result},
            )
        return task

    @staticmethod
    def _map_grpc_exception(error: Exception) -> NovaOSError | None:
        """Map a bare gRPC transport-level failure, or return ``None``.

        The gRPC counterpart of the ``httpx.HTTPStatusError`` branch in
        :meth:`_map_exception`: a structured A2A domain error never reaches
        here at all — :class:`~novaos_client_sdk._a2a_grpc.GrpcTransport`
        already translates one into the identical
        :class:`~novaos_client_sdk.exceptions.A2AProtocolError` a JSON-RPC or
        REST peer reporting the same failure would raise, and that is caught
        by :meth:`_call`/:meth:`_stream`'s own ``except NovaOSError: raise``
        before this method ever runs. What lands here is everything beneath
        the protocol layer — the peer unreachable, a deadline exceeded — which
        has no A2A error code to translate, only a ``grpc.StatusCode``.

        The import is local and guarded rather than a module-level one: `grpc`
        is the optional ``[grpc]`` extra, so a plain install
        must be able to import this module — and every other exception this
        client can raise — without it. Without the extra installed, this
        always returns ``None``, falling through to the generic mapping
        exactly as it did before gRPC existed.
        """
        try:
            import grpc
        except ImportError:
            return None
        if not isinstance(error, grpc.aio.AioRpcError):
            return None

        code = error.code()
        message = error.details() or str(error)
        details = {"original_error": "AioRpcError", "grpc_status": code.name}

        if code == grpc.StatusCode.NOT_FOUND:
            return NovaOSAgentNotFoundError(f"Agent not found: {message}", details=details)
        if code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
            return NovaOSAuthenticationError(f"Authentication failed: {message}", details=details)
        if code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            return NovaOSRateLimitError(f"Rate limit exceeded: {message}", details=details)
        if code in (
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.FAILED_PRECONDITION,
            grpc.StatusCode.OUT_OF_RANGE,
        ):
            return NovaOSValidationError(f"Validation failed: {message}", details=details)
        if code in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.CANCELLED,
        ):
            return NovaOSConnectionError(f"Connection failed: {message}", details=details)
        if code in (grpc.StatusCode.INTERNAL, grpc.StatusCode.UNKNOWN, grpc.StatusCode.DATA_LOSS):
            return NovaOSServerError(f"Server error: {message}", status_code=500, details=details)
        return NovaOSError(f"Client error: {message}", details=details)

    def _map_exception(self, error: Exception) -> NovaOSError:
        """Map a transport-level exception into this SDK's hierarchy.

        Errors the agent *replied* with do not pass through here — the
        transport already turns a JSON-RPC ``error`` into the right exception
        via :func:`~novaos_client_sdk.exceptions.error_from_json_rpc`.  What this
        maps is the layer beneath: HTTP statuses and socket failures, which are
        not protocol conditions at all.  That split is the point of this
        protocol-neutral root — an HTTP 503 from a load balancer that never
        reached the agent is not an A2A error.

        Args:
            error: Exception raised by the transport.

        Returns:
            Mapped NovaOSError exception.
        """
        error_str = str(error)
        error_type = type(error).__name__
        details = {"original_error": error_type}

        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code
            if status_code == 400:
                return NovaOSValidationError(f"Validation failed: {error_str}", details=details)
            if status_code == 401:
                return NovaOSAuthenticationError(
                    f"Authentication failed: {error_str}", details=details
                )
            if status_code == 403:
                return NovaOSAuthenticationError(f"Permission denied: {error_str}", details=details)
            if status_code == 404:
                return NovaOSAgentNotFoundError(f"Agent not found: {error_str}", details=details)
            if status_code == 429:
                retry_after: float | None = None
                retry_header = error.response.headers.get("Retry-After")
                if retry_header is not None:
                    try:
                        retry_after = float(retry_header)
                    except ValueError:
                        pass
                return NovaOSRateLimitError(
                    f"Rate limit exceeded: {error_str}",
                    retry_after=retry_after,
                    details=details,
                )
            if status_code in (500, 502, 503, 504):
                return NovaOSServerError(
                    f"Server error: {error_str}",
                    status_code=status_code,
                    details=details,
                )
            if 400 <= status_code < 500:
                return NovaOSValidationError(f"Validation failed: {error_str}", details=details)
            if 500 <= status_code < 600:
                return NovaOSServerError(
                    f"Server error: {error_str}",
                    status_code=status_code,
                    details=details,
                )

        grpc_error = self._map_grpc_exception(error)
        if grpc_error is not None:
            return grpc_error

        # `httpx.TransportError` covers every "the request never completed"
        # failure this transport can raise — `ConnectError`, `ReadError`,
        # `ConnectTimeout`, `ReadTimeout`, `PoolTimeout` — and none of them
        # inherit Python's builtin `ConnectionError` or `TimeoutError`.  Matching
        # only the builtins therefore classified a genuinely unreachable agent as
        # a generic `NovaOSError` with `is_retryable` False, so `send_message_safe`
        # would not swallow it and a caller's retry loop would treat a network
        # blip as permanent.  `HTTPStatusError` is checked above and
        # is not a `TransportError`, so a real HTTP status still maps by status.
        if isinstance(error, httpx.TransportError):
            return NovaOSConnectionError(f"Connection failed: {error_str}", details=details)

        # The builtins are kept alongside: a caller can inject either, and
        # `ConnectionRefusedError` is what a test double naturally raises.
        if isinstance(error, ConnectionError | TimeoutError):
            return NovaOSConnectionError(f"Connection failed: {error_str}", details=details)

        return NovaOSError(f"Client error: {error_str}", details=details)

    async def _call(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make one non-streaming call, mapping transport failures.

        Args:
            headers: Extra headers for this call only, on top of the ones
                fixed at construction — see
                :meth:`novaos_client_sdk._a2a.A2ATransport.call`.
        """
        transport = self._require_transport()
        try:
            return await transport.call(method, params, task_id=task_id, headers=headers)
        except NovaOSError:
            # `A2AProtocolError` is a `NovaOSError`, so this re-raises the
            # agent's own protocol errors untouched rather than flattening them
            # into a generic client error.
            raise
        except Exception as e:
            raise self._map_exception(e) from e

    async def _stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[StreamFrame]:
        """Make one streaming call, decoding frames and mapping transport failures.

        A frame :func:`~novaos_client_sdk.a2a_events.stream_frame_from_dict`
        cannot decode is dropped rather than raised — one bad frame must not
        abort a run that is otherwise progressing.  But a stream that produced
        *only* such frames never gave the caller anything, and silently ending
        it looks identical to an agent that legitimately had nothing left to
        say.  So the terminal case is made explicit: if every payload
        this stream ever received was unrecognizable, the last one is reported
        as an :class:`~novaos_client_sdk.exceptions.InvalidAgentResponseError`
        instead of ending the generator with nothing yielded at all. An empty
        stream — no payloads at all — is unaffected; that is not this failure.

        Args:
            headers: As :meth:`_call`.
        """
        transport = self._require_transport()
        try:
            yielded_any = False
            last_undecodable: Any = None
            saw_undecodable = False
            async for payload in transport.stream(method, params, task_id=task_id, headers=headers):
                frame = stream_frame_from_dict(payload)
                if frame is not None:
                    yielded_any = True
                    yield frame
                else:
                    saw_undecodable = True
                    last_undecodable = payload
            if saw_undecodable and not yielded_any:
                raise InvalidAgentResponseError(
                    "Stream produced no readable frame; every payload received "
                    "was unrecognizable",
                    details={"response": last_undecodable},
                )
        except NovaOSError:
            raise
        except Exception as e:
            raise self._map_exception(e) from e

    async def _execute(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        context: ClientCallContext | None = None,
    ) -> Any:
        """Run every interceptor's ``before``/``after`` around one non-streaming call.

        The one execution path every non-streaming operation goes through —
        :meth:`send_message`, :meth:`get_task`,
        :meth:`cancel_task`, :meth:`list_tasks` and the four
        ``TaskPushNotificationConfig`` methods all call this rather
        than :meth:`_call` directly, so an operation added later cannot forget
        to run interceptors and cannot run them in a subtly different order
        than the rest. See :meth:`_execute_stream` for the streaming
        counterpart.

        With no interceptors configured, the ``for`` loops below run zero
        times and ``http_kwargs`` stays empty, so this reduces to exactly
        :meth:`_call` — the request produced is the byte-identical one this
        client's own snapshot test already established.

        A ``before`` that raises propagates immediately, before :meth:`_call`
        — and so before any transport I/O — runs at all: there is nothing for
        a later ``__aexit__`` to have to clean up.
        """
        ctx = context if context is not None else ClientCallContext()
        http_kwargs: dict[str, Any] = {}
        for interceptor in self._interceptors:
            await interceptor.before(
                BeforeArgs(
                    method=method,
                    params=params,
                    http_kwargs=http_kwargs,
                    card=self._card,
                    context=ctx,
                )
            )

        error: Exception | None = None
        result: Any = None
        try:
            result = await self._call(
                method, params, task_id=task_id, headers=http_kwargs.get("headers")
            )
        except Exception as exc:
            error = exc
            raise
        finally:
            for interceptor in self._interceptors:
                await interceptor.after(
                    AfterArgs(
                        method=method,
                        params=params,
                        result=result,
                        error=error,
                        card=self._card,
                        context=ctx,
                    )
                )
        return result

    async def _execute_stream(
        self,
        method: str,
        params: dict[str, Any],
        *,
        task_id: str | None = None,
        context: ClientCallContext | None = None,
    ) -> AsyncIterator[StreamFrame]:
        """Run every interceptor's ``before``/``after`` around one streaming call.

        The streaming counterpart of :meth:`_execute` —
        :meth:`send_streaming_message` and :meth:`subscribe_to_task` both call
        this. ``before`` runs once, ahead of the whole stream; ``after`` runs
        once per frame, matching the reference ``a2a-python``'s
        ``_process_stream``, so an interceptor watching for a mid-stream error
        frame sees each frame as it arrives rather than only once at the end.
        """
        ctx = context if context is not None else ClientCallContext()
        http_kwargs: dict[str, Any] = {}
        for interceptor in self._interceptors:
            await interceptor.before(
                BeforeArgs(
                    method=method,
                    params=params,
                    http_kwargs=http_kwargs,
                    card=self._card,
                    context=ctx,
                )
            )

        try:
            async for frame in self._stream(
                method, params, task_id=task_id, headers=http_kwargs.get("headers")
            ):
                for interceptor in self._interceptors:
                    await interceptor.after(
                        AfterArgs(
                            method=method,
                            params=params,
                            result=frame,
                            error=None,
                            card=self._card,
                            context=ctx,
                        )
                    )
                yield frame
        except Exception as exc:
            for interceptor in self._interceptors:
                await interceptor.after(
                    AfterArgs(
                        method=method,
                        params=params,
                        result=None,
                        error=exc,
                        card=self._card,
                        context=ctx,
                    )
                )
            raise

    # ------------------------------------------------------------------
    # A2A operations
    # ------------------------------------------------------------------

    async def send_message(
        self,
        message: MessageInput,
        *,
        skill_id: str | None = None,
        context_id: str | None = None,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        return_immediately: bool = False,
        history_length: int | None = None,
        accepted_output_modes: list[str] | None = None,
        task_push_notification_config: TaskPushNotificationConfig | None = None,
        context: ClientCallContext | None = None,
    ) -> Task | A2AMessage:
        """Send a message to the agent (A2A ``SendMessage``).

        Replaces ``run_sync``.

        Args:
            message: The message to send — an
                :class:`~novaos_client_sdk.a2a_types.A2AMessage`, the SDK's
                :class:`~novaos_client_sdk.models.Message` convenience
                dataclass, or a plain string.
            skill_id: Which of the agent's skills should run.  A2A has no
                dedicated field for this, so it travels as
                ``message.metadata["skillId"]`` — the one placement every NovaOS
                pod implementation reads.  A single-skill agent does not need it;
                a multi-skill agent rejects a request that names no skill rather
                than guessing.
            context_id: A2A ``contextId``, grouping several tasks into one
                conversation.  This is what ACP called ``session_id``.
            task_id: Continue an existing task rather than starting a new one —
                how a caller answers a task sitting in
                ``TASK_STATE_INPUT_REQUIRED``.
            metadata: Request-level metadata (``params.metadata``), distinct from
                the message's own.
            return_immediately: A2A ``configuration.returnImmediately``.
                ``False`` (the default, and the spec's) asks the agent to hold
                the response until the task is terminal or interrupted; ``True``
                asks for the task back as soon as it exists, to be followed with
                :meth:`subscribe_to_task` or polled with :meth:`get_task`.
                **The sense is inverted from A2A v0.3's ``blocking``** — v1.0
                flipped both the name and its meaning, so a v0.3-era ``True``
                means this ``False``.
            history_length: Hint for how many history messages to include in the
                returned task.  A hint only: §3.7 lets an agent decline to keep
                history at all.
            accepted_output_modes: MIME types this caller can render, sent as
                ``configuration.acceptedOutputModes`` — the client half of A2A
                content negotiation.  Like ``history_length``, this is a
                *request*, not a guarantee: the spec lets the agent decline
                and answer with a mode not on the list.  ``None`` (the
                default) falls back to the :class:`ClientConfig` this client
                was constructed with, if any; an explicit ``[]`` is
                indistinguishable on the wire from omitting the argument
                entirely and is likewise sent as no key at all.  A non-empty
                value here always replaces the ``ClientConfig`` default rather
                than merging with it.
            task_push_notification_config: Register (or replace) a webhook to
                be notified of this task's status changes, sent as
                ``configuration.taskPushNotificationConfig``. ``None`` (the
                default) falls back to the :class:`ClientConfig` this client
                was constructed with, if any — see
                :attr:`ClientConfig.push_notification_config`. Check
                ``card.capabilities.push_notifications`` before relying on
                this: an agent that does not support it answers
                :class:`~novaos_client_sdk.exceptions.PushNotificationNotSupportedError`
                (A2A ``-32003``).
            context: This call's
                :class:`~novaos_client_sdk.interceptors.ClientCallContext`,
                read by this client's ``interceptors=``.
                ``None`` (the default) runs them against a fresh, empty one.

        Returns:
            The :class:`~novaos_client_sdk.a2a_task.Task` the agent created, or
            an :class:`~novaos_client_sdk.a2a_types.A2AMessage` in the case the
            spec allows — an agent answering without creating a task.  Both are
            part of the contract; ``isinstance`` is how a caller tells them
            apart, since that is what the ``oneof`` on the wire means.

        Raises:
            NovaOSError: If the client is not initialized.
            A2AProtocolError: If the agent replied with an A2A protocol error.
            InvalidAgentResponseError: If the response has neither a ``task``
                nor a ``message``, and no bare task id either (A2A ``-32006``)
                — the agent's reply cannot be a valid A2A response at all.
            NovaOSConnectionError: If the connection to the agent fails.
            NovaOSAuthenticationError: If authentication fails.
            NovaOSAgentNotFoundError: If the agent URL answers 404.
            NovaOSValidationError: If request validation fails.
            NovaOSServerError: If the agent returns a server error.
            NovaOSRateLimitError: If the rate limit is exceeded.
        """
        params = self._send_params(
            message,
            skill_id=skill_id,
            context_id=context_id,
            task_id=task_id,
            metadata=metadata,
            return_immediately=return_immediately,
            history_length=history_length,
            accepted_output_modes=accepted_output_modes,
            task_push_notification_config=task_push_notification_config,
        )
        result = await self._execute(METHOD_SEND_MESSAGE, params, task_id=task_id, context=context)
        return self._send_message_result(result)

    async def send_streaming_message(
        self,
        message: MessageInput,
        *,
        skill_id: str | None = None,
        context_id: str | None = None,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        history_length: int | None = None,
        accepted_output_modes: list[str] | None = None,
        task_push_notification_config: TaskPushNotificationConfig | None = None,
        context: ClientCallContext | None = None,
    ) -> AsyncIterator[StreamFrame]:
        """Send a message and stream the agent's updates (A2A ``SendStreamingMessage``).

        Replaces ``run_stream``.  The frames are A2A's ``StreamResponse``
        members rather than ACP's ``RunYield``: a
        :class:`~novaos_client_sdk.a2a_events.TaskStatusUpdateEvent`, a
        :class:`~novaos_client_sdk.a2a_events.TaskArtifactUpdateEvent`, a whole
        :class:`~novaos_client_sdk.a2a_task.Task`, or a bare
        :class:`~novaos_client_sdk.a2a_types.A2AMessage`.

        **Read the task state to know whether the run finished — not the fact
        that iteration stopped.**  A2A v1.0 removed v0.3's ``final`` flag, so a
        stream that ends without a terminal or interrupt state is a *transport*
        failure wearing the shape of a completed one, which is the
        misclassification this check exists to prevent.
        :func:`novaos_client_sdk.a2a_events.is_last_frame` is the check.

        **This method does not always call ``SendStreamingMessage``.**
        :meth:`_should_stream_message` decides first, and this method never
        opens a stream when it says no:

        * ``self.config.streaming is False`` — the caller opted out.
        * ``self.card.capabilities.streaming is False`` — the agent declared it
          does not support this operation.  ``AgentBuilder``
          implements ``SendStreamingMessage`` for real (in
          ``handle_a2a_streaming_request``), so this now happens only for a
          pod whose card generator was told the streaming entry
          point is not wired in front of it — not for every
          ``AgentBuilder``-backed pod unconditionally.
        * **A client with no card still streams.**  Discovery is optional, not
          a precondition — see :meth:`__init__`'s ``card`` parameter — so a
          missing card is not evidence the agent lacks the capability, and
          treating it as evidence would regress every caller who builds a
          ``Client`` directly from a URL.

        When streaming is skipped, :meth:`_send_streaming_message_fallback`
        calls :meth:`send_message` instead and yields its single result as one
        frame, so **the return type never changes** — a caller that only ever
        reads this method's iterator cannot tell the two paths apart except by
        frame count.  If it only got one frame carrying a terminal or interrupt
        task, that is this fallback, not a run that produced nothing.

        **A card can still be wrong, or missing.**  An agent whose card claims
        ``capabilities.streaming = true`` but does not actually route the
        method — or one this client has no card for at all — answers ``-32004``
        (:class:`~novaos_client_sdk.exceptions.UnsupportedOperationError`) or,
        if it does not route the method at all, ``-32601``.  Neither is a run
        failure — fall back to :meth:`send_message` by hand in that case, the
        same way this method's own proactive check does.  A pod may send either
        as an ordinary JSON body rather than as an SSE frame, which the
        transport reads as an answer rather than as an empty stream.

        The two codes land in **different branches** of the exception hierarchy
        on purpose — ``-32004`` is one of A2A's own errors and ``-32601`` is
        JSON-RPC's — so detect this condition by code rather than by type:
        :func:`~novaos_client_sdk.exceptions.jsonrpc_error_code` reads it off
        either, and
        :data:`~novaos_client_sdk.a2a_jsonrpc.METHOD_UNAVAILABLE_ERROR_CODES` is
        the set to test against.

        Args:
            message: As :meth:`send_message`.
            skill_id: As :meth:`send_message`.
            context_id: As :meth:`send_message`.
            task_id: As :meth:`send_message`.
            metadata: As :meth:`send_message`.
            history_length: As :meth:`send_message`.
            accepted_output_modes: As :meth:`send_message`.
            task_push_notification_config: As :meth:`send_message`.
            context: As :meth:`send_message`, except ``after`` runs once per
                frame here rather than once total — see
                :meth:`_execute_stream`.

        Yields:
            Each decoded stream frame — or, on the fallback path, exactly the
            frames :meth:`_send_streaming_message_fallback` produces.

        Raises:
            NovaOSError: If the client is not initialized.
            A2AProtocolError: If the agent replied with one of A2A's own error
                codes, including one sent mid-stream.
            NovaOSValidationError: If the agent replied with a standard JSON-RPC
                error such as ``-32601``, which is a transport-layer condition
                rather than an A2A one.
            NovaOSConnectionError: If the connection to the agent fails.
        """
        if not self._should_stream_message():
            async for frame in self._send_streaming_message_fallback(
                message,
                skill_id=skill_id,
                context_id=context_id,
                task_id=task_id,
                metadata=metadata,
                history_length=history_length,
                accepted_output_modes=accepted_output_modes,
                task_push_notification_config=task_push_notification_config,
                context=context,
            ):
                yield frame
            return

        params = self._send_params(
            message,
            skill_id=skill_id,
            context_id=context_id,
            task_id=task_id,
            metadata=metadata,
            # A streaming caller is asking the agent to hold the connection open
            # and narrate; returning as soon as the task exists would close it
            # before anything had been narrated.
            return_immediately=False,
            history_length=history_length,
            accepted_output_modes=accepted_output_modes,
            task_push_notification_config=task_push_notification_config,
        )
        async for frame in self._execute_stream(
            METHOD_SEND_STREAMING_MESSAGE, params, task_id=task_id, context=context
        ):
            yield frame

    def _should_stream_message(self) -> bool:
        """Decide whether :meth:`send_streaming_message` may call ``SendStreamingMessage``.

        ``True`` unless something concrete says otherwise:

        * ``self._config`` is set and ``self._config.streaming`` is ``False``.
        * ``self._card`` is set and ``self._card.capabilities.streaming`` is
          ``False``.

        A missing config and a missing card both mean "no opinion", not "no" —
        see the class-level note on ``card`` in :meth:`__init__` and
        :attr:`ClientConfig.streaming`'s default.  In particular **no card
        streams**: absence of discovery must not read as absence of the
        capability, or every caller who skips discovery — which is every
        caller who did not opt into it — would silently lose streaming.
        """
        if self._config is not None and not self._config.streaming:
            return False
        if self._card is not None and not self._card.capabilities.streaming:
            return False
        return True

    async def _send_streaming_message_fallback(
        self,
        message: MessageInput,
        *,
        skill_id: str | None,
        context_id: str | None,
        task_id: str | None,
        metadata: dict[str, Any] | None,
        history_length: int | None,
        accepted_output_modes: list[str] | None,
        task_push_notification_config: TaskPushNotificationConfig | None,
        context: ClientCallContext | None = None,
    ) -> AsyncIterator[StreamFrame]:
        """Serve :meth:`send_streaming_message` without opening a stream.

        Two shapes, both preserving the ``AsyncIterator[StreamFrame]`` contract
        so the caller's loop does not need to know which one ran:

        * **Single frame (the default).**  One :meth:`send_message` call, made
          exactly as a streaming caller would want it — held open until the
          task is terminal or interrupted (``return_immediately=False``) — with
          its ``Task`` or ``A2AMessage`` result yielded as the stream's only
          frame.

        * **Polling (``self.config.polling`` is ``True``).**  The initial
          ``send_message`` call instead uses ``return_immediately=True``, so it
          returns as soon as the task exists rather than blocking for it, and
          this method takes over narrating progress: it polls :meth:`get_task`
          every :data:`_POLL_INTERVAL_SECONDS`, stopping at
          :data:`~novaos_client_sdk.a2a_task.TERMINAL_TASK_STATES` or
          :data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES` — the same
          stopping rule :func:`~novaos_client_sdk.a2a_events.is_last_frame`
          checks, so ``TASK_STATE_INPUT_REQUIRED`` stops the loop rather than
          spinning on it.  A state change that is *not* the stopping one yields
          a lightweight :class:`~novaos_client_sdk.a2a_events.TaskStatusUpdateEvent`;
          **the change that stops the loop yields the whole ``Task``**, not a
          status event, because that is the only frame carrying
          ``artifacts`` — a bare status update would silently drop a
          completed task's output, the same loss the single-frame fallback
          and the already-terminal case below both avoid by yielding the
          ``Task`` outright.  An agent that answers without creating a task (a
          bare ``A2AMessage``) has nothing to poll, so that case yields the
          message itself, same as the non-polling shape.

        Either way, at least one frame is always yielded — never a silently
        empty iterator — so a caller looping to
        :func:`~novaos_client_sdk.a2a_events.is_last_frame` still terminates.
        """
        polling = self._config is not None and self._config.polling
        result = await self.send_message(
            message,
            skill_id=skill_id,
            context_id=context_id,
            task_id=task_id,
            metadata=metadata,
            return_immediately=polling,
            history_length=history_length,
            accepted_output_modes=accepted_output_modes,
            task_push_notification_config=task_push_notification_config,
            context=context,
        )

        if not polling or isinstance(result, A2AMessage):
            yield result
            return

        task = result
        if task.status.state in TERMINAL_TASK_STATES or task.status.state in INTERRUPT_TASK_STATES:
            yield task
            return

        last_state = task.status.state
        while True:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            task = await self.get_task(task.id, history_length=history_length, context=context)
            if task.status.state == last_state:
                continue
            last_state = task.status.state
            if last_state in TERMINAL_TASK_STATES or last_state in INTERRUPT_TASK_STATES:
                # The stopping frame carries `artifacts`; a `TaskStatusUpdateEvent`
                # here would report the state change while silently discarding
                # whatever the task produced.
                yield task
                return
            yield TaskStatusUpdateEvent(
                status=task.status, task_id=task.id, context_id=task.context_id
            )

    async def get_task(
        self,
        task_id: str,
        *,
        history_length: int | None = None,
        context: ClientCallContext | None = None,
    ) -> Task:
        """Fetch one authoritative snapshot of a task (A2A ``GetTask``).

        No ACP equivalent: an ACP run could not be re-queried once its response
        had been read.

        Args:
            task_id: The task's A2A id.
            history_length: Hint for how many history messages to include.  A
                hint only — see :meth:`send_message`.
            context: As :meth:`send_message`.

        Returns:
            The task as the agent currently holds it.

        Raises:
            TaskNotFoundError: If the agent does not know this task id
                (A2A ``-32001``).
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"id": task_id}
        if history_length is not None:
            params["historyLength"] = history_length
        self._apply_tenant(params)

        result = await self._execute(METHOD_GET_TASK, params, task_id=task_id, context=context)
        return Task.from_dict(result)

    async def subscribe_to_task(
        self, task_id: str, *, context: ClientCallContext | None = None
    ) -> AsyncIterator[StreamFrame]:
        """Reconnect to a running task's update stream (A2A ``SubscribeToTask``).

        The spec (§3.1.6) guarantees the **first frame is the task's current
        state**, which is what makes this usable as reconnect: a client that
        dropped its stream calls this and is told where the task stands, with no
        cursor to keep.

        **The snapshot is all that is guaranteed.**  A2A does *not* promise to
        replay messages or artifacts that arrived while the connection was down
        (§3.7); those can be permanently lost.  That is a deliberate resolution,
        and it is why replacing a cursor-keyed poll loop with this is a
        simplification for *status* tracking specifically, not for content.

        Args:
            task_id: The task's A2A id.
            context: As :meth:`send_streaming_message`.

        Yields:
            Each decoded stream frame, beginning with the current snapshot.

        Raises:
            TaskNotFoundError: If the agent does not know this task id.
            UnsupportedOperationError: If the agent does not implement the
                method — an ``AgentBuilder`` pod does not.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"id": task_id}
        self._apply_tenant(params)
        async for frame in self._execute_stream(
            METHOD_SUBSCRIBE_TO_TASK, params, task_id=task_id, context=context
        ):
            yield frame

    async def cancel_task(
        self,
        task_id: str,
        *,
        metadata: dict[str, Any] | None = None,
        context: ClientCallContext | None = None,
    ) -> Task:
        """Ask the agent to stop a task (A2A ``CancelTask``).

        No ACP equivalent — an ACP run could not be stopped once dispatched.

        Cancellation is a *request*: the agent decides.  The returned task shows
        what it decided, so a caller reads ``task.status.state`` rather than
        assuming ``TASK_STATE_CANCELED``.  A task already in a terminal state
        cannot be canceled and answers ``-32002``
        (:class:`~novaos_client_sdk.exceptions.TaskNotCancelableError`).

        Args:
            task_id: The task's A2A id.
            metadata: Optional metadata to carry with the request.
            context: As :meth:`send_message`.

        Returns:
            The task as it stands after the agent handled the request.

        Raises:
            TaskNotFoundError: If the agent does not know this task id.
            TaskNotCancelableError: If the task is already terminal.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"id": task_id}
        if metadata:
            params["metadata"] = metadata
        self._apply_tenant(params)

        result = await self._execute(METHOD_CANCEL_TASK, params, task_id=task_id, context=context)
        return Task.from_dict(result)

    async def list_tasks(
        self,
        *,
        context_id: str | None = None,
        status: TaskState | str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        history_length: int | None = None,
        status_timestamp_after: str | None = None,
        include_artifacts: bool | None = None,
        context: ClientCallContext | None = None,
    ) -> TaskList:
        """Enumerate the agent's tasks (A2A ``ListTasks``).

        **This operation has no ACP counterpart at all** — ACP offered no way to
        ask an agent what it was working on — and it was missing from the
        migration's own client-method list until ADR finding F13 caught it.

        Every filter is optional, and every one is a *request*: the spec lets an
        agent implement as much or as little of the filtering as it can, so a
        caller must not assume the returned page is exactly the set it asked for.

        Args:
            context_id: Only tasks belonging to this A2A conversation.
            status: Only tasks in this state.  Accepts a
                :class:`~novaos_client_sdk.a2a_task.TaskState` or its wire
                string.
            page_size: Maximum tasks per page.
            page_token: Continue a previous page, using the ``next_page_token``
                it returned.
            history_length: Hint for how much history to include per task.
            status_timestamp_after: Only tasks whose status changed after this
                RFC 3339 timestamp.
            include_artifacts: Ask for each task's artifacts.  Off by default at
                most agents, since a listing is usually a lightweight overview.
            context: As :meth:`send_message`.

        Returns:
            One page, as a :class:`~novaos_client_sdk.a2a_task.TaskList`.  A
            non-empty ``next_page_token`` means there is more.

        Raises:
            UnsupportedOperationError: If the agent does not implement the
                method (A2A ``-32004``).
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {}
        if context_id is not None:
            params["contextId"] = context_id
        if status is not None:
            params["status"] = str(status)
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token is not None:
            params["pageToken"] = page_token
        if history_length is not None:
            params["historyLength"] = history_length
        if status_timestamp_after is not None:
            params["statusTimestampAfter"] = status_timestamp_after
        if include_artifacts is not None:
            params["includeArtifacts"] = include_artifacts
        self._apply_tenant(params)

        result = await self._execute(METHOD_LIST_TASKS, params, context=context)
        return TaskList.from_dict(result)

    # ------------------------------------------------------------------
    # Push notification config
    # ------------------------------------------------------------------

    async def create_task_push_notification_config(
        self, config: TaskPushNotificationConfig, *, context: ClientCallContext | None = None
    ) -> TaskPushNotificationConfig:
        """Register (or replace) a webhook for one task's status changes.

        A2A ``CreateTaskPushNotificationConfig``. Check
        ``card.capabilities.push_notifications`` before calling — an agent
        that does not support push notification config answers ``-32003``.

        Args:
            config: The registration to create.  ``config.task_id`` is the
                task to watch; ``config.id`` names *this* registration, since
                a task may carry more than one (§3.1.7) — leave it empty to
                let the agent assign one, which is what most agents expect
                for a first registration.  ``config.token`` and
                ``config.authentication.credentials``, if set, are secrets:
                see :mod:`novaos_client_sdk.a2a_push_notification`.
                ``config.tenant``, if set, is sent as given — an explicit
                value on the object itself wins, the same precedence
                :meth:`send_message` gives an explicit per-call argument over
                a client default; only when the caller left it unset does
                this client's own resolved :attr:`tenant` fill it in.
            context: As :meth:`send_message`.

        Returns:
            The registration as the agent stored it — read ``.id`` off this
            if the caller left it empty, since that is the only way to learn
            what the agent assigned.

        Raises:
            PushNotificationNotSupportedError: If the agent does not support
                push notification config (A2A ``-32003``).
            TaskNotFoundError: If the agent does not know ``config.task_id``.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params = config.to_dict()
        if config.tenant is None:
            self._apply_tenant(params)

        result = await self._execute(
            METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
            params,
            task_id=config.task_id or None,
            context=context,
        )
        return TaskPushNotificationConfig.from_dict(result)

    async def get_task_push_notification_config(
        self, task_id: str, config_id: str, *, context: ClientCallContext | None = None
    ) -> TaskPushNotificationConfig:
        """Fetch one push notification registration (A2A ``GetTaskPushNotificationConfig``).

        Args:
            task_id: The task the registration was created against.
            config_id: The registration's own id, from
                :meth:`create_task_push_notification_config` or
                :meth:`list_task_push_notification_configs`.
            context: As :meth:`send_message`.

        Returns:
            The registration as the agent currently holds it.

        Raises:
            PushNotificationNotSupportedError: If the agent does not support
                push notification config (A2A ``-32003``).
            TaskNotFoundError: If the agent does not know this task id, or
                this config id.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"id": config_id, "taskId": task_id}
        self._apply_tenant(params)

        result = await self._execute(
            METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG, params, task_id=task_id, context=context
        )
        return TaskPushNotificationConfig.from_dict(result)

    async def list_task_push_notification_configs(
        self,
        task_id: str,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
        context: ClientCallContext | None = None,
    ) -> TaskPushNotificationConfigList:
        """Enumerate one task's push notification registrations.

        A2A ``ListTaskPushNotificationConfigs`` — **plural**, since v1.0
        pluralised it from v0.3's singular name — a task may carry more
        than one registration (§3.1.7). Paginates the same way
        :meth:`list_tasks` does: a non-empty ``next_page_token`` means there
        is more, fetched by passing it back as ``page_token``.

        Args:
            task_id: The task to enumerate registrations for.
            page_size: Maximum registrations per page.
            page_token: Continue a previous page, using the
                ``next_page_token`` it returned.
            context: As :meth:`send_message`.

        Returns:
            One page, as a
            :class:`~novaos_client_sdk.a2a_push_notification.TaskPushNotificationConfigList`.

        Raises:
            PushNotificationNotSupportedError: If the agent does not support
                push notification config (A2A ``-32003``).
            TaskNotFoundError: If the agent does not know this task id.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"taskId": task_id}
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token is not None:
            params["pageToken"] = page_token
        self._apply_tenant(params)

        result = await self._execute(
            METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS, params, task_id=task_id, context=context
        )
        return TaskPushNotificationConfigList.from_dict(result)

    async def delete_task_push_notification_config(
        self, task_id: str, config_id: str, *, context: ClientCallContext | None = None
    ) -> None:
        """Remove one push notification registration (A2A ``DeleteTaskPushNotificationConfig``).

        Args:
            task_id: The task the registration was created against.
            config_id: The registration's own id.
            context: As :meth:`send_message`.

        Raises:
            PushNotificationNotSupportedError: If the agent does not support
                push notification config (A2A ``-32003``).
            TaskNotFoundError: If the agent does not know this task id, or
                this config id.
            NovaOSError: If the client is not initialized, or the call fails
                below the protocol layer.
        """
        params: dict[str, Any] = {"id": config_id, "taskId": task_id}
        self._apply_tenant(params)
        await self._execute(
            METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG, params, task_id=task_id, context=context
        )

    async def send_message_safe(
        self,
        message: MessageInput,
        *,
        default: Task | A2AMessage | None = None,
        **kwargs: Any,
    ) -> Task | A2AMessage | None:
        """Send a message, returning *default* instead of raising for two errors.

        Replaces ``run_safe``.  Only agent-not-found and connection failures are
        swallowed — both mean "this agent was not reachable", which a caller
        fanning out across several agents can reasonably shrug at.  Everything
        else, **A2A protocol errors included**, propagates: an agent that
        answered ``-32001`` was reached and had something to say, and hiding
        that behind a default would turn a diagnosable failure into an empty
        result.

        Args:
            message: As :meth:`send_message`.
            default: Value to return on the two handled errors.
            **kwargs: Forwarded to :meth:`send_message`.

        Returns:
            The task or message, or *default*.
        """
        try:
            return await self.send_message(message, **kwargs)
        except (NovaOSAgentNotFoundError, NovaOSConnectionError):
            return default

    # ------------------------------------------------------------------
    # Standardized message helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _message_carriers(task: Task) -> list[Artifact | A2AMessage]:
        """Every part-carrying object a task's NovaOS metadata can be attached to.

        Both halves, because A2A splits them and a caller reading only one would
        silently miss the other: ``artifacts`` is *what the task produced*, and
        ``status.message`` is *what the agent had to say about reaching this
        state*.  A HITL prompt lives in the second, an output document in the
        first.  Artifacts come first — outputs before commentary.
        """
        carriers: list[Artifact | A2AMessage] = list(task.artifacts)
        if task.status.message is not None:
            carriers.append(task.status.message)
        return carriers

    def extract_standardized_messages(self, task: Task) -> dict[str, list[Any]]:
        """Extract all standardized NovaOS messages from a task.

        ``MessageParser`` takes an artifact or a message unchanged — it
        dispatches on ``.parts``, which both have, and NovaOS metadata is a
        tagged ``DataPart`` in either.

        Args:
            task: Task returned by :meth:`send_message`, :meth:`get_task`, or
                any other operation on this client.

        Returns:
            Dictionary with keys ``'hitl'``, ``'error'``, ``'progress'``,
            ``'hitl_response'``; each value is a list of extracted messages.
        """
        # Lazy import to avoid circular dependencies
        from novaos_client_sdk.message_parser import MessageParser

        result: dict[str, list[Any]] = {
            "hitl": [],
            "error": [],
            "progress": [],
            "hitl_response": [],
        }

        for carrier in self._message_carriers(task):
            hitl = MessageParser.extract_hitl_message(carrier)
            if hitl:
                result["hitl"].append(hitl)

            error = MessageParser.extract_error_message(carrier)
            if error:
                result["error"].append(error)

            progress = MessageParser.extract_progress_message(carrier)
            if progress:
                result["progress"].append(progress)

            hitl_response = MessageParser.extract_hitl_response(carrier)
            if hitl_response:
                result["hitl_response"].append(hitl_response)

        return result

    def extract_hitl_messages(self, task: Task) -> list[Any]:
        """Extract all HITL messages from a task.

        Args:
            task: Task returned by any operation on this client.

        Returns:
            List of HITLMessage objects found in the task's artifacts and
            status message.
        """
        from novaos_client_sdk.message_parser import MessageParser

        hitl_messages: list[Any] = []
        for carrier in self._message_carriers(task):
            hitl = MessageParser.extract_hitl_message(carrier)
            if hitl:
                hitl_messages.append(hitl)
        return hitl_messages

    def extract_error_messages(self, task: Task) -> list[Any]:
        """Extract all error messages from a task.

        Args:
            task: Task returned by any operation on this client.

        Returns:
            List of ErrorMessage objects found in the task.
        """
        from novaos_client_sdk.message_parser import MessageParser

        error_messages: list[Any] = []
        for carrier in self._message_carriers(task):
            error = MessageParser.extract_error_message(carrier)
            if error:
                error_messages.append(error)
        return error_messages

    def extract_progress_messages(self, task: Task) -> list[Any]:
        """Extract all progress messages from a task.

        Args:
            task: Task returned by any operation on this client.

        Returns:
            List of ProgressMessage objects found in the task.
        """
        from novaos_client_sdk.message_parser import MessageParser

        progress_messages: list[Any] = []
        for carrier in self._message_carriers(task):
            progress = MessageParser.extract_progress_message(carrier)
            if progress:
                progress_messages.append(progress)
        return progress_messages

    async def extract_progress_from_stream(
        self,
        stream: AsyncIterator[StreamFrame],
        on_progress: Callable[[Any], None] | None = None,
    ) -> AsyncIterator[StreamFrame]:
        """Pass a stream through, invoking *on_progress* for each progress message.

        Both frame kinds that can carry one are inspected: an agent may report
        progress as a status update's message or as an artifact chunk, and which
        it chooses is its business rather than the caller's.

        Args:
            stream: The iterator returned by :meth:`send_streaming_message` or
                :meth:`subscribe_to_task`.
            on_progress: Called with each ProgressMessage found.

        Yields:
            Every frame, unchanged.

        Example:
            ```python
            from novaos_client_sdk.a2a_events import TaskArtifactUpdateEvent

            async for frame in client.extract_progress_from_stream(
                client.send_streaming_message("Go", skill_id="research"),
                on_progress=lambda p: print(f"Progress: {p.progress}"),
            ):
                if isinstance(frame, TaskArtifactUpdateEvent):
                    print(frame.artifact.text)
            ```
        """
        from novaos_client_sdk.message_parser import MessageParser

        async for frame in stream:
            carrier: Any = None
            if isinstance(frame, TaskStatusUpdateEvent):
                carrier = frame.status.message
            elif isinstance(frame, TaskArtifactUpdateEvent):
                carrier = frame.artifact

            if carrier is not None:
                progress = MessageParser.extract_progress_message(carrier)
                if progress and on_progress:
                    on_progress(progress)

            yield frame

    def extract_session_id(self, hitl_message: Any) -> str | None:
        """Extract session_id from a HITL message.

        Helper method to get session_id for HITL response correlation.  A NovaOS
        session id is what A2A calls a ``contextId``, so the value this returns
        is what :meth:`send_message`'s ``context_id`` takes.

        Args:
            hitl_message: HITLMessage object

        Returns:
            Session ID if present, None otherwise
        """
        return hitl_message.session_id if hasattr(hitl_message, "session_id") else None

    async def send_hitl_response(
        self,
        action_id: str,
        session_id: str,
        *,
        task_id: str | None = None,
        input_value: str | None = None,
        content: str | None = None,
        metadata: dict[str, Any] | None = None,
        skill_id: str | None = None,
        context: ClientCallContext | None = None,
    ) -> Task | A2AMessage:
        """Send a HITL response back to the agent.

        **Pass ``task_id`` when answering a task that is waiting.**  Under ACP a
        HITL reply was simply another run, correlated only by the NovaOS session
        id inside its payload.  A2A has a first-class notion of continuing a
        task — one sitting in ``TASK_STATE_INPUT_REQUIRED`` resumes when it
        receives a message naming it — so the reply should carry the id of the
        task it answers.  Without one, a conformant agent has no protocol-level
        way to know which paused task this belongs to.

        Args:
            action_id: Action ID from the user's response.
            session_id: Session ID from the original HITL message; sent as the
                A2A ``contextId``.
            task_id: The waiting task's id, from the ``Task`` that asked.
            input_value: Optional input value (for ``input_request`` responses).
            content: Optional human-readable content for the message.
            metadata: Optional additional metadata.
            skill_id: Optional skill selector, if the agent needs one.
            context: As :meth:`send_message`.

        Returns:
            The task as it stands after the agent processed the response.

        Example:
            ```python
            task = await client.send_message("Deploy it", skill_id="ops")
            hitl = client.extract_hitl_messages(task)[0]

            resumed = await client.send_hitl_response(
                action_id="confirm",
                session_id=client.extract_session_id(hitl),
                task_id=task.id,
                content="User confirmed the plan",
            )
            ```
        """
        # Lazy import to avoid circular dependencies
        from novaos_client_sdk.message_builder import MessageBuilder

        message_content = content or f"User responded with action: {action_id}"
        message = MessageBuilder.create_message_with_hitl_response(
            content=message_content,
            action_id=action_id,
            session_id=session_id,
            input_value=input_value,
            metadata=metadata,
        )

        return await self.send_message(
            message,
            skill_id=skill_id,
            context_id=session_id,
            task_id=task_id,
            context=context,
        )

    async def continue_with_hitl_response(
        self,
        hitl_response: Any,
        *,
        task_id: str | None = None,
        content: str | None = None,
        skill_id: str | None = None,
        context: ClientCallContext | None = None,
    ) -> Task | A2AMessage:
        """Continue a task with a HITL response object.

        Convenience wrapper over :meth:`send_hitl_response`.

        Args:
            hitl_response: HITLResponse object.
            task_id: The waiting task's id — see :meth:`send_hitl_response`.
            content: Optional human-readable content for the message.
            skill_id: Optional skill selector.
            context: As :meth:`send_message`.

        Returns:
            The task as it stands after the agent processed the response.
        """
        return await self.send_hitl_response(
            action_id=hitl_response.action_id,
            session_id=hitl_response.session_id,
            task_id=task_id,
            input_value=hitl_response.input_value,
            content=content,
            metadata=getattr(hitl_response, "metadata", None),
            skill_id=skill_id,
            context=context,
        )
