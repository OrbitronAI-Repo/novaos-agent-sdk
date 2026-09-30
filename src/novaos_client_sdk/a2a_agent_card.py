"""A2A ``AgentCard`` types for the NovaOS client SDK.

Fourth module of the A2A type family, after :mod:`novaos_client_sdk.a2a_types`
(the ``Message`` / ``Part`` family),
:mod:`novaos_client_sdk.a2a_task` (the ``Task`` family) and
:mod:`novaos_client_sdk.a2a_artifact` (task outputs).  An **Agent
Card** is how an agent describes itself: what it is called, where it can be
reached, which protocol bindings it speaks, which skills it offers, and which
protocol extensions it has declared.

**Why this exists.**  ACP had no discovery mechanism at
all.  ``discovery.py`` asked an ACP server ``GET /agents`` and got back a list of
free-form ``{name, description, capabilities, metadata}`` objects — a NovaOS
convention, not a protocol, describing agents that all lived behind the one
server that answered.  A2A replaces that with a document at a fixed well-known
path, and nothing in the SDK could read one until this module: a pod
*being* an A2A agent and the gateway *serving* the card were both already
covered elsewhere, but until this module there was no half that *consumes* it.

**The card is the client's entry point, because under A2A an agent is a URL.**
:class:`novaos_client_sdk.client.Client` takes no ``agent=`` parameter — see its
module docstring — so the question "where do I point a client?" has exactly one
protocol-level answer, and it is :attr:`AgentInterface.url` from this document.
:meth:`AgentCard.interface_url` is that lookup.

**Who serves it.**  Not the agent pod.  NovaOS agent pods are never
internet-reachable, so ``AgentBuilder`` deliberately serves no card, the
Galaxy scaffold generates card *content* without a route, and
the platform's execution gateway hosts ``/.well-known/agent-card.json`` on every published
agent's behalf.  This module does not care which of them answered: it
reads whatever is at the well-known path of the base URL it is given, so it
works equally against the NovaOS gateway and against a third-party partner
agent.

**Extensions are modelled, not just carried.**  A2A declares
protocol extensions on ``capabilities.extensions[]``, and this module builds
declaration/negotiation — the ``A2A-Extensions`` header half — on top of that
declaration.  :class:`AgentExtension` and the
:meth:`AgentCard.extension` / :attr:`AgentCard.required_extension_uris`
accessors are the read side that negotiation runs against, so it adds a
header and a policy rather than a second parse of the same field.

**Wire format.**  A2A v1.0 defines ``AgentCard`` in ``specification/a2a.proto``
and every JSON binding carries the proto3 JSON mapping of it: ``lowerCamelCase``
keys, ``supportedInterfaces`` rather than a bare ``url``, and ``securitySchemes``
values wrapped in their ``oneof`` variant name (``httpAuthSecurityScheme``).
The same mapping the Galaxy scaffold's generator emits — the two are checked
against each other by ``tests/test_a2a_agent_card.py``, and against the real
``a2a-sdk`` protobuf by ``tests/test_a2a_v1_conformance.py``, so a field this
module misspells fails a test rather than silently reading as absent.

**Two fields are carried as opaque JSON on purpose:** ``securitySchemes`` and
``securityRequirements``.  ``SecurityScheme`` is a ``oneof`` over five scheme
types (API key, HTTP auth, OAuth2, OpenID Connect, mTLS), and this SDK never
*acts* on the declaration: every call into NovaOS authenticates with the
execution token the gateway injects, so a typed model here would be five
dataclasses that nothing reads.  They round-trip byte-for-byte instead, which is
what a consumer forwarding a card actually needs, and modelling them later is an
additive change rather than a correction.

``signatures`` is carried the same untyped way, but for a different reason:
this module never itself acts on it, and verifying it needs the raw JWS
strings byte-for-byte, not a parsed dataclass.  JWS verification lives in
:mod:`novaos_client_sdk.a2a_card_signing` instead, as an opt-in
check :class:`~novaos_client_sdk.discovery.AgentDiscoveryService` can run after
a card is fetched and before it is cached or trusted.  Opt-in, and off by
default, because every card this SDK has consumed so far arrives over a
trusted path from NovaOS's own gateway, and an unsigned card from that path
must keep working.  It stops being optional the day this SDK fetches its first
third-party card, whose only other guarantee is
:func:`novaos_client_sdk.dns.validate_agent_url`'s SSRF allowlist — a check on
*where* the advertised URL points, not on whether the card that named it was
tampered with.

Not to be confused with :mod:`novaos_client_sdk.agent_card`,
the ``python -m novaos_client_sdk.agent_card`` entry point that *emits* a
running agent's card as JSON — this module only defines the ``AgentCard``
type that emitter serializes.
"""

from dataclasses import dataclass, field
from typing import Any, Final

from .a2a_types import as_bool, as_dict, as_str, as_str_list

__all__ = [
    "AGENT_CARD_WELL_KNOWN_PATH",
    "PROTOCOL_BINDING_GRPC",
    "PROTOCOL_BINDING_HTTP_JSON",
    "PROTOCOL_BINDING_JSONRPC",
    "AgentCapabilities",
    "AgentCard",
    "AgentExtension",
    "AgentInterface",
    "AgentProvider",
    "AgentSkill",
    "select_interface",
]

#: Path an A2A Agent Card is published at, relative to the agent's base URL
#: (A2A v1.0 §"Agent Discovery").  Matches ``a2a-sdk``'s own
#: ``AGENT_CARD_WELL_KNOWN_PATH`` — asserted in the conformance tests, since a
#: client that guesses this path wrong finds no agent anywhere.
AGENT_CARD_WELL_KNOWN_PATH: Final = "/.well-known/agent-card.json"

#: ``AgentInterface.protocolBinding`` for A2A over JSON-RPC 2.0 — the binding
#: every NovaOS surface speaks: ``AgentBuilder`` pods, the Galaxy scaffold's
#: generated server, and the gateway's ``POST /a2a/v1/agents/:agentId``.
PROTOCOL_BINDING_JSONRPC: Final = "JSONRPC"

#: ``AgentInterface.protocolBinding`` for A2A over gRPC.  Nothing in NovaOS
#: *serves* it — no NovaOS pod, gateway route or scaffold speaks it — but this
#: SDK can *call* it via ``GrpcTransport`` (behind the optional
#: ``grpc`` extra), for a third-party agent that advertises only this binding.
PROTOCOL_BINDING_GRPC: Final = "GRPC"

#: ``AgentInterface.protocolBinding`` for A2A over HTTP/REST.  Spelled with a
#: ``+``, not an underscore — this is the spec's own value, and a client that
#: normalises it will not match a conformant card.
PROTOCOL_BINDING_HTTP_JSON: Final = "HTTP+JSON"


@dataclass
class AgentExtension:
    """A2A ``AgentExtension`` — one protocol extension an agent declares.

    The declaration half of A2A's extension mechanism.  :attr:`required` is the
    field that carries obligation: an agent may *offer* an extension a caller
    can ignore, or *demand* one, and a caller that invokes an agent without
    activating a required extension is answered ``-32008``
    (:class:`~novaos_client_sdk.exceptions.ExtensionSupportRequiredError`).
    Extension negotiation turns this declaration into the ``A2A-Extensions``
    request header; this module only reports what was declared.
    """

    uri: str = ""
    description: str | None = None
    required: bool = False
    params: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {"uri": self.uri}
        if self.description:
            out["description"] = self.description
        if self.required:
            out["required"] = True
        if self.params:
            out["params"] = self.params
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AgentExtension":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        return cls(
            uri=as_str(obj.get("uri")),
            description=as_str(obj.get("description")) or None,
            required=as_bool(obj.get("required")),
            params=dict(as_dict(obj.get("params"))) or None,
        )


@dataclass
class AgentCapabilities:
    """A2A ``AgentCapabilities`` — optional protocol features an agent supports.

    Every field is a *negative-by-default* declaration: an absent capabilities
    block and one with all three booleans ``false`` mean the same thing, because
    proto3 JSON cannot tell an omitted boolean from a ``false`` one.  So a caller
    reads "streaming is not offered" rather than "streaming is unknown", and
    there is no third state to represent.
    """

    streaming: bool = False
    push_notifications: bool = False
    extended_agent_card: bool = False
    extensions: list[AgentExtension] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape.

        The three booleans are always emitted, even when ``false``.  Proto3 JSON
        would let them be omitted, but a card that states its non-support is
        materially easier to debug than one that is silent about it — and it is
        what the Galaxy scaffold's generator writes, so emitting the same keys
        keeps a card read here and re-emitted byte-identical to its source.
        """
        out: dict[str, Any] = {
            "streaming": self.streaming,
            "pushNotifications": self.push_notifications,
            "extendedAgentCard": self.extended_agent_card,
        }
        if self.extensions:
            out["extensions"] = [e.to_dict() for e in self.extensions]
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AgentCapabilities":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        extensions = obj.get("extensions")
        return cls(
            streaming=as_bool(obj.get("streaming")),
            push_notifications=as_bool(obj.get("pushNotifications", obj.get("push_notifications"))),
            extended_agent_card=as_bool(
                obj.get("extendedAgentCard", obj.get("extended_agent_card"))
            ),
            extensions=(
                [AgentExtension.from_dict(e) for e in extensions if isinstance(e, dict)]
                if isinstance(extensions, list)
                else []
            ),
        )


@dataclass
class AgentInterface:
    """A2A ``AgentInterface`` — one address at which an agent answers.

    A list rather than a single ``url`` because one agent may serve several
    protocol bindings at different addresses.  :attr:`protocol_binding` is what
    tells them apart, and a client must pick the one it can actually speak
    rather than the first entry: reading ``supportedInterfaces[0].url`` and
    POSTing JSON-RPC at a gRPC endpoint is the failure this field exists to
    prevent.
    """

    url: str = ""
    protocol_binding: str = PROTOCOL_BINDING_JSONRPC
    protocol_version: str | None = None
    tenant: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape."""
        out: dict[str, Any] = {
            "url": self.url,
            "protocolBinding": self.protocol_binding,
        }
        if self.protocol_version:
            out["protocolVersion"] = self.protocol_version
        if self.tenant:
            out["tenant"] = self.tenant
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AgentInterface":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        return cls(
            url=as_str(obj.get("url")),
            protocol_binding=(
                as_str(obj.get("protocolBinding", obj.get("protocol_binding")))
                or PROTOCOL_BINDING_JSONRPC
            ),
            protocol_version=as_str(obj.get("protocolVersion", obj.get("protocol_version")))
            or None,
            tenant=as_str(obj.get("tenant")) or None,
        )


@dataclass
class AgentProvider:
    """A2A ``AgentProvider`` — who publishes the agent.

    Both fields are REQUIRED by the spec when the block is present at all, which
    is why the Galaxy generator omits the whole block for an author with no URL
    rather than emitting half of one.
    """

    organization: str = ""
    url: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape."""
        return {"organization": self.organization, "url": self.url}

    @classmethod
    def from_dict(cls, data: Any) -> "AgentProvider":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        return cls(
            organization=as_str(obj.get("organization")),
            url=as_str(obj.get("url")),
        )


@dataclass
class AgentSkill:
    """A2A ``AgentSkill`` — one named thing an agent can do.

    **A skill is not an ACP agent, and the difference is why discovery changed
    shape.**  An ACP server fronted many separately named agents, so
    ``GET /agents`` returned a list and a caller picked a name from it.  A2A
    models one agent whose card advertises many skills, so the list moved
    *inside* the card and the selector moved onto the request — see
    :data:`novaos_client_sdk.client.SKILL_ID_METADATA_KEY`, which is where
    :attr:`id` is sent when calling.
    """

    id: str = ""
    name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    input_modes: list[str] = field(default_factory=list)
    output_modes: list[str] = field(default_factory=list)
    security_requirements: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape.

        ``id``, ``name``, ``description`` and ``tags`` are always emitted: the
        spec marks all four REQUIRED, and omitting an empty ``tags`` would
        produce a skill a conformant peer rejects.
        """
        out: dict[str, Any] = {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
        }
        if self.examples:
            out["examples"] = list(self.examples)
        if self.input_modes:
            out["inputModes"] = list(self.input_modes)
        if self.output_modes:
            out["outputModes"] = list(self.output_modes)
        if self.security_requirements:
            out["securityRequirements"] = [dict(r) for r in self.security_requirements]
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AgentSkill":
        """Create from the A2A JSON wire shape."""
        obj = as_dict(data)
        requirements = obj.get("securityRequirements", obj.get("security_requirements"))
        return cls(
            id=as_str(obj.get("id")),
            name=as_str(obj.get("name")),
            description=as_str(obj.get("description")),
            tags=as_str_list(obj.get("tags")) or [],
            examples=as_str_list(obj.get("examples")) or [],
            input_modes=as_str_list(obj.get("inputModes", obj.get("input_modes"))) or [],
            output_modes=as_str_list(obj.get("outputModes", obj.get("output_modes"))) or [],
            security_requirements=(
                [dict(r) for r in requirements if isinstance(r, dict)]
                if isinstance(requirements, list)
                else []
            ),
        )


@dataclass
class AgentCard:
    """A2A ``AgentCard`` (spec v1.0.1) — an agent's self-description.

    Construction never validates, and :meth:`from_dict` never raises.  This is
    the same rule the rest of the A2A type family follows and for the same
    reason that applies to the message types: ``from_dict`` reads
    untrusted wire data, so a validating parser turns a peer's malformed payload
    into an unhandled exception at the boundary instead of an error the caller
    can handle.  Deciding that a *fetched* document is too broken to be a card
    is policy, and it lives one layer up in
    :class:`novaos_client_sdk.discovery.AgentDiscoveryService`, which has the URL
    and the HTTP context needed to say anything useful about it.
    """

    name: str = ""
    description: str = ""
    version: str = ""
    supported_interfaces: list[AgentInterface] = field(default_factory=list)
    capabilities: AgentCapabilities = field(default_factory=AgentCapabilities)
    default_input_modes: list[str] = field(default_factory=list)
    default_output_modes: list[str] = field(default_factory=list)
    skills: list[AgentSkill] = field(default_factory=list)
    provider: AgentProvider | None = None
    documentation_url: str | None = None
    icon_url: str | None = None
    security_schemes: dict[str, Any] = field(default_factory=dict)
    security_requirements: list[dict[str, Any]] = field(default_factory=list)
    signatures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def extensions(self) -> list[AgentExtension]:
        """Every protocol extension this agent declares.

        Lifted from ``capabilities.extensions`` so a caller negotiating
        extensions reads one attribute rather than reaching two
        levels into the card for the field it actually wants.
        """
        return self.capabilities.extensions

    @property
    def extension_uris(self) -> list[str]:
        """The URI of every declared extension, in declaration order."""
        return [e.uri for e in self.capabilities.extensions if e.uri]

    @property
    def required_extension_uris(self) -> list[str]:
        """The URI of every extension the agent marks ``required``.

        The set a caller must activate to be answered at all: invoking without
        one is what A2A's ``-32008``
        (:class:`~novaos_client_sdk.exceptions.ExtensionSupportRequiredError`)
        reports.  Extension negotiation starts from this list.
        """
        return [e.uri for e in self.capabilities.extensions if e.uri and e.required]

    @property
    def skill_ids(self) -> list[str]:
        """The id of every advertised skill, in card order."""
        return [s.id for s in self.skills if s.id]

    def extension(self, uri: str) -> AgentExtension | None:
        """Return the declared extension with this URI, or ``None``.

        Args:
            uri: The extension's URI, matched exactly.  A2A extension URIs are
                opaque identifiers, so no normalisation is applied — two URIs
                that differ only by a trailing slash are two extensions.
        """
        return next((e for e in self.capabilities.extensions if e.uri == uri), None)

    def supports_extension(self, uri: str) -> bool:
        """Is this extension declared on the card?"""
        return self.extension(uri) is not None

    def skill(self, skill_id: str) -> AgentSkill | None:
        """Return the advertised skill with this id, or ``None``.

        Args:
            skill_id: The value a caller would send as
                :data:`novaos_client_sdk.client.SKILL_ID_METADATA_KEY`.
        """
        return next((s for s in self.skills if s.id == skill_id), None)

    def interface(self, protocol_binding: str = PROTOCOL_BINDING_JSONRPC) -> AgentInterface | None:
        """Return the *first* interface serving this protocol binding, or ``None``.

        For picking among several bindings a card offers, by preference —
        this SDK's or the card's own — use :func:`select_interface` instead;
        this method is for the narrower "does this card offer binding X at
        all" question, and returns the first match if a card lists more than
        one interface for the same binding (different tenants, say).

        Args:
            protocol_binding: One of :data:`PROTOCOL_BINDING_JSONRPC`,
                :data:`PROTOCOL_BINDING_GRPC` or
                :data:`PROTOCOL_BINDING_HTTP_JSON`.  Defaults to JSON-RPC.
        """
        return next(
            (i for i in self.supported_interfaces if i.protocol_binding == protocol_binding),
            None,
        )

    def interface_url(self, protocol_binding: str = PROTOCOL_BINDING_JSONRPC) -> str | None:
        """Return the URL to point a client at, or ``None`` if none is offered.

        ``None`` rather than a fallback to a different binding: a caller
        asking for one specific binding by name wants that binding or nothing,
        and silently substituting another would turn a clear "not offered"
        into a confusing protocol error at the first request.  Picking *among*
        several bindings a card offers is :func:`select_interface`'s job, not
        this method's.
        """
        interface = self.interface(protocol_binding)
        return interface.url if interface and interface.url else None

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase).

        Optional fields are omitted when empty rather than sent as ``null``,
        matching the rest of the A2A types.  The REQUIRED ones — ``name``,
        ``description``, ``version``, ``supportedInterfaces``, ``capabilities``,
        ``defaultInputModes``, ``defaultOutputModes`` and ``skills`` — are always
        emitted, so a card that lost a required field on the way in does not
        quietly lose it again on the way out.
        """
        out: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "supportedInterfaces": [i.to_dict() for i in self.supported_interfaces],
            "capabilities": self.capabilities.to_dict(),
            "defaultInputModes": list(self.default_input_modes),
            "defaultOutputModes": list(self.default_output_modes),
            "skills": [s.to_dict() for s in self.skills],
        }
        if self.provider is not None:
            out["provider"] = self.provider.to_dict()
        if self.documentation_url:
            out["documentationUrl"] = self.documentation_url
        if self.icon_url:
            out["iconUrl"] = self.icon_url
        if self.security_schemes:
            out["securitySchemes"] = dict(self.security_schemes)
        if self.security_requirements:
            out["securityRequirements"] = [dict(r) for r in self.security_requirements]
        if self.signatures:
            out["signatures"] = [dict(s) for s in self.signatures]
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "AgentCard":
        """Create from the A2A JSON wire shape.

        Accepts the JSON binding's ``camelCase`` keys and the gRPC binding's
        ``snake_case`` equivalents, and coerces every field it reads, for the
        reason :meth:`novaos_client_sdk.a2a_types.A2AMessage.from_dict` records:
        tolerance that yields an object :meth:`to_dict` cannot re-serialize is a
        trap, not a kindness.

        Args:
            data: An A2A Agent Card JSON object.  Typed ``Any`` because this is
                public API over untrusted wire data.
        """
        obj = as_dict(data)

        interfaces = obj.get("supportedInterfaces", obj.get("supported_interfaces"))
        skills = obj.get("skills")
        requirements = obj.get("securityRequirements", obj.get("security_requirements"))
        signatures = obj.get("signatures")
        provider = obj.get("provider")

        return cls(
            name=as_str(obj.get("name")),
            description=as_str(obj.get("description")),
            version=as_str(obj.get("version")),
            supported_interfaces=(
                [AgentInterface.from_dict(i) for i in interfaces if isinstance(i, dict)]
                if isinstance(interfaces, list)
                else []
            ),
            capabilities=AgentCapabilities.from_dict(obj.get("capabilities")),
            default_input_modes=(
                as_str_list(obj.get("defaultInputModes", obj.get("default_input_modes"))) or []
            ),
            default_output_modes=(
                as_str_list(obj.get("defaultOutputModes", obj.get("default_output_modes"))) or []
            ),
            skills=(
                [AgentSkill.from_dict(s) for s in skills if isinstance(s, dict)]
                if isinstance(skills, list)
                else []
            ),
            # A provider that arrived as something other than an object becomes
            # no provider at all, rather than an empty one: `AgentProvider`'s
            # two fields are both REQUIRED, so an empty block is a payload no
            # conformant peer parses, while an absent one is valid.
            provider=AgentProvider.from_dict(provider) if isinstance(provider, dict) else None,
            documentation_url=as_str(obj.get("documentationUrl", obj.get("documentation_url")))
            or None,
            icon_url=as_str(obj.get("iconUrl", obj.get("icon_url"))) or None,
            security_schemes=dict(as_dict(obj.get("securitySchemes", obj.get("security_schemes")))),
            security_requirements=(
                [dict(r) for r in requirements if isinstance(r, dict)]
                if isinstance(requirements, list)
                else []
            ),
            signatures=(
                [dict(s) for s in signatures if isinstance(s, dict)]
                if isinstance(signatures, list)
                else []
            ),
        )


def select_interface(
    interfaces: list[AgentInterface],
    supported_bindings: list[str],
    *,
    use_client_preference: bool = False,
) -> AgentInterface | None:
    """Pick the interface to call, from a card's several and a client's several.

    Before multi-binding support existed, :meth:`AgentDiscoveryService.to_client
    <novaos_client_sdk.discovery.AgentDiscoveryService.to_client>` looked for
    exactly one binding (JSON-RPC) and gave up if the card did not offer it.
    Once a client can speak more than one binding, "which interface" has two
    reasonable answers depending on whose ordering should win, and this
    function is both — the flag says which:

    * **Server preference (``use_client_preference=False``, the default).**
      Walk *interfaces* in the card's own listed order; return the first
      whose binding is anywhere in *supported_bindings*.  The agent's
      preference decides which binding is used; the client's list is only a
      filter of what it is able to speak at all.
    * **Client preference (``use_client_preference=True``).**  Walk
      *supported_bindings* in the *client's* order instead; return the first
      interface matching each, in turn.  The client's own priority decides.

    Mirrors the reference ``a2a-python`` ``ClientFactory.create``'s identical
    precedence, minus its protocol-*version*-priority tiebreaking (picking
    among several interfaces that already share one binding but differ in
    ``protocol_version``) — not needed here, since no NovaOS surface or
    calling convention this ticket covers publishes more than one version per
    binding; :meth:`AgentInterface.protocol_version` handling is otherwise
    unchanged.

    Args:
        interfaces: A card's ``supported_interfaces``, in the order the card
            lists them.
        supported_bindings: The bindings a client is able to speak, most
            preferred first.  An empty list matches nothing — pass
            ``[PROTOCOL_BINDING_JSONRPC]`` for this SDK's original JSON-RPC-only behavior,
            which is what an unset :class:`~novaos_client_sdk.client.ClientConfig`
            resolves to.
        use_client_preference: See above.

    Returns:
        The chosen :class:`AgentInterface`, or ``None`` if no interface's
        binding is in *supported_bindings* at all.
    """
    if use_client_preference:
        for binding in supported_bindings:
            match = next((i for i in interfaces if i.protocol_binding == binding), None)
            if match is not None:
                return match
        return None

    return next((i for i in interfaces if i.protocol_binding in supported_bindings), None)
