"""A2A protocol extensions: declaration, negotiation and dispatch.

A2A lets an agent do more than the base protocol describes without forking it.
An **extension** is a URI-identified agreement between a caller and an agent —
the agent *declares* it on its Agent Card, the caller *activates* it with a
request header, and whatever data the agreement carries travels in the request's
``metadata`` map under the extension's own URI.  This module is all three halves
of that, and nothing more: it knows how to declare, negotiate and dispatch
extensions, and it knows nothing about any particular one.

**Why the SDK needs it.**  The migration's design analysis
found *zero* references to "extension"
anywhere in this SDK — the mechanism did not exist, so the ``user-context/v1``
extension chosen for secret delivery had
nothing to be built on.  An earlier stage landed the read side (:class:`AgentExtension` and
the ``AgentCard.extension*`` accessors); this module is the half that *acts* on
a declaration.

**Open for extension, closed for modification (an explicit OCP
requirement of this design).**  Adding an extension must never mean editing this file.  So the
core here dispatches through :class:`A2AExtension`, and a new extension is a new
class in a new module that sets a ``uri`` and overrides at most two methods.
``tests/test_a2a_extensions.py::TestOpenClosedPrinciple`` is the proof: it
defines a whole extension inside the test module and drives it end to end —
declaration, header, data plane — against unmodified core code.  The
``novaos.platform/extensions/user-context/v1`` extension is the first real one and is
expected to land the same way.

**The three halves, and who calls which.**

* **Declaration** — an agent's card advertises what it understands.
  :meth:`ExtensionRegistry.declarations` produces the
  ``capabilities.extensions[]`` array.  NovaOS pods do not serve their own card
  (pods are not internet-reachable; the gateway serves it on their
  behalf), so the array is *published* by whoever builds the card, and
  :meth:`novaos_client_sdk.agent_builder.AgentBuilder.declared_extensions` is
  where a pod hands it over.
* **Negotiation** — the ``A2A-Extensions`` request header carries the URIs a
  caller wants active.  :meth:`ExtensionRegistry.request_headers` is the calling
  side, :meth:`ExtensionRegistry.activate` the serving side, and
  :class:`ExtensionActivation` is the outcome both read.
* **Dispatch** — an activated extension's data arrives at
  ``metadata[<uri>]``; :meth:`ExtensionActivation.value` routes it to the
  extension that owns the URI and returns whatever that extension makes of it.

**Activation is not authorisation, and it is not encryption.**  This is
explicit by design: extension data is plain JSON in the request body, visible to
exactly the same infrastructure a header would be.  The protocol provides no
confidentiality for it, and neither does this module — an extension that carries
secrets is responsible for encrypting them before
:meth:`A2AExtension.request_value` returns them, which is why that
work is handled by a separate sealed-envelope mechanism rather than treating it as part of this mechanism.

**Unknown extensions are ignored, not refused.**  A caller may ask for anything;
an agent activates the intersection with what it implements and says so in its
response.  The one hard failure is the mirror image — an extension the *agent*
declared ``required`` that the caller did not activate — which A2A answers
``-32008`` (:class:`~novaos_client_sdk.exceptions.ExtensionSupportRequiredError`).

Design notes: the migration's SDK gap analysis, the
extension decision, and the spec-version pin (see ``a2a_types.A2A_SPEC_VERSION``)
are documented in the migration's architecture decision record.
"""

from collections.abc import Iterable, Iterator
from typing import Any, ClassVar, Final

from .a2a_agent_card import AgentCard, AgentExtension

__all__ = [
    "A2A_EXTENSIONS_HEADER",
    "A2AExtension",
    "ExtensionActivation",
    "ExtensionRegistry",
    "extensions_header_value",
    "requested_extension_uris",
]

#: The header a caller activates extensions with, and an agent echoes activated
#: ones back in (A2A v1.0).  Spelled without an ``X-`` prefix: that form
#: (``X-A2A-Extensions``) is v0.3's, kept alive only by the reference
#: implementation's v0.3 compatibility shim.  NovaOS is pinned to v1.0 (ADR F17)
#: and every NovaOS surface already answers ``-32009`` to a v0.3 caller before
#: any extension is looked at, so accepting the legacy spelling here would be
#: code no request could reach.  ``tests/test_a2a_v1_conformance.py`` pins this
#: against ``a2a.extensions.common.HTTP_EXTENSION_HEADER``.
A2A_EXTENSIONS_HEADER: Final = "A2A-Extensions"


def requested_extension_uris(headers: Any) -> set[str]:
    """Return every extension URI ``headers`` asks to activate.

    Reads the header the two ways HTTP allows a caller to send a list — repeated
    header lines, and one line of comma-separated values — because a caller may
    use either and both mean the same thing (RFC 9110 §5.3).  Empty items are
    dropped rather than activating an extension named ``""``.

    Header lookup is case-insensitive per RFC 9110 §5.1, matching
    :func:`novaos_client_sdk.a2a_types.requested_protocol_version`.

    Args:
        headers: Any mapping with ``.items()``, or ``None``.  A value may be a
            string or a list of strings, which is what header multi-maps yield
            for a repeated header.  Prefer passing the *original* headers object
            rather than a dict built from it: collapsing a multi-map into a
            ``dict`` keeps only one of several repeated header lines.

    Returns:
        The requested URIs, deduplicated.  Empty when the header is absent —
        which is the ordinary case, since most calls activate nothing.
    """
    if not hasattr(headers, "items"):
        return set()

    wanted = A2A_EXTENSIONS_HEADER.lower()
    values: list[str] = []
    for key, value in headers.items():
        if not isinstance(key, str) or key.lower() != wanted:
            continue
        if isinstance(value, str):
            values.append(value)
        elif isinstance(value, (list, tuple)):
            values.extend(str(item) for item in value)
        elif value is not None:
            values.append(str(value))

    return {stripped for value in values for item in value.split(",") if (stripped := item.strip())}


def extensions_header_value(uris: Iterable[str]) -> str:
    """Render extension URIs as one ``A2A-Extensions`` header value.

    Deduplicated and **sorted**, matching the reference implementation's
    ``with_a2a_extensions``.  Sorting is not cosmetic: it makes the header a
    function of the *set* of extensions rather than of the order they happened to
    be registered in, so a request is byte-identical across runs and a test can
    assert on it without pinning registration order.

    Args:
        uris: Extension URIs.  Blank entries are dropped.

    Returns:
        A comma-separated header value, or ``""`` when nothing was supplied —
        which the caller should treat as "send no header at all", since an empty
        header line is a request to activate nothing, spelled the long way.
    """
    return ",".join(sorted({uri.strip() for uri in uris if uri and uri.strip()}))


class A2AExtension:
    """One A2A protocol extension — the class a new extension subclasses.

    **This is the seam that keeps the mechanism closed for modification.**
    Everything the core needs to know about an extension is asked of this class,
    so adding one is writing a subclass, not editing
    :class:`ExtensionRegistry`.  A minimal extension is four lines::

        class TraceContextExtension(A2AExtension):
            uri = "https://example.com/extensions/trace-context/v1"
            description = "Propagates the caller's trace context."

    Every method has a working default, deliberately.  Some extensions carry no
    data at all — activating them *is* the whole agreement — and those should not
    have to implement a data plane to say so.  An extension that does carry data
    overrides :meth:`request_value` (what the caller publishes) and
    :meth:`read_value` (what the agent makes of it), which are the two ends of
    the same wire.

    Attributes are class-level because they describe the extension, not an
    instance of it; :meth:`params` is a method rather than an attribute because a
    declaration's params can legitimately depend on how the instance was
    configured (a key id, an endpoint, a supported-algorithms list).
    """

    #: The extension's identifying URI.  Opaque and matched exactly — A2A gives
    #: URIs no normalisation rules, so a trailing slash makes a different
    #: extension.  A subclass that leaves this empty cannot be registered.
    uri: ClassVar[str] = ""

    #: Human-readable summary, published on the Agent Card.
    description: ClassVar[str] = ""

    #: Whether a caller *must* activate this extension to be served at all.
    #: ``True`` means an agent declaring it answers ``-32008`` to a request that
    #: does not name it in the ``A2A-Extensions`` header.  Default ``False``:
    #: an extension that can be ignored is one an unaware caller can still talk
    #: to, and that is the friendlier default for a protocol built on discovery.
    #:
    #: Not a :class:`~typing.ClassVar`, unlike :attr:`uri` and
    #: :attr:`description`: whether an extension is required is a *deployment*
    #: decision, not a property of the extension, and a subclass may reasonably
    #: take it as a constructor argument — :class:`.a2a_user_context.UserContextExtension`
    #: does.  ``declaration()`` and :attr:`ExtensionRegistry.required_uris` both
    #: read ``self.required``, so the instance value is what has always been
    #: honoured at runtime; an earlier version of this base class annotated it
    #: ``ClassVar`` anyway, and the user-context extension was
    #: the first caller to be type-checked against that.
    required: bool = False

    def params(self) -> dict[str, Any] | None:
        """Extension-specific configuration to publish in the declaration.

        Returns:
            A JSON object for ``AgentExtension.params``, or ``None`` to publish
            none.  The default publishes none.
        """
        return None

    def declaration(self) -> AgentExtension:
        """This extension as the Agent Card declares it.

        Returns:
            The ``capabilities.extensions[]`` entry for this extension.
        """
        return AgentExtension(
            uri=self.uri,
            description=self.description or None,
            required=self.required,
            params=self.params(),
        )

    def request_value(self) -> Any:
        """The value to publish at ``metadata[uri]`` on an outbound request.

        The calling half of the data plane.  Called once per request by
        :meth:`ExtensionRegistry.request_metadata`, so an extension whose payload
        is per-run (a token, a bundle of encrypted secrets) can return live data
        rather than something frozen at construction.

        **Whatever this returns is plain JSON on the wire.**  The A2A extension
        mechanism provides no confidentiality (ADR F7/G1), so an extension
        carrying anything sensitive must encrypt it here, not assume the
        transport did.

        Returns:
            A JSON-serializable value, or ``None`` to publish nothing.  The
            default publishes nothing, which is right for an extension whose
            activation is the entire agreement.
        """
        return None

    def read_value(self, value: Any) -> Any:
        """Interpret the value a caller published at ``metadata[uri]``.

        The serving half of the data plane, and the dispatch target: the core
        finds *which* extension owns a URI, and this decides what its payload
        means.  An extension that encrypts in :meth:`request_value` decrypts
        here.

        This is called from :meth:`ExtensionActivation.value`, on demand rather
        than during negotiation, so an extension whose reading can fail (a bad
        decryption, a malformed payload) raises where a skill can attribute it,
        not from inside the request handler's dispatch path.

        Args:
            value: The raw value found in the request metadata, or ``None`` when
                the caller activated the extension but published no data for it.

        Returns:
            Whatever the extension makes of it.  The default returns it
            unchanged, which is right for an extension whose payload is already
            the value a skill wants.
        """
        return value


class ExtensionActivation:
    """The outcome of one ``A2A-Extensions`` negotiation.

    Produced by :meth:`ExtensionRegistry.activate` on the serving side and
    :meth:`ExtensionRegistry.negotiate_card` on the calling side.  The same four
    lists describe both directions, read from the point of view of whoever asked
    for the negotiation:

    * :attr:`activated` — understood by both sides, and in force.
    * :attr:`requested` — everything the caller asked for, as it arrived.
    * :attr:`ignored` — asked for but not implemented here.  Not an error: A2A
      says an agent activates what it can and gets on with the request.
    * :attr:`unmet_required` — declared ``required`` but not activated.  This
      *is* an error, and :meth:`unmet_requirement_reason` words it.

    Instances are immutable in practice — the lists are exposed as tuples — but
    the object is not a frozen dataclass, because it also carries the request's
    metadata maps so that :meth:`value` can dispatch without the caller
    re-supplying them.
    """

    #: An activation in which nothing was requested and nothing is in force.
    #: Used wherever a request was handled without a registry, so callers get an
    #: object with the usual API rather than ``None`` to guard against.
    EMPTY: ClassVar["ExtensionActivation"]

    def __init__(
        self,
        *,
        activated: Iterable[str] = (),
        requested: Iterable[str] = (),
        ignored: Iterable[str] = (),
        unmet_required: Iterable[str] = (),
        registry: "ExtensionRegistry | None" = None,
        metadata: Iterable[Any] = (),
    ) -> None:
        """Build an activation.  Normally produced by an ``ExtensionRegistry``.

        Args:
            activated: URIs in force for this request.
            requested: URIs the caller asked for.
            ignored: Requested URIs this side does not implement.
            unmet_required: Declared-required URIs that were not activated.
            registry: The registry to dispatch :meth:`value` through.
            metadata: The request's metadata maps, most authoritative first.
        """
        self._activated = tuple(sorted(set(activated)))
        self._requested = tuple(sorted(set(requested)))
        self._ignored = tuple(sorted(set(ignored)))
        self._unmet_required = tuple(sorted(set(unmet_required)))
        self._registry = registry
        self._metadata = tuple(m for m in metadata if isinstance(m, dict))

    @property
    def activated(self) -> tuple[str, ...]:
        """URIs in force for this request, sorted."""
        return self._activated

    @property
    def requested(self) -> tuple[str, ...]:
        """URIs the caller asked to activate, sorted."""
        return self._requested

    @property
    def ignored(self) -> tuple[str, ...]:
        """Requested URIs this side does not implement, sorted."""
        return self._ignored

    @property
    def unmet_required(self) -> tuple[str, ...]:
        """Declared-required URIs the caller did not activate, sorted."""
        return self._unmet_required

    def __bool__(self) -> bool:
        """Is any extension in force?

        ⚠️ **Falsy does not mean "no negotiation happened".**  A request that
        asked only for extensions this side does not implement produces a real
        activation — :attr:`requested` and :attr:`ignored` populated,
        :attr:`activated` empty — and that activation is falsy.  So never write
        ``activation or ExtensionActivation.EMPTY`` to fill in a missing one:
        that discards exactly the outcome a caller would want to inspect.  Test
        against ``None`` instead.
        """
        return bool(self._activated)

    def is_active(self, uri: str) -> bool:
        """Is this extension in force for the request?

        Args:
            uri: The extension's URI, matched exactly.
        """
        return uri in self._activated

    def value(self, uri: str) -> Any:
        """The data an activated extension's caller published, interpreted.

        Dispatches to :meth:`A2AExtension.read_value` on the extension that owns
        ``uri`` — this is the mechanism's dispatch step, and the reason a skill
        never has to know which metadata map a payload arrived in.

        The metadata maps are searched most-authoritative first, the same rule
        and the same order
        :func:`novaos_client_sdk.agent_context.novaos_context_from_metadata`
        uses for the NovaOS context: A2A gives a caller two places to attach
        metadata to a ``SendMessage`` (the request ``params`` and the message
        itself), and neither placement should be a silent no-op.

        Args:
            uri: The extension's URI.

        Returns:
            ``None`` if the extension is not active or is unknown here.
            Otherwise whatever its :meth:`A2AExtension.read_value` returns —
            including for an activated extension that published no data, since
            "activated but silent" is a state an extension may legitimately give
            meaning to.
        """
        if not self.is_active(uri) or self._registry is None:
            return None

        extension = self._registry.get(uri)
        if extension is None:  # pragma: no cover - activation implies membership
            return None

        for candidate in self._metadata:
            if uri in candidate:
                return extension.read_value(candidate[uri])
        return extension.read_value(None)

    def header_value(self) -> str:
        """The activated URIs as one ``A2A-Extensions`` header value."""
        return extensions_header_value(self._activated)

    def response_headers(self) -> dict[str, str]:
        """Headers reporting what this request actually activated.

        A2A's negotiation is two-way: a caller says what it wants and the agent
        answers with what is in force, so a caller that asked for three
        extensions and got one knows which one without inferring it from the
        response body.

        Returns:
            ``{"A2A-Extensions": …}`` when anything is active, and an **empty
            dict** otherwise — an empty header line would claim a negotiation
            happened and produced nothing, which is not the same as an agent
            that has no extensions at all.
        """
        value = self.header_value()
        return {A2A_EXTENSIONS_HEADER: value} if value else {}

    def unmet_requirement_reason(self) -> str | None:
        """Why this request cannot be served as it stands, or ``None``.

        Returns a message rather than raising, matching
        :func:`novaos_client_sdk.a2a_types.unsupported_version_reason`: the
        caller decides how to frame it, which on the JSON-RPC binding means a
        ``-32008`` error object
        (:class:`~novaos_client_sdk.exceptions.ExtensionSupportRequiredError`).

        Returns:
            A human-readable reason naming the missing URIs, or ``None`` when
            every required extension was activated.
        """
        if not self._unmet_required:
            return None

        missing = ", ".join(f"'{uri}'" for uri in self._unmet_required)
        return (
            f"This agent requires the A2A extension(s) {missing}, which the request did "
            f"not activate. Send them in the '{A2A_EXTENSIONS_HEADER}' header."
        )


ExtensionActivation.EMPTY = ExtensionActivation()


class ExtensionRegistry:
    """The extensions one side of a conversation implements.

    Both roles use the same container, because both sides of an extension are
    the same set of agreements read in opposite directions:

    * an **agent** registers what it understands, publishes
      :meth:`declarations` on its card, and calls :meth:`activate` per request;
    * a **caller** registers what it intends to use, sends
      :meth:`request_headers` and :meth:`request_metadata`, and may check itself
      against an agent's card with :meth:`negotiate_card` first.

    A registry is a plain object with no global state.  There is deliberately no
    module-level default registry to import: process-wide mutable registration
    makes what an agent declares depend on which modules happened to be
    imported, and an agent's declared surface should be something its
    construction states outright.
    """

    def __init__(self, extensions: Iterable[A2AExtension] = ()) -> None:
        """Build a registry over ``extensions``.

        Args:
            extensions: Extension instances to register, in order.

        Raises:
            ValueError: If an extension has no ``uri``, or two claim the same one.
        """
        self._extensions: dict[str, A2AExtension] = {}
        for extension in extensions:
            self.register(extension)

    def register(self, extension: A2AExtension) -> A2AExtension:
        """Register one extension and return it.

        Args:
            extension: The extension to add.

        Returns:
            The extension, so a call site can register and keep a reference in
            one expression.

        Raises:
            ValueError: If ``extension.uri`` is empty, or another extension has
                already claimed it.  A duplicate is refused rather than
                overwritten because a URI *is* the agreement: two
                implementations under one URI means a caller's activation
                silently reaches whichever module was imported last.
        """
        uri = (extension.uri or "").strip()
        if not uri:
            raise ValueError(
                f"{type(extension).__name__} declares no 'uri'. An A2A extension is "
                "identified by its URI, so a class without one cannot be declared, "
                "activated or dispatched to."
            )
        if uri in self._extensions:
            existing = type(self._extensions[uri]).__name__
            raise ValueError(
                f"Extension URI '{uri}' is already registered by {existing}; "
                f"{type(extension).__name__} cannot claim it too."
            )

        self._extensions[uri] = extension
        return extension

    def get(self, uri: str) -> A2AExtension | None:
        """The registered extension with this URI, or ``None``."""
        return self._extensions.get(uri)

    def __contains__(self, uri: object) -> bool:
        """Is an extension registered under this URI?"""
        return isinstance(uri, str) and uri in self._extensions

    def __iter__(self) -> Iterator[A2AExtension]:
        """Iterate the registered extensions, in registration order."""
        return iter(self._extensions.values())

    def __len__(self) -> int:
        """How many extensions are registered."""
        return len(self._extensions)

    @property
    def uris(self) -> list[str]:
        """Every registered URI, in registration order."""
        return list(self._extensions)

    @property
    def required_uris(self) -> list[str]:
        """The URI of every registered extension marked ``required``."""
        return [uri for uri, ext in self._extensions.items() if ext.required]

    def declarations(self) -> list[AgentExtension]:
        """The ``capabilities.extensions[]`` array for an Agent Card.

        The declaration half of the mechanism.  What publishes it differs by
        deployment — the gateway serves a NovaOS pod's card, the
        Galaxy scaffold generates card content at build time — so this
        returns the array rather than writing it anywhere.
        """
        return [extension.declaration() for extension in self._extensions.values()]

    # ------------------------------------------------------------------
    # Serving side
    # ------------------------------------------------------------------

    def activate(self, headers: Any, *metadata: Any) -> ExtensionActivation:
        """Negotiate an inbound request's ``A2A-Extensions`` header.

        Args:
            headers: The request's headers, any mapping with ``.items()``.  Pass
                the original headers object where there is one: a multi-map
                flattened into a ``dict`` loses all but one of several repeated
                ``A2A-Extensions`` lines.
            *metadata: The request's metadata maps, most authoritative first —
                for ``SendMessage`` that is the request ``params`` metadata and
                then the message's own.  Only used to answer
                :meth:`ExtensionActivation.value`.

        Returns:
            What is in force, what was ignored, and what is missing.  A request
            asking for an extension this agent does not implement is *not* an
            error — it lands in :attr:`ExtensionActivation.ignored` and the
            request proceeds.
        """
        requested = requested_extension_uris(headers)
        activated = {uri for uri in requested if uri in self._extensions}

        return ExtensionActivation(
            activated=activated,
            requested=requested,
            ignored=requested - activated,
            unmet_required=[uri for uri in self.required_uris if uri not in activated],
            registry=self,
            metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Calling side
    # ------------------------------------------------------------------

    def request_headers(self) -> dict[str, str]:
        """Headers activating every registered extension.

        Returns:
            ``{"A2A-Extensions": …}``, or an empty dict when nothing is
            registered — so a client with no extensions sends no header rather
            than an empty one.
        """
        value = extensions_header_value(self.uris)
        return {A2A_EXTENSIONS_HEADER: value} if value else {}

    def request_metadata(self) -> dict[str, Any]:
        """The metadata every registered extension wants to publish.

        Each extension's :meth:`A2AExtension.request_value` is placed under its
        own URI, which is where A2A puts extension data and where the serving
        side's :meth:`ExtensionActivation.value` looks for it.  An extension
        returning ``None`` contributes no key at all, rather than a ``null`` a
        peer would have to distinguish from an absent one.

        Returns:
            A metadata fragment to merge into the outbound request.
        """
        published: dict[str, Any] = {}
        for uri, extension in self._extensions.items():
            value = extension.request_value()
            if value is not None:
                published[uri] = value
        return published

    def negotiate_card(self, card: AgentCard) -> ExtensionActivation:
        """Check what this registry and ``card``'s agent actually agree on.

        The calling side's mirror of :meth:`activate`, and worth running before
        a call rather than after a failure: it is how a caller learns that the
        agent requires an extension it cannot speak — a condition that otherwise
        surfaces as a ``-32008`` from the first request, and one no retry fixes.

        Args:
            card: The agent's Agent Card, as
                :meth:`novaos_client_sdk.discovery.AgentDiscoveryService.agent_card`
                returns it.

        Returns:
            :attr:`~ExtensionActivation.activated` — declared by the agent and
            implemented here, which is exactly what to send;
            :attr:`~ExtensionActivation.ignored` — implemented here but not
            declared by this agent, so sending them would be noise;
            :attr:`~ExtensionActivation.unmet_required` — required by the agent
            and *not* implemented here, which is the call that cannot succeed.
            A card carries no request data, so this result is deliberately not
            bound to a registry and :meth:`ExtensionActivation.value` on it is
            always ``None``.
        """
        declared = set(card.extension_uris)
        mine = set(self._extensions)
        activated = mine & declared

        return ExtensionActivation(
            activated=activated,
            requested=mine,
            ignored=mine - declared,
            unmet_required=[uri for uri in card.required_extension_uris if uri not in mine],
        )
