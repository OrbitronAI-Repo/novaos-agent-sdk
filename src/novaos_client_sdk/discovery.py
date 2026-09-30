"""A2A Agent Card discovery — fetch and cache an agent's self-description.

Part of the ACP-to-A2A migration.  This module used to ask
an ACP server ``GET /agents`` and hand back a list of
:class:`novaos_client_sdk.models.Agent` objects; it now fetches one
:class:`~novaos_client_sdk.a2a_agent_card.AgentCard` from
``/.well-known/agent-card.json``.

**Why the shape changed, and not just the endpoint.**  ``GET /agents`` was a
*listing*: one ACP server fronted many separately named agents, so discovery
meant enumerating them and then filtering the result — which is what this
module's ``find_agents`` / ``find_one`` / ``get_or_raise`` surface existed to do.
A2A has no listing operation at all.  An agent *is* a URL
(:mod:`novaos_client_sdk.client` explains why there is no ``agent=`` parameter
any more), and one well-known path describes exactly one agent.  So the filter
API had nothing left to filter over and was removed outright rather than
reimplemented against a document that cannot answer it — the same call was made
on ``run_sync``/``run_stream``.  There are no aliases and no deprecation window,
per the migration's scope decision.

**Enumeration did not disappear from the SDK — it moved to where it already
was.**  The migration's design settles this explicitly: the agent DNS service remains the
right place for internal discovery, and it is reached through
:class:`novaos_client_sdk.dns.AgentDNSClient`, which already offers
``list_agents`` / ``search_agents`` / ``find_agents`` / ``get_agent`` over a
registry that genuinely holds many agents.  External and internal discovery are
two different audiences: a caller that needs "which agents exist?" asks the DNS
service, and a caller that needs "what does *this* agent do, and where do I call
it?" asks for its Agent Card.  This module is the second question only.

**What answers the well-known path.**  Not the agent pod: NovaOS pods are
never internet-reachable, so the execution gateway serves the card on a
published agent's behalf.  Nothing here depends on that, though — the
request is the plain A2A one, so the same code discovers a third-party partner
agent that hosts its own card, which is a case the migration's discovery design
calls out.

**The extended Agent Card is deliberately absent.**  A2A defines a second,
authenticated card carrying detail the public one omits, retrieved with the
``GetExtendedAgentCard`` JSON-RPC method rather than from a URL.  It is out of
scope here and stays that way: the migration's design requires that no
extended-card-shaped path is ever registered as public at the gateway, and a
fetcher in this module would be a second way to reach for it.
:attr:`AgentCapabilities.extended_agent_card
<novaos_client_sdk.a2a_agent_card.AgentCapabilities.extended_agent_card>` reports
whether the agent offers one.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any, Final
from urllib.parse import urljoin

from novaos_client_sdk._http import httpx
from novaos_client_sdk.a2a_agent_card import (
    AGENT_CARD_WELL_KNOWN_PATH,
    PROTOCOL_BINDING_JSONRPC,
    AgentCard,
    AgentExtension,
    AgentSkill,
    select_interface,
)
from novaos_client_sdk.client import Client, ClientConfig
from novaos_client_sdk.dns import validate_agent_url
from novaos_client_sdk.exceptions import (
    NovaOSAgentNotFoundError,
    NovaOSAuthenticationError,
    NovaOSConnectionError,
    NovaOSError,
    NovaOSRateLimitError,
    NovaOSServerError,
    NovaOSValidationError,
)

__all__ = [
    "DEFAULT_AGENT_CARD_CACHE_TTL",
    "AgentDiscoveryService",
    "fetch_agent_card",
]

#: Seconds a fetched card is reused before it is re-fetched.  Five minutes,
#: unchanged from what this module cached ACP agent listings for: an Agent Card
#: changes when an agent is republished, which is a deploy-time event, so a
#: caller in a request path should not pay a discovery round-trip per call.
#: ``None`` disables caching entirely.
DEFAULT_AGENT_CARD_CACHE_TTL: Final = 300


def _card_url(base_url: str) -> str:
    """Resolve the well-known Agent Card URL for an agent's base URL.

    ``urljoin`` on a base ending in ``/`` rather than plain concatenation,
    because A2A resolves the well-known path against the agent's base URL and a
    NovaOS gateway addresses each agent under a path prefix
    (``/a2a/v1/agents/{agentId}``).  Concatenating would put the card at
    the *host* root for those agents — the wrong agent's card, or none.
    """
    return urljoin(base_url.rstrip("/") + "/", AGENT_CARD_WELL_KNOWN_PATH.lstrip("/"))


class AgentDiscoveryService:
    """Fetches and caches one agent's Agent Card.

    The service is an async context manager; the HTTP connection is opened on
    entry and reused, so repeated lookups against the same agent — skills,
    extensions, the interface URL — cost one request between cache expiries
    rather than one each.

    Example:
        ```python
        async with AgentDiscoveryService(base_url="https://gateway.example") as discovery:
            card = await discovery.agent_card()
            print(card.name, card.skill_ids)

            async with await discovery.to_client() as client:
                task = await client.send_message("hello", skill_id=card.skill_ids[0])
        ```
    """

    def __init__(
        self,
        base_url: str,
        cache_ttl: int | None = DEFAULT_AGENT_CARD_CACHE_TTL,
        timeout: float | None = None,
        auth: Any | None = None,
        *,
        verify_signatures: bool = False,
        jwks_url: str | None = None,
        signature_algorithms: Sequence[str] | None = None,
    ) -> None:
        """Initialize the discovery service.

        Args:
            base_url: Base URL the agent is addressed at.  Its Agent Card is
                read from ``{base_url}/.well-known/agent-card.json``.
            cache_ttl: Seconds to reuse a fetched card for.  ``None`` disables
                caching.
            timeout: Request timeout in seconds.
            auth: Optional auth provider.  The *basic* Agent Card is public by
                design — it is what a caller reads before it has credentials —
                so this is normally unnecessary; it exists for a deployment that
                puts the whole origin behind a gate.
            verify_signatures: Verify the card's JWS ``signatures`` against
                ``jwks_url`` after every fetch, before the card is cached or
                handed to :meth:`to_client`.  **Opt-in and ``False`` by
                default** — every card this SDK has consumed so far comes from
                NovaOS's own gateway over a trusted path, and an unsigned card
                from that path must keep working.  Turn this on
                once a fetched card may come from a third-party agent, per this
                module's docstring.  Requires the ``crypto`` extra: ``pip
                install 'novaos-client-sdk[crypto]'``.
            jwks_url: JWKS endpoint to verify against.  Required when
                ``verify_signatures`` is ``True``.
            signature_algorithms: Acceptable JWS algorithms.  See
                :data:`novaos_client_sdk.a2a_card_signing.DEFAULT_SIGNATURE_ALGORITHMS`
                for the default.

        Raises:
            NovaOSValidationError: If ``verify_signatures`` is ``True`` and no
                ``jwks_url`` was given.
        """
        if verify_signatures and not (jwks_url and jwks_url.strip()):
            raise NovaOSValidationError(
                "jwks_url is required when verify_signatures=True.",
                field="jwks_url",
                reason="missing_required_field",
            )
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth
        self._cache_ttl = cache_ttl
        self._verify_signatures = verify_signatures
        self._jwks_url = jwks_url
        self._signature_algorithms = signature_algorithms
        self._cache: AgentCard | None = None
        self._cache_timestamp: float | None = None
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> AgentDiscoveryService:
        """Open the underlying HTTP client."""
        headers: dict[str, str] = {}
        if self._auth and hasattr(self._auth, "get_headers"):
            auth_headers = self._auth.get_headers()
            if auth_headers:
                headers.update(auth_headers)

        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the underlying HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def base_url(self) -> str:
        """The agent base URL this service discovers."""
        return self._base_url

    @property
    def card_url(self) -> str:
        """The well-known URL this service reads the card from."""
        return _card_url(self._base_url)

    def _ensure_client(self) -> httpx.AsyncClient:
        """Return the open HTTP client, or raise if there is none."""
        if not self._client:
            raise NovaOSError(
                "Service is not initialized. Use async context manager or call __aenter__() first."
            )
        return self._client

    def _is_cache_valid(self) -> bool:
        """Is there a cached card still inside its TTL?"""
        if self._cache is None or self._cache_timestamp is None:
            return False
        if self._cache_ttl is None:
            return False
        return (time.time() - self._cache_timestamp) < self._cache_ttl

    def clear_cache(self) -> None:
        """Drop any cached card, forcing the next lookup to re-fetch."""
        self._cache = None
        self._cache_timestamp = None

    async def _fetch_agent_card(self) -> AgentCard:
        """Fetch and parse the Agent Card, mapping every failure onto the SDK's errors."""
        client = self._ensure_client()

        try:
            response = await client.get(AGENT_CARD_WELL_KNOWN_PATH)
            response.raise_for_status()
            payload = response.json()
        except httpx.HTTPStatusError as e:
            raise _status_error(e, self.card_url) from e
        except (httpx.TransportError, ConnectionError, TimeoutError) as e:
            # `httpx.TransportError`, not the `(ConnectError, TimeoutException)`
            # pair the ACP-era code caught: `ReadError`, `WriteError`,
            # `RemoteProtocolError` and `ProxyError` are neither, so a connection
            # dropped *while reading the card* escaped as a raw httpx exception.
            # `is_retryable` is decided by the SDK's exception class, so a retry
            # loop saw a network blip as a permanent failure — the same defect
            # fixed in `Client._map_exception`. The builtins are
            # matched alongside for the same reason it gives: a test double
            # naturally raises `ConnectionRefusedError`.
            raise NovaOSConnectionError(
                f"Failed to connect to {self.card_url}: {e}",
                details={"original_error": type(e).__name__, "card_url": self.card_url},
            ) from e
        except ValueError as e:
            # `response.json()` on a body that is not JSON at all — an HTML error
            # page from a proxy in front of the agent is the common case.
            raise NovaOSValidationError(
                f"Agent Card at {self.card_url} is not valid JSON: {e}",
                field="agent_card",
                reason="not_json",
                details={"card_url": self.card_url},
            ) from e

        card = _parsed_card(payload, self.card_url)
        if self._verify_signatures:
            await self._verify_card(payload)
        return card

    async def _verify_card(self, payload: dict[str, Any]) -> None:
        """Verify the fetched card's signatures, or raise.

        Takes the **raw fetched payload**, not the parsed :class:`AgentCard`.
        :meth:`AgentCard.from_dict` is deliberately tolerant of wire data it
        does not model, which makes it lossy: canonicalizing
        ``AgentCard.from_dict(payload).to_dict()`` instead of ``payload``
        itself would hash a document the signer never signed the moment a
        fetched card carries a field this SDK does not yet know about — see
        :mod:`novaos_client_sdk.a2a_card_signing`'s module docstring.

        Imported lazily so that a caller who never sets ``verify_signatures``
        keeps importing this module without the ``crypto`` extra installed —
        the same reason :mod:`novaos_client_sdk.a2a_card_signing` itself raises
        ``ImportError`` at *its* import time rather than this one's.  The
        verification runs in a thread because the JWKS fetch inside it is a
        blocking call (``PyJWKClient`` uses ``urllib``, not ``httpx``), and this
        method is awaited from an async fetch path.
        """
        from novaos_client_sdk.a2a_card_signing import verify_agent_card_signatures

        await asyncio.to_thread(
            verify_agent_card_signatures,
            payload,
            jwks_url=self._jwks_url,  # type: ignore[arg-type]  # guarded non-None in __init__
            algorithms=self._signature_algorithms,
            timeout=self._timeout,
        )

    async def agent_card(self, *, force_refresh: bool = False) -> AgentCard:
        """Return the agent's Agent Card, fetching it if the cache is cold.

        Args:
            force_refresh: Re-fetch even when a cached card is still valid.

        Returns:
            The parsed :class:`~novaos_client_sdk.a2a_agent_card.AgentCard`.

        Raises:
            NovaOSAgentNotFoundError: If no card is published at the well-known
                path (HTTP 404) — the agent is not discoverable, which for a
                gateway-hosted card means it was never published.
            NovaOSAuthenticationError: If the origin refused the request.
            NovaOSRateLimitError: If the origin rate-limited the request.
            NovaOSServerError: If the origin answered 5xx.
            NovaOSConnectionError: If the origin could not be reached.
            NovaOSValidationError: On any other 4xx, or if the response is not a
                usable Agent Card.
            NoAgentCardSignatureError: If ``verify_signatures=True`` and the
                fetched card carries no ``signatures``.
            InvalidAgentCardSignatureError: If ``verify_signatures=True`` and
                none of the card's signatures verify against ``jwks_url``.
        """
        if not force_refresh and self._is_cache_valid() and self._cache is not None:
            return self._cache

        card = await self._fetch_agent_card()
        self._cache = card
        self._cache_timestamp = time.time()
        return card

    async def skills(self) -> list[AgentSkill]:
        """Every skill the agent advertises."""
        return (await self.agent_card()).skills

    async def skill(self, skill_id: str) -> AgentSkill | None:
        """The advertised skill with this id, or ``None``.

        Args:
            skill_id: The value a caller would send as
                :data:`novaos_client_sdk.client.SKILL_ID_METADATA_KEY`.
        """
        return (await self.agent_card()).skill(skill_id)

    async def require_skill(self, skill_id: str) -> AgentSkill:
        """The advertised skill with this id, or raise.

        The counterpart of the removed ``get_or_raise``: the useful "or raise"
        question under A2A is about a *skill*, since which agent is being
        addressed is already settled by the URL.

        Args:
            skill_id: The skill id to require.

        Raises:
            NovaOSAgentNotFoundError: If the card advertises no such skill.  The
                error carries the ids that *are* advertised, which is what a
                caller needs to correct the call.
        """
        card = await self.agent_card()
        skill = card.skill(skill_id)
        if skill is None:
            raise NovaOSAgentNotFoundError(
                f"Agent '{card.name or self._base_url}' advertises no skill '{skill_id}'",
                agent_name=card.name or None,
                details={"skill_id": skill_id, "available_skills": card.skill_ids},
            )
        return skill

    async def extensions(self) -> list[AgentExtension]:
        """Every A2A protocol extension the agent declares.

        The read half of A2A's extension mechanism; the extension mechanism builds
        ``A2A-Extensions`` header negotiation on top of it.
        """
        return (await self.agent_card()).extensions

    async def supports_extension(self, uri: str) -> bool:
        """Does the agent declare this extension?

        Args:
            uri: The extension's URI, matched exactly.
        """
        return (await self.agent_card()).supports_extension(uri)

    async def interface_url(self, protocol_binding: str = PROTOCOL_BINDING_JSONRPC) -> str | None:
        """The URL to call the agent at, or ``None`` if it offers no such binding.

        Args:
            protocol_binding: Which A2A binding to look for.  Defaults to
                JSON-RPC, the only one this SDK's transport speaks.
        """
        return (await self.agent_card()).interface_url(protocol_binding)

    async def to_client(
        self,
        *,
        allowed_host_suffixes: list[str] | None = None,
        config: ClientConfig | None = None,
        **client_kwargs: Any,
    ) -> Client:
        """Build a :class:`~novaos_client_sdk.client.Client` from the agent's card.

        This is discovery's payoff: one of the card's interface URLs is
        exactly what a client needs as its ``base_url``, so a caller goes from
        "an origin" to "a callable agent" without hard-coding a path.

        **Which interface, when a card offers several.**
        :func:`~novaos_client_sdk.a2a_agent_card.select_interface` picks one
        from ``config.supported_protocol_bindings`` (JSON-RPC only if
        unset — this SDK's original behavior, unchanged) and
        ``config.use_client_preference`` (server preference, i.e. the card's
        own listed order, unless set). A card offering only JSON-RPC and a
        default ``config`` therefore behaves exactly as it always has.

        The returned client's :attr:`~novaos_client_sdk.client.Client.card` is
        this same cached card — by identity, not a re-parse — passed straight
        through rather than discarded: this service already paid for it, and a
        second HTTP request to re-fetch what is already in hand would be waste
        for no gain.

        **The URL is validated before it is used.**  It arrives from a remote
        document, so it is the classic SSRF vector — a card is the one place an
        attacker who can influence published metadata gets to choose an address
        the SDK will connect to.  :func:`novaos_client_sdk.dns.validate_agent_url`
        is the same guard :meth:`novaos_client_sdk.dns.AgentDNSRecord.to_client`
        applies to a DNS record's URL, for the same reason — applied here to
        whichever interface was selected, regardless of binding.

        Args:
            allowed_host_suffixes: Optional hostname suffix allowlist.  Pass the
                deployment's own domains to refuse a card that points elsewhere.
            config: Per-client defaults, including which bindings this caller
                can speak and whose preference order wins — see above.  Also
                forwarded to the constructed :class:`~novaos_client_sdk.client.Client`
                the same as every other ``client_kwargs`` entry.
            **client_kwargs: Forwarded to :class:`~novaos_client_sdk.client.Client`
                (e.g. ``timeout``, ``headers``, ``auth``).

        Returns:
            A client addressed at the selected interface. It is not entered —
            ``async with await discovery.to_client() as client:``.

        Raises:
            NovaOSValidationError: If the card advertises no interface this
                client can speak.
            NovaOSError: If the advertised URL fails the SSRF guard.
        """
        card = await self.agent_card()
        cfg = config if config is not None else ClientConfig()
        supported_bindings = cfg.supported_protocol_bindings or [PROTOCOL_BINDING_JSONRPC]

        selected = select_interface(
            card.supported_interfaces,
            supported_bindings,
            use_client_preference=cfg.use_client_preference,
        )
        if selected is None:
            offered = [i.protocol_binding for i in card.supported_interfaces]
            raise NovaOSValidationError(
                f"Agent Card at {self.card_url} advertises no interface this client can "
                f"speak (offers {offered}, this client supports {supported_bindings}).",
                field="supportedInterfaces",
                reason="no_compatible_binding",
                details={
                    "card_url": self.card_url,
                    "protocol_bindings": offered,
                    "supported_protocol_bindings": supported_bindings,
                },
            )

        validate_agent_url(selected.url, allowed_host_suffixes)
        return Client(
            base_url=selected.url,
            card=card,
            protocol_binding=selected.protocol_binding,
            config=config,
            **client_kwargs,
        )


def _retry_after(response: httpx.Response) -> float | None:
    """Read a ``Retry-After`` delay in seconds, or ``None``.

    Only the delta-seconds form, matching
    :meth:`novaos_client_sdk.client.Client._map_exception`: an HTTP-date value
    yields ``None`` rather than a guess, and a malformed header is ignored rather
    than failing the request a second time.
    """
    header = response.headers.get("Retry-After")
    if header is None:
        return None
    try:
        return float(header)
    except ValueError:
        return None


def _status_error(error: httpx.HTTPStatusError, card_url: str) -> NovaOSError:
    """Map a non-2xx Agent Card response onto the SDK's exception hierarchy.

    Follows :meth:`novaos_client_sdk.client.Client._map_exception` status for
    status, so a caller sees the same class from a discovery call as from the
    invocation that follows it.  That matters more than it looks: ``is_retryable``
    is decided by the class, so mapping every non-404 onto
    :class:`~novaos_client_sdk.exceptions.NovaOSConnectionError` would tell a
    retry loop that a malformed request is worth sending again.

    **A 404 is not an empty result.**  The ACP-era code returned ``[]`` for it,
    which was defensible for a listing — no agents is a valid answer — but a
    single-document fetch has no empty value: either this origin publishes a card
    or it does not describe an A2A agent at all, and reporting that as a blank
    card would push the failure to whatever tried to use it.
    """
    status = error.response.status_code
    details: dict[str, Any] = {"status_code": status, "card_url": card_url}

    if status == 404:
        return NovaOSAgentNotFoundError(f"No Agent Card published at {card_url}", details=details)
    if status in (401, 403):
        return NovaOSAuthenticationError(
            f"Access to the Agent Card at {card_url} was refused ({status})",
            details=details,
        )
    if status == 429:
        return NovaOSRateLimitError(
            f"Rate limited fetching the Agent Card at {card_url}",
            retry_after=_retry_after(error.response),
            details=details,
        )
    if status >= 500:
        return NovaOSServerError(
            f"Failed to fetch the Agent Card at {card_url}: {status}",
            status_code=status,
            details=details,
        )
    return NovaOSValidationError(
        f"Failed to fetch the Agent Card at {card_url}: {status}",
        field="agent_card",
        reason="http_error",
        details=details,
    )


def _parsed_card(payload: Any, card_url: str) -> AgentCard:
    """Parse a fetched payload into an Agent Card, or raise.

    :meth:`AgentCard.from_dict` is deliberately total — it never raises on wire
    data — so the "is this actually a card?" judgement is made here, where the
    URL is known and the error can say which document was wrong.  The bar is
    just above nothing: a JSON object carrying a ``name``.  Anything stricter
    would reject cards that are merely sparse, which is a publisher's problem
    rather than a discovery failure, and anything looser would let an unrelated
    JSON document (a proxy's error body, a directory listing) pass as an agent.
    """
    if not isinstance(payload, dict):
        raise NovaOSValidationError(
            f"Agent Card at {card_url} is a JSON {type(payload).__name__}, not an object.",
            field="agent_card",
            reason="not_an_object",
            details={"card_url": card_url},
        )

    card = AgentCard.from_dict(payload)
    if not card.name:
        raise NovaOSValidationError(
            f"Document at {card_url} has no 'name' and is not an Agent Card.",
            field="name",
            reason="missing_required_field",
            details={"card_url": card_url},
        )
    return card


async def fetch_agent_card(
    base_url: str,
    *,
    timeout: float | None = None,
    auth: Any | None = None,
    verify_signatures: bool = False,
    jwks_url: str | None = None,
    signature_algorithms: Sequence[str] | None = None,
) -> AgentCard:
    """Fetch one agent's Agent Card, without keeping a service around.

    The one-shot form of :meth:`AgentDiscoveryService.agent_card`, for the common
    case of reading a card once at startup.  Prefer the service when several
    lookups follow, since this opens and closes a connection per call and caches
    nothing.

    Args:
        base_url: Base URL the agent is addressed at.
        timeout: Request timeout in seconds.
        auth: Optional auth provider.
        verify_signatures: See :meth:`AgentDiscoveryService.__init__`.
        jwks_url: See :meth:`AgentDiscoveryService.__init__`.
        signature_algorithms: See :meth:`AgentDiscoveryService.__init__`.

    Returns:
        The parsed :class:`~novaos_client_sdk.a2a_agent_card.AgentCard`.
    """
    async with AgentDiscoveryService(
        base_url=base_url,
        cache_ttl=None,
        timeout=timeout,
        auth=auth,
        verify_signatures=verify_signatures,
        jwks_url=jwks_url,
        signature_algorithms=signature_algorithms,
    ) as discovery:
        return await discovery.agent_card()
