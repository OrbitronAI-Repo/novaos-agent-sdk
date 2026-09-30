"""``AuthInterceptor`` — per-call credential resolution.

Separate module from :mod:`novaos_client_sdk.auth` on purpose, the same way
the reference ``a2a-python`` splits ``client/auth/interceptor.py`` from
``client/auth/credentials.py``: this class subclasses
:class:`novaos_client_sdk.interceptors.ClientCallInterceptor` for real, so
importing it pulls in the rest of the package.
:class:`novaos_client_sdk.auth.CredentialService` does not need that and stays
free of it — see the ``TYPE_CHECKING`` note on its import in ``auth.py`` —
which is what lets ``tests/test_auth.py`` keep unit-testing the credential
seam without the full package import chain, while this module and its
``Client`` wiring are exercised through the ordinary package-level imports the
rest of the test suite already uses.
"""

from __future__ import annotations

from typing import Any

from novaos_client_sdk.a2a_agent_card import AgentCard
from novaos_client_sdk.auth import AuthProvider, CredentialService
from novaos_client_sdk.interceptors import BeforeArgs, ClientCallContext, ClientCallInterceptor

__all__ = ["AuthInterceptor"]

#: ``securitySchemes`` oneof variant name for API-key schemes (A2A v1.0,
#: proto3 JSON). Carries OpenAPI-style ``name`` / ``in`` fields.
_API_KEY_SCHEME = "apiKeySecurityScheme"


class AuthInterceptor(ClientCallInterceptor):
    """Resolves and attaches per-call credentials.

    **Resolution rule, in order** — deliberately simple, per the ticket:

    1. If a :class:`~novaos_client_sdk.auth.CredentialService` was configured,
       pick the scheme to ask it for by matching the call's
       :class:`~novaos_client_sdk.a2a_agent_card.AgentCard` ``securitySchemes``
       against its ``securityRequirements`` — see :meth:`_select_scheme` — and
       call ``get_credentials(scheme_name, context)``.
    2. If that returned a credential, format it into a header using the
       matched scheme's declared shape — see :meth:`_format_header` — unless
       the service already returned a ready-made ``{header: value}`` mapping,
       which is used unchanged.
    3. Otherwise (no ``CredentialService`` configured, no card to read a
       scheme from, or the service returned nothing), fall back to the
       constructor's synchronous ``AuthProvider`` — exactly what
       :class:`~novaos_client_sdk.client.Client` did before this ticket, so a
       caller who supplies only ``auth=`` and no ``CredentialService`` sees no
       change at all.

    This interceptor implements ``before`` only; the default no-op ``after``
    from :class:`~novaos_client_sdk.interceptors.ClientCallInterceptor` is
    inherited unchanged. A caller wanting a refresh-and-retry policy on a 401
    builds it from ``after`` observing the error — this class does not attempt
    one itself.
    """

    def __init__(
        self,
        *,
        credential_service: CredentialService | None = None,
        auth: AuthProvider | None = None,
    ) -> None:
        """Initialize the interceptor.

        Args:
            credential_service: Resolves a fresh credential per call. Tried
                first when configured.
            auth: The same synchronous provider a caller could pass straight
                to ``Client(auth=...)``. Used as the fallback here rather than
                a second, independent auth path — a caller migrating onto
                interceptors keeps exactly the provider they had.
        """
        self._credential_service = credential_service
        self._auth = auth

    async def before(self, args: BeforeArgs) -> None:
        """Attach the resolved credential's header(s) to ``args.http_kwargs``."""
        headers = await self._resolve_headers(args.card, args.context)
        if headers:
            args.http_kwargs.setdefault("headers", {}).update(headers)

    async def _resolve_headers(
        self, card: AgentCard | None, context: ClientCallContext
    ) -> dict[str, str] | None:
        """Resolve this call's headers per the class docstring's three steps.

        A configured ``CredentialService`` that resolves a credential always
        owns the answer, even where :meth:`_format_header` has nothing to send
        it as (an ``apiKeySecurityScheme`` declared in a query string or
        cookie, say) and returns ``{}``: falling back to the constructor's
        ``AuthProvider`` in that case would mean the call goes out carrying a
        *different* credential than the one the service just resolved for it,
        which is a worse outcome than sending none. The ``AuthProvider``
        fallback below is for "no service resolved anything at all" — no
        ``CredentialService`` configured, or one configured that returned
        ``None`` — not for "the service resolved something this interceptor
        cannot place in a header".
        """
        if self._credential_service is not None:
            scheme_name, scheme = self._select_scheme(card)
            credential = await self._credential_service.get_credentials(scheme_name or "", context)
            if credential:
                return self._format_header(scheme, credential)
        if self._auth is not None:
            return self._auth.get_headers()
        return None

    @staticmethod
    def _select_scheme(card: AgentCard | None) -> tuple[str | None, dict[str, Any] | None]:
        """Pick which of the card's ``securitySchemes`` this call should use.

        Prefers a scheme named in the card's ``securityRequirements`` — each
        requirement is ``{"schemes": {scheme_name: {...}}}`` (see
        ``tests/test_a2a_agent_card.py``'s fixture) — over just taking
        whichever scheme happens to be declared first, since a card
        advertising several schemes states its actual requirement there. Falls
        back to the first declared scheme when there is no requirements list
        to read (or none of it names a declared scheme), so a minimal card
        that skips the — optional — requirements block still resolves.

        Returns:
            ``(None, None)`` if the card has no schemes at all to choose from.
        """
        if card is None or not card.security_schemes:
            return None, None

        for requirement in card.security_requirements:
            schemes = requirement.get("schemes")
            if not isinstance(schemes, dict):
                continue
            for scheme_name in schemes:
                if scheme_name in card.security_schemes:
                    return scheme_name, card.security_schemes[scheme_name]

        scheme_name = next(iter(card.security_schemes))
        return scheme_name, card.security_schemes[scheme_name]

    @staticmethod
    def _format_header(
        scheme: dict[str, Any] | None, credential: str | dict[str, str]
    ) -> dict[str, str]:
        """Shape *credential* into the header(s) the matched scheme expects.

        A ``dict`` credential is already a complete ``{header: value}``
        mapping — how a wrapped synchronous ``AuthProvider`` answers, since it
        already knows its own shape — and is used unchanged.

        For a bare credential value: an ``apiKeySecurityScheme`` with
        ``"in": "header"`` becomes its declared header name (default
        ``X-API-Key`` — matching :class:`~novaos_client_sdk.auth.APIKeyAuth`'s
        own default — when the scheme omits one); an API key scheme placed in
        a query string or cookie is not sent through this interceptor at all,
        since neither is a header this client can attach here. Every other
        scheme type — ``httpAuthSecurityScheme`` (NovaOS's own
        ``novaos_exec``), ``oauth2SecurityScheme`` (the external/Logto path),
        ``openIdConnectSecurityScheme``, and any scheme this client does not
        recognize — resolves to ``Authorization: Bearer <credential>``. That
        is not a guess for the two NovaOS carries today: both a
        gateway-injected execution token and a Logto client-credentials token
        are bearer-shaped on the wire; a scheme this client cannot identify is
        safest read the same way rather than sent with no ``Authorization``
        header at all.
        """
        if isinstance(credential, dict):
            return credential

        if scheme is not None:
            api_key = scheme.get(_API_KEY_SCHEME)
            if isinstance(api_key, dict):
                if api_key.get("in", "header") == "header":
                    header_name = api_key.get("name") or "X-API-Key"
                    return {header_name: credential}
                # Declared in a query string or cookie — not a header this
                # interceptor can attach. Returning no headers here is
                # deliberate: falling through to the bearer default below
                # would send a plainly wrong `Authorization` header for a
                # scheme that asked for something else entirely.
                return {}

        return {"Authorization": f"Bearer {credential}"}
