"""Authentication providers for the NovaOS Client SDK."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Only for `CredentialService.get_credentials`'s annotation. Kept
    # `TYPE_CHECKING`-only, not a real import, so this module keeps the
    # property `tests/test_auth.py` relies on: importing it standalone
    # (`sys.path` pointed straight at this file, no package `__init__`) pulls
    # in nothing else. A real import here would make that bypass import the
    # whole `novaos_client_sdk` package instead, defeating it.
    from novaos_client_sdk.interceptors import ClientCallContext


class AuthProvider(ABC):
    """Abstract base class for authentication providers.

    All authentication providers must implement this interface to provide
    authentication headers for HTTP requests.

    Example:
        ```python
        class CustomAuth(AuthProvider):
            def get_headers(self) -> dict[str, str]:
                return {"X-Custom-Auth": "value"}
        ```
    """

    @abstractmethod
    def get_headers(self) -> dict[str, str]:
        """Get authentication headers for HTTP requests.

        Returns:
            Dictionary of HTTP headers to include in requests.
            Should include authentication credentials in the appropriate format.
        """
        pass


class BearerTokenAuth(AuthProvider):
    """Bearer token authentication provider.

    Formats a Bearer token into the standard `Authorization: Bearer <token>` header.

    The SDK is NOT responsible for creating, obtaining, or managing token lifecycle.
    The application must provide the token, and the SDK formats it into headers.

    Example:
        ```python
        from novaos_client_sdk.auth import BearerTokenAuth
        from novaos_client_sdk.client import Client

        # Application provides token (e.g., from OAuth flow)
        token = my_app.get_access_token()

        # SDK formats and uses it
        auth = BearerTokenAuth(token=token)
        client = Client(base_url="http://localhost:8000", auth=auth)
        ```

    Note:
        Bearer tokens are dynamic and should be provided by the application at runtime.
        Do NOT store Bearer tokens in environment variables or `.env` files.
    """

    def __init__(self, token: str) -> None:
        """Initialize Bearer token authentication.

        Args:
            token: Bearer token to use for authentication.
                The token should be provided by the application (e.g., from OAuth flow).
        """
        if not token:
            raise ValueError("Token cannot be empty")
        self._token = token

    def get_headers(self) -> dict[str, str]:
        """Get Bearer token authentication headers.

        Returns:
            Dictionary with `Authorization: Bearer <token>` header.
        """
        return {"Authorization": f"Bearer {self._token}"}

    @property
    def token(self) -> str:
        """Get the current Bearer token (read-only for debugging/logging)."""
        return self._token

    @token.setter
    def token(self, value: str) -> None:
        """Update the Bearer token (e.g., after refresh).

        Args:
            value: New Bearer token to use.
        """
        if not value:
            raise ValueError("Token cannot be empty")
        self._token = value


class APIKeyAuth(AuthProvider):
    """API key authentication provider.

    Formats an API key into a custom header (default: `X-API-Key: <key>`).

    The SDK is NOT responsible for creating or validating API keys.
    The application must provide the API key, and the SDK formats it into headers.

    Example:
        ```python
        from novaos_client_sdk.auth import APIKeyAuth
        from novaos_client_sdk.client import Client

        # Application provides API key
        api_key = "your-api-key"

        # SDK formats and uses it
        auth = APIKeyAuth(api_key=api_key)
        client = Client(base_url="http://localhost:8000", auth=auth)
        ```

    Note:
        API keys are static and can be stored in environment variables for
        service-to-service authentication. Use `get_api_key_from_env()` helper
        for convenience.
    """

    def __init__(self, api_key: str, header_name: str = "X-API-Key") -> None:
        """Initialize API key authentication.

        Args:
            api_key: API key to use for authentication.
            header_name: Name of the HTTP header to use (default: "X-API-Key").
        """
        if not api_key:
            raise ValueError("API key cannot be empty")
        if not header_name:
            raise ValueError("Header name cannot be empty")
        self._api_key = api_key
        self._header_name = header_name

    def get_headers(self) -> dict[str, str]:
        """Get API key authentication headers.

        Returns:
            Dictionary with API key header (e.g., `{"X-API-Key": "<key>"}`).
        """
        return {self._header_name: self._api_key}

    @property
    def api_key(self) -> str:
        """Get the current API key (read-only for debugging/logging)."""
        return self._api_key

    @property
    def header_name(self) -> str:
        """Get the header name used for the API key."""
        return self._header_name


def get_api_key_from_env(
    env_var: str = "ACP_API_KEY", header_name: str = "X-API-Key"
) -> APIKeyAuth:
    """Get API key from environment variable and create APIKeyAuth instance.

    This is a convenience helper for API keys only. Bearer tokens should NOT
    be stored in environment variables and should be provided by the application
    at runtime.

    Args:
        env_var: Name of the environment variable containing the API key
            (default: "ACP_API_KEY").
        header_name: Name of the HTTP header to use (default: "X-API-Key").

    Returns:
        APIKeyAuth instance configured with the API key from environment.

    Raises:
        ValueError: If the environment variable is not set or is empty.

    Example:
        ```python
        import os
        from novaos_client_sdk.auth import get_api_key_from_env

        # Set environment variable
        os.environ["ACP_API_KEY"] = "your-api-key"

        # Get auth from environment
        auth = get_api_key_from_env()
        ```

    Note:
        This helper is for API keys only. Bearer tokens are dynamic and should
        be provided by the application at runtime, not from environment variables.
    """
    import os

    api_key = os.getenv(env_var)
    if not api_key:
        raise ValueError(
            f"Environment variable '{env_var}' is not set or is empty. "
            "Please set the API key in the environment or provide it directly to APIKeyAuth."
        )
    return APIKeyAuth(api_key=api_key, header_name=header_name)


class CredentialService(ABC):
    """Async credential seam for
    :class:`novaos_client_sdk.auth_interceptor.AuthInterceptor`.

    ``AuthProvider`` above is synchronous and read once, at ``Client``
    construction — deliberately: see its module docstring's "The SDK is NOT
    responsible for creating, obtaining, or managing token lifecycle." That
    remains true here. What ``AuthProvider`` cannot do is hand back a
    *different* credential on a later call, which is exactly what a
    client-credentials OAuth2 token needs once it is close to expiring. This
    class is the seam that adds that, without touching ``AuthProvider`` at
    all: :class:`~novaos_client_sdk.auth_interceptor.AuthInterceptor` prefers a
    ``CredentialService`` when one is configured and falls back to the
    constructor's ``AuthProvider`` otherwise, so nothing already built against
    ``AuthProvider`` has to change.

    The application still owns obtaining the credential — refreshing an OAuth2
    token, say. This class only adds a hook the application can refresh
    *through*; it does not implement any refresh itself.

    Example:
        ```python
        class LogtoTokenService(CredentialService):
            def __init__(self, logto_client) -> None:
                self._logto_client = logto_client

            async def get_credentials(self, scheme_name, context):
                # The application's own refresh logic decides whether the
                # cached token is still good; the SDK never sees that logic.
                return await self._logto_client.get_access_token()
        ```
    """

    @abstractmethod
    async def get_credentials(
        self, scheme_name: str, context: ClientCallContext
    ) -> str | dict[str, str] | None:
        """Return the credential for *scheme_name*, or ``None`` if this
        service has nothing for it.

        Args:
            scheme_name: The Agent Card ``securitySchemes`` key
                :class:`~novaos_client_sdk.auth_interceptor.AuthInterceptor`
                resolved for this call — empty string if the call has no
                :class:`~novaos_client_sdk.a2a_agent_card.AgentCard` to read a
                scheme from at all.
            context: This call's
                :class:`~novaos_client_sdk.interceptors.ClientCallContext`.

        Returns:
            A bare credential value (a bearer token, an API key) for
            ``AuthInterceptor`` to format into a header using the scheme's
            declared shape; or a ready-made ``{header: value}`` mapping to use
            unchanged, which is how a wrapped synchronous ``AuthProvider``
            answers, since it already knows its own header shape; or ``None``
            to say this service has nothing for this scheme, which
            ``AuthInterceptor`` reads as "fall back to the constructor's
            ``AuthProvider``".
        """


class InMemoryContextCredentialStore(CredentialService):
    """Fixed credentials, keyed by session id and then by scheme name.

    Matches the reference ``a2a-python``
    ``InMemoryContextCredentialStore`` (``src/a2a/client/auth/credentials.py``):
    a lookup table an application populates itself, rather than a service that
    fetches or refreshes anything. It exists for the common case where the
    application already has a per-session token — from its own OAuth2 flow —
    and needs no more than a place to hand it to
    :class:`~novaos_client_sdk.auth_interceptor.AuthInterceptor`. An
    application whose credential genuinely changes without being told (a
    background refresh loop) should implement :class:`CredentialService`
    directly instead of trying to keep this store's contents current.

    The session id comes from ``context.state["session_id"]`` — a convention
    this store alone reads; ``AuthInterceptor`` passes its ``context`` through
    unopened — and defaults to ``""`` when a call's context sets none, so a
    single caller that never populates ``session_id`` still gets one shared
    credential per scheme rather than a ``KeyError``.
    """

    def __init__(self) -> None:
        self._credentials: dict[str, dict[str, str]] = {}

    def set_credentials(self, session_id: str, scheme_name: str, credential: str) -> None:
        """Store *credential* for *scheme_name*, under *session_id*.

        Args:
            session_id: Matches ``context.state["session_id"]`` on the calls
                this credential should answer; ``""`` for the no-session
                default.
            scheme_name: The Agent Card ``securitySchemes`` key this credential
                answers.
            credential: The bare credential value — see
                :meth:`CredentialService.get_credentials`.
        """
        self._credentials.setdefault(session_id, {})[scheme_name] = credential

    async def get_credentials(
        self, scheme_name: str, context: ClientCallContext
    ) -> str | dict[str, str] | None:
        """Return the stored credential for this call's session and scheme, or
        ``None`` if nothing was stored for either."""
        session_id = context.state.get("session_id", "") if context is not None else ""
        return self._credentials.get(session_id, {}).get(scheme_name)
