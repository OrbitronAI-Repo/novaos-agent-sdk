"""Token verifier for validating execution tokens from the gateway.

Agents that want to verify execution tokens can use the JWKSTokenVerifier
to fetch and cache public keys from the gateway's JWKS endpoint, then
verify RS256-signed JWTs.

Usage:
    ```python
    import os
    from novaos_client_sdk.token_verifier import JWKSTokenVerifier

    # Gateway URL is typically set as an environment variable
    gateway_url = os.environ["GATEWAY_URL"]
    verifier = JWKSTokenVerifier(gateway_url, issuer=os.environ["NOVAOS_TOKEN_ISSUER"])

    # Verify a token from the Authorization header
    token = request.headers["Authorization"].removeprefix("Bearer ")
    claims = verifier.verify(token)
    print(f"Verified: user_id={claims['user_id']}, agent_id={claims['agent_id']}")
    ```

Requires PyJWT[crypto]:
    pip install PyJWT[crypto]
"""

from __future__ import annotations

from typing import Any, cast

import jwt
from jwt import PyJWKClient

from novaos_client_sdk.config import validate_host_allowed


class JWKSTokenVerifier:
    """Verifies execution tokens using the gateway's JWKS endpoint.

    Caches JWKS for a configurable duration. Agents derive the JWKS URL
    from the gateway URL they already know (GATEWAY_URL env var).

    The verifier uses the ``kid`` (Key ID) claim in the JWT header to
    match the correct key from the JWKS endpoint, supporting seamless
    key rotation.

    Args:
        gateway_url: Base URL of the execution gateway
            (e.g., ``http://novaos-execution-gateway:8081``). Validated against
            an outbound-host allowlist (``NOVAOS_ALLOWED_HOSTS``, if set) before
            any request is made — see :func:`novaos_client_sdk.config.validate_host_allowed`.
        audience: Expected ``aud`` claim in the JWT (default: ``acp-agent``).
        issuer: Expected ``iss`` claim in the JWT — **required**. Without this,
            a validly-*signed* token from an unrelated IAM tenant that
            happens to share this JWKS endpoint would still verify; PyJWT
            raises ``InvalidIssuerError`` when the token's ``iss`` doesn't
            match. There is deliberately no default that skips this check —
            pass the issuer your gateway actually issues tokens for.
        cache_ttl_seconds: How long to cache the JWKS response (default: 300s).
        allowed_hosts: Explicit hostname-suffix allowlist for ``gateway_url``.
            When ``None`` (default), falls back to the ``NOVAOS_ALLOWED_HOSTS``
            env var; when neither is set, no host restriction is applied.
    """

    def __init__(
        self,
        gateway_url: str,
        audience: str = "acp-agent",
        *,
        issuer: str,
        cache_ttl_seconds: int = 300,
        allowed_hosts: list[str] | None = None,
    ) -> None:
        validate_host_allowed(gateway_url, allowed_hosts)
        self._jwks_url = f"{gateway_url.rstrip('/')}/.well-known/jwks.json"
        self._audience = audience
        self._issuer = issuer
        self._jwks_client = PyJWKClient(
            self._jwks_url,
            cache_jwk_set=True,
            lifespan=cache_ttl_seconds,
        )

    @property
    def jwks_url(self) -> str:
        """The JWKS endpoint URL derived from the gateway URL."""
        return self._jwks_url

    def verify(self, token: str) -> dict:
        """Verify an execution token and return decoded claims.

        Automatically fetches and caches the JWKS. Matches the ``kid``
        from the JWT header to the correct key in the JWKS.

        Args:
            token: The JWT string (without ``Bearer `` prefix).

        Returns:
            Dictionary of decoded JWT claims, including:
            - ``user_id``: The authenticated user
            - ``tenant_id``: The tenant
            - ``run_id``: The execution run ID
            - ``instance_id``: The agent instance ID
            - ``agent_id``: The target agent ID (optional)
            - ``agent_name``: The target agent name (optional)
            - ``scopes``: Allowed scopes (optional)

        Raises:
            jwt.exceptions.InvalidTokenError: If the token is invalid,
                expired, or cannot be verified.
            jwt.exceptions.DecodeError: If the token is malformed.
        """
        signing_key = self._jwks_client.get_signing_key_from_jwt(token)
        decode_kwargs: dict[str, Any] = {
            "algorithms": ["RS256"],
            "audience": self._audience,
            "issuer": self._issuer,
        }
        return cast(
            dict[Any, Any],
            jwt.decode(token, signing_key.key, **decode_kwargs),
        )
