"""JWS signature verification for A2A Agent Cards.

``AgentCard.signatures`` (:mod:`novaos_client_sdk.a2a_agent_card`) is parsed and
re-emitted byte-for-byte but was never checked — defensible while every card
this SDK consumed came from NovaOS's own gateway over a trusted path, and
indefensible the moment :class:`~novaos_client_sdk.discovery.AgentDiscoveryService`
fetches a third-party partner agent's card, which its own docstring says it
supports. This module is the check: it turns a *well-formed* card into a
*trustworthy* one.

**Reference implementation.** ``a2a-sdk``'s ``a2a.utils.signing`` module is the
interoperability target — :func:`canonicalize_agent_card` reproduces its
``_canonicalize_agent_card`` byte-for-byte (verified against the real signer in
``tests/test_a2a_card_signing.py``), and :func:`verify_agent_card_signatures`
mirrors its ``create_signature_verifier``: at least one valid signature is
enough, and an unsigned card and a signed-but-invalid one raise distinct errors
(:class:`~novaos_client_sdk.exceptions.NoAgentCardSignatureError` and
:class:`~novaos_client_sdk.exceptions.InvalidAgentCardSignatureError`).

**One deliberate divergence.** The reference's ``create_signature_verifier``
takes a caller-supplied ``key_provider(kid, jku)``, so it can fetch keys from a
``jku`` URL named inside the *card's own* protected header. This module never
does that — trusting a URL the document being verified points to would make
the check circular. It only ever fetches keys from the ``jwks_url`` the caller
passed in explicitly, which is why that is the one required argument.

**Canonicalization is where this is won or lost.** Verification only means
anything if the bytes this module hashes are the exact bytes the signer hashed.
:func:`canonicalize_agent_card` uses ``rfc8785`` (an RFC 8785 JSON
Canonicalization Scheme implementation) rather than hand-rolling one, with the
same ``signatures``-removal and empty-value-stripping the reference applies
before canonicalizing.

**Canonicalize the fetched JSON, not a re-serialized dataclass.**
:func:`canonicalize_agent_card` and :func:`verify_agent_card_signatures` both
accept either an :class:`AgentCard` or a raw ``Mapping`` (a parsed
``response.json()`` body). Passing the raw mapping is not a convenience
overload — it is the only sound choice for a *fetched* card.
:meth:`AgentCard.from_dict` is deliberately tolerant, which means it is also
lossy: a field this SDK does not yet model (a vendor extension, a later spec
revision, anything ``from_dict`` does not read into a named attribute) does
not survive `AgentCard.from_dict(payload).to_dict()`. Canonicalizing that
round-tripped dict instead of the original payload would hash a *different*
document than whatever the signer actually signed, and a caller that verifies
the wrong bytes gets no guarantee at all — a genuinely-signed card carrying an
unmodeled field would fail verification, while a still-unmodeled field an
attacker tampered with would never be examined either way.
:class:`~novaos_client_sdk.discovery.AgentDiscoveryService` verifies the raw
``response.json()`` payload for exactly this reason; the ``AgentCard`` overload
exists for a card built or held in memory with no surviving wire bytes (tests,
mainly).

Requires the ``crypto`` extra::

    pip install 'novaos-client-sdk[crypto]'
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

try:  # pragma: no cover - exercised by TestAgentCardSigningImportGuard
    import jwt
    from jwt import PyJWKClient
    from jwt.exceptions import PyJWTError
    from jwt.utils import base64url_encode
except ImportError as exc:  # pragma: no cover - exercised by TestAgentCardSigningImportGuard
    raise ImportError(
        "Agent Card signature verification requires PyJWT. "
        "Install the SDK's crypto extra: pip install 'novaos-client-sdk[crypto]'"
    ) from exc

try:  # pragma: no cover - exercised by TestAgentCardSigningImportGuard
    import rfc8785
except ImportError as exc:  # pragma: no cover - exercised by TestAgentCardSigningImportGuard
    raise ImportError(
        "Agent Card signature verification requires rfc8785 (RFC 8785 JSON "
        "Canonicalization Scheme). Install the SDK's crypto extra: "
        "pip install 'novaos-client-sdk[crypto]'"
    ) from exc

from .a2a_agent_card import AgentCard
from .config import validate_host_allowed
from .exceptions import InvalidAgentCardSignatureError, NoAgentCardSignatureError

__all__ = [
    "DEFAULT_SIGNATURE_ALGORITHMS",
    "canonicalize_agent_card",
    "verify_agent_card_signatures",
]

#: Algorithms :func:`verify_agent_card_signatures` accepts when the caller does
#: not name its own list. Asymmetric only: a JWKS entry is a public key, and
#: accepting a symmetric algorithm like ``HS256`` here would let an attacker
#: who can influence the protected header sign with that public key treated as
#: an HMAC secret — the classic algorithm-confusion attack, which is exactly
#: why the reference implementation's ``algorithms`` parameter has no default.
DEFAULT_SIGNATURE_ALGORITHMS: Final = ["RS256", "ES256", "PS256"]


def _clean_empty(value: Any) -> Any:
    """Recursively drop empty strings, lists and dicts; keep everything else.

    A byte-for-byte port of ``a2a.utils.signing._clean_empty``: ``False`` and
    ``0`` are not empty and survive, only ``""``, ``[]`` and ``{}`` (including
    ones that became empty because every value inside them did) do not.
    """
    if isinstance(value, dict):
        cleaned = {k: c for k, v in value.items() if (c := _clean_empty(v)) is not None}
        return cleaned or None
    if isinstance(value, list):
        cleaned_list = [c for v in value if (c := _clean_empty(v)) is not None]
        return cleaned_list or None
    if isinstance(value, str) and not value:
        return None
    return value


def _payload_and_signatures(
    card: AgentCard | Mapping[str, Any],
) -> tuple[Mapping[str, Any], list[Any]]:
    """Split ``card`` into the mapping to canonicalize and its ``signatures``.

    Both come from the *same* source on purpose: mixing a raw payload's bytes
    with a dataclass's (possibly re-coerced) ``signatures`` list, or vice
    versa, would check a signature entry against a document it was never
    computed alongside.
    """
    if isinstance(card, AgentCard):
        return card.to_dict(), card.signatures
    signatures = card.get("signatures")
    return card, list(signatures) if isinstance(signatures, list) else []


def canonicalize_agent_card(card: AgentCard | Mapping[str, Any]) -> bytes:
    """Return the RFC 8785 canonical JSON bytes a card's signature is computed over.

    Drops ``signatures`` (a signature cannot cover itself) and recursively
    strips empty values the same way the reference implementation does, then
    canonicalizes what remains.

    Args:
        card: The card to canonicalize — either a parsed :class:`AgentCard`
            (its :meth:`~AgentCard.to_dict` is used) or, preferably for a
            *fetched* card, the raw JSON mapping it was parsed from. See the
            module docstring for why the raw mapping is the sound choice
            there. Not mutated either way.

    Returns:
        Canonical JSON as bytes, ready to base64url-encode into a JWS payload.
    """
    payload, _ = _payload_and_signatures(card)
    payload = dict(payload)
    payload.pop("signatures", None)
    cleaned = _clean_empty(payload)
    return rfc8785.dumps(cleaned if isinstance(cleaned, dict) else {})


def verify_agent_card_signatures(
    card: AgentCard | Mapping[str, Any],
    *,
    jwks_url: str,
    algorithms: Sequence[str] | None = None,
    timeout: float | None = None,
    allowed_hosts: list[str] | None = None,
) -> None:
    """Verify at least one of ``card``'s ``signatures`` against a JWKS endpoint.

    Succeeds silently the moment one signature verifies, matching the
    reference implementation's policy — an agent that rotates keys can carry
    both an old and a new signature during the overlap window without this
    treating the old one's presence as a failure.

    Args:
        card: The card to verify — either a parsed :class:`AgentCard` or, for
            a *fetched* card, the raw JSON mapping it was parsed from (the
            sound choice there; see the module docstring). Not mutated,
            regardless of outcome.
        jwks_url: JWKS endpoint to fetch verification keys from. This is
            **caller-supplied trusted configuration**, the same trust class as
            ``token_verifier.JWKSTokenVerifier``'s ``gateway_url`` — never a
            URL read out of the card itself (see the module docstring).
        algorithms: Acceptable JWS algorithms. Defaults to
            :data:`DEFAULT_SIGNATURE_ALGORITHMS`. Pass this explicitly to
            narrow it further (e.g. to a single algorithm your publisher uses).
        timeout: JWKS HTTP fetch timeout in seconds. ``None`` uses PyJWT's
            default.
        allowed_hosts: Explicit hostname-suffix allowlist for ``jwks_url``.
            When ``None`` (default), falls back to the ``NOVAOS_ALLOWED_HOSTS``
            env var; when neither is set, no host restriction is applied — see
            :func:`novaos_client_sdk.config.validate_host_allowed`.

    Raises:
        NoAgentCardSignatureError: ``card`` carries no signatures.
        InvalidAgentCardSignatureError: ``card`` carries signatures but no
            entry verifies against ``jwks_url`` under ``algorithms``.
    """
    payload, signatures = _payload_and_signatures(card)
    if not signatures:
        raise NoAgentCardSignatureError(
            "Agent Card has no signatures to verify.",
            details={"jwks_url": jwks_url},
        )

    validate_host_allowed(jwks_url, allowed_hosts)

    algs = list(algorithms) if algorithms is not None else list(DEFAULT_SIGNATURE_ALGORITHMS)
    jwks_client_kwargs: dict[str, Any] = {}
    if timeout is not None:
        jwks_client_kwargs["timeout"] = timeout
    jwks_client = PyJWKClient(jwks_url, **jwks_client_kwargs)

    encoded_payload = base64url_encode(canonicalize_agent_card(payload)).decode("utf-8")

    errors: list[str] = []
    for entry in signatures:
        protected = entry.get("protected") if isinstance(entry, dict) else None
        signature = entry.get("signature") if isinstance(entry, dict) else None
        if not isinstance(protected, str) or not isinstance(signature, str):
            errors.append("signature entry is missing 'protected' or 'signature'")
            continue

        token = f"{protected}.{encoded_payload}.{signature}"
        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
            jwt.decode(token, signing_key.key, algorithms=algs)
        except PyJWTError as exc:
            errors.append(str(exc))
            continue
        return  # at least one signature verified

    raise InvalidAgentCardSignatureError(
        f"None of the {len(signatures)} signature(s) on this Agent Card "
        f"verified against {jwks_url}.",
        details={"jwks_url": jwks_url, "errors": errors},
    )
