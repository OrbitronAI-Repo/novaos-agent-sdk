"""NovaOS Sealed Envelope v1 — the opening half.

A2A carries extension data as plain JSON in the request body's ``metadata``
map.  The protocol gives that field no confidentiality at all: it is visible to
exactly the infrastructure a request header would be — the auth-proxy sidecar,
any proxy on the path, and APM tooling that records request bodies.  The
extension mechanism was chosen for ``context.secrets`` delivery anyway, on
the explicit condition that NovaOS build its own encryption on top.  The
Temporal worker seals each secret with the platform's shared crypto module;
this module is what opens it, and it runs **inside the agent process** —
past the sidecar, in the one place the plaintext is supposed to exist.

**The suite.** X25519 ECDH with a fresh ephemeral sender key per envelope,
HKDF-SHA256 to a 256-bit content key, AES-256-GCM to seal.  The construction is
spelled out byte for byte rather than delegated to a JOSE library, because the
sealing half is TypeScript and the two must agree exactly; a fully specified
suite can be pinned by shared test vectors, and "whatever two JOSE libraries
both call ECDH-ES+A256GCM" cannot.  ``tests/test_a2a_sealed_envelope.py`` opens
the same fixture ``packages/shared/src/crypto/testdata/sealed-envelope-vectors.json``
does in the platform monorepo.  Normative spec:
``docs/security/sealed-envelope-v1.md`` in the platform monorepo.

**Every envelope is bound to one delivery.**  The AEAD's additional data commits
to the tenant, instance, run and secret name the value was sealed for
(:class:`SecretDelivery`).  That data is never transmitted — both sides rebuild
it from identifiers they hold independently, which is exactly what makes the
check worth anything: an envelope lifted from one request and replayed into
another fails to open rather than quietly delivering a value the caller was
never entitled to.  On a fleet pod, where one process serves many tenants, that
is the difference between an encryption layer and a real isolation boundary.

**What this module is not.**  It does not know where an envelope came from —
reading ``metadata`` and picking the extension URI is a caller's job — and it
does not manage keys; :mod:`novaos_client_sdk.a2a_pod_keyring` holds those.

Requires the ``crypto`` extra::

    pip install 'novaos-client-sdk[crypto]'
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Final

from .exceptions import SealedEnvelopeError, SealedEnvelopeFormatError

try:  # pragma: no cover - exercised by the import-guard test
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric.x25519 import (
        X25519PrivateKey,
        X25519PublicKey,
    )
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
except ImportError as exc:  # pragma: no cover - exercised by the import-guard test
    raise ImportError(
        "Sealed-envelope secret delivery needs the 'cryptography' package. "
        "Install the SDK's crypto extra: pip install 'novaos-client-sdk[crypto]'"
    ) from exc

__all__ = [
    "MAX_SEALED_PLAINTEXT_BYTES",
    "SEALED_ENVELOPE_SUITE",
    "SEALED_ENVELOPE_VERSION",
    "SecretDelivery",
    "b64u_decode",
    "b64u_encode",
    "envelope_key_id",
    "open_envelope",
]

#: The cipher suite label.  Mixed into both the HKDF ``info`` and the AEAD's
#: additional data, so an envelope sealed under a future suite cannot open as a
#: v1 one even if its JSON still parses.  A new primitive means a new label and
#: a new :data:`SEALED_ENVELOPE_VERSION`, never a silent substitution.
SEALED_ENVELOPE_SUITE: Final = "NOVAOS-SEAL-X25519-HKDF-SHA256-AES256GCM/v1"

#: The envelope format version carried as ``v``.
SEALED_ENVELOPE_VERSION: Final = 1

#: Largest plaintext the format carries.  Mirrors the sealing side's limit so a
#: value that could not have been produced by NovaOS is refused before any key
#: material is touched.
MAX_SEALED_PLAINTEXT_BYTES: Final = 64 * 1024

_NONCE_BYTES: Final = 12
_TAG_BYTES: Final = 16
_CONTENT_KEY_BYTES: Final = 32
_X25519_KEY_BYTES: Final = 32


@dataclass(frozen=True, slots=True)
class SecretDelivery:
    """The one delivery an envelope is valid for.

    Every field is an identifier the pod already has from the request it is
    serving, and the worker had from the run it dispatched.  None of them is
    secret; binding to them does not hide them, it makes the ciphertext refuse
    to open anywhere else.

    Prefer values the **auth-proxy sidecar** vouched for over values read out of
    the request body where both are available — ``tenant_id`` in particular
    arrives as the ``X-NovaOS-Tenant-Id`` header the sidecar injects, and the
    sidecar strips any the caller tried to set.  A binding built from the body
    alone still defeats replay between runs, but a binding built from the header
    also defeats a caller who lies about who they are.

    Attributes:
        tenant_id: Tenant the run belongs to.
        instance_id: User-agent instance being executed.
        run_id: The run this secret was sealed for.
        secret_name: Name the value is delivered under, so an envelope cannot be
            moved from one secret to another inside the same run.
    """

    tenant_id: str
    instance_id: str
    run_id: str
    secret_name: str


def b64u_encode(data: bytes) -> str:
    """Encode ``data`` as unpadded base64url — the only encoding this format uses."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64u_decode(value: Any, field: str, *, expected_bytes: int | None = None) -> bytes:
    """Decode unpadded base64url strictly.

    ``base64.urlsafe_b64decode`` is permissive by default — it will happily
    accept the standard alphabet's ``+`` and ``/``, and Python's decoder ignores
    some invalid input rather than failing.  Re-encoding and comparing is the
    cheapest way to get the strict parser a wire format needs, and it keeps this
    decoder's acceptance identical to the TypeScript half's.

    Typed ``Any`` rather than ``str`` on purpose: every caller is parsing
    untrusted JSON, where a member can be absent or of any type, and the check
    belongs in one place rather than at each call site.

    Args:
        value: The encoded string, or whatever arrived where one was expected.
        field: Field name, used only in the error message.
        expected_bytes: When given, the exact decoded length required.

    Raises:
        SealedEnvelopeFormatError: If ``value`` is not unpadded base64url, or
            decodes to the wrong length.
    """
    if not isinstance(value, str) or not value:
        raise SealedEnvelopeFormatError(
            f"sealed envelope field '{field}' must be a non-empty string"
        )
    padded = value + "=" * (-len(value) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded)
    except (ValueError, TypeError) as exc:
        raise SealedEnvelopeFormatError(
            f"sealed envelope field '{field}' is not valid base64url"
        ) from exc
    if b64u_encode(decoded) != value:
        raise SealedEnvelopeFormatError(f"sealed envelope field '{field}' is not valid base64url")
    if expected_bytes is not None and len(decoded) != expected_bytes:
        raise SealedEnvelopeFormatError(
            f"sealed envelope field '{field}' must decode to {expected_bytes} bytes, "
            f"got {len(decoded)}"
        )
    return decoded


def envelope_key_id(envelope: Any) -> str:
    """Return the ``kid`` an envelope names, without opening it.

    A keyring needs this to pick which private key to try, before it has
    anything to decrypt with.  Validates only enough to answer the question.

    Raises:
        SealedEnvelopeFormatError: If ``envelope`` is not an object with a
            non-empty string ``kid``.
    """
    if not isinstance(envelope, dict):
        raise SealedEnvelopeFormatError("sealed envelope must be a JSON object")
    kid = envelope.get("kid")
    if not isinstance(kid, str) or not kid:
        raise SealedEnvelopeFormatError("sealed envelope field 'kid' must be a non-empty string")
    return kid


def _length_prefixed(parts: tuple[str | bytes, ...]) -> bytes:
    """Concatenate ``parts``, each behind its own four-byte big-endian length.

    Both the HKDF ``info`` and the AEAD's additional data are built from several
    variable-length strings, and both are rebuilt independently by the
    TypeScript half.  Joining on a delimiter would be ambiguous the moment a
    tenant id or secret name contained it — ``("a:b", "c")`` and ``("a", "b:c")``
    would produce the same bytes, which is precisely the confusion the binding
    exists to prevent.  A length prefix removes the ambiguity and is trivial to
    reimplement identically.
    """
    out = bytearray()
    for part in parts:
        raw = part.encode("utf-8") if isinstance(part, str) else part
        out += len(raw).to_bytes(4, "big")
        out += raw
    return bytes(out)


def _content_key(shared_secret: bytes, ephemeral_public: bytes, recipient_public: bytes) -> bytes:
    """Derive the AES key for one envelope.

    Including both public keys in ``info`` makes this a KEM context rather than
    a bare Diffie-Hellman output: a content key is inseparable from the pair of
    keys it was agreed between.  The HKDF salt is empty because the ephemeral
    public key already supplies the per-envelope entropy, and an empty salt is
    the one value Python's ``HKDF`` and Node's ``hkdfSync`` agree on without
    further specification.
    """
    return HKDF(
        algorithm=hashes.SHA256(),
        length=_CONTENT_KEY_BYTES,
        salt=b"",
        info=_length_prefixed((SEALED_ENVELOPE_SUITE, ephemeral_public, recipient_public)),
    ).derive(shared_secret)


def _additional_data(kid: str, delivery: SecretDelivery) -> bytes:
    """Build the AEAD's additional data for one delivery.

    Never transmitted.  Both sides construct it from values they already hold,
    so it authenticates *the circumstances of delivery* rather than anything in
    the envelope — which is the whole point.  Field order is part of the wire
    format and must match the TypeScript half exactly.
    """
    return _length_prefixed(
        (
            SEALED_ENVELOPE_SUITE,
            kid,
            delivery.tenant_id,
            delivery.instance_id,
            delivery.run_id,
            delivery.secret_name,
        )
    )


def open_envelope(
    envelope: Any,
    private_key: X25519PrivateKey,
    recipient_public: bytes,
    kid: str,
    delivery: SecretDelivery,
) -> str:
    """Open one sealed envelope.

    Args:
        envelope: The envelope as it came off the wire — a plain ``dict`` parsed
            from the request's ``metadata``.
        private_key: The recipient private key, held only by this pod.
        recipient_public: The 32 raw bytes of ``private_key``'s public half.
            Passed in rather than re-derived so a keyring that already holds it
            does not recompute it per secret.
        kid: The key id ``private_key`` is filed under.  Checked against the
            envelope before anything is decrypted.
        delivery: The delivery this pod believes it is serving.

    Returns:
        The secret value, decoded as UTF-8.

    Raises:
        SealedEnvelopeFormatError: The envelope is malformed, or names a version
            or suite this implementation does not speak.
        SealedEnvelopeError: The envelope names a different key, or failed to
            open.  A failure to open is reported without saying whether the
            cause was the wrong key, a tampered ciphertext or a binding
            mismatch — distinguishing them would be a decryption oracle.
    """
    parsed = _parse(envelope)
    envelope_kid = parsed["kid"]
    if envelope_kid != kid:
        raise SealedEnvelopeError(
            f"envelope is sealed for key '{envelope_kid}', not '{kid}'",
            details={"envelope_kid": envelope_kid, "key_id": kid},
        )

    ephemeral_public = b64u_decode(parsed["epk"], "epk", expected_bytes=_X25519_KEY_BYTES)
    nonce = b64u_decode(parsed["iv"], "iv", expected_bytes=_NONCE_BYTES)
    sealed = b64u_decode(parsed["ct"], "ct")
    if len(sealed) < _TAG_BYTES:
        raise SealedEnvelopeFormatError("sealed envelope field 'ct' is shorter than the GCM tag")
    if len(sealed) - _TAG_BYTES > MAX_SEALED_PLAINTEXT_BYTES:
        raise SealedEnvelopeFormatError(
            f"sealed envelope carries more than the {MAX_SEALED_PLAINTEXT_BYTES}-byte limit"
        )

    try:
        shared = private_key.exchange(X25519PublicKey.from_public_bytes(ephemeral_public))
    except ValueError as exc:
        # Curve25519 has a handful of low-order points — all-zero among them —
        # whose shared secret is the identity, and `cryptography` refuses them
        # with a plain ValueError. An `epk` naming one is perfectly well-formed,
        # so it gets past every check above and would otherwise escape as an
        # exception no caller of this module is told to expect. It is reported
        # as the same failure a wrong key gives: an attacker who can tell "your
        # key rejected my point" from "your key decrypted to garbage" has
        # learned something about the key.
        raise SealedEnvelopeError("sealed envelope failed to open") from exc

    key = _content_key(shared, ephemeral_public, recipient_public)

    try:
        plaintext = AESGCM(key).decrypt(nonce, sealed, _additional_data(envelope_kid, delivery))
    except InvalidTag as exc:
        raise SealedEnvelopeError("sealed envelope failed to open") from exc

    try:
        return plaintext.decode("utf-8")
    except UnicodeDecodeError as exc:
        # The tag already passed, so this is a NovaOS-produced envelope carrying
        # bytes that are not text — a sealing-side bug, not an attack.
        raise SealedEnvelopeFormatError("sealed envelope plaintext is not valid UTF-8") from exc


def _parse(envelope: Any) -> dict[str, str]:
    """Validate an envelope's JSON shape before any key material is touched.

    Everything is checked here so the crypto path can assume well-formed input,
    and so a malformed envelope is reported as malformed instead of surfacing as
    an opaque decryption failure that looks like an attack.
    """
    if not isinstance(envelope, dict):
        raise SealedEnvelopeFormatError("sealed envelope must be a JSON object")

    version = envelope.get("v")
    if version != SEALED_ENVELOPE_VERSION:
        raise SealedEnvelopeFormatError(f"unsupported sealed envelope version {version!r}")

    suite = envelope.get("suite")
    if suite != SEALED_ENVELOPE_SUITE:
        raise SealedEnvelopeFormatError(f"unsupported sealed envelope suite {suite!r}")

    parsed = {"kid": envelope_key_id(envelope)}
    for field in ("epk", "iv", "ct"):
        value = envelope.get(field)
        if not isinstance(value, str) or not value:
            raise SealedEnvelopeFormatError(
                f"sealed envelope field '{field}' must be a non-empty string"
            )
        parsed[field] = value
    return parsed
