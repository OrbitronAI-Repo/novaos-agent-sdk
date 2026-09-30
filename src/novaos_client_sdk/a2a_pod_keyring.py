"""The private keys a pod opens sealed envelopes with.

:mod:`novaos_client_sdk.a2a_sealed_envelope` knows how to open an envelope given
a key.  This module is where the key comes from, and it is the whole of the
pod's side of NovaOS's key management: it loads recipient private keys from the
volume the control plane mounts, picks the right one by ``kid``, and notices
when that volume changes underneath it.

**Where the keys come from.**  The control plane mints an X25519 key pair per
*scope* — one agent pod, or one fleet agent — writes the private half into a
Kubernetes Secret, and keeps only the public half to seal with.  The Secret is
mounted as a directory into the agent container and into nothing else, one file
per key, named ``{kid}.jwk``.  Nothing here reaches out over the network: a key
that is not on the filesystem is a key this pod does not have.

**Why a directory and not an environment variable.**  Environment variables are
read once at process start, so rotating a key would mean restarting the pod —
which for a long-lived fleet Deployment is a rolling restart of the thing
currently serving traffic, an operational cost high enough that rotation would
quietly stop happening.  The kubelet updates a mounted Secret in place, so
:class:`PodKeyring` picks up a new key by re-reading the directory.  Two smaller
reasons point the same way: a mounted file is not inherited by child processes
the way the environment is, and it does not show up in ``kubectl describe pod``,
in crash dumps, or in agent-ops' env-visibility endpoint.

**Rotation, seen from the pod.**  During a rotation the directory holds two
keys: the one the worker is now sealing to, and its predecessor, kept until
requests sealed just before the change have all landed.  The pod needs no
notion of which is which — it opens what it is given, by ``kid``.  A key
disappearing from the directory is how *both* the end of an overlap window and
a revocation reach this process; from here they look the same, and both mean
the same thing: envelopes naming that key can no longer be opened.

**A stale view is re-read before a cached key is trusted, not only after a
miss.**  A ``kid`` this pod has not seen is the normal first symptom of a
rotation, so a miss re-reads the directory and tries again — but a *hit* is
re-checked too, because otherwise a key that has been revoked keeps opening
envelopes for the life of the process and revocation never reaches a pod that
had already loaded it successfully.  That mattered here more than anywhere: a
fleet Deployment is long-lived by definition and is the only pod model that
reads this keyring at all.

Neither path re-reads more often than :data:`MIN_RELOAD_INTERVAL_SECONDS`.  The
throttle is what makes checking a hit affordable — at most one directory read
per interval however many envelopes arrive — and it is also a defence: an
unknown ``kid`` is what a stream of forged envelopes looks like, and an
unthrottled reload would turn that into filesystem load an attacker controls.
A revocation therefore takes effect within that interval plus the kubelet's own
Secret refresh, rather than at the next pod restart.

Requires the ``crypto`` extra::

    pip install 'novaos-client-sdk[crypto]'
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from .a2a_sealed_envelope import (
    SecretDelivery,
    b64u_decode,
    b64u_encode,
    envelope_key_id,
    open_envelope,
)
from .exceptions import RecipientKeyUnavailableError, SealedEnvelopeFormatError

try:  # pragma: no cover - exercised by the import-guard test
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
except ImportError as exc:  # pragma: no cover - exercised by the import-guard test
    raise ImportError(
        "Sealed-envelope secret delivery needs the 'cryptography' package. "
        "Install the SDK's crypto extra: pip install 'novaos-client-sdk[crypto]'"
    ) from exc

__all__ = [
    "AGENT_KEYS_DIR_ENV",
    "DEFAULT_AGENT_KEYS_DIR",
    "KEY_FILE_SUFFIX",
    "MIN_RELOAD_INTERVAL_SECONDS",
    "PodKeyring",
    "RecipientKey",
    "jwk_thumbprint",
]

logger = logging.getLogger(__name__)

#: Where the control plane mounts the recipient key Secret.
DEFAULT_AGENT_KEYS_DIR: Final = "/etc/novaos/agent-keys"

#: Overrides :data:`DEFAULT_AGENT_KEYS_DIR`.  Set by the pod spec in
#: environments whose mount path differs, and by local development.
AGENT_KEYS_DIR_ENV: Final = "NOVAOS_AGENT_KEYS_DIR"

#: Extension on every key file.  The stem is the key's ``kid``.
KEY_FILE_SUFFIX: Final = ".jwk"

#: Floor on how often the key directory may be re-read — on a cache *hit* as
#: well as a miss, since a hit that never re-checked is how a revoked key
#: survived in a running pod.  Long enough that a forged-``kid`` flood cannot
#: drive filesystem load, short enough that a rotation is picked up well inside
#: the overlap window the control plane keeps, and that a revocation takes
#: effect in seconds rather than at the next restart.
MIN_RELOAD_INTERVAL_SECONDS: Final = 5.0

_X25519_KEY_BYTES: Final = 32


def jwk_thumbprint(x: str) -> str:
    """Return the RFC 7638 thumbprint of an X25519 public key.

    RFC 8037 §2 fixes an OKP key's thumbprint members at ``crv``, ``kty`` and
    ``x``; RFC 7638 fixes their order and requires compact JSON with no
    whitespace.  The canonical form is written out literally rather than handed
    to :func:`json.dumps`, because the TypeScript half computes the same value
    from the same rule and a serialiser's key ordering is not part of either
    language's contract.

    Args:
        x: The public key, unpadded base64url — a JWK's ``x`` member.
    """
    canonical = f'{{"crv":"X25519","kty":"OKP","x":"{x}"}}'
    return b64u_encode(hashlib.sha256(canonical.encode("utf-8")).digest())


@dataclass(frozen=True, slots=True)
class RecipientKey:
    """One private key this pod holds, ready to open envelopes sealed to it.

    ``public_bytes`` is kept alongside the private key because the sealed
    envelope's key schedule mixes the recipient public key into its HKDF
    context: deriving it once at load time saves doing it per secret, per run.

    The instance is deliberately opaque — it has no ``__str__`` worth printing
    and the underlying :class:`X25519PrivateKey` does not render its scalar — so
    an accidental log line carries a key *id*, never key material.
    """

    kid: str
    private_key: X25519PrivateKey
    public_bytes: bytes


class PodKeyring:
    """The recipient keys available inside this agent process.

    Construct it once per process.  It is cheap to hold and its
    :meth:`open` is the only entry point the rest of the SDK needs;
    the ``user-context/v1`` extension is the intended caller.

    Args:
        directory: Where key files live.  Defaults to the
            :data:`AGENT_KEYS_DIR_ENV` environment variable, then to
            :data:`DEFAULT_AGENT_KEYS_DIR`.
        now: Injectable clock, used only for the reload throttle.  Tests drive
            it; production omits it.

    Example:
        >>> keyring = PodKeyring()  # doctest: +SKIP
        >>> secret = keyring.open(  # doctest: +SKIP
        ...     envelope,
        ...     SecretDelivery(
        ...         tenant_id=ctx.tenant_id,
        ...         instance_id=ctx.instance_id,
        ...         run_id=ctx.run_id,
        ...         secret_name="OPENAI_API_KEY",
        ...     ),
        ... )
    """

    def __init__(
        self,
        directory: str | os.PathLike[str] | None = None,
        *,
        now: Any = time.monotonic,
    ) -> None:
        if directory is None:
            directory = os.getenv(AGENT_KEYS_DIR_ENV) or DEFAULT_AGENT_KEYS_DIR
        self._directory: Path | None = Path(directory)
        self._now = now
        self._keys: dict[str, RecipientKey] = {}
        self._loaded = False
        self._last_load = float("-inf")

    @classmethod
    def from_keys(cls, keys: list[RecipientKey]) -> PodKeyring:
        """Build a keyring over keys already in memory.

        For tests and for embedding hosts that hold key material themselves.
        The caller owns the keys outright: this keyring has no directory at all,
        so a cache miss is final rather than a prompt to reload, and
        :meth:`reload` is a no-op instead of something that would replace the
        caller's keys with whatever is (not) on disk.
        """
        keyring = cls.__new__(cls)
        keyring._directory = None
        keyring._now = time.monotonic
        keyring._keys = {key.kid: key for key in keys}
        keyring._loaded = True
        keyring._last_load = float("inf")
        return keyring

    @property
    def key_ids(self) -> list[str]:
        """The ids of every key currently loaded, for diagnostics and health checks.

        Safe to log and to expose: a ``kid`` is a hash of the *public* half.
        Being able to see which keys a pod holds is most of what makes a
        rotation or a suspected compromise investigable.
        """
        self._ensure_loaded()
        return sorted(self._keys)

    def open(self, envelope: Any, delivery: SecretDelivery) -> str:
        """Open one sealed envelope with whichever key it names.

        Args:
            envelope: The envelope as it came off the wire.
            delivery: The delivery this pod believes it is serving — see
                :class:`~novaos_client_sdk.a2a_sealed_envelope.SecretDelivery`
                for why it should be built from sidecar-vouched values where
                they exist.

        Returns:
            The secret value.

        Raises:
            RecipientKeyUnavailableError: This pod holds no key with the
                envelope's ``kid``.  Usually a key that rotated out of its
                overlap window or was revoked; possibly a missing mount.
            SealedEnvelopeFormatError: The envelope is malformed or names a
                suite this SDK does not speak.
            SealedEnvelopeError: The envelope did not open.
        """
        kid = envelope_key_id(envelope)
        key = self._resolve(kid)
        return open_envelope(envelope, key.private_key, key.public_bytes, key.kid, delivery)

    def reload(self) -> None:
        """Re-read the key directory now, ignoring the throttle.

        Worth calling from a pod's readiness path, where the cost of a
        filesystem read is irrelevant and starting to serve with a stale view of
        the mounted Secret is not.

        A no-op on a keyring built by :meth:`from_keys`, which has no directory
        to re-read — the readiness-path advice above must not cost an embedding
        host the keys it handed over.
        """
        self._load()

    # -- internals ---------------------------------------------------------

    def _stale(self) -> bool:
        """Whether the loaded view is old enough to be worth re-reading."""
        return self._now() - self._last_load >= MIN_RELOAD_INTERVAL_SECONDS

    def _resolve(self, kid: str) -> RecipientKey:
        self._ensure_loaded()

        # Re-read a stale view BEFORE trusting a cached hit, not only after a
        # miss.
        #
        # `_load` replaces the key set rather than merging into it precisely so
        # that a key's *disappearance* takes effect — that is what makes
        # revocation reach this process. But while the only re-read was on the
        # miss path, a cached hit never checked anything: once a kid had been
        # loaded successfully, it kept opening envelopes for the life of the
        # process, and deleting the key from the mounted Secret changed nothing.
        # Revocation was therefore decorative in exactly the pod model that
        # needs it, since a fleet Deployment is long-lived by definition and is
        # the only model that reads this keyring at all.
        #
        # Observed end-to-end: with the key removed from the Secret and gone
        # from the mount, a fleet pod still returned the correct
        # plaintext, and the same request raised `RecipientKeyUnavailableError`
        # only after the pod was restarted.
        #
        # The throttle is what keeps this cheap and is the reason the check is a
        # staleness test rather than an unconditional read: at most one directory
        # read per MIN_RELOAD_INTERVAL_SECONDS however many envelopes arrive, so
        # a revocation takes effect within that window plus the kubelet's own
        # Secret refresh, and a flood of envelopes cannot drive filesystem load.
        if self._stale():
            self._load()

        key = self._keys.get(kid)
        if key is not None:
            return key

        # An unknown kid is the normal first symptom of a rotation, so look
        # again — but no more often than the throttle allows, since it is also
        # what a stream of forged envelopes looks like. Usually a no-op now: the
        # staleness check above will already have re-read and reset the clock,
        # so this fires only for a miss against a view too fresh to re-read.
        if self._stale():
            self._load()
            key = self._keys.get(kid)
            if key is not None:
                return key

        raise RecipientKeyUnavailableError(
            f"no recipient key '{kid}' is available in this pod",
            details={"key_id": kid, "available_key_ids": sorted(self._keys)},
        )

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self._load()

    def _load(self) -> None:
        """Replace the loaded key set with what is on disk right now.

        Replacing rather than merging is what makes a key's *disappearance*
        take effect: revocation reaches this process as a deleted file, and a
        keyring that only ever added keys would keep opening envelopes sealed to
        a key the platform has declared compromised.
        """
        if self._directory is None:
            # Built by `from_keys`: the caller owns the key material and there
            # is nothing on disk that could be newer than it.
            return

        self._loaded = True
        self._last_load = self._now()

        keys: dict[str, RecipientKey] = {}
        try:
            entries = sorted(self._directory.iterdir())
        except (FileNotFoundError, NotADirectoryError, PermissionError) as exc:
            # Not fatal at load time: a pod with no key mount is simply a pod
            # that receives no sealed secrets, and it should keep serving the
            # runs that do not need any. The failure surfaces at `open`, where
            # it can name the key that was actually wanted.
            logger.warning(
                "Recipient key directory is unavailable; no sealed secrets can be opened",
                extra={"directory": str(self._directory), "error": str(exc)},
            )
            self._keys = keys
            return

        for entry in entries:
            if entry.suffix != KEY_FILE_SUFFIX or not entry.is_file():
                continue
            key = self._read_key(entry)
            if key is not None:
                keys[key.kid] = key

        self._keys = keys
        logger.info(
            "Loaded recipient keys",
            extra={"directory": str(self._directory), "key_ids": sorted(keys)},
        )

    def _read_key(self, path: Path) -> RecipientKey | None:
        """Parse one key file, or return ``None`` and say why.

        One unreadable file must not cost the pod its other keys — a
        half-written file during a kubelet refresh is an ordinary transient,
        and failing the whole load would turn it into an outage.
        """
        try:
            jwk = json.loads(path.read_text(encoding="utf-8"))
            return _recipient_key_from_jwk(jwk, expected_kid=path.stem)
        except Exception as exc:  # noqa: BLE001 - any parse failure is the same non-fatal outcome
            # The file name is a key id, which is safe to log; the contents are
            # never touched by this message.
            logger.warning(
                "Skipping unreadable recipient key file",
                extra={"file": path.name, "error": str(exc)},
            )
            return None


def _recipient_key_from_jwk(jwk: Any, *, expected_kid: str | None = None) -> RecipientKey:
    """Build a :class:`RecipientKey` from a private JWK, validating as it goes.

    The thumbprint is recomputed and checked against the ``kid`` — and, when the
    key came from a file, against the file name too.  A key filed under a name
    that is not its own would be selected for envelopes it cannot open, and the
    resulting failure would look like a corrupted ciphertext rather than a
    misfiled key.
    """
    if not isinstance(jwk, dict):
        raise SealedEnvelopeFormatError("recipient key must be a JWK object")
    if jwk.get("kty") != "OKP":
        raise SealedEnvelopeFormatError(
            f"recipient key 'kty' must be 'OKP', got {jwk.get('kty')!r}"
        )
    if jwk.get("crv") != "X25519":
        raise SealedEnvelopeFormatError(
            f"recipient key 'crv' must be 'X25519', got {jwk.get('crv')!r}"
        )

    x = jwk.get("x")
    if not isinstance(x, str):
        raise SealedEnvelopeFormatError("recipient key 'x' must be a string")
    public_bytes = b64u_decode(x, "x", expected_bytes=_X25519_KEY_BYTES)
    private_bytes = b64u_decode(jwk.get("d"), "d", expected_bytes=_X25519_KEY_BYTES)

    kid = jwk_thumbprint(x)
    if jwk.get("kid") != kid:
        raise SealedEnvelopeFormatError(
            f"recipient key 'kid' is {jwk.get('kid')!r} but its thumbprint is {kid!r}"
        )
    if expected_kid is not None and expected_kid != kid:
        raise SealedEnvelopeFormatError(
            f"recipient key file is named {expected_kid!r} but holds key {kid!r}"
        )

    private_key = X25519PrivateKey.from_private_bytes(private_bytes)
    # Clear the decoded scalar; the key now lives inside the cryptography
    # object. Python offers no real guarantee here — the base64 source string is
    # an immutable str that stays interned until collected — but zeroing the one
    # mutable copy costs nothing and keeps the shortest-lived copy short-lived.
    del private_bytes

    return RecipientKey(kid=kid, private_key=private_key, public_bytes=public_bytes)
