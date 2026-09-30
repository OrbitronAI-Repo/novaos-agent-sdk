"""The ``user-context/v1`` A2A extension — sealed ``context.secrets`` delivery.

This is the extension chosen to carry a run's
resolved secrets from the Temporal worker to the agent pod, and it is the first
real implementation on top of the generic extension mechanism. It is deliberately a
small module: the three hard problems it sits between are already solved
elsewhere, and this is the join.

============================  ==============================================
:mod:`.a2a_extensions`        declaring, negotiating and dispatching *any*
                              extension
:mod:`.a2a_sealed_envelope`   the cipher suite and the wire format
:mod:`.a2a_pod_keyring`       which private key opens which envelope
**this module**               "the payload at ``metadata[user-context/v1]``
                              is a map of sealed secrets, and here is how a
                              skill gets one out"
============================  ==============================================

**Adding it changed nothing in the core.**  The extension mechanism's OCP requirement was that a
future extension be addable without editing the negotiation/dispatch code, and
this was that requirement's first real test: :class:`UserContextExtension`
sets ``uri``, overrides :meth:`~.a2a_extensions.A2AExtension.read_value`, and is
otherwise an ordinary :class:`~.a2a_extensions.A2AExtension`.  No line of
``a2a_extensions.py`` moved to accommodate it.

**The wire shape.**  One JSON object under the extension's own URI::

    "metadata": {
      "novaos.platform/extensions/user-context/v1": {
        "secrets": {
          "OPENAI_API_KEY": {"v": 1, "suite": "…", "kid": "…", "epk": "…", "iv": "…", "ct": "…"},
          "SMTP_PASSWORD":  {"v": 1, …}
        }
      }
    }

The wrapper object rather than a bare secret map is what lets ``v1`` grow a
sibling field — ``mcp_gateway_token`` is the named candidate (see
the platform's adapter layer note on it) — without becoming ``v2`` and invalidating every
Agent Card that declared this URI.  **Secret names travel in the clear**, by
design: a pod must route a value to ``ctx.get_secret("SMTP_PASSWORD")`` before
it can decrypt anything, and the threat model this design addresses is disclosure of credential
*values*.

**Opening is deferred, and that is the whole design.**  Every envelope is bound
to one delivery — tenant, instance, run and secret name — and
:meth:`~.a2a_extensions.A2AExtension.read_value` is handed the payload and
nothing else: it has no idea which run it is serving.  So it returns a
:class:`SealedUserContext`, which holds the envelopes and opens one only when a
caller supplies the delivery it believes it is serving.  Deferring is not
laziness for its own sake; it is what keeps the binding check meaningful, since
a binding built from the envelope itself would check nothing at all.

**Who calls what.**  A skill does not touch this module:
:meth:`novaos_client_sdk.agent_context.AgentExecutionContext.get_secret` builds
the delivery from the request it is serving and calls
:meth:`SealedUserContext.open`.  A pod author's whole involvement is one
registration at construction::

    agent = AgentBuilder(name="mailer", version="1.0.0")
    agent.register_extension(UserContextExtension())

That registration is explicit on purpose (the extension mechanism deliberately has no
module-level default registry) — what an agent declares on its card should be something its
construction states outright, not a consequence of which modules were imported.

Requires the ``crypto`` extra::

    pip install 'novaos-client-sdk[crypto]'

Design notes: this extension's choice and the SDK gap it fills are documented in the
migration's architecture decision record.
Sealing side: the platform's shared A2A library's ``buildUserContextExtension``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Final

from .a2a_extensions import A2AExtension
from .a2a_pod_keyring import PodKeyring
from .a2a_sealed_envelope import SEALED_ENVELOPE_SUITE, SecretDelivery
from .exceptions import SealedEnvelopeFormatError, SecretNotDeliveredError

__all__ = [
    "USER_CONTEXT_EXTENSION_URI",
    "USER_CONTEXT_SECRETS_FIELD",
    "SealedUserContext",
    "UserContextExtension",
]

#: The extension's identifying URI, fixed by design.  Matched exactly on
#: both sides — A2A gives extension URIs no normalisation rules — so it is a
#: constant here and a constant in the platform's shared A2A library, never a
#: string literal at a call site.
USER_CONTEXT_EXTENSION_URI: Final = "novaos.platform/extensions/user-context/v1"

#: The payload field holding the sealed secret map.
USER_CONTEXT_SECRETS_FIELD: Final = "secrets"


class SealedUserContext:
    """The sealed payload of one request, opened one secret at a time.

    Produced by :meth:`UserContextExtension.read_value`.  Holding one is holding
    ciphertext and a keyring — never plaintext: nothing is decrypted until
    :meth:`open` is called, and nothing is cached afterwards.  A skill that
    needs a value twice should keep the string it was given rather than ask
    again, which is the cheaper *and* the more careful of the two.

    The instance is safe to log: :meth:`secret_names` is the only thing it will
    render, and names travel in the clear on the wire anyway.
    """

    __slots__ = ("_envelopes", "_keyring")

    def __init__(self, envelopes: Mapping[str, Any], keyring: PodKeyring) -> None:
        """Build a sealed context.  Normally produced by :class:`UserContextExtension`.

        Args:
            envelopes: The ``secrets`` map as it arrived, name → sealed envelope.
            keyring: The private keys this pod holds.
        """
        self._envelopes = dict(envelopes)
        self._keyring = keyring

    @property
    def secret_names(self) -> tuple[str, ...]:
        """Every secret name this request delivered, sorted.

        Safe to log, and the right thing to log when a skill cannot find the
        name it expected: it distinguishes "the instance declares no such
        secret" from "delivery is not working at all".
        """
        return tuple(sorted(self._envelopes))

    def __contains__(self, name: object) -> bool:
        """Was a secret delivered under this name?  Says nothing about opening it."""
        return isinstance(name, str) and name in self._envelopes

    def __len__(self) -> int:
        """How many secrets this request delivered."""
        return len(self._envelopes)

    def __repr__(self) -> str:
        """Render by name and count only — never by content."""
        return f"SealedUserContext(secrets={list(self.secret_names)!r})"

    def open(self, name: str, *, tenant_id: str, instance_id: str, run_id: str) -> str:
        """Open one delivered secret.

        The delivery is supplied by the caller rather than read out of the
        envelope, which is the point of the whole exercise: an envelope lifted
        from one request and replayed into another carries its original
        binding, so opening it against *this* request's identifiers fails
        instead of handing over a value the caller was never entitled to.

        ``secret_name`` is not a parameter — it is ``name``.  Binding the value
        to the key it is filed under is what stops an envelope being moved from
        ``API_KEY`` to ``SMTP_PASSWORD`` inside one run, and letting a caller
        pass the two separately would let that check be defeated by a typo.

        Args:
            name: The secret's name.
            tenant_id: Tenant this run belongs to.  Pass the value the
                auth-proxy sidecar vouched for where there is one — see
                :class:`~novaos_client_sdk.a2a_sealed_envelope.SecretDelivery`.
            instance_id: The user-agent instance being executed.
            run_id: The run being served.

        Returns:
            The secret value.

        Raises:
            SecretNotDeliveredError: No secret arrived under that name.
            RecipientKeyUnavailableError: The envelope names a key this pod does
                not hold — usually one that rotated out of its overlap window,
                or was revoked.
            SealedEnvelopeFormatError: The envelope is malformed or names a
                suite this SDK does not speak.
            SealedEnvelopeError: The envelope did not open.  Reported without
                saying whether the cause was the wrong key, a tampered
                ciphertext or a binding mismatch, since telling them apart is a
                decryption oracle.
        """
        envelope = self._envelopes.get(name)
        if envelope is None:
            raise SecretNotDeliveredError(
                f"no secret named '{name}' was delivered with this request",
                details={"secret_name": name, "delivered": list(self.secret_names)},
            )

        return self._keyring.open(
            envelope,
            SecretDelivery(
                tenant_id=tenant_id,
                instance_id=instance_id,
                run_id=run_id,
                secret_name=name,
            ),
        )


class UserContextExtension(A2AExtension):
    """The ``novaos.platform/extensions/user-context/v1`` extension.

    Register it on an agent that needs per-run secrets::

        agent = AgentBuilder(name="mailer", version="1.0.0")
        agent.register_extension(UserContextExtension())

        @agent.skill("send")
        async def send(ctx, text):
            password = ctx.get_secret("SMTP_PASSWORD")

    **Serving side only.**  The caller's half —
    :meth:`~.a2a_extensions.A2AExtension.request_value` — is deliberately not
    overridden: sealing needs the destination pod's *public* key, which lives in
    the control plane's key store, and nothing in an agent process has it.  An
    agent calling another agent therefore declares this extension without being
    able to populate it, and inherits the base class's "publish nothing", which
    is the truthful answer rather than a stub that would seal to itself.

    Args:
        keyring: The private keys this pod holds.  Defaults to a
            :class:`~novaos_client_sdk.a2a_pod_keyring.PodKeyring` over the
            mounted key directory, which reads nothing until the first envelope
            actually needs opening.
        required: Whether a caller must activate this extension to be served at
            all.  Defaults to ``False``, matching the base class: an agent whose
            secrets are optional stays reachable by a caller that cannot seal
            any.  An agent that cannot function without them should pass
            ``True`` and get a clean ``-32008`` instead of a skill failing
            somewhere deeper.
    """

    uri: ClassVar[str] = USER_CONTEXT_EXTENSION_URI
    description: ClassVar[str] = (
        "Delivers a run's resolved NovaOS secrets, each sealed to this pod's "
        "recipient key and bound to the tenant, instance, run and secret name "
        "it was issued for."
    )

    def __init__(self, keyring: PodKeyring | None = None, *, required: bool = False) -> None:
        self._keyring = keyring if keyring is not None else PodKeyring()
        # Shadows the class attribute per instance: two agents in one process
        # may reasonably disagree about whether secrets are optional, and the
        # base class reads `self.required`.
        self.required = required

    @property
    def keyring(self) -> PodKeyring:
        """The keys this extension opens envelopes with."""
        return self._keyring

    def params(self) -> dict[str, Any]:
        """What the Agent Card publishes about this extension.

        The suite is the compatibility contract and the only thing a caller can
        act on: a worker that seals under a suite the pod does not implement
        should discover that from the card, not from a pod-side format error on
        the first run of a rollout.  Key ids are deliberately absent — the
        sealing side gets those from the control plane's key store, which is
        also the only place that knows when one was revoked.
        """
        return {"suite": SEALED_ENVELOPE_SUITE, "fields": [USER_CONTEXT_SECRETS_FIELD]}

    def read_value(self, value: Any) -> SealedUserContext:
        """Interpret the payload a caller published at ``metadata[uri]``.

        Args:
            value: The payload, or ``None`` when the caller activated the
                extension but published nothing — which is ordinary: a fleet run
                for an instance that declares no secrets activates the extension
                and sends no payload, and that is "no secrets", not an error.

        Returns:
            The delivered envelopes, unopened.

        Raises:
            SealedEnvelopeFormatError: The payload is not the shape this version
                describes.  Refused rather than ignored: a caller that sent
                *something* here and got silence would look like a caller that
                sent nothing, and the run would fail later with
                :class:`~novaos_client_sdk.exceptions.SecretNotDeliveredError`
                pointing at the wrong problem.
        """
        if value is None:
            return SealedUserContext({}, self._keyring)
        if not isinstance(value, dict):
            raise SealedEnvelopeFormatError(
                f"'{USER_CONTEXT_EXTENSION_URI}' payload must be a JSON object, "
                f"got {type(value).__name__}"
            )

        secrets = value.get(USER_CONTEXT_SECRETS_FIELD)
        if secrets is None:
            return SealedUserContext({}, self._keyring)
        if not isinstance(secrets, dict):
            raise SealedEnvelopeFormatError(
                f"'{USER_CONTEXT_EXTENSION_URI}' field "
                f"'{USER_CONTEXT_SECRETS_FIELD}' must be a JSON object, "
                f"got {type(secrets).__name__}"
            )

        # Envelope *contents* are not validated here — `open_envelope` does that
        # per secret, at the point where a failure can be attributed to one name
        # rather than failing the whole request for one bad entry.
        return SealedUserContext(secrets, self._keyring)
