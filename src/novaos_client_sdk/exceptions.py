"""Exception classes for NovaOS Client SDK.

Hierarchy strategy
------------------

Before the A2A migration this hierarchy was rooted in ``ACPClientError`` and every
subclass carried an ``ACP`` prefix.  This design chose **neither** of the two obvious
options — it is not a mechanical ``ACP*`` → ``A2A*`` rename, and it is not a
hierarchy rooted in A2A.  Both would have been wrong for the same reason: *these
exceptions are overwhelmingly not protocol errors*.

Ten SDK modules raise from this hierarchy, and only ``client.py`` and
``discovery.py`` talk to an agent over the wire protocol at all.  ``memory.py``,
``mcp.py``, ``llm.py``, ``heartbeat.py``, ``observability.py``, ``gateway.py`` and
``dns.py`` are plain NovaOS platform HTTP clients.  Calling a memory-service
socket failure an ``A2AConnectionError`` would be as untrue as calling it an
``ACPConnectionError`` was — the prefix was already a misnomer, and migrating it
to ``A2A`` would have re-entrenched that misnomer under a name we could no longer
blame on history.

So the root is **protocol-neutral** (``NovaOSError``), and A2A gets a real,
narrow subtree of its own (``A2AProtocolError``) holding exactly the errors the
A2A specification actually defines.  Two properties follow:

* The genuinely-A2A errors have somewhere correct to live.  A2A v1.0.1 defines
  nine protocol error codes (``-32001`` … ``-32009``); under a flat ``A2A*``
  rename those would have had to fight the generic HTTP errors for names.
* A future protocol migration touches only the ``A2AProtocolError`` subtree,
  never the platform errors — the same open/closed argument applied
  to the extension mechanism.

There is no back-compat shim and there are no deprecated ``ACP*`` aliases, per
the migration's no-backward-compatibility scope decision.

Liskov substitutability (an explicit LSP requirement)
-----------------------------------------------------

Every subclass honors ``NovaOSError``'s contract, so code that catches the base
type is never surprised by a subclass:

* **Constructor** — every subclass accepts ``(message)`` positionally and
  ``retry_after`` / ``details`` by keyword.  Subclass-specific extras
  (``field``, ``status_code``, ``agent_name``, …) are **keyword-only**, so no
  subclass can reinterpret a positional argument the base gives a different
  meaning.  That was a live defect before this hierarchy: ``ACPValidationError("m", 1.5)``
  bound ``1.5`` to ``field`` while ``ACPClientError("m", 1.5)`` bound it to
  ``retry_after``.
* **Attributes** — ``message``, ``retry_after``, ``details`` and the
  ``is_retryable`` property are present on every subclass.  Subclasses may
  refine ``is_retryable``'s *value*, never its type or its presence.
"""

from collections.abc import Mapping
from typing import Any, ClassVar, Final

_RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})

# The A2A specification release the ``A2AProtocolError`` subtree is written
# against.  Pinned per ADR finding F17 and kept in step with
# ``a2a_types.A2A_SPEC_VERSION`` — bumping it is a deliberate edit, not drift.
A2A_ERROR_SPEC_VERSION: Final = "1.0.1"


class NovaOSError(Exception):
    """Base exception for every error raised by the NovaOS client SDK.

    Protocol-neutral by design — see the module docstring.  Catching this type
    catches everything the SDK raises, including :class:`A2AProtocolError`.
    """

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the error.

        Args:
            message: Human-readable error message
            retry_after: Optional retry delay in seconds
            details: Optional additional error details
        """
        super().__init__(message)
        self.message = message
        self.retry_after = retry_after
        self.details = details or {}

    @property
    def is_retryable(self) -> bool:
        """Whether this error is transient and the request can be retried."""
        return False


class NovaOSConnectionError(NovaOSError):
    """Raised when a connection to a NovaOS service or agent fails."""

    @property
    def is_retryable(self) -> bool:
        return True


class NovaOSAuthenticationError(NovaOSError):
    """Raised when authentication fails."""


class MissingExecutionTokenError(NovaOSAuthenticationError):
    """Raised when an execution token is required but not present.

    This typically means the agent is running outside the auth-proxy (e.g. local
    dev, direct HTTP, or a test harness) and no ``X-NovaOS-Execution-Token`` header
    was injected.  Set the environment variable ``NOVAOS_EXECUTION_TOKEN`` or pass
    an explicit token to resolve this.
    """

    #: Default message, used when the caller does not supply one.
    DEFAULT_MESSAGE: ClassVar[str] = (
        "No execution token available. "
        "In production this token is injected by the auth-proxy sidecar via "
        "the 'X-NovaOS-Execution-Token' header. "
        "For local development, set the 'NOVAOS_EXECUTION_TOKEN' environment "
        "variable or pass an explicit execution_context with a valid token."
    )

    def __init__(
        self,
        message: str = DEFAULT_MESSAGE,
        *,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize missing token error.

        ``message`` defaults to :attr:`DEFAULT_MESSAGE`.  ``retry_after`` and
        ``details`` are accepted for base-class substitutability (LSP) even
        though this error is not transient — before this hierarchy this subclass took only
        ``message``, so re-raising it through generic base-typed error handling
        raised ``TypeError``.
        """
        super().__init__(message, retry_after=retry_after, details=details)


class NovaOSRateLimitError(NovaOSError):
    """Raised when a rate limit is exceeded."""

    @property
    def is_retryable(self) -> bool:
        return True


class NovaOSValidationError(NovaOSError):
    """Raised when request validation fails."""

    def __init__(
        self,
        message: str,
        *,
        field: str | None = None,
        reason: str | None = None,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize validation error.

        Args:
            message: Human-readable error message
            field: Optional field name that failed validation
            reason: Optional reason for validation failure
            retry_after: Optional retry delay in seconds
            details: Optional additional error details
        """
        super().__init__(message, retry_after=retry_after, details=details)
        self.field = field
        self.reason = reason


class NovaOSAgentNotFoundError(NovaOSError):
    """Raised when an agent is not found."""

    def __init__(
        self,
        message: str,
        *,
        agent_name: str | None = None,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize agent not found error.

        Args:
            message: Human-readable error message
            agent_name: Optional name of the agent that was not found
            retry_after: Optional retry delay in seconds
            details: Optional additional error details
        """
        super().__init__(message, retry_after=retry_after, details=details)
        self.agent_name = agent_name


class NovaOSServerError(NovaOSError):
    """Raised when a server returns an error."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize server error.

        Args:
            message: Human-readable error message
            status_code: Optional HTTP status code
            retry_after: Optional retry delay in seconds
            details: Optional additional error details
        """
        super().__init__(message, retry_after=retry_after, details=details)
        self.status_code = status_code

    @property
    def is_retryable(self) -> bool:
        return self.status_code in _RETRYABLE_STATUS_CODES


class NovaOSDNSError(NovaOSError):
    """Base exception for DNS-related errors."""


class NovaOSDNSConnectionError(NovaOSDNSError):
    """Raised when a connection to the DNS service fails.

    Retryable, matching :class:`NovaOSConnectionError`.  ``dns.py`` raises this
    for connect errors, timeouts *and* DNS-service 5xx responses — all transient
    — so the DNS subtree must not report them as permanent when the platform
    subtree reports the identical conditions as retryable.
    """

    @property
    def is_retryable(self) -> bool:
        return True


class NovaOSDNSRateLimitError(NovaOSDNSError):
    """Raised when the DNS service rate-limits a lookup.

    The DNS mirror of :class:`NovaOSRateLimitError`.  Exists so the 429 branch of
    ``dns.py`` can honor the ``Retry-After`` header it already parses: before
    this class, that branch stored ``retry_after`` on an error whose
    ``is_retryable`` was ``False``, telling the caller "here is when to retry"
    and "do not retry" at the same time.
    """

    @property
    def is_retryable(self) -> bool:
        return True


class NovaOSDNSAgentNotFoundError(NovaOSDNSError):
    """Raised when an agent is not found in the DNS service."""

    def __init__(
        self,
        message: str,
        *,
        agent_name: str | None = None,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize DNS agent not found error.

        Args:
            message: Human-readable error message
            agent_name: Optional name of the agent that was not found
            retry_after: Optional retry delay in seconds
            details: Optional additional error details
        """
        super().__init__(message, retry_after=retry_after, details=details)
        self.agent_name = agent_name


class SealedEnvelopeError(NovaOSError):
    """Base for failures of NovaOS's own secret-delivery encryption.

    A2A gives the request ``metadata`` map no confidentiality of its own, so NovaOS seals each secret
    value into an envelope only the destination pod can open.  This subtree is
    what that layer raises.

    Deliberately **not** an :class:`A2AProtocolError`: the sealed envelope is a
    NovaOS construction, not something the A2A specification defines, and it has
    no spec error code.  Filing it under the protocol subtree would make a
    NovaOS-only failure look like something a remote agent reported.

    Never retryable, and never carries plaintext, key material or ciphertext in
    its message or ``details``.  A message that said *which* part of the
    decryption failed would be a decryption oracle; one that echoed the value
    would defeat the point of encrypting it.
    """


class SealedEnvelopeFormatError(SealedEnvelopeError):
    """Raised when a sealed envelope is malformed, or is not one this SDK speaks.

    Separate from the base class because the two mean different things to
    whoever reads the log.  A format error is a version or wiring problem —
    a worker sealing under a suite this pod does not implement, a truncated
    payload — and is fixed by aligning the two sides.  The base class means the
    envelope was well-formed and still did not open, which is either the wrong
    key or an envelope that does not belong to this delivery.
    """


class RecipientKeyUnavailableError(SealedEnvelopeError):
    """Raised when this pod holds no private key able to open an envelope.

    The ordinary causes are operational rather than adversarial: the key
    rotated and this envelope was sealed to a predecessor that has since aged
    out of its overlap window, the key was revoked after a suspected
    compromise, or the key volume is not mounted at all.  All three are
    reported the same way, because the pod cannot tell them apart — only that
    the key it is being asked for is not one it has.
    """


class SecretNotDeliveredError(NovaOSError):
    """Raised when a skill asks for a secret this request did not carry.

    Deliberately **not** a :class:`SealedEnvelopeError`: nothing failed to open,
    and nothing cryptographic happened at all.  Either the run delivered no
    secret under that name, or the ``user-context/v1`` extension was never
    activated for this request — the latter being what a skill sees when it is
    invoked by a caller that does not seal secrets, such as an agent-to-agent
    call or a hand-written ``curl``.  Filing it under the envelope subtree would
    send whoever reads the log looking for a key problem that does not exist.

    :attr:`details` names what *was* delivered, because the difference between
    "this instance declares no such secret" and "delivery is not working at
    all" is otherwise invisible from the message alone.
    """


class AgentCardSignatureError(NovaOSError):
    """Base for Agent Card JWS signature-verification failures.

    Raised only when a caller opts into verification —
    :func:`novaos_client_sdk.a2a_card_signing.verify_agent_card_signatures` and
    :class:`novaos_client_sdk.discovery.AgentDiscoveryService` never check
    signatures unless asked, so an unsigned card from a trusted source keeps
    working for every existing caller.

    Deliberately **not** an :class:`A2AProtocolError`: the A2A specification does
    not define signature verification as one of its JSON-RPC error codes, so
    filing this under the protocol subtree would make a local trust decision
    look like something a remote agent reported over the wire — the same
    reasoning that keeps :class:`SealedEnvelopeError` out of that subtree too.
    """


class NoAgentCardSignatureError(AgentCardSignatureError):
    """Raised when verification is requested but the card carries no ``signatures``.

    Distinct from :class:`InvalidAgentCardSignatureError` on purpose: an
    unsigned card is not evidence of tampering, only of a publisher that has not
    adopted signing — while a signed-but-invalid card means either tampering or
    a misconfigured verifier. An operator needs to tell those two apart rather
    than see one "verification failed" error for both.
    """


class InvalidAgentCardSignatureError(AgentCardSignatureError):
    """Raised when a card carries one or more signatures and none of them verify.

    Covers a tampered card, a signature from a key the caller's JWKS does not
    have, and one produced with an algorithm outside the caller's allowlist
    alike — all three mean the same thing to a caller: the card's declared
    content is not backed by a valid signature from a key it trusts.
    """


class A2AProtocolError(NovaOSError):
    """Base for errors the **A2A protocol itself** defines.

    Distinct from the platform errors above: an ``A2AProtocolError`` means a
    remote agent replied with one of the A2A specification's own JSON-RPC error
    codes, not that an HTTP call failed.  Subclasses set :attr:`error_code` to
    the spec's numeric code; see :data:`A2A_ERROR_SPEC_VERSION` for the pinned
    release.

    One code in this subtree, :class:`InvalidAgentResponseError` (``-32006``),
    is not only something a peer can send *to* this SDK — it is also something
    this SDK raises *about* a peer's reply, when that reply cannot be a valid
    A2A response at all.  That is still an ``A2AProtocolError`` rather
    than a platform error: the defect is the same either way — a response the
    spec does not allow — whether the peer reported it as an explicit JSON-RPC
    error or simply sent malformed data.

    Standard JSON-RPC transport errors (``-32700`` … ``-32603``) are deliberately
    *not* modelled here — they are not A2A-specific, and the SDK surfaces them as
    :class:`NovaOSServerError` / :class:`NovaOSValidationError` alongside every
    other transport failure.
    """

    #: JSON-RPC error code from the A2A spec.  Each concrete subclass overrides
    #: this with the code it represents, so it can be read off the *class*
    #: (``TaskNotFoundError.error_code``) as well as an instance.  It is
    #: deliberately a plain class attribute rather than a ``ClassVar``: an
    #: instance shadows it when constructed with an explicit ``error_code``, so
    #: an unrecognized code coming off the wire is still carried on the error
    #: instead of being silently dropped to ``None`` by the
    #: :func:`a2a_error_for_code` fallback.
    error_code: int | None = None

    def __init__(
        self,
        message: str,
        *,
        error_code: int | None = None,
        task_id: str | None = None,
        retry_after: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize an A2A protocol error.

        Args:
            message: Human-readable error message
            error_code: The JSON-RPC code actually received.  Defaults to the
                code this class represents, so concrete subclasses need not pass
                it.  Supply it when the code has no dedicated subclass — that is
                the only way :class:`A2AProtocolError` itself can report which
                code it stands for.
            task_id: Optional A2A ``Task.id`` the error refers to
            retry_after: Optional retry delay in seconds
            details: Optional additional error details, e.g. the JSON-RPC
                ``error.data`` payload
        """
        super().__init__(message, retry_after=retry_after, details=details)
        if error_code is not None:
            self.error_code = error_code
        self.task_id = task_id


class TaskNotFoundError(A2AProtocolError):
    """A2A ``-32001``: the requested task id is unknown to the agent."""

    error_code = -32001


class TaskNotCancelableError(A2AProtocolError):
    """A2A ``-32002``: the task is in a terminal state and cannot be canceled."""

    error_code = -32002


class PushNotificationNotSupportedError(A2AProtocolError):
    """A2A ``-32003``: the agent does not support push notification config."""

    error_code = -32003


class UnsupportedOperationError(A2AProtocolError):
    """A2A ``-32004``: the agent does not implement the requested operation."""

    error_code = -32004


class ContentTypeNotSupportedError(A2AProtocolError):
    """A2A ``-32005``: no overlap between requested and supported content types."""

    error_code = -32005


class InvalidAgentResponseError(A2AProtocolError):
    """A2A ``-32006``: the agent returned a response the spec does not allow."""

    error_code = -32006


class ExtendedAgentCardNotConfiguredError(A2AProtocolError):
    """A2A ``-32007``: an extended Agent Card was requested but none is configured."""

    error_code = -32007


class ExtensionSupportRequiredError(A2AProtocolError):
    """A2A ``-32008``: the request needs an extension the caller did not activate."""

    error_code = -32008


class VersionNotSupportedError(A2AProtocolError):
    """A2A ``-32009``: the requested protocol version is not supported."""

    error_code = -32009


#: Every concrete A2A protocol error, keyed by its spec error code.
A2A_ERROR_TYPES: Final[dict[int, type[A2AProtocolError]]] = {
    cls.error_code: cls
    for cls in (
        TaskNotFoundError,
        TaskNotCancelableError,
        PushNotificationNotSupportedError,
        UnsupportedOperationError,
        ContentTypeNotSupportedError,
        InvalidAgentResponseError,
        ExtendedAgentCardNotConfiguredError,
        ExtensionSupportRequiredError,
        VersionNotSupportedError,
    )
    if cls.error_code is not None
}


def a2a_error_for_code(code: int) -> type[A2AProtocolError]:
    """Return the exception class for an A2A JSON-RPC error code.

    Unknown codes fall back to :class:`A2AProtocolError` rather than raising, so
    an agent on a newer spec release degrades to the base type instead of
    breaking the caller.

    Args:
        code: JSON-RPC ``error.code`` from an A2A response

    Returns:
        The matching :class:`A2AProtocolError` subclass, or the base class.
    """
    return A2A_ERROR_TYPES.get(code, A2AProtocolError)


#: ``details`` key carrying the JSON-RPC code a transport-layer error came from.
#:
#: An :class:`A2AProtocolError` carries its code as :attr:`~A2AProtocolError.error_code`,
#: but the standard JSON-RPC codes (``-32700`` … ``-32603``) deliberately land in
#: the *platform* branch of the hierarchy instead — see
#: :func:`error_from_json_rpc` — where there is no such attribute.  Recording the
#: code here means it is never lost just because the error landed on the other
#: side of that split.
JSONRPC_CODE_DETAIL_KEY: Final = "jsonrpc_code"

#: The standard JSON-RPC 2.0 transport error codes, and the platform exception
#: each becomes.
#:
#: These are **not** A2A errors and must not become :class:`A2AProtocolError`.
#: They mean "this was not a well-formed call" (or "the peer broke handling it"),
#: which is a different question from A2A's own nine codes — putting "the JSON was
#: malformed" in the same hierarchy as "that task id is unknown" is exactly the
#: conflation :mod:`novaos_client_sdk.a2a_jsonrpc` was split out to avoid.
#:
#: ``-32603`` is the peer's failure and so is a server error; the other four are
#: all "your request was wrong", which is a validation error on this side.
_JSONRPC_TRANSPORT_ERRORS: Final[dict[int, type[NovaOSError]]] = {
    -32700: NovaOSValidationError,  # Parse error
    -32600: NovaOSValidationError,  # Invalid Request
    -32601: NovaOSValidationError,  # Method not found
    -32602: NovaOSValidationError,  # Invalid params
    -32603: NovaOSServerError,  # Internal error
}


def jsonrpc_error_code(error: BaseException) -> int | None:
    """Return the JSON-RPC code an error came from, whichever layer it landed in.

    A caller that wants to branch on the code — "did this agent simply not
    implement the method?" is the common case — should not have to know which
    branch of the hierarchy the code put the exception in.  This reads
    :attr:`A2AProtocolError.error_code` when it is there and
    :data:`JSONRPC_CODE_DETAIL_KEY` otherwise.

    Args:
        error: Any exception.

    Returns:
        The code, or ``None`` if the error did not come from a JSON-RPC reply.
    """
    code = getattr(error, "error_code", None)
    if isinstance(code, int):
        return code

    details = getattr(error, "details", None)
    if isinstance(details, dict):
        detail_code = details.get(JSONRPC_CODE_DETAIL_KEY)
        if isinstance(detail_code, int) and not isinstance(detail_code, bool):
            return detail_code

    return None


def error_from_json_rpc(
    error: Mapping[str, Any],
    *,
    task_id: str | None = None,
) -> NovaOSError:
    """Build the right exception for a JSON-RPC ``error`` object from *either* layer.

    Prefer this over :func:`a2a_error_from_json_rpc` at a transport boundary,
    where the code that arrives has not been vetted: a peer answers A2A's own
    codes and JSON-RPC's standard ones over the same channel, and only the
    former belong in the :class:`A2AProtocolError` subtree.  Routing everything
    through the A2A constructor would classify "method not found" as an A2A
    protocol failure.

    An **unrecognized** code still goes to :class:`A2AProtocolError`, which
    carries it: a code outside both tables is most likely a newer spec
    revision's, and the base class is where such a thing degrades to.

    Args:
        error: A JSON-RPC ``error`` object.
        task_id: Optional A2A ``Task.id`` the error refers to.

    Returns:
        A platform error for the standard JSON-RPC codes, an
        :class:`A2AProtocolError` otherwise.
    """
    raw_code = error.get("code")
    code = raw_code if isinstance(raw_code, int) and not isinstance(raw_code, bool) else None

    error_cls = _JSONRPC_TRANSPORT_ERRORS.get(code) if code is not None else None
    if error_cls is None:
        return a2a_error_from_json_rpc(error, task_id=task_id)

    message = str(error.get("message") or f"JSON-RPC error {code}")
    details: dict[str, Any] = {JSONRPC_CODE_DETAIL_KEY: code}
    if task_id is not None:
        details["task_id"] = task_id
    data = error.get("data")
    if data is not None:
        details["data"] = data

    if error_cls is NovaOSServerError:
        # `status_code` stays absent: nothing about a `-32603` says what HTTP
        # status carried it, and inventing a 500 would make `is_retryable` a
        # guess rather than a fact.
        return NovaOSServerError(message, details=details)
    return error_cls(message, details=details)


def a2a_error_from_json_rpc(
    error: Mapping[str, Any],
    *,
    task_id: str | None = None,
) -> A2AProtocolError:
    """Build the right :class:`A2AProtocolError` from a JSON-RPC ``error`` object.

    Prefer this over calling :func:`a2a_error_for_code` and constructing the
    class yourself: it always records the code that actually came off the wire,
    including when the code has no dedicated subclass and the lookup falls back
    to :class:`A2AProtocolError`.  Constructing the fallback by hand is the easy
    way to end up with ``error_code is None`` on exactly the errors you most need
    to identify.

    The JSON-RPC ``error.data`` payload — which the spec allows to be any JSON
    value, not just an object — is always stored as ``details["data"]`` so the
    shape of ``details`` does not vary with the payload's type.

    Args:
        error: A JSON-RPC ``error`` object, i.e. ``{"code": ..., "message": ...}``
            with an optional ``"data"``
        task_id: Optional A2A ``Task.id`` the error refers to

    Returns:
        An instance of the subclass matching ``error["code"]``, or of
        :class:`A2AProtocolError` carrying that code when it is unrecognized.
    """
    raw_code = error.get("code")
    code = raw_code if isinstance(raw_code, int) and not isinstance(raw_code, bool) else None
    message = error.get("message") or (
        f"A2A protocol error {code}" if code is not None else "Unknown A2A protocol error"
    )
    data = error.get("data")
    error_cls = a2a_error_for_code(code) if code is not None else A2AProtocolError
    return error_cls(
        str(message),
        error_code=code,
        task_id=task_id,
        details={"data": data} if data is not None else None,
    )
