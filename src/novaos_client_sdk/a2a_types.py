"""A2A-native message types for the NovaOS client SDK.

These types implement the ``Message`` / ``Part`` family of the **A2A protocol
specification v1.0.1** (`a2aproject/A2A`, tag ``v1.0.1``).  The migration
target is pinned to that specific release — *not* ``latest`` / ``@main``, which is a moving
target that could silently invalidate these shapes.  ``A2A_SPEC_VERSION`` below
records the pin in code so a future spec bump is a deliberate edit, not drift.

**The 1.0.0 → 1.0.1 bump, evidence.**  ``git diff v1.0.0 v1.0.1`` against
`a2aproject/A2A` (released 2026-05-26 per its ``CHANGELOG.md``) touches
``specification/a2a.proto`` in exactly 37 lines, all of them comments on the
``tenant`` field of ``AgentInterface`` and of the nine request messages that
carry one — no field is added, removed, renumbered or retyped, so there is no
wire-format break and this bump needed no migration. Three things follow from
the fuller diff, none of which change what this SDK does on the wire:

1. ``AgentInterface.tenant``'s comment was rewritten from a description into a
   MUST: when a card interface declares a tenant, clients MUST send it back on
   every request to that interface. NovaOS already does this — :class:`Client`
   resolves ``tenant`` from the card once at construction and sends it on every
   call (see ``client.py``'s ``_tenant_from_card``/``_apply_tenant``) —
   so this SDK was already conformant with 1.0.1's tightened text before this
   bump landed.
2. The HTTP binding's docs now prefer ``application/a2a+json`` over
   ``application/json`` (CHANGELOG: "prefer application/a2a+json in HTTP
   binding", #1753). That is the HTTP+JSON binding, which this SDK does not
   implement; the JSON-RPC binding this SDK speaks keeps
   ``Content-Type: application/json`` in both spec releases, so nothing here
   changes.
3. A separate fix ("recent transcoding-related error changes", #1627) loosens
   the JSON-RPC ``error.data`` contract — 1.0.0's text had, by the fixing PR's
   own account, mistakenly required a strict ``google.rpc.ErrorInfo`` shape in
   JSON-RPC error data; 1.0.1 reverts that to "any object with an ``@type``
   key". This SDK never depended on the stricter reading:
   :func:`~novaos_client_sdk.exceptions.a2a_error_from_json_rpc` has always
   stored ``error.data`` verbatim as ``details["data"]`` without assuming its
   shape, so it was already compatible with the relaxed 1.0.1 contract. The
   same commit also changes several HTTP/gRPC status-code mappings in the
   error table (e.g. ``TaskNotCancelableError`` 409→400); those bindings are
   likewise not implemented here.

The net effect: this bump is a version-string update plus documentation, not a
behavior change. The next person bumping this pin should re-run the same
``git diff v<old> v<new> -- specification/a2a.proto`` check and re-read the
release's ``CHANGELOG.md`` in full — not just the proto diff — since 1.0.1's
two most consequential fixes (the error-data contract change and the tenant
MUST) were both in prose outside the proto file, and would have been missed
by a proto-only diff.

**Why NovaOS-owned dataclasses rather than a vendor SDK's concrete types**
(DIP requirement, ticket-plan item 18): the SDK's own message code must not
depend directly on ``agentstack_sdk``'s (or any other vendor's) concrete types.
``message_builder.py`` was the one place in the SDK that violated this — it
returned ``acp_sdk.models.Message`` by design.  Building on these internal types
instead means a vendor client library, if one is ever adopted, is an
implementation detail behind
``client.py``, not a type that leaks through the SDK's public message API.

**Wire format (corrected by F18).**  A2A v1.0 defines its data structures in
`specification/a2a.proto`, and every JSON binding — JSON-RPC included — carries
the **proto3 JSON mapping** of those messages: ``lowerCamelCase`` field names,
enums as their full value names, and a ``oneof`` rendered as whichever member is
set.  These types originally emitted A2A **v0.3**'s hand-rolled JSON instead
(``{"kind": "text", ...}`` parts, ``"user"``/``"agent"`` roles, a ``kind`` field
on ``Message``).  That shape is rejected outright by a v1.0 peer — the reference
implementation answers ``-32602 ... has no field named "kind"`` — so it was
corrected here rather than left to fail at integration time.  Concretely, in
v1.0:

- ``Part`` has **no** ``kind`` discriminator.  It is a ``oneof`` over ``text``,
  ``raw``, ``url`` and ``data``, with ``filename``, ``mediaType`` and
  ``metadata`` as siblings available to every variant.  A part is identified by
  *which content field is present*.
- ``Message`` has no ``kind`` field either.
- ``Role`` serialises as ``ROLE_USER`` / ``ROLE_AGENT``.
- Text parts carry their media type in a real ``mediaType`` field, so NovaOS no
  longer needs a metadata key to express one.

**Version negotiation.**  A2A v1.0 §7 requires every client to send an
``A2A-Version`` header; an absent header is read as ``0.3``, and a v1.0-only
agent answers ``VersionNotSupportedError``.  ``A2A_VERSION_HEADER`` and
``A2A_PROTOCOL_VERSION`` below exist so callers send it consistently, and
:func:`unsupported_version_reason` is the server half — it is what
:meth:`novaos_client_sdk.agent_builder.AgentBuilder.handle_a2a_request` applies
so that a NovaOS pod and a vendor-SDK pod answer an identical request
identically.

Spec mapping notes (ADR F2):

- ACP ``MessagePart(content=..., content_type=...)`` has no single A2A
  equivalent.  A2A splits parts by *content*: ``text`` for human-readable text,
  ``data`` for structured JSON, ``raw``/``url`` for binary or referenced files.
- ACP's ``session_id`` maps to A2A's ``contextId``.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar, Final
from uuid import uuid4

# The A2A specification release these types are written against.  Pinned per F17;
# bumping it is a deliberate follow-up decision, not implicit drift from "latest".
A2A_SPEC_VERSION: Final = "1.0.1"

# The Major.Minor form the protocol itself uses for version negotiation.  A2A
# v1.0 §14.2.1 defines the header value as Major.Minor, so it is *not* the same
# string as the release above and the two must not be conflated.
A2A_PROTOCOL_VERSION: Final = "1.0"

# Header every A2A client must send (v1.0 §7).  An absent header means "0.3" to
# a compliant agent, which is why omitting it makes a v1.0 peer reject the call
# rather than default to the current version.
A2A_VERSION_HEADER: Final = "A2A-Version"

# The version a peer must assume when the header is absent or empty (v1.0 §7).
# This is not a NovaOS default: the spec makes omission an explicit request for
# 0.3, so a v1.0-only agent has to reject it rather than read it as "unset".
A2A_ABSENT_VERSION: Final = "0.3"

# Media type A2A assumes for a text part when none is declared.
DEFAULT_TEXT_MEDIA_TYPE: Final = "text/plain"


def requested_protocol_version(headers: Any) -> str:
    """Return the A2A protocol version a request asked for.

    Per v1.0 §7 an absent *or empty* ``A2A-Version`` header is not "unspecified"
    — it means :data:`A2A_ABSENT_VERSION`.  That is the whole reason a v1.0-only
    agent must answer ``-32009`` to a header-less call instead of assuming the
    current version; see :func:`unsupported_version_reason`.

    Header lookup is case-insensitive, per RFC 9110 §5.1.  The reference
    implementation only probes the canonical and all-lowercase spellings, so it
    reads ``A2A-VERSION`` as absent where this returns its value.  The
    difference is unreachable behind a real HTTP server (ASGI lower-cases header
    names before a handler sees them) and this side of it is the RFC-correct
    one, so it is left as a deliberate superset rather than bug-compatibility.

    Args:
        headers: Any mapping with ``.items()``, or ``None``.

    Returns:
        The requested version string, or :data:`A2A_ABSENT_VERSION`.
    """
    if not hasattr(headers, "items"):
        return A2A_ABSENT_VERSION

    wanted = A2A_VERSION_HEADER.lower()
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == wanted and value:
            return str(value)

    return A2A_ABSENT_VERSION


def unsupported_version_reason(headers: Any) -> str | None:
    """Return why this handler cannot serve ``headers``' version, else ``None``.

    Returns a message rather than raising, so the caller decides how to frame it
    — a JSON-RPC ``error`` object here, something else on another binding.  The
    wording is copied verbatim from the reference implementation
    (``a2a.utils.version_validator``) so that a caller matching on the string
    cannot tell a NovaOS pod from a vendor-SDK one; ``tests/`` pins that.

    **Acceptance is major-version only**, again matching the reference
    implementation: ``1.0``, ``1.0.0`` and ``1.4`` are all served, ``0.3`` and
    ``2.0`` are not.  The vendor's own docstring claims it compares major *and*
    minor while its code compares major alone; the code is what pods actually
    run, so it is what this matches.  Narrowing to an exact ``1.0`` would make a
    NovaOS pod reject a request a scaffolded pod accepts, which is precisely the
    two-kinds-of-pod divergence this module exists to remove.

    Args:
        headers: Any mapping with ``.items()``, or ``None``.

    Returns:
        A human-readable reason, or ``None`` when the version is servable.
    """
    requested = requested_protocol_version(headers)

    if requested == A2A_PROTOCOL_VERSION or _major_version(requested) == _major_version(
        A2A_PROTOCOL_VERSION
    ):
        return None

    return (
        f"A2A version '{requested}' is not supported by this handler. "
        f"Expected version '{A2A_PROTOCOL_VERSION}'."
    )


def _major_version(version: str) -> int | None:
    """Return the major component of ``version``, or ``None`` if unparseable.

    The reference implementation uses ``packaging.version.Version`` for this.
    NovaOS will not take a runtime dependency on ``packaging`` just to read one
    integer, so the tolerances that actually matter are reproduced here: leading
    and trailing whitespace, a single optional ``v`` prefix, and any number of
    components after the major.  A value that is not a version at all yields
    ``None`` and is therefore rejected, which is what the vendor does too.

    **Digits must be ASCII**, and that is load-bearing twice over.  ``str.isdigit``
    is true for characters ``int`` refuses (``"²"``), so gating on it alone
    raises ``ValueError`` out of a function whose contract is to return a reason
    — and ``unsupported_version_reason`` is called outside the handler's
    ``try``, so that escapes as an unhandled exception instead of a ``-32009``.
    Widening to ``str.isdecimal`` fixes the crash and introduces a worse bug:
    ``"١"`` is decimal and ``int`` reads it as ``1``, so NovaOS would serve a
    version the reference implementation rejects — PEP 440's grammar is ASCII
    ``[0-9]`` only. Accepting what a scaffold pod refuses is the exact
    divergence this module exists to prevent, so the check is ASCII-and-digit.
    """
    stripped = version.strip()
    # Exactly one `v`, the way PEP 440 allows it — chaining removals would let
    # `vv1.0` through, which the reference implementation rejects.
    unprefixed = stripped[1:] if stripped[:1] in ("v", "V") else stripped
    candidate = unprefixed.split(".", 1)[0]

    return int(candidate) if candidate.isascii() and candidate.isdigit() else None


def as_dict(value: Any) -> dict[str, Any]:
    """Return ``value`` when it is a dict, else an empty dict.

    Every nested envelope in an A2A payload is caller-controlled JSON, so any of
    them can arrive as a list, a string or a number.  ``or {}`` is not a
    sufficient guard: it catches ``None`` and ``{}``, but a *truthy* non-object
    sails through and the next ``.get()`` raises ``AttributeError``.
    """
    return value if isinstance(value, dict) else {}


def as_str(value: Any) -> str:
    """Return ``value`` when it is a string, else the empty string.

    Used for the identifier fields.  These are less a crash risk than a *wire*
    risk: proto3 JSON rejects a non-string where the schema says string, so an id
    accepted as a number and echoed back unchanged yields a payload no conformant
    peer can parse.  Empty is already how every caller treats "absent".
    """
    return value if isinstance(value, str) else ""


def as_str_list(value: Any) -> list[str] | None:
    """Return the string members of ``value``, or ``None`` when there are none.

    ``None`` rather than ``[]`` because the fields using this are omitted from the
    wire shape when empty, and an explicit empty list is not the same as absent.
    """
    if not isinstance(value, list):
        return None
    items = [v for v in value if isinstance(v, str)]
    return items or None


def as_int(value: Any) -> int | None:
    """Return ``value`` as an ``int``, or ``None`` when it is not a whole number.

    Wider than a bare ``isinstance`` check because proto3 JSON is: a 32-bit
    integer is rendered as a JSON number, a 64-bit one as a *string*, and a
    number that has been through a ``google.protobuf.Struct`` comes back as a
    double (``25.0``).  All three spell the same value, so all three are read.

    ``bool`` is rejected ahead of ``int`` for the reason
    :func:`novaos_client_sdk.a2a_artifact._coerce_revision` gives: ``True`` is an
    ``int`` in Python, and "page size True" is not a page size.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def as_bool(value: Any) -> bool:
    """Return ``value`` when it is a real ``bool``, else ``False``.

    Deliberately *not* Python truthiness: the fields using this are proto3
    booleans whose absence and whose ``false`` are indistinguishable on the
    wire, so anything that is not an actual ``true`` means the same thing.
    Coercing ``"false"`` — a string a hand-rolled server might emit — to ``True``
    by truthiness would invert the caller's meaning.
    """
    return value is True


class Role(StrEnum):
    """A2A message role (spec v1.0.1, ``Message.role``).

    The values are the proto enum's value names, which is what proto3 JSON
    emits.  v0.3's bare ``"user"``/``"agent"`` are accepted on the read side
    only — see :meth:`A2AMessage.from_dict`.
    """

    USER = "ROLE_USER"
    AGENT = "ROLE_AGENT"


# v0.3 spelling -> v1.0 spelling, for tolerant reads of older payloads.
_LEGACY_ROLES: Final[dict[str, Role]] = {"user": Role.USER, "agent": Role.AGENT}


class PartKind(StrEnum):
    """Which content variant of A2A's ``Part`` ``oneof`` a part carries.

    v1.0 does not put this on the wire — a part is identified by which content
    field is set.  It is kept as an internal label because dispatching on a name
    reads better than dispatching on ``"text" in data``, and because callers
    already branch on it.
    """

    TEXT = "text"
    FILE = "file"
    DATA = "data"


@dataclass
class FileWithBytes:
    """Inline file content, base64-encoded.

    Serialises into the enclosing ``Part`` (v1.0 has no nested file object):
    ``bytes`` becomes the part's ``raw``, ``name`` its ``filename``, and
    ``mime_type`` its ``mediaType``.
    """

    bytes: str
    mime_type: str | None = None
    name: str | None = None


@dataclass
class FileWithUri:
    """File content referenced by URI.

    Serialises into the enclosing ``Part``: ``uri`` becomes the part's ``url``,
    ``name`` its ``filename``, and ``mime_type`` its ``mediaType``.
    """

    uri: str
    mime_type: str | None = None
    name: str | None = None


FileContent = FileWithBytes | FileWithUri


def _with_common_fields(
    out: dict[str, Any],
    *,
    media_type: str | None = None,
    filename: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach the fields every A2A ``Part`` variant shares."""
    if media_type:
        out["mediaType"] = media_type
    if filename:
        out["filename"] = filename
    if metadata:
        out["metadata"] = metadata
    return out


@dataclass
class TextPart:
    """A2A ``TextPart`` — human-readable text.

    ``media_type`` maps to the part's own ``mediaType`` field.  (Under v0.3 no
    such field existed and NovaOS recorded the media type in ``metadata``; v1.0
    has a real one, so the workaround is gone — see ``message_builder``.)
    """

    text: str
    media_type: str | None = None
    metadata: dict[str, Any] | None = None

    kind: ClassVar[str] = PartKind.TEXT

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape."""
        return _with_common_fields(
            {"text": self.text},
            media_type=self.media_type,
            metadata=self.metadata,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TextPart":
        """Create from the A2A JSON wire shape.

        Coerced, not trusted — for the reasons in :meth:`A2AMessage.from_dict`.
        A numeric ``text`` is the sharpest case: it survives construction and then
        raises from :attr:`A2AMessage.text`, which joins the text parts, so the
        failure surfaces during dispatch rather than at the boundary.
        """
        return cls(
            text=as_str(data.get("text")),
            media_type=as_str(data.get("mediaType", data.get("media_type"))) or None,
            metadata=as_dict(data.get("metadata")) or None,
        )


@dataclass
class DataPart:
    """A2A ``DataPart`` — structured JSON payload.

    This is where NovaOS's structured metadata (HITL, error, progress) lives
    under A2A.  Under ACP it was a second ``MessagePart`` carrying a JSON
    *string* with the vendor content-type ``application/vnd.novaos.metadata+json``;
    A2A models structured data as a first-class part variant, so the payload is
    a real object rather than an embedded string.
    """

    data: dict[str, Any]
    media_type: str | None = None
    metadata: dict[str, Any] | None = None

    kind: ClassVar[str] = PartKind.DATA

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape."""
        return _with_common_fields(
            {"data": self.data},
            media_type=self.media_type,
            metadata=self.metadata,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataPart":
        """Create from the A2A JSON wire shape."""
        return cls(
            data=as_dict(data.get("data")),
            media_type=as_str(data.get("mediaType", data.get("media_type"))) or None,
            metadata=as_dict(data.get("metadata")) or None,
        )


@dataclass
class FilePart:
    """A2A file part — binary or URI-referenced content.

    v1.0 has no nested ``file`` object: the content is the part's own ``raw`` or
    ``url`` field, with ``filename`` and ``mediaType`` alongside.  The
    :class:`FileWithBytes` / :class:`FileWithUri` wrappers are kept because they
    make which-variant-is-this explicit in Python; they are flattened on the way
    out and reconstructed on the way in.
    """

    file: FileContent
    metadata: dict[str, Any] | None = None

    kind: ClassVar[str] = PartKind.FILE

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape."""
        content: dict[str, Any]
        if isinstance(self.file, FileWithUri):
            content = {"url": self.file.uri}
        else:
            content = {"raw": self.file.bytes}

        return _with_common_fields(
            content,
            media_type=self.file.mime_type,
            filename=self.file.name,
            metadata=self.metadata,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FilePart":
        """Create from the A2A JSON wire shape."""
        mime_type = as_str(data.get("mediaType", data.get("media_type"))) or None
        name = as_str(data.get("filename")) or None

        file: FileContent
        if "url" in data:
            file = FileWithUri(uri=as_str(data["url"]), mime_type=mime_type, name=name)
        else:
            file = FileWithBytes(bytes=as_str(data.get("raw")), mime_type=mime_type, name=name)

        return cls(file=file, metadata=as_dict(data.get("metadata")) or None)


Part = TextPart | FilePart | DataPart


def part_from_dict(data: Any) -> Part:
    """Deserialize a single A2A ``Part`` by which content field it carries.

    v1.0's ``Part`` is a ``oneof``, so the variant is identified by presence
    rather than by a discriminator field.

    Args:
        data: An A2A part JSON object.  Typed ``Any`` rather than ``dict`` on
            purpose: this is public API over untrusted wire data, and a
            signature promising ``dict`` would invite callers to skip the check
            this function already performs.

    Returns:
        The matching part dataclass.  A part carrying none of the known content
        fields falls back to :class:`TextPart` so a forward-compatible sender (a
        future spec revision adding a variant) degrades to something renderable
        rather than raising.
    """
    if not isinstance(data, dict):
        # Public API, so a caller may hand this anything; A2AMessage.from_dict
        # filters non-objects out of `parts` before reaching here, but a direct
        # caller does not.
        return TextPart(text="")
    if "data" in data:
        return DataPart.from_dict(data)
    if "raw" in data or "url" in data:
        return FilePart.from_dict(data)
    return TextPart.from_dict(data)


@dataclass
class A2AMessage:
    """A2A ``Message`` (spec v1.0.1).

    Every factory method on :class:`~novaos_client_sdk.message_builder.MessageBuilder`
    returns one of these.  ``message_id`` is generated when not supplied, since
    the spec requires it on every message.
    """

    parts: list[Part] = field(default_factory=list)
    role: Role = Role.AGENT
    message_id: str = field(default_factory=lambda: str(uuid4()))
    context_id: str | None = None
    task_id: str | None = None
    reference_task_ids: list[str] | None = None
    extensions: list[str] | None = None
    metadata: dict[str, Any] | None = None

    @property
    def text(self) -> str:
        """Concatenate every ``TextPart``'s text — the human-readable content."""
        return "".join(p.text for p in self.parts if isinstance(p, TextPart))

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase)."""
        out: dict[str, Any] = {
            "messageId": self.message_id,
            "role": str(self.role),
            "parts": [p.to_dict() for p in self.parts],
        }
        if self.context_id is not None:
            out["contextId"] = self.context_id
        if self.task_id is not None:
            out["taskId"] = self.task_id
        if self.reference_task_ids:
            out["referenceTaskIds"] = self.reference_task_ids
        if self.extensions:
            out["extensions"] = self.extensions
        if self.metadata:
            out["metadata"] = self.metadata
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "A2AMessage":
        """Create from the A2A JSON wire shape.

        Accepts the JSON-RPC binding's ``camelCase`` keys, the gRPC binding's
        ``snake_case`` equivalents, and v0.3's lowercase role spellings — reads
        stay tolerant so a peer that has not caught up yet is still legible.
        """
        raw_role = data.get("role", Role.AGENT)
        try:
            role = Role(raw_role)
        except ValueError:
            role = _LEGACY_ROLES.get(str(raw_role).lower(), Role.AGENT)

        # Every field below is coerced, not trusted.  Tolerant reads are the point
        # of this method, but tolerance that yields an object ``to_dict`` cannot
        # re-serialize is a trap rather than a kindness: a non-string id read here
        # used to travel straight into a Task's history and make the whole response
        # unparseable by a conformant peer.
        message_id = as_str(data.get("messageId", data.get("message_id"))) or str(uuid4())
        return cls(
            parts=[part_from_dict(p) for p in data.get("parts", []) if isinstance(p, dict)],
            role=role,
            message_id=message_id,
            context_id=as_str(data.get("contextId", data.get("context_id"))) or None,
            task_id=as_str(data.get("taskId", data.get("task_id"))) or None,
            reference_task_ids=as_str_list(
                data.get("referenceTaskIds", data.get("reference_task_ids"))
            ),
            extensions=as_str_list(data.get("extensions")),
            metadata=as_dict(data.get("metadata")) or None,
        )


def is_a2a_shaped(message: Any) -> bool:
    """Does this object present as an A2A ``Message``?

    v0.3 put a ``kind: "message"`` discriminator on the wire and NovaOS used it
    to tell A2A messages apart from ACP ones.  v1.0's ``Message`` has no such
    field (F18), so the test is structural: an :class:`A2AMessage`, or anything
    carrying the envelope the spec makes REQUIRED — ``messageId``, ``role`` and
    ``parts``.

    Duck-typing rather than ``isinstance`` alone is deliberate: call sites
    receive messages built elsewhere (a decoded wire payload, a test double)
    that are A2A-shaped without being this exact class.
    """
    if isinstance(message, A2AMessage):
        return True

    return all(getattr(message, attr, None) is not None for attr in ("message_id", "role", "parts"))


__all__ = [
    "A2AMessage",
    "A2A_ABSENT_VERSION",
    "A2A_PROTOCOL_VERSION",
    "A2A_SPEC_VERSION",
    "A2A_VERSION_HEADER",
    "DEFAULT_TEXT_MEDIA_TYPE",
    "DataPart",
    "FileContent",
    "FilePart",
    "FileWithBytes",
    "FileWithUri",
    "Part",
    "PartKind",
    "Role",
    "TextPart",
    "as_bool",
    "as_dict",
    "as_int",
    "as_str",
    "as_str_list",
    "is_a2a_shaped",
    "part_from_dict",
    "requested_protocol_version",
    "unsupported_version_reason",
]
