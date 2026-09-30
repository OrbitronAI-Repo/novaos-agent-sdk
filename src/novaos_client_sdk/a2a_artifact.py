"""A2A ``Artifact`` types for the NovaOS client SDK.

Third module of the A2A type family, after :mod:`novaos_client_sdk.a2a_types`
(the ``Message`` / ``Part`` family) and
:mod:`novaos_client_sdk.a2a_task` (the ``Task`` family).  An
**Artifact** is a task's *output*: the typed, named, versioned thing an agent
produced, as distinct from the message it sent explaining itself.

**Why this exists.**  ACP had no equivalent concept at
all.  A run's result was ``Run.output``, a flat ``list[Message]`` — unstructured
by construction: nothing named an output, nothing said what type it was beyond a
content-type string on each part, and a second version of the same output was
just another message appended to the list, indistinguishable from a different
output.  A2A makes outputs first-class, and this module is where NovaOS models
them.  ``Run`` (``models.py``) and ``Task`` (``a2a_task.py``) both carry
``artifacts`` now; neither carries a flat message list.

**Typed, named, versioned** — how each half is actually delivered:

- *Typed*: :attr:`Artifact.parts` is the same ``Part`` union messages use, so
  structured output is a real :class:`~novaos_client_sdk.a2a_types.DataPart`
  holding a JSON object, a file is a
  :class:`~novaos_client_sdk.a2a_types.FilePart`, and prose is a
  :class:`~novaos_client_sdk.a2a_types.TextPart` with its own ``mediaType``.
  Under ACP a structured result was a JSON *string* inside a text part.
- *Named*: :attr:`Artifact.artifact_id` is the identity (unique within a task,
  generated when not supplied — the spec marks it REQUIRED), with
  :attr:`Artifact.name` and :attr:`Artifact.description` as the human-readable
  labels.
- *Versioned*: :attr:`Artifact.revision`, plus :meth:`Artifact.revised` to
  advance it while keeping the identity.  See below for why the revision travels
  in ``metadata`` rather than as a field of its own.

**Wire format.**  A2A v1.0 defines ``Artifact`` in ``specification/a2a.proto``
and every JSON binding carries the proto3 JSON mapping of it, so the keys are
``artifactId``, ``name``, ``description``, ``parts``, ``metadata`` and
``extensions``.  That list is closed: a conformant peer parses with
unknown-field rejection on and answers ``-32602 ... has no field named "x"`` for
anything else — the exact failure F18 found when NovaOS emitted A2A v0.3's
hand-rolled JSON at a v1.0 peer.

**That closed list is why ``revision`` is not a top-level key.**  A2A has no
version field on ``Artifact``; the spec expresses versioning through
``TaskArtifactUpdateEvent``, where a later chunk carrying the same
``artifactId`` updates or appends to the earlier one.  So a NovaOS revision
counter cannot be its own field without producing a payload no conformant peer
will parse.  It is carried instead under :data:`ARTIFACT_REVISION_KEY` inside
``metadata``, which is a ``google.protobuf.Struct`` and is the spec's own
extension point for exactly this.  :meth:`Artifact.from_dict` lifts the key back
out into :attr:`Artifact.revision`, so the counter is a typed attribute in
Python and a metadata entry on the wire, and ``metadata`` never carries a
duplicate of it.

**No ``TaskArtifactUpdateEvent`` here.**  It is the streaming frame for
incremental artifact delivery, and this SDK has no streaming surface yet —
``sendStreamingMessage``/``subscribeToTask`` come with the transport work that
follows.  Modelling the
frame before the transport that carries it would be a stub that later work has to migrate
off, which is the mistake :class:`~novaos_client_sdk.a2a_task.Task` avoided by
leaving ``artifacts`` to this module.  :meth:`Artifact.revised` already carries
the ``append`` semantics that frame needs, so the transport work adds an envelope, not a model.

Design notes: the SDK gap this fills, the spec-version
pin, and the wire format are documented in the migration's architecture decision record.
"""

from dataclasses import dataclass, field, replace
from typing import Any, Final
from uuid import uuid4

from .a2a_types import Part, TextPart, as_dict, as_str, as_str_list, part_from_dict

__all__ = [
    "ARTIFACT_REVISION_KEY",
    "DEFAULT_ARTIFACT_REVISION",
    "Artifact",
]

#: ``metadata`` key carrying an artifact's NovaOS revision counter.  Namespaced
#: the same way as ``message_builder.NOVAOS_METADATA_PART_KEY`` so every NovaOS
#: key on an A2A object is recognisable as one and cannot collide with a key a
#: caller or another extension puts in the same ``Struct``.
ARTIFACT_REVISION_KEY: Final = "novaos.platform/artifact-revision"

#: The revision a freshly produced artifact carries, and what an artifact whose
#: metadata says nothing about revisions is read as.  Absent therefore means
#: "first revision", not "unversioned" — there is no third state to represent.
DEFAULT_ARTIFACT_REVISION: Final = 1


def _coerce_revision(value: Any) -> int:
    """Read a revision counter off the wire, falling back to the default.

    Coerced rather than trusted, for the reason
    :meth:`novaos_client_sdk.a2a_types.A2AMessage.from_dict` gives: ``metadata``
    is caller-controlled JSON, so this can be a string, a list, or ``-3``.

    ``float`` is accepted because it is the *normal* case rather than an
    exotic one: ``metadata`` is a ``google.protobuf.Struct``, whose only numeric
    type is a double, so a revision that has been through real protobuf comes
    back as ``2.0``.  ``bool`` is rejected ahead of ``int`` because ``True`` is
    an ``int`` in Python and "revision True" is not a revision.
    """
    if isinstance(value, bool):
        return DEFAULT_ARTIFACT_REVISION
    if isinstance(value, int) and value >= 1:
        return value
    if isinstance(value, float) and value.is_integer() and value >= 1:
        return int(value)
    return DEFAULT_ARTIFACT_REVISION


@dataclass
class Artifact:
    """A2A ``Artifact`` (spec v1.0.1) — one typed, named, versioned task output.

    ``parts`` comes first to mirror
    :class:`~novaos_client_sdk.a2a_types.A2AMessage`, whose shape this otherwise
    parallels; the remaining field order follows the proto.  The spec marks
    ``parts`` REQUIRED ("must contain at least one part") but construction does
    **not** raise on an empty one: :meth:`from_dict` reads untrusted wire data,
    and a validating constructor would turn a peer's malformed payload into an
    unhandled exception at the boundary — the same failure mode fixed in
    the message types.  Emitters in this SDK never build an empty one.
    """

    parts: list[Part] = field(default_factory=list)
    artifact_id: str = field(default_factory=lambda: str(uuid4()))
    name: str | None = None
    description: str | None = None
    metadata: dict[str, Any] | None = None
    extensions: list[str] | None = None
    revision: int = DEFAULT_ARTIFACT_REVISION

    @property
    def text(self) -> str:
        """Concatenate every ``TextPart``'s text — the human-readable output.

        The artifact-side counterpart of
        :attr:`novaos_client_sdk.a2a_types.A2AMessage.text`, so a caller reads an
        output the same way whether it arrived as a message or an artifact.
        """
        return "".join(p.text for p in self.parts if isinstance(p, TextPart))

    def revised(self, parts: list[Part], *, append: bool = False) -> "Artifact":
        """Return the next revision of this artifact, keeping its identity.

        Identity is what makes a revision a revision: ``artifact_id``, ``name``
        and ``description`` are carried over unchanged, so a consumer that keyed
        on the id sees an update rather than a second, unrelated output — which
        is precisely what ACP's flat ``Run.output`` list could not express.

        Args:
            parts: The new content.
            append: When ``True``, ``parts`` extends the existing content rather
                than replacing it — the same distinction A2A's
                ``TaskArtifactUpdateEvent.append`` draws between a chunk of a
                streamed artifact and a wholesale update of it.

        Returns:
            A new :class:`Artifact`; the receiver is left unmodified.
        """
        return replace(
            self,
            parts=[*self.parts, *parts] if append else list(parts),
            revision=self.revision + 1,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to the A2A v1.0 JSON wire shape (proto3 JSON, camelCase).

        Optional fields are omitted when empty rather than sent as ``null``,
        matching the rest of the A2A types: proto3 JSON reads an absent field
        and an explicit default identically, and omitting keeps the payload
        readable.  :attr:`revision` is folded into ``metadata`` under
        :data:`ARTIFACT_REVISION_KEY` — and only when it is past the default, so
        an artifact nobody has revised carries no metadata it did not ask for.
        """
        out: dict[str, Any] = {
            "artifactId": self.artifact_id,
            "parts": [p.to_dict() for p in self.parts],
        }
        if self.name:
            out["name"] = self.name
        if self.description:
            out["description"] = self.description

        metadata = dict(self.metadata) if self.metadata else {}
        if self.revision != DEFAULT_ARTIFACT_REVISION:
            metadata[ARTIFACT_REVISION_KEY] = self.revision
        if metadata:
            out["metadata"] = metadata

        if self.extensions:
            out["extensions"] = self.extensions
        return out

    @classmethod
    def from_dict(cls, data: Any) -> "Artifact":
        """Create from the A2A JSON wire shape.

        Accepts the JSON-RPC binding's ``camelCase`` keys and the gRPC binding's
        ``snake_case`` equivalents, and coerces every field it reads for the
        reason :meth:`novaos_client_sdk.a2a_types.A2AMessage.from_dict` records:
        tolerance that yields an object :meth:`to_dict` cannot re-serialize is a
        trap, not a kindness.  A missing ``artifactId`` is replaced by a
        generated one because the spec makes it REQUIRED, so re-emitting the
        artifact without one would produce a payload no peer can parse.

        Args:
            data: An A2A artifact JSON object.  Typed ``Any`` because this is
                public API over untrusted wire data.
        """
        obj = as_dict(data)
        metadata = dict(as_dict(obj.get("metadata")))
        # Popped, not read: the revision lives in `revision` on this side, and
        # leaving a copy behind in `metadata` would let the two disagree after a
        # `revised()` call and emit a stale duplicate on the way back out.
        revision = _coerce_revision(metadata.pop(ARTIFACT_REVISION_KEY, None))

        parts = obj.get("parts")
        return cls(
            # Non-object members are dropped rather than degraded to an empty
            # part, matching `A2AMessage.from_dict`: a part that was never an
            # object carries nothing to render.
            parts=(
                [part_from_dict(p) for p in parts if isinstance(p, dict)]
                if isinstance(parts, list)
                else []
            ),
            artifact_id=as_str(obj.get("artifactId", obj.get("artifact_id"))) or str(uuid4()),
            name=as_str(obj.get("name")) or None,
            description=as_str(obj.get("description")) or None,
            metadata=metadata or None,
            extensions=as_str_list(obj.get("extensions")),
            revision=revision,
        )
