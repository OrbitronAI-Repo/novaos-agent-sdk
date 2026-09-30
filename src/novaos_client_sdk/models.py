"""Data models for NovaOS Client SDK.

**``Run``, ``RunYield`` and ``YieldType`` were removed during the ACP→A2A
migration.**  They were ACP's result and streaming
envelopes, and ``client.run_sync`` / ``client.run_stream`` were the only things
that produced them; once those methods went, nothing in the SDK could fill either
shape.  Their A2A replacements are :class:`novaos_client_sdk.a2a_task.Task` —
which carries everything ``Run`` did, plus the id the task-addressed operations
need — and the ``StreamResponse`` frames in
:mod:`novaos_client_sdk.a2a_events`.  Per the migration's no-backward-compat
decision they were deleted rather than kept as aliases.

**``Agent`` was removed during the same migration** for the same reason.  It was the shape
``discovery.py`` parsed ACP's ``GET /agents`` listing into — a NovaOS convention
rather than a protocol type — and that call is gone.  Its A2A replacement is
:class:`novaos_client_sdk.a2a_agent_card.AgentCard`, which is the real protocol
document and carries what an agent actually publishes about itself: interfaces,
skills, declared extensions, security schemes.  ``Capability`` **stays**, since
:class:`novaos_client_sdk.dns.AgentDNSRecord` and
:mod:`novaos_client_sdk.capabilities` still produce and match it.

:class:`Message` and :class:`MessagePart` **stay**, and are not ACP leftovers by
the same argument: they are a small convenience shape for constructing input,
:meth:`novaos_client_sdk.client.Client.send_message` converts one to a real A2A
message, and ``message_parser`` / ``message_validator`` still accept them.  An
agent's *output* is never one of these.
"""

from dataclasses import dataclass, field
from typing import Any, Literal

# Type-safe content type values
ContentType = Literal[
    "text/plain",
    "text/markdown",
    "application/json",
    "image/png",
    "image/jpeg",
    "image/gif",
    "video/mp4",
    "audio/mpeg",
]


@dataclass
class Author:
    """Author model for message attribution."""

    name: str
    role: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "role": self.role,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Author":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            role=data.get("role"),
            metadata=data.get("metadata"),
        )


@dataclass
class MessagePart:
    """Simple content-plus-content-type input part, with validation.

    Converted to a real :class:`~novaos_client_sdk.a2a_types.TextPart` — with
    ``content_type`` becoming A2A v1.0's own ``mediaType`` field — when it is
    sent.
    """

    content: str
    content_type: str | ContentType | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate message part."""
        if not isinstance(self.content, str):
            raise TypeError(f"content must be str, got {type(self.content)}")
        if not self.content.strip():
            raise ValueError("content cannot be empty")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "content": self.content,
            "content_type": self.content_type,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessagePart":
        """Create from dictionary."""
        return cls(
            content=data["content"],
            content_type=data.get("content_type"),
            metadata=data.get("metadata"),
        )


@dataclass
class Message:
    """Simple multi-part input message, for callers not building A2A types.

    Accepted by :meth:`novaos_client_sdk.client.Client.send_message` alongside an
    :class:`~novaos_client_sdk.a2a_types.A2AMessage` and a plain string.
    """

    parts: list[MessagePart] = field(default_factory=list)
    author: Author | None = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate message structure."""
        if not self.parts:
            raise ValueError("Message must contain at least one part")

    def validate(self) -> bool:
        """Validate message structure and content.

        Checks that:
        - Message has at least one part
        - All parts have non-empty content

        Returns:
            True if message is valid, False otherwise
        """
        if not self.parts:
            return False
        for part in self.parts:
            if not part.content or not part.content.strip():
                return False
        return True

    def to_dict(self) -> dict[str, Any]:
        """Convert message to dictionary representation."""
        return {
            "parts": [part.to_dict() for part in self.parts],
            "author": self.author.to_dict() if self.author else None,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """Create Message from dictionary."""
        return cls(
            parts=[MessagePart.from_dict(p) for p in data.get("parts", [])],
            author=Author.from_dict(data["author"]) if data.get("author") else None,
            metadata=data.get("metadata"),
        )


@dataclass
class Capability:
    """Agent capability model."""

    name: str
    description: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "metadata": self.metadata or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Capability":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            description=data.get("description"),
            metadata=data.get("metadata"),
        )


# Type alias for metadata
Metadata = dict[str, Any]
