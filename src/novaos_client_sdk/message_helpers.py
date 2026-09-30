"""Convenience functions for working with standardized messages.

These are simple wrapper functions around MessageBuilder, MessageParser, and MessageValidator
for easier usage.
"""

from typing import Any

from novaos_client_sdk.a2a_types import is_a2a_shaped
from novaos_client_sdk.message_builder import MessageBuilder
from novaos_client_sdk.message_parser import MessageParser, _is_novaos_metadata_part
from novaos_client_sdk.messages import ErrorMessage, HITLMessage, HITLResponse, ProgressMessage

# Accept A2A messages (a2a_types.A2AMessage — what MessageBuilder now produces),
# ACP-shaped messages, and novaos_client_sdk Message (dataclass).  The read side
# stays format-tolerant while producers migrate; see message_parser's docstring.
# The type alias is intentionally broad.
Message = Any  # type alias for duck-typed message


def is_a2a_message(message: Message) -> bool:
    """Check whether a message is A2A-shaped (spec v1.0.1).

    Useful for call sites that still receive a mix of formats and need to branch
    — e.g. reading ``context_id`` (A2A) versus a ``session_id`` buried in the
    NovaOS metadata payload (ACP).

    Args:
        message: Message to check

    Returns:
        True if the message presents as an A2A Message, False otherwise
    """
    return is_a2a_shaped(message)


def has_novaos_metadata_part(message: Message) -> bool:
    """Check whether a message carries a NovaOS metadata part.

    Recognizes both the A2A ``DataPart`` form and the ACP vendor-content-type
    form, mirroring the parser's own dual-format handling.

    Args:
        message: Message to check

    Returns:
        True if a NovaOS metadata part is present, False otherwise
    """
    from novaos_client_sdk.message_builder import NOVAOS_METADATA_CONTENT_TYPE

    parts = getattr(message, "parts", None) or []
    for part in parts:
        if _is_novaos_metadata_part(part):
            return True
        if getattr(part, "content_type", None) == NOVAOS_METADATA_CONTENT_TYPE:
            return True
    return False


def is_hitl_message(message: Message) -> bool:
    """Check if message contains a HITL message.

    Convenience function that wraps MessageParser.is_hitl_message().

    Args:
        message: message to check

    Returns:
        True if message contains HITL message, False otherwise
    """
    return MessageParser.is_hitl_message(message)


def is_error_message(message: Message) -> bool:
    """Check if message contains an error message.

    Convenience function that wraps MessageParser.is_error_message().

    Args:
        message: message to check

    Returns:
        True if message contains error message, False otherwise
    """
    return MessageParser.is_error_message(message)


def is_progress_message(message: Message) -> bool:
    """Check if message contains a progress message.

    Convenience function that wraps MessageParser.is_progress_message().

    Args:
        message: message to check

    Returns:
        True if message contains progress message, False otherwise
    """
    return MessageParser.is_progress_message(message)


def is_hitl_response(message: Message) -> bool:
    """Check if message contains a HITL response.

    Convenience function that wraps MessageParser.is_hitl_response().

    Args:
        message: message to check

    Returns:
        True if message contains HITL response, False otherwise
    """
    return MessageParser.is_hitl_response(message)


def extract_hitl_message(message: Message) -> HITLMessage | None:
    """Extract HITL message from message metadata.

    Convenience function that wraps MessageParser.extract_hitl_message().

    Args:
        message: message to parse

    Returns:
        HITLMessage if found, None otherwise
    """
    return MessageParser.extract_hitl_message(message)


def extract_error_message(message: Message) -> ErrorMessage | None:
    """Extract error message from message metadata.

    Convenience function that wraps MessageParser.extract_error_message().

    Args:
        message: message to parse

    Returns:
        ErrorMessage if found, None otherwise
    """
    return MessageParser.extract_error_message(message)


def extract_progress_message(message: Message) -> ProgressMessage | None:
    """Extract progress message from message metadata.

    Convenience function that wraps MessageParser.extract_progress_message().

    Args:
        message: message to parse

    Returns:
        ProgressMessage if found, None otherwise
    """
    return MessageParser.extract_progress_message(message)


def extract_hitl_response(message: Message) -> HITLResponse | None:
    """Extract HITL response from message metadata.

    Convenience function that wraps MessageParser.extract_hitl_response().

    Args:
        message: message to parse

    Returns:
        HITLResponse if found, None otherwise
    """
    return MessageParser.extract_hitl_response(message)


def embed_hitl_message(message: Message, hitl: HITLMessage) -> Message:
    """Embed a HITL message in an existing message's metadata.

    Convenience function that wraps MessageBuilder.embed_hitl_message().

    Args:
        message: Existing message
        hitl: HITL message to embed

    Returns:
        New Message with HITL embedded in metadata
    """
    return MessageBuilder.embed_hitl_message(message, hitl)


def embed_error_message(message: Message, error: ErrorMessage) -> Message:
    """Embed an error message in an existing message's metadata.

    Convenience function that wraps MessageBuilder.embed_error_message().

    Args:
        message: Existing message
        error: Error message to embed

    Returns:
        New Message with error embedded in metadata
    """
    return MessageBuilder.embed_error_message(message, error)


def embed_progress_message(message: Message, progress: ProgressMessage) -> Message:
    """Embed a progress message in an existing message's metadata.

    Convenience function that wraps MessageBuilder.embed_progress_message().

    Args:
        message: Existing message
        progress: Progress message to embed

    Returns:
        New Message with progress embedded in metadata
    """
    return MessageBuilder.embed_progress_message(message, progress)


def embed_hitl_response(message: Message, hitl_response: HITLResponse) -> Message:
    """Embed a HITL response in an existing message's metadata.

    Convenience function that wraps MessageBuilder.embed_hitl_response().

    Args:
        message: Existing message
        hitl_response: HITL response to embed

    Returns:
        New Message with HITL response embedded in metadata
    """
    return MessageBuilder.embed_hitl_response(message, hitl_response)


def create_message_with_hitl(content: str, hitl: HITLMessage) -> Message:
    """Create a message with HITL message embedded.

    Convenience function that wraps MessageBuilder.create_message_with_hitl().

    Args:
        content: Human-readable content for MessagePart
        hitl: HITL message to embed

    Returns:
        Message with HITL embedded in metadata
    """
    return MessageBuilder.create_message_with_hitl(content, hitl)


def create_message_with_hitl_response(
    content: str,
    action_id: str,
    session_id: str,
    input_value: str | None = None,
) -> Message:
    """Create a message with HITL response embedded.

    Convenience function that wraps MessageBuilder.create_message_with_hitl_response().

    Args:
        content: Human-readable content for MessagePart
        action_id: Action ID from user's response
        session_id: Session ID for multi-turn conversations
        input_value: Optional input value (for input_request responses)

    Returns:
        Message with HITL response embedded in metadata
    """
    return MessageBuilder.create_message_with_hitl_response(
        content, action_id, session_id, input_value
    )
