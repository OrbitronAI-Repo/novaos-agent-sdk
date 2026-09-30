"""MessageParser utility for parsing and extracting standardized messages.

Supports three message formats:
- **A2A** (``novaos_client_sdk.a2a_types.A2AMessage``, spec v1.0.1) where
  metadata is carried as a ``DataPart`` whose part-level ``metadata`` tags it
  with ``NOVAOS_METADATA_PART_KEY: "application/vnd.novaos.metadata+json"``.
  This is what ``MessageBuilder`` produces post-migration.
- ACP-shaped messages where metadata is a part with
  ``content_type="application/vnd.novaos.metadata+json"`` carrying a JSON string.
- Legacy ``novaos_client_sdk.models.Message`` (dataclass) where metadata is
  stored in a ``.metadata`` dict attribute.

Parsing stays duck-typed and format-tolerant on purpose: a parser is read-side,
and the SDK reads messages produced by components that migrate on their own
schedules (the worker rewrite, and the per-agent rollout that followed it).
The *builder* is a clean A2A-only cutover; the parser is not.
"""

import json
from typing import Any

from novaos_client_sdk.message_builder import (
    NOVAOS_METADATA_CONTENT_TYPE,
    NOVAOS_METADATA_PART_KEY,
)
from novaos_client_sdk.messages import (
    ErrorMessage,
    HITLMessage,
    HITLResponse,
    ProgressMessage,
)


def _is_novaos_metadata_part(part: Any) -> bool:
    """Check whether an A2A part is tagged as a NovaOS metadata ``DataPart``."""
    part_metadata = getattr(part, "metadata", None)
    if not isinstance(part_metadata, dict):
        return False
    return part_metadata.get(NOVAOS_METADATA_PART_KEY) == NOVAOS_METADATA_CONTENT_TYPE


def _extract_metadata(message: Any) -> dict[str, Any]:
    """Extract structured metadata from a message, supporting multiple formats.

    1. A2A parts-based: scan ``message.parts`` for ``DataPart`` entries tagged with
       ``NOVAOS_METADATA_PART_KEY`` and merge their ``data`` objects.
    2. ACP parts-based: scan ``message.parts`` for parts whose ``content_type``
       equals the NovaOS vendor content-type, JSON-parse each, and merge.
    3. Attribute-based (legacy): fall back to ``message.metadata`` dict.
    """
    merged: dict[str, Any] = {}

    parts = getattr(message, "parts", None)
    if parts:
        # A2A branch: metadata-tagged DataParts carry a real JSON object.
        for part in parts:
            if not _is_novaos_metadata_part(part):
                continue
            data = getattr(part, "data", None)
            if isinstance(data, dict):
                merged.update(data)

        # ACP branch: vendor content-type on the part, JSON string in `content`.
        if not merged:
            for part in parts:
                ct = getattr(part, "content_type", None)
                if ct == NOVAOS_METADATA_CONTENT_TYPE:
                    try:
                        parsed = json.loads(part.content)
                        if isinstance(parsed, dict):
                            merged.update(parsed)
                    except (json.JSONDecodeError, AttributeError, TypeError):
                        continue

    if merged:
        return merged

    # Fall back to .metadata dict (legacy SDK Message)
    metadata = getattr(message, "metadata", None)
    if isinstance(metadata, dict):
        return metadata

    return {}


class MessageParser:
    """Utility class for parsing and extracting standardized messages from A2A Messages."""

    @staticmethod
    def extract_hitl_message(message: Any) -> HITLMessage | None:
        """Extract HITL message from message metadata.

        Args:
            message: A2A, ACP-shaped, or legacy message to parse

        Returns:
            HITLMessage if found, None otherwise
        """
        metadata = _extract_metadata(message)
        if "hitl" not in metadata:
            return None

        try:
            hitl_data = metadata["hitl"]
            if isinstance(hitl_data, dict) and hitl_data.get("type") == "hitl_message":
                return HITLMessage.from_dict(hitl_data)
        except (KeyError, ValueError, TypeError):
            return None

        return None

    @staticmethod
    def extract_error_message(message: Any) -> ErrorMessage | None:
        """Extract error message from message metadata.

        Args:
            message: A2A, ACP-shaped, or legacy message to parse

        Returns:
            ErrorMessage if found, None otherwise
        """
        metadata = _extract_metadata(message)
        if "error" not in metadata:
            return None

        try:
            error_data = metadata["error"]
            if isinstance(error_data, dict) and error_data.get("type") == "error_message":
                return ErrorMessage.from_dict(error_data)
        except (KeyError, ValueError, TypeError):
            return None

        return None

    @staticmethod
    def extract_progress_message(message: Any) -> ProgressMessage | None:
        """Extract progress message from message metadata.

        Args:
            message: A2A, ACP-shaped, or legacy message to parse

        Returns:
            ProgressMessage if found, None otherwise
        """
        metadata = _extract_metadata(message)
        if "progress" not in metadata:
            return None

        try:
            progress_data = metadata["progress"]
            if isinstance(progress_data, dict) and progress_data.get("type") == "progress_message":
                return ProgressMessage.from_dict(progress_data)
        except (KeyError, ValueError, TypeError):
            return None

        return None

    @staticmethod
    def extract_hitl_response(message: Any) -> HITLResponse | None:
        """Extract HITL response from message metadata.

        Args:
            message: A2A, ACP-shaped, or legacy message to parse

        Returns:
            HITLResponse if found, None otherwise
        """
        metadata = _extract_metadata(message)
        if "hitl_response" not in metadata:
            return None

        try:
            response_data = metadata["hitl_response"]
            if isinstance(response_data, dict) and response_data.get("type") == "hitl_response":
                return HITLResponse.from_dict(response_data)
        except (KeyError, ValueError, TypeError):
            return None

        return None

    @staticmethod
    def is_hitl_message(message: Any) -> bool:
        """Check if message contains a HITL message."""
        return MessageParser.extract_hitl_message(message) is not None

    @staticmethod
    def is_error_message(message: Any) -> bool:
        """Check if message contains an error message."""
        return MessageParser.extract_error_message(message) is not None

    @staticmethod
    def is_progress_message(message: Any) -> bool:
        """Check if message contains a progress message."""
        return MessageParser.extract_progress_message(message) is not None

    @staticmethod
    def is_hitl_response(message: Any) -> bool:
        """Check if message contains a HITL response."""
        return MessageParser.extract_hitl_response(message) is not None

    @staticmethod
    def extract_all(message: Any) -> dict[str, Any]:
        """Extract all standardized messages from a message.

        Args:
            message: A2A, ACP-shaped, or legacy message to parse

        Returns:
            Dictionary with keys 'hitl', 'error', 'progress', 'hitl_response'
            (only present if found)
        """
        result: dict[str, Any] = {}

        hitl = MessageParser.extract_hitl_message(message)
        if hitl:
            result["hitl"] = hitl

        error = MessageParser.extract_error_message(message)
        if error:
            result["error"] = error

        progress = MessageParser.extract_progress_message(message)
        if progress:
            result["progress"] = progress

        hitl_response = MessageParser.extract_hitl_response(message)
        if hitl_response:
            result["hitl_response"] = hitl_response

        return result
