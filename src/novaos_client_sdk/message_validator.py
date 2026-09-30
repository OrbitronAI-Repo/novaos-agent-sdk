"""MessageValidator utility for validating standardized message structures."""

from typing import Any

from novaos_client_sdk.a2a_types import Role, is_a2a_shaped
from novaos_client_sdk.message_parser import _extract_metadata


class MessageValidator:
    """Utility class for validating standardized message structures."""

    @staticmethod
    def validate_hitl_message(data: dict[str, Any]) -> bool:
        """Validate HITL message structure.

        Args:
            data: Dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            if data.get("type") != "hitl_message":
                return False
            if "hitl_type" not in data:
                return False
            if "content" not in data or not data["content"]:
                return False
            if "requires_user_action" not in data:
                return False
            return True
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def validate_error_message(data: dict[str, Any]) -> bool:
        """Validate error message structure.

        Args:
            data: Dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            if data.get("type") != "error_message":
                return False
            if "error_type" not in data:
                return False
            if "message" not in data or not data["message"]:
                return False
            return True
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def validate_progress_message(data: dict[str, Any]) -> bool:
        """Validate progress message structure.

        Args:
            data: Dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            if data.get("type") != "progress_message":
                return False
            if "status" not in data:
                return False
            if "progress" not in data or not isinstance(data["progress"], dict):
                return False
            if "message" not in data or not data["message"]:
                return False
            return True
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def validate_hitl_response(data: dict[str, Any]) -> bool:
        """Validate HITL response structure.

        Args:
            data: Dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            if data.get("type") != "hitl_response":
                return False
            if "action_id" not in data or not data["action_id"]:
                return False
            if "session_id" not in data or not data["session_id"]:
                return False
            return True
        except (AttributeError, TypeError):
            return False

    @staticmethod
    def validate_a2a_envelope(message: Any) -> bool:
        """Validate the A2A-specific envelope fields of a message.

        A2A spec v1.0.1 requires ``messageId``, ``role`` and at least one part
        on every ``Message``.  ACP had no such envelope, so this check only
        applies to messages that present as A2A; anything else passes here,
        leaving format-agnostic validation to :meth:`validate_message`.

        (v0.3 also required ``kind == "message"``.  v1.0 dropped that field —
        see F18 — so the A2A-shape test is structural now, not a discriminator
        lookup.)

        Args:
            message: Message to validate

        Returns:
            True if the message is not A2A-shaped, or is a valid A2A envelope
        """
        if not is_a2a_shaped(message):
            return True

        message_id = getattr(message, "message_id", None)
        if not message_id or not isinstance(message_id, str):
            return False

        try:
            Role(str(getattr(message, "role", None)))
        except ValueError:
            return False

        return bool(getattr(message, "parts", None))

    @staticmethod
    def validate_message(message: Any) -> bool:
        """Validate a message and any embedded standardized messages.

        Supports A2A messages (``novaos_client_sdk.a2a_types.A2AMessage``),
        ACP-shaped messages, and ``novaos_client_sdk.models.Message``.

        Args:
            message: A2A, ACP-shaped, or legacy message to validate

        Returns:
            True if valid, False otherwise
        """
        parts = getattr(message, "parts", None)
        if not parts:
            return False

        if not MessageValidator.validate_a2a_envelope(message):
            return False

        metadata = _extract_metadata(message)
        if metadata:
            if "hitl" in metadata:
                if not MessageValidator.validate_hitl_message(metadata["hitl"]):
                    return False
            if "error" in metadata:
                if not MessageValidator.validate_error_message(metadata["error"]):
                    return False
            if "progress" in metadata:
                if not MessageValidator.validate_progress_message(metadata["progress"]):
                    return False
            if "hitl_response" in metadata:
                if not MessageValidator.validate_hitl_response(metadata["hitl_response"]):
                    return False

        return True

    @staticmethod
    def get_validation_errors(data: dict[str, Any], message_type: str | None = None) -> list[str]:
        """Get validation errors for a message dictionary.

        Args:
            data: Dictionary to validate
            message_type: Optional message type hint ('hitl', 'error', 'progress', 'hitl_response')

        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if message_type == "hitl" or (not message_type and data.get("type") == "hitl_message"):
            if not MessageValidator.validate_hitl_message(data):
                if data.get("type") != "hitl_message":
                    errors.append("Missing or invalid 'type' field (expected 'hitl_message')")
                if "hitl_type" not in data:
                    errors.append("Missing required 'hitl_type' field")
                if "content" not in data or not data["content"]:
                    errors.append("Missing or empty 'content' field")
                if "requires_user_action" not in data:
                    errors.append("Missing required 'requires_user_action' field")

        elif message_type == "error" or (not message_type and data.get("type") == "error_message"):
            if not MessageValidator.validate_error_message(data):
                if data.get("type") != "error_message":
                    errors.append("Missing or invalid 'type' field (expected 'error_message')")
                if "error_type" not in data:
                    errors.append("Missing required 'error_type' field")
                if "message" not in data or not data["message"]:
                    errors.append("Missing or empty 'message' field")

        elif message_type == "progress" or (
            not message_type and data.get("type") == "progress_message"
        ):
            if not MessageValidator.validate_progress_message(data):
                if data.get("type") != "progress_message":
                    errors.append("Missing or invalid 'type' field (expected 'progress_message')")
                if "status" not in data:
                    errors.append("Missing required 'status' field")
                if "progress" not in data or not isinstance(data["progress"], dict):
                    errors.append("Missing or invalid 'progress' field")
                if "message" not in data or not data["message"]:
                    errors.append("Missing or empty 'message' field")

        elif message_type == "hitl_response" or (
            not message_type and data.get("type") == "hitl_response"
        ):
            if not MessageValidator.validate_hitl_response(data):
                if data.get("type") != "hitl_response":
                    errors.append("Missing or invalid 'type' field (expected 'hitl_response')")
                if "action_id" not in data or not data["action_id"]:
                    errors.append("Missing or empty 'action_id' field")
                if "session_id" not in data or not data["session_id"]:
                    errors.append("Missing or empty 'session_id' field")

        return errors
