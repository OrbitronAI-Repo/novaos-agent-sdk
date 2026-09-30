"""Standardized message models for NovaOS Client SDK.

This module provides type-safe message models for standardized communication
between agents and clients, including HITL messages, error messages, and progress messages.
All messages are designed to work within A2A Message structures (spec v1.0.1);
they are carried as a DataPart, built by ``message_builder``.

Helper classes and utilities are in separate modules:
- message_builder.py: MessageBuilder class
- message_parser.py: MessageParser class
- message_validator.py: MessageValidator class
- message_helpers.py: Convenience functions
"""

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    # Import for type checking only to avoid circular imports
    from novaos_client_sdk.message_builder import MessageBuilder
    from novaos_client_sdk.message_helpers import (
        create_message_with_hitl,
        create_message_with_hitl_response,
        embed_error_message,
        embed_hitl_message,
        embed_hitl_response,
        embed_progress_message,
        extract_error_message,
        extract_hitl_message,
        extract_hitl_response,
        extract_progress_message,
        is_error_message,
        is_hitl_message,
        is_hitl_response,
        is_progress_message,
    )
    from novaos_client_sdk.message_parser import MessageParser
    from novaos_client_sdk.message_validator import MessageValidator

# Type aliases for common structures
Action = dict[str, Any]  # Action dict with id, label, type, etc.
ProgressData = dict[str, Any]  # Progress dict with current, total, percentage, etc.
ErrorDetails = dict[str, Any]  # Error details dict with code, original_error, context, etc.


class HITLMessageType(StrEnum):
    """HITL message type enumeration."""

    PLAN_CONFIRMATION = "plan_confirmation"
    INPUT_REQUEST = "input_request"
    BATCH_INPUT_REQUEST = "batch_input_request"
    ERROR_NOTIFICATION = "error_notification"
    INFO_MESSAGE = "info_message"
    SUCCESS_MESSAGE = "success_message"
    EXECUTION_PROGRESS = "execution_progress"


class ErrorType(StrEnum):
    """Error type enumeration."""

    VALIDATION_ERROR = "validation_error"
    CONNECTION_ERROR = "connection_error"
    NOT_FOUND_ERROR = "not_found_error"
    PERMISSION_ERROR = "permission_error"
    EXECUTION_ERROR = "execution_error"
    UNEXPECTED_ERROR = "unexpected_error"


class ProgressStatus(StrEnum):
    """Progress status enumeration."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class InputType(StrEnum):
    """Supported input types for HITL input_request messages.

    Agents use these to declare what kind of value they need.
    The frontend uses them to render the appropriate input widget
    and apply client-side validation.
    """

    TEXT = "text"  # Free-form text input
    EMAIL = "email"  # Email address (validated format)
    URL = "url"  # URL (must start with http:// or https://)
    NUMBER = "number"  # Numeric value
    PHONE = "phone"  # Phone number
    CHOICE = "choice"  # Select from options (requires options in validation)
    DATE = "date"  # Date picker
    PASSWORD = "password"  # Masked text input


@dataclass
class InputValidation:
    """Validation rules for HITL input_request messages.

    Agents attach these to input requests so the frontend can
    validate user input before submitting (faster feedback loop).
    The agent should still validate server-side as a safety net.
    """

    required: bool = True
    min_length: int | None = None
    max_length: int | None = None
    min_value: float | None = None  # for NUMBER type
    max_value: float | None = None  # for NUMBER type
    pattern: str | None = None  # regex for custom validation
    pattern_message: str | None = None  # human-readable error for pattern
    placeholder: str | None = None  # hint text in the input field
    default_value: str | None = None  # pre-filled value
    options: list[dict[str, str]] | None = None  # for CHOICE type: [{"id": "...", "label": "..."}]


@dataclass
class InputField:
    """A single field in a batch input request.

    Used by ``HITLMessage.batch_input_request()`` to describe each field
    in a multi-field form.
    """

    name: str  # field key (e.g., "to", "subject")
    label: str  # display label
    input_type: InputType = InputType.TEXT
    validation: InputValidation | None = None
    description: str | None = None  # help text


@dataclass
class HITLMessage:
    """Human-in-the-Loop message model.

    Used when an agent needs user input, confirmation, or decision-making.
    """

    type: Literal["hitl_message"] = field(default="hitl_message", init=False)
    hitl_type: HITLMessageType | str
    content: str
    requires_user_action: bool
    session_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    actions: list[Action] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate HITL message structure."""
        if not self.content or not self.content.strip():
            raise ValueError("HITL message content cannot be empty")
        if self.requires_user_action and not self.actions:
            raise ValueError("HITL message requiring user action must have defined actions")
        # Normalize hitl_type to string if it's an enum
        if isinstance(self.hitl_type, HITLMessageType):
            object.__setattr__(self, "hitl_type", self.hitl_type.value)

    def to_dict(self) -> dict[str, Any]:
        """Convert HITL message to dictionary."""
        hitl_type_value = (
            self.hitl_type.value if isinstance(self.hitl_type, HITLMessageType) else self.hitl_type
        )
        return {
            "type": self.type,
            "hitl_type": hitl_type_value,
            "requires_user_action": self.requires_user_action,
            "content": self.content,
            "metadata": self.metadata,
            "actions": self.actions,
            "session_id": self.session_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HITLMessage":
        """Create HITLMessage from dictionary.

        Args:
            data: Dictionary containing HITL message data

        Returns:
            HITLMessage instance

        Raises:
            KeyError: If required fields are missing
            ValueError: If data is invalid
        """
        try:
            return cls(
                hitl_type=data["hitl_type"],  # Accept string or enum
                content=data["content"],
                requires_user_action=data["requires_user_action"],
                metadata=data.get("metadata", {}),
                actions=data.get("actions", []),
                session_id=data.get("session_id"),
            )
        except KeyError as e:
            raise KeyError(f"Missing required field in HITL message: {e}") from e

    @classmethod
    def plan_confirmation(
        cls,
        content: str,
        plan: dict[str, Any],
        actions: list[Action],
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "HITLMessage":
        """Factory method for plan confirmation HITL message."""
        _metadata = metadata if metadata is not None else {}
        _metadata["plan"] = plan
        return cls(
            hitl_type=HITLMessageType.PLAN_CONFIRMATION,
            content=content,
            requires_user_action=True,
            actions=actions,
            session_id=session_id,
            metadata=_metadata,
        )

    @classmethod
    def input_request(
        cls,
        content: str,
        input_type: InputType | str = InputType.TEXT,
        validation: InputValidation | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        actions: list[Action] | None = None,
    ) -> "HITLMessage":
        """Factory method for input request HITL message.

        Args:
            content: Prompt text for the user.
            input_type: Type of input expected (e.g., InputType.EMAIL). Accepts
                both ``InputType`` enum values and plain strings for backward
                compatibility.
            validation: Optional validation rules for client-side validation.
            session_id: Session identifier for the HITL conversation.
            metadata: Additional metadata to attach.
            actions: Action buttons (typically empty for input requests).

        Note: Input requests typically don't require action buttons as the user
        provides input directly. If actions are provided, they will be used.
        """
        _metadata = metadata if metadata is not None else {}
        _metadata["input_type"] = (
            input_type.value if isinstance(input_type, InputType) else input_type
        )
        if validation:
            _metadata["validation"] = asdict(validation)
        # Input requests don't require action buttons (user provides input directly)
        # But if actions are provided, use them
        _actions = actions if actions is not None else []
        return cls(
            hitl_type=HITLMessageType.INPUT_REQUEST,
            content=content,
            requires_user_action=bool(_actions),  # Only require action if actions provided
            session_id=session_id,
            metadata=_metadata,
            actions=_actions,
        )

    @classmethod
    def batch_input_request(
        cls,
        content: str,
        fields: list[InputField],
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        actions: list[Action] | None = None,
    ) -> "HITLMessage":
        """Request multiple inputs from the user in a single form.

        Args:
            content: Introductory prompt text for the form.
            fields: List of ``InputField`` objects describing each field.
            session_id: Session identifier for the HITL conversation.
            metadata: Additional metadata to attach.
            actions: Action buttons (defaults to a Submit button).
        """
        _metadata = metadata if metadata is not None else {}
        _metadata["fields"] = [asdict(f) for f in fields]
        _actions = (
            actions
            if actions is not None
            else [
                {"id": "submit", "label": "Submit", "type": "button"},
            ]
        )
        return cls(
            hitl_type=HITLMessageType.BATCH_INPUT_REQUEST,
            content=content,
            requires_user_action=bool(_actions),
            session_id=session_id,
            metadata=_metadata,
            actions=_actions,
        )

    @classmethod
    def error_notification(
        cls,
        content: str,
        error_type: str,
        recovery_suggestions: list[str] | None = None,
        session_id: str | None = None,
        requires_user_action: bool = False,
        actions: list[Action] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "HITLMessage":
        """Factory method for error notification HITL message."""
        _metadata = metadata if metadata is not None else {}
        _metadata["error_type"] = error_type
        if recovery_suggestions:
            _metadata["recovery_suggestions"] = recovery_suggestions
        return cls(
            hitl_type=HITLMessageType.ERROR_NOTIFICATION,
            content=content,
            requires_user_action=requires_user_action,
            session_id=session_id,
            actions=actions if actions is not None else [],
            metadata=_metadata,
        )

    @classmethod
    def info_message(
        cls,
        content: str,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "HITLMessage":
        """Factory method for informational HITL message."""
        return cls(
            hitl_type=HITLMessageType.INFO_MESSAGE,
            content=content,
            requires_user_action=False,
            session_id=session_id,
            metadata=metadata if metadata is not None else {},
        )

    @classmethod
    def success_message(
        cls,
        content: str,
        session_id: str | None = None,
        result: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "HITLMessage":
        """Factory method for success HITL message."""
        _metadata = metadata if metadata is not None else {}
        if result:
            _metadata["result"] = result
        return cls(
            hitl_type=HITLMessageType.SUCCESS_MESSAGE,
            content=content,
            requires_user_action=False,
            session_id=session_id,
            metadata=_metadata,
        )

    @classmethod
    def execution_progress(
        cls,
        content: str,
        progress: ProgressData,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "HITLMessage":
        """Factory method for execution progress HITL message."""
        _metadata = metadata if metadata is not None else {}
        _metadata["progress"] = progress
        return cls(
            hitl_type=HITLMessageType.EXECUTION_PROGRESS,
            content=content,
            requires_user_action=False,
            session_id=session_id,
            metadata=_metadata,
        )


@dataclass
class HITLResponse:
    """Human-in-the-Loop response model.

    Used when users respond to HITL messages.
    """

    action_id: str
    session_id: str
    type: Literal["hitl_response"] = field(default="hitl_response", init=False)
    input_value: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate HITL response structure."""
        if not self.action_id or not self.action_id.strip():
            raise ValueError("HITL response action_id cannot be empty")
        if not self.session_id or not self.session_id.strip():
            raise ValueError("HITL response session_id cannot be empty")

    def to_dict(self) -> dict[str, Any]:
        """Convert HITL response to dictionary."""
        return {
            "type": self.type,
            "action_id": self.action_id,
            "input_value": self.input_value,
            "session_id": self.session_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HITLResponse":
        """Create HITLResponse from dictionary.

        Args:
            data: Dictionary containing HITL response data

        Returns:
            HITLResponse instance

        Raises:
            KeyError: If required fields are missing
        """
        try:
            return cls(
                action_id=data["action_id"],
                session_id=data["session_id"],
                input_value=data.get("input_value"),
                metadata=data.get("metadata", {}),
            )
        except KeyError as e:
            raise KeyError(f"Missing required field in HITL response: {e}") from e


@dataclass
class ErrorMessage:
    """Standardized error message model."""

    type: Literal["error_message"] = field(default="error_message", init=False)
    error_type: ErrorType | str
    message: str
    session_id: str | None = None
    details: ErrorDetails = field(default_factory=dict)
    recovery_suggestions: list[str] = field(default_factory=list)
    retryable: bool = False

    def __post_init__(self) -> None:
        """Validate error message structure."""
        if not self.message or not self.message.strip():
            raise ValueError("Error message content cannot be empty")
        # Normalize error_type to string if it's an enum
        if isinstance(self.error_type, ErrorType):
            object.__setattr__(self, "error_type", self.error_type.value)

    def to_dict(self) -> dict[str, Any]:
        """Convert error message to dictionary."""
        error_type_value = (
            self.error_type.value if isinstance(self.error_type, ErrorType) else self.error_type
        )
        return {
            "type": self.type,
            "error_type": error_type_value,
            "message": self.message,
            "details": self.details,
            "recovery_suggestions": self.recovery_suggestions,
            "retryable": self.retryable,
            "session_id": self.session_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ErrorMessage":
        """Create ErrorMessage from dictionary.

        Args:
            data: Dictionary containing error message data

        Returns:
            ErrorMessage instance

        Raises:
            KeyError: If required fields are missing
        """
        try:
            return cls(
                error_type=data["error_type"],  # Accept string or enum
                message=data["message"],
                details=data.get("details", {}),
                recovery_suggestions=data.get("recovery_suggestions", []),
                retryable=data.get("retryable", False),
                session_id=data.get("session_id"),
            )
        except KeyError as e:
            raise KeyError(f"Missing required field in error message: {e}") from e

    @classmethod
    def connection_error(
        cls,
        message: str,
        session_id: str | None = None,
        details: ErrorDetails | None = None,
        recovery_suggestions: list[str] | None = None,
        retryable: bool = True,
    ) -> "ErrorMessage":
        """Factory method for connection error."""
        return cls(
            error_type=ErrorType.CONNECTION_ERROR,
            message=message,
            session_id=session_id,
            details=details if details is not None else {},
            recovery_suggestions=(
                recovery_suggestions
                if recovery_suggestions is not None
                else ["Check network connection", "Try again"]
            ),
            retryable=retryable,
        )

    @classmethod
    def validation_error(
        cls,
        message: str,
        session_id: str | None = None,
        details: ErrorDetails | None = None,
        recovery_suggestions: list[str] | None = None,
        retryable: bool = False,
    ) -> "ErrorMessage":
        """Factory method for validation error."""
        return cls(
            error_type=ErrorType.VALIDATION_ERROR,
            message=message,
            session_id=session_id,
            details=details if details is not None else {},
            recovery_suggestions=(
                recovery_suggestions
                if recovery_suggestions is not None
                else ["Review input and try again"]
            ),
            retryable=retryable,
        )

    @classmethod
    def not_found_error(
        cls,
        message: str,
        session_id: str | None = None,
        details: ErrorDetails | None = None,
        recovery_suggestions: list[str] | None = None,
        retryable: bool = False,
    ) -> "ErrorMessage":
        """Factory method for not found error."""
        return cls(
            error_type=ErrorType.NOT_FOUND_ERROR,
            message=message,
            session_id=session_id,
            details=details if details is not None else {},
            recovery_suggestions=(
                recovery_suggestions
                if recovery_suggestions is not None
                else ["Verify resource exists"]
            ),
            retryable=retryable,
        )


@dataclass
class ProgressMessage:
    """Standardized progress message model."""

    type: Literal["progress_message"] = field(default="progress_message", init=False)
    status: ProgressStatus | str
    message: str
    session_id: str | None = None
    progress: ProgressData = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate progress message structure."""
        if not self.message or not self.message.strip():
            raise ValueError("Progress message content cannot be empty")
        # Normalize status to string if it's an enum
        if isinstance(self.status, ProgressStatus):
            object.__setattr__(self, "status", self.status.value)
        # Check if progress data is required (after normalization, status is always a string)
        if self.status == ProgressStatus.IN_PROGRESS.value and not self.progress:
            raise ValueError("Progress data is required for in_progress status")

    def to_dict(self) -> dict[str, Any]:
        """Convert progress message to dictionary."""
        status_value = self.status.value if isinstance(self.status, ProgressStatus) else self.status
        return {
            "type": self.type,
            "status": status_value,
            "message": self.message,
            "progress": self.progress,
            "metadata": self.metadata,
            "session_id": self.session_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProgressMessage":
        """Create ProgressMessage from dictionary.

        Args:
            data: Dictionary containing progress message data

        Returns:
            ProgressMessage instance

        Raises:
            KeyError: If required fields are missing
        """
        try:
            return cls(
                status=data["status"],  # Accept string or enum
                message=data["message"],
                progress=data.get("progress", {}),
                metadata=data.get("metadata", {}),
                session_id=data.get("session_id"),
            )
        except KeyError as e:
            raise KeyError(f"Missing required field in progress message: {e}") from e


from novaos_client_sdk.message_builder import (
    MessageBuilder,  # noqa: E402
)
from novaos_client_sdk.message_helpers import (  # noqa: E402
    create_message_with_hitl,
    create_message_with_hitl_response,
    embed_error_message,
    embed_hitl_message,
    embed_hitl_response,
    embed_progress_message,
    extract_error_message,
    extract_hitl_message,
    extract_hitl_response,
    extract_progress_message,
    has_novaos_metadata_part,
    is_a2a_message,
    is_error_message,
    is_hitl_message,
    is_hitl_response,
    is_progress_message,
)
from novaos_client_sdk.message_parser import MessageParser  # noqa: E402
from novaos_client_sdk.message_validator import MessageValidator  # noqa: E402

__all__ = [
    # Enums
    "HITLMessageType",
    "ErrorType",
    "ProgressStatus",
    "InputType",
    # Models
    "HITLMessage",
    "HITLResponse",
    "ErrorMessage",
    "ProgressMessage",
    "InputValidation",
    "InputField",
    # Type aliases
    "Action",
    "ProgressData",
    "ErrorDetails",
    # Helper classes (re-exported)
    "MessageBuilder",
    "MessageParser",
    "MessageValidator",
    # Convenience functions (re-exported)
    "is_hitl_message",
    "is_error_message",
    "is_progress_message",
    "is_hitl_response",
    "extract_hitl_message",
    "extract_error_message",
    "extract_progress_message",
    "extract_hitl_response",
    "is_a2a_message",
    "has_novaos_metadata_part",
    "embed_hitl_message",
    "embed_error_message",
    "embed_progress_message",
    "embed_hitl_response",
    "create_message_with_hitl",
    "create_message_with_hitl_response",
]
