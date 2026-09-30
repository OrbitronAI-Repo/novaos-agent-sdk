"""MessageBuilder utility for creating and embedding standardized messages in A2A Messages.

IMPORTANT: All factory methods (create_*, embed_*) return
``novaos_client_sdk.a2a_types.A2AMessage`` — NovaOS's own A2A-native message
type, targeting **A2A specification v1.0.1** (see ``a2a_types.A2A_SPEC_VERSION``).
They previously returned
``acp_sdk.models.Message``; ACP is dead (the upstream repo has been archived
since 2025-08-25) and there is no backward-compatibility requirement, so this is
a clean cutover rather than a dual-format builder.

Structured metadata (HITL, errors, progress) is carried as a second part.  Under
ACP that was a ``MessagePart`` holding a JSON *string* with the vendor
content-type ``application/vnd.novaos.metadata+json``; under A2A it is a
first-class ``DataPart`` whose ``data`` is the payload object, tagged via the
part's own ``metadata`` with ``NOVAOS_METADATA_PART_KEY`` so consumers can tell
NovaOS control metadata apart from an agent's ordinary structured output.
"""

import json
from typing import Any
from uuid import uuid4

from novaos_client_sdk.a2a_types import (
    DEFAULT_TEXT_MEDIA_TYPE,
    A2AMessage,
    DataPart,
    Part,
    Role,
    TextPart,
)
from novaos_client_sdk.messages import (
    Action,
    ErrorDetails,
    ErrorMessage,
    ErrorType,
    HITLMessage,
    HITLMessageType,
    HITLResponse,
    InputType,
    InputValidation,
    ProgressData,
    ProgressMessage,
    ProgressStatus,
)

# Vendor content-type identifying a part as structured NovaOS metadata rather
# than agent output.  Under ACP this was the part's `content_type`; under A2A a
# DataPart has no content-type field, so the same string is carried as the value
# of NOVAOS_METADATA_PART_KEY in the part's `metadata`.  The literal is
# unchanged on purpose — it is the stable NovaOS vendor identifier that
# downstream consumers (e.g. mintAndExecute) match on.
NOVAOS_METADATA_CONTENT_TYPE = "application/vnd.novaos.metadata+json"

# Part-metadata key under which NOVAOS_METADATA_CONTENT_TYPE is recorded.
NOVAOS_METADATA_PART_KEY = "novaos.platform/content-type"

# Keys inside the NovaOS metadata part's payload that carry a *pause* — a task
# state where the agent cannot proceed without something from outside it.
# ``HITL`` is the generic case (a question, a plan to confirm); ``OAUTH`` and
# ``WRITE_CONFIRMATION`` are two pause kinds that were their own ACP event
# *types* (``connector.oauth_required``, ``tool.write_confirmation_required``)
# and have no A2A vocabulary home of their own, so they are carried as
# NovaOS metadata instead.
#
# ⚠️ MUST match ``NOVAOS_PAUSE_KEYS`` in the platform's A2A wire contract —
#    that contract's ``extractPausePayloads`` is the only reader of these
#    three literals, and it lives in a different repository and language, so
#    nothing but this comment and the pinning tests on each side (here:
#    ``tests/test_a2a_message_types.py::TestPausePayloadKeysMatchWireTs``;
#    there, the wire contract's own test suite) keeps the two from drifting.
#    The payload field names below (``connector``/``auth_url``,
#    ``call_id``/``tool_name``) are ACP's unchanged — only the carrier moved.
NOVAOS_PAUSE_HITL_KEY = "hitl"
NOVAOS_PAUSE_OAUTH_KEY = "oauth"
NOVAOS_PAUSE_WRITE_CONFIRMATION_KEY = "write_confirmation"

_DEFAULT_MEDIA_TYPE = DEFAULT_TEXT_MEDIA_TYPE


def _text_part(content: str, content_type: str = _DEFAULT_MEDIA_TYPE) -> TextPart:
    """Build the visible text part, declaring a non-default media type.

    A2A v1.0 gives every part a real ``mediaType`` field, so a caller asking for
    e.g. "text/markdown" gets it in the protocol's own slot.  (Against v0.3 this
    went into a NovaOS metadata key, because that revision's text part had no
    content-type field — see F18.)  Plain "text/plain" is left unset: it is
    A2A's default and needs no annotation.
    """
    if content_type and content_type != _DEFAULT_MEDIA_TYPE:
        return TextPart(text=content, media_type=content_type)
    return TextPart(text=content)


def _metadata_part(metadata: dict[str, Any]) -> DataPart:
    """Build the invisible NovaOS metadata part.

    A2A's ``DataPart.data`` is a JSON object, so the payload is round-tripped
    through ``json.dumps(..., default=str)`` to coerce anything caller-supplied
    that isn't natively JSON-serializable.  Under ACP the same ``default=str``
    guard applied when the payload was serialized into the part's string
    content; keeping it here means the builder still cannot emit a message that
    fails to serialize downstream.
    """
    return DataPart(
        data=json.loads(json.dumps(metadata, default=str)),
        metadata={NOVAOS_METADATA_PART_KEY: NOVAOS_METADATA_CONTENT_TYPE},
    )


def _a2a_message_with_metadata(
    content: str,
    metadata: dict[str, Any],
    content_type: str = _DEFAULT_MEDIA_TYPE,
    session_id: str | None = None,
) -> A2AMessage:
    """Build an A2A Message with a visible text part and an invisible metadata part.

    ``session_id`` maps to A2A's ``contextId`` (ADR F2), so a multi-turn NovaOS
    session is expressed in the protocol's own vocabulary rather than only
    inside the NovaOS metadata payload.
    """
    parts: list[Part] = [_text_part(content, content_type), _metadata_part(metadata)]
    return A2AMessage(parts=parts, role=Role.AGENT, context_id=session_id)


def _appended(message: A2AMessage, metadata: dict[str, Any]) -> A2AMessage:
    """Return a copy of ``message`` with a NovaOS metadata part appended.

    Accepts any message exposing A2A-shaped ``parts``.  Every non-part field the
    A2A ``Message`` object carries (``role``, ``messageId``, ``contextId``,
    ``taskId``, ``referenceTaskIds``, ``extensions``, ``metadata``) is carried
    over — embedding NovaOS metadata appends a part, it must not re-key the
    message or quietly drop protocol fields the caller set.
    """
    return A2AMessage(
        parts=[*message.parts, _metadata_part(metadata)],
        role=getattr(message, "role", Role.AGENT),
        message_id=getattr(message, "message_id", None) or str(uuid4()),
        context_id=getattr(message, "context_id", None),
        task_id=getattr(message, "task_id", None),
        reference_task_ids=getattr(message, "reference_task_ids", None),
        extensions=getattr(message, "extensions", None),
        metadata=getattr(message, "metadata", None),
    )


class MessageBuilder:
    """Utility class for building and embedding standardized messages in A2A Messages."""

    @staticmethod
    def create_hitl_message(
        content: str,
        hitl_type: HITLMessageType | str,
        plan: dict[str, Any] | None = None,
        actions: list[Action] | None = None,
        session_id: str | None = None,
        input_type: InputType | str | None = None,
        validation: InputValidation | dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2AMessage:
        """Create an A2A Message with HITL message embedded as a metadata part.

        Args:
            content: Human-readable message content
            hitl_type: Type of HITL message (HITLMessageType enum or string)
            plan: Optional plan data (for plan_confirmation)
            actions: Optional list of action buttons
            session_id: Optional session ID for multi-turn conversations
            input_type: Optional input type (for input_request). Accepts
                ``InputType`` enum or plain string for backward compatibility.
            validation: Optional validation rules (for input_request). Accepts
                ``InputValidation`` dataclass or plain dict.
            metadata: Optional additional metadata

        Returns:
            A2AMessage with HITL embedded as a DataPart
        """
        from dataclasses import asdict as _asdict

        hitl_metadata: dict[str, Any] = metadata or {}
        if plan:
            hitl_metadata["plan"] = plan
        if input_type:
            hitl_metadata["input_type"] = (
                input_type.value if isinstance(input_type, InputType) else input_type
            )
        if validation:
            if isinstance(validation, InputValidation):
                hitl_metadata["validation"] = _asdict(validation)
            else:
                hitl_metadata["validation"] = validation

        hitl = HITLMessage(
            hitl_type=hitl_type,
            requires_user_action=hitl_type
            in (
                HITLMessageType.PLAN_CONFIRMATION,
                HITLMessageType.INPUT_REQUEST,
                HITLMessageType.BATCH_INPUT_REQUEST,
            ),
            content=content,
            metadata=hitl_metadata,
            actions=actions or [],
            session_id=session_id,
        )

        return _a2a_message_with_metadata(
            content, {NOVAOS_PAUSE_HITL_KEY: hitl.to_dict()}, session_id=session_id
        )

    @staticmethod
    def create_oauth_required_message(
        content: str,
        connector: str,
        auth_url: str,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2AMessage:
        """Create an A2A Message pausing the run for a connector OAuth flow.

        Under ACP this pause was its own event type, ``connector.oauth_required``;
        A2A has no event vocabulary, so it travels as a NovaOS metadata part under
        :data:`NOVAOS_PAUSE_OAUTH_KEY`, read on the platform side by
        ``extractPausePayloads`` in the platform's A2A wire contract. A caller
        that also wants the task itself to reach ``TASK_STATE_AUTH_REQUIRED``
        (rather than relying on this payload's mere presence) sets that state on
        the ``Task`` it builds around this message — this factory only builds the
        message, matching every other ``create_*`` method here.

        Args:
            content: Human-readable message content (e.g. "Connect your Gmail
                account to continue.")
            connector: The connector needing authorization (e.g. "gmail").
            auth_url: The URL the user must visit to complete the OAuth flow.
            session_id: Optional session ID for multi-turn conversations.
            metadata: Optional additional fields merged into the ``oauth``
                payload alongside ``connector``/``auth_url``.

        Returns:
            A2AMessage with the OAuth pause embedded as a DataPart
        """
        payload: dict[str, Any] = {**(metadata or {}), "connector": connector, "auth_url": auth_url}
        return _a2a_message_with_metadata(
            content, {NOVAOS_PAUSE_OAUTH_KEY: payload}, session_id=session_id
        )

    @staticmethod
    def create_write_confirmation_required_message(
        content: str,
        call_id: str,
        tool_name: str,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2AMessage:
        """Create an A2A Message pausing the run for a destructive-tool confirmation.

        Under ACP this pause was its own event type, ``tool.write_confirmation_required``;
        A2A has no event vocabulary, so it travels as a NovaOS metadata part under
        :data:`NOVAOS_PAUSE_WRITE_CONFIRMATION_KEY`, read on the platform side by
        ``extractPausePayloads``. The Temporal worker mints its resume execution
        token bound to ``call_id`` in its background execution path, so an agent
        that omits it turns a confirmable write into a failed run rather than a
        blanket write authorization — always pass the real ``call_id`` the tool
        call carried, never a placeholder.

        Args:
            content: Human-readable message content (e.g. "Confirm sending this
                email?")
            call_id: The id of the pending tool call the confirmation is bound to.
            tool_name: The name of the tool awaiting confirmation.
            session_id: Optional session ID for multi-turn conversations.
            metadata: Optional additional fields merged into the
                ``write_confirmation`` payload alongside ``call_id``/``tool_name``.

        Returns:
            A2AMessage with the write-confirmation pause embedded as a DataPart
        """
        payload: dict[str, Any] = {
            **(metadata or {}),
            "call_id": call_id,
            "tool_name": tool_name,
        }
        return _a2a_message_with_metadata(
            content, {NOVAOS_PAUSE_WRITE_CONFIRMATION_KEY: payload}, session_id=session_id
        )

    @staticmethod
    def create_error_message(
        content: str,
        error_type: ErrorType | str,
        details: ErrorDetails | None = None,
        recovery_suggestions: list[str] | None = None,
        retryable: bool = False,
        session_id: str | None = None,
    ) -> A2AMessage:
        """Create an A2A Message with error message embedded as a metadata part.

        Args:
            content: Human-readable error message
            error_type: Type of error (ErrorType enum or string)
            details: Optional error details
            recovery_suggestions: Optional recovery suggestions
            retryable: Whether the error is retryable
            session_id: Optional session ID

        Returns:
            A2AMessage with error embedded as a DataPart
        """
        error = ErrorMessage(
            error_type=error_type,
            message=content,
            details=details or {},
            recovery_suggestions=recovery_suggestions or [],
            retryable=retryable,
            session_id=session_id,
        )

        return _a2a_message_with_metadata(
            content, {"error": error.to_dict()}, session_id=session_id
        )

    @staticmethod
    def create_progress_message(
        content: str,
        status: ProgressStatus | str,
        progress: ProgressData,
        metadata: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> A2AMessage:
        """Create an A2A Message with progress message embedded as a metadata part.

        Args:
            content: Human-readable progress message
            status: Progress status (ProgressStatus enum or string)
            progress: Progress data dictionary
            metadata: Optional additional metadata
            session_id: Optional session ID

        Returns:
            A2AMessage with progress embedded as a DataPart
        """
        progress_msg = ProgressMessage(
            status=status,
            progress=progress,
            message=content,
            metadata=metadata or {},
            session_id=session_id,
        )

        return _a2a_message_with_metadata(
            content, {"progress": progress_msg.to_dict()}, session_id=session_id
        )

    @staticmethod
    def embed_hitl_message(message: A2AMessage, hitl: HITLMessage) -> A2AMessage:
        """Embed a HITL message in an existing A2A Message as a metadata part.

        Args:
            message: Existing A2A Message
            hitl: HITL message to embed

        Returns:
            New A2AMessage with HITL appended as a DataPart
        """
        return _appended(message, {NOVAOS_PAUSE_HITL_KEY: hitl.to_dict()})

    @staticmethod
    def embed_error_message(message: A2AMessage, error: ErrorMessage) -> A2AMessage:
        """Embed an error message in an existing A2A Message as a metadata part.

        Args:
            message: Existing A2A Message
            error: Error message to embed

        Returns:
            New A2AMessage with error appended as a DataPart
        """
        return _appended(message, {"error": error.to_dict()})

    @staticmethod
    def embed_progress_message(message: A2AMessage, progress: ProgressMessage) -> A2AMessage:
        """Embed a progress message in an existing A2A Message as a metadata part.

        Args:
            message: Existing A2A Message
            progress: Progress message to embed

        Returns:
            New A2AMessage with progress appended as a DataPart
        """
        return _appended(message, {"progress": progress.to_dict()})

    @staticmethod
    def create_message_with_hitl(
        content: str,
        hitl: HITLMessage,
        content_type: str = _DEFAULT_MEDIA_TYPE,
    ) -> A2AMessage:
        """Create an A2A Message with HITL message embedded as a metadata part.

        Args:
            content: Human-readable content for the visible TextPart
            hitl: HITL message to embed
            content_type: Media type for the visible TextPart. A2A's TextPart has
                no content-type field, so anything other than ``text/plain`` is
                recorded in the part's ``metadata`` under
                the part's own ``mediaType``.

        Returns:
            A2AMessage with HITL embedded as a DataPart
        """
        return _a2a_message_with_metadata(
            content,
            {NOVAOS_PAUSE_HITL_KEY: hitl.to_dict()},
            content_type,
            session_id=hitl.session_id,
        )

    @staticmethod
    def create_message_with_hitl_response(
        content: str,
        action_id: str,
        session_id: str,
        input_value: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2AMessage:
        """Create an A2A Message with HITL response embedded as a metadata part.

        A HITL response travels from the user back to the agent, so the message
        carries ``role="user"`` — unlike the agent-authored factory methods above.

        Args:
            content: Human-readable content for the visible TextPart
            action_id: Action ID from user's response
            session_id: Session ID for multi-turn conversations
            input_value: Optional input value (for input_request responses)
            metadata: Optional additional metadata

        Returns:
            A2AMessage with HITL response embedded as a DataPart
        """
        hitl_response = HITLResponse(
            action_id=action_id,
            session_id=session_id,
            input_value=input_value,
            metadata=metadata or {},
        )

        return A2AMessage(
            parts=[
                _text_part(content),
                _metadata_part({"hitl_response": hitl_response.to_dict()}),
            ],
            role=Role.USER,
            context_id=session_id,
        )

    @staticmethod
    def create_hitl_response(
        action_id: str,
        session_id: str,
        input_value: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> HITLResponse:
        """Create a HITL response object.

        Args:
            action_id: Action ID from user's response
            session_id: Session ID for multi-turn conversations
            input_value: Optional input value (for input_request responses)
            metadata: Optional additional metadata

        Returns:
            HITLResponse object
        """
        return HITLResponse(
            action_id=action_id,
            session_id=session_id,
            input_value=input_value,
            metadata=metadata or {},
        )

    @staticmethod
    def embed_hitl_response(message: A2AMessage, hitl_response: HITLResponse) -> A2AMessage:
        """Embed a HITL response in an existing A2A Message as a metadata part.

        Args:
            message: Existing A2A Message
            hitl_response: HITL response to embed

        Returns:
            New A2AMessage with HITL response appended as a DataPart
        """
        return _appended(message, {"hitl_response": hitl_response.to_dict()})
