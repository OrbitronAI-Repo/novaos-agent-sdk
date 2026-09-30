"""NovaOS Client SDK - Python client library for A2A agents (spec v1.0.1)."""

from novaos_client_sdk._resilience import CircuitBreaker
from novaos_client_sdk.a2a_agent_card import (
    AGENT_CARD_WELL_KNOWN_PATH,
    PROTOCOL_BINDING_GRPC,
    PROTOCOL_BINDING_HTTP_JSON,
    PROTOCOL_BINDING_JSONRPC,
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentProvider,
    AgentSkill,
    select_interface,
)
from novaos_client_sdk.a2a_artifact import (
    ARTIFACT_REVISION_KEY,
    DEFAULT_ARTIFACT_REVISION,
    Artifact,
)
from novaos_client_sdk.a2a_event_queue import EventQueue, TaskEvent
from novaos_client_sdk.a2a_events import (
    StreamFrame,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
    is_last_frame,
    stream_frame_from_dict,
)
from novaos_client_sdk.a2a_extensions import (
    A2A_EXTENSIONS_HEADER,
    A2AExtension,
    ExtensionActivation,
    ExtensionRegistry,
    extensions_header_value,
    requested_extension_uris,
)
from novaos_client_sdk.a2a_jsonrpc import (
    A2A_JSONRPC_PATH,
    JSONRPC_VERSION,
    METHOD_CANCEL_TASK,
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_GET_TASK,
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS,
    METHOD_LIST_TASKS,
    METHOD_SEND_MESSAGE,
    METHOD_SEND_STREAMING_MESSAGE,
    METHOD_SUBSCRIBE_TO_TASK,
    METHOD_UNAVAILABLE_ERROR_CODES,
    JsonRpcRequest,
)
from novaos_client_sdk.a2a_push_notification import (
    AuthenticationInfo,
    TaskPushNotificationConfig,
    TaskPushNotificationConfigList,
)
from novaos_client_sdk.a2a_push_notification_store import (
    DEFAULT_MAX_PUSH_CONFIGS,
    InMemoryPushNotificationConfigStore,
    PushNotificationConfigStore,
)
from novaos_client_sdk.a2a_task import (
    INTERRUPT_TASK_STATES,
    TERMINAL_TASK_STATES,
    Task,
    TaskList,
    TaskState,
    TaskStatus,
)
from novaos_client_sdk.a2a_task_store import (
    DEFAULT_MAX_TASKS,
    InMemoryTaskStore,
    TaskStore,
)
from novaos_client_sdk.a2a_task_updater import TaskUpdater
from novaos_client_sdk.a2a_types import (
    A2A_PROTOCOL_VERSION,
    A2A_SPEC_VERSION,
    A2A_VERSION_HEADER,
    A2AMessage,
    DataPart,
    FilePart,
    FileWithBytes,
    FileWithUri,
    PartKind,
    Role,
    TextPart,
)
from novaos_client_sdk.agent_builder import (
    NOVAOS_METADATA_CONTENT_TYPE,
    NOVAOS_METADATA_PART_KEY,
    NOVAOS_SKILL_METADATA_CONTENT_TYPE,
    OUTPUT_ARTIFACT_ID,
    SKILL_ID_METADATA_KEY,
    AgentBuilder,
)
from novaos_client_sdk.agent_context import (
    NOVAOS_CONTEXT_METADATA_KEY,
    AgentExecutionContext,
)
from novaos_client_sdk.agent_types import (
    AgentRuntimeContext,
    ConversationTurn,
    SkillResult,
    ToolResponse,
)
from novaos_client_sdk.auth import (
    APIKeyAuth,
    AuthProvider,
    BearerTokenAuth,
    CredentialService,
    InMemoryContextCredentialStore,
    get_api_key_from_env,
)
from novaos_client_sdk.auth_interceptor import AuthInterceptor
from novaos_client_sdk.interceptors import (
    AfterArgs,
    BeforeArgs,
    ClientCallContext,
    ClientCallInterceptor,
)
from novaos_client_sdk.tools import (
    BatchIngestResponse,
    ConnectorTool,
    IngestItem,
    IngestJobStatus,
    ToolBindings,
)
from novaos_client_sdk.tracing import (
    configure_otel,
    extract_parent_id,
    extract_trace_id,
    get_traceparent,
    inject_trace_headers,
    is_valid_traceparent,
    propagate_trace,
    traced_gateway_call,
    traced_tool_call,
)

# Token verification (requires optional 'crypto' dependency: pip install novaos-agent-sdk[crypto])
try:
    from novaos_client_sdk.token_verifier import JWKSTokenVerifier
except ImportError:
    JWKSTokenVerifier = None  # type: ignore[assignment,misc]

# Sealed-envelope secret delivery (requires the optional 'crypto'
# dependency: pip install novaos-agent-sdk[crypto]). Guarded the same way
# JWKSTokenVerifier is, so a pod that never receives sealed secrets still
# imports the SDK without the extra installed.
try:
    from novaos_client_sdk.a2a_pod_keyring import PodKeyring, RecipientKey
    from novaos_client_sdk.a2a_sealed_envelope import (
        SEALED_ENVELOPE_SUITE,
        SecretDelivery,
        open_envelope,
    )

    # The user-context/v1 extension rides on the same extra: it is
    # the thing that puts sealed envelopes to work, so a pod that cannot open
    # one has no use for it either.
    from novaos_client_sdk.a2a_user_context import (
        USER_CONTEXT_EXTENSION_URI,
        SealedUserContext,
        UserContextExtension,
    )
except ImportError:
    PodKeyring = None  # type: ignore[assignment,misc]
    RecipientKey = None  # type: ignore[assignment,misc]
    SecretDelivery = None  # type: ignore[assignment,misc]
    open_envelope = None  # type: ignore[assignment]
    SEALED_ENVELOPE_SUITE = None  # type: ignore[assignment,misc]
    USER_CONTEXT_EXTENSION_URI = None  # type: ignore[assignment,misc]
    SealedUserContext = None  # type: ignore[assignment,misc]
    UserContextExtension = None  # type: ignore[assignment,misc]

# Agent Card JWS signature verification (requires the optional
# 'crypto' dependency: pip install novaos-agent-sdk[crypto]). Guarded the same
# way as the sealed-envelope imports above, so a caller who never verifies a
# card's signatures still imports the SDK without the extra installed.
try:
    from novaos_client_sdk.a2a_card_signing import (
        DEFAULT_SIGNATURE_ALGORITHMS,
        canonicalize_agent_card,
        verify_agent_card_signatures,
    )
except ImportError:
    DEFAULT_SIGNATURE_ALGORITHMS = None  # type: ignore[assignment,misc]
    canonicalize_agent_card = None  # type: ignore[assignment,misc]
    verify_agent_card_signatures = None  # type: ignore[assignment,misc]

# LLM Gateway Client (requires optional 'llm' dependency: pip install novaos-agent-sdk[llm])
try:
    from novaos_client_sdk.llm import LLMClient
except ImportError:
    LLMClient = None  # type: ignore[assignment,misc]
from novaos_client_sdk.capabilities import CapabilityIndex, CapabilityMatcher, MatchResult
from novaos_client_sdk.client import Client, ClientConfig
from novaos_client_sdk.config import (
    Environment,
    get_dns_url_for_environment,
    get_dns_url_from_env,
    get_environment,
)
from novaos_client_sdk.context import ExecutionContext
from novaos_client_sdk.discovery import (
    DEFAULT_AGENT_CARD_CACHE_TTL,
    AgentDiscoveryService,
    fetch_agent_card,
)
from novaos_client_sdk.dns import AgentDNSClient, AgentDNSRecord, discover_and_connect
from novaos_client_sdk.exceptions import (
    A2A_ERROR_SPEC_VERSION,
    A2A_ERROR_TYPES,
    JSONRPC_CODE_DETAIL_KEY,
    A2AProtocolError,
    AgentCardSignatureError,
    ContentTypeNotSupportedError,
    ExtendedAgentCardNotConfiguredError,
    ExtensionSupportRequiredError,
    InvalidAgentCardSignatureError,
    InvalidAgentResponseError,
    MissingExecutionTokenError,
    NoAgentCardSignatureError,
    NovaOSAgentNotFoundError,
    NovaOSAuthenticationError,
    NovaOSConnectionError,
    NovaOSDNSAgentNotFoundError,
    NovaOSDNSConnectionError,
    NovaOSDNSError,
    NovaOSDNSRateLimitError,
    NovaOSError,
    NovaOSRateLimitError,
    NovaOSServerError,
    NovaOSValidationError,
    PushNotificationNotSupportedError,
    RecipientKeyUnavailableError,
    SealedEnvelopeError,
    SealedEnvelopeFormatError,
    SecretNotDeliveredError,
    TaskNotCancelableError,
    TaskNotFoundError,
    UnsupportedOperationError,
    VersionNotSupportedError,
    a2a_error_for_code,
    a2a_error_from_json_rpc,
    error_from_json_rpc,
    jsonrpc_error_code,
)
from novaos_client_sdk.gateway import (
    TERMINAL_RUN_STATUSES,
    ExecuteResult,
    GatewayClient,
    StreamEvent,
)
from novaos_client_sdk.heartbeat import (
    HeartbeatBudget,
    HeartbeatClient,
    HeartbeatConfig,
    HeartbeatConfigCreated,
    HeartbeatConfigStatus,
    HeartbeatSchedule,
)
from novaos_client_sdk.logging import get_agent_logger
from novaos_client_sdk.mcp import MCPClient, MCPTool, MCPToolResult
from novaos_client_sdk.memory import (
    AssembleResult,
    ForgetResult,
    HintResult,
    LiveMessage,
    MemoryClient,
    MemoryItem,
    MemoryMetadata,
    ProposalStatus,
    QueryResult,
    WriteResult,
)

# isort: off
# `messages` must be imported before `message_builder`: `messages.py` imports
# `message_builder` at its own bottom (after defining the names
# `message_builder` in turn imports back), which is what makes the cycle work.
# Importing `message_builder` first here would re-enter it before `MessageBuilder`
# is defined — see the comment on `agent_builder.NOVAOS_METADATA_CONTENT_TYPE`
# for the same constraint from the other side. Do not let isort/ruff reorder
# these two imports alphabetically.
from novaos_client_sdk.messages import (
    Action,
    ErrorDetails,
    ErrorMessage,
    ErrorType,
    HITLMessage,
    HITLMessageType,
    HITLResponse,
    InputField,
    InputType,
    InputValidation,
    MessageBuilder,
    MessageParser,
    MessageValidator,
    ProgressData,
    ProgressMessage,
    ProgressStatus,
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
from novaos_client_sdk.message_builder import (
    NOVAOS_PAUSE_HITL_KEY,
    NOVAOS_PAUSE_OAUTH_KEY,
    NOVAOS_PAUSE_WRITE_CONFIRMATION_KEY,
)

# isort: on
from novaos_client_sdk.models import (
    Author,
    Capability,
    ContentType,
    Message,
    MessagePart,
    Metadata,
)
from novaos_client_sdk.observability import (
    DebugControlResult,
    ObservabilityClient,
    RunEvent,
    RunEventsPage,
)
from novaos_client_sdk.pii import mask_pii, mask_pii_in_dict
from novaos_client_sdk.schema_validation import validate_against_schema
from novaos_client_sdk.utils import match_capabilities, normalize_capability_name

__version__ = "2.0.0"

__all__ = [
    "__version__",
    # Agent Building
    "AgentBuilder",
    "NOVAOS_CONTEXT_METADATA_KEY",
    "NOVAOS_METADATA_CONTENT_TYPE",
    "NOVAOS_METADATA_PART_KEY",
    "NOVAOS_SKILL_METADATA_CONTENT_TYPE",
    "OUTPUT_ARTIFACT_ID",
    "SKILL_ID_METADATA_KEY",
    "A2A_JSONRPC_PATH",
    "JSONRPC_VERSION",
    "METHOD_CANCEL_TASK",
    "METHOD_GET_TASK",
    "METHOD_LIST_TASKS",
    "METHOD_SEND_MESSAGE",
    "METHOD_SEND_STREAMING_MESSAGE",
    "METHOD_SUBSCRIBE_TO_TASK",
    "METHOD_UNAVAILABLE_ERROR_CODES",
    "JsonRpcRequest",
    "INTERRUPT_TASK_STATES",
    "TERMINAL_TASK_STATES",
    "Task",
    "TaskList",
    "TaskState",
    "TaskStatus",
    # A2A push notification config: register a webhook for a
    # task's status changes instead of holding a stream open or polling.
    "METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG",
    "METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS",
    "AuthenticationInfo",
    "TaskPushNotificationConfig",
    "TaskPushNotificationConfigList",
    # A2A streaming frames (SendStreamingMessage / SubscribeToTask)
    "StreamFrame",
    "TaskArtifactUpdateEvent",
    "TaskStatusUpdateEvent",
    "is_last_frame",
    "stream_frame_from_dict",
    # A2A task lifecycle machinery: persistence, in-flight
    # events, and the skill-facing progress API
    "DEFAULT_MAX_TASKS",
    "InMemoryTaskStore",
    "TaskStore",
    "TaskUpdater",
    "EventQueue",
    "TaskEvent",
    # Push notification config persistence
    "DEFAULT_MAX_PUSH_CONFIGS",
    "InMemoryPushNotificationConfigStore",
    "PushNotificationConfigStore",
    # A2A Artifacts (task outputs)
    "ARTIFACT_REVISION_KEY",
    "DEFAULT_ARTIFACT_REVISION",
    "Artifact",
    # A2A Agent Card (discovery document)
    "AGENT_CARD_WELL_KNOWN_PATH",
    "PROTOCOL_BINDING_GRPC",
    "PROTOCOL_BINDING_HTTP_JSON",
    "PROTOCOL_BINDING_JSONRPC",
    "AgentCapabilities",
    "AgentCard",
    "AgentExtension",
    "AgentInterface",
    "AgentProvider",
    "AgentSkill",
    "select_interface",
    # A2A protocol extensions (declaration / negotiation / dispatch)
    "A2A_EXTENSIONS_HEADER",
    "A2AExtension",
    "ExtensionActivation",
    "ExtensionRegistry",
    "extensions_header_value",
    "requested_extension_uris",
    # Sealed-envelope secret delivery (requires [crypto] extra)
    "SEALED_ENVELOPE_SUITE",
    "PodKeyring",
    "RecipientKey",
    "SecretDelivery",
    "open_envelope",
    # The user-context/v1 extension that carries them (same extra)
    "USER_CONTEXT_EXTENSION_URI",
    "SealedUserContext",
    "UserContextExtension",
    # Agent Card JWS signature verification (requires [crypto] extra)
    "DEFAULT_SIGNATURE_ALGORITHMS",
    "canonicalize_agent_card",
    "verify_agent_card_signatures",
    # Logging
    "get_agent_logger",
    # PII Masking
    "mask_pii",
    "mask_pii_in_dict",
    # Structured-extraction schema validation (Ex10, requires [schema] extra)
    "validate_against_schema",
    # Agent Runtime Context and Types
    "AgentExecutionContext",
    "AgentRuntimeContext",
    "ConversationTurn",
    "SkillResult",
    "ToolBindings",
    "ToolResponse",
    "ConnectorTool",
    # Batch ingestion types
    "IngestItem",
    "IngestJobStatus",
    "BatchIngestResponse",
    # Resilience primitives
    "CircuitBreaker",
    # Tracing
    "get_traceparent",
    "extract_trace_id",
    "extract_parent_id",
    "propagate_trace",
    "is_valid_traceparent",
    "inject_trace_headers",
    "traced_tool_call",
    "traced_gateway_call",
    "configure_otel",
    # Client
    "Client",
    "ClientConfig",
    # LLM Gateway Client (optional: requires novaos-agent-sdk[llm])
    "LLMClient",
    # Discovery (A2A Agent Card fetching)
    "DEFAULT_AGENT_CARD_CACHE_TTL",
    "AgentDiscoveryService",
    "fetch_agent_card",
    # DNS
    "AgentDNSClient",
    "AgentDNSRecord",
    "discover_and_connect",
    # Capabilities
    "CapabilityIndex",
    "CapabilityMatcher",
    "MatchResult",
    # Utils
    "normalize_capability_name",
    "match_capabilities",
    # Authentication
    "AuthProvider",
    "BearerTokenAuth",
    "APIKeyAuth",
    "get_api_key_from_env",
    "CredentialService",
    "InMemoryContextCredentialStore",
    "AuthInterceptor",
    # Client-call interception seam
    "ClientCallInterceptor",
    "ClientCallContext",
    "BeforeArgs",
    "AfterArgs",
    # Token Verification (optional: requires novaos-agent-sdk[crypto])
    "JWKSTokenVerifier",
    # Configuration
    "Environment",
    "get_environment",
    "get_dns_url_for_environment",
    "get_dns_url_from_env",
    # Platform Context (execution headers)
    "ExecutionContext",
    # MCP Gateway Client
    "MCPClient",
    "MCPTool",
    "MCPToolResult",
    # Memory Service Client
    "MemoryClient",
    "MemoryItem",
    "MemoryMetadata",
    "QueryResult",
    "WriteResult",
    "ProposalStatus",
    "ForgetResult",
    "HintResult",
    "LiveMessage",
    "AssembleResult",
    # Gateway Observability Client (EventIndex / DebugSession)
    "ObservabilityClient",
    "RunEvent",
    "RunEventsPage",
    "DebugControlResult",
    # agent-ops Heartbeat Schedule Client
    "HeartbeatClient",
    "HeartbeatBudget",
    "HeartbeatSchedule",
    "HeartbeatConfig",
    "HeartbeatConfigCreated",
    "HeartbeatConfigStatus",
    # Execution Gateway Client
    "GatewayClient",
    "ExecuteResult",
    "StreamEvent",
    "TERMINAL_RUN_STATUSES",
    # Models
    "Author",
    "Capability",
    "ContentType",
    "Message",
    "MessagePart",
    "Metadata",
    # Exceptions
    "NovaOSError",
    "NovaOSConnectionError",
    "NovaOSAuthenticationError",
    "MissingExecutionTokenError",
    "NovaOSRateLimitError",
    "NovaOSValidationError",
    "NovaOSAgentNotFoundError",
    "NovaOSServerError",
    "NovaOSDNSError",
    "NovaOSDNSConnectionError",
    "NovaOSDNSAgentNotFoundError",
    "NovaOSDNSRateLimitError",
    # Sealed secret delivery errors — raised by ctx.get_secret(), so a
    # skill author can catch them without reaching into the exceptions module.
    "SealedEnvelopeError",
    "SealedEnvelopeFormatError",
    "RecipientKeyUnavailableError",
    "SecretNotDeliveredError",
    # Agent Card JWS signature verification
    "AgentCardSignatureError",
    "NoAgentCardSignatureError",
    "InvalidAgentCardSignatureError",
    # A2A protocol errors (spec v1.0.1, JSON-RPC codes -32001..-32009)
    "A2AProtocolError",
    "TaskNotFoundError",
    "TaskNotCancelableError",
    "PushNotificationNotSupportedError",
    "UnsupportedOperationError",
    "ContentTypeNotSupportedError",
    "InvalidAgentResponseError",
    "ExtendedAgentCardNotConfiguredError",
    "ExtensionSupportRequiredError",
    "VersionNotSupportedError",
    "A2A_ERROR_TYPES",
    "JSONRPC_CODE_DETAIL_KEY",
    "A2A_ERROR_SPEC_VERSION",
    "a2a_error_for_code",
    "a2a_error_from_json_rpc",
    "error_from_json_rpc",
    "jsonrpc_error_code",
    # Message Models
    "HITLMessage",
    "HITLMessageType",
    "HITLResponse",
    "ErrorMessage",
    "ErrorType",
    "ProgressMessage",
    "ProgressStatus",
    "InputType",
    "InputValidation",
    "InputField",
    "Action",
    "ProgressData",
    "ErrorDetails",
    # A2A message types (spec v1.0.1)
    "A2AMessage",
    "A2A_SPEC_VERSION",
    "TextPart",
    "DataPart",
    "FilePart",
    "FileWithBytes",
    "FileWithUri",
    "A2A_PROTOCOL_VERSION",
    "A2A_VERSION_HEADER",
    "PartKind",
    "Role",
    # Message Utilities
    "MessageBuilder",
    "MessageParser",
    "MessageValidator",
    "NOVAOS_PAUSE_HITL_KEY",
    "NOVAOS_PAUSE_OAUTH_KEY",
    "NOVAOS_PAUSE_WRITE_CONFIRMATION_KEY",
    # Convenience Functions
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
