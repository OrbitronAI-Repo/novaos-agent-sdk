# Error handling

## Exception hierarchy

Every exception the SDK raises inherits from [`NovaOSError`](../reference/api.md#novaos_client_sdk.exceptions). The root is deliberately **protocol-neutral**, not `A2AError`: most SDK modules (`memory`, `mcp`, `llm`, `heartbeat`, `observability`, `gateway`, `dns`) are plain NovaOS platform HTTP clients that never speak an agent wire protocol — only `client`/`discovery` do. A2A-specific errors get their own narrow subtree, `A2AProtocolError`, reserved for genuine A2A JSON-RPC errors rather than generalized to every SDK failure.

```
NovaOSError (base)
├── NovaOSConnectionError            (retryable)
├── NovaOSAuthenticationError
│   └── MissingExecutionTokenError
├── NovaOSRateLimitError             (retryable)
├── NovaOSValidationError            (field, reason)
├── NovaOSAgentNotFoundError         (agent_name)
├── NovaOSServerError                (status_code; retryable for 429/502/503/504)
├── NovaOSDNSError
│   ├── NovaOSDNSConnectionError     (retryable)
│   ├── NovaOSDNSRateLimitError      (retryable)
│   └── NovaOSDNSAgentNotFoundError  (agent_name)
├── SealedEnvelopeError
│   ├── SealedEnvelopeFormatError
│   └── RecipientKeyUnavailableError
├── SecretNotDeliveredError
├── AgentCardSignatureError
│   ├── NoAgentCardSignatureError
│   └── InvalidAgentCardSignatureError
└── A2AProtocolError                 (error_code, task_id)
    ├── TaskNotFoundError                       (-32001)
    ├── TaskNotCancelableError                  (-32002)
    ├── PushNotificationNotSupportedError        (-32003)
    ├── UnsupportedOperationError                (-32004)
    ├── ContentTypeNotSupportedError              (-32005)
    ├── InvalidAgentResponseError                 (-32006)
    ├── ExtendedAgentCardNotConfiguredError        (-32007)
    ├── ExtensionSupportRequiredError              (-32008)
    └── VersionNotSupportedError                   (-32009)
```

This is the 2.x renamed successor of an older `ACP*`-prefixed hierarchy with the same shape but no A2A subtree — see [Migrating from 1.x](migrating-from-1x.md) if you're porting `except ACP...` handlers.

Every instance carries `message`, `retry_after: float | None`, and `details: dict`, plus `is_retryable: bool` (`True` for connection errors, rate limits, and server errors with a retryable status code). Subclass extras — `field`/`reason`, `status_code`, `agent_name`, `error_code`/`task_id` — are **keyword-only**, so `NovaOSValidationError("m", 1.5)` can't be mistaken for a positional `retry_after` the way an older `ACPValidationError` accepted.

```python
try:
    task = await client.send_message(message)
except NovaOSError as e:
    if e.is_retryable:
        await asyncio.sleep(e.retry_after or 1.0)
        # retry
    else:
        raise
```

## Handling specific failures

```python
from novaos_client_sdk import (
    NovaOSAgentNotFoundError,
    NovaOSConnectionError,
    NovaOSAuthenticationError,
    NovaOSRateLimitError,
)

try:
    task = await client.send_message(message, skill_id="screen")
except NovaOSAgentNotFoundError as e:
    print(f"Agent not found: {e.agent_name}")
except NovaOSConnectionError:
    print("Connection failed — retrying")
except NovaOSAuthenticationError:
    print("Auth failed — refresh the token and retry")
except NovaOSRateLimitError as e:
    print(f"Rate limited — wait {e.retry_after}s")
```

`NovaOSValidationError` additionally carries `field` and `reason`; `NovaOSServerError` carries `status_code`. See [Authentication](authentication.md) for `MissingExecutionTokenError`.

## Graceful degradation with `send_message_safe`

```python
task = await client.send_message_safe(message, default=None)
if task is None:
    print("Execution failed, handled gracefully")
```

Only `NovaOSAgentNotFoundError` and `NovaOSConnectionError` are caught this way. Everything else — **including `A2AProtocolError`** — still propagates: an agent that answered `-32001` was reached and had something to say, and swallowing that would turn a diagnosable failure into an empty result.

## A2A protocol errors

`A2AProtocolError` means a remote agent replied with one of the A2A spec's own JSON-RPC error codes (spec `1.0.1`), not that the HTTP call itself failed. `TaskNotCancelableError`, `UnsupportedOperationError`, and the rest above each set `error_code` on the class, so you can catch by type or branch on the code:

```python
from novaos_client_sdk import TaskNotCancelableError, jsonrpc_error_code

try:
    await client.cancel_task(task_id)
except TaskNotCancelableError:
    print("Task already reached a terminal state")
except A2AProtocolError as e:
    print(f"A2A error {jsonrpc_error_code(e)}: {e.message}")
```

`a2a_error_for_code(code)` and `jsonrpc_error_code(error)` round-trip between an exception class and a wire code; an unrecognized code falls back to the base `A2AProtocolError` instead of raising, so a peer on a newer spec revision degrades gracefully. `error_from_json_rpc(error)` builds the right exception from either the A2A codes above or the standard JSON-RPC transport codes (`-32700`…`-32603`), which land as `NovaOSValidationError`/`NovaOSServerError` — a malformed request is not an A2A protocol failure.

## DNS-specific errors

Discovery failures (`AgentDNSClient`, `discover_and_connect`) raise `NovaOSDNSError` and its subtypes rather than the platform hierarchy, since a DNS lookup and an agent call are different trust boundaries:

```python
from novaos_client_sdk import NovaOSDNSConnectionError, NovaOSDNSAgentNotFoundError

try:
    record = await dns_client.get_agent("my-agent")
except NovaOSDNSConnectionError:
    print("DNS service unavailable")
except NovaOSDNSAgentNotFoundError as e:
    print(f"Not found in DNS: {e.agent_name}")
```

## Retries, the circuit breaker, and `ctx.tools`

`MemoryClient` and `ctx.tools.knowledge` use the same `CircuitBreaker` class, each with its own instance (one tripping does not open the other): CLOSED lets requests through; five consecutive failures trip it OPEN for 30s (fail-fast, no calls out); a half-open probe then decides whether to close again or re-open, with exponential backoff (`backoff_delay`, base 1s, capped at 16s, jittered) applied between attempts.

Platform tool calls (see [Platform tools](platform-tools.md)) never raise for a failed call, even after retries are exhausted — they return a `ToolResponse` with `success=False` and an `error` message instead:

```python
result = await ctx.tools.knowledge.search(query)
if not result.success:
    ctx.logger.error("Search failed", extra={"error": result.error})
    return SkillResult(output=None, status="FAILED", error=result.error)
```

## Streaming errors

`send_streaming_message` yields A2A `StreamResponse` frames, not a dedicated error-frame type. A protocol failure (an unsupported operation, a card that overclaims streaming support) raises `A2AProtocolError` out of the iterator instead of being yielded; a well-formed stream that never reaches a terminal or interrupt state is itself a transport failure — check with `is_last_frame`, since A2A dropped ACP's `final` flag:

```python
from novaos_client_sdk import A2AProtocolError, is_last_frame

try:
    async for frame in client.send_streaming_message(message):
        ...
except A2AProtocolError as e:
    print(f"Stream error: {e.message}")
```

## The standardized `ErrorMessage`

Agents signal a structured error back to a caller with `MessageBuilder.create_error_message(content, error_type, ...)`, which embeds an `ErrorMessage` (`error_type: ErrorType`, `message`, `details`, `recovery_suggestions`, `retryable`) as a metadata part on the reply. `ErrorType` covers `validation_error`, `connection_error`, `not_found_error`, `permission_error`, `execution_error`, and `unexpected_error`:

```python
return MessageBuilder.create_error_message(
    "Could not reach the upstream service",
    error_type=ErrorType.CONNECTION_ERROR,
    retryable=True,
)
```
