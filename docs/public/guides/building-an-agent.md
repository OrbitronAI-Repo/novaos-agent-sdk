# Building an agent

An agent built on this SDK is an [`AgentBuilder`](../reference/api.md#novaos_client_sdk.agent_builder) with one or more registered **skills**, mounted onto an ASGI app. The SDK speaks [A2A](../concepts/a2a-primer.md) — request parsing, task lifecycle, streaming, and response shaping are handled for you; your skill functions contain the actual logic.

```python
from novaos_client_sdk import A2A_JSONRPC_PATH, AgentBuilder, AgentExecutionContext, SkillResult
from fastapi import FastAPI

agent = AgentBuilder(name="my-agent", version="1.0.0")

@agent.skill("greet", description="Greet a user")
async def greet(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
    return SkillResult(output=f"Hello, {input_text}!")

app = FastAPI()
app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())
```

`asgi_endpoint()` returns a ready-made route handler that decodes the body, dispatches `SendMessage`/`SendStreamingMessage`/`GetTask`/`CancelTask`/etc., and returns a `JSONResponse` or a streaming SSE `Response` as appropriate — there is nothing else to wire up. It requires Starlette (FastAPI pulls it in transitively).

## Skills

`@agent.skill(name, description, *, id=None, tags=None, input_modes=None, output_modes=None)` registers a handler. `id` defaults to `name` and is the routing key a caller sends to pick this skill; `tags`/`input_modes`/`output_modes` populate the corresponding fields on the published `AgentSkill`.

An agent with a single skill routes every request to it automatically. A **multi-skill** agent requires the caller to name one — via the message's `skillId` metadata, or `config.skill` in the platform context — otherwise the request is rejected outright (`TASK_STATE_REJECTED`) rather than guessed at.

## `SkillResult` and task state

A skill returns a `SkillResult(output, status="COMPLETED", error=None, metadata={})`. `status` is translated to an A2A task state:

| `SkillResult.status` | A2A `TaskState` |
|---|---|
| `COMPLETED` (default) | `TASK_STATE_COMPLETED` |
| `FAILED` | `TASK_STATE_FAILED` |
| `WAITING_USER` | `TASK_STATE_INPUT_REQUIRED` |
| `REJECTED` | `TASK_STATE_REJECTED` |
| `AUTH_REQUIRED` | `TASK_STATE_AUTH_REQUIRED` |

`REJECTED` means "this was never going to work" (not worth retrying unchanged), distinct from `FAILED` ("the agent tried and could not"). `AUTH_REQUIRED` means "go complete an out-of-band step" (visit a URL, approve a write) — distinct from `WAITING_USER`, which means "answer a question." A caller cannot resolve `AUTH_REQUIRED` by simply replying with text.

The legacy spellings `QUEUED`, `RUNNING`, `IN_PROGRESS`, and `ERROR` are also accepted, mapping to `SUBMITTED`, `WORKING`, `WORKING`, and `FAILED` respectively — any other value fails the task with the bad value quoted in the status message. `metadata` is serialized as a second output part alongside your text, so a caller can read structured data without parsing it out.

There is deliberately no `CANCELED` member: cancellation is always the outcome of an external `CancelTask` call, never a value a skill returns.

## Pausing: HITL, OAuth, and write-confirmation

A skill can pause a task instead of finishing it. Three kinds of pause exist, each with its own [`MessageBuilder`](../reference/api.md#novaos_client_sdk.message_builder) factory:

| Pause | Task state | Factory |
|---|---|---|
| Human input / confirmation | `INPUT_REQUIRED` | `MessageBuilder.create_hitl_message(...)` |
| Connector OAuth | `AUTH_REQUIRED` | `MessageBuilder.create_oauth_required_message(...)` |
| Destructive-write confirmation | `INPUT_REQUIRED` | `MessageBuilder.create_write_confirmation_required_message(...)` |

Build the message, publish it through `ctx.task_updater`, and return a matching `SkillResult`:

```python
from novaos_client_sdk import MessageBuilder

@agent.skill("connect-gmail")
async def connect_gmail(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
    auth_url = await start_oauth_flow()
    message = MessageBuilder.create_oauth_required_message(
        content="Connect your Gmail account to continue.",
        connector="gmail",
        auth_url=auth_url,
    )
    await ctx.task_updater.requires_auth(message)
    return SkillResult(output="Connect your Gmail account to continue.", status="AUTH_REQUIRED")
```

For a write-confirmation pause, always pass the tool call's real `call_id` — the platform binds the resume token to it, so a placeholder turns a scoped confirmation into a run failure rather than a blanket write authorization.

**Resuming a paused task.** A2A v1.0 defines no dedicated resume RPC: the caller sends a follow-up `SendMessage` carrying the same `task_id`, which your agent receives as a normal request. See [Invoking agents](invoking-agents.md) for the calling side.

## The execution context (`ctx`)

Every skill receives an [`AgentExecutionContext`](../reference/api.md#novaos_client_sdk.agent_context), built from the inbound A2A request and headers:

- `ctx.tools` — governed access to platform services; see [Platform tools](platform-tools.md).
- `ctx.config` — agent-level configuration passed by the caller.
- `ctx.history` — earlier turns of the conversation, oldest first, as `ConversationTurn(role, content)`; empty for a single-shot invocation.
- `ctx.get_secret(name, default=...)` — opens a sealed secret delivered with this request; raises if undeclared and no default is given.
- `ctx.idempotency_key` — stable across retries of one logical execution; agent execution is **at-least-once**, so a skill with non-idempotent side effects should dedupe on this key, never on `ctx.run_id`.
- `ctx.is_cancelled` / `ctx.raise_if_cancelled()` — check inside synchronous work between awaits; an `await` is already interrupted automatically once `CancelTask` arrives.
- `ctx.logger` — see [Logging](#logging) below.

## `to_agent_card()`

Agent pods are never internet-reachable, so a pod never serves its own Agent Card — `to_agent_card()` returns the content for whoever publishes it on your behalf (the platform's execution gateway, or the scaffold at build time). Every skill you registered is carried through under its own `id`, so a card built here can never advertise a `skillId` the agent would reject at request time. Fields like `capabilities.streaming` and `capabilities.pushNotifications` are derived from how the builder was constructed (whether a `push_notification_store` was supplied), not restated by hand — they cannot drift from what the pod actually serves. `description`, `provider`, `documentation_url`, and the reachable `supported_interfaces` are things only the publisher knows, so they're passed in as arguments.

## Extensions

`AgentBuilder(extensions=[...])` or the incremental `agent.register_extension(ext)` declares which [A2A protocol extensions](../concepts/a2a-primer.md) this agent understands — for example, `UserContextExtension()` to receive sealed secrets via `ctx.get_secret()`. A skill reads what was actually negotiated for its request through `ctx.extensions`, independent of what the agent declares in general.

## Logging

`ctx.logger` is a structured JSON logger pre-bound to this execution's `run_id`, `agent_id`, and `tenant_id` — every line is filterable by run without threading those fields through by hand:

```python
ctx.logger.info("Processing request", extra={"input_length": len(input_text)})
```

Outside a request handler (startup code, background tasks), call `get_agent_logger` directly with the same fields. Both write newline-delimited JSON to stdout.
