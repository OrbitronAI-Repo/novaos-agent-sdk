# Agent memory

`ctx.memory` gives an agent durable, tenant-isolated memory backed by NovaOS's Memory Service. There
are two ways to reach it from a skill, and they are not interchangeable:

- **`ctx.memory.client()`** returns a [`MemoryClient`](../reference/api.md#novaos_client_sdk.memory)
  that talks **directly** to Memory Service over `POST /api/v1/mcp`. It exposes the whole memory
  surface — `write`, `query`, `update`, `forget`, `hint`, `assemble_context` — with typed results, retries and
  idempotency keys derived from your execution context. This is the path most of this guide
  documents, and the one to use today.
- **`ctx.tools.memory`** is a [platform tool binding](platform-tools.md) that goes **through the MCP
  Gateway** (`POST {mcp_gateway_url}/mcp/v1/tools/call`, JSON-RPC `tools/call` for
  the six `novaos.memory.*` tools). It trades typed results and retries for Gateway governance:
  policy, audit, metering and visible tool events. See
  [The governed door](#the-governed-door-ctxtoolsmemory).

Whichever door you use, Memory Service enforces its own rules on every call — tenant isolation from
your execution token, the memory scopes your token carries, and the write restrictions below. The
Gateway adds controls on top; it never relaxes them.

## Which door to use

| | `ctx.memory.client()` (`MemoryClient`) | `ctx.tools.memory` (`MemoryTool`) |
|---|---|---|
| Route | Direct to Memory Service | Through the MCP Gateway, then Memory Service |
| Operations | `write`, `query`, `update`, `forget`, `hint`, `assemble_context` | `write`, `query`, `update`, `forget`, `hint`, `assemble` |
| Results | Typed (`WriteResult`, `QueryResult`, `AssembleResult`, ...); raises on failure | Untyped [`ToolResponse`](../reference/api.md#novaos_client_sdk.agent_types) (`success`, `data`, `error`); never raises for a failed call |
| Retries | Built in (429/502/503/504, connection errors) plus a circuit breaker | None |
| Write idempotency | Key derived from `ctx.idempotency_key` when you pass `ctx=ctx` | None |
| Governance | Memory Service's own policy | Gateway OPA policy, audit (`MEMORY_RETRIEVAL_REQUEST`, `TOOL_INVOKED`) and metering (`memory.read` / `memory.write`), then Memory Service's own policy. A `write` to `agent`, `run` or `task` scope needs no human approval; any other scope does ([details](#the-governed-door-ctxtoolsmemory)) |
| Visibility | Not shown as a tool call | `tool.call` / `tool.result` events in the A2A stream on streaming requests |
| Token needs | `memory:*` scopes | `memory:*` scopes **and** `mcp:tools:memory` (or `mcp:tools:*`) |
| Status in this release | Supported | Supported |

Use `MemoryClient` unless you specifically need a memory call to be governed, metered and visible as
a tool call the way every other `ctx.tools` call is.

## Getting a client

```python
async with ctx.memory.client() as memory:
    recalled = await memory.query("agent", "what does this user prefer?")
```

`ctx.memory.client()` resolves your execution token the same way every other platform call does and
raises `MissingExecutionTokenError` if none is available — there's no separate memory-specific auth
setup. It reads the service address from the `MEMORY_SERVICE_URL` environment variable, which the
platform sets on every agent pod. Always use the client as an `async with` block: that is what opens
(and closes) its HTTP connection, and calling a method on a client that was never entered raises
`NovaOSError`.

Use `ctx.memory`, not `ctx.to_execution_context().memory`. `ctx.memory` is created once per
execution and cached, so it is the object the [pre-fetch hook](#pre-fetched-context) writes into;
`to_execution_context()` builds a fresh, empty namespace on every call and never sees pre-fetched
context.

Keyword arguments to `.client()` are forwarded to `MemoryClient`:

```python
async with ctx.memory.client(timeout=10.0, max_retries=2) as memory:
    recalled = await memory.query("run", "scratch notes for this run")
```

## The id table

Memory calls are keyed by ids that already exist on your execution context — you rarely need to pass
them explicitly. The identity Memory Service enforces comes from the claims in your execution token,
not from anything the SDK sends in a request body.

| Field | Where it comes from | Role in memory calls |
|---|---|---|
| `ctx.run_id` | The NovaOS run | `scope="run"` memory belongs to the token's run; also sent as `assemble_context()`'s `request_id`. Don't deduplicate on it — a HITL continuation reuses the same run id. |
| `ctx.idempotency_key` | Platform, per execution | Prefix of the write idempotency key (last row). Identical across retries of one execution, different between executions. |
| `ctx.user_id` | Platform context | `scope="user"` memory belongs to the token's user. |
| `ctx.tenant_id` | Platform context | Every memory is tenant-isolated; the token's tenant claim is what Memory Service enforces. |
| `ctx.context_id` (alias `ctx.session_id`) | A2A `contextId` | Default `conversation_id` for `hint()`. |
| `ctx.task_id` | A2A `Task.id` | Correlates with `run_id` but is not the same field. |
| `ctx.instance_id` | Platform context | Best available proxy for the agent id; there is no separate `agent_id` on the context. `scope="agent"` memory is keyed by the token's agent id. |
| `ctx.trace_id` | W3C `traceparent` | Sent as `assemble_context()`'s `trace_id`. |
| `memory_id` | `WriteResult`, `MemoryItem` | Id of a stored memory. A `prop_` prefix means a non-durable proposal, not a stored memory. |
| `hint_id` | `HintResult` | Id of an enqueued consolidation hint. |
| `retrieval_id` | `QueryResult`, `AssembleResult` | Opaque id identifying one retrieval. |
| `write_event_id` / `delete_event_id` | `WriteResult` / `ForgetResult` | Audit-trail event ids. |
| Write idempotency key | `f"{ctx.idempotency_key}:{ordinal}"` | Derived per write when you pass `ctx=ctx` — see [Writing memory](#writing-memory). |

`run_id` and `task_id` (like `session_id` and `context_id`) correlate but are deliberately not the
same field: a NovaOS run is a platform record with its own lifecycle, while an A2A task is a protocol
object owned by whichever agent serves the request.

## Writing memory

```python
async with ctx.memory.client() as memory:
    result = await memory.write(
        "agent",
        "User prefers concise summaries.",
        tags=["preferences"],
        ctx=ctx,
    )

if not result.durable:
    ctx.logger.info("Memory queued for review", extra={"proposal_id": result.memory_id})
```

- Agents can write `agent` and `run` scope only. Writing `user` or `tenant` scope fails with a
  `NovaOSServerError`: 403 when your token lacks `memory:write:user` / `memory:write:tenant` (the
  normal case — agent tokens don't carry them), or 422 from Memory Service's own policy even if it
  does. The SDK can't override either. To get a fact into user memory, submit a
  [hint](#reading-memory) and let consolidation decide.
- `scope="run"` writes are stored immediately and expire after a server-configured TTL (5 minutes
  by default); they are scratch space for one run, not durable memory.
- `scope="agent"` writes are **proposals**. Memory Service queues them for its agent write gate
  (PII and secret scanning, then a durable commit) and answers straight away with a `memory_id`
  prefixed `prop_` and `result.durable == False`. The committed memory gets a different id, and the
  gate can reject a proposal with no signal back to the caller — so don't pass a `prop_` id to
  `forget()`.
- Pass `ctx=ctx` and the idempotency key defaults to `f"{ctx.idempotency_key}:{ordinal}"`, where
  `ordinal` is a per-execution counter stored on `ctx` itself (not on the `MemoryClient` instance —
  you typically open a fresh client per block, so a client-instance counter would restart at 1 every
  time and every write in one run would collide on the same key). Repeated writes within one
  execution get distinct, stable keys, and a retried execution that makes the same writes in the
  same order replays the same keys (deduplicated for 24 hours) instead of writing twice. Pass `idempotency_key=` explicitly to override.

```python
from novaos_client_sdk import NovaOSServerError

async with ctx.memory.client() as memory:
    try:
        await memory.write("user", "User is based in Lisbon.", ctx=ctx)
    except NovaOSServerError as exc:
        assert exc.status_code in (403, 422)
        await memory.hint("User mentioned they are based in Lisbon.", ctx=ctx)
```

### Updating a memory in place

`update()` edits one of your own `agent`-scope memories without writing a new one. It applies
immediately — no proposal — so the next `query()` returns the new content. Read the memory's
`version` from `query()` and pass it back; if the memory changed in between, the call fails with a
409 `NovaOSServerError` and you re-read and retry.

The call is `await memory.update("agent", item.memory_id, item.version, content="...")`, where
`item` is a `MemoryItem` from `memory.query(...)`. The returned `UpdateResult.version` is the
version to pass on the next update.

- Only `agent` scope can be updated. `user` and `tenant` are refused (422) for agent principals, and
  `run` / `task` memories are short-lived scratch: `forget()` and write a new one.
- Pass the committed `memory_id` from `query()`, not a `prop_` proposal id.
- New `content` goes through the same PII and secret screens as a write and is rejected (422) on a
  hit. `tags`, `confidence` and `custom` replace the stored values; other metadata is not
  changeable.
- `write()` also takes `ttl_seconds` (the service enforces its own minimum and per-scope maximum)
  and `classification`.

## Reading memory

```python
async with ctx.memory.client() as memory:
    recalled = await memory.query("agent", "what does this user prefer?", top_k=5)
    hinted = await memory.hint("User asked for weekly summaries.", ctx=ctx)
    assembled = await memory.assemble_context("what does this user prefer?", ctx=ctx)

for item in recalled.items:
    ctx.logger.info("Recalled", extra={"memory_id": item.memory_id, "score": item.score})
```

- `query(scope, query, *, top_k=10, filters=None, memory_type=None, conversation_id=None)`
  searches one scope. Agents can query `user`,
  `agent` and `run`; `tenant` needs `memory:read:tenant`, which agent tokens don't normally carry.
  `filters` accepts `tags`, `after`, `before` and `classification`. `memory_type` and
  `conversation_id` are shortcuts for the same two keys in `filters`; see
  [Typed memories and conversation recall](#typed-memories-and-conversation-recall).
- `hint(hint, *, conversation_id=None, ctx=None)` submits an observation for background
  consolidation into user memory. It needs a `conversation_id`: pass `ctx=ctx` and it defaults
  from `ctx.context_id`; with neither, the call raises `NovaOSValidationError` before any request
  is sent. `hinted.status == "accepted"` means the hint was durably enqueued — not that a fact will
  result from it.
- `assemble_context(query, *, top_k=10, live_message_limit=20, token_budget=None,
  latency_budget_ms=None, memory_type=None, conversation_id=None, ctx=None)` fans out to every
  source your token can read — user and agent memories, the live conversation, run data — in one
  call. Live messages are scoped from your token's claims, not from the `conversation_id`
  argument, which is an opt-in recall filter over durable memories (below). With `ctx=ctx`,
  `request_id` and `trace_id` default from `ctx.run_id` and `ctx.trace_id`.

### Typed memories and conversation recall

Both are opt-in. Leave them out and every call behaves exactly as before.

- **`memory_type`** is an optional label on a memory: `"semantic"`, `"episodic"` or
  `"procedural"` (anything else raises `ValueError`, and the service answers 422). Set it on
  `write(..., memory_type="episodic")`. Query with `memory_type="episodic"` to get only memories
  written with that type — every match up to `top_k`, because the service filters before it ranks.
  A memory written without a type is never returned by a typed query. `run` and `task` scope store
  and match the type by exact comparison.
- **`conversation_id`** on `query` and `assemble_context` recalls durable memories written under
  that conversation, ranked by similarity to your query text, so something saved early in a long
  conversation is still found after many later turns. It applies to `user`, `agent` and `tenant`
  scope and is ignored for `run` and `task`, which are plain key-value scratch space.
- **`item.score`** is the similarity of a durable memory to your query (0 to 1). It is always
  `None` for `run` and `task` items: those are read from Redis, not similarity-ranked.

For example, `await memory.query("agent", "how do we deploy?", memory_type="procedural")` returns
only procedural memories, and `await memory.query("user", "invoice currency",
conversation_id=ctx.context_id)` recalls what was saved under that conversation.

`forget()` deletes one stored memory by id:

```python
async with ctx.memory.client() as memory:
    note = await memory.write("run", "Draft outline, superseded.", ctx=ctx)
    removed = await memory.forget("run", note.memory_id, reason="superseded by final outline")

assert removed.deleted_count == 1
```

Agents can forget `agent` and `run` scope memories. `reason` (1–500 characters) is stored in the
audit trail.

## Pre-fetched context

An agent can ask the platform to assemble memory context **before** each turn, so a skill starts with
relevant memories already in hand instead of spending its first call fetching them.

**Opt in** by declaring `memory` in `permissions.tools` in your agent's catalog manifest. The platform
then attaches a pre-fetch budget to every request for that agent (`token_budget` 4000 and
`latency_budget_ms` 800 by default), readable as `ctx.memory_budget` — `None` when the agent hasn't
opted in.

When a budget is present, `AgentBuilder` runs one pre-fetch per turn, before your skill handler
starts:

- It calls `assemble_context(<the turn's input text>, token_budget=..., latency_budget_ms=...,
  ctx=ctx)` on a `MemoryClient` — directly to Memory Service, not through the Gateway. So the
  pre-fetch is not metered, audited or HITL-gated by the Gateway; Memory Service's own audit of
  the assemble call still applies.
- It runs outside your skill, never inside it, so a retried or resumed turn cannot fetch twice.
- On success it sets `ctx.memory.context` to the
  [`AssembleResult`](../reference/api.md#novaos_client_sdk.memory) — including a result with
  `partial=True` — and emits a `memory.assembled` platform event carrying `retrieval_id`, `partial`,
  `sources_consulted` and `sources_skipped`. On a streaming request the event arrives as a `WORKING`
  status frame; on a non-streaming request it is folded into the terminal message's platform
  metadata part.
- Any failure — no token, service unreachable, request rejected — is logged and swallowed. The turn
  never fails because of it; `ctx.memory.context` simply stays `None`.
- Of the two budget fields, only `latency_budget_ms` is enforced: Memory Service applies it as one
  deadline shared by every source it fans out to, and a source that misses it is dropped from the
  result with `partial=True`. `token_budget` is accepted and forwarded but not enforced yet — don't
  rely on it to bound the size of `ctx.memory.context`.
- The pre-fetch has no client-side deadline of its own. The HTTP call uses `MemoryClient`'s
  defaults (30 s timeout, up to 3 attempts), so a Memory Service that is unreachable rather than
  slow can delay the start of your skill by more than `latency_budget_ms`.

Always treat `ctx.memory.context` as optional — `None` is the normal state for an agent that hasn't
opted in, and a possible one for an agent that has:

```python
from novaos_client_sdk import AgentBuilder, SkillResult

agent = AgentBuilder("support-agent")


@agent.skill("answer")
async def answer(ctx, user_input):
    context = ctx.memory.context
    if context is None:
        async with ctx.memory.client() as memory:
            context = await memory.assemble_context(user_input, ctx=ctx)

    if context.partial:
        ctx.logger.warning("Memory context is incomplete", extra={"skipped": context.sources_skipped})

    notes = [item.content for item in context.agent_memories + context.durable_memories]
    return SkillResult(output=f"Found {len(notes)} relevant memories.")
```

## Reading degradation state

Every query/assemble result reports whether it's complete, rather than silently returning partial
data indistinguishable from "nothing found":

| Field | Type | Meaning |
|---|---|---|
| `QueryResult.partial` | `bool` | `True` only if a store failed reaching or querying — not a proxy for zero matches |
| `QueryResult.stores_queried` | `list[str]` | Which stores were actually consulted |
| `AssembleResult.partial` | `bool` | Same meaning, across every source `assemble_context` fans out to |
| `AssembleResult.sources_consulted` / `.sources_skipped` | `list[str]` | Skipped sources are missing capabilities/ids, not failures |

```python
async with ctx.memory.client() as memory:
    recalled = await memory.query("agent", "deployment preferences")

if recalled.partial:
    ctx.logger.warning("Recall incomplete", extra={"stores": recalled.stores_queried})
elif not recalled.items:
    ctx.logger.info("No stored memories match")
```

Always check `partial` before treating an empty result as "no memories exist" versus "the query
didn't fully succeed."

## Retries

`MemoryClient` retries 429/502/503/504 and connection errors with exponential backoff
(`max_retries=3` total attempts by default; `max_retries=0` still makes one attempt) and trips a
circuit breaker after repeated failures (`circuit_breaker_threshold=5`, resetting after 30 s). You
don't need your own retry loop around it. `ctx.tools.memory` does not retry.

## The governed door: `ctx.tools.memory`

`ctx.tools.memory` has one method per Memory Service tool, each returning a
[`ToolResponse`](../reference/api.md#novaos_client_sdk.agent_types) instead of raising:

- `write(scope, content, metadata=None, *, memory_type=None, ttl_seconds=None, classification=None, timeout_s=None)`
  calls `novaos.memory.write`. `metadata` accepts `tags`, `source`, `confidence` and `custom`;
  `memory_type` is added to it.
- `query(scope, query, top_k=10, filters=None, *, memory_type=None, conversation_id=None, token_budget=None, latency_budget_ms=None, timeout_s=None)`
  calls `novaos.memory.query`, sending `top_k` as the tool's `budget.top_k`.
- `update(scope, memory_id, version, content=None, metadata=None, *, timeout_s=None)` calls
  `novaos.memory.update` ([details](#updating-a-memory-in-place)); `metadata` accepts `tags`,
  `confidence` and `custom`.
- `forget(scope, memory_id, reason="agent-requested", *, timeout_s=None)` calls
  `novaos.memory.forget`.
- `hint(hint, conversation_id, priority="normal", tags=None, context=None, *, timeout_s=None)` calls
  `novaos.memory.hint`.
- `assemble(query, top_k=10, live_message_limit=20, token_budget=None, latency_budget_ms=None, *, memory_type=None, conversation_id=None, timeout_s=None)`
  calls `novaos.memory.assemble`.

`tests/test_memory_contract.py` compares these signatures and `MemoryClient`'s with Memory Service's
tool schemas, so a field one side accepts cannot go missing from the other.

<!-- doctest: skip reason="needs an MCP Gateway; run against a mocked Gateway by tests/docs/test_memory_guide_tool_samples.py" -->
```python
written = await ctx.tools.memory.write(
    "agent",
    "User prefers concise summaries.",
    metadata={"tags": ["preferences"], "source": "support-chat"},
)
if not written.success:
    ctx.logger.warning("Governed memory write failed", extra={"error": written.error})

recalled = await ctx.tools.memory.query("agent", "what does this user prefer?", top_k=5)
if recalled.success:
    ctx.logger.info("Governed recall", extra={"result": recalled.data})
```

What going through the Gateway adds:

- **Policy.** The Gateway evaluates OPA policy on every call. Your execution token must carry
  `mcp:tools:memory` (or `mcp:tools:*`) **in addition to** the `memory:*` scopes Memory Service
  checks. Declaring `permissions.tools: ["memory"]` in your manifest turns on
  [pre-fetch](#pre-fetched-context); it does **not** grant that scope. A missing grant surfaces as
  `error == "Tool access denied: novaos.memory.<tool> (category mismatch)"`.
- **Human approval.** A `write` to `agent`, `run` or `task` scope needs no human approval: it runs
  on its own and is metered as `memory.write`. A `write` to `user`, `tenant` or `platform` scope, or
  with a missing or unrecognized scope, needs an approval bound to that call. Without one the call
  returns `success == False` with `error == "Human approval required before this tool can execute"`.
  Underneath, the Gateway answered `-32042 HITL_REQUIRED` for action class `mcp.tools.call.write`;
  the SDK keeps only the message. An agent can't write `user` or `tenant` scope at all (Memory
  Service rejects it), so in practice an approval doesn't turn those into successful writes.
  `query` is a read and never needs approval. Operators: the no-approval path needs OPA policy
  revision `s17-od02-v2` or later; on a v1 policy, agent-scope writes are denied outright.
- **Audit and metering.** Each call is audited (`MEMORY_RETRIEVAL_REQUEST` / `TOOL_INVOKED`) and
  metered as `memory.read` or `memory.write`.
- **Visibility.** On a streaming request every call emits a `tool.call` / `tool.result` pair into the
  A2A stream, like every other `ctx.tools` call. Non-streaming requests emit nothing.

What it doesn't have: typed results, retries, and ctx-derived idempotency keys. `data` on a successful response is the Gateway's raw MCP tool result.

## Consistency guarantees

Consistency is a property of Memory Service's write path, so it is the same whichever door a write
came through. What you can and can't assume about when a write becomes visible to a later `query()`
or `assemble_context()`:

| Path | Scope | Guarantee | Notes |
|---|---|---|---|
| Direct write (`write()`) | `run` | **Read-your-write.** A `write()` that returns is visible to the next `query()` in that run. | Stored before the response is sent. |
| Direct write (`write()`) | `agent` | **Eventually consistent, on an unmeasured timeline.** | The write is a `prop_` proposal queued for the agent write gate; it becomes queryable once the gate commits it, and the gate may reject it. |
| Consolidated write (via `hint()`) | `user` | **Eventually consistent, on an unmeasured timeline.** | `hint()` only enqueues a hint for later processing; there is no guarantee about when — or whether a specific hint even survives extraction as a fact. |

**There is currently no published bound ("N") for how long a consolidated fact takes to become
recallable.** The consolidation pipeline polls for pending hints on a fixed interval (server-side
config, currently 20 seconds), but that interval is the pipeline's poll cadence, not an end-to-end
SLA — it doesn't account for extraction latency, retry/backoff on a failed extraction, batching, or
per-tenant fair-share throttling under load. A measured N will be published here once it exists.
Until then:

- Don't assume a `hint()` call's resulting facts — or an agent-scope proposal — are recallable by
  any specific deadline. Poll `query()` if your caller genuinely needs to know when a memory
  landed, rather than waiting a fixed duration and assuming success.
- Do rely on read-your-write for `run` scope.
- A hint being `accepted` means it was durably enqueued, not that extraction succeeded or that any
  fact will result from it — a hint can be dropped by extraction (e.g. nothing worth remembering in
  the text) with no separate signal back to the caller.

```python
async with ctx.memory.client() as memory:
    await memory.write("run", "Customer chose the annual plan.", ctx=ctx)
    recalled = await memory.query("run", "which plan did the customer choose?")

assert any(item.content == "Customer chose the annual plan." for item in recalled.items)
```

## Removed surfaces

- **OpenMemory is gone.** Memory Service no longer calls out to a separately-deployed OpenMemory
  service for vector search — it runs in-process against NovaOS's own Postgres/pgvector store. This
  is transparent to `MemoryClient` callers; nothing in this SDK changes because of it.
- **`ctx.tools.memory` was removed in `2.0.0` and is back.** The `2.0.0`-era binding pointed at
  Gateway tools with no backend and failed `tool not found` everywhere. The binding documented
  [above](#the-governed-door-ctxtoolsmemory) is new, targets real `novaos.memory.*` Gateway tools,
  and offers one method per `novaos.memory.*` tool: `write`, `query`, `update`, `forget`, `hint`
  and `assemble`.

See the full reference at [`novaos_client_sdk.memory`](../reference/api.md#novaos_client_sdk.memory).
