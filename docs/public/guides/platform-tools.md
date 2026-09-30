# Platform tools

Inside an agent skill, `ctx.tools` gives you governed access to platform services — every call is authenticated with the current run's token, scoped to the current tenant, and routed through the MCP Gateway. You never construct or manage these bindings yourself; [`AgentExecutionContext`](../reference/api.md#novaos_client_sdk.agent_context) builds `ctx.tools` for you from the inbound request.

## What's available

[`ToolBindings`](../reference/api.md#novaos_client_sdk.tools) exposes one attribute per tool category:

| Binding | Purpose |
|---|---|
| `ctx.tools.knowledge` | RAG search, document ingestion, GraphRAG queries |
| `ctx.tools.agents` | Call another agent, or discover agents by capability |
| `ctx.tools.hitl` | Request human approval |
| `ctx.tools.files` | Scoped file upload/download/list |
| `ctx.tools.memory` | Governed memory `write` / `query` (see [Agent memory](memory.md#the-governed-door-ctxtoolsmemory)) |
| `ctx.tools.notify` | Send a notification (email, Slack, ...) |
| `ctx.tools.http` | Governed outbound HTTP (domain-allowlisted) |
| `ctx.tools.browser` | Browser automation (navigate, screenshot, extract) |
| `ctx.tools.code` | Sandboxed code execution |
| `ctx.tools.connectors` | Generic third-party connector invocation (ClickUp, Slack, ...) |

For memory, `ctx.tools.memory` covers only governed `write` and `query`; the full memory surface (`forget`, `hint`, `assemble_context`, typed results, retries) is on `ctx.memory.client()`, which talks to Memory Service directly rather than through the Gateway. [Agent memory](memory.md#which-door-to-use) compares the two.

Every call returns a [`ToolResponse`](../reference/api.md#novaos_client_sdk.agent_types) — `success: bool`, `data: Any`, `error: str | None` — rather than raising for a failed tool call, so a skill can inspect and handle a failure inline:

```python
result = await ctx.tools.knowledge.search("compliance regulations")
if result.success:
    print(result.data)
else:
    ctx.logger.error("Knowledge search failed", extra={"error": result.error})
```

## Timeouts

Every tool call accepts a `timeout_s` override — for example `ctx.tools.files.upload(filename, content, *, timeout_s=None)`, the same pattern on every method across every binding. Left unset, it falls back to a default of **128 seconds**, deliberately set *above* the MCP Gateway's own upstream ceilings (up to 120s for built-in tools, 30s by default for manifest-registered ones) so a slow call surfaces the Gateway's own, more informative timeout error instead of a generic client-side one:

```python
result = await ctx.tools.knowledge.ingest(large_document, timeout_s=180)
```

Configure the instance-wide default with the `NOVAOS_TOOL_TIMEOUT_S` environment variable, or per-binding via `ToolBindings(default_timeout_s=...)`. There is no single Gateway-wide timeout to validate against — it varies by dispatch path — so an opt-in floor check (`gateway_timeout_s`) is available if you know the ceiling for the tools you're calling, but it's off by default; a shorter effective timeout than the floor raises `ValueError` before any network call is made.

## Knowledge search and ingestion

`ctx.tools.knowledge` retries transient failures (HTTP 429/502/503/504, network timeouts) automatically with exponential backoff and a shared circuit breaker:

```python
# Search
result = await ctx.tools.knowledge.search("compliance regulations", limit=5)

# Ingest and wait for completion
ingest_result = await ctx.tools.knowledge.ingest(document_text)
final = await ctx.tools.knowledge.poll_until_complete(ingest_result.data["job_id"])

# Batch ingest with bounded concurrency
from novaos_client_sdk import IngestItem

batch = await ctx.tools.knowledge.ingest_batch([
    IngestItem(content="doc one"),
    IngestItem(content="doc two"),
])
final_batch = await ctx.tools.knowledge.poll_batch_until_complete(batch)
```

GraphRAG (entity-graph) search follows the same build-then-query shape: `build_graph(collection_id)` kicks off indexing, `get_graph_status` / `poll_graph_build_until_complete` observe it, and `query_graph_local` / `query_graph_global` answer entity-centric vs. corpus-wide questions once the graph is ready.

## Calling connectors

`ctx.tools.connectors.call(connector_id, operation, params)` invokes a third-party integration (ClickUp, Slack, Gmail, ...) that doesn't have a bespoke binding of its own:

```python
result = await ctx.tools.connectors.call(
    "clickup",
    "create_task",
    params={"list_id": "123", "name": "Follow up"},
)
```

`params` is forwarded verbatim as the connector's request body, capped at 256 KiB client-side — check the specific connector's documentation for its expected shape. Pass `params_schema` (a JSON Schema, requires the `schema` extra) for client-side validation before the call goes out. Unlike `knowledge`, connector calls are **not** retried automatically — many connector operations (create a task, send a message) aren't safe to retry blindly.

Most connectors are `tier="standard"` (the default). A licensed or third-party ("ext") connector needs its tier passed explicitly:

```python
result = await ctx.tools.connectors.call(
    "some-crm",
    "get-deal",
    params={"deal_id": "123"},
    tier="licensed",  # or "ext"
)
```

The SDK has no catalog access to know a connector's tier on its own, so calling a licensed/ext connector without `tier=` fails tool-not-found — that error's message is augmented with a hint to retry with the right tier. The MCP Gateway enforces whether your token actually has the scope for that tier; this argument only picks the tool name.

## Human-in-the-loop approval

```python
approval = await ctx.tools.hitl.request_approval(
    "Deploy to production?",
    options=["approve", "reject"],
)
```

`ctx.tools.hitl.request_input` (free-form user input outside the approval flow) is **not** backed by a platform service and fails closed — it returns `success=False` without making a network call, rather than silently hanging or approving. Use the [HITL pause pattern](building-an-agent.md#pausing-hitl-oauth-and-write-confirmation) for input requests instead.

## Calling other agents

```python
result = await ctx.tools.agents.call("other-agent-id", "Summarize this thread")
discovered = await ctx.tools.agents.discover(query="summarization")
```

This is a governed, token-chained call — distinct from using [`Client`](../reference/api.md#novaos_client_sdk.client) directly, which is for a standalone process rather than a running agent's own skill.

## Escape hatch: `call_ungoverned_tool`

If you need to reach an MCP tool that has no first-class binding above, `ctx.tools.<any binding>.call_ungoverned_tool(tool_name, arguments)` is available on every binding class. It bypasses the SDK's typed tool surface — the platform's own policy may still deny the call — and is logged as a warning each time it's used. Treat repeated use of this for the same tool as a signal that the SDK should grow a proper binding for it, not as a long-term pattern.
