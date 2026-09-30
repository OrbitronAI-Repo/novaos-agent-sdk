# Migrating from 1.x

The SDK's `1.x` line spoke [ACP](https://agentcommunicationprotocol.dev/); `2.x` speaks [A2A](../concepts/a2a-primer.md) instead. This is a breaking change with no compatibility window — no deprecated aliases, no ACP shim. `handle_acp_request` is gone, importing any `ACP*` exception raises `ImportError`, and `acp-sdk` is no longer a dependency. For how A2A itself works, see the [A2A primer](../concepts/a2a-primer.md); for a new agent, see [Building an agent](building-an-agent.md).

## Find your pattern

How much work is needed depends on how your agent currently serves ACP requests:

```bash
grep -rn "handle_acp_request" --include=*.py .
grep -rn "acp_sdk.server\|from acp_sdk import.*Server" --include=*.py .
```

**Already on `AgentBuilder`** (a route calling `agent.handle_acp_request(...)`)? Skills are already registered against `AgentBuilder`, and the only ACP-shaped part is the route itself — you're on the mechanically portable path covered below.

**A raw ACP server** (`acp_sdk.server.Server()` owns your routing)? There's no `handle_*` call site to swap — A2A has no equivalent server object. Port your routing onto `AgentBuilder` first, registering each ACP agent function as a skill, then follow the steps below. Budget for this as a rewrite, not an edit.

## The five changes

### 1. Route and handler

A2A is JSON-RPC 2.0 at a single endpoint, not REST across several:

```diff
-from novaos_client_sdk import AgentBuilder, AgentExecutionContext, SkillResult
+from novaos_client_sdk import A2A_JSONRPC_PATH, AgentBuilder, AgentExecutionContext, SkillResult

-@app.post("/runs")
-async def run(request: Request):
-    body = await request.json()
-    return await agent.handle_acp_request(body, request.headers)
+app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())
```

Don't hand-roll the route: `asgi_endpoint()` reads the raw body itself, answers a malformed one with a proper JSON-RPC `-32700` error (a hand-written `await request.json()` raises before your code runs, surfacing as a bare HTTP 500 instead), and dispatches streaming methods to an SSE response. Need the route inline for another reason? Call `agent.handle_a2a_bytes_with_headers(await request.body(), request.headers)` instead.

### 2. `A2A-Version` is mandatory — including its absence

A2A treats an absent or empty `A2A-Version` header as a request for an older protocol version, so a header-less call is refused (`VersionNotSupportedError`, JSON-RPC `-32009`). Nothing changes inside your agent, but any caller you control needs to start sending it:

```python
from novaos_client_sdk import A2A_PROTOCOL_VERSION, A2A_VERSION_HEADER
headers = {A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION}   # "A2A-Version: 1.0"
```

### 3. Exceptions are re-rooted — every `ACP*` name is gone

Importing any `ACP*Error` now raises `ImportError`. The new root is `NovaOSError`, not an `A2A*`-prefixed name — most of this hierarchy (secrets, memory, tool calls, observability) has nothing to do with the agent wire protocol.

| Old | New |
|---|---|
| `ACPClientError` | `NovaOSError` |
| `ACPConnectionError` | `NovaOSConnectionError` |
| `ACPAuthenticationError` | `NovaOSAuthenticationError` |
| `ACPRateLimitError` | `NovaOSRateLimitError` |
| `ACPValidationError` | `NovaOSValidationError` |
| `ACPAgentNotFoundError` | `NovaOSAgentNotFoundError` |
| `ACPServerError` | `NovaOSServerError` |
| `ACPDNSError` | `NovaOSDNSError` |
| `ACPDNSConnectionError` | `NovaOSDNSConnectionError` |
| `ACPDNSAgentNotFoundError` | `NovaOSDNSAgentNotFoundError` |

`MissingExecutionTokenError` keeps its name; the hierarchy's shape is unchanged (see [Error handling](error-handling.md) for the full picture). If you subclass one of these, `retry_after`, `details`, `field`, `reason`, `status_code`, and `agent_name` are now keyword-only, so a subclass passing more than one positional argument now raises `TypeError` instead of silently binding the wrong attribute. Also delete any `except ImportError: ACPClientError = Exception` fallback — under the rename it silently widens to a bare `except Exception`.

### 4. Output is artifacts; metadata goes on the task

A run's output is no longer a flat `list[Message]`. It's a list of typed, named `Artifact` objects on `Task.artifacts`. If your skill lifted fields onto the top level of the response envelope, that's no longer possible — the reply is a JSON-RPC response, and top-level keys collide with `jsonrpc` / `id` / `result`. Move those to `Task.metadata` instead. `SkillResult.metadata` is preserved end-to-end as a tagged `DataPart` rather than dropped.

### 5. Pin `uvicorn` explicitly

Dropping `acp-sdk` also drops its transitive `uvicorn[standard]` dependency. An unpinned `uvicorn` makes your `Dockerfile`'s `CMD uvicorn agent:app` fail at start-up after the migration, and nothing in a typical test suite catches it:

```diff
 dependencies = [
-  "acp-sdk>=1.0.0,<2",
+  "uvicorn[standard]>=0.30",
 ]
```

## Behaviour that changes with no code edit

**HITL pauses.** `SkillResult(status="WAITING_USER")` now maps to `TASK_STATE_INPUT_REQUIRED`, and resuming happens via a *continuation* `SendMessage` carrying the same task id, not a fresh run — A2A defines no dedicated resume operation (see [Invoking agents](invoking-agents.md) for the caller side).

**Skill routing.** The old first-skill fallback — silently routing an unrouted request to whichever skill registered first — is now restricted to agents with exactly one skill; a multi-skill agent that relied on this now gets an explicit routing failure instead, so make sure every caller names the skill it wants.

**Conversation history.** Reading prior turns out of the tail of your input no longer works — `SendMessage` carries one message. Prior turns are now `ctx.history`, a tuple of `(role, content)` pairs, oldest first, current turn excluded, empty outside a multi-turn conversation (which is most calls) — so an agent that never reads it behaves exactly as before.

**Cancellation.** A caller can now preempt a running skill mid-execution via `CancelTask`, not just stop tracking it while it runs to completion. Most skills need no change: the harness cancels the skill's `asyncio.Task`, so any `await` inside it is interrupted immediately. Sustained synchronous work between awaits opts in with `ctx.raise_if_cancelled()` (or `ctx.is_cancelled`) at a safe point in the loop, to stop promptly instead of running to completion regardless. A couple of other interruption paths — an unrequested `CancelledError`, a caller disconnecting mid-skill — are handled differently on purpose; see `AgentExecutionContext`'s own docstring in the SDK source if your agent does anything unusual here.

## `transform`: preview and apply a rewrite

`python -m novaos_client_sdk.migrate transform` applies much of the mechanical rewrite, driven by a plan from `migrate analyze` (full command set in the [migrate CLI reference](../reference/migrate-cli.md)):

```bash
python -m novaos_client_sdk.migrate transform ./my-agent --plan ./out/my-agent.plan.json         # preview: prints a diff, changes nothing
python -m novaos_client_sdk.migrate transform ./my-agent --plan ./out/my-agent.plan.json --write  # apply it
```

Preview is the default; `--write` applies it and is idempotent — running it again on an already-migrated file is a no-op, not a second rewrite. This needs the `migrate` extra (`pip install "novaos-agent-sdk[migrate]"`); without it, the command exits with a message on stderr rather than an import traceback. Not every agent is eligible — one with genuinely custom streaming control flow is always refused rather than partially rewritten.

## `verify`: check a migrated agent for real

`migrate verify` is the one step that actually runs your migrated agent: it converts your agent's own pre-migration test fixtures into real A2A `SendMessage` requests, sends them to a live instance, and checks each reaches the expected task state:

```bash
python -m novaos_client_sdk.migrate verify ./my-agent --base-url http://127.0.0.1:8001 \
    --expect summarize=INPUT_REQUIRED --stream summarize
```

Every fixture defaults to expecting `TASK_STATE_COMPLETED`; `--expect skill=STATE` overrides that per skill, and `--stream skill[,skill...]` additionally probes the named skills with `SendStreamingMessage`, checking the stream produces at least one frame and closes on a proper terminal state. `verify` also cross-checks your agent's live Agent Card against `manifest.json` — the two must declare the same skill set, or a caller routing off the card can reach a skill your manifest never declared (or fail to reach one it did).

## API reference: before and after

### `Client`

| 1.x | 2.x |
|---|---|
| `run_sync(agent=…, input=[…])` | `send_message(message, skill_id=…)` |
| `run_stream(agent=…, input=[…])` | `send_streaming_message(message, skill_id=…)` |
| — | `get_task(task_id)` |
| — | `subscribe_to_task(task_id)` |
| — | `cancel_task(task_id)` |
| — | `list_tasks(…)` |

There's no `agent=` parameter any more, reflecting the protocol: under A2A an agent *is* a URL, named once at construction as the `Client`'s `base_url`, not passed per call:

```python
run = await client.run_sync(agent="invoice-extractor", input=[message])   # before
task = await client.send_message(message, skill_id="extract")             # after
```

`skill_id` isn't optional for a multi-skill agent (see Skill routing above). `send_message` returns `Task | A2AMessage` — a conformant agent replies with a `Task`, but the spec permits a bare message, so narrow the type before reaching for `task.artifacts`.

### Models

| 1.x | 2.x |
|---|---|
| `Run` | `Task` |
| `Run.output` (`list[Message]`) | `Task.artifacts` (`list[Artifact]`) |
| `Run.first_output_content` | `Artifact.text` on the artifact you want |
| `models.Agent` | `AgentCard` |

`Message` / `MessagePart` are unchanged — a convenience shape for input, converted into a real A2A message by `send_message`. `Capability` is also unchanged.

### Discovery

| 1.x | 2.x |
|---|---|
| `agents()` | `agent_card()` |
| `get_agent(name)` / `get_or_raise(name)` | `skill(id)` / `require_skill(id)` |
| `find_agents(...)` / `find_agents_list(...)` / `find_one(...)` | removed, no replacement |

The filter-and-list surface was removed rather than reimplemented because A2A has no listing operation — there's no `GET /agents`-shaped endpoint to enumerate. An A2A agent publishes a single Agent Card describing itself; discovering which agents *exist* is a separate question from asking one what it does. `AgentDNSClient`'s `list_agents` / `search_agents` / `find_agents` are unaffected by this migration and still cover enumeration.

### Exceptions

See the [rename table above](#3-exceptions-are-re-rooted-every-acp-name-is-gone) — the keyword-only change is a substitutability fix as much as a rename, since a subclass constructor could previously bind a stray positional argument to the wrong attribute depending on which class caught it.

### `GatewayClient` is unchanged

If your code also uses `GatewayClient` to talk to the NovaOS platform's execution gateway — starting a run, polling a `run_id`, following a `QUEUED` → `RUNNING` → `COMPLETED` lifecycle — none of this page applies. `Client` addresses an individual agent and speaks whatever protocol it serves; `GatewayClient` addresses the platform itself, in platform-level concepts that were never ACP or A2A. Nothing about it changed here.

## A heads-up on the import path

The package on PyPI is `novaos-agent-sdk`, but the code you import is still `novaos_client_sdk` — the package name and import path don't match. A separate, not-yet released change will rename the import path to `novaos_agent_sdk`, keeping `novaos_client_sdk` working as a deprecated alias with a `DeprecationWarning`. That hasn't landed yet; when it does, it will be called out in the [changelog](../changelog.md). For now, keep importing from `novaos_client_sdk`.
