# Invoking agents

There are two distinct paths for driving agent execution, and they're not interchangeable:

- **`Client`** speaks A2A directly to an agent's own HTTP endpoint. Use this for agent-to-agent calls.
- **`GatewayClient`** drives execution through the NovaOS Execution Gateway's REST/SSE API. Use this from end-user applications (a frontend, a bot) that don't talk A2A directly.

## `Client`: send, stream, or fire-and-poll

```python
import asyncio
from novaos_client_sdk import Client

async def main() -> None:
    async with Client(base_url="http://my-agent.example.internal:8000") as client:
        task = await client.send_message("Hello, agent!", skill_id="chat")
        print(task.artifacts[0].text if task.artifacts else task.status.state)

asyncio.run(main())
```

`send_message` replaces the 1.x `run_sync` and is A2A's `SendMessage`. `skill_id` picks which of the agent's skills should run — A2A has no dedicated field for it, so it travels as `message.metadata["skillId"]`; a single-skill agent doesn't need it. It returns either a `Task` (the common case) or a bare `A2AMessage` when the agent answers without creating one — `isinstance` tells them apart.

By default `send_message` blocks until the task reaches a terminal or interrupt state. Pass `return_immediately=True` to get the task back as soon as it exists instead, and follow up with `subscribe_to_task` or by polling `get_task` — the A2A v1.0 flag is `return_immediately`, and its sense is **inverted** from v0.3's `blocking`: a v0.3-era `blocking=True` is this SDK's default (`return_immediately=False`), not the other way round.

For output produced incrementally, use `send_streaming_message` (A2A `SendStreamingMessage`, replacing `run_stream`):

```python
from novaos_client_sdk import Client, is_last_frame, TaskArtifactUpdateEvent

async def main() -> None:
    async with Client(base_url="http://my-agent.example.internal:8000") as client:
        async for frame in client.send_streaming_message("Tell me a story", skill_id="storyteller"):
            if isinstance(frame, TaskArtifactUpdateEvent):
                print(frame.artifact.text, end="", flush=True)
            if is_last_frame(frame):
                break
```

Each frame is one of A2A's `StreamResponse` members: a `TaskStatusUpdateEvent`, a `TaskArtifactUpdateEvent`, a whole `Task`, or a bare `A2AMessage` — dispatch with `isinstance`, not a `kind` tag, since A2A v1.0 carries none.

**Call `is_last_frame(frame)` to know when the stream is over — never just the fact that iteration stopped.** A2A v1.0 removed v0.3's `final` boolean, so nothing on the wire tells a caller directly "this is the end." The only reliable signal is the *task state*: `is_last_frame` returns `True` once a frame carries a terminal state (`COMPLETED`, `FAILED`, `CANCELED`, `REJECTED`) or an interrupt state (`INPUT_REQUIRED`, `AUTH_REQUIRED`) — both close the stream, but only the first means the task is actually finished. A stream that closes with *no* such frame at all failed in transport; treating that as a successful completion is exactly the bug this check exists to prevent. If the agent doesn't support streaming (or this client has no Agent Card to check), `send_streaming_message` transparently falls back to `send_message` and yields its single result as one frame — your loop doesn't need a separate code path for that case.

## Tasks: fetch, reconnect, list, cancel

```python
# One authoritative snapshot — no ACP equivalent; an ACP run couldn't be re-queried.
task = await client.get_task(task_id)

# Reconnect a dropped stream — the first frame is always the task's current
# state, so there's no cursor to keep. A2A does not promise to replay
# messages or artifacts missed while disconnected, only the status.
async for frame in client.subscribe_to_task(task_id):
    if is_last_frame(frame):
        break

# Enumerate what the agent is working on — also no ACP equivalent.
page = await client.list_tasks(status="TASK_STATE_WORKING", page_size=20)
for t in page.tasks:
    print(t.id, t.status.state)

# Ask the agent to stop a task. Cancellation is a request, not a guarantee —
# read task.status.state on the result rather than assuming it canceled.
canceled = await client.cancel_task(task_id)
```

`list_tasks` accepts a `TaskState` or its wire string for `status`, plus `context_id`, `page_token`, `history_length`, `status_timestamp_after`, and `include_artifacts` — every filter is a request the agent may honor partially. `cancel_task` raises `TaskNotCancelableError` (A2A `-32002`) if the task is already terminal.

## Discovery

Two different discovery scopes exist, matching two different questions:

**"What can this agent do, and how do I reach it?"** — [`fetch_agent_card`](../reference/api.md#novaos_client_sdk.discovery) fetches one Agent Card, the A2A document describing an agent's skills, capabilities, and `securitySchemes`:

```python
from novaos_client_sdk import fetch_agent_card

card = await fetch_agent_card("http://my-agent.example.internal:8000")
print(card.name, [s.id for s in card.skills])
```

For repeated lookups, [`AgentDiscoveryService`](../reference/api.md#novaos_client_sdk.discovery) keeps a client around and caches the card for `cache_ttl` seconds (default 300); `svc.to_client(allowed_host_suffixes=[...])` builds a `Client` from the card it already fetched, matching the protocol binding and tenant the card declares. The card's URL decides where the client connects and where its credentials are sent, so you must name the domains it may point at (`allowed_host_suffixes`).

**"Where is this agent, anywhere on the network?"** — the Agent DNS service answers that, and `discover_and_connect` combines the lookup with client construction in one call:

```python
import asyncio
from novaos_client_sdk import discover_and_connect, get_dns_url_from_env

async def main() -> None:
    dns_url = get_dns_url_from_env()  # reads AGENT_DNS_URL — see Getting started

    async with discover_and_connect(agent="my-agent", dns_base_url=dns_url) as client:
        task = await client.send_message("Hello", skill_id="chat")
        print(task.artifacts[0].text if task.artifacts else task.status.state)

asyncio.run(main())
```

`get_dns_url_from_env()` checks, in order: an explicit argument, `AGENT_DNS_URL` (the platform convention), `ACP_DNS_URL`, `DNS_SERVICE_HOST`/`DNS_SERVICE_PORT`, then a per-environment variable (`ACP_DNS_URL_<ENV>`). There is no default host for any environment — it raises `ValueError` naming every variable it checked if none is set.

`discover_and_connect` defaults to `trust_dns_urls=False`: a discovered URL is validated against the same private/loopback/link-local guard as any other, since Agent DNS is not itself the trusted golden path to an agent — the Execution Gateway is. Pass `trust_dns_urls=True` only when your caller knowingly needs a direct dial. For lower-level control — filtering by capability, domain, or tag before connecting — use [`AgentDNSClient`](../reference/api.md#novaos_client_sdk.dns) directly; `discover_and_connect` is built on top of it.

## `GatewayClient`: for end-user applications

The Execution Gateway is the platform's REST/SSE front door for applications that aren't themselves A2A agents — a marketplace frontend, a bot. `GatewayClient` addresses the *platform*, not an agent directly, so it speaks the same REST/SSE contract regardless of which protocol the pod underneath happens to run; `execute()` and `events()` are unchanged from before this SDK's A2A migration. Authentication uses a NovaOS IAM (OIDC) bearer token — see [Authentication](authentication.md).

```python
import asyncio
from novaos_client_sdk import GatewayClient, BearerTokenAuth

async def main() -> None:
    auth = BearerTokenAuth(token="<novaos-iam-token>")

    async with GatewayClient(base_url="http://execution-gateway.example.com", auth=auth) as gw:
        result = await gw.execute(instance_id="<installed-agent-instance-uuid>", input_text="Inspire me!")

        async for event in gw.events(result.run_id):
            if event.type == "chunk":
                print(event.content, end="", flush=True)
            if event.is_terminal:
                if event.is_failure:
                    print(f"\nRun failed: {event.content}")
                break

asyncio.run(main())
```

`execute()` starts a run and returns immediately with a `run_id`; `events()` opens a Server-Sent Events subscription for it. Branch on `event.is_terminal` / `event.is_failure` rather than on `event.type == "done"` or `"error"`: the gateway can also end a run by replaying a `status` event carrying a terminal run status — `is_terminal` is the one check that covers every shape an ending arrives in. `instance_id` identifies an *installed* agent instance (not an agent name) — the UUID your application already has from installing the agent on the platform.

Unlike `Client.cancel_task`, `GatewayClient` exposes no generic "cancel this run" call — a running gateway execution is stopped the same way any HTTP/SSE request is, by closing the connection or cancelling the `asyncio` task awaiting it. `gw.streaming_health()` checks whether the real-time SSE subsystem is available before you subscribe, so you can fail over to polling if it isn't.
