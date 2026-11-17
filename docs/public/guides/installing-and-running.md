# Installing and running a published agent

!!! warning "Interim"
    These calls are a stopgap until the CLI has its own commands to install and run an agent. They are real, working routes, but treat them as interim: the CLI commands will replace them, and the details may change.

Publishing an agent puts it in the marketplace catalog. To use it, each person installs their **own copy** (an *instance*), runs that copy, and reads the answer. This page does that from a terminal with the CLI, `curl` and `jq`, or from Python with [`GatewayClient`](invoking-agents.md#gatewayclient-for-end-user-applications).

You need:

- the agent's ID (`publish` and `publish-status` print it);
- `novaos-cli`, signed in with `novaos-cli auth login`;
- an agent whose review has finished. `novaos-cli publish-status <review-id> --wait` (CLI 2.2.0 or later; with an older CLI, repeat `publish-status <review-id>` until it says `ACTIVE`) waits for that and says whether a person still has to approve it. An agent that is still in review cannot be installed; [Steps that need a person](steps-that-need-a-person.md) says who approves it and where.

## Set up

```bash
TOKEN=$(novaos-cli auth token)   # your access token; it refreshes when expired
GW=https://<gateway-url>         # the "Gateway:" line of `novaos-cli env show`
MP=https://<marketplace-url>     # "marketplaceUrl" for your environment in ~/.novaos/environments.json
AGENT_ID=<agent-id>
```

`GW` is the Execution Gateway and `MP` the marketplace. Every call below except the first sends `Authorization: Bearer $TOKEN`.

## The six calls

| # | Step | Call |
|---|---|---|
| 1 | Read what the agent expects | `GET $GW/a2a/v1/agents/<agent-id>/.well-known/agent-card.json` |
| 2 | Install your own copy | `POST $MP/api/instances` |
| 3 | Find your copy | `GET $GW/api/v1/instances` |
| 4 | Run it | `POST $GW/api/v1/execute` |
| 5 | Wait until it finishes | `GET $GW/api/v1/runs/<run-id>` |
| 6 | Read the answer | `GET $GW/api/v1/runs/<run-id>/events` |

### 1. Read what the agent expects

```bash
curl -sS "$GW/a2a/v1/agents/$AGENT_ID/.well-known/agent-card.json" > card.json
jq '{name, version, skills: [.skills[] | {id, description, examples}]}' card.json
AGENT_NAME=$(jq -r .name card.json)
```

The Agent Card is public, so this call needs no token. Its `skills` list says what each skill does. `examples` show the input a skill expects, when the agent's author added them. Keep `name`: the install call needs it.

### 2. Install your own copy

```bash
curl -sS -X POST "$MP/api/instances" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Requested-With: XMLHttpRequest" \
  -d "$(jq -n --arg id "$AGENT_ID" --arg name "$AGENT_NAME" \
        '{agent_id: $id, agent_name: $name, adapter_type: "A2A"}')" > install.json
INSTANCE_ID=$(jq -r .data.instance.instance_id install.json)
```

- The marketplace refuses a `POST` without the `X-Requested-With` header (403).
- `agent_name` must be the name the agent is listed under. The marketplace records the name you send, so a different value renames the agent's listing for everyone. For an agent you just published, that is your manifest's `name`, which the card's `name` repeats. If you already have a copy of the agent, use its `agent_name` from step 3, and if the card's `name` differs from the name shown in the marketplace, use the marketplace's.
- A new install returns `201`, with the copy's ID at `.data.instance.instance_id`.
- Installing again is safe. It returns the same instance, upgraded to the agent's newest published version; `.data.message` says which.
- You need permission to install agents in your organization; without it the call returns `403`.
- An agent published for API use only, and one whose review has not finished, cannot be installed this way.

An instance belongs to the person who installed it. Someone else's instance ID does not work for you, so each person (and each assistant acting for them) installs once.

### 3. Find your copy

```bash
curl -sS "$GW/api/v1/instances?limit=50" -H "Authorization: Bearer $TOKEN" \
  | jq --arg id "$AGENT_ID" '.data[] | select(.agent_id == $id) | {instance_id, agent_version, status}'
```

This lists the instances you can run, as `.data[]` with `instance_id`, `agent_id`, `agent_name`, `agent_version` and `status`. `limit` can be at most 50; when `.meta.has_more` is `true`, pass `cursor=<.meta.cursor.next>` for the next page. Use it to find an instance you installed earlier.

### 4. Run it

```bash
curl -sS -X POST "$GW/api/v1/execute" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Idempotency-Key: $(uuidgen)" \
  -d "$(jq -n --arg i "$INSTANCE_ID" --arg p "Tell me a short joke." '{instance_id: $i, input: $p}')" > run.json
RUN_ID=$(jq -r .run_id run.json)
```

- The call returns `200` straight away with `run_id` and a starting `status` (`QUEUED` or `RUNNING`); the agent works in the background.
- `input` is plain text (at most 100,000 characters).
- An agent with more than one skill needs `"skill": "<skill-id>"` from the card; without it the run ends `REJECTED`. A single-skill agent needs none.
- `X-Idempotency-Key` makes a retry safe: sending the same key with the same body again returns the first run instead of starting another. Use a new key for each new run. The same key with a different body is refused with `409`.

### 5. Wait until it finishes

```bash
until STATUS=$(curl -sS "$GW/api/v1/runs/$RUN_ID" -H "Authorization: Bearer $TOKEN" | jq -r .status)
      case "$STATUS" in COMPLETED|FAILED|CANCELLED|REJECTED|WAITING_USER|WAITING_AUTH|null) true ;; *) false ;; esac
do sleep 2; done
echo "$STATUS"
```

A status of `null` means the call itself failed (for example an expired token or a wrong `RUN_ID`); run the `curl` on its own to see the error.

| Status | Meaning |
|---|---|
| `QUEUED`, `RUNNING` | Still working. |
| `COMPLETED` | Finished; read the answer. |
| `FAILED`, `REJECTED`, `CANCELLED` | Ended without an answer; see [When a run fails](#when-a-run-fails). |
| `WAITING_USER`, `WAITING_AUTH` | Paused until a person answers, approves or connects an account. |

The run record has no answer in it: the answer is in the run's events.

### 6. Read the answer

```bash
curl -sS "$GW/api/v1/runs/$RUN_ID/events" -H "Authorization: Bearer $TOKEN" \
  | jq -r '[.events[] | select(.type == "message.final")] | last | .payload.content'
```

The events come as `.events[]`, each with `type`, `payload` and `sequence`. The answer is the `payload.content` of the `message.final` event. Files the agent produced arrive as `artifact.update` events, and the last `status` event has the run's final status. A page holds up to 1,000 events; `?after=<sequence>` returns the events from that sequence number on.

## The same from Python

`GatewayClient` covers steps 4 to 6: it starts the run and streams the answer as it is written. Steps 1 to 3 have no SDK call yet; use the calls above.

```python
import asyncio
import subprocess
import uuid

from novaos_client_sdk import BearerTokenAuth, GatewayClient

GW = "https://<gateway-url>"
INSTANCE_ID = "<instance-id>"


async def main() -> None:
    token = subprocess.run(
        ["novaos-cli", "auth", "token"], capture_output=True, text=True, check=True
    ).stdout.strip()
    # One key per run: a retry of this run with the same key returns the same run_id.
    headers = {"X-Idempotency-Key": str(uuid.uuid4())}
    async with GatewayClient(base_url=GW, auth=BearerTokenAuth(token=token), headers=headers) as gw:
        result = await gw.execute(instance_id=INSTANCE_ID, input_text="Tell me a short joke.")
        async for event in gw.events(result.run_id):
            if event.type == "chunk":
                print(event.content, end="", flush=True)
            if event.is_terminal:
                if event.is_failure:
                    print(f"\nRun failed: {event.content}")
                break


asyncio.run(main())
```

`execute()` sends no idempotency key of its own, so pass one through `headers` as above. Because the headers are set on the client, open a new `GatewayClient` (or pass a new key) for each new run.

## When a run fails

```bash
novaos-cli debug trace "$RUN_ID" --json     # the run, its events and a summary (errors, duration)
novaos-cli agent logs "$AGENT_ID" --run-id "$RUN_ID"   # the agent's log lines for this run
```

`debug trace` needs the developer role. `agent logs` takes the agent ID (not the instance ID) and keeps following new lines; stop it with Ctrl-C.
