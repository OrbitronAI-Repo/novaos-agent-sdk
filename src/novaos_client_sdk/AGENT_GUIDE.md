# NovaOS Agent SDK: guide for coding assistants

This file ships inside the `novaos-agent-sdk` package (import name `novaos_client_sdk`), so an
assistant that only has the installed package can find it. It covers building, publishing,
installing and running an agent on NovaOS, the environment variables involved, and the steps
only a person can do. Full documentation: https://orbitronai-repo.github.io/novaos-agent-sdk/

## 1. The lifecycle

You build agents with this SDK (`AgentBuilder`) and ship them with the NovaOS command line,
`novaos-cli` (Node.js and Docker required). Commands run from the agent's project directory.

```bash
npm i -g novaos-cli

novaos-cli env list                          # environments the CLI knows about
novaos-cli env use <env>                     # pick one (`env add` registers a new one)
novaos-cli auth login --env <env>            # sign in (opens a browser: a person does this)
novaos-cli init --name my-agent --lang python   # scaffold an agent project with a manifest.json
cd my-agent                                  # build your skills with AgentBuilder

novaos-cli test                              # discover and run the project's tests
novaos-cli run --input "Hello"               # start the agent locally and send it one message
novaos-cli manifest validate manifest.json   # check the manifest
novaos-cli publish manifest.json --dry-run   # validate and authorize; no build, no push
novaos-cli publish manifest.json             # build the image, push it, submit it for review
novaos-cli publish-status <review-id> --wait # follow the review; the id is printed by `publish`
```

`--wait` needs `novaos-cli` 2.2.0 or later. With an older CLI, run
`novaos-cli publish-status <review-id>` again until it says `ACTIVE`.

Then install your own copy of the agent and run it (section 3).

- `init` takes the publisher from your signed-in identity; pass `--publisher <slug>` if your
  account has none.
- `publish` and `publish-status` need the developer role on your account.
- `publish-status --wait` (CLI 2.2.0 or later) polls until the version is `ACTIVE`, `REJECTED`
  or `ROLLED_BACK`, and gives up after 900 seconds (`--timeout <seconds>`, also 2.2.0 or later,
  changes that). It exits 0 for `ACTIVE`, and 1 for `REJECTED`, `ROLLED_BACK` or a timeout.
  `--verify` also fetches the served Agent Card once the agent is live.

What happens after `publish`:

1. The submission goes through an automated security scan. A failed scan rejects it.
2. The admins' approval policy decides whether a person reviews it. An unverified publisher,
   or a tool category the admins ticked, always goes to an admin. Otherwise a trusted publisher
   is approved automatically if that is switched on, and everything else follows the policy's
   default: either an admin reviews it or it is approved automatically.
3. Once approved the agent is **active** and can be installed. Installing it creates an
   **instance**, and the instance ID it returns is what you run.

A minimal agent:

```python
from novaos_client_sdk import AgentBuilder, AgentExecutionContext, SkillResult

agent = AgentBuilder(name="greeter", version="1.0.0")

@agent.skill("greet", description="Greet the caller by name")
async def greet(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
    return SkillResult(output=f"Hello, {input_text or 'there'}!")
```

Mount `agent.asgi_endpoint()` on your ASGI app at `A2A_JSONRPC_PATH`. See
[Building an agent](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/building-an-agent/).

### The manifest's tool list

`permissions.tools` in `manifest.json` lists the platform tool categories the agent uses
(`memory`, `knowledge`, `llm`, `hitl`, `files`, `notify`, `http`, `browser`, `code`, `agents`,
`connectors`). The security scan and the admins' approval policies read it (a category can be
set to always need review), `memory` turns on memory pre-fetch (`ctx.memory_budget`), and
`publish` warns when a write tool is listed but `resumable` is not set.

## 2. Environment variables

Only the variables an agent author or caller needs. "CLI" is `novaos-cli`; "SDK" is this
package. `NOVAOS_GATEWAY_URL` and `NOVAOS_AGENT_OPS_URL` are read by both, with the same meaning.

| Variable | Read by | What it does |
|---|---|---|
| `NOVAOS_ENV` | CLI | Target environment (same as `--env`). |
| `NOVAOS_ENV_CONFIG` | CLI | Path of the environments file (default `~/.novaos/environments.json`). |
| `NOVAOS_GATEWAY_URL` | CLI, SDK | Execution Gateway URL. CLI: overrides the active environment's. SDK: default `base_url` of `ObservabilityClient`. |
| `NOVAOS_AGENT_OPS_URL` | CLI, SDK | Agent Ops URL. CLI: overrides the active environment's. SDK: default `base_url` of `HeartbeatClient`. |
| `NOVAOS_REVIEW_URL`, `NOVAOS_IAM_URL`, `NOVAOS_MCP_OPS_URL`, `NOVAOS_CONNECTOR_HOST_URL` | CLI | Override one service URL of the active environment. Order: flag, then variable, then environments file, then the `local` defaults. |
| `NOVAOS_MARKETPLACE_URL` | CLI | Marketplace URL for `agent list` when the environment has no `marketplaceUrl`. |
| `NOVAOS_CLIENT_ID` | CLI | OAuth client ID override. |
| `NOVAOS_ORGANIZATION_ID` | CLI | Active organization for org-scoped tokens, instead of `auth org use`. |
| `NOVAOS_RETRY_MODE`, `NOVAOS_MAX_ATTEMPTS` | CLI | HTTP retries: `none`, `standard` (default) or `adaptive`; attempts including the first (default 3). |
| `NOVAOS_SANDBOX_COMPOSE_FILE` | CLI | Compose file for `sandbox` and `--sandbox`, instead of searching up from the current directory. |
| `AGENT_DNS_URL` | SDK | Agent DNS URL for `get_dns_url_from_env()` and `discover_and_connect`. Then `DNS_SERVICE_HOST` + `DNS_SERVICE_PORT`, among other fallbacks. No default host. Set on agent pods by the platform. |
| `NOVAOS_EXECUTION_TOKEN` | SDK | Local runs only: the token used when a request carries none. |
| `NOVAOS_TOOL_TIMEOUT_S` | SDK | Default `ctx.tools` timeout in seconds (default 128). |
| `NOVAOS_SECRET_NAMES` | SDK | Comma-separated names `ctx.secret_names` lists for secrets injected as environment variables. |
| `NOVAOS_STRUCTURED_ROOT_LOGS` | SDK | `false` stops the SDK adding its JSON log handler to the root logger. |
| `NOVAOS_JUDGE_MODEL` | SDK | Model for `call_llm_judge()` when none is passed (offline evaluation). |
| `MCP_GATEWAY_URL`, `LLM_GATEWAY_URL`, `MEMORY_SERVICE_URL`, `CHECKPOINT_SERVICE_URL` | SDK | Platform services an agent calls. Set on agent pods by the platform; set them yourself only to run against a service locally. |
| `NOVAOS_TOKEN_ENDPOINT` | SDK | Local endpoint the SDK asks for the run's newest execution token. Set by the platform on agent pods where it serves one; leave it unset elsewhere. |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME` | SDK | OTLP/HTTP collector the SDK exports spans to, and the service name on them. |
| `LANGFUSE_HOST`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY` | SDK | Langfuse access for the `evaluation` extra. |

## 3. Install and run a published agent

These six calls are an interim route until the CLI has its own install and run commands. They
work, but details may change. Full page:
[Installing and running](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/installing-and-running/).

```bash
TOKEN=$(novaos-cli auth token)   # your access token
GW=https://<gateway-url>         # the "Gateway:" line of `novaos-cli env show`
MP=https://<marketplace-url>     # "marketplaceUrl" for your environment in ~/.novaos/environments.json
AGENT_ID=<agent-id>              # printed by `publish` and `publish-status`
```

| # | Step | Call |
|---|---|---|
| 1 | Read what the agent expects | `GET $GW/a2a/v1/agents/<agent-id>/.well-known/agent-card.json` (no token) |
| 2 | Install your own copy | `POST $MP/api/instances` |
| 3 | Find your copy | `GET $GW/api/v1/instances` |
| 4 | Run it | `POST $GW/api/v1/execute` |
| 5 | Wait until it finishes | `GET $GW/api/v1/runs/<run-id>` |
| 6 | Read the answer | `GET $GW/api/v1/runs/<run-id>/events` |

Every call but the first sends `Authorization: Bearer $TOKEN`.

- **Install** sends `{"agent_id": ..., "agent_name": ..., "adapter_type": "A2A"}` with the
  header `X-Requested-With: XMLHttpRequest` (without it: 403). `agent_name` must be the name the
  agent is listed under (for a new agent, the manifest's `name`, which the card's `name`
  repeats); a different value renames the listing for everyone. The instance ID is at
  `.data.instance.instance_id`. Installing again is safe and returns the same instance.
- An instance belongs to the person who installed it: each person installs once. Find an
  earlier one in `.data[]` of step 3 (`limit` at most 50, page with `cursor`).
- **Run** sends `{"instance_id": ..., "input": "<text>"}` with an `X-Idempotency-Key: <uuid>`
  header (same key and body returns the same run; a new key per new run). It returns `run_id`
  straight away. An agent with more than one skill also needs `"skill": "<skill-id>"` from the
  card.
- **Wait** until `.status` is `COMPLETED`, `FAILED`, `CANCELLED`, `REJECTED`, `WAITING_USER` or
  `WAITING_AUTH`. The run record has no answer in it.
- **Answer:** the `payload.content` of the last `message.final` event in `.events[]`.
- From Python, `GatewayClient` covers steps 4 to 6 (`execute()`, then `events()`); pass the
  idempotency key through `headers`.
- When a run fails: `novaos-cli debug trace <run-id> --json` and
  `novaos-cli agent logs <agent-id> --run-id <run-id>`.

## 4. Steps that need a person

| Step | Who | Where |
|---|---|---|
| Sign in | You | `novaos-cli auth login` opens a browser |
| Approve a new version (unless auto-approve applies) | A platform admin | `<marketplace-url>/admin/approvals` |
| Install your copy, only through the web UI | You | `<marketplace-url>/dashboard` |

While `publish-status` shows **Awaiting admin review** (`PENDING_REVIEW` in `--json`), nothing
in the terminal can move the version on. An admin approves it; a publisher who isn't a
platform admin can't approve their own version, while a platform admin can approve any version,
their own included (recorded as a self-review). Installing needs no person when you make the install call
(section 3) yourself; only the web-UI install does.

Rules for an assistant:

- **Stop and ask; never pretend.** At a step that needs a person, stop. Name the step, the
  page, who must act and why, then wait.
- **Never** say the agent is live while it is in review, **never** make up an instance ID, and
  **never** try to approve your own agent: approving is a person's decision, even when the
  person is a platform admin.
- **Check after the person acts**, before going on: `novaos-cli publish-status <review-id>`
  shows `ACTIVE` after approval; `GET $GW/api/v1/instances` lists the copy after a web-UI install;
  `novaos-cli auth whoami` shows the identity after sign-in. If the check does not confirm it,
  say what you see and wait again.
- **Browser help needs consent.** An assistant that can control the person's signed-in browser
  may open the page and do a UI step, asking before each action. It still does not approve the
  agent itself: a platform admin decides that.

Full page:
[Steps that need a person](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/steps-that-need-a-person/).

## 5. More documentation

- [Getting started](https://orbitronai-repo.github.io/novaos-agent-sdk/getting-started/)
- [Building an agent](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/building-an-agent/)
- [Installing and running](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/installing-and-running/)
- [Steps that need a person](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/steps-that-need-a-person/)
- [Invoking agents](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/invoking-agents/)
- [Platform tools](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/platform-tools/)
- [API reference](https://orbitronai-repo.github.io/novaos-agent-sdk/reference/api/)
