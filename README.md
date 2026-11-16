# NovaOS Agent SDK

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/novaos-agent-sdk.svg)](https://pypi.org/project/novaos-agent-sdk/)
[![Development Status](https://img.shields.io/badge/status-development%20(2.x%20A2A%20line)-orange.svg)](https://orbitronai-repo.github.io/novaos-agent-sdk/versioning-and-support/)

A Python client library for connecting to and communicating with AI Agents on the NovaOS platform. Speaks the **Agent2Agent Protocol (A2A), spec v1.0.1** for agent-to-agent communication, and the NovaOS Execution Gateway for end-user applications.

> Published on PyPI as `novaos-agent-sdk`; imported as `novaos_client_sdk`. This is the `2.x` line, built on the **Agent2Agent (A2A)** protocol; pin it as `novaos-agent-sdk>=2,<3`. Coming from `1.x`? See [Migrating from 1.x](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/migrating-from-1x/). See also [Versioning and support](https://orbitronai-repo.github.io/novaos-agent-sdk/versioning-and-support/).

## Installation

```bash
pip install novaos-agent-sdk
```

See [Getting started](https://orbitronai-repo.github.io/novaos-agent-sdk/getting-started/) for the full extras table (`crypto`, `llm`, `schema`, `otel`, `evaluation`, `migrate`, `grpc`, `agent-loop`, `baml`).

## Quick start: call an agent

Under A2A the **base URL identifies the agent** — a `Client` addresses one agent, and `skill_id` chooses which of its skills runs.

```python
import asyncio
from novaos_client_sdk import Client

async def main() -> None:
    async with Client(base_url="http://my-agent:8000") as client:
        task = await client.send_message("Hello, agent!", skill_id="chat")

        if task.is_terminal and task.artifacts:
            print(task.artifacts[0].text)

asyncio.run(main())
```

## Quick start: build an agent

```python
from novaos_client_sdk import AgentBuilder, AgentExecutionContext, SkillResult

agent = AgentBuilder(name="greeter", version="1.0.0")

@agent.skill("greet", description="Greet the caller by name")
async def greet(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
    return SkillResult(output=f"Hello, {input_text or 'there'}!")
```

Mount `agent.asgi_endpoint()` on your ASGI app at `A2A_JSONRPC_PATH` and run it with uvicorn. See [Getting started](https://orbitronai-repo.github.io/novaos-agent-sdk/getting-started/) for the full snippet.

## Build, publish and run on NovaOS

The package installs as `novaos-agent-sdk` but you import it as `novaos_client_sdk`. To take an agent from your laptop to a running instance on NovaOS you also need the NovaOS command line, `novaos-cli` (Node.js and Docker required):

```bash
npm i -g novaos-cli

novaos-cli env list                        # environments the CLI knows about
novaos-cli env use <env>                   # pick one (`env add` registers a new one)
novaos-cli auth login --env <env>          # sign in
novaos-cli init --name my-agent --lang python   # scaffold an agent project with a manifest.json
cd my-agent                                # build your skills with AgentBuilder, test locally
novaos-cli publish ./manifest.json         # build the image, push it, submit it for review
novaos-cli publish-status <review-id>      # follow the review; the id is printed by `publish`
```

`init` takes the publisher from your signed-in identity; pass `--publisher <slug>` if your account has none. `publish` and `publish-status` need the developer role on your account. `publish` accepts `--dry-run` to validate and authorize without building or pushing. `publish-status --verify` also fetches the served Agent Card once the agent is live.

What happens after `publish`:

1. The submission goes through an automated security scan.
2. An admin reviews it. If the publisher is trusted and auto-approve is switched on for it, this step is skipped and the agent goes straight on.
3. Once approved the agent is **active** and can be installed from the NovaOS Marketplace. Installing it creates an **instance**, and the instance ID it returns is what you run.
4. Run the instance from your application with `GatewayClient`:

```python
import asyncio

from novaos_client_sdk import BearerTokenAuth, GatewayClient


async def main() -> None:
    auth = BearerTokenAuth(token="<iam-token>")
    async with GatewayClient(base_url="<gateway-url>", auth=auth) as gw:
        run = await gw.execute(instance_id="<instance-id>", input_text="Hello")
        answer = ""
        async for event in gw.events(run.run_id):
            if event.type == "chunk":  # streamed text
                answer += event.content or ""
            elif event.type == "message.final":  # the complete answer, on replay
                answer = event.content or answer
            if event.is_terminal:
                if event.is_failure:
                    raise RuntimeError(f"Run failed: {event.content}")
                break
        print(answer)


asyncio.run(main())
```

The full walkthrough, including streaming events and answering a paused run, is in [Invoking agents](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/invoking-agents/).

The package also ships `AGENT_GUIDE.md`, a short guide for coding assistants (this lifecycle, the SDK and CLI environment variables, the install and run calls, and the steps that need a person), with an `llms.txt` index beside it. Both sit in the installed `novaos_client_sdk` directory; print the guide's path with `python -c "import novaos_client_sdk, pathlib; print(pathlib.Path(novaos_client_sdk.__file__).with_name('AGENT_GUIDE.md'))"`.

Agents that call a model through BAML can use `novaos_client_sdk.baml_helpers.baml_llm_options` to route BAML calls through the platform's LLM Gateway with the run's token. Inside a skill, pass it `ctx.to_execution_context()`, not the skill's `ctx` itself. Install its dependency with `pip install "novaos-agent-sdk[baml]"`.

## Documentation

Full documentation, including streaming, discovery, authentication, platform tools, migrating a 1.x agent, and the complete API reference, is at:

**https://orbitronai-repo.github.io/novaos-agent-sdk/**

- [Getting started](https://orbitronai-repo.github.io/novaos-agent-sdk/getting-started/)
- [The A2A primer](https://orbitronai-repo.github.io/novaos-agent-sdk/concepts/a2a-primer/)
- [Invoking agents](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/invoking-agents/)
- [Building an agent](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/building-an-agent/)
- [Authentication](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/authentication/)
- [Platform tools](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/platform-tools/)
- [Error handling](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/error-handling/)
- [API reference](https://orbitronai-repo.github.io/novaos-agent-sdk/reference/api/)
- [Versioning and support](https://orbitronai-repo.github.io/novaos-agent-sdk/versioning-and-support/)
- [Security](https://orbitronai-repo.github.io/novaos-agent-sdk/security/)

## Migrating from 1.x

`1.x` is the previous, final line (security and critical fixes only). `2.x` is a breaking change: the `1.x` handler surface (`handle_acp_request`, `from_acp_request`, the `/runs` handler shape) is removed, not deprecated. The `migrate` extra (`pip install "novaos-agent-sdk[migrate]"`) ships a three-stage CLI that ports a 1.x agent onto the `2.x` shell:

```bash
python -m novaos_client_sdk.migrate analyze ./my-agent --out ./migration-out
python -m novaos_client_sdk.migrate transform ./my-agent --plan ./migration-out/my-agent.plan.json --write
python -m novaos_client_sdk.migrate verify ./my-agent --base-url http://127.0.0.1:8001
```

See [Migrating from 1.x](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/migrating-from-1x/) and the [`migrate` CLI reference](https://orbitronai-repo.github.io/novaos-agent-sdk/reference/migrate-cli/) for the full walkthrough.

## Requirements

- Python 3.12+
- `httpx>=0.24.0` — the HTTP client: HTTP for the A2A JSON-RPC transport, DNS and the gateway services. The A2A wire format is NovaOS's own code (no vendor A2A library), verified against the official `a2a-sdk` in the test suite.
- OpenTelemetry (`opentelemetry-api`, `-sdk`, `-exporter-otlp-proto-http`), `httpcore` and `idna` are also installed by default, for tracing and URL validation.

## License

MIT License (see [LICENSE](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/blob/main/LICENSE))
