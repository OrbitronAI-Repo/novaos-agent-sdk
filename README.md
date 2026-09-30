# NovaOS Agent SDK

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/novaos-agent-sdk.svg)](https://pypi.org/project/novaos-agent-sdk/)
[![Development Status](https://img.shields.io/badge/status-development%20(2.x%20A2A%20line)-orange.svg)](https://orbitronai-repo.github.io/novaos-agent-sdk/versioning-and-support/)

A Python client library for connecting to and communicating with AI Agents on the NovaOS platform. Speaks the **Agent2Agent Protocol (A2A), spec v1.0.1** for agent-to-agent communication, and the NovaOS Execution Gateway for end-user applications.

> **Version lines.** `2.x` is the **Agent2Agent (A2A)** line: a breaking change from `1.x` — the ACP surface (`handle_acp_request`, `from_acp_request`, the `/runs` handler shape) is removed and replaced by the A2A JSON-RPC surface — and it ships a `migrate` tool that rewrites a 1.x agent onto the A2A shell. `1.x` is the **final ACP line**: stable, security and critical fixes only, end of life at the NovaOS ACP→A2A cutover. New agents should target 2.x; pin it as `novaos-agent-sdk>=2,<3`. See [Versioning and support](https://orbitronai-repo.github.io/novaos-agent-sdk/versioning-and-support/).
>
> Published on PyPI as `novaos-agent-sdk`; imported as `novaos_client_sdk`.

## Installation

```bash
pip install novaos-agent-sdk
```

See [Getting started](https://orbitronai-repo.github.io/novaos-agent-sdk/getting-started/) for the full extras table (`crypto`, `llm`, `schema`, `otel`, `evaluation`, `migrate`, `grpc`).

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

## Migrating a 1.x (ACP) agent

The `migrate` extra (`pip install "novaos-agent-sdk[migrate]"`) ships a three-stage CLI to port a 1.x agent onto the A2A shell:

```bash
python -m novaos_client_sdk.migrate analyze ./my-agent --out ./migration-out
python -m novaos_client_sdk.migrate transform ./my-agent --plan ./migration-out/my-agent.plan.json --write
python -m novaos_client_sdk.migrate verify ./my-agent --base-url http://127.0.0.1:8001
```

See [Migrating from 1.x](https://orbitronai-repo.github.io/novaos-agent-sdk/guides/migrating-from-1x/) and the [`migrate` CLI reference](https://orbitronai-repo.github.io/novaos-agent-sdk/reference/migrate-cli/) for the full walkthrough.

## Requirements

- Python 3.12+
- `httpx>=0.24.0` — the only required runtime dependency: HTTP for the A2A JSON-RPC transport, DNS and the gateway services. The A2A wire format is NovaOS's own code (no vendor A2A library), verified against the official `a2a-sdk` in the test suite.

## License

MIT License (see [LICENSE](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/blob/main/LICENSE))
