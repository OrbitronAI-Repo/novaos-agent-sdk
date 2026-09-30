# Getting started

## Install

```bash
pip install novaos-agent-sdk
```

Requires Python 3.12+. A few features need an optional extra:

| Extra | Adds | Needed for |
|---|---|---|
| `crypto` | `PyJWT[crypto]`, `cryptography` | `JWKSTokenVerifier`, sealed-envelope secrets, Agent Card signature verification |
| `llm` | `openai` | `LLMClient` (calling the LLM Gateway directly) |
| `schema` | `jsonschema` | `validate_against_schema` (structured-output validation) |
| `otel` | OpenTelemetry SDK + OTLP exporter | Exporting traces, not just creating them |
| `evaluation` | `langfuse`, `openai` | The offline/CI agent-evaluation harness |
| `migrate` | `libcst` | `python -m novaos_client_sdk.migrate transform` (the 1.x-to-2.x codemod) |
| `grpc` | `a2a-sdk` (schema only), `grpcio` | The gRPC A2A protocol binding |
| `all` | everything above | — |

```bash
pip install "novaos-agent-sdk[llm]"
```

## Verify the install

```python
import novaos_client_sdk

print(novaos_client_sdk.__version__)
```

## Your first call: run an agent

Every agent-to-agent call goes through [`Client`](reference/api.md#novaos_client_sdk.client), which speaks A2A JSON-RPC 2.0 to an agent's HTTP endpoint. Under A2A the URL *is* the agent — there is no separate agent name to pass — so a `Client` addresses one agent, and `skill_id` picks which of its skills runs.

```python
import asyncio
from novaos_client_sdk import Client

async def main() -> None:
    async with Client(base_url="http://localhost:8000") as client:
        task = await client.send_message("Hello, agent!", skill_id="chat")

        if task.is_terminal and task.artifacts:
            print(task.artifacts[0].text)

asyncio.run(main())
```

`send_message` returns the A2A `Task` the agent created; `task.artifacts` holds its output. (An agent may also answer with a bare message and no task at all — see [The A2A primer](concepts/a2a-primer.md) for the vocabulary, and [Invoking agents](guides/invoking-agents.md) for streaming, reconnecting to a task, and the Execution Gateway path used by end-user applications.)

## Your first agent: build a skill

[`AgentBuilder`](reference/api.md#novaos_client_sdk.agent_builder) registers one or more **skills** — async functions that receive a governed `AgentExecutionContext` (`ctx`) and the caller's input text, and return a `SkillResult`.

```python
from novaos_client_sdk import AgentBuilder, AgentExecutionContext, SkillResult

agent = AgentBuilder(name="greeter", version="1.0.0")

@agent.skill("greet", description="Greet the caller by name")
async def greet(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
    ctx.logger.info("Handling greet skill")
    return SkillResult(output=f"Hello, {input_text or 'there'}!")
```

`agent.asgi_endpoint()` turns that into a ready-made ASGI route handler — mount it at `A2A_JSONRPC_PATH` and the builder handles JSON-RPC decoding, task dispatch, and streaming for you:

```python
from fastapi import FastAPI
from novaos_client_sdk import A2A_JSONRPC_PATH

app = FastAPI()
app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())
```

Run it locally (`uvicorn my_agent:app --port 8000`) and call it with the `Client` snippet above, pointed at `http://localhost:8000`. See [Building an agent](guides/building-an-agent.md) for platform tools, multi-skill routing, and streaming responses.

## Environment variables you'll actually need

Most SDK calls take everything they need as constructor arguments (`base_url`, `auth`, ...) and need no environment configuration. Only agent discovery reads the environment, and only if you use it:

- `AGENT_DNS_URL` — the Agent DNS service URL, e.g. `http://agent-dns.example.internal:8000`. Required by `discover_and_connect` and `get_dns_url_from_env()` when you don't pass a URL explicitly. There is no built-in default host for any environment — the SDK raises a clear error naming the variables it checked if none are set.

Everything else (auth tokens, gateway URLs, tenant IDs) is either passed explicitly by your code or injected by the NovaOS platform at runtime when your agent runs there — see [Authentication](guides/authentication.md).
