# NovaOS Agent SDK

The NovaOS Agent SDK is a Python library for two roles:

- **Building agents.** [`AgentBuilder`](reference/api.md#novaos_client_sdk.agent_builder) registers skills and serves them over the Agent2Agent Protocol (A2A): mount one ASGI endpoint and the builder handles JSON-RPC dispatch, task lifecycle, and streaming for you.
- **Calling agents.** [`Client`](reference/api.md#novaos_client_sdk.client) lets any Python application send a message to an agent, stream its progress, and query, cancel, or list its tasks — whether the caller is another agent or an end-user application.

Both roles ship in the same package, `novaos-agent-sdk`, imported as `novaos_client_sdk`.

## Two version lines

`2.x` (this line) is the **Agent2Agent (A2A)** SDK: it speaks A2A JSON-RPC 2.0 end to end. It is a breaking change from `1.x` — the ACP-era surface is gone outright, not deprecated, so there is no dual-protocol serving to migrate off of gradually. `1.x` is the **final ACP line**: stable, security and critical fixes only. There is no hosted `1.x` docs site yet; to read its docs, install `novaos-agent-sdk<2` and read the README that ships with it. Migrating an existing `1.x` agent onto this line? Start with [Migrating from 1.x](guides/migrating-from-1x.md).

## Where to go next

- New to the SDK? Start with [Getting started](getting-started.md) — install, verify the import, make your first call, and build your first agent.
- New to A2A itself? [The A2A primer](concepts/a2a-primer.md) covers the vocabulary the rest of these docs assume.
- Calling an existing agent? See [Invoking agents](guides/invoking-agents.md) and [Authentication](guides/authentication.md).
- Building an agent? See [Building an agent](guides/building-an-agent.md) and [Platform tools](guides/platform-tools.md).
- Looking for a specific class or function? See the [API reference](reference/api.md).

## Reporting issues

Found a bug or have a question about the SDK itself? Open an issue on this repository's [Issues](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/issues) page. For a security vulnerability, see [Security](security.md) instead of filing a public issue.
