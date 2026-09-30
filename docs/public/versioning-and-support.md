# Versioning and support

`novaos-agent-sdk` follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and is developed across two parallel release lines that speak different agent protocols.

| Line | Protocol | Status |
|---|---|---|
| `2.x` (this SDK) | A2A (Agent2Agent) | **The current line.** New features and fixes land here. |
| `1.x` | ACP (Agent Communication Protocol) | **Final ACP line.** Stable. Security and critical fixes only — no new features. Reaches end of life at the NovaOS ACP→A2A cutover. |

Pin `2.x` as:

```
novaos-agent-sdk>=2,<3
```

## What changed in 2.x

2.x is a breaking change from `1.x`: the ACP-specific surface (`AgentBuilder.handle_acp_request`, `AgentExecutionContext.from_acp_request`, the `/runs` handler shape) is removed outright and replaced by an A2A-native one — no deprecated aliases, no coexistence period. See [Migrating from 1.x](guides/migrating-from-1x.md) for what to change in an existing agent, and the [`migrate` CLI reference](reference/migrate-cli.md) for the tool that automates most of it.

## What "current" means for `2.x`

- The public API — everything importable from `novaos_client_sdk` directly, and documented in the [API reference](reference/api.md) — will not change in a breaking way within `2.x`.
- New features and non-security fixes land on this line.
- There is no fixed end-of-life date published yet.

## Staying on 1.x

`1.x` is still available for agents that have not migrated yet: pin it as `novaos-agent-sdk>=1,<2`, and install `"novaos-agent-sdk<2"` to read its README for the ACP-line quick start. It receives security and critical fixes only, and reaches end of life at the NovaOS ACP→A2A cutover — new agents should target `2.x` instead.

## Reporting a bug

Open an issue on this repository's [Issues](https://github.com/OrbitronAI-Repo/novaos-agent-sdk/issues) page with a minimal reproduction, the SDK version (`novaos_client_sdk.__version__`), and your Python version. See [Security](security.md) instead if what you found is a vulnerability rather than a functional bug.

There is no formal support SLA — the SDK is maintained on a best-effort basis.
