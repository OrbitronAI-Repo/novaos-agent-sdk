# API reference

Generated from source docstrings. Everything here is importable from `novaos_client_sdk` directly (`from novaos_client_sdk import Client`) unless noted otherwise.

## Agent building

::: novaos_client_sdk.agent_builder

::: novaos_client_sdk.agent_context

::: novaos_client_sdk.context

::: novaos_client_sdk.agent_types

::: novaos_client_sdk.tools

## Calling agents

::: novaos_client_sdk.client

::: novaos_client_sdk.gateway

::: novaos_client_sdk.discovery

::: novaos_client_sdk.dns

## The A2A protocol layer

Types and wire constants for the Agent2Agent protocol itself — see the [A2A primer](../concepts/a2a-primer.md) for the concepts behind them.

::: novaos_client_sdk.a2a_types

::: novaos_client_sdk.a2a_task

::: novaos_client_sdk.a2a_artifact

::: novaos_client_sdk.a2a_events

::: novaos_client_sdk.a2a_agent_card

::: novaos_client_sdk.a2a_jsonrpc

::: novaos_client_sdk.a2a_extensions

## Task and push-notification persistence

The stores and helpers `AgentBuilder` uses by default to track a task's lifecycle and any registered push-notification webhooks; swap in your own by implementing the same interface.

::: novaos_client_sdk.a2a_task_store

::: novaos_client_sdk.a2a_task_updater

::: novaos_client_sdk.a2a_event_queue

::: novaos_client_sdk.a2a_push_notification

::: novaos_client_sdk.a2a_push_notification_store

## Authentication and configuration

::: novaos_client_sdk.auth

::: novaos_client_sdk.auth_interceptor

::: novaos_client_sdk.interceptors

::: novaos_client_sdk.config

## Models

::: novaos_client_sdk.models

::: novaos_client_sdk.capabilities

## Standardized messages

::: novaos_client_sdk.messages

::: novaos_client_sdk.message_builder

::: novaos_client_sdk.message_parser

::: novaos_client_sdk.message_validator

## Exceptions

::: novaos_client_sdk.exceptions

## Platform service clients

These are part of the core package — no extra install required — but each talks to a specific platform service, so you'll typically only use the ones your agent actually depends on.

::: novaos_client_sdk.memory

::: novaos_client_sdk.observability

::: novaos_client_sdk.heartbeat

::: novaos_client_sdk.mcp

## Utilities

::: novaos_client_sdk.logging

::: novaos_client_sdk.pii

::: novaos_client_sdk.utils

::: novaos_client_sdk._resilience

## Optional extras

These modules need an additional extra installed (`pip install "novaos-agent-sdk[extra]"`) to actually function — the base package imports without them, but calling into these fails with a clear error naming the extra until it's installed.

### `crypto` — execution token verification, sealed secrets, and card signing

::: novaos_client_sdk.token_verifier

::: novaos_client_sdk.a2a_user_context

::: novaos_client_sdk.a2a_sealed_envelope

::: novaos_client_sdk.a2a_pod_keyring

::: novaos_client_sdk.a2a_card_signing

### `llm` — direct LLM Gateway access

::: novaos_client_sdk.llm

### `schema` — structured-output validation

::: novaos_client_sdk.schema_validation

### `otel` — exporting traces

Tracing helpers (`get_traceparent`, `inject_trace_headers`, `traced_tool_call`, `configure_otel`, ...) work without this extra as no-ops; install it to actually export spans to an OpenTelemetry collector.

::: novaos_client_sdk.tracing

### `evaluation` — offline/CI agent evaluation

::: novaos_client_sdk.evaluation

### `migrate` — migrating a 1.x agent to A2A

A CLI, not an importable API — see the [`migrate` CLI reference](migrate-cli.md) for `analyze`/`transform`/`verify`. Requires `pip install "novaos-agent-sdk[migrate]"`.
