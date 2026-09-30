"""BAML + LLM Gateway integration helpers.

Provides utilities for BAML agents that use the NovaOS LLM Gateway
for per-request execution token auth and Langfuse trace propagation.

BAML's ``ClientRegistry`` allows overriding the LLM client configuration
at call time. The ``baml_llm_options`` helper creates a registry that
sets the ``api_key`` to the execution token from the current request context
and optionally forwards Langfuse trace metadata via ``langfuse_*`` headers
so that the LLM Gateway (LiteLLM) groups generations under a single trace.

Example:
    ```python
    from novaos_client_sdk.context import ExecutionContext
    from novaos_client_sdk.baml_helpers import baml_llm_options

    ctx = ExecutionContext.from_headers(request.headers)
    opts = baml_llm_options(ctx, trace_name="marketplace-assistant")
    result = await b.DecomposeTask(input=input_obj, baml_options=opts)
    ```
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any

from novaos_client_sdk.context import ExecutionContext

logger = logging.getLogger(__name__)


def baml_llm_options(
    exec_ctx: ExecutionContext,
    model: str = "gpt-4o",
    client_name: str = "NovaOSLLM",
    *,
    trace_id: str | None = None,
    trace_name: str | None = None,
    generation_name: str | None = None,
    session_id: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Build baml_options with per-request execution token auth and Langfuse trace context.

    BAML's ``ClientRegistry`` allows overriding the LLM client configuration
    at call time. This creates a registry that sets the ``api_key`` to the
    execution token from the current request context, routing through
    the LLM Gateway.

    When Langfuse trace parameters are provided, they are forwarded to the
    LLM Gateway as ``langfuse_*`` HTTP headers. The LiteLLM proxy reads
    these headers and attaches the generation to the specified Langfuse trace,
    enabling hierarchical per-execution trace views.

    Args:
        exec_ctx: The current ExecutionContext (contains execution_token).
        model: Model name to use (default: "gpt-4o").
        client_name: Name of the BAML client to override (must match clients.baml).
        trace_id: Langfuse trace ID to group multiple LLM calls under one trace.
            If not provided, a new UUID is auto-generated so that all calls
            within the same ``baml_llm_options`` invocation share one trace.
        trace_name: Human-readable name for the Langfuse trace
            (e.g. "joke-agent", "marketplace-assistant").
        generation_name: Name for this specific LLM generation span
            (e.g. "GetJoke", "ClassifyDNSReadQuery").
        session_id: Langfuse session ID (defaults to ``exec_ctx.run_id``
            so all runs in a conversation share a session).
        tags: Optional list of tags for the Langfuse trace.

    Returns:
        A dict suitable for passing as ``baml_options`` to any BAML function.
    """
    try:
        import baml_py
    except ImportError as err:
        raise ImportError(
            "The 'baml-py' package is required for BAML helpers. "
            "Install it with: pip install baml-py"
        ) from err

    gateway_url = os.environ.get("LLM_GATEWAY_URL", "")

    # Build Langfuse trace headers for the LiteLLM proxy.
    # LiteLLM reads headers prefixed with ``langfuse_`` and maps them
    # to the corresponding Langfuse trace/generation fields.
    langfuse_headers: dict[str, str] = {}

    resolved_trace_id = trace_id or str(uuid.uuid4())
    langfuse_headers["langfuse_trace_id"] = resolved_trace_id

    if trace_name:
        langfuse_headers["langfuse_trace_name"] = trace_name

    if generation_name:
        langfuse_headers["langfuse_generation_name"] = generation_name

    resolved_session_id = session_id or exec_ctx.run_id
    if resolved_session_id:
        langfuse_headers["langfuse_session_id"] = resolved_session_id

    if exec_ctx.user_id:
        langfuse_headers["langfuse_trace_user_id"] = exec_ctx.user_id

    trace_metadata: dict[str, Any] = {}
    if exec_ctx.tenant_id:
        trace_metadata["tenant_id"] = exec_ctx.tenant_id
    if exec_ctx.agent_id:
        trace_metadata["agent_id"] = exec_ctx.agent_id
    if trace_metadata:
        langfuse_headers["langfuse_trace_metadata"] = json.dumps(trace_metadata)

    if tags:
        langfuse_headers["langfuse_tags"] = json.dumps(tags)

    logger.debug(
        "BAML LLM options: trace_id=%s trace_name=%s generation=%s",
        resolved_trace_id,
        trace_name,
        generation_name,
    )

    cr = baml_py.ClientRegistry()
    cr.add_llm_client(
        name=client_name,
        provider="openai",
        options={
            "model": model,
            "api_key": exec_ctx.require_token(),
            "base_url": gateway_url,
            "headers": langfuse_headers,
        },
    )
    cr.set_primary(client_name)
    return {"client_registry": cr}
