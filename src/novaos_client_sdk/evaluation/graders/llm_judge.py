"""
LLM-as-judge grader: faithfulness, relevance, context_relevance.

Each evaluator is a RAGAS-style prompt that asks a judge LLM to score the
agent's output on a 0.0–1.0 scale.  The judge call goes through the LLM
Gateway so it inherits tracing, budget enforcement, and rate limiting.

The :func:`call_llm_judge` function routes through :class:`LLMClient` so
any future hardening (retry, mTLS, header propagation) automatically applies
here too.  Callers *must* supply an execution token — a missing token raises
:class:`~novaos_client_sdk.exceptions.MissingExecutionTokenError` before any
network call is made, rather than relying on the Gateway to reject an
unauthenticated request with a 401.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from novaos_client_sdk.context import ExecutionContext

logger = logging.getLogger("novaos.evaluation.llm_judge")

DEFAULT_JUDGE_MODEL = "gpt-4o-mini"
LLM_GATEWAY_URL_ENV = "LLM_GATEWAY_URL"
DEFAULT_GATEWAY_URL = "http://localhost:4000"
GATEWAY_DEFAULTS_PATH = "/gateway/defaults"
GATEWAY_JUDGE_DEFAULT_NAMESPACE = "llm_judge"
GATEWAY_JUDGE_DEFAULT_KEY = "default_model"
GATEWAY_DEFAULTS_TIMEOUT_SECONDS = 3.0

SCORE_PARSE_PATTERN = re.compile(r'"?score"?\s*[:=]\s*([01](?:\.\d+)?)')


@dataclass(frozen=True)
class JudgePromptTemplate:
    """Immutable prompt template for a single LLM judge evaluator."""

    name: str
    system_prompt: str
    user_prompt_template: str
    requires_contexts: bool = False


JUDGE_TEMPLATES: dict[str, JudgePromptTemplate] = {
    "faithfulness": JudgePromptTemplate(
        name="faithfulness",
        system_prompt=(
            "You are an impartial evaluation judge.  Your task is to assess whether "
            "the assistant's answer is grounded in (faithful to) the provided reference "
            "contexts.  Every claim in the answer must be directly supported by the contexts. "
            "Penalise hallucinated facts that are not in the contexts."
        ),
        user_prompt_template=(
            "## Reference Contexts\n{contexts}\n\n"
            "## User Question\n{user_input}\n\n"
            "## Assistant Answer\n{output}\n\n"
            "Rate the faithfulness of the answer on a scale from 0.0 (completely "
            "unfaithful / hallucinated) to 1.0 (fully grounded in the contexts).\n\n"
            'Respond with ONLY a JSON object: {{"score": <float>, "reasoning": "<brief explanation>"}}'
        ),
        requires_contexts=True,
    ),
    "relevance": JudgePromptTemplate(
        name="relevance",
        system_prompt=(
            "You are an impartial evaluation judge.  Your task is to assess whether "
            "the assistant's answer is relevant and helpful in response to the user's "
            "question.  A relevant answer directly addresses the question, is complete, "
            "and does not include unnecessary information."
        ),
        user_prompt_template=(
            "## User Question\n{user_input}\n\n"
            "## Assistant Answer\n{output}\n\n"
            "Rate the relevance of the answer on a scale from 0.0 (completely "
            "irrelevant) to 1.0 (perfectly relevant and helpful).\n\n"
            'Respond with ONLY a JSON object: {{"score": <float>, "reasoning": "<brief explanation>"}}'
        ),
        requires_contexts=False,
    ),
    "context_relevance": JudgePromptTemplate(
        name="context_relevance",
        system_prompt=(
            "You are an impartial evaluation judge.  Your task is to assess whether "
            "the retrieved contexts are relevant to the user's question.  Relevant "
            "contexts contain information that is useful for answering the question. "
            "Irrelevant or noisy contexts should lower the score."
        ),
        user_prompt_template=(
            "## User Question\n{user_input}\n\n"
            "## Retrieved Contexts\n{contexts}\n\n"
            "Rate the relevance of the retrieved contexts to the user's question on "
            "a scale from 0.0 (completely irrelevant) to 1.0 (all contexts are highly "
            "relevant).\n\n"
            'Respond with ONLY a JSON object: {{"score": <float>, "reasoning": "<brief explanation>"}}'
        ),
        requires_contexts=True,
    ),
}


def get_judge_template(evaluator_name: str) -> JudgePromptTemplate | None:
    """Look up a built-in judge template by evaluator name."""
    return JUDGE_TEMPLATES.get(evaluator_name)


def build_judge_messages(
    template: JudgePromptTemplate,
    transcript: dict[str, Any],
) -> list[dict[str, str]]:
    """Render the judge system + user messages from a transcript dict."""
    contexts_raw = transcript.get("contexts") or []
    context_texts: list[str] = []
    for i, ctx in enumerate(contexts_raw, 1):
        content = ctx.get("content", "") if isinstance(ctx, dict) else str(ctx)
        source = ctx.get("source", "") if isinstance(ctx, dict) else ""
        header = f"[{i}]"
        if source:
            header += f" (source: {source})"
        context_texts.append(f"{header} {content}")
    contexts_str = "\n\n".join(context_texts) if context_texts else "(none)"

    user_input = transcript.get("user_input", "")
    output = transcript.get("output", "")

    user_content = template.user_prompt_template.format(
        contexts=contexts_str,
        user_input=user_input,
        output=output,
    )

    return [
        {"role": "system", "content": template.system_prompt},
        {"role": "user", "content": user_content},
    ]


def parse_judge_score(raw_text: str) -> tuple[float | None, str]:
    """Extract a 0.0–1.0 score and reasoning from the judge LLM response.

    Returns (score, reasoning).  score is None if parsing fails.
    """
    text = raw_text.strip()

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            score_val = data.get("score")
            reasoning = data.get("reasoning", "")
            if score_val is not None:
                score_float = float(score_val)
                return max(0.0, min(1.0, score_float)), str(reasoning)
    except (json.JSONDecodeError, ValueError, TypeError):
        pass

    match = SCORE_PARSE_PATTERN.search(text)
    if match:
        score_float = float(match.group(1))
        return max(0.0, min(1.0, score_float)), text

    return None, text


_DEFAULT_CACHE_TTL_SECONDS = 60.0
_DEFAULT_CACHE_MAX_ENTRIES = 16
# gateway base URL -> (fetched at, default model or None). A failed lookup is cached too, so a
# slow or down gateway costs one 3 s wait per TTL, not one per judge call.
_default_cache: dict[str, tuple[float, str | None]] = {}


def clear_gateway_default_cache() -> None:
    """Forget the cached default judge model. For tests."""
    _default_cache.clear()


async def fetch_gateway_judge_default(
    gateway_url: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> str | None:
    """Return the default judge model the LLM Gateway serves, or None.

    Reads ``GET /gateway/defaults``, which needs no token, and takes ``llm_judge`` /
    ``default_model`` from the response (``{"defaults": {namespace: {key: value}}}``, the platform
    defaults). Any failure (gateway down, no such route on an older gateway, no
    default set) returns None so the caller falls back to its own default. The result, failure
    included, is cached per gateway URL for 60 seconds.
    """
    base = (gateway_url or os.environ.get(LLM_GATEWAY_URL_ENV, DEFAULT_GATEWAY_URL)).rstrip("/")
    if base.endswith("/v1"):
        base = base[: -len("/v1")]
    now = time.monotonic()
    cached = _default_cache.get(base)
    if cached is not None and now - cached[0] < _DEFAULT_CACHE_TTL_SECONDS:
        return cached[1]
    own_client = client is None
    http = client or httpx.AsyncClient(timeout=GATEWAY_DEFAULTS_TIMEOUT_SECONDS)
    model: str | None = None
    try:
        response = await http.get(base + GATEWAY_DEFAULTS_PATH)
        response.raise_for_status()
        value = response.json()["defaults"][GATEWAY_JUDGE_DEFAULT_NAMESPACE][
            GATEWAY_JUDGE_DEFAULT_KEY
        ]
        model = value.strip() if isinstance(value, str) and value.strip() else None
    except (httpx.HTTPError, ValueError, KeyError, TypeError):
        logger.debug("No default judge model from %s", base, exc_info=True)
    finally:
        if own_client:
            await http.aclose()
    if len(_default_cache) >= _DEFAULT_CACHE_MAX_ENTRIES:
        _default_cache.pop(next(iter(_default_cache)))  # the oldest entry
    _default_cache[base] = (now, model)
    return model


async def call_llm_judge(
    messages: list[dict[str, str]],
    model: str | None = None,
    gateway_url: str | None = None,
    execution_context: ExecutionContext | None = None,
    execution_token: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 256,
) -> str:
    """Make an async LLM call through the Gateway for judge scoring (offline evaluation).

    The model is the ``model`` argument, else ``NOVAOS_JUDGE_MODEL``, else the default
    judge model the Gateway serves (:func:`fetch_gateway_judge_default`), else
    ``gpt-4o-mini``. That suits local and offline evaluation. The agent loop uses
    :func:`call_pinned_judge` instead, which never reads the environment.

    Routes through :class:`~novaos_client_sdk.llm.LLMClient` so future
    hardening (retry, mTLS, header propagation) applies automatically.

    The call is tagged with ``novaos_eval_judge: true`` in the request
    metadata so the Gateway can recognise a judge call.  The Gateway turns
    message logging off for every request itself and refuses the flag when a
    client sends it.

    Auth is *required*.  Resolution order:

    1. ``execution_context`` (preferred) — passed directly to ``LLMClient``.
    2. ``execution_token`` string (deprecated) — wrapped into a minimal
       :class:`~novaos_client_sdk.context.ExecutionContext`.
    3. ``NOVAOS_EXECUTION_TOKEN`` environment variable — used automatically
       when neither argument is provided (local dev fallback from ECO-01).

    If no token is available after all three steps,
    :class:`~novaos_client_sdk.exceptions.MissingExecutionTokenError` is
    raised *before* any network call is made.

    Args:
        messages: Chat messages in OpenAI format.
        model: Override model name (default: ``NOVAOS_JUDGE_MODEL`` env, then the
            Gateway's default judge model, then ``gpt-4o-mini``).
        gateway_url: Override LLM Gateway URL (default: ``LLM_GATEWAY_URL``
            env or ``http://localhost:4000``).
        execution_context: :class:`~novaos_client_sdk.context.ExecutionContext`
            carrying the execution token and tenant headers.
        execution_token: **Deprecated.** Raw JWT string.  Prefer passing an
            ``ExecutionContext`` via ``execution_context``.
        temperature: Sampling temperature (default: 0.0 for determinism).
        max_tokens: Maximum tokens in the judge response (default: 256).

    Returns:
        Raw text content of the judge LLM response.

    Raises:
        MissingExecutionTokenError: No execution token was available.
        NovaOSError: ``openai`` package is not installed.
        openai.APIStatusError: Non-2xx response from the Gateway.
        ValueError: The Gateway returned no choices.
    """
    resolved_model = model or os.environ.get("NOVAOS_JUDGE_MODEL")
    if not resolved_model:
        resolved_model = await fetch_gateway_judge_default(gateway_url) or DEFAULT_JUDGE_MODEL
    return await _call_judge_gateway(
        messages,
        model=resolved_model,
        gateway_url=gateway_url,
        execution_context=execution_context,
        execution_token=execution_token,
        temperature=temperature,
        max_tokens=max_tokens,
    )


@dataclass(frozen=True)
class PinnedJudgeReply:
    """A judge reply and the model version the gateway says served it (``None`` if it did not)."""

    content: str
    model_version: str | None = None


async def call_pinned_judge_reply(
    messages: list[dict[str, str]],
    *,
    model: str,
    gateway_url: str | None = None,
    execution_context: ExecutionContext | None = None,
    temperature: float = 0.0,
    max_tokens: int = 256,
) -> PinnedJudgeReply:
    """:func:`call_pinned_judge`, returning the served model version with the text."""
    if not model or not model.strip():
        raise ValueError("call_pinned_judge requires a model; it does not fall back to a default")
    return await _request_judge(
        messages,
        model=model,
        gateway_url=gateway_url,
        execution_context=execution_context,
        execution_token=None,
        temperature=temperature,
        max_tokens=max_tokens,
    )


async def call_pinned_judge(
    messages: list[dict[str, str]],
    *,
    model: str,
    gateway_url: str | None = None,
    execution_context: ExecutionContext | None = None,
    temperature: float = 0.0,
    max_tokens: int = 256,
) -> str:
    """Make a judge call to a model chosen by the caller, never by the environment.

    This is the judge path for the agent loop, where the rubric pins the model.
    Unlike :func:`call_llm_judge` it does not read ``NOVAOS_JUDGE_MODEL`` and has
    no default model, so a pod cannot switch the judge by setting a variable.
    ``NOVAOS_JUDGE_MODEL`` stays an offline-evaluation setting.

    The call carries the same ``novaos_eval_judge`` tag as every judge call. The
    tag is set by this client, so it helps the Gateway catch a misconfigured
    call; it does not stop a caller that leaves it out.

    Raises:
        ValueError: ``model`` is empty, raised before any network call.
        MissingExecutionTokenError: No execution token was available.
    """
    reply = await call_pinned_judge_reply(
        messages,
        model=model,
        gateway_url=gateway_url,
        execution_context=execution_context,
        temperature=temperature,
        max_tokens=max_tokens,
    )
    return reply.content


def _resolve_judge_context(
    execution_context: ExecutionContext | None, execution_token: str | None
) -> ExecutionContext:
    """The context a judge call authenticates with; raises before any network call."""
    from novaos_client_sdk.context import ExecutionContext as _ExecutionContext

    # Resolution order: context with token → execution_token string → env var.
    # If execution_context is provided but has no token, fall through to the
    # execution_token kwarg so callers that pass both don't get a spurious error.
    if execution_context is not None and execution_context.execution_token:
        resolved_ctx = execution_context
    elif execution_token:
        resolved_ctx = _ExecutionContext(execution_token=execution_token)
    else:
        # Fall back to env-var; from_headers({}) reads NOVAOS_EXECUTION_TOKEN.
        resolved_ctx = _ExecutionContext.from_headers({})

    # Validate early — raises MissingExecutionTokenError before any network call.
    resolved_ctx.require_token()
    return resolved_ctx


async def _call_judge_gateway(
    messages: list[dict[str, str]],
    *,
    model: str,
    gateway_url: str | None,
    execution_context: ExecutionContext | None,
    execution_token: str | None,
    temperature: float,
    max_tokens: int,
) -> str:
    """Send one tagged judge request through the LLM Gateway."""
    return (
        await _request_judge(
            messages,
            model=model,
            gateway_url=gateway_url,
            execution_context=execution_context,
            execution_token=execution_token,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    ).content


async def _request_judge(
    messages: list[dict[str, str]],
    *,
    model: str,
    gateway_url: str | None,
    execution_context: ExecutionContext | None,
    execution_token: str | None,
    temperature: float,
    max_tokens: int,
) -> PinnedJudgeReply:
    # Deferred import — avoid a hard dependency at module load time.
    from novaos_client_sdk.llm import LLMClient

    resolved_url = gateway_url or os.environ.get(LLM_GATEWAY_URL_ENV, DEFAULT_GATEWAY_URL)
    resolved_ctx = _resolve_judge_context(execution_context, execution_token)

    async with LLMClient(base_url=resolved_url, execution_context=resolved_ctx) as llm:
        response = await llm.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
            temperature=temperature,
            max_tokens=max_tokens,
            extra_body={
                "metadata": {
                    "novaos_eval_judge": True,
                },
            },
        )

    choices = response.choices
    if not choices:
        raise ValueError("Judge LLM returned no choices")

    return PinnedJudgeReply(choices[0].message.content or "", getattr(response, "model", None))
