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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from novaos_client_sdk.context import ExecutionContext

logger = logging.getLogger("novaos.evaluation.llm_judge")

DEFAULT_JUDGE_MODEL = "gpt-4o-mini"
LLM_GATEWAY_URL_ENV = "LLM_GATEWAY_URL"
DEFAULT_GATEWAY_URL = "http://localhost:4000"

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


async def call_llm_judge(
    messages: list[dict[str, str]],
    model: str | None = None,
    gateway_url: str | None = None,
    execution_context: ExecutionContext | None = None,
    execution_token: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 256,
) -> str:
    """Make an async LLM call through the Gateway for judge scoring.

    Routes through :class:`~novaos_client_sdk.llm.LLMClient` so future
    hardening (retry, mTLS, header propagation) applies automatically.

    The call is tagged with ``novaos_eval_judge: true`` in the request
    metadata so the Gateway can apply PII policy (no message logging).

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
        model: Override model name (default: ``NOVAOS_JUDGE_MODEL`` env or
            ``gpt-4o-mini``).
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
    # Deferred imports — avoid hard dependencies at module load time.
    from novaos_client_sdk.context import ExecutionContext as _ExecutionContext
    from novaos_client_sdk.llm import LLMClient

    resolved_model = model or os.environ.get("NOVAOS_JUDGE_MODEL", DEFAULT_JUDGE_MODEL)
    resolved_url = gateway_url or os.environ.get(LLM_GATEWAY_URL_ENV, DEFAULT_GATEWAY_URL)

    # Build the resolved ExecutionContext with auth validation.
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

    async with LLMClient(base_url=resolved_url, execution_context=resolved_ctx) as llm:
        response = await llm.chat.completions.create(
            model=resolved_model,
            messages=messages,  # type: ignore[arg-type]
            temperature=temperature,
            max_tokens=max_tokens,
            extra_body={
                "metadata": {
                    "novaos_eval_judge": True,
                    "turn_off_message_logging": True,
                },
            },
        )

    choices = response.choices
    if not choices:
        raise ValueError("Judge LLM returned no choices")

    return choices[0].message.content or ""
