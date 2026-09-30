"""
``@evaluate()`` decorator for NovaOS skill handlers.

Wraps a skill handler to capture runtime data, run graders, and optionally
send traces/scores to Langfuse -- all without changing the handler's return value.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import os
import random
from collections.abc import Callable, Sequence
from typing import Any

from novaos_client_sdk.evaluation.context import (
    EvaluationContext,
    _reset_eval_context,
    _set_eval_context,
    get_eval_context,
)
from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.graders.cost import CostGrader
from novaos_client_sdk.evaluation.graders.custom import get_custom_graders
from novaos_client_sdk.evaluation.graders.error_rate import ErrorRateGrader
from novaos_client_sdk.evaluation.graders.latency import LatencyGrader
from novaos_client_sdk.evaluation.graders.tool_expected import ToolCallExpectedGrader
from novaos_client_sdk.evaluation.graders.tool_success import ToolCallSuccessGrader
from novaos_client_sdk.evaluation.graders.ttft import TTFTGrader
from novaos_client_sdk.evaluation.models import Score

logger = logging.getLogger("novaos.evaluation")

_DEFAULT_GRADERS: list[type[Grader]] = [
    ToolCallSuccessGrader,
    LatencyGrader,
    CostGrader,
    TTFTGrader,
    ErrorRateGrader,
]

PRESETS: dict[str, dict[str, Any]] = {
    "rag": {
        "extra_grader_classes": [],
        "langfuse_evaluators": ["faithfulness", "relevance", "context_relevance"],
        "max_latency_ms": 15_000,
        "max_cost_usd": 0.15,
    },
    "tools": {
        "extra_grader_classes": [ToolCallExpectedGrader],
        "langfuse_evaluators": [],
        "max_latency_ms": 20_000,
        "max_cost_usd": 0.20,
    },
    "chat": {
        "extra_grader_classes": [],
        "langfuse_evaluators": ["relevance"],
        "max_latency_ms": 10_000,
        "max_cost_usd": 0.10,
    },
    "code": {
        "extra_grader_classes": [],
        "langfuse_evaluators": [],
        "max_latency_ms": 30_000,
        "max_cost_usd": 0.25,
    },
}


def evaluate(
    *,
    preset: str | None = None,
    max_latency_ms: float | None = None,
    max_cost_usd: float | None = None,
    max_ttft_ms: float | None = None,
    graders: Sequence[Grader] | None = None,
    extra_graders: Sequence[Grader] | None = None,
    langfuse_evaluators: list[str] | None = None,
    sample_rate: float = 1.0,
    langfuse_eval_sample_rate: float = 1.0,
    run_async: bool = True,
    agent_version: str | None = None,
    environment: str | None = None,
    judge_model: str | None = None,
) -> Callable:
    """Decorator that adds evaluation to a NovaOS skill handler.

    Usage::

        agent = AgentBuilder("my-agent", "1.0.0")

        @agent.skill("summarize")
        @evaluate(preset="rag", max_latency_ms=5000)
        async def summarize(ctx, input_text):
            ...
            return SkillResult(output="summary")
    """
    resolved_preset = PRESETS.get(preset, {}) if preset else {}
    resolved_langfuse_evals = (
        langfuse_evaluators
        if langfuse_evaluators is not None
        else resolved_preset.get("langfuse_evaluators", [])
    )

    def _build_graders() -> list[Grader]:
        if graders is not None:
            return list(graders) + list(get_custom_graders())

        result: list[Grader] = []
        for cls in _DEFAULT_GRADERS:
            if cls is LatencyGrader:
                threshold = (
                    max_latency_ms
                    if max_latency_ms is not None
                    else resolved_preset.get("max_latency_ms", 10_000)
                )
                result.append(LatencyGrader(max_latency_ms=threshold))
            elif cls is CostGrader:
                threshold = (
                    max_cost_usd
                    if max_cost_usd is not None
                    else resolved_preset.get("max_cost_usd", 0.10)
                )
                result.append(CostGrader(max_cost_usd=threshold))
            elif cls is TTFTGrader:
                threshold = max_ttft_ms if max_ttft_ms is not None else 2_000
                result.append(TTFTGrader(max_ttft_ms=threshold))
            else:
                result.append(cls())

        for cls in resolved_preset.get("extra_grader_classes", []):
            result.append(cls())

        if extra_graders:
            result.extend(extra_graders)

        result.extend(get_custom_graders())
        return result

    def decorator(fn: Callable) -> Callable:
        _is_async = inspect.iscoroutinefunction(fn)

        async def _invoke(
            fn: Callable, ctx: Any, input_text: str, *args: Any, **kwargs: Any
        ) -> Any:
            if _is_async:
                return await fn(ctx, input_text, *args, **kwargs)
            return await asyncio.to_thread(fn, ctx, input_text, *args, **kwargs)

        @functools.wraps(fn)
        async def wrapper(ctx: Any, input_text: str, *args: Any, **kwargs: Any) -> Any:
            if random.random() > sample_rate:
                return await _invoke(fn, ctx, input_text, *args, **kwargs)

            env = environment or os.environ.get("ENVIRONMENT", "development")
            version = agent_version or os.environ.get("AGENT_VERSION", "0.0.0")

            runtime_ctx = getattr(ctx, "runtime_context", None)
            skill_name = getattr(fn, "__name__", "")

            parent_ctx = get_eval_context()
            agent_name = ""
            if parent_ctx.is_active:
                agent_name = parent_ctx.transcript.agent_name or ""
                logger.debug(
                    "Overwriting active EvaluationContext from AgentBuilder; "
                    "grading will use the @evaluate context only"
                )
            if not agent_name and runtime_ctx is not None:
                agent_name = getattr(runtime_ctx, "agent_name", "")

            eval_ctx = EvaluationContext(
                agent_name=agent_name,
                agent_version=version,
                environment=env,
                runtime_context=runtime_ctx,
                user_input=input_text,
                skill_name=skill_name,
            )

            ctx_token = _set_eval_context(eval_ctx)

            try:
                result = await _invoke(fn, ctx, input_text, *args, **kwargs)

                output_str = ""
                if hasattr(result, "output"):
                    output_str = str(result.output) if result.output is not None else ""
                eval_ctx.set_output(output_str)

                return result
            except Exception as exc:
                eval_ctx.record_error(
                    error_type=type(exc).__name__,
                    message=str(exc),
                    recoverable=False,
                )
                raise
            finally:
                eval_ctx.finalize()
                _reset_eval_context(ctx_token)

                if run_async:
                    task = asyncio.create_task(
                        _run_evaluation(
                            eval_ctx,
                            _build_graders(),
                            resolved_langfuse_evals,
                            langfuse_eval_sample_rate,
                            judge_model,
                        )
                    )
                    task.add_done_callback(_handle_eval_task_result)
                else:
                    await _run_evaluation(
                        eval_ctx,
                        _build_graders(),
                        resolved_langfuse_evals,
                        langfuse_eval_sample_rate,
                        judge_model,
                    )

        return wrapper

    return decorator


def _handle_eval_task_result(task: asyncio.Task[Any]) -> None:
    """Log exceptions from fire-and-forget evaluation tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("Background evaluation task failed: %s", exc, exc_info=exc)


async def _run_evaluation(
    eval_ctx: EvaluationContext,
    grader_list: list[Grader],
    langfuse_evaluators: list[str],
    langfuse_eval_sample_rate: float,
    judge_model: str | None = None,
) -> list[Score]:
    """Run all graders, LLM judges, and optionally send results to Langfuse."""
    transcript = eval_ctx.to_transcript()
    scores: list[Score] = []

    for grader in grader_list:
        try:
            score = grader.grade(transcript)
            scores.append(score)
        except Exception:
            logger.exception("Grader %s failed", grader.name)

    execution_token = eval_ctx.execution_token or ""

    try:
        from novaos_client_sdk.evaluation.langfuse_service import LangfuseService

        service = LangfuseService.get_instance()
        if service is not None:
            trace_id = service.trace_and_score(eval_ctx.transcript, scores)
            if trace_id:
                transcript["trace_id"] = trace_id

            if langfuse_evaluators and random.random() <= langfuse_eval_sample_rate:
                for evaluator_name in langfuse_evaluators:
                    try:
                        llm_score = await service.run_llm_evaluator(
                            evaluator_name,
                            transcript,
                            judge_model=judge_model,
                            execution_token=execution_token,
                        )
                        if llm_score is not None:
                            scores.append(llm_score)
                    except Exception:
                        logger.exception("LLM judge evaluator %s failed", evaluator_name)
    except ImportError:
        pass
    except Exception:
        logger.debug("Langfuse integration unavailable", exc_info=True)

    return scores
