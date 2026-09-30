"""
Langfuse integration for the NovaOS evaluation framework.

Provides trace creation, score writing, dataset lifecycle, and
LLM-as-judge evaluation.

LLM-as-judge evaluators (faithfulness, relevance, context_relevance) run
inside the SDK via ``run_llm_evaluator``.  The judge LLM call goes through
the LLM Gateway (OpenAI-compatible ``/v1/chat/completions``), so it inherits
tracing, budget enforcement, and rate limiting.  The ``metadata`` field on
the request includes ``turn_off_message_logging: true`` to prevent the judge
prompt (which contains the agent's full transcript) from being persisted in
Langfuse by the Gateway.

All operations degrade gracefully when the ``langfuse`` package is not
installed or the service is unreachable.

Requires: pip install novaos-agent-sdk[evaluation]
"""

from __future__ import annotations

import logging
import os
from typing import Any

from novaos_client_sdk.evaluation.models import Score, Transcript

logger = logging.getLogger("novaos.evaluation.langfuse")

_instance: LangfuseService | None = None


def _item_meets_threshold(
    client: Any,
    item: Any,
    min_score: float,
    score_name: str | None,
) -> bool:
    """Check whether a dataset item's linked runs achieved ``min_score``."""
    try:
        for run in item.observed_runs:
            trace = client.get_trace(run.trace_id)
            for score in getattr(trace, "scores", []):
                if score_name and score.name != score_name:
                    continue
                if score.value is not None and score.value >= min_score:
                    return True
    except Exception:
        logger.debug("Could not inspect runs for item %s", getattr(item, "id", "?"))
    return False


class LangfuseService:
    """Singleton wrapper around the Langfuse Python SDK."""

    def __init__(
        self,
        secret_key: str | None = None,
        public_key: str | None = None,
        host: str | None = None,
    ):
        try:
            from langfuse import Langfuse
        except ImportError:
            logger.debug("langfuse package not installed -- evaluation tracing disabled")
            self._client = None
            return

        self._secret_key = secret_key or os.environ.get("LANGFUSE_SECRET_KEY", "")
        self._public_key = public_key or os.environ.get("LANGFUSE_PUBLIC_KEY", "")
        self._host = host or os.environ.get("LANGFUSE_HOST", "")

        if not self._secret_key or not self._public_key:
            logger.debug("Langfuse keys not configured -- evaluation tracing disabled")
            self._client = None
            return

        try:
            self._client = Langfuse(
                secret_key=self._secret_key,
                public_key=self._public_key,
                host=self._host or None,
            )
        except Exception:
            logger.warning("Failed to initialize Langfuse client", exc_info=True)
            self._client = None

    @classmethod
    def get_instance(cls) -> LangfuseService | None:
        """Return the singleton instance, creating it on first call."""
        global _instance
        if _instance is None:
            _instance = cls()
        if _instance._client is None:
            return None
        return _instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton (for testing)."""
        global _instance
        _instance = None

    @property
    def client(self) -> Any:
        return self._client

    def trace_and_score(self, transcript: Transcript, scores: list[Score]) -> str:
        """Create or update a Langfuse trace and write scores.

        Uses Langfuse Python SDK v2 (``client.trace()`` / ``trace.score()``).
        Production pins ``langfuse>=2.0.0,<3``.

        .. note:: **Langfuse v3 migration reference** (SDK >=3.0)

           v3 removes ``client.trace()`` and ``trace.score()``.  Replace with::

               trace_id = (transcript.trace_id or uuid.uuid4().hex)[:32]
               with self._client.start_as_current_observation(
                   as_type="span", name=trace_name,
                   trace_context={"trace_id": trace_id},
                   input=transcript.user_input, output=transcript.output,
                   metadata=trace_metadata,
               ):
                   self._client.update_current_trace(
                       name=trace_name, user_id=..., session_id=...,
                       version=..., input=..., output=..., metadata=..., tags=...,
                   )
               for score in scores:
                   self._client.create_score(trace_id=trace_id, name=..., value=...)

           v3 trace IDs must be 32-char lowercase hex (strip UUID dashes).
           ``run_llm_evaluator`` score writes: use ``client.create_score()``
           instead of ``client.score()``.
        """
        if self._client is None:
            return ""

        trace_name = (
            f"{transcript.agent_name}:{transcript.skill_name}"
            if transcript.skill_name
            else transcript.agent_name
        )
        trace_metadata = {
            **transcript.custom_metadata,
            "agent_id": transcript.agent_name,
            "agent_name": transcript.agent_name,
            "run_id": transcript.run_id,
            "thread_id": transcript.thread_id,
            "tenant_id": transcript.tenant_id,
            "instance_id": transcript.instance_id,
            "environment": transcript.environment,
            "skill_name": transcript.skill_name,
            "prompt_template_id": transcript.prompt_template_id,
            "prompt_template_version": transcript.prompt_template_version,
            "prompt_template_hash": transcript.prompt_template_hash,
            "model_name": transcript.model_name,
            "model_provider": transcript.model_provider,
            "model_parameters": transcript.model_parameters,
            "timing": {
                "total_ms": transcript.timing.total_ms,
                "first_token_ms": transcript.timing.first_token_ms,
                "llm_ms": transcript.timing.llm_ms,
                "tool_ms": transcript.timing.tool_ms,
            },
            "tokens": {
                "total": transcript.tokens.total_tokens,
                "cost_usd": transcript.tokens.cost_usd,
            },
            "tool_call_count": len(transcript.tool_calls),
            "error_count": len(transcript.errors),
            "experiment_id": transcript.experiment_id,
            "variant_id": transcript.variant_id,
        }

        try:
            trace_id = transcript.trace_id or None

            trace = self._client.trace(
                id=trace_id,
                name=trace_name,
                input=transcript.user_input,
                output=transcript.output,
                session_id=transcript.session_id,
                user_id=transcript.user_id,
                version=transcript.agent_version,
                metadata=trace_metadata,
                tags=transcript.tags or [],
            )
            for score in scores:
                if score.value is not None:
                    trace.score(
                        name=score.name,
                        value=score.value,
                        comment=score.comment or None,
                        data_type=score.data_type.value if score.data_type else "NUMERIC",
                    )

            self._client.flush()
            return getattr(trace, "id", "") or ""
        except Exception:
            logger.warning("Failed to send trace/scores to Langfuse", exc_info=True)
            return ""

    async def run_llm_evaluator(
        self,
        evaluator_name: str,
        transcript: dict[str, Any],
        judge_model: str | None = None,
        gateway_url: str | None = None,
        execution_context: Any = None,
        execution_token: str | None = None,
    ) -> Score | None:
        """Run an LLM-as-judge evaluator and write the numeric score to Langfuse.

        The judge call goes through the LLM Gateway (``/v1/chat/completions``)
        with ``turn_off_message_logging: true`` in the request metadata to
        prevent PII leakage.  The resulting float score (0.0–1.0) is written
        to the Langfuse trace.  No reasoning text or transcript content is
        persisted — only the numeric score and score name.

        Auth resolution: ``execution_context`` (preferred) → ``execution_token``
        string (deprecated) → ``NOVAOS_EXECUTION_TOKEN`` env var (local dev).
        A missing token is logged at WARNING and ``None`` is returned (the
        evaluator is non-critical and must not surface auth misconfigurations
        as user-facing errors from the ``@evaluate`` decorator).  Use
        :func:`~novaos_client_sdk.evaluation.graders.llm_judge.call_llm_judge`
        directly if you need the exception to propagate.

        Returns a ``Score`` with a real numeric value, or ``None`` if the
        evaluator cannot run (e.g. missing contexts, parse failure).
        """
        from novaos_client_sdk.evaluation.graders.llm_judge import (
            build_judge_messages,
            call_llm_judge,
            get_judge_template,
            parse_judge_score,
        )

        contexts = transcript.get("contexts", [])
        if evaluator_name in ("faithfulness", "context_relevance") and not contexts:
            return None

        template = get_judge_template(evaluator_name)
        if template is None:
            logger.warning("No judge template for evaluator '%s'", evaluator_name)
            return None

        if template.requires_contexts and not contexts:
            return None

        try:
            messages = build_judge_messages(template, transcript)
            raw_response = await call_llm_judge(
                messages,
                model=judge_model,
                gateway_url=gateway_url,
                execution_context=execution_context,
                execution_token=execution_token,
            )
            score_value, reasoning = parse_judge_score(raw_response)

            score_name = f"llm_judge_{evaluator_name}"

            if score_value is None:
                logger.warning(
                    "Could not parse score from LLM judge '%s' (response length=%d)",
                    evaluator_name,
                    len(raw_response),
                )
                return None

            if self._client is not None:
                trace_id = transcript.get("trace_id", "")
                if trace_id:
                    self._client.score(
                        trace_id=trace_id,
                        name=score_name,
                        value=score_value,
                    )
                    self._client.flush()

            return Score(
                name=score_name,
                value=score_value,
                comment=reasoning[:500] if reasoning else "",
            )
        except Exception:
            logger.warning("LLM evaluator %s failed", evaluator_name, exc_info=True)
            return None

    def create_dataset(self, name: str, description: str = "") -> Any:
        """Create a Langfuse dataset."""
        if self._client is None:
            return None
        return self._client.create_dataset(name=name, description=description or None)

    def get_dataset(self, name: str) -> Any:
        """Fetch a Langfuse dataset by name."""
        if self._client is None:
            return None
        return self._client.get_dataset(name=name)

    def add_dataset_item(
        self,
        dataset_name: str,
        input_data: Any,
        expected_output: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        """Add an item to a Langfuse dataset."""
        if self._client is None:
            return None
        return self._client.create_dataset_item(
            dataset_name=dataset_name,
            input=input_data,
            expected_output=expected_output,
            metadata=metadata,
        )

    def graduate_to_regression(
        self,
        source_dataset: str,
        regression_dataset: str,
        min_score: float = 0.8,
        score_name: str | None = None,
    ) -> int:
        """Promote high-scoring dataset items into a regression suite.

        Scans ``source_dataset`` for items whose linked trace achieved at
        least ``min_score`` on the named score (or any score if
        ``score_name`` is ``None``).  Qualifying items are copied into
        ``regression_dataset``, which is created if it does not exist.

        Returns the number of items promoted.
        """
        if self._client is None:
            return 0

        try:
            source = self._client.get_dataset(name=source_dataset)
        except Exception:
            logger.warning(
                "Cannot read source dataset '%s' for graduation",
                source_dataset,
                exc_info=True,
            )
            return 0

        try:
            self._client.get_dataset(name=regression_dataset)
        except Exception:
            self._client.create_dataset(
                name=regression_dataset,
                description=f"Regression suite graduated from '{source_dataset}'",
            )

        promoted = 0
        for item in source.items:
            if not _item_meets_threshold(self._client, item, min_score, score_name):
                continue

            self._client.create_dataset_item(
                dataset_name=regression_dataset,
                input=item.input,
                expected_output=item.expected_output,
                metadata={
                    **(item.metadata or {}),
                    "graduated_from": source_dataset,
                    "source_item_id": item.id,
                },
            )
            promoted += 1

        if promoted:
            self._client.flush()
        logger.info(
            "Graduated %d items from '%s' to '%s'",
            promoted,
            source_dataset,
            regression_dataset,
        )
        return promoted

    def flush(self) -> None:
        if self._client is not None:
            self._client.flush()
