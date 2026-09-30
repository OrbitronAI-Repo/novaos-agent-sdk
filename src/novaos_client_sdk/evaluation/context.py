"""
Evaluation context -- mutable capture of runtime data during agent execution.

The ``EvaluationContext`` accumulates tool calls, LLM calls, RAG contexts, timing,
and errors as the skill handler runs.  At the end, ``to_transcript()`` produces the
full dict that graders consume. Only a derived subset of it is sent to Langfuse
(see :meth:`novaos_client_sdk.evaluation.langfuse_service.LangfuseService.trace_and_score`,
which forwards ``user_input``/``output``/summary metadata — including a
``tool_call_count`` int, not the tool calls themselves) — the full dict, with raw
tool-call arguments and results, also reaches whatever the caller does with
``to_transcript()`` directly: local grading, and CI reporting
(``evaluation/ci.py`` prints it as JSON/markdown, which can land in CI logs).

.. admonition:: Tool-call arguments/results are never masked

   Connector calls routinely pass customer data as tool arguments. Neither
   ``track_tool``/``add_tool_call`` below nor anything downstream applies PII
   masking to ``ToolCall.arguments``/``.result`` — see the note on
   :class:`novaos_client_sdk.evaluation.models.ToolCall` for the full default
   posture and why ``AgentBuilder(mask_output_pii=True)`` does not help here.

DX-optimized (Galaxy-compatible):
- NullEvaluationContext eliminates all ``if eval_ctx:`` checks
- ``track_retrieval`` / ``track_tool`` async context managers for auto-timing
- ``add_contexts`` bulk method with smart field extraction
- ``use_prompt`` loads from Langfuse prompt management
- Chainable methods that return ``self``

Use ``get_eval_context()`` from inside any skill handler to access the active context.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any

from novaos_client_sdk.evaluation.models import (
    ContextItem,
    ErrorRecord,
    ToolCall,
    Transcript,
)

logger = logging.getLogger("novaos.evaluation.context")

if TYPE_CHECKING:
    from novaos_client_sdk.agent_types import AgentRuntimeContext

_eval_context_var: ContextVar[EvaluationContext | None] = ContextVar(
    "novaos_eval_context", default=None
)


def get_eval_context() -> EvaluationContext:
    """Return the active EvaluationContext, or a NullEvaluationContext if none is set."""
    ctx = _eval_context_var.get()
    if ctx is None:
        return NullEvaluationContext()
    return ctx


def _set_eval_context(ctx: EvaluationContext | None) -> Token[EvaluationContext | None]:
    """Set the active EvaluationContext and return a token for restoring the previous value."""
    return _eval_context_var.set(ctx)


def _reset_eval_context(token: Token[EvaluationContext | None]) -> None:
    """Restore the previous EvaluationContext using a token from ``_set_eval_context``."""
    _eval_context_var.reset(token)


class EvaluationContext:
    """Mutable capture of runtime evaluation data during a single skill execution."""

    def __init__(
        self,
        agent_name: str = "",
        agent_version: str = "0.0.0",
        environment: str = "development",
        *,
        runtime_context: AgentRuntimeContext | None = None,
        user_input: str = "",
        skill_name: str = "",
    ):
        self._transcript = Transcript(
            agent_name=agent_name,
            agent_version=agent_version,
            environment=environment,
            user_input=user_input,
            skill_name=skill_name,
        )

        self._execution_token: str = ""

        if runtime_context is not None:
            self._transcript.run_id = runtime_context.run_id
            self._transcript.trace_id = runtime_context.trace_id
            self._transcript.thread_id = runtime_context.thread_id
            self._transcript.session_id = runtime_context.session_id
            self._transcript.user_id = runtime_context.user_id
            self._transcript.tenant_id = runtime_context.tenant_id
            self._transcript.instance_id = runtime_context.instance_id
            self._execution_token = runtime_context.execution_token or ""

        self._start_time: float = time.perf_counter()
        self._first_token_recorded: bool = False
        self._is_active: bool = True

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def transcript(self) -> Transcript:
        return self._transcript

    @property
    def execution_token(self) -> str:
        """The RS256 execution token from the agent's runtime context, if available."""
        return self._execution_token

    # -- Prompt --

    def set_prompt_info(
        self,
        *,
        template_id: str | None = None,
        template_version: str | None = None,
        template_hash: str | None = None,
        system_prompt: str | None = None,
        variables: dict[str, Any] | None = None,
    ) -> None:
        if template_id is not None:
            self._transcript.prompt_template_id = template_id
        if template_version is not None:
            self._transcript.prompt_template_version = template_version
        if template_hash is not None:
            self._transcript.prompt_template_hash = template_hash
        if system_prompt is not None:
            self._transcript.system_prompt = system_prompt
        if variables is not None:
            self._transcript.prompt_variables = variables

    # -- Model --

    def set_model_info(
        self,
        *,
        provider: str | None = None,
        name: str | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        if provider is not None:
            self._transcript.model_provider = provider
        if name is not None:
            self._transcript.model_name = name
        if parameters is not None:
            self._transcript.model_parameters = parameters

    # -- Prompt loading (Galaxy-compatible) --

    def use_prompt(
        self,
        prompt_name: str,
        *,
        version: int | None = None,
        variables: dict[str, str] | None = None,
    ) -> str:
        """Load a prompt from Langfuse Prompt Management and record it on the transcript.

        Returns the compiled prompt string with ``variables`` interpolated.
        Falls back to an empty string if the prompt is not found.
        """
        try:
            from novaos_client_sdk.evaluation.langfuse_service import LangfuseService

            service = LangfuseService.get_instance()
            if service is not None and service.client is not None:
                prompt_obj = service.client.get_prompt(prompt_name, version=version, type="text")
                compiled = prompt_obj.compile(**(variables or {}))
                self.set_prompt_info(
                    template_id=prompt_name,
                    template_version=str(version or "latest"),
                    template_hash=hashlib.sha256(compiled.encode()).hexdigest()[:16],
                    system_prompt=compiled,
                    variables=variables,
                )
                return compiled
        except Exception:
            logger.debug("use_prompt failed for %r", prompt_name, exc_info=True)
        return ""

    # -- RAG contexts --

    def add_context(
        self,
        content: str,
        source: str = "",
        score: float = 0.0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._transcript.contexts.append(
            ContextItem(
                content=content,
                source=source,
                score=score,
                metadata=metadata or {},
            )
        )

    def add_contexts(self, items: list[ContextItem | dict[str, Any] | Any]) -> None:
        """Add multiple context items.  Accepts ContextItem objects, dicts, or arbitrary
        document-like objects (Galaxy-compatible smart extraction)."""
        for item in items:
            if isinstance(item, ContextItem):
                self._transcript.contexts.append(item)
            elif isinstance(item, dict):
                self._transcript.contexts.append(
                    ContextItem(
                        content=item.get(
                            "content", item.get("text", item.get("page_content", str(item)))
                        ),
                        source=item.get("source", ""),
                        score=float(item.get("score") or item.get("relevance_score") or 0.0),
                        metadata={
                            k: v
                            for k, v in item.items()
                            if k
                            not in (
                                "content",
                                "text",
                                "page_content",
                                "source",
                                "score",
                                "relevance_score",
                            )
                        },
                    )
                )
            elif isinstance(item, str):
                self._transcript.contexts.append(ContextItem(content=item))
            else:
                content = (
                    getattr(item, "content", None)
                    or getattr(item, "text", None)
                    or getattr(item, "page_content", None)
                    or str(item)
                )
                self._transcript.contexts.append(ContextItem(content=str(content)))

    @asynccontextmanager
    async def track_retrieval(self, source: str = "retrieval") -> AsyncIterator[None]:
        """Async context manager that auto-times a retrieval operation."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._transcript.timing.retrieval_ms += elapsed_ms

    @contextmanager
    def track_retrieval_sync(self, source: str = "retrieval") -> Iterator[None]:
        """Sync version of ``track_retrieval``."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._transcript.timing.retrieval_ms += elapsed_ms

    # -- Tool calls --

    def add_tool_call(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None = None,
        result: Any = None,
        success: bool = True,
        error: str = "",
        latency_ms: float = 0.0,
    ) -> None:
        self._transcript.tool_calls.append(
            ToolCall(
                tool_name=tool_name,
                arguments=arguments or {},
                result=result,
                success=success,
                error=error,
                latency_ms=latency_ms,
            )
        )

    @asynccontextmanager
    async def track_tool(
        self, tool_name: str, parameters: dict[str, Any] | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Async context manager that auto-times a tool call and records the result.

        Usage::

            async with eval_ctx.track_tool("search", {"q": "hello"}) as ctx:
                result = await search_api("hello")
                ctx["response"] = result

        ``parameters`` and whatever ``ctx["response"]`` is set to are recorded
        verbatim, unmasked — see the module docstring above and
        :class:`~novaos_client_sdk.evaluation.models.ToolCall` before passing a
        connector call's raw params/result here if they may contain customer
        PII.
        """
        start = time.perf_counter()
        bag: dict[str, Any] = {"response": None, "success": True, "error": None}
        try:
            yield bag
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.add_tool_call(
                tool_name=tool_name,
                arguments=parameters,
                result=bag.get("response"),
                success=bag.get("success", True),
                latency_ms=elapsed_ms,
            )
            self.add_tool_latency(elapsed_ms)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.add_tool_call(
                tool_name=tool_name,
                arguments=parameters,
                result=str(exc),
                success=False,
                error=str(exc),
                latency_ms=elapsed_ms,
            )
            self.add_tool_latency(elapsed_ms)
            raise

    @contextmanager
    def track_tool_sync(
        self, tool_name: str, parameters: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        """Sync version of ``track_tool``."""
        start = time.perf_counter()
        bag: dict[str, Any] = {"response": None, "success": True, "error": None}
        try:
            yield bag
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.add_tool_call(
                tool_name=tool_name,
                arguments=parameters,
                result=bag.get("response"),
                success=bag.get("success", True),
                latency_ms=elapsed_ms,
            )
            self.add_tool_latency(elapsed_ms)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.add_tool_call(
                tool_name=tool_name,
                arguments=parameters,
                result=str(exc),
                success=False,
                error=str(exc),
                latency_ms=elapsed_ms,
            )
            self.add_tool_latency(elapsed_ms)
            raise

    # -- LLM calls --

    def add_llm_call(self, call_record: dict[str, Any]) -> None:
        self._transcript.llm_calls.append(call_record)

    # -- Token usage --

    def add_token_usage(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        cost_usd: float = 0.0,
        model: str = "",
    ) -> None:
        t = self._transcript.tokens
        t.prompt_tokens += prompt_tokens
        t.completion_tokens += completion_tokens
        t.total_tokens += total_tokens
        t.cost_usd += cost_usd
        if model:
            t.model = model

    # -- Timing --

    def record_first_token(self) -> None:
        if not self._first_token_recorded:
            self._transcript.timing.first_token_ms = (time.perf_counter() - self._start_time) * 1000
            self._first_token_recorded = True

    def add_llm_latency(self, ms: float) -> None:
        self._transcript.timing.llm_ms += ms

    def add_retrieval_latency(self, ms: float) -> None:
        self._transcript.timing.retrieval_ms += ms

    def add_tool_latency(self, ms: float) -> None:
        self._transcript.timing.tool_ms += ms

    # -- Errors --

    def record_error(
        self,
        error_type: str,
        message: str,
        recoverable: bool = False,
    ) -> None:
        self._transcript.errors.append(
            ErrorRecord(
                error_type=error_type,
                message=message,
                recoverable=recoverable,
            )
        )

    # -- Output --

    def set_output(self, output: str) -> None:
        self._transcript.output = output

    # -- Experiments --

    def set_experiment(
        self,
        experiment_id: str,
        variant_id: str = "",
        feature_flags: dict[str, Any] | None = None,
    ) -> None:
        self._transcript.experiment_id = experiment_id
        self._transcript.variant_id = variant_id
        if feature_flags is not None:
            self._transcript.feature_flags = feature_flags

    # -- Custom metadata --

    def add_metadata(self, key: str, value: Any) -> None:
        self._transcript.custom_metadata[key] = value

    def add_tag(self, tag: str) -> None:
        if tag not in self._transcript.tags:
            self._transcript.tags.append(tag)

    def add_tags(self, tags: list[str]) -> None:
        for tag in tags:
            self.add_tag(tag)

    # -- Finalization --

    def finalize(self) -> None:
        """Compute final timing and mark context as inactive."""
        elapsed = (time.perf_counter() - self._start_time) * 1000
        self._transcript.timing.total_ms = elapsed
        self._transcript.timing.overhead_ms = max(
            0,
            elapsed
            - self._transcript.timing.llm_ms
            - self._transcript.timing.tool_ms
            - self._transcript.timing.retrieval_ms,
        )
        self._is_active = False

    def to_transcript(self) -> dict[str, Any]:
        """Return the transcript as a dict for graders."""
        if self._is_active:
            self.finalize()
        return self._transcript.to_dict()


class NullEvaluationContext(EvaluationContext):
    """No-op evaluation context used when evaluation is disabled.

    All methods are safe to call — they do nothing.  This eliminates the need
    for ``if eval_ctx.is_active:`` guards throughout agent code.
    """

    def __init__(self) -> None:
        self._transcript = Transcript()
        self._start_time: float = 0.0
        self._first_token_recorded: bool = False
        self._is_active = False
        self._execution_token: str = ""

    @property
    def is_active(self) -> bool:
        return False

    @property
    def transcript(self) -> Transcript:
        return self._transcript

    def use_prompt(self, prompt_name: str, **kwargs: Any) -> str:
        return ""

    def set_prompt_info(self, **kwargs: Any) -> None:
        pass

    def set_model_info(self, **kwargs: Any) -> None:
        pass

    def add_context(self, content: str, **kwargs: Any) -> None:
        pass

    def add_contexts(self, items: list[Any]) -> None:
        pass

    @asynccontextmanager
    async def track_retrieval(self, source: str = "retrieval") -> AsyncIterator[None]:
        yield

    @contextmanager
    def track_retrieval_sync(self, source: str = "retrieval") -> Iterator[None]:
        yield

    def add_tool_call(self, tool_name: str, **kwargs: Any) -> None:
        pass

    @asynccontextmanager
    async def track_tool(
        self, tool_name: str, parameters: dict[str, Any] | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        yield {"response": None, "success": True, "error": None}

    @contextmanager
    def track_tool_sync(
        self, tool_name: str, parameters: dict[str, Any] | None = None
    ) -> Iterator[dict[str, Any]]:
        yield {"response": None, "success": True, "error": None}

    def add_llm_call(self, call_record: dict[str, Any]) -> None:
        pass

    def add_token_usage(self, **kwargs: Any) -> None:
        pass

    def record_first_token(self) -> None:
        pass

    def add_llm_latency(self, ms: float) -> None:
        pass

    def add_retrieval_latency(self, ms: float) -> None:
        pass

    def add_tool_latency(self, ms: float) -> None:
        pass

    def record_error(self, error_type: str, message: str, recoverable: bool = False) -> None:
        pass

    def set_output(self, output: str) -> None:
        pass

    def set_experiment(self, experiment_id: str, **kwargs: Any) -> None:
        pass

    def add_metadata(self, key: str, value: Any) -> None:
        pass

    def add_tag(self, tag: str) -> None:
        pass

    def add_tags(self, tags: list[str]) -> None:
        pass

    def finalize(self) -> None:
        pass

    def to_transcript(self) -> dict[str, Any]:
        return self._transcript.to_dict()
