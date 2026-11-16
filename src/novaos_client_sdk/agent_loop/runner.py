"""``AgentLoop``: the default graph, its nodes and the run, resume and cancel paths.

LangGraph owns the shape of the loop; the nodes call the limits, the stall guard and the key
builder, and reach the model and tools only through the client interfaces they are given.

The default graph::

    START -> context -> model -> tools -> context -> ...
                          |  -> interrupt -> context
                          |  -> check -> END
                          `- (any stop) -> END

Context and check pass through unless a context layer or an in-run check is given. ``AgentLoop.graph`` is the uncompiled ``StateGraph``, so an author can add or
rewire nodes; ``AgentLoop.add_node`` adds one with the same step event and span as the rest.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import inspect
import json
import logging
import time
import weakref
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, TypedDict, cast

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.errors import GraphInterrupt
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from .. import tracing
from ..agent_builder import describe_exception
from ..checkpoint.errors import LeaseConflict
from ..message_builder import MessageBuilder
from ..run_context import run_span_attributes
from .check import InRunCheck, evaluate, not_run_record
from .context import ContextLayer, count_prompt, estimate_tokens, size_rejection
from .guards import StallGuard, idempotency_key
from .limits import LimitName, LoopLimits, Usage, limit_reached
from .ports import ModelClient, ToolClient, ToolSpec
from .result import LoopResult, StopReason, approval_fields, approval_request, parse_decision

__all__ = ["AgentLoop", "LoopState"]

STEP_EVENT = "agent_loop.step"

# When the gateway refuses a prompt as too large, the budget shrinks and the call is made again,
# at most this many times and never below this many estimated tokens.
MAX_SHRINKS = 3
MIN_PROMPT_TOKENS = 256
SHRINK_TO = 0.7  # of the refused prompt, when the gateway named no limit
WITHIN_LIMIT = 0.9  # of a limit the gateway named

logger = logging.getLogger(__name__)


class LoopState(TypedDict, total=False):
    """Graph state. Plain values only, so any checkpointer can store it."""

    messages: list[dict[str, Any]]
    usage: dict[str, Any]
    call_log: list[str]
    write_keys: dict[str, str]
    repeat_skips: int
    summary: str
    prompt_cap: int
    evidence: list[dict[str, Any]]
    memory_reads: list[dict[str, Any]]
    context: dict[str, Any]
    refresh_evidence: bool
    pending_calls: list[dict[str, Any]]
    rejected_calls: list[dict[str, Any]]
    wrote: bool
    in_run_check: dict[str, Any] | None
    output: Any
    output_retries: int
    retry: bool
    input_request: Any
    stop_reason: str | None
    limit: str | None
    error: str | None


@dataclass
class _RunEnv:
    """What one ``run`` or ``resume`` gives the nodes, outside the checkpointed state."""

    loop: AgentLoop
    task_id: str
    business_key: str | None
    cancelled: Callable[[], bool]
    emit: Callable[[dict[str, Any]], Any] | None
    deadline: float
    # Read once per run or resume and kept only for this call: never put in the state.
    memory: list[Any] | None = None
    memory_error: str | None = None
    prompt: list[dict[str, Any]] | None = None

    def time_left(self) -> float:
        return self.deadline - time.monotonic()

    def limit_hit(self, usage: Usage, *, count_steps: bool) -> LimitName | None:
        return limit_reached(self.loop.limits, usage, self.time_left(), count_steps=count_steps)

    async def within_budget(self, awaitable: Awaitable[Any]) -> Any:
        """Await a model or tool call for at most the run's remaining wall time."""
        try:
            return await asyncio.wait_for(awaitable, timeout=max(self.time_left(), 0))
        except TimeoutError:
            if self.time_left() <= 0:
                raise _WallTime from None
            raise


class _WallTime(Exception):
    pass


_ENV: ContextVar[_RunEnv] = ContextVar("novaos_agent_loop_env")

# One lock per task for the whole process, so loops built per request still do not overlap.
# Held only while a run of that task is in progress; idle tasks leave nothing behind.
_LOCKS: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()

# Paused tasks the default in-memory saver keeps; the oldest is dropped past this, as an
# unanswered pause is normal (approvals expire).
MAX_PAUSED_TASKS = 1000


def _stop(reason: StopReason, **extra: Any) -> dict[str, Any]:
    return {"stop_reason": reason.value, **extra}


def _limit(name: LimitName) -> dict[str, Any]:
    return _stop(StopReason.LIMIT_REACHED, limit=name.value)


def _step(name: str, fn: Callable[[LoopState], Any]) -> Callable[[LoopState], Awaitable[Any]]:
    """Wrap a node: cancel check first, then one span and one step event per run of it."""

    async def node(state: LoopState) -> dict[str, Any]:
        env = _ENV.get()
        tracer = tracing._get_tracer()
        span_cm = (
            tracer.start_as_current_span(
                f"agent_loop.{name}",
                attributes={
                    "novaos.task_id": env.task_id,
                    "novaos.agent_loop.step": Usage.from_dict(state.get("usage")).steps,
                    **run_span_attributes(),
                },
            )
            if tracer is not None
            else contextlib.nullcontext()
        )
        update: dict[str, Any] = {}
        with span_cm:
            try:
                if env.cancelled():
                    update = _stop(StopReason.CANCELLED)
                else:
                    result = fn(state)
                    update = (await result if inspect.isawaitable(result) else result) or {}
            except GraphInterrupt:
                _emit(env, name, dict(state), {"paused": True})
                raise
        extra = {k: update[k] for k in ("context", "in_run_check") if k in update}
        _emit(env, name, {**state, **update}, extra or None)
        return update

    return node


def _emit(
    env: _RunEnv, node: str, state: dict[str, Any], extra: dict[str, Any] | None = None
) -> None:
    if env.emit is None:
        return
    usage = Usage.from_dict(state.get("usage"))
    env.emit(
        {
            "node": node,
            "task_id": env.task_id,
            "step": usage.steps,
            "stop_reason": state.get("stop_reason"),
            "limit": state.get("limit"),
            "usage": usage.to_dict(),
            **(extra or {}),
        }
    )


# --- nodes -------------------------------------------------------------------------------


async def _build(state: LoopState, env: _RunEnv) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """The state update and the prompt for the next model call."""
    layer = env.loop.context
    messages = list(state.get("messages") or [])
    if layer is None:
        return {}, messages

    async def refetch(tool: str, args: dict[str, Any]) -> Any:
        spec = env.loop.tool_specs.get(tool)
        if spec is None or not spec.read_only:
            raise LookupError(f"{tool} cannot be read again")
        return await env.within_budget(
            env.loop.tool_client.call(
                tool, args, task_id=env.task_id, idempotency_key=None, timeout_s=env.time_left()
            )
        )

    steps = Usage.from_dict(state.get("usage")).steps
    messages, evidence, fetches = await layer.refresh(
        messages,
        list(state.get("evidence") or []),
        resumed=bool(state.get("refresh_evidence")),
        refetch=refetch,
    )
    if env.memory is None:
        query = next(
            (
                m["content"]
                for m in reversed(messages)
                if m.get("role") == "user" and isinstance(m.get("content"), str)
            ),
            "",
        )
        env.memory, env.memory_error = await layer.read_memory(query, within_s=env.time_left())
    built = await layer.build(
        messages,
        state.get("summary") or "",
        evidence,
        env.memory,
        reserve_tokens=env.loop.tool_tokens,
        cap_tokens=state.get("prompt_cap"),
    )
    stats = {**built.stats, "memory_error": env.memory_error, "refreshed": fetches}
    # Kept in the checkpoint when they change; every step event carries what that step read.
    reads = list(state.get("memory_reads") or [])
    if not reads or reads[-1]["items"] != built.stats["memory"]:
        reads.append({"step": steps, "items": built.stats["memory"]})
    update = {
        "messages": built.messages,
        "summary": built.summary,
        "evidence": built.evidence,
        "memory_reads": reads,
        "context": stats,
        "refresh_evidence": False,
    }
    return update, built.prompt


async def context_node(state: LoopState) -> dict[str, Any]:
    env = _ENV.get()
    update, env.prompt = await _build(state, env)
    return update


def _smaller_cap(
    loop: AgentLoop, exc: Exception, prompt: list[dict[str, Any]], shrinks: int
) -> int | None:
    """The budget to try after the gateway refused ``prompt`` as too large, or None to give up:
    nothing to shrink with, a refusal for another reason, too many tries, or too small to help."""
    if loop.context is None or shrinks >= MAX_SHRINKS:
        return None
    refusal = size_rejection(exc)
    if refusal is None:
        return None
    sent = count_prompt(prompt) + loop.tool_tokens
    target = int(sent * SHRINK_TO)
    if refusal.limit_tokens and refusal.needed_tokens:
        # The gateway counted the request in its own units: shrink by the ratio it gave.
        target = min(
            target, int(sent * refusal.limit_tokens / refusal.needed_tokens * WITHIN_LIMIT)
        )
    elif refusal.limit_tokens:
        target = min(target, int(refusal.limit_tokens * WITHIN_LIMIT))
    return target if MIN_PROMPT_TOKENS <= target < sent else None


async def model_node(state: LoopState) -> dict[str, Any]:
    env = _ENV.get()
    loop = env.loop
    usage = Usage.from_dict(state.get("usage"))
    hit = env.limit_hit(usage, count_steps=True)
    if hit:
        return _limit(hit)
    prompt, env.prompt = env.prompt, None
    if prompt is None:
        # Resumed past the context node: build the prompt again, memory read live.
        _, prompt = await _build(state, env)
    rebuilt: dict[str, Any] = {}
    shrinks = 0
    while True:
        try:
            turn = await env.within_budget(loop.model.complete(prompt, loop.tools))
            break
        except _WallTime:
            return {**rebuilt, **_limit(LimitName.WALL_TIME)}
        except Exception as exc:
            cap = _smaller_cap(loop, exc, prompt, shrinks)
            if cap is None:
                return {**rebuilt, **_stop(StopReason.FAILED, error=describe_exception(exc))}
            # The gateway refused the prompt as too large: build a smaller one and ask again.
            shrinks += 1
            state = cast(LoopState, {**state, **rebuilt, "prompt_cap": cap})
            rebuilt, prompt = await _build(state, env)
            rebuilt["prompt_cap"] = cap
    messages = list(rebuilt.get("messages", state.get("messages")) or [])
    state = cast(LoopState, {**state, **rebuilt})

    usage.add_model_call(turn.model, turn.usage.tokens_in, turn.usage.tokens_out)
    if turn.revises_plan:
        if usage.plan_revisions >= loop.limits.max_plan_revisions:
            return {**rebuilt, "usage": usage.to_dict(), **_limit(LimitName.PLAN_REVISIONS)}
        usage.plan_revisions += 1
    calls = [{"name": c.name, "args": dict(c.args), "id": c.id} for c in turn.tool_calls]
    messages.append({"role": "assistant", "content": turn.content, "tool_calls": calls})
    update: dict[str, Any] = {
        **rebuilt,
        "usage": usage.to_dict(),
        "messages": messages,
        "pending_calls": calls,
        "input_request": turn.input_request,
        "retry": False,
    }
    if turn.input_request is not None or calls:
        return update

    if loop.output_validator is not None:
        try:
            loop.output_validator(turn.content or "")
        except Exception as exc:
            retries = int(state.get("output_retries") or 0) + 1
            if retries > loop.max_output_retries:
                return {**update, **_stop(StopReason.FAILED, error=f"invalid output: {exc}")}
            messages.append(
                {"role": "user", "content": f"Your answer was not valid: {exc}. Answer again."}
            )
            return {**update, "output_retries": retries, "retry": True}
    return {**update, "output": turn.content}


async def tools_node(state: LoopState) -> dict[str, Any]:
    env = _ENV.get()
    loop = env.loop
    usage = Usage.from_dict(state.get("usage"))
    messages = list(state.get("messages") or [])
    log = list(state.get("call_log") or [])
    skips = int(state.get("repeat_skips") or 0)
    # Keys of writes that failed, by signature: a retry sends the same key, so a write that
    # committed before its reply was lost is not written twice.
    write_keys = dict(state.get("write_keys") or {})
    evidence = list(state.get("evidence") or [])

    def done(**stop: Any) -> dict[str, Any]:
        return {
            "usage": usage.to_dict(),
            "messages": messages,
            "call_log": log,
            "repeat_skips": skips,
            "write_keys": write_keys,
            "evidence": evidence,
            "wrote": wrote,
            "pending_calls": [],
            **stop,
        }

    def reply(call: dict[str, Any], content: Any) -> None:
        messages.append(
            {"role": "tool", "name": call["name"], "tool_call_id": call["id"], "content": content}
        )

    writes = 0
    wrote = bool(state.get("wrote"))
    for call in state.get("pending_calls") or []:
        if env.cancelled():
            return done(**_stop(StopReason.CANCELLED))
        hit = env.limit_hit(usage, count_steps=False)
        if hit:
            return done(**_limit(hit))
        spec = loop.tool_specs.get(call["name"])
        if spec is None:
            reply(call, f"error: unknown tool {call['name']!r}")
            continue
        if any(
            r.get("name") == call["name"] and r.get("args") == call["args"]
            for r in state.get("rejected_calls") or []
        ):
            reply(call, "error: the person rejected this write; it was not made")
            continue
        if loop.stall_guard.should_skip(spec, call["args"], log):
            skips += 1
            reply(call, "Skipped: this call already ran with the same arguments.")
            if loop.stall_guard.stalled(skips):
                return done(**_stop(StopReason.STALLED))
            continue
        usage.tool_calls += 1
        key = None
        signature = loop.stall_guard.signature(spec, call["args"])
        if not spec.read_only:
            key = write_keys.get(signature) or idempotency_key(
                env.business_key or "", step=usage.steps, index=writes
            )
            writes += 1
            wrote = True
        try:
            result = await env.within_budget(
                loop.tool_client.call(
                    call["name"],
                    call["args"],
                    task_id=env.task_id,
                    idempotency_key=key,
                    timeout_s=env.time_left(),
                )
            )
        except _WallTime:
            return done(**_limit(LimitName.WALL_TIME))
        except Exception as exc:
            if key is not None:
                write_keys[signature] = key
            reply(call, f"error: {describe_exception(exc)}")
            continue
        write_keys.pop(signature, None)
        loop.stall_guard.record(spec, call["args"], log)
        if spec.read_only and loop.context is not None:
            evidence.append(
                {
                    "at": len(messages),
                    "tool": call["name"],
                    "args": call["args"],
                    "fetched_at": loop.context.clock(),
                }
            )
        reply(call, result)
    return done()


def interrupt_node(state: LoopState) -> dict[str, Any]:
    answer = interrupt(state.get("input_request"))
    messages = [*(state.get("messages") or []), {"role": "user", "content": answer}]
    update: dict[str, Any] = {"messages": messages, "input_request": None, "refresh_evidence": True}
    fields = approval_fields(state.get("input_request"))
    if fields is not None and parse_decision(answer) is False:
        # The person rejected the held write: refuse that call, with those arguments, from now on.
        _, call_id, tool_name = fields
        rejected = [
            {"name": c["name"], "args": c["args"]}
            for c in state.get("pending_calls") or []
            if c.get("id") == call_id and c.get("name") == tool_name
        ]
        update["rejected_calls"] = [*(state.get("rejected_calls") or []), *rejected]
    return update


async def check_node(state: LoopState) -> dict[str, Any]:
    env = _ENV.get()
    loop = env.loop
    check = loop.check
    if check is None:
        return _stop(StopReason.DONE)
    usage = Usage.from_dict(state.get("usage"))
    messages = list(state.get("messages") or [])
    answer = state.get("output") or ""

    async def revise(feedback: str) -> str | None:
        # One corrective pass on the same model, with no tools: it cannot do more than answer.
        asked = cast(
            LoopState, {**state, "messages": [*messages, {"role": "user", "content": feedback}]}
        )
        _, prompt = await _build(asked, env)
        try:
            turn = await env.within_budget(loop.model.complete(prompt, ()))
        except Exception:
            logger.warning("the corrective pass failed; keeping the draft", exc_info=True)
            return None
        usage.add_model_call(turn.model, turn.usage.tokens_in, turn.usage.tokens_out)
        if turn.tool_calls or not turn.content:
            return None
        if loop.output_validator is not None:
            try:
                loop.output_validator(turn.content)
            except Exception:
                return None
        return str(turn.content)

    refinements = check.config.max_refinements
    outcome = await evaluate(
        check,
        answer=answer,
        question=_question(messages),
        wrote=bool(state.get("wrote")),
        can_revise=refinements != 0 and env.limit_hit(usage, count_steps=True) is None,
        revise=revise,
        time_left=env.time_left,
        contexts=_contexts(messages),
    )
    _annotate_span(check, outcome.record, outcome.reasoning)
    update: dict[str, Any] = {
        "usage": usage.to_dict(),
        "in_run_check": outcome.record,
        **_stop(StopReason.DONE),
    }
    if outcome.feedback is not None:
        update["output"] = outcome.answer
        update["messages"] = [
            *messages,
            {"role": "user", "content": outcome.feedback},
            {"role": "assistant", "content": outcome.answer},
        ]
    return update


def _question(messages: list[dict[str, Any]]) -> str:
    """The request being answered: the last message from the person before the first reply,
    so an answer to a pause for input is not taken for the question."""
    request = ""
    for message in messages:
        if message.get("role") == "assistant":
            break
        if message.get("role") == "user" and isinstance(message.get("content"), str):
            request = str(message["content"])
    return request


def _contexts(messages: list[dict[str, Any]]) -> list[str]:
    """The tool results the answer was written from, for a rubric that checks it against them."""
    return [
        m["content"] if isinstance(m["content"], str) else json.dumps(m["content"], default=str)
        for m in messages
        if m.get("role") == "tool"
    ]


def _annotate_span(check: InRunCheck, record: dict[str, Any], reasoning: str) -> None:
    """Put the check's result on the node's span. The reasoning is masked and capped already."""
    try:
        from opentelemetry import trace
    except ImportError:
        return
    span = trace.get_current_span()
    span.set_attribute("novaos.in_run_check.result", record["result"])
    span.set_attribute("novaos.in_run_check.judge_model", check.config.judge_model)
    if record["judge_model_version"]:
        span.set_attribute("novaos.in_run_check.judge_model_version", record["judge_model_version"])
    span.set_attribute("novaos.in_run_check.rubrics", [r["ref"] for r in record["rubrics"]])
    if reasoning:
        span.set_attribute("novaos.in_run_check.judge_reasoning", reasoning)


# --- routing -----------------------------------------------------------------------------


def _or_end(next_node: str) -> Callable[[LoopState], str]:
    return lambda state: END if state.get("stop_reason") else next_node


def _after_model(state: LoopState) -> str:
    if state.get("stop_reason"):
        return END
    if state.get("input_request") is not None:
        return "interrupt"
    if state.get("pending_calls"):
        return "tools"
    return "model" if state.get("retry") else "check"


def build_default_graph() -> StateGraph:
    graph = StateGraph(LoopState)
    for name, fn in {
        "context": context_node,
        "model": model_node,
        "tools": tools_node,
        "interrupt": interrupt_node,
        "check": check_node,
    }.items():
        graph.add_node(name, _step(name, fn))  # type: ignore[call-overload]
    graph.add_edge(START, "context")
    graph.add_conditional_edges("context", _or_end("model"))
    graph.add_conditional_edges("model", _after_model)
    graph.add_conditional_edges("tools", _or_end("context"))
    graph.add_conditional_edges("interrupt", _or_end("context"))
    graph.add_edge("check", END)
    return graph


# --- runner ------------------------------------------------------------------------------


class AgentLoop:
    """A bounded agent loop that says why it stopped and can be cancelled and resumed.

    ``checkpointer`` is any LangGraph checkpointer: the in-memory saver by default, or
    ``NovaCheckpointSaver`` to keep state outside the pod. With the latter, ``task_id`` must be
    the run id of the execution token. The default saver forgets a task once its run stops, so
    a long-lived pod does not keep every finished task in memory; a paused task stays until it
    is resumed and stops, up to the newest :data:`MAX_PAUSED_TASKS` pauses.
    """

    def __init__(
        self,
        model: ModelClient,
        tool_client: ToolClient,
        *,
        tools: Sequence[ToolSpec] = (),
        limits: LoopLimits | None = None,
        checkpointer: Any | None = None,
        stall_guard: StallGuard | None = None,
        output_validator: Callable[[str], Any] | None = None,
        max_output_retries: int = 2,
        context: ContextLayer | None = None,
        check: InRunCheck | None = None,
        lease_wait_s: float | None = None,
        lease_retry_s: float = 5.0,
    ) -> None:
        self.model = model
        self.tool_client = tool_client
        self.tools = list(tools)
        self.tool_specs = {t.name: t for t in self.tools}
        self.limits = limits or LoopLimits()
        self._owns_saver = checkpointer is None
        self._paused: OrderedDict[str, None] = OrderedDict()
        self.checkpointer = checkpointer if checkpointer is not None else InMemorySaver()
        self.stall_guard = stall_guard or StallGuard()
        self.output_validator = output_validator
        self.max_output_retries = max_output_retries
        self.context = context
        self.check = check
        self.lease_wait_s = lease_wait_s
        self.lease_retry_s = lease_retry_s
        # The tool definitions go to the model with every prompt, so they count against the budget.
        self.tool_tokens = estimate_tokens(
            json.dumps(
                [
                    {"name": t.name, "description": t.description, "parameters": t.parameters}
                    for t in self.tools
                ]
            )
            if self.tools
            else ""
        )
        self.graph = build_default_graph()
        self._writes = any(not t.read_only for t in self.tools)

    def add_node(self, name: str, fn: Callable[[LoopState], Any]) -> None:
        """Add a node to :attr:`graph` with the step event and span every node has."""
        self.graph.add_node(name, _step(name, fn))  # type: ignore[call-overload]

    async def run(
        self,
        request: str | list[dict[str, Any]],
        *,
        task_id: str,
        business_key: str | None = None,
        cancelled: Callable[[], bool] | None = None,
        emit: Callable[[dict[str, Any]], Any] | None = None,
        summary: str | None = None,
    ) -> LoopResult:
        """Run a request. ``summary`` seeds the summary of turns before ``request``."""
        messages = (
            [{"role": "user", "content": request}] if isinstance(request, str) else list(request)
        )
        fresh: LoopState = {
            "messages": messages,
            "usage": Usage().to_dict(),
            "call_log": [],
            "write_keys": {},
            "repeat_skips": 0,
            "summary": summary or "",
            "evidence": [],
            "wrote": False,
            "in_run_check": None,
            "memory_reads": [],
            "context": {},
            "refresh_evidence": False,
            "pending_calls": [],
            "rejected_calls": [],
            "output": None,
            "output_retries": 0,
            "retry": False,
            "input_request": None,
            "stop_reason": None,
            "limit": None,
            "error": None,
        }
        return await self._invoke(fresh, task_id, business_key, cancelled, emit)

    async def resume(
        self,
        answer: Any,
        *,
        task_id: str,
        business_key: str | None = None,
        cancelled: Callable[[], bool] | None = None,
        emit: Callable[[dict[str, Any]], Any] | None = None,
    ) -> LoopResult:
        """Continue a paused run with the person's answer."""
        return await self._invoke(Command(resume=answer), task_id, business_key, cancelled, emit)

    async def run_in(
        self, ctx: Any, request: str | list[dict[str, Any]], *, business_key: str | None = None
    ) -> LoopResult:
        """Run inside a skill: the task, the cancel signal and progress come from ``ctx``.

        On a task paused for a person, ``request`` is their answer and the run resumes. On a task
        whose run was cut off mid-way (its pod died), the run continues from its last checkpoint
        and ``request`` is not used.

        A pause whose ``input_request`` names the write it holds back (``question``, ``call_id``
        and ``tool_name``) is sent to the platform as a write-confirmation pause. The platform
        then resumes the same run when it is decided, so the saved state is found. Any other
        pause is a plain question, and its answer starts a new run.
        """
        result = await self._run_in(ctx, request, business_key)
        approval = result.approval
        updater = getattr(ctx, "task_updater", None)
        if approval is not None and updater is not None:
            question, call_id, tool_name = approval
            await updater.requires_input(
                MessageBuilder.create_write_confirmation_required_message(
                    content=question, call_id=call_id, tool_name=tool_name
                )
            )
        return result

    async def _run_in(
        self, ctx: Any, request: str | list[dict[str, Any]], business_key: str | None
    ) -> LoopResult:
        kwargs: dict[str, Any] = {
            "task_id": ctx.task_id,
            "business_key": business_key,
            "cancelled": lambda: ctx.is_cancelled,
            "emit": lambda event: ctx._emit_platform_event(STEP_EVENT, event),
        }
        try:
            snapshot = await self._compiled().aget_state(self._config(ctx.task_id))
        except Exception as exc:
            # Not knowing where the task stands must not start it over: report the failure
            # so the platform retries the message.
            return _result(_stop(StopReason.FAILED, error=describe_exception(exc)))
        if snapshot.interrupts:
            paused = _result({**snapshot.values, "__interrupt__": snapshot.interrupts})
            approval = approval_request(paused.input_request, self._held_writes(snapshot.values))
            if approval is not None and parse_decision(request) is None:
                # An approval is decided by the platform's decision, not by any next message: a
                # re-sent or retried message leaves the run paused, and its pause is sent again.
                return dataclasses.replace(paused, approval=approval)
            # The pod that paused may be the one that died, and its lease lives on for about a
            # minute: wait it out before the answer runs anything.
            if (stopped := await self._lease_or_stop(ctx, kwargs)) is not None:
                return stopped
            return await self.resume(request, **kwargs)
        if snapshot.next and not snapshot.values.get("stop_reason"):
            # Steps are left and no stop was recorded: the run was cut off, not stopped. Take the
            # task's lease before any step runs, so a write is never run while another holder
            # may still be running the same task. A killed pod's lease lasts about a minute, so a
            # held lease is waited out (up to ``lease_wait_s``) rather than failing the run.
            if (stopped := await self._lease_or_stop(ctx, kwargs)) is not None:
                return stopped
            return await self._invoke(None, **kwargs)
        return await self.run(request, **kwargs)

    async def _lease_or_stop(self, ctx: Any, kwargs: dict[str, Any]) -> LoopResult | None:
        """Take the task's lease, waiting out a held one; a stop result when it cannot be taken."""
        try:
            acquire = getattr(self.checkpointer, "aacquire", None)
            if acquire is not None and not await self._acquire_lease(acquire, ctx, kwargs):
                return _result(_stop(StopReason.CANCELLED))
        except Exception as exc:
            return _result(_stop(StopReason.FAILED, error=describe_exception(exc)))
        return None

    def _lease_wait_s(self) -> float:
        """``lease_wait_s``, else the saver's lease life plus 15 s (75 s for a 60 s lease)."""
        if self.lease_wait_s is not None:
            return self.lease_wait_s
        return (getattr(self.checkpointer, "lease_ttl_s", None) or 60) + 15

    async def _acquire_lease(
        self, acquire: Callable[[str], Awaitable[None]], ctx: Any, kwargs: dict[str, Any]
    ) -> bool:
        """Take the lease, retrying a conflict for up to ``lease_wait_s``. False if cancelled."""
        deadline = time.monotonic() + self._lease_wait_s()
        while True:
            try:
                await acquire(ctx.task_id)
                return True
            except LeaseConflict:
                if kwargs["cancelled"]():
                    return False
                if time.monotonic() + self.lease_retry_s > deadline:
                    raise
                await asyncio.sleep(self.lease_retry_s)

    async def checkpoint(self, task_id: str) -> dict[str, Any] | None:
        """The state in the task's latest checkpoint, or None if it has none."""
        snapshot = await self._compiled().aget_state(self._config(task_id))
        return dict(snapshot.values) if snapshot.values else None

    def _compiled(self) -> Any:
        return self.graph.compile(checkpointer=self.checkpointer)

    @staticmethod
    def _config(task_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": task_id}}

    @staticmethod
    def _lock_for(task_id: str) -> asyncio.Lock:
        lock = _LOCKS.get(task_id)
        if lock is None:
            lock = asyncio.Lock()
            _LOCKS[task_id] = lock
        return lock

    async def _invoke(
        self,
        graph_input: Any,
        task_id: str,
        business_key: str | None,
        cancelled: Callable[[], bool] | None,
        emit: Callable[[dict[str, Any]], Any] | None,
    ) -> LoopResult:
        if self._writes and not business_key:
            raise ValueError("business_key is required when the loop has write tools")
        env = _RunEnv(
            loop=self,
            task_id=task_id,
            business_key=business_key,
            cancelled=cancelled or (lambda: False),
            emit=emit,
            deadline=time.monotonic() + self.limits.max_wall_time_s,
        )
        graph = self._compiled()
        config = self._config(task_id)
        # shortcut: writing loops save every step synchronously rather than only before a
        # write; switch to per-step durability if save latency shows up in the step time.
        durability = "sync" if self._writes else "async"
        async with self._lock_for(task_id):
            token = _ENV.set(env)
            try:
                try:
                    values = await graph.ainvoke(graph_input, config, durability=durability)
                except asyncio.CancelledError:
                    if not env.cancelled():
                        raise
                    # CancelTask: keep the partial state and record the stop in a final save.
                    current = asyncio.current_task()
                    if current is not None:
                        current.uncancel()
                    await graph.aupdate_state(config, _stop(StopReason.CANCELLED))
                    values = (await graph.aget_state(config)).values
            except Exception as exc:
                # A checkpointer, emit callback or custom node failed: still a stop reason.
                values = _stop(StopReason.FAILED, error=describe_exception(exc))
            finally:
                _ENV.reset(token)
            result = _result(values)
            if result.paused:
                result = dataclasses.replace(result, approval=self._held_approval(result, values))
            if self.check is not None and result.in_run_check is None and not result.paused:
                result = dataclasses.replace(result, in_run_check=not_run_record())
            if (
                result.summary
                and not result.paused
                and self.context is not None
                and not self.context.keep_summary
            ):
                # The summary goes back with the result; the checkpoint keeps none.
                try:
                    await graph.aupdate_state(config, {"summary": ""})
                except Exception:
                    logger.warning(
                        "could not clear the summary from the checkpoint of task %s",
                        task_id,
                        exc_info=True,
                    )
            if self._owns_saver:
                await self._forget_stopped(task_id, paused=result.paused)
        return result

    def _held_writes(self, values: dict[str, Any]) -> list[dict[str, Any]]:
        """The write calls (tools that are not read-only) the paused turn holds."""
        return [
            c
            for c in values.get("pending_calls") or []
            if (spec := self.tool_specs.get(c["name"])) is not None and not spec.read_only
        ]

    def _held_approval(
        self, result: LoopResult, values: dict[str, Any]
    ) -> tuple[str, str, str] | None:
        """The approval a pause asks for, if it names a write call this turn holds."""
        approval = approval_request(result.input_request, self._held_writes(values))
        if approval is None and approval_fields(result.input_request) is not None:
            logger.warning(
                "input_request names a call that is not a write the turn holds; "
                "pausing as a plain question"
            )
        return approval

    async def _forget_stopped(self, task_id: str, *, paused: bool) -> None:
        """Keep the default saver bounded: drop a stopped task, and the oldest pauses past
        :data:`MAX_PAUSED_TASKS`."""
        self._paused.pop(task_id, None)
        if not paused:
            await self.checkpointer.adelete_thread(task_id)
            return
        self._paused[task_id] = None
        while len(self._paused) > MAX_PAUSED_TASKS:
            oldest, _ = self._paused.popitem(last=False)
            await self.checkpointer.adelete_thread(oldest)


def _result(values: dict[str, Any]) -> LoopResult:
    usage = Usage.from_dict(values.get("usage"))
    messages = list(values.get("messages") or [])
    interrupts = values.get("__interrupt__")
    if interrupts:
        return LoopResult(
            stop_reason=None,
            usage=usage,
            input_request=interrupts[0].value,
            messages=messages,
            summary=values.get("summary") or None,
        )
    reason = values.get("stop_reason")
    limit = values.get("limit")
    return LoopResult(
        stop_reason=StopReason(reason) if reason else StopReason.FAILED,
        usage=usage,
        output=values.get("output"),
        in_run_check=values.get("in_run_check"),
        limit=LimitName(limit) if limit else None,
        error=values.get("error") if reason else "the run ended without a stop reason",
        messages=messages,
        summary=values.get("summary") or None,
    )
