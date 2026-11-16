"""The check itself: deterministic checks, a judge, at most one corrective pass, one record.

``evaluate`` decides and records; it reaches the model only through the ``revise`` callable it
is handed, and the judge only through :class:`JudgeClient`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ...check_config import InRunCheckConfig, ResolvedRubric
from ...pii import mask_pii
from .deterministic import Check, DeterministicOutcome, run_deterministic
from .judge import MAX_REASONING_CHARS, JudgeClient, JudgeRejected, JudgeReply, criteria_of

logger = logging.getLogger(__name__)

KIND = "agent_loop"
PASSED = "passed"
FAILED_KEPT_DRAFT = "failed_kept_draft"
JUDGE_ERROR = "judge_error"
NOT_RUN = "not_run"

Scored = list[tuple[ResolvedRubric, JudgeReply]]


@dataclass(frozen=True)
class InRunCheck:
    """What ``AgentLoop(check=...)`` needs: the resolved configuration, a judge, the checks."""

    config: InRunCheckConfig
    judge: JudgeClient
    checks: Mapping[str, Check] = field(default_factory=dict)
    #: The regulated fields each deterministic check covers. A regulated field in an answer
    #: passes only when a check that covers it passed; a judge score never covers it.
    covers: Mapping[str, Sequence[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class CheckOutcome:
    answer: str
    #: The ``in_run_check`` object a run reports. It never holds the judge's reasoning.
    record: dict[str, Any]
    #: The corrective message that was sent, when the revised answer replaced the draft.
    feedback: str | None = None
    #: The judge's reasoning, length-capped and PII-masked, for the trace only.
    reasoning: str = ""


def corrective_message(failures: list[tuple[ResolvedRubric, tuple[str, ...]]]) -> str:
    """Name the failed criteria by their published text. The judge's own words never appear."""
    lines: list[str] = []
    for rubric, failed in failures:
        criteria = criteria_of(rubric)
        for criterion_id in failed or tuple(criteria):
            lines.append(f'- {criterion_id}: "{criteria[criterion_id]}"')
    return (
        "Your answer did not meet these criteria:\n"
        + "\n".join(lines)
        + "\nAnswer again so it meets them, using what you already have. Do not call tools."
    )


def _keys(data: Any) -> set[str]:
    if isinstance(data, dict):
        return set(data) | {k for v in data.values() for k in _keys(v)}
    if isinstance(data, list):
        return {k for v in data for k in _keys(v)}
    return set()


def _regulated_in(answer: str, fields: tuple[str, ...]) -> set[str]:
    """The regulated fields the answer may carry. Text that is not a JSON object is not
    searched: every regulated field is assumed present."""
    if not fields:
        return set()
    try:
        data = json.loads(answer)
    except ValueError:
        return set(fields)
    return set(fields) & _keys(data) if isinstance(data, dict) else set(fields)


def _lowest(scored: Scored) -> float:
    return min(reply.verdict.score for _, reply in scored)


async def _judge_all(
    check: InRunCheck,
    answer: str,
    question: str,
    contexts: Sequence[str],
    remaining: Callable[[], float],
) -> Scored:
    async def one(rubric: ResolvedRubric) -> tuple[ResolvedRubric, JudgeReply]:
        reply = await check.judge.judge(rubric, answer, question=question, contexts=contexts)
        unknown = set(reply.verdict.failed_criteria) - set(criteria_of(rubric))
        if unknown:
            raise JudgeRejected(f"unknown criterion ids: {', '.join(sorted(unknown))}")
        return rubric, reply

    return list(
        await asyncio.wait_for(
            asyncio.gather(*(one(r) for r in check.config.rubrics)), timeout=max(remaining(), 0)
        )
    )


def not_run_record() -> dict[str, Any]:
    """The record of a run that ended before its answer was checked."""
    return {
        "ran": False,
        "kind": KIND,
        "result": NOT_RUN,
        "deterministic": [],
        "rubrics": [],
        "judge_model": None,
        "judge_model_version": None,
    }


def _record(
    check: InRunCheck,
    result: str,
    deterministic: list[DeterministicOutcome],
    scored: Scored,
    *,
    judged: bool = True,
) -> dict[str, Any]:
    versions = [reply.model_version for _, reply in scored if reply.model_version]
    return {
        "ran": True,
        "kind": KIND,
        "result": result,
        "deterministic": [{"check": d.check, "passed": d.passed} for d in deterministic],
        "rubrics": [
            {
                "ref": f"{rubric.id}@{rubric.version}",
                "score": reply.verdict.score,
                "threshold": rubric.threshold,
                "failed_criteria": list(reply.verdict.failed_criteria),
            }
            for rubric, reply in scored
        ],
        "judge_model": check.config.judge_model if judged else None,
        "judge_model_version": versions[-1] if versions else None,
    }


def _reasoning(scored: Scored) -> str:
    text = " | ".join(r.verdict.reasoning for _, r in scored if r.verdict.reasoning)
    return (mask_pii(text) or "")[:MAX_REASONING_CHARS]


async def evaluate(
    check: InRunCheck,
    *,
    answer: str,
    question: str,
    wrote: bool,
    can_revise: bool,
    revise: Callable[[str], Awaitable[str | None]],
    time_left: Callable[[], float],
    contexts: Sequence[str] = (),
) -> CheckOutcome:
    """Check ``answer`` and say which answer to keep.

    A failed deterministic check ends it before any judge call. After a write, or without
    budget, the answer is scored and never revised. A judge that errors, times out or replies
    with anything but a strict verdict keeps the draft. A regulated field is never passed on a
    judge score alone.
    """
    config = check.config
    # One deadline for the whole check: both judge calls and the corrective pass.
    budget = time_left()
    if config.budget_ms is not None:
        budget = min(budget, config.budget_ms / 1000)
    deadline = time.monotonic() + budget

    def remaining() -> float:
        return deadline - time.monotonic()

    deterministic = run_deterministic(config.deterministic, check.checks, answer, contexts)
    if not all(d.passed for d in deterministic):
        return CheckOutcome(
            answer, _record(check, FAILED_KEPT_DRAFT, deterministic, [], judged=False)
        )

    try:
        scored = await _judge_all(check, answer, question, contexts, remaining)
    except Exception:
        logger.warning("the in-run check judge failed; keeping the draft", exc_info=True)
        return CheckOutcome(answer, _record(check, JUDGE_ERROR, deterministic, []))

    kept, feedback = answer, None
    failing = [(r, v.verdict.failed_criteria) for r, v in scored if v.verdict.score < r.threshold]
    if failing and can_revise and not wrote:
        message = corrective_message(failing)
        try:
            revised = await asyncio.wait_for(revise(message), timeout=max(remaining(), 0))
        except TimeoutError:
            revised = None
        if revised and revised != answer:
            try:
                rescored = await _judge_all(check, revised, question, contexts, remaining)
            except Exception:
                logger.warning("the in-run check judge failed on the revision", exc_info=True)
                return CheckOutcome(
                    answer,
                    _record(check, JUDGE_ERROR, deterministic, scored),
                    None,
                    _reasoning(scored),
                )
            if _lowest(rescored) > _lowest(scored):
                kept, scored, feedback = revised, rescored, message

    passed = all(reply.verdict.score >= r.threshold for r, reply in scored)
    present = _regulated_in(kept, config.regulated_outputs)
    if passed and present:
        covered = {f for d in deterministic if d.passed for f in check.covers.get(d.check, ())}
        passed = present <= covered
    result = PASSED if passed else FAILED_KEPT_DRAFT
    return CheckOutcome(
        kept, _record(check, result, deterministic, scored), feedback, _reasoning(scored)
    )
