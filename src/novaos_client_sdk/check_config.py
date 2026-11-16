"""The in-run check configuration the platform resolved for a run.

The platform attaches it to the run's ``novaos.platform/context`` as
``in_run_check``: the judge model, the rubric texts with their ids and versions,
and the bar each rubric must meet. This module only decodes it. It reads no
environment variable and no skill argument, so nothing the agent's own process
controls can change which rubrics are judged or how high the bar is.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypeGuard


@dataclass(frozen=True)
class ResolvedRubric:
    """One rubric as the platform resolved it for this run."""

    #: ``publisher/name``, for example ``novaos/relevance``.
    id: str
    version: int
    text: str
    #: The bar this rubric must meet, already raised by every applicable floor.
    threshold: float


@dataclass(frozen=True)
class InRunCheckConfig:
    """The whole resolved configuration of a run's in-run check."""

    judge_model: str
    rubrics: tuple[ResolvedRubric, ...]
    max_refinements: int | None = None
    budget_ms: int | None = None
    deterministic: tuple[str, ...] = ()
    regulated_outputs: tuple[str, ...] = ()


def _is_int(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> TypeGuard[int | float]:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _rubric(raw: Any) -> ResolvedRubric | None:
    if not isinstance(raw, Mapping):
        return None
    rubric_id, version = raw.get("id"), raw.get("version")
    text, threshold = raw.get("text"), raw.get("threshold")
    if not (isinstance(rubric_id, str) and rubric_id):
        return None
    if not _is_int(version) or version < 1:
        return None
    if not (isinstance(text, str) and text):
        return None
    if not _is_number(threshold):
        return None
    return ResolvedRubric(rubric_id, version, text, float(threshold))


def _strings(raw: Any) -> tuple[str, ...] | None:
    if raw is None:
        return ()
    if isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        return tuple(raw)
    return None


def _optional_int(raw: Any) -> tuple[bool, int | None]:
    if raw is None:
        return True, None
    return (True, raw) if _is_int(raw) else (False, None)


def resolve_in_run_check(novaos_context: Mapping[str, Any]) -> InRunCheckConfig | None:
    """Decode ``in_run_check`` from the platform's context, or ``None``.

    Anything malformed gives ``None`` for the whole configuration rather than a
    partial one: a check that silently dropped a rubric would pass answers the
    platform meant to hold to it.

    Args:
        novaos_context: The decoded ``novaos.platform/context`` payload.
    """
    raw = novaos_context.get("in_run_check")
    if not isinstance(raw, Mapping):
        return None
    judge_model = raw.get("judge_model")
    raw_rubrics = raw.get("rubrics")
    if not (isinstance(judge_model, str) and judge_model):
        return None
    if not isinstance(raw_rubrics, list) or not raw_rubrics:
        return None
    rubrics = [_rubric(item) for item in raw_rubrics]
    if any(rubric is None for rubric in rubrics):
        return None
    deterministic = _strings(raw.get("deterministic"))
    regulated_outputs = _strings(raw.get("regulated_outputs"))
    refinements_ok, max_refinements = _optional_int(raw.get("max_refinements"))
    budget_ok, budget_ms = _optional_int(raw.get("budget_ms"))
    if deterministic is None or regulated_outputs is None or not (refinements_ok and budget_ok):
        return None
    return InRunCheckConfig(
        judge_model=judge_model,
        rubrics=tuple(r for r in rubrics if r is not None),
        max_refinements=max_refinements,
        budget_ms=budget_ms,
        deterministic=deterministic,
        regulated_outputs=regulated_outputs,
    )
