"""Registry for user-defined custom graders via the ``@custom_grader`` decorator."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score

_custom_graders: list[Grader] = []


class _FunctionGrader(Grader):
    """Wraps a plain function as a Grader instance."""

    def __init__(self, fn: Callable[[dict[str, Any]], Score], grader_name: str):
        self._fn = fn
        self._name = grader_name

    @property
    def name(self) -> str:
        return self._name

    def grade(self, transcript: dict[str, Any]) -> Score:
        return self._fn(transcript)


def custom_grader(
    name: str | None = None,
) -> Callable[[Callable[[dict[str, Any]], Score]], Callable[[dict[str, Any]], Score]]:
    """Decorator that registers a function as a custom grader.

    Usage::

        @custom_grader("domain_check")
        def check_domain(transcript: dict) -> Score:
            output = transcript.get("output", "")
            has_keywords = "compliance" in output.lower()
            return Score(name="domain_check", value=1.0 if has_keywords else 0.0)
    """

    def decorator(
        fn: Callable[[dict[str, Any]], Score],
    ) -> Callable[[dict[str, Any]], Score]:
        grader_name = name or fn.__name__
        _custom_graders.append(_FunctionGrader(fn, grader_name))
        return fn

    return decorator


def get_custom_graders() -> list[Grader]:
    """Return all registered custom graders."""
    return list(_custom_graders)
