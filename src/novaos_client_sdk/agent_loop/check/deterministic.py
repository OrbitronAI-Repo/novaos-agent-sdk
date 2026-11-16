"""Deterministic checks: named, registered by the agent, run before any judge."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial

logger = logging.getLogger(__name__)

#: A check reads the answer and returns why it fails, or ``None`` when it holds.
Check = Callable[[str], str | None]


_MARKER = re.compile(r"\[(\d+)\]")
_CODE = re.compile(r"```.*?```|`[^`\n]*`", re.DOTALL)


@dataclass(frozen=True)
class DeterministicOutcome:
    """One check that ran, and whether it held."""

    check: str
    passed: bool


def output_schema(validator: Callable[[str], object]) -> Check:
    """A check from a validator that raises on an invalid answer, such as ``AgentLoop``'s."""

    def check(answer: str) -> str | None:
        try:
            validator(answer)
        except Exception as exc:
            return str(exc)
        return None

    return check


def citations_resolve(answer: str, contexts: Sequence[str]) -> str | None:
    """Fail when a ``[n]`` marker has no retrieved context: ``[n]`` is the n-th one, from 1.

    Only this form counts as a citation. An answer with no markers holds; whether it must cite
    is for the manifest to say. Code spans and fenced blocks are skipped; any other bracketed
    number (a year, a JSON array element) is read as a marker.
    """
    dangling = sorted(
        {int(n) for n in _MARKER.findall(_CODE.sub("", answer)) if not 1 <= int(n) <= len(contexts)}
    )
    if dangling:
        return "citation with no retrieved context: " + ", ".join(f"[{n}]" for n in dangling)
    return None


def run_deterministic(
    names: Sequence[str],
    registry: Mapping[str, Check],
    answer: str,
    contexts: Sequence[str] = (),
) -> list[DeterministicOutcome]:
    """Run the checks the manifest names, in order, stopping at the first that fails.

    A name the agent has no check for fails: an answer is not passed on a check nobody ran. The
    one built-in, ``citations_resolve``, reads the run's tool results; a registered check of the
    same name replaces it.
    """
    outcomes: list[DeterministicOutcome] = []
    for name in names:
        check = registry.get(name)
        if check is None and name == "citations_resolve":
            check = partial(citations_resolve, contexts=contexts)
        if check is None:
            logger.warning("no deterministic check is registered for %r", name)
            return [*outcomes, DeterministicOutcome(name, False)]
        try:
            failure = check(answer)
        except Exception:
            logger.warning("deterministic check %r raised", name, exc_info=True)
            return [*outcomes, DeterministicOutcome(name, False)]
        if failure is not None:
            return [*outcomes, DeterministicOutcome(name, False)]
        outcomes.append(DeterministicOutcome(name, True))
    return outcomes
