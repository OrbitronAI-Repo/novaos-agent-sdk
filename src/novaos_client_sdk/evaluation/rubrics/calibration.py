"""Calibrate a rubric against its labelled good and bad cases.

Takes the judge as a callable, so it knows nothing about how rubrics are stored
or how the judge is reached.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from novaos_client_sdk.evaluation.rubrics.schema import Rubric

DEFAULT_THRESHOLD = 0.5
MIN_CLASS_ACCURACY = 0.8

Judge = Callable[[Rubric, dict[str, Any]], Awaitable[float]]


@dataclass(frozen=True)
class CalibrationResult:
    case_count: int
    good_accuracy: float
    bad_accuracy: float
    passed: bool


async def calibrate_rubric(
    rubric: Rubric,
    judge: Judge,
    min_class_accuracy: float = MIN_CLASS_ACCURACY,
) -> CalibrationResult:
    """Score each labelled case; a case is right when pass/fail matches its label.

    Both classes must reach ``min_class_accuracy``, so a judge that always passes
    or always fails cannot calibrate. Cases with only one label never calibrate.
    """
    threshold = rubric.threshold if rubric.threshold is not None else DEFAULT_THRESHOLD
    right: dict[str, int] = {"good": 0, "bad": 0}
    total: dict[str, int] = {"good": 0, "bad": 0}
    for case in rubric.labelled_cases:
        label = case["label"]
        total[label] += 1
        score = await judge(rubric, case)
        if not math.isfinite(score) or not 0.0 <= score <= 1.0:
            raise ValueError(f"judge returned an unusable score {score!r} for case {case['id']!r}")
        passed = score >= threshold
        right[label] += passed == (label == "good")

    accuracy = {k: right[k] / total[k] if total[k] else 0.0 for k in total}
    return CalibrationResult(
        case_count=len(rubric.labelled_cases),
        good_accuracy=accuracy["good"],
        bad_accuracy=accuracy["bad"],
        passed=all(total.values()) and min(accuracy.values()) >= min_class_accuracy,
    )
