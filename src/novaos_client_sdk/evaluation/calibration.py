"""
Human calibration workflow for LLM-as-judge graders.

Provides stratified sampling, calibration metric computation (agreement rate,
Cohen's kappa, MAE/RMSE, bias), and calibration scheduling.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("novaos.evaluation.calibration")

MIN_AGREEMENT_RATE = 0.80
MAX_MAE = 0.15
MAX_BIAS = 0.10
MIN_SAMPLES_FOR_CALIBRATION = 20


@dataclass
class CalibrationSample:
    """A single sample for human review."""

    trace_id: str
    grader_name: str
    llm_score: float
    human_score: float | None = None
    input_text: str = ""
    output_text: str = ""
    score_bucket: str = ""


@dataclass
class CalibrationMetrics:
    """Computed calibration metrics between LLM grader and human scores."""

    grader_name: str
    sample_count: int = 0
    agreement_rate: float = 0.0
    cohens_kappa: float = 0.0
    mae: float = 0.0
    mse: float = 0.0
    rmse: float = 0.0
    bias: float = 0.0
    needs_recalibration: bool = False
    details: str = ""


@dataclass
class CalibrationReport:
    metrics: list[CalibrationMetrics] = field(default_factory=list)
    overall_health: str = "unknown"
    recommendations: list[str] = field(default_factory=list)


class LLMGraderCalibration:
    """Manages the human calibration workflow for LLM-as-judge graders."""

    def __init__(
        self,
        agreement_threshold: float = 0.15,
        min_samples: int = MIN_SAMPLES_FOR_CALIBRATION,
    ):
        self._threshold = agreement_threshold
        self._min_samples = min_samples

    def sample_for_review(
        self,
        scored_items: list[dict[str, Any]],
        grader_name: str,
        sample_size: int = 30,
    ) -> list[CalibrationSample]:
        """Stratified sampling by score bucket (low / medium / high)."""
        buckets: dict[str, list[dict[str, Any]]] = {"low": [], "medium": [], "high": []}

        for item in scored_items:
            raw_score = item.get("score")
            score = float(raw_score) if raw_score is not None else 0.5
            if score < 0.33:
                buckets["low"].append(item)
            elif score < 0.67:
                buckets["medium"].append(item)
            else:
                buckets["high"].append(item)

        per_bucket = max(1, sample_size // 3)
        samples: list[CalibrationSample] = []

        for bucket_name, items in buckets.items():
            selected = items[:per_bucket]
            for item in selected:
                samples.append(
                    CalibrationSample(
                        trace_id=item.get("trace_id", ""),
                        grader_name=grader_name,
                        llm_score=item.get("score", 0.0),
                        input_text=item.get("input", ""),
                        output_text=item.get("output", ""),
                        score_bucket=bucket_name,
                    )
                )

        return samples

    def compute_calibration_metrics(
        self,
        samples: list[CalibrationSample],
    ) -> CalibrationMetrics:
        """Compute agreement, kappa, MAE, RMSE, and bias from reviewed samples."""
        grader_name = samples[0].grader_name if samples else "unknown"
        reviewed = [s for s in samples if s.human_score is not None]

        if len(reviewed) < self._min_samples:
            return CalibrationMetrics(
                grader_name=grader_name,
                sample_count=len(reviewed),
                needs_recalibration=False,
                details=f"Insufficient samples ({len(reviewed)}/{self._min_samples})",
            )

        n = len(reviewed)

        # Agreement rate: proportion within threshold
        agreements = sum(
            1
            for s in reviewed
            if abs(s.llm_score - s.human_score) <= self._threshold  # type: ignore[operator]
        )
        agreement_rate = agreements / n

        # MAE, MSE, RMSE
        errors = [s.llm_score - s.human_score for s in reviewed]  # type: ignore[operator]
        mae = sum(abs(e) for e in errors) / n
        mse = sum(e**2 for e in errors) / n
        rmse = math.sqrt(mse)

        # Bias (mean signed error)
        bias = sum(errors) / n

        # Cohen's kappa (discretized into 3 buckets)
        def _bucket(v: float) -> int:
            if v < 0.33:
                return 0
            if v < 0.67:
                return 1
            return 2

        observed_agreement = (
            sum(
                1
                for s in reviewed
                if _bucket(s.llm_score) == _bucket(s.human_score)  # type: ignore[arg-type]
            )
            / n
        )

        llm_dist = [0, 0, 0]
        human_dist = [0, 0, 0]
        for s in reviewed:
            llm_dist[_bucket(s.llm_score)] += 1
            human_dist[_bucket(s.human_score)] += 1  # type: ignore[arg-type]

        expected_agreement = sum((llm_dist[i] / n) * (human_dist[i] / n) for i in range(3))
        if expected_agreement >= 1.0:
            kappa = 1.0 if observed_agreement >= 1.0 else 0.0
        else:
            kappa = (observed_agreement - expected_agreement) / (1 - expected_agreement)

        needs_recalibration = (
            agreement_rate < MIN_AGREEMENT_RATE or mae > MAX_MAE or abs(bias) > MAX_BIAS
        )

        return CalibrationMetrics(
            grader_name=grader_name,
            sample_count=n,
            agreement_rate=round(agreement_rate, 4),
            cohens_kappa=round(kappa, 4),
            mae=round(mae, 4),
            mse=round(mse, 4),
            rmse=round(rmse, 4),
            bias=round(bias, 4),
            needs_recalibration=needs_recalibration,
        )

    def generate_calibration_report(
        self,
        metrics_list: list[CalibrationMetrics],
    ) -> CalibrationReport:
        """Generate a summary report across multiple graders."""
        recommendations: list[str] = []
        any_needs_recal = False

        for m in metrics_list:
            if m.needs_recalibration:
                any_needs_recal = True
                parts = []
                if m.agreement_rate < MIN_AGREEMENT_RATE:
                    parts.append(f"agreement {m.agreement_rate:.0%} < {MIN_AGREEMENT_RATE:.0%}")
                if m.mae > MAX_MAE:
                    parts.append(f"MAE {m.mae:.3f} > {MAX_MAE}")
                if abs(m.bias) > MAX_BIAS:
                    parts.append(f"bias {m.bias:+.3f} exceeds +/-{MAX_BIAS}")
                recommendations.append(f"Recalibrate '{m.grader_name}': {'; '.join(parts)}")

        health = "healthy" if not any_needs_recal else "needs_attention"

        return CalibrationReport(
            metrics=metrics_list,
            overall_health=health,
            recommendations=recommendations,
        )

    @staticmethod
    def get_calibration_schedule() -> dict[str, str]:
        """Suggested calibration frequencies by grader type."""
        return {
            "faithfulness": "weekly",
            "relevance": "weekly",
            "context_relevance": "biweekly",
            "custom_llm_judge": "after each prompt change",
            "safety": "monthly",
            "tone": "monthly",
        }
