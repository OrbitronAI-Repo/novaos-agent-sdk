"""
Statistical tools for NovaOS evaluation: multi-trial analysis and A/B testing.

- ``MultiTrialRunner``: repeated execution for variance, pass@k, pass^k.
- ``ABTestAnalyzer``: Welch's t-test between two variant score distributions.
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TrialResult:
    """Result of a single trial."""

    trial_number: int = 0
    success: bool = False
    scores: dict[str, float] = field(default_factory=dict)
    output: Any = None
    latency_ms: float = 0.0
    error: str | None = None


@dataclass
class MultiTrialResult:
    """Aggregated results of running the same input multiple times.

    Carries **per-metric** score statistics (mean/std/min/max per score name)
    matching the Galaxy evaluation framework, plus overall aggregate values
    for backward compatibility.
    """

    input_data: Any = None
    num_trials: int = 0
    success_count: int = 0
    success_rate: float = 0.0
    pass_at_k: float = 0.0
    pass_pow_k: float = 0.0

    # per-metric score stats (Galaxy-compatible)
    mean_scores: dict[str, float] = field(default_factory=dict)
    std_scores: dict[str, float] = field(default_factory=dict)
    min_scores: dict[str, float] = field(default_factory=dict)
    max_scores: dict[str, float] = field(default_factory=dict)

    # single-metric aggregate (backward compat)
    score_mean: float = 0.0
    score_std: float = 0.0
    score_min: float = 0.0
    score_max: float = 0.0

    # latency percentiles
    latency_mean_ms: float = 0.0
    latency_std_ms: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0

    raw_scores: list[float] = field(default_factory=list)
    raw_latencies: list[float] = field(default_factory=list)
    trials: list[TrialResult] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Trials: {self.num_trials}",
            f"Success Rate: {self.success_rate:.1%}",
            f"pass@k: {self.pass_at_k:.1%}",
            f"pass^k: {self.pass_pow_k:.1%} (reliability)",
            f"Mean Latency: {self.latency_mean_ms:.0f}ms",
            f"P95 Latency: {self.latency_p95_ms:.0f}ms",
        ]
        if self.mean_scores:
            lines.append("Scores:")
            for name in sorted(self.mean_scores):
                lines.append(
                    f"  {name}: mean={self.mean_scores[name]:.3f} "
                    f"std={self.std_scores.get(name, 0):.3f} "
                    f"[{self.min_scores.get(name, 0):.3f}, {self.max_scores.get(name, 0):.3f}]"
                )
        return "\n".join(lines)


class MultiTrialRunner:
    """Run the same input N times to measure variance and compute pass@k / pass^k.

    ``k`` is the number of samples to draw for the pass@k estimator (defaults to 1).
    """

    def __init__(
        self,
        num_trials: int = 5,
        success_threshold: float = 0.7,
        success_metric: str = "overall",
        k: int = 1,
    ):
        self._num_trials = num_trials
        self._threshold = success_threshold
        self._metric = success_metric
        self._k = max(1, k)

    async def run(
        self,
        agent_fn: Callable[[str], Awaitable[Any]],
        input_data: str,
        score_fn: Callable[[Any], float | dict[str, float]] | None = None,
    ) -> MultiTrialResult:
        """Execute ``agent_fn(input_data)`` N times and compute statistics.

        ``score_fn`` may return either:
        - a single ``float`` (stored under ``success_metric`` name), or
        - a ``dict[str, float]`` with named scores (Galaxy-compatible).

        If ``score_fn`` is None, each non-exception trial scores 1.0 under
        the ``success_metric`` name.

        Success is determined by ``scores[success_metric] >= success_threshold``.
        """
        import time

        trials: list[TrialResult] = []

        for trial_num in range(self._num_trials):
            start = time.perf_counter()
            try:
                result = await agent_fn(input_data)
                elapsed = (time.perf_counter() - start) * 1000

                scores: dict[str, float] = {}
                if score_fn is not None:
                    raw = score_fn(result)
                    if isinstance(raw, dict):
                        scores = raw
                    else:
                        scores = {self._metric: float(raw)}
                else:
                    scores = {self._metric: 1.0}

                primary = scores.get(self._metric)
                if primary is None:
                    primary = 0.0
                success = primary >= self._threshold

                trials.append(
                    TrialResult(
                        trial_number=trial_num + 1,
                        success=success,
                        scores=scores,
                        output=result,
                        latency_ms=elapsed,
                    )
                )
            except Exception as exc:
                elapsed = (time.perf_counter() - start) * 1000
                trials.append(
                    TrialResult(
                        trial_number=trial_num + 1,
                        success=False,
                        scores={},
                        output=None,
                        latency_ms=elapsed,
                        error=str(exc),
                    )
                )

        return self._compute_statistics(input_data, trials)

    def _compute_statistics(self, input_data: Any, trials: list[TrialResult]) -> MultiTrialResult:
        n = len(trials)
        successes = sum(1 for t in trials if t.success)
        success_rate = successes / n if n > 0 else 0.0

        k = min(self._k, n) if n > 0 else 1
        denom = math.comb(n, k)
        if denom > 0 and n > 0:
            pass_at_k = 1.0 - math.comb(n - successes, k) / denom
        else:
            pass_at_k = 0.0
        pass_pow_k = success_rate**k if n > 0 else 0.0

        # per-metric score stats (Galaxy-style)
        all_scores: dict[str, list[float]] = {}
        for t in trials:
            for name, val in t.scores.items():
                all_scores.setdefault(name, []).append(val)

        mean_scores: dict[str, float] = {}
        std_scores: dict[str, float] = {}
        min_scores: dict[str, float] = {}
        max_scores: dict[str, float] = {}

        for name, vals in all_scores.items():
            if vals:
                mean_scores[name] = sum(vals) / len(vals)
                min_scores[name] = min(vals)
                max_scores[name] = max(vals)
                if len(vals) >= 2:
                    m = mean_scores[name]
                    var = sum((v - m) ** 2 for v in vals) / (len(vals) - 1)
                    std_scores[name] = math.sqrt(var)
                else:
                    std_scores[name] = 0.0

        # single-metric aggregate (backward compat: primary metric)
        primary_vals = all_scores.get(self._metric, [])
        p_mean = sum(primary_vals) / len(primary_vals) if primary_vals else 0.0
        p_var = (
            sum((v - p_mean) ** 2 for v in primary_vals) / (len(primary_vals) - 1)
            if len(primary_vals) >= 2
            else 0.0
        )

        # latency stats
        latencies = [t.latency_ms for t in trials]
        lat_mean = sum(latencies) / n if n else 0.0
        lat_var = sum((lat - lat_mean) ** 2 for lat in latencies) / (n - 1) if n >= 2 else 0.0
        sorted_lat = sorted(latencies)
        p50_idx = max(0, int(n * 0.5) - 1)
        p95_idx = max(0, int(n * 0.95) - 1)

        return MultiTrialResult(
            input_data=input_data,
            num_trials=n,
            success_count=successes,
            success_rate=success_rate,
            pass_at_k=pass_at_k,
            pass_pow_k=pass_pow_k,
            mean_scores=mean_scores,
            std_scores=std_scores,
            min_scores=min_scores,
            max_scores=max_scores,
            score_mean=round(p_mean, 4),
            score_std=round(math.sqrt(p_var), 4),
            score_min=round(min(primary_vals), 4) if primary_vals else 0.0,
            score_max=round(max(primary_vals), 4) if primary_vals else 0.0,
            latency_mean_ms=round(lat_mean, 1),
            latency_std_ms=round(math.sqrt(lat_var), 1),
            latency_p50_ms=round(sorted_lat[p50_idx], 1) if sorted_lat else 0.0,
            latency_p95_ms=round(sorted_lat[p95_idx], 1) if sorted_lat else 0.0,
            raw_scores=primary_vals,
            raw_latencies=latencies,
            trials=trials,
        )


@dataclass
class ABTestResult:
    """Result of comparing two score distributions."""

    control_mean: float
    variant_mean: float
    lift: float
    t_statistic: float
    p_value: float
    significant: bool
    confidence_level: float
    control_n: int
    variant_n: int
    required_sample_size: int


class ABTestAnalyzer:
    """Welch's t-test comparing score distributions of two agent variants."""

    def __init__(self, confidence_level: float = 0.95):
        self._confidence = confidence_level

    def compare(
        self,
        control_scores: list[float],
        variant_scores: list[float],
    ) -> ABTestResult:
        """Run Welch's t-test between control and variant score lists."""
        n1 = len(control_scores)
        n2 = len(variant_scores)

        if n1 < 2 or n2 < 2:
            return ABTestResult(
                control_mean=sum(control_scores) / n1 if n1 else 0,
                variant_mean=sum(variant_scores) / n2 if n2 else 0,
                lift=0,
                t_statistic=0,
                p_value=1.0,
                significant=False,
                confidence_level=self._confidence,
                control_n=n1,
                variant_n=n2,
                required_sample_size=self._required_sample_size(0.5, 0.5),
            )

        mean1 = sum(control_scores) / n1
        mean2 = sum(variant_scores) / n2
        var1 = sum((x - mean1) ** 2 for x in control_scores) / (n1 - 1)
        var2 = sum((x - mean2) ** 2 for x in variant_scores) / (n2 - 1)

        se = math.sqrt(var1 / n1 + var2 / n2) if (var1 / n1 + var2 / n2) > 0 else 1e-10
        t_stat = (mean2 - mean1) / se

        # Welch-Satterthwaite degrees of freedom
        num = (var1 / n1 + var2 / n2) ** 2
        denom = (
            (var1 / n1) ** 2 / (n1 - 1) + (var2 / n2) ** 2 / (n2 - 1) if (n1 > 1 and n2 > 1) else 1
        )
        df = num / denom if denom > 0 else 1

        p_value = self._t_to_p(abs(t_stat), df)
        alpha = 1 - self._confidence
        significant = p_value < alpha
        lift = (mean2 - mean1) / mean1 if mean1 != 0 else 0

        return ABTestResult(
            control_mean=round(mean1, 4),
            variant_mean=round(mean2, 4),
            lift=round(lift, 4),
            t_statistic=round(t_stat, 4),
            p_value=round(p_value, 6),
            significant=significant,
            confidence_level=self._confidence,
            control_n=n1,
            variant_n=n2,
            required_sample_size=self._required_sample_size(mean1, math.sqrt(max(var1, var2))),
        )

    def compare_multiple_variants(
        self,
        control_scores: list[float],
        variants: dict[str, list[float]],
    ) -> dict[str, ABTestResult]:
        return {name: self.compare(control_scores, scores) for name, scores in variants.items()}

    @staticmethod
    def _required_sample_size(
        baseline_rate: float,
        std: float = 0.5,
        mde: float = 0.05,
        alpha: float = 0.05,
        power: float = 0.8,
    ) -> int:
        """Approximate sample size for a two-sample proportion test."""
        z_alpha = 1.96 if alpha <= 0.05 else 1.645
        z_beta = 0.84 if power >= 0.8 else 0.675
        if mde <= 0:
            return 9999
        n = (2 * (std**2) * (z_alpha + z_beta) ** 2) / (mde**2)
        return max(2, int(math.ceil(n)))

    @staticmethod
    def _t_to_p(t: float, df: float) -> float:
        """Approximate two-tailed p-value from t-statistic using normal approximation for large df."""
        if df > 30:
            z = t
        else:
            z = t * (1 - 1 / (4 * df)) if df > 0 else t

        # Standard normal CDF approximation (Abramowitz & Stegun)
        a1 = 0.254829592
        a2 = -0.284496736
        a3 = 1.421413741
        a4 = -1.453152027
        a5 = 1.061405429
        p_param = 0.3275911

        sign = 1 if z >= 0 else -1
        z_abs = abs(z)
        t_val = 1.0 / (1.0 + p_param * z_abs)
        y = 1.0 - (
            ((((a5 * t_val + a4) * t_val) + a3) * t_val + a2) * t_val + a1
        ) * t_val * math.exp(-z_abs * z_abs / 2.0)

        cdf = 0.5 * (1.0 + sign * y)
        return 2 * (1 - cdf)
