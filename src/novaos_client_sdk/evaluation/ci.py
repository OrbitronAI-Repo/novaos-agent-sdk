"""
CI/CD evaluation gate for NovaOS agents.

Provides ``EvaluationGate`` to run experiments and check thresholds, ``CIReporter``
for GitHub Actions annotations / Markdown / JSON output, and a CLI entrypoint.

Usage::

    python -m novaos_client_sdk.evaluation.ci \\
        --local --test-file tests/eval_cases.json \\
        --agent-module myagent --agent-function handle \\
        --min-success-rate 0.9 --version 1.2.0
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from novaos_client_sdk.evaluation.experiments import (
    ExperimentResult,
    ExperimentRunner,
    LocalExperimentRunner,
)

logger = logging.getLogger("novaos.evaluation.ci")


@dataclass
class EvalThresholds:
    """Minimum acceptable scores for the CI gate.

    Mirrors the Galaxy evaluation thresholds.  Defaults are production-grade;
    set a metric to ``0.0`` to disable it.  If a metric has a positive
    threshold but no grader produces a score with that name, the gate
    **fails closed** (marks the metric as MISSING / failed).

    Percentile thresholds are enforced against values computed from
    ``ExperimentResult.item_results``:

    - ``max_latency_p95_ms``: P95 of raw latencies in milliseconds.
    - ``min_cost_p95_score``: minimum acceptable **P5** of the CostGrader
      score (0.0–1.0 scale where 1.0 = within budget).  P5 is used because
      cost scores are higher-is-better; checking P5 ≥ threshold guarantees
      that 95 % of runs score at or above the threshold.
    """

    tool_call_success: float = 0.95
    latency: float = 0.90
    cost: float = 0.95
    error_rate: float = 0.95
    ttft: float = 0.7
    faithfulness: float = 0.0
    relevance: float = 0.0
    success_rate: float = 0.90
    max_latency_p95_ms: float = 10_000
    min_cost_p95_score: float = 0.5

    def to_dict(self) -> dict[str, float]:
        return {
            "tool_call_success": self.tool_call_success,
            "latency": self.latency,
            "cost": self.cost,
            "error_rate": self.error_rate,
            "ttft": self.ttft,
            "faithfulness": self.faithfulness,
            "relevance": self.relevance,
            "success_rate": self.success_rate,
            "max_latency_p95_ms": self.max_latency_p95_ms,
            "min_cost_p95_score": self.min_cost_p95_score,
        }


@dataclass
class GateResult:
    passed: bool
    experiment_result: ExperimentResult
    threshold_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    summary: str = ""


class EvaluationGate:
    """Runs an evaluation experiment and checks scores against thresholds.

    When ``graders`` are supplied they are forwarded to the underlying
    ``ExperimentRunner`` / ``LocalExperimentRunner`` so that every item
    is scored during the experiment.  If a configured threshold has a
    non-zero minimum but no grader produced that score, the gate **fails
    closed** (the metric is marked as missing/failed).
    """

    def __init__(
        self,
        agent_fn: Callable[[str], Awaitable[Any]],
        thresholds: EvalThresholds | None = None,
        concurrency: int = 5,
        timeout_s: float = 60.0,
        graders: list[Any] | None = None,
    ):
        self._agent_fn = agent_fn
        self._thresholds = thresholds or EvalThresholds()
        self._concurrency = concurrency
        self._timeout = timeout_s
        self._graders = graders or []

    async def check_local(
        self,
        test_cases: list[dict[str, Any]],
        experiment_name: str = "ci-gate",
    ) -> GateResult:
        runner = LocalExperimentRunner(
            self._agent_fn,
            concurrency=self._concurrency,
            timeout_s=self._timeout,
            graders=self._graders,
        )
        result = await runner.run_experiment(test_cases, experiment_name)
        return self._evaluate_result(result)

    async def check_dataset(
        self,
        dataset_name: str,
        experiment_name: str = "ci-gate",
        agent_version: str = "",
    ) -> GateResult:
        runner = ExperimentRunner(
            self._agent_fn,
            concurrency=self._concurrency,
            timeout_s=self._timeout,
            graders=self._graders,
        )
        result = await runner.run_experiment(dataset_name, experiment_name, agent_version)
        return self._evaluate_result(result)

    @staticmethod
    def _compute_percentile(values: list[float], percentile: float) -> float:
        """Compute an arbitrary percentile (0–1) of a list of values."""
        if not values:
            return 0.0
        s = sorted(values)
        idx = int(len(s) * percentile)
        return s[min(idx, len(s) - 1)]

    def _evaluate_result(self, result: ExperimentResult) -> GateResult:
        result.aggregate()
        thresholds = self._thresholds.to_dict()

        threshold_results: dict[str, dict[str, Any]] = {}
        all_passed = True

        # --- success rate ---
        if result.success_rate < self._thresholds.success_rate:
            all_passed = False
            threshold_results["success_rate"] = {
                "actual": result.success_rate,
                "threshold": self._thresholds.success_rate,
                "passed": False,
            }
        else:
            threshold_results["success_rate"] = {
                "actual": result.success_rate,
                "threshold": self._thresholds.success_rate,
                "passed": True,
            }

        _p95_keys = {"success_rate", "max_latency_p95_ms", "min_cost_p95_score"}
        # --- grader-based score thresholds ---
        for score_name, min_val in thresholds.items():
            if score_name in _p95_keys or min_val <= 0:
                continue
            actual = result.aggregated_scores.get(score_name)
            if actual is None:
                all_passed = False
                threshold_results[score_name] = {
                    "actual": None,
                    "threshold": min_val,
                    "passed": False,
                    "reason": "no grader produced this score",
                }
                continue
            passed = actual >= min_val
            if not passed:
                all_passed = False
            threshold_results[score_name] = {
                "actual": round(actual, 4),
                "threshold": min_val,
                "passed": passed,
            }

        # --- P95 latency (from successful item latencies) ---
        if self._thresholds.max_latency_p95_ms > 0:
            latencies = [ir.latency_ms for ir in result.item_results if ir.success]
            if latencies:
                p95_lat = self._compute_percentile(latencies, 0.95)
                passed = p95_lat <= self._thresholds.max_latency_p95_ms
                if not passed:
                    all_passed = False
                threshold_results["latency_p95_ms"] = {
                    "actual": round(p95_lat, 1),
                    "threshold": self._thresholds.max_latency_p95_ms,
                    "passed": passed,
                }
            else:
                all_passed = False
                threshold_results["latency_p95_ms"] = {
                    "actual": None,
                    "threshold": self._thresholds.max_latency_p95_ms,
                    "passed": False,
                    "reason": "no successful runs to compute P95 latency",
                }

        # --- P5 cost score (from CostGrader 0–1 scores) ---
        # Cost scores are higher-is-better (1.0 = within budget).  To ensure
        # 95% of runs meet the bar we check the *5th* percentile, not the 95th.
        if self._thresholds.min_cost_p95_score > 0:
            cost_scores = []
            for ir in result.item_results:
                for sc in ir.scores:
                    if sc.name == "cost" and sc.value is not None:
                        cost_scores.append(sc.value)
            if cost_scores:
                p5_cost_score = self._compute_percentile(cost_scores, 0.05)
                passed = p5_cost_score >= self._thresholds.min_cost_p95_score
                if not passed:
                    all_passed = False
                threshold_results["cost_p5_score"] = {
                    "actual": round(p5_cost_score, 4),
                    "threshold": self._thresholds.min_cost_p95_score,
                    "passed": passed,
                }
            else:
                all_passed = False
                threshold_results["cost_p5_score"] = {
                    "actual": None,
                    "threshold": self._thresholds.min_cost_p95_score,
                    "passed": False,
                    "reason": "no cost scores to compute P5",
                }

        lines = [f"Gate {'PASSED' if all_passed else 'FAILED'}"]
        lines.append(
            f"  Success rate: {result.success_rate:.1%} ({result.success_count}/{result.total_items})"
        )
        for name, info in threshold_results.items():
            if name == "success_rate":
                continue
            status = "PASS" if info["passed"] else "FAIL"
            actual_str = f"{info['actual']:.4f}" if info["actual"] is not None else "MISSING"
            op = "<=" if name == "latency_p95_ms" else ">="
            lines.append(f"  {name}: {actual_str} {op} {info['threshold']:.4f} [{status}]")

        return GateResult(
            passed=all_passed,
            experiment_result=result,
            threshold_results=threshold_results,
            summary="\n".join(lines),
        )

    async def check_with_retries(
        self,
        test_cases: list[dict[str, Any]] | None = None,
        dataset_name: str | None = None,
        retries: int = 2,
        experiment_name: str = "ci-gate",
    ) -> GateResult:
        best: GateResult | None = None
        for attempt in range(retries + 1):
            if test_cases is not None:
                result = await self.check_local(test_cases, f"{experiment_name}-attempt-{attempt}")
            elif dataset_name is not None:
                result = await self.check_dataset(
                    dataset_name, f"{experiment_name}-attempt-{attempt}"
                )
            else:
                raise ValueError("Provide test_cases or dataset_name")

            if result.passed:
                return result
            if (
                best is None
                or result.experiment_result.success_rate > best.experiment_result.success_rate
            ):
                best = result

        return best  # type: ignore[return-value]


class CIReporter:
    """Formats gate results for CI systems."""

    @staticmethod
    def github_annotations(gate_result: GateResult) -> str:
        lines = []
        for name, info in gate_result.threshold_results.items():
            if not info["passed"]:
                actual_str = f"{info['actual']:.4f}" if info["actual"] is not None else "MISSING"
                lines.append(
                    f"::error::Evaluation gate failed: {name} = {actual_str} "
                    f"(threshold: {info['threshold']:.4f})"
                )
        return "\n".join(lines)

    @staticmethod
    def markdown(gate_result: GateResult) -> str:
        status = "PASSED" if gate_result.passed else "FAILED"
        lines = [f"## Evaluation Gate: {status}", ""]
        lines.append("| Metric | Actual | Threshold | Status |")
        lines.append("|--------|--------|-----------|--------|")
        for name, info in gate_result.threshold_results.items():
            s = "PASS" if info["passed"] else "FAIL"
            actual_str = f"{info['actual']:.4f}" if info["actual"] is not None else "MISSING"
            lines.append(f"| {name} | {actual_str} | {info['threshold']:.4f} | {s} |")
        lines.append("")
        er = gate_result.experiment_result
        lines.append(
            f"Total: {er.total_items} items, {er.success_count} succeeded, {er.failure_count} failed"
        )
        return "\n".join(lines)

    @staticmethod
    def to_json(gate_result: GateResult) -> str:
        return json.dumps(
            {
                "passed": gate_result.passed,
                "success_rate": gate_result.experiment_result.success_rate,
                "total_items": gate_result.experiment_result.total_items,
                "thresholds": gate_result.threshold_results,
                "aggregated_scores": gate_result.experiment_result.aggregated_scores,
            },
            indent=2,
        )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NovaOS Evaluation CI Gate")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dataset", type=str, help="Langfuse dataset name")
    mode.add_argument("--local", action="store_true", help="Use local test file")
    parser.add_argument("--test-file", type=str, help="Path to JSON test cases (with --local)")
    parser.add_argument(
        "--agent-module", type=str, required=True, help="Python module with agent function"
    )
    parser.add_argument(
        "--agent-function", type=str, default="handle", help="Function name in module"
    )
    parser.add_argument("--version", type=str, default="", help="Agent version")
    parser.add_argument("--min-success-rate", type=float, default=0.8, help="Minimum success rate")
    parser.add_argument(
        "--output", type=str, choices=["text", "json", "markdown", "github"], default="text"
    )
    parser.add_argument("--retries", type=int, default=0, help="Number of retry attempts")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


async def _main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    module = importlib.import_module(args.agent_module)
    agent_fn = getattr(module, args.agent_function)

    from novaos_client_sdk.evaluation.graders.cost import CostGrader
    from novaos_client_sdk.evaluation.graders.error_rate import ErrorRateGrader
    from novaos_client_sdk.evaluation.graders.latency import LatencyGrader
    from novaos_client_sdk.evaluation.graders.tool_success import ToolCallSuccessGrader
    from novaos_client_sdk.evaluation.graders.ttft import TTFTGrader

    thresholds = EvalThresholds(success_rate=args.min_success_rate)
    default_graders: list[Any] = [
        ToolCallSuccessGrader(),
        LatencyGrader(),
        CostGrader(),
        TTFTGrader(),
        ErrorRateGrader(),
    ]
    gate = EvaluationGate(
        agent_fn,
        thresholds=thresholds,
        concurrency=args.concurrency,
        timeout_s=args.timeout,
        graders=default_graders,
    )

    if args.local:
        if not args.test_file:
            print("--test-file required with --local", file=sys.stderr)
            return 2
        test_cases = LocalExperimentRunner.load_test_cases(args.test_file)
        result = await gate.check_with_retries(test_cases=test_cases, retries=args.retries)
    else:
        result = await gate.check_with_retries(dataset_name=args.dataset, retries=args.retries)

    reporter = CIReporter()
    if args.output == "json":
        print(reporter.to_json(result))
    elif args.output == "markdown":
        print(reporter.markdown(result))
    elif args.output == "github":
        annotations = reporter.github_annotations(result)
        if annotations:
            print(annotations)
        print(reporter.markdown(result))
    else:
        print(result.summary)

    return 0 if result.passed else 1


def main() -> None:
    sys.exit(asyncio.run(_main()))


if __name__ == "__main__":
    main()
