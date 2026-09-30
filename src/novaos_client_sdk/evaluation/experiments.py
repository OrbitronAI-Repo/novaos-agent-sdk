"""
Experiment runners and dataset management for NovaOS evaluation.

- ``ExperimentRunner``: runs an agent function against a Langfuse dataset,
  applies graders to each item, and aggregates scores.
- ``LocalExperimentRunner``: same, but against local JSON test cases (no Langfuse).
- ``DatasetManager``: CRUD wrapper for Langfuse datasets.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from novaos_client_sdk.evaluation.models import Score

logger = logging.getLogger("novaos.evaluation.experiments")

AgentFn = Callable[[str], Awaitable[Any]]


@dataclass
class ItemResult:
    item_id: str
    input_data: Any
    expected_output: Any = None
    actual_output: Any = None
    scores: list[Score] = field(default_factory=list)
    success: bool = True
    error: str = ""
    latency_ms: float = 0.0


@dataclass
class ExperimentResult:
    experiment_name: str
    total_items: int = 0
    success_count: int = 0
    failure_count: int = 0
    item_results: list[ItemResult] = field(default_factory=list)
    aggregated_scores: dict[str, float] = field(default_factory=dict)
    total_latency_ms: float = 0.0
    latency_avg_ms: float = 0.0
    latency_p95_ms: float = 0.0

    @property
    def success_rate(self) -> float:
        return self.success_count / self.total_items if self.total_items > 0 else 0.0

    def aggregate(self) -> None:
        score_sums: dict[str, list[float]] = {}
        for item in self.item_results:
            for score in item.scores:
                if score.value is not None:
                    score_sums.setdefault(score.name, []).append(score.value)
        self.aggregated_scores = {
            name: sum(vals) / len(vals) for name, vals in score_sums.items() if vals
        }
        # compute latency percentiles from successful items
        latencies = sorted(ir.latency_ms for ir in self.item_results if ir.success)
        if latencies:
            self.latency_avg_ms = sum(latencies) / len(latencies)
            idx = int(len(latencies) * 0.95)
            self.latency_p95_ms = latencies[min(idx, len(latencies) - 1)]


def _build_item_transcript(ir: ItemResult) -> dict[str, Any]:
    """Build a minimal transcript dict from an ItemResult so graders can score it."""
    output_str = ""
    if ir.actual_output is not None:
        if hasattr(ir.actual_output, "output"):
            output_str = str(ir.actual_output.output) if ir.actual_output.output is not None else ""
        else:
            output_str = str(ir.actual_output)

    return {
        "user_input": (
            ir.input_data if isinstance(ir.input_data, str) else json.dumps(ir.input_data)
        ),
        "output": output_str,
        "timing": {
            "total_ms": ir.latency_ms,
            "first_token_ms": 0,
            "llm_ms": 0,
            "tool_ms": 0,
            "retrieval_ms": 0,
            "overhead_ms": 0,
        },
        "tokens": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
            "cost_usd": 0.0,
            "model": "",
        },
        "tool_calls": [],
        "contexts": [],
        "errors": (
            [{"error_type": "AgentError", "message": ir.error, "recoverable": False}]
            if ir.error
            else []
        ),
        "custom_metadata": {},
        "tags": [],
    }


def _run_graders(graders: list[Any], transcript: dict[str, Any]) -> list[Score]:
    """Run all graders against a transcript, logging failures."""
    scores: list[Score] = []
    for grader in graders:
        try:
            score = grader.grade(transcript)
            scores.append(score)
        except Exception:
            logger.exception("Grader %s failed during experiment", getattr(grader, "name", grader))
    return scores


class ExperimentRunner:
    """Runs an agent function against every item in a Langfuse dataset.

    When ``graders`` are provided, each item's output is scored after execution
    and the scores are attached to ``ItemResult.scores`` and aggregated into
    ``ExperimentResult.aggregated_scores``.
    """

    def __init__(
        self,
        agent_fn: AgentFn,
        *,
        concurrency: int = 5,
        timeout_s: float = 60.0,
        graders: list[Any] | None = None,
    ):
        self._agent_fn = agent_fn
        self._concurrency = concurrency
        self._timeout = timeout_s
        self._graders = graders or []

    async def run_experiment(
        self,
        dataset_name: str,
        experiment_name: str = "",
        agent_version: str = "",
    ) -> ExperimentResult:
        from novaos_client_sdk.evaluation.langfuse_service import LangfuseService

        service = LangfuseService.get_instance()
        if service is None:
            raise RuntimeError("Langfuse not configured -- cannot run dataset experiment")

        dataset = service.get_dataset(dataset_name)
        if dataset is None:
            raise ValueError(f"Dataset '{dataset_name}' not found")

        items = dataset.items
        result = ExperimentResult(
            experiment_name=experiment_name or f"{dataset_name}-{agent_version}",
            total_items=len(items),
        )

        sem = asyncio.Semaphore(self._concurrency)

        async def run_item(item: Any) -> ItemResult:
            async with sem:
                start = time.perf_counter()
                try:
                    output = await asyncio.wait_for(
                        self._agent_fn(
                            item.input if isinstance(item.input, str) else json.dumps(item.input)
                        ),
                        timeout=self._timeout,
                    )
                    elapsed = (time.perf_counter() - start) * 1000
                    ir = ItemResult(
                        item_id=getattr(item, "id", ""),
                        input_data=item.input,
                        expected_output=getattr(item, "expected_output", None),
                        actual_output=output,
                        latency_ms=elapsed,
                    )
                except Exception as exc:
                    elapsed = (time.perf_counter() - start) * 1000
                    ir = ItemResult(
                        item_id=getattr(item, "id", ""),
                        input_data=item.input,
                        success=False,
                        error=str(exc),
                        latency_ms=elapsed,
                    )

                if self._graders:
                    transcript = _build_item_transcript(ir)
                    ir.scores = _run_graders(self._graders, transcript)

                return ir

        tasks = [run_item(item) for item in items]
        result.item_results = await asyncio.gather(*tasks)
        result.success_count = sum(1 for ir in result.item_results if ir.success)
        result.failure_count = len(result.item_results) - result.success_count
        result.total_latency_ms = sum(ir.latency_ms for ir in result.item_results)
        result.aggregate()
        return result


class LocalExperimentRunner:
    """Runs an agent function against local JSON test cases (no Langfuse required).

    When ``graders`` are provided, each item's output is scored after execution.
    """

    def __init__(
        self,
        agent_fn: AgentFn,
        *,
        concurrency: int = 5,
        timeout_s: float = 60.0,
        graders: list[Any] | None = None,
    ):
        self._agent_fn = agent_fn
        self._concurrency = concurrency
        self._timeout = timeout_s
        self._graders = graders or []

    async def run_experiment(
        self,
        test_cases: list[dict[str, Any]],
        experiment_name: str = "local",
    ) -> ExperimentResult:
        result = ExperimentResult(
            experiment_name=experiment_name,
            total_items=len(test_cases),
        )

        sem = asyncio.Semaphore(self._concurrency)

        async def run_case(case: dict[str, Any]) -> ItemResult:
            async with sem:
                input_data = case.get("input", "")
                input_str = input_data if isinstance(input_data, str) else json.dumps(input_data)
                start = time.perf_counter()
                try:
                    output = await asyncio.wait_for(
                        self._agent_fn(input_str),
                        timeout=self._timeout,
                    )
                    elapsed = (time.perf_counter() - start) * 1000
                    ir = ItemResult(
                        item_id=case.get("id", ""),
                        input_data=input_data,
                        expected_output=case.get("expected_output"),
                        actual_output=output,
                        latency_ms=elapsed,
                    )
                except Exception as exc:
                    elapsed = (time.perf_counter() - start) * 1000
                    ir = ItemResult(
                        item_id=case.get("id", ""),
                        input_data=input_data,
                        success=False,
                        error=str(exc),
                        latency_ms=elapsed,
                    )

                if self._graders:
                    transcript = _build_item_transcript(ir)
                    ir.scores = _run_graders(self._graders, transcript)

                return ir

        tasks = [run_case(case) for case in test_cases]
        result.item_results = await asyncio.gather(*tasks)
        result.success_count = sum(1 for ir in result.item_results if ir.success)
        result.failure_count = len(result.item_results) - result.success_count
        result.total_latency_ms = sum(ir.latency_ms for ir in result.item_results)
        result.aggregate()
        return result

    @classmethod
    def load_test_cases(cls, path: str | Path) -> list[dict[str, Any]]:
        """Load test cases from a JSON file."""
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "cases" in data:
            return data["cases"]
        raise ValueError(f"Invalid test cases format in {path}")


class DatasetManager:
    """CRUD wrapper for Langfuse datasets."""

    def __init__(self) -> None:
        from novaos_client_sdk.evaluation.langfuse_service import LangfuseService

        self._service = LangfuseService.get_instance()
        if self._service is None:
            raise RuntimeError("Langfuse not configured -- DatasetManager requires Langfuse")

    def create_dataset(self, name: str, description: str = "") -> Any:
        return self._service.create_dataset(name, description)

    def get_dataset(self, name: str) -> Any:
        return self._service.get_dataset(name)

    def add_item(
        self,
        dataset_name: str,
        input_data: Any,
        expected_output: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self._service.add_dataset_item(dataset_name, input_data, expected_output, metadata)

    def add_items_batch(
        self,
        dataset_name: str,
        items: list[dict[str, Any]],
    ) -> list[Any]:
        results = []
        for item in items:
            r = self.add_item(
                dataset_name,
                input_data=item.get("input"),
                expected_output=item.get("expected_output"),
                metadata=item.get("metadata"),
            )
            results.append(r)
        return results

    def export_dataset(self, dataset_name: str, output_path: str | Path) -> None:
        dataset = self.get_dataset(dataset_name)
        if dataset is None:
            raise ValueError(f"Dataset '{dataset_name}' not found")

        items_data = []
        for item in dataset.items:
            items_data.append(
                {
                    "id": getattr(item, "id", ""),
                    "input": item.input,
                    "expected_output": getattr(item, "expected_output", None),
                    "metadata": getattr(item, "metadata", {}),
                }
            )

        with open(output_path, "w") as f:
            json.dump({"dataset_name": dataset_name, "cases": items_data}, f, indent=2)

    def import_dataset(self, path: str | Path, dataset_name: str | None = None) -> str:
        with open(path) as f:
            data = json.load(f)

        name = dataset_name or data.get("dataset_name", "imported")
        self.create_dataset(name)
        cases = data.get("cases", data if isinstance(data, list) else [])
        self.add_items_batch(name, cases)
        return name

    def clone_dataset(
        self,
        source_name: str,
        target_name: str | None = None,
    ) -> str:
        """Copy all items from *source_name* into a new dataset.

        Unlike ``LangfuseService.graduate_to_regression`` this does **not**
        filter by score -- it blindly copies every item.  Use this for creating
        snapshots or seeding a regression suite that you will curate manually.
        """
        target = target_name or f"{source_name}-regression"
        source = self.get_dataset(source_name)
        if source is None:
            raise ValueError(f"Source dataset '{source_name}' not found")

        self.create_dataset(target, description=f"Clone of {source_name}")
        for item in source.items:
            self.add_item(
                target,
                input_data=item.input,
                expected_output=getattr(item, "expected_output", None),
                metadata=getattr(item, "metadata", {}),
            )
        return target
