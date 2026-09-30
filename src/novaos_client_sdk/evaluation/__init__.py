"""NovaOS Agent Evaluation Framework.

Provides runtime evaluation, grading, experiment management, CI/CD gates,
statistical tools, and human calibration for NovaOS agents.

Install with: pip install novaos-agent-sdk[evaluation]

Public symbols are lazily imported on first access to avoid pulling in the
entire evaluation dependency graph when only ``evaluation.context`` is needed
(e.g. from ``AgentBuilder``).
"""

from __future__ import annotations

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "CIReporter": ("novaos_client_sdk.evaluation.ci", "CIReporter"),
    "EvalThresholds": ("novaos_client_sdk.evaluation.ci", "EvalThresholds"),
    "EvaluationGate": ("novaos_client_sdk.evaluation.ci", "EvaluationGate"),
    "GateResult": ("novaos_client_sdk.evaluation.ci", "GateResult"),
    "EvaluationContext": ("novaos_client_sdk.evaluation.context", "EvaluationContext"),
    "NullEvaluationContext": ("novaos_client_sdk.evaluation.context", "NullEvaluationContext"),
    "get_eval_context": ("novaos_client_sdk.evaluation.context", "get_eval_context"),
    "evaluate": ("novaos_client_sdk.evaluation.decorator", "evaluate"),
    "DatasetManager": ("novaos_client_sdk.evaluation.experiments", "DatasetManager"),
    "ExperimentResult": ("novaos_client_sdk.evaluation.experiments", "ExperimentResult"),
    "ExperimentRunner": ("novaos_client_sdk.evaluation.experiments", "ExperimentRunner"),
    "ItemResult": ("novaos_client_sdk.evaluation.experiments", "ItemResult"),
    "LocalExperimentRunner": ("novaos_client_sdk.evaluation.experiments", "LocalExperimentRunner"),
    "ContextItem": ("novaos_client_sdk.evaluation.models", "ContextItem"),
    "ErrorRecord": ("novaos_client_sdk.evaluation.models", "ErrorRecord"),
    "Score": ("novaos_client_sdk.evaluation.models", "Score"),
    "ScoreDataType": ("novaos_client_sdk.evaluation.models", "ScoreDataType"),
    "TimingMetrics": ("novaos_client_sdk.evaluation.models", "TimingMetrics"),
    "TokenUsage": ("novaos_client_sdk.evaluation.models", "TokenUsage"),
    "ToolCall": ("novaos_client_sdk.evaluation.models", "ToolCall"),
    "Transcript": ("novaos_client_sdk.evaluation.models", "Transcript"),
    "ABTestAnalyzer": ("novaos_client_sdk.evaluation.statistics", "ABTestAnalyzer"),
    "ABTestResult": ("novaos_client_sdk.evaluation.statistics", "ABTestResult"),
    "MultiTrialResult": ("novaos_client_sdk.evaluation.statistics", "MultiTrialResult"),
    "MultiTrialRunner": ("novaos_client_sdk.evaluation.statistics", "MultiTrialRunner"),
    "TrialResult": ("novaos_client_sdk.evaluation.statistics", "TrialResult"),
}

__all__ = list(_LAZY_IMPORTS)


def __getattr__(name: str) -> object:
    entry = _LAZY_IMPORTS.get(name)
    if entry is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_path, attr = entry
    import importlib

    mod = importlib.import_module(module_path)
    value = getattr(mod, attr)
    globals()[name] = value
    return value
