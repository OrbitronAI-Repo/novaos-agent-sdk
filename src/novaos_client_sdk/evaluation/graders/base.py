"""Abstract base class for evaluation graders."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from novaos_client_sdk.evaluation.models import Score


class Grader(ABC):
    """Base class for all evaluation graders.

    Subclasses implement ``grade()`` which receives a transcript dict
    (from ``EvaluationContext.to_transcript()``) and returns a ``Score``.
    """

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def grade(self, transcript: dict[str, Any]) -> Score:
        """Evaluate the transcript and return a Score."""
        ...
