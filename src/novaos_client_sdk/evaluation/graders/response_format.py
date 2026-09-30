"""Grader that validates agent output conforms to an expected structure."""

from __future__ import annotations

import json
from typing import Any

from novaos_client_sdk.evaluation.graders.base import Grader
from novaos_client_sdk.evaluation.models import Score


class ResponseFormatGrader(Grader):
    """Validates that agent output is well-formed JSON with expected shape.

    Useful for agents that return structured ACP responses, tool-call results,
    or any JSON contract.  Scoring:

    * JSON parse check (always): 1.0 if valid, 0.0 if not.
    * Required-keys check (when ``required_keys`` given): fraction of keys present.
    * JSON-schema check (when ``json_schema`` given, needs ``jsonschema`` package):
      1.0 if valid, 0.0 on violation. Delegates to
      :func:`novaos_client_sdk.schema_validation.validate_against_schema`, which
      blocks remote ``$ref`` resolution — a caller-supplied ``json_schema``
      cannot be turned into an SSRF primitive via an embedded ``$ref``.

    Final score is the mean of all applicable checks.
    """

    def __init__(
        self,
        required_keys: list[str] | None = None,
        json_schema: dict[str, Any] | None = None,
    ):
        self._required_keys = required_keys or []
        self._json_schema = json_schema

    def grade(self, transcript: dict[str, Any]) -> Score:
        output = transcript.get("output", "")
        if not isinstance(output, str):
            output = json.dumps(output) if output is not None else ""

        if not output or not output.strip():
            return Score(name="response_format", value=0.0, comment="Empty output")

        try:
            parsed = json.loads(output)
        except (json.JSONDecodeError, TypeError):
            return Score(
                name="response_format",
                value=0.0,
                comment="Output is not valid JSON",
            )

        checks: list[float] = [1.0]
        comments: list[str] = ["valid JSON"]

        if self._required_keys:
            if isinstance(parsed, dict):
                present = sum(1 for k in self._required_keys if k in parsed)
                ratio = present / len(self._required_keys)
                checks.append(ratio)
                missing = [k for k in self._required_keys if k not in parsed]
                if missing:
                    comments.append(f"missing keys: {missing}")
                else:
                    comments.append("all required keys present")
            else:
                checks.append(0.0)
                comments.append("output is not a JSON object; cannot check keys")

        if self._json_schema is not None:
            try:
                from novaos_client_sdk.schema_validation import validate_against_schema

                errors = validate_against_schema(parsed, self._json_schema)
            except ImportError:
                comments.append("jsonschema not installed; schema check skipped")
            else:
                if errors:
                    checks.append(0.0)
                    comments.append(f"schema violation: {'; '.join(errors)}")
                else:
                    checks.append(1.0)
                    comments.append("schema valid")

        value = sum(checks) / len(checks)
        return Score(
            name="response_format",
            value=round(value, 4),
            comment="; ".join(comments),
        )
