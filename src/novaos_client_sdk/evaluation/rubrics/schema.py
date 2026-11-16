"""Rubric file format: YAML front matter plus a numbered body.

The schema has no field for the judge's output format; the judge template owns
that, and ``additionalProperties: false`` keeps a rubric from adding one.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from importlib.resources import files
from typing import Any

import yaml

MAX_BODY_CHARS = 8000
MAX_CRITERIA = 20
MAX_LABELLED_CASES = 200
MAX_CASE_FIELD_CHARS = 2000
MAX_DOCUMENT_CHARS = 100_000

_FRONT_MATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---\r?\n(.*)\Z", re.DOTALL)
_NUMBERED_LINE = re.compile(r"^\s*\d+[.)]\s+\S", re.MULTILINE)


class RubricValidationError(ValueError):
    """The rubric file is malformed, oversized, or breaks the schema."""


def rubric_json_schema() -> dict[str, Any]:
    """JSON Schema for a rubric file (front matter fields plus ``body``).

    The file is a byte-identical copy of the schema the platform publishes.
    """
    return json.loads(files(__package__).joinpath("rubric.schema.json").read_text(encoding="utf-8"))


@dataclass(frozen=True)
class Criterion:
    id: str
    text: str
    penalty: float | None = None


@dataclass(frozen=True)
class Rubric:
    name: str
    version: int
    description: str
    owner: str
    criteria: tuple[Criterion, ...]
    labelled_cases: tuple[dict[str, Any], ...]
    body: str
    scale: dict[str, float] | None = None
    threshold: float | None = None
    applies_to: tuple[str, ...] = ()


def parse_rubric(text: str) -> Rubric:
    """Parse and validate a rubric file; raise :class:`RubricValidationError` if invalid."""
    import jsonschema

    if len(text) > MAX_DOCUMENT_CHARS:
        raise RubricValidationError(f"file exceeds {MAX_DOCUMENT_CHARS} characters")
    match = _FRONT_MATTER.match(text)
    if not match:
        raise RubricValidationError("rubric must start with YAML front matter delimited by ---")
    raw_front, body = match.groups()
    body = body.replace("\r\n", "\n").strip()
    try:
        front = yaml.safe_load(raw_front)
    except yaml.YAMLError as exc:
        raise RubricValidationError(f"front matter is not valid YAML: {exc}") from exc
    try:
        if not isinstance(front, dict):
            raise RubricValidationError("front matter must be a mapping")
        jsonschema.validate({**front, "body": body}, rubric_json_schema())
    except jsonschema.ValidationError as exc:
        raise RubricValidationError(exc.message) from exc

    ids = [c["id"] for c in front["criteria"]]
    if len(set(ids)) != len(ids):
        raise RubricValidationError("criterion ids must be unique")

    return Rubric(
        name=front["name"],
        version=front["version"],
        description=front["description"],
        owner=front["owner"],
        criteria=tuple(Criterion(c["id"], c["text"], c.get("penalty")) for c in front["criteria"]),
        labelled_cases=tuple(front["labelled_cases"]),
        body=body,
        scale=front.get("scale"),
        threshold=front.get("threshold"),
        applies_to=tuple(front.get("applies_to", ())),
    )
