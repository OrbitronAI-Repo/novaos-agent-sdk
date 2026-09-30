"""Data model for the ACP→A2A migration analyzer.

:mod:`novaos_client_sdk.migrate.analyze` walks a single ACP agent tree — one
built against the pre-migration SDK, still shaped like the Galaxy scaffold's
``server.py`` + ``AgentBuilder`` + ``skills/<skill>/handler.py`` template —
and emits a :class:`Plan`. Every later migration stage (the codemod,
verification, and the docs guide) consumes that ``Plan``
rather than re-deriving it, so its shape here *is* the contract between
stages. Bump :data:`SCHEMA_VERSION` on any incompatible change to these
dataclasses' JSON shape.

Nothing in this module imports or executes the agent under analysis — every
:class:`Plan` is built by :mod:`~novaos_client_sdk.migrate.surfaces` walking
an :mod:`ast` tree, which is what makes running this analyzer safe against an
arbitrary, untrusted agent checkout.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

#: Bumped whenever the JSON shape a :class:`Plan` serializes to changes in a
#: way a consumer (the codemod, the docs generator) would need to
#: know about. Additive, backwards-compatible fields do not need a bump;
#: renaming or removing a field does.
SCHEMA_VERSION = 1


class Shape(StrEnum):
    """Which of the three ACP shells a ``server.py`` implements.

    Ordered ``A < B < C`` for :func:`Shape.highest` — a mixed agent (one
    ``/runs`` route with both an eager :class:`AgentBuilder` branch and a
    ``StreamingResponse`` branch, e.g. ``dynamic-screening-agent``) reports
    the higher of the two, because that is the branch the codemod cannot
    mechanically rewrite.
    """

    A = "A"
    B = "B"
    C = "C"
    UNKNOWN = "unknown"

    @property
    def _rank(self) -> int:
        return {"A": 0, "B": 1, "C": 2, "unknown": -1}[self.value]

    @staticmethod
    def highest(shapes: list[Shape]) -> Shape:
        known = [s for s in shapes if s is not Shape.UNKNOWN]
        if not known:
            return Shape.UNKNOWN
        return max(known, key=lambda s: s._rank)


@dataclass(frozen=True)
class Surface:
    """One ACP-shaped construct found in the agent's source.

    A surface is *not* automatically a problem — most are exactly what the
    codemod is going to mechanically rewrite. ``kind`` is one of the
    constants in :mod:`novaos_client_sdk.migrate.surfaces` (e.g.
    ``"runs_route"``, ``"handle_acp_request_call"``,
    ``"async_job_manager_construct"``); see that module for the full list and
    what each one means.
    """

    kind: str
    file: str
    line: int
    detail: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "kind": self.kind,
            "file": self.file,
            "line": self.line,
            "detail": self.detail,
        }
        if self.extra:
            d["extra"] = self.extra
        return d


@dataclass(frozen=True)
class Blocker:
    """Something that will crash or silently misbehave after a mechanical port.

    Unlike a :class:`Surface`, a blocker cannot be left for the codemod — it
    needs a human decision (a manual codemod step) or a prerequisite fix landing
    first (for ``unknown_skill_status``). ``kind`` is one of:
    ``"acp_sdk_import"``, ``"unknown_skill_status"``,
    ``"from_acp_request_call"``, ``"unmovable_runs_logic"``.
    """

    kind: str
    file: str
    line: int
    value: str = ""
    effect: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "file": self.file,
            "line": self.line,
            "value": self.value,
            "effect": self.effect,
        }


@dataclass(frozen=True)
class DeadModule:
    """A local module that imports something migration-breaking but is never
    actually loaded — found unreachable from ``server.py`` or any test file in
    the same reachability walk that finds :class:`Blocker` ``acp_sdk_import``
    sites. Safe to delete; not a blocker because nothing exercises it."""

    file: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"file": self.file, "reason": self.reason}


@dataclass(frozen=True)
class TestInventory:
    """Which of the agent's own tests touch ACP-shaped surfaces and will need
    updating alongside the codemod or a fresh assertion pass."""

    total_files: int = 0
    runs_route_files: list[str] = field(default_factory=list)
    make_ctx_files: list[str] = field(default_factory=list)
    routing_inference_files: list[str] = field(default_factory=list)
    fixture_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_files": self.total_files,
            "runs_route_files": self.runs_route_files,
            "make_ctx_files": self.make_ctx_files,
            "routing_inference_files": self.routing_inference_files,
            "fixture_files": self.fixture_files,
        }


@dataclass(frozen=True)
class CodemodPlan:
    """Whether the codemod can run on this agent unattended, and what it
    would do if so. Shape C is always refused: a streaming ``/runs`` route's
    control flow (which frame to yield next, when to fall back to a plain
    JSON error) is not a mechanical rewrite — see
    :data:`~novaos_client_sdk.migrate.analyze.REFUSAL_STREAMING_ROUTE`."""

    eligible: bool
    actions: list[str] = field(default_factory=list)
    refusal_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"eligible": self.eligible}
        if self.eligible:
            d["actions"] = self.actions
        else:
            d["refusal_reason"] = self.refusal_reason
        return d


@dataclass(frozen=True)
class ManifestFacts:
    """Facts read from ``manifest.json`` and the ``Dockerfile`` — not from any
    Python source, so unaffected by the AST walk that produces the rest of a
    :class:`Plan`."""

    schema_version: str = ""
    protocol: str = ""
    skills: list[str] = field(default_factory=list)
    sdk_path: str = ""
    vendored_sdk_version: str = ""
    exposed_port: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "protocol": self.protocol,
            "skills": self.skills,
            "sdk_path": self.sdk_path,
            "vendored_sdk_version": self.vendored_sdk_version,
            "exposed_port": self.exposed_port,
        }


@dataclass
class Plan:
    """The full migration plan for one agent — what
    ``python -m novaos_client_sdk.migrate analyze`` writes as
    ``<out>/<agent>.plan.json``, and what every later migration stage reads
    instead of re-deriving these facts itself.
    """

    schema_version: int
    agent: str
    agent_path: str
    manifest: ManifestFacts
    shape: Shape
    shape_reason: str
    mixed: bool
    surfaces: list[Surface] = field(default_factory=list)
    blockers: list[Blocker] = field(default_factory=list)
    dead_code: list[DeadModule] = field(default_factory=list)
    tests: TestInventory = field(default_factory=TestInventory)
    codemod: CodemodPlan = field(default_factory=lambda: CodemodPlan(eligible=False))
    manual: list[str] = field(default_factory=list)
    verify_fixtures: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "agent": self.agent,
            "agent_path": self.agent_path,
            "manifest": self.manifest.to_dict(),
            "shape": self.shape.value,
            "shape_reason": self.shape_reason,
            "mixed": self.mixed,
            "surfaces": [s.to_dict() for s in self.surfaces],
            "blockers": [b.to_dict() for b in self.blockers],
            "dead_code": [d.to_dict() for d in self.dead_code],
            "tests": self.tests.to_dict(),
            "codemod": self.codemod.to_dict(),
            "manual": self.manual,
            "verify_fixtures": self.verify_fixtures,
        }

    @property
    def exit_code(self) -> int:
        return 1 if self.blockers else 0


def load_plan(plan_path: Path) -> Plan:
    """Deserialize a ``<agent>.plan.json`` (this module's ``Plan.to_dict()``
    output, ``python -m novaos_client_sdk.migrate analyze``'s ``--out``
    artifact) back into these dataclasses.

    Lives here rather than in ``transform.py`` (the codemod, which first wrote this
    function) so that ``verify`` can read a plan back without pulling
    in ``libcst`` — nothing about deserializing a plan needs a CST library,
    only rewriting an agent's source does.
    """
    raw = json.loads(plan_path.read_text(encoding="utf-8"))
    manifest = ManifestFacts(**raw["manifest"])
    tests = TestInventory(**raw["tests"])
    codemod_raw = raw["codemod"]
    codemod = CodemodPlan(
        eligible=codemod_raw["eligible"],
        actions=codemod_raw.get("actions", []),
        refusal_reason=codemod_raw.get("refusal_reason", ""),
    )
    return Plan(
        schema_version=raw["schema_version"],
        agent=raw["agent"],
        agent_path=raw["agent_path"],
        manifest=manifest,
        shape=Shape(raw["shape"]),
        shape_reason=raw["shape_reason"],
        mixed=raw["mixed"],
        surfaces=[Surface(**s) for s in raw.get("surfaces", [])],
        blockers=[Blocker(**b) for b in raw.get("blockers", [])],
        dead_code=[DeadModule(**d) for d in raw.get("dead_code", [])],
        tests=tests,
        codemod=codemod,
        manual=raw.get("manual", []),
        verify_fixtures=raw.get("verify_fixtures", []),
    )
