"""Analyze one ACP agent tree and build its migration :class:`~.model.Plan`.

This is the part of the migration analyzer that turns the per-file findings
from :mod:`~novaos_client_sdk.migrate.surfaces` into one coherent picture of
an agent: which of the three ACP shapes its ``server.py`` implements, which
of its surfaces are just mechanical rewrite targets versus genuine blockers,
and whether the codemod can run on it unattended at all.

**Reachability, not just presence.** A file can import ``acp_sdk`` and still
be entirely harmless — if nothing else in the agent ever imports *that* file,
removing ``acp_sdk`` from the vendored SDK's dependencies breaks nothing,
because the file never loads. :func:`_build_reachability` walks every local
import in the agent tree (including one buried inside a function body — a
lazy import still crashes the first time that code path actually runs) and
answers, for each ``acp_sdk``-tainted file, whether ``server.py`` or any test
module ever reaches it. Reachable → :class:`~.model.Blocker`; unreachable →
:class:`~.model.DeadModule`, safe for the codemod to delete outright.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

from .model import (
    SCHEMA_VERSION,
    Blocker,
    CodemodPlan,
    DeadModule,
    ManifestFacts,
    Plan,
    Shape,
    Surface,
    TestInventory,
)
from .surfaces import FileFindings, analyze_file

#: Directory names, anywhere under an agent root, whose Python files this
#: analyzer never parses. ``__pycache__`` is compiled artifacts; a
#: fleet root's own ``commons``/``sdk``/``test_kit`` siblings are not agents
#: themselves (see :func:`iter_fleet_agents`) and are skipped there before
#: this module ever sees them, but the guard is repeated here too in case a
#: single-agent run is pointed at a directory that happens to vendor a copy.
_SKIP_DIR_NAMES = frozenset({"__pycache__", ".git", ".venv", "node_modules"})

_REFUSAL_STREAMING_ROUTE = (
    "Shape C: /runs streams Server-Sent Events directly from the route "
    "handler. Which frame to yield next, and when to fall back to a plain "
    "JSON error, is control flow the codemod cannot rewrite mechanically — "
    "this agent needs a manual pass."
)
_REFUSAL_UNKNOWN_SHAPE = (
    "Could not classify this agent's /runs shape from static analysis — "
    "manual triage needed before the codemod can run."
)


class AnalysisError(Exception):
    """This path is not an analyzable ACP agent tree, or a file in it could
    not be parsed. Callers map this to exit code 2."""


def analyze_agent(agent_path: Path) -> Plan:
    agent_path = agent_path.resolve()
    manifest_path = agent_path / "manifest.json"
    if not manifest_path.is_file():
        raise AnalysisError(f"{agent_path}: no manifest.json — not an agent directory")

    py_files = _collect_py_files(agent_path)
    if not py_files:
        raise AnalysisError(f"{agent_path}: no Python files found")

    modules: dict[str, Path] = {}
    is_package: dict[str, bool] = {}
    parsed: dict[str, tuple[ast.Module, str]] = {}  # module -> (tree, rel_path)

    for path in py_files:
        rel = path.relative_to(agent_path).as_posix()
        mod_name = _module_name(path, agent_path)
        source = path.read_text(encoding="utf-8", errors="replace")
        try:
            tree = ast.parse(source, filename=rel)
        except SyntaxError as exc:
            raise AnalysisError(f"{rel}: could not parse ({exc})") from exc
        modules[mod_name] = path
        is_package[mod_name] = path.name == "__init__.py"
        parsed[mod_name] = (tree, rel)

    all_surfaces: list[Surface] = []
    all_blockers: list[Blocker] = []
    edges: dict[str, set[str]] = {name: set() for name in modules}
    acp_sdk_modules: dict[str, int] = {}  # module -> earliest line with acp_sdk import

    for mod_name, (tree, rel) in sorted(parsed.items()):
        findings: FileFindings = analyze_file(tree, rel_path=rel)
        all_surfaces.extend(findings.surfaces)
        all_blockers.extend(findings.blockers)
        for surface in findings.surfaces:
            if surface.kind == "acp_sdk_import":
                acp_sdk_modules[mod_name] = min(
                    acp_sdk_modules.get(mod_name, surface.line), surface.line
                )
        for raw_name, _lineno in findings.imports:
            resolved = _resolve_import(raw_name, mod_name, is_package)
            if resolved in modules:
                edges[mod_name].add(resolved)

    reachable = _reachable_from_roots(edges, _entry_roots(modules))

    dead_code: list[DeadModule] = []
    for mod_name, line in sorted(acp_sdk_modules.items(), key=lambda kv: modules[kv[0]].as_posix()):
        rel = modules[mod_name].relative_to(agent_path).as_posix()
        if mod_name in reachable:
            all_blockers.append(
                Blocker(
                    kind="acp_sdk_import",
                    file=rel,
                    line=line,
                    value="acp_sdk",
                    effect=(
                        "acp_sdk left the SDK's dependencies; this module is reachable "
                        "from server.py or a test and fails at import"
                    ),
                )
            )
        else:
            dead_code.append(
                DeadModule(
                    file=rel,
                    reason="imports acp_sdk but is never imported by server.py or any test",
                )
            )

    shape, shape_reason, mixed = _classify_shape(all_surfaces)
    manifest = _read_manifest_facts(agent_path)
    tests = _build_test_inventory(agent_path, py_files)
    codemod = _plan_codemod(shape, all_surfaces, tests)
    manual = _build_manual_notes(shape, mixed, dead_code, all_blockers)

    all_surfaces.sort(key=lambda s: (s.file, s.line, s.kind))
    all_blockers.sort(key=lambda b: (b.file, b.line, b.kind))
    dead_code.sort(key=lambda d: d.file)

    return Plan(
        schema_version=SCHEMA_VERSION,
        agent=agent_path.name,
        agent_path=str(agent_path),
        manifest=manifest,
        shape=shape,
        shape_reason=shape_reason,
        mixed=mixed,
        surfaces=all_surfaces,
        blockers=all_blockers,
        dead_code=dead_code,
        tests=tests,
        codemod=codemod,
        manual=manual,
        verify_fixtures=tests.fixture_files,
    )


def _collect_py_files(agent_path: Path) -> list[Path]:
    out: list[Path] = []
    for path in agent_path.rglob("*.py"):
        if any(part in _SKIP_DIR_NAMES for part in path.relative_to(agent_path).parts):
            continue
        out.append(path)
    return sorted(out)


def _module_name(path: Path, root: Path) -> str:
    rel = path.relative_to(root)
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else rel.stem


def _resolve_import(raw_name: str, current_module: str, is_package: dict[str, bool]) -> str:
    """Resolve one ``import``/``from ... import`` target to the local dotted
    module name it would bind to, or ``""`` if it does not resolve to
    anything a plain string match on ``modules`` would find (an installed
    package, e.g. ``fastapi`` or ``novaos_client_sdk``) — those are fine to
    drop, since they can never be the *source* of a local ``acp_sdk`` taint
    this walk cares about.
    """
    if not raw_name.startswith("."):
        return raw_name

    level = len(raw_name) - len(raw_name.lstrip("."))
    remainder = raw_name[level:]
    cur_parts = current_module.split(".")
    base_parts = cur_parts if is_package.get(current_module) else cur_parts[:-1]
    trim = level - 1
    target_parts = base_parts[: len(base_parts) - trim] if trim else base_parts
    if remainder:
        target_parts = target_parts + remainder.split(".")
    return ".".join(p for p in target_parts if p)


def _entry_roots(modules: dict[str, Path]) -> list[str]:
    roots = [name for name in modules if name == "server"]
    roots += sorted(name for name in modules if name == "tests" or name.startswith("tests."))
    return roots


def _reachable_from_roots(edges: dict[str, set[str]], roots: list[str]) -> set[str]:
    seen: set[str] = set()
    stack = [r for r in roots if r in edges]
    seen.update(stack)
    while stack:
        current = stack.pop()
        for neighbor in edges.get(current, ()):
            if neighbor not in seen:
                seen.add(neighbor)
                stack.append(neighbor)
    return seen


def _classify_shape(surfaces: list[Surface]) -> tuple[Shape, str, bool]:
    kinds = {s.kind for s in surfaces}
    has_streaming = "streaming_response" in kinds or "acp_frame_builder_call" in kinds
    has_b_markers = bool(
        kinds
        & {
            "normalize_acp_body_call",
            "ensure_acp_output_call",
            "routing_meta_call",
            "async_job_manager_construct",
        }
    )
    has_handle_acp_request = "handle_acp_request_call" in kinds

    if has_streaming:
        mixed = has_b_markers or has_handle_acp_request
        reason = "/runs route returns StreamingResponse or calls an ACP SSE frame builder"
        if mixed:
            reason += "; also has a non-streaming AgentBuilder shell branch (mixed)"
        return Shape.C, reason, mixed

    if has_b_markers:
        return (
            Shape.B,
            "imports the ACP routing/response helpers (normalize_acp_body, ensure_acp_output, routing metadata, or AsyncJobManager)",
            False,
        )

    if has_handle_acp_request:
        return Shape.A, "/runs route delegates directly to AgentBuilder.handle_acp_request", False

    return (
        Shape.UNKNOWN,
        "no /runs route calling handle_acp_request, no B-shape helpers, no streaming route found",
        False,
    )


def _read_manifest_facts(agent_path: Path) -> ManifestFacts:
    manifest_path = agent_path / "manifest.json"
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raw = {}

    skills = (
        [s.get("name", "") for s in raw.get("skills", []) if isinstance(s, dict)]
        if isinstance(raw.get("skills"), list)
        else []
    )

    sdk_path, exposed_port = _read_dockerfile_facts(agent_path)
    vendored_version = _read_vendored_sdk_version(agent_path, sdk_path)

    return ManifestFacts(
        schema_version=str(raw.get("schema_version", "")),
        protocol=str(raw.get("protocol", "")),
        skills=skills,
        sdk_path=sdk_path,
        vendored_sdk_version=vendored_version,
        exposed_port=exposed_port,
    )


def _read_dockerfile_facts(agent_path: Path) -> tuple[str, str]:
    dockerfile = agent_path / "Dockerfile"
    if not dockerfile.is_file():
        return "", ""
    text = dockerfile.read_text(encoding="utf-8", errors="replace")

    sdk_path = ""
    arg_match = re.search(r"^ARG\s+SDK_PATH=(\S+)", text, re.MULTILINE)
    if arg_match:
        sdk_path = arg_match.group(1)

    exposed_port = ""
    expose_match = re.search(r"^EXPOSE\s+(\d+)", text, re.MULTILINE)
    if expose_match:
        exposed_port = expose_match.group(1)

    return sdk_path, exposed_port


def _read_vendored_sdk_version(agent_path: Path, sdk_path: str) -> str:
    if not sdk_path:
        return ""
    pyproject = agent_path.parent / sdk_path / "pyproject.toml"
    if not pyproject.is_file():
        return ""
    text = pyproject.read_text(encoding="utf-8", errors="replace")
    match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
    return match.group(1) if match else ""


def _build_test_inventory(agent_path: Path, py_files: list[Path]) -> TestInventory:
    test_files = [
        p
        for p in py_files
        if "tests" in p.relative_to(agent_path).parts and p.name.startswith("test_")
    ]

    runs_route_files: list[str] = []
    make_ctx_files: list[str] = []
    routing_inference_files: list[str] = []

    for path in test_files:
        rel = path.relative_to(agent_path).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        if "TestClient" in text or '"/runs"' in text or "'/runs'" in text:
            runs_route_files.append(rel)
        if "make_ctx" in text:
            make_ctx_files.append(rel)
        if "acp_request" in path.name or "normalize_acp_body" in text or "pop_routing_meta" in text:
            routing_inference_files.append(rel)

    fixture_files = sorted(
        p.relative_to(agent_path).as_posix()
        for p in (agent_path / "tests" / "test-data").glob("*")
        if (agent_path / "tests" / "test-data").is_dir() and p.is_file()
    )

    return TestInventory(
        total_files=len(test_files),
        runs_route_files=sorted(runs_route_files),
        make_ctx_files=sorted(make_ctx_files),
        routing_inference_files=sorted(routing_inference_files),
        fixture_files=fixture_files,
    )


def _plan_codemod(shape: Shape, surfaces: list[Surface], tests: TestInventory) -> CodemodPlan:
    if shape is Shape.C:
        return CodemodPlan(eligible=False, refusal_reason=_REFUSAL_STREAMING_ROUTE)
    if shape is Shape.UNKNOWN:
        return CodemodPlan(eligible=False, refusal_reason=_REFUSAL_UNKNOWN_SHAPE)

    kinds = {s.kind for s in surfaces}
    actions = ["route_line"]
    if "ensure_acp_output_call" in kinds:
        actions.append("drop_ensure_acp_output")
    if kinds & {"normalize_acp_body_call", "routing_meta_call"}:
        actions.append("replace_acp_request_with_commons_router")
    if tests.routing_inference_files:
        actions.append("rewrite_routing_tests")
    if "from_acp_request_call" in kinds:
        actions.append("replace_from_acp_request")

    return CodemodPlan(eligible=True, actions=actions)


def _build_manual_notes(
    shape: Shape,
    mixed: bool,
    dead_code: list[DeadModule],
    blockers: list[Blocker],
) -> list[str]:
    notes: list[str] = []
    if mixed:
        notes.append(
            "Shape C classification comes from a streaming branch alongside a "
            "non-streaming AgentBuilder shell — review both branches separately."
        )
    if dead_code:
        notes.append(
            f"{len(dead_code)} module(s) import acp_sdk but are unreachable — safe to "
            "delete outright rather than migrate."
        )
    if any(b.kind == "unknown_skill_status" for b in blockers):
        notes.append(
            "Out-of-vocabulary SkillResult.status values found — blocked on a "
            "status-vocabulary fix before these can be trusted to map correctly."
        )
    if any(b.kind == "acp_sdk_import" for b in blockers):
        notes.append(
            "acp_sdk-importing module(s) are actually reachable — decide whether to "
            "port or delete the feature they back before removing the dependency."
        )
    if any(b.kind == "from_acp_request_call" for b in blockers):
        notes.append(
            "AgentExecutionContext.from_acp_request no longer exists on the migrated "
            "SDK — replace with from_a2a_request at each call site listed above."
        )
    return notes
