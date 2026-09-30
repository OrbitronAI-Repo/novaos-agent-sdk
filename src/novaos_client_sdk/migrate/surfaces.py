"""AST visitors that find ACP-shaped constructs in one Python source file.

Every function here takes already-parsed source (an :class:`ast.Module` plus
the file's text, for line lookups) and returns a list of
:class:`~novaos_client_sdk.migrate.model.Surface` /
:class:`~novaos_client_sdk.migrate.model.Blocker` records. Nothing here
imports the file it is looking at — the whole point of this package is to
classify an ACP agent without running any of its code (a live import would
drag in the vendored ``acp_sdk`` dependency, ``baml_client``, and network
clients).

**Surface kinds** (the ``kind`` a :class:`Surface` carries):

============================== ===============================================
``runs_route``                 ``@app.post("/runs")`` handler function
``handle_acp_request_call``    ``<agent>.handle_acp_request(...)``
``streaming_response``         ``StreamingResponse(...)`` construction
``normalize_acp_body_call``    call to ``normalize_acp_body``
``ensure_acp_output_call``     call to ``ensure_acp_output``
``routing_meta_call``          call to ``pop_routing_meta`` / ``attach_routing_metadata``
``acp_mcp_request_with``       ``with acp_mcp_request(...):`` — passthrough only,
                                never a shape signal (shape-A ``ai-assistant``
                                uses it too; see :mod:`.analyze`)
``async_job_manager_construct`` ``AsyncJobManager(...)`` construction
``acp_frame_builder_call``     call to a ``commons``/``services`` ACP SSE frame
                                builder (``run_created_frame`` and friends)
``fold_acp_attachments_call``  call to ``fold_acp_attachments_into_skill_input``
``acp_sdk_import``             ``import acp_sdk`` / ``from acp_sdk...`` — also
                                feeds :func:`.analyze.classify_acp_sdk_imports`
``skill_result_status``        ``SkillResult(status=<literal>)`` keyword,
                                known or not — unknown ones double as a
                                :class:`~.model.Blocker`
``from_acp_request_call``      ``<Name>.from_acp_request(...)`` — always a
                                :class:`~.model.Blocker` too; the method no
                                longer exists on the migrated SDK
============================== ===============================================
"""

from __future__ import annotations

import ast
from dataclasses import dataclass

from ..agent_types import _SKILL_STATUS_TO_TASK_STATE
from .model import Blocker, Surface

#: SkillResult.status values AgentBuilder's ``_task_state_for`` actually maps
#: (``agent_builder.py``). Derived from the same
#: ``agent_types._SKILL_STATUS_TO_TASK_STATE`` the runtime itself reads,
#: rather than a second, hand-kept copy of the vocabulary — the two
#: disagreeing was a real, observed failure mode: ``QUEUED``/``RUNNING``
#: have been observed in real agent code as statuses this analyzer flagged
#: as unknown while the runtime silently accepted them as ``COMPLETED``.
#: Anything still outside this set fails the task loudly at runtime rather
#: than silently, but it is still a :class:`~.model.Blocker` here, not a
#: surface: a status this SDK has never seen is a bug in the agent
#: regardless of how the runtime now reports it, and still needs a human
#: decision before a port.
KNOWN_SKILL_STATUSES = frozenset(_SKILL_STATUS_TO_TASK_STATE)

#: ACP SSE frame-builder functions, as defined in both
#: ``commons/acp_sse.py`` and the per-agent ``services/acp_sse.py`` copies
#: (dynamic-screening-agent). A call to any of these inside a ``/runs``
#: handler is the strongest signal that route is Shape C.
ACP_FRAME_BUILDER_NAMES = frozenset(
    {
        "format_sse",
        "run_created_frame",
        "run_in_progress_frame",
        "run_completed_frame",
        "run_failed_frame",
        "domain_failure_output",
        "step_progress_frame",
        "message_delta_frame",
        "final_content_frames",
        "build_progress_sse_frames",
    }
)

ROUTING_HELPER_NAMES = frozenset({"pop_routing_meta", "attach_routing_metadata"})


def _decorator_targets_runs_route(decorator: ast.expr) -> bool:
    """``True`` for ``@app.post("/runs")`` (any receiver name, any casing of
    the literal path — always ``"/runs"`` in this corpus, but matched exactly
    rather than by substring so ``/runs/health`` or similar never false-positives)."""
    if not isinstance(decorator, ast.Call):
        return False
    func = decorator.func
    if not (isinstance(func, ast.Attribute) and func.attr == "post"):
        return False
    if not decorator.args:
        return False
    first = decorator.args[0]
    return isinstance(first, ast.Constant) and first.value == "/runs"


@dataclass
class FileFindings:
    surfaces: list[Surface]
    blockers: list[Blocker]
    imports: list[tuple[str, int]]
    """``(dotted_module_name, lineno)`` for every ``import``/``from ... import``
    in the file, at any nesting depth — including inside a function body,
    since a lazily-imported ``acp_sdk`` still crashes the first time that
    function runs. Used by :mod:`.analyze` to build the local reachability
    graph."""


def _iter_import_module_names(node: ast.Import | ast.ImportFrom) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]

    # ImportFrom: level > 0 is a relative import (`from .foo import bar`,
    # `from . import bar`) — `module` is None for the bare `from . import x`
    # form, so the base name is the leading dots plus nothing else useful to
    # resolve here; callers ignore rows where the returned name is empty.
    prefix = "." * node.level
    base = f"{prefix}{node.module}" if node.module else prefix

    # `from X import Y` is ambiguous from the grammar alone: Y might be an
    # attribute defined in X (a name, a class, ...), or it might be a
    # submodule `X.Y` bound as a side effect of the import (the common
    # `from agents import insights_agent` shape). Emit both candidates —
    # the bare base and each `base.name` combination — and let
    # `_resolve_import` keep only whichever actually matches a real local
    # module; a candidate that resolves to nothing is simply dropped there.
    names = [base] if base else []
    joiner = "" if base.endswith(".") else "."
    for alias in node.names:
        if alias.name != "*":
            names.append(f"{base}{joiner}{alias.name}")
    return names


def analyze_file(tree: ast.Module, *, rel_path: str) -> FileFindings:
    """Walk one already-parsed file and collect every surface, blocker, and
    import edge in it. ``rel_path`` is used only to stamp ``file`` fields —
    always the path relative to the agent root, so a :class:`Plan` reads the
    same regardless of where the agent checkout lives on disk (a determinism
    requirement)."""
    surfaces: list[Surface] = []
    blockers: list[Blocker] = []
    imports: list[tuple[str, int]] = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for name in _iter_import_module_names(node):
                if name:
                    imports.append((name, node.lineno))
            _check_acp_sdk_import(node, rel_path, surfaces)

        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(_decorator_targets_runs_route(d) for d in node.decorator_list):
                surfaces.append(
                    Surface(
                        kind="runs_route",
                        file=rel_path,
                        line=node.lineno,
                        detail=node.name,
                    )
                )

        elif isinstance(node, ast.With):
            _check_acp_mcp_request_with(node, rel_path, surfaces)

        elif isinstance(node, ast.Call):
            _check_call(node, rel_path, surfaces, blockers)

    return FileFindings(surfaces=surfaces, blockers=blockers, imports=imports)


def _check_acp_sdk_import(
    node: ast.Import | ast.ImportFrom, rel_path: str, surfaces: list[Surface]
) -> None:
    if isinstance(node, ast.Import):
        hit = any(
            alias.name == "acp_sdk" or alias.name.startswith("acp_sdk.") for alias in node.names
        )
    else:
        hit = node.module is not None and (
            node.module == "acp_sdk" or node.module.startswith("acp_sdk.")
        )
    if hit:
        surfaces.append(Surface(kind="acp_sdk_import", file=rel_path, line=node.lineno))


def _check_acp_mcp_request_with(node: ast.With, rel_path: str, surfaces: list[Surface]) -> None:
    for item in node.items:
        call = item.context_expr
        if isinstance(call, ast.Call) and _callee_name(call.func) == "acp_mcp_request":
            surfaces.append(Surface(kind="acp_mcp_request_with", file=rel_path, line=node.lineno))
            return


def _callee_name(func: ast.expr) -> str | None:
    """The bare name a call targets — ``foo`` for both ``foo(...)`` and
    ``anything.foo(...)`` — since this analyzer never resolves *which* object
    ``anything`` is (that would need the import to run); a name match is the
    best a static walk can do, and every name checked here is distinctive
    enough in practice (``handle_acp_request``, ``from_acp_request``, the SSE
    frame builders) that a false positive would need a coincidental rename."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _check_call(
    node: ast.Call,
    rel_path: str,
    surfaces: list[Surface],
    blockers: list[Blocker],
) -> None:
    name = _callee_name(node.func)
    if name is None:
        return

    if name == "handle_acp_request":
        surfaces.append(Surface(kind="handle_acp_request_call", file=rel_path, line=node.lineno))
    elif name == "StreamingResponse":
        surfaces.append(Surface(kind="streaming_response", file=rel_path, line=node.lineno))
    elif name == "normalize_acp_body":
        surfaces.append(Surface(kind="normalize_acp_body_call", file=rel_path, line=node.lineno))
    elif name == "ensure_acp_output":
        surfaces.append(Surface(kind="ensure_acp_output_call", file=rel_path, line=node.lineno))
    elif name in ROUTING_HELPER_NAMES:
        surfaces.append(
            Surface(kind="routing_meta_call", file=rel_path, line=node.lineno, detail=name)
        )
    elif name == "AsyncJobManager":
        surfaces.append(
            Surface(kind="async_job_manager_construct", file=rel_path, line=node.lineno)
        )
    elif name in ACP_FRAME_BUILDER_NAMES:
        surfaces.append(
            Surface(kind="acp_frame_builder_call", file=rel_path, line=node.lineno, detail=name)
        )
    elif name == "fold_acp_attachments_into_skill_input":
        surfaces.append(Surface(kind="fold_acp_attachments_call", file=rel_path, line=node.lineno))
    elif name == "from_acp_request":
        surfaces.append(Surface(kind="from_acp_request_call", file=rel_path, line=node.lineno))
        blockers.append(
            Blocker(
                kind="from_acp_request_call",
                file=rel_path,
                line=node.lineno,
                value="AgentExecutionContext.from_acp_request",
                effect=(
                    "removed from the migrated SDK (replaced by from_a2a_request); "
                    "this call raises AttributeError at runtime"
                ),
            )
        )
    elif name == "SkillResult":
        _check_skill_result_status(node, rel_path, surfaces, blockers)


def _check_skill_result_status(
    node: ast.Call,
    rel_path: str,
    surfaces: list[Surface],
    blockers: list[Blocker],
) -> None:
    for kw in node.keywords:
        if kw.arg != "status":
            continue
        if not (isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str)):
            continue
        status = kw.value.value
        surfaces.append(
            Surface(
                kind="skill_result_status",
                file=rel_path,
                line=node.lineno,
                detail=status,
                extra={"known": status in KNOWN_SKILL_STATUSES},
            )
        )
        if status not in KNOWN_SKILL_STATUSES:
            blockers.append(
                Blocker(
                    kind="unknown_skill_status",
                    file=rel_path,
                    line=node.lineno,
                    value=status,
                    effect=(
                        "not in agent_types._SKILL_STATUS_TO_TASK_STATE; fails the task "
                        "as TaskState.FAILED regardless of the skill's actual outcome"
                    ),
                )
            )
        return
