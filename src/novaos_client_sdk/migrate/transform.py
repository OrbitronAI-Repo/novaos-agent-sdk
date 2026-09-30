"""Mechanically rewrite one ACP agent tree onto the A2A shell.

::

    python -m novaos_client_sdk.migrate transform ./my-agent \\
        --plan ./out/my-agent.plan.json [--write]

This consumes a :class:`~novaos_client_sdk.migrate.model.Plan` rather
than re-deriving anything about the agent itself — every rewrite this module
performs is one already named in ``plan.codemod.actions``. An action string
this module has no handler for, or a plan whose ``codemod.eligible`` is
``False`` (Shape C, or a shape the analyzer could not classify), is refused
rather than guessed at: :func:`transform_agent` raises :class:`TransformError`
naming the plan's own ``refusal_reason``, or the unrecognized action, instead
of attempting a heuristic beyond what the plan already decided.

**Why ``route_line`` alone accounts for most of a Shape A agent's diff.**
Under ACP, a ``/runs`` route was itself the routing logic — read the body,
call ``AgentBuilder.handle_acp_request``, shape the response. Under A2A, skill
selection is a platform-supplied fact (``message.metadata["skillId"]``, see
``AgentBuilder._resolve_skill``'s docstring) rather than something a pod
infers from body content at the HTTP layer, so the entire handler collapses
to one line: ``app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())``. Every
other action in this module is really cleanup *around* that collapse — an
import an ACP-era helper left behind, a test written against a function that
no longer exists — not a second independent rewrite of runtime behaviour.

**Shape B is different in one respect: the collapse cannot be total.** A
Shape B agent's ``skills/acp_request.py`` did skill *inference* no A2A caller
replaces yet (a customer's shared request-routing module has an
``_infer_skill`` helper of exactly this kind — a bare UUID or a
``job_run_id`` in the body means "poll", not "start"). ``replace_acp_request_
with_commons_router`` therefore does not delete that inference outright; it
relocates the calling convention to a shared ``commons.a2a_request.route(raw,
headers, agent)`` hook (built alongside this work but not by it — that
router lives in the customer's own tree, not here) and rewrites ``route_line``'s target to
call it, rather than collapsing to the bare ``asgi_endpoint()`` one-liner.

**``replace_from_acp_request`` is deliberately conservative.** ACP's
``AgentExecutionContext.from_acp_request(body, headers)`` and A2A's
``from_a2a_request(message, params, headers, *, task_id=..., ...)`` do not
share an argument shape — ``body`` is a flat ACP dict, while ``message``/
``params`` are A2A JSON-RPC objects a caller typically only has in scope
inside an A2A dispatch handler already. A syntactic rename cannot manufacture
those from an ACP-shaped ``body``. Every ``from_acp_request_call`` surface
this module finds becomes a ``manual[]`` entry naming its exact site, never
an attempted rewrite — see :func:`_handle_replace_from_acp_request`.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import libcst as cst

from .analyze import _module_name, _resolve_import
from .model import Plan
from .model import load_plan as load_plan  # noqa: F401 - re-exported; this module used to define it
from .surfaces import _iter_import_module_names

#: Action strings this module has a handler for, in the order they are
#: applied. Order matters: ``route_line`` deletes the whole ``/runs`` handler
#: body (so any action targeting what *used* to be inside it must not assume
#: that body still exists), and ``replace_acp_request_with_commons_router``
#: must run before ``rewrite_routing_tests`` picks its rewritten import path
#: up. Deliberately excludes ``remove_unreferenced_acp_module`` — that is not
#: a distinct action the plan's analyzer ever emits; it is a conservative,
#: self-verifying side effect of ``replace_acp_request_with_commons_router``
#: (see that handler) rather than a plan-level action name, because whether a
#: module has no remaining importer is a fact only knowable *after* this
#: module's own rewrite runs, not something the plan's static analysis can commit to
#: in advance.
_ACTION_ORDER = [
    "route_line",
    "drop_ensure_acp_output",
    "replace_acp_request_with_commons_router",
    "replace_from_acp_request",
    "rewrite_routing_tests",
]

_ROUTING_HELPER_NAMES = ("normalize_acp_body", "pop_routing_meta", "attach_routing_metadata")


def _without_trailing_comma(aliases: list[cst.ImportAlias]) -> list[cst.ImportAlias]:
    """Clear a stale ``Comma()`` off the new last alias after filtering a
    ``names`` list — libcst's node for a removed non-last import keeps its
    own trailing-comma token otherwise, rendering ``import Foo, `` with
    nothing after the comma."""
    if not aliases:
        return aliases
    return [*aliases[:-1], aliases[-1].with_changes(comma=cst.MaybeSentinel.DEFAULT)]


class TransformError(Exception):
    """Refused: an ineligible plan, or a ``codemod.actions`` entry this
    codemod version has no handler for. Callers map this to exit code 2."""


@dataclass
class FileChange:
    """One file this transform run would write (or, with ``removed``, delete)."""

    path: str  # POSIX-style, relative to the agent root
    before: str
    after: str
    removed: bool = False

    def unified_diff(self) -> str:
        import difflib

        before_lines = self.before.splitlines(keepends=True)
        after_lines = [] if self.removed else self.after.splitlines(keepends=True)
        return "".join(
            difflib.unified_diff(
                before_lines,
                after_lines,
                fromfile=f"a/{self.path}",
                tofile="/dev/null" if self.removed else f"b/{self.path}",
            )
        )


@dataclass
class TransformResult:
    """What :func:`transform_agent` did — or refused to do."""

    agent: str
    applied_actions: list[str] = field(default_factory=list)
    changes: list[FileChange] = field(default_factory=list)
    manual: list[str] = field(default_factory=list)
    refused: bool = False
    refusal_reason: str = ""

    @property
    def changed(self) -> bool:
        return bool(self.changes)

    def diff_text(self) -> str:
        return "".join(c.unified_diff() for c in self.changes)


def transform_agent(agent_path: Path, plan: Plan, *, write: bool) -> TransformResult:
    """Apply every action in ``plan.codemod.actions`` to the agent tree at
    ``agent_path``. Never touches the tree unless ``write=True`` — a dry run
    still computes every :class:`FileChange` (and its diff), just without
    calling :meth:`Path.write_text`/:meth:`Path.unlink`, so ``--write`` and a
    second, idempotent run against the already-written tree both go through
    exactly the same code path.
    """
    agent_path = agent_path.resolve()

    if not plan.codemod.eligible:
        return TransformResult(
            agent=plan.agent,
            manual=list(plan.manual),
            refused=True,
            refusal_reason=plan.codemod.refusal_reason,
        )

    unknown = [a for a in plan.codemod.actions if a not in _ACTION_ORDER]
    if unknown:
        raise TransformError(
            f"refused: unknown codemod action(s) {unknown!r} — no handler in this "
            "codemod version; re-run `migrate analyze` with an up-to-date SDK, or "
            "extend transform.py's _ACTION_ORDER before running --write"
        )

    ctx = _TransformContext(agent_path=agent_path, plan=plan)
    ordered = [a for a in _ACTION_ORDER if a in plan.codemod.actions]

    for action in ordered:
        _HANDLERS[action](ctx)

    if ctx.pending_module_removal is not None:
        dotted_module, keep = ctx.pending_module_removal
        _maybe_remove_unreferenced_module(ctx, dotted_module, keep=keep)

    if write:
        for change in ctx.changes:
            full = agent_path / change.path
            if change.removed:
                full.unlink(missing_ok=True)
            else:
                full.parent.mkdir(parents=True, exist_ok=True)
                full.write_text(change.after, encoding="utf-8")

    return TransformResult(
        agent=plan.agent,
        applied_actions=ordered,
        changes=ctx.changes,
        manual=ctx.manual,
    )


class _TransformContext:
    """Mutable state one :func:`transform_agent` call threads through its
    action handlers. Every handler reads/writes source through here rather
    than the filesystem directly, so a dry run and a ``--write`` run compute
    the identical set of :class:`FileChange` objects."""

    def __init__(self, *, agent_path: Path, plan: Plan) -> None:
        self.agent_path = agent_path
        self.plan = plan
        self.manual: list[str] = list(plan.manual)
        self.changes: list[FileChange] = []
        self._sources: dict[str, str] = {}
        # Set once route_line decides whether this agent's replacement needs
        # the commons router (Shape B) or the bare asgi_endpoint() one-liner
        # (Shape A) — replace_acp_request_with_commons_router reads this back
        # rather than re-deciding it, since route_line already committed to a
        # shape when it ran (it always runs first, per _ACTION_ORDER).
        self.uses_commons_router = "replace_acp_request_with_commons_router" in plan.codemod.actions
        # (dotted_module, server_rel_path) set by replace_acp_request_with_
        # commons_router — its own unreferenced-module check must run last,
        # after rewrite_routing_tests has had a chance to stop the old
        # per-agent test file from referencing that module too (see
        # transform_agent's final step).
        self.pending_module_removal: tuple[str, str] | None = None

    def read(self, rel_path: str) -> str:
        if rel_path not in self._sources:
            self._sources[rel_path] = (self.agent_path / rel_path).read_text(encoding="utf-8")
        return self._sources[rel_path]

    def exists(self, rel_path: str) -> bool:
        for change in self.changes:
            if change.path == rel_path:
                return not change.removed
        return (self.agent_path / rel_path).is_file()

    def write(self, rel_path: str, new_source: str) -> None:
        before = self.read(rel_path) if self.exists(rel_path) else ""
        self._sources[rel_path] = new_source
        for change in self.changes:
            if change.path == rel_path:
                change.after = new_source
                change.removed = False
                return
        self.changes.append(FileChange(path=rel_path, before=before, after=new_source))

    def remove(self, rel_path: str) -> None:
        before = self.read(rel_path)
        for change in self.changes:
            if change.path == rel_path:
                change.removed = True
                change.after = ""
                return
        self.changes.append(FileChange(path=rel_path, before=before, after="", removed=True))

    def current_text(self, rel_path: str) -> str | None:
        """The would-be-on-disk text of ``rel_path`` after every change
        applied so far — ``None`` if this run has deleted it."""
        for change in self.changes:
            if change.path == rel_path:
                return None if change.removed else change.after
        full = self.agent_path / rel_path
        return full.read_text(encoding="utf-8") if full.is_file() else None

    def all_tracked_files(self) -> list[str]:
        """Every ``.py`` file under the agent root, reflecting deletions this
        run has already staged — used by the reachability check
        ``replace_acp_request_with_commons_router`` runs before deleting the
        now-superseded routing module."""
        removed = {c.path for c in self.changes if c.removed}
        on_disk = {
            p.relative_to(self.agent_path).as_posix()
            for p in self.agent_path.rglob("*.py")
            if "__pycache__" not in p.parts
        }
        return sorted(on_disk - removed)


# ── route_line ───────────────────────────────────────────────────────────


def _find_server_file(ctx: _TransformContext) -> str:
    if (ctx.agent_path / "server.py").is_file():
        return "server.py"
    raise TransformError(
        f"{ctx.agent_path}: no server.py found — the plan's Plan.surfaces should always "
        "name a runs_route file if codemod.eligible is True; this is a bug in either "
        "the plan or this codemod, not something to route around"
    )


def _handle_route_line(ctx: _TransformContext) -> None:
    rel_path = _find_server_file(ctx)
    source = ctx.current_text(rel_path)
    assert source is not None
    module = cst.parse_module(source)
    transformer = _RouteLineTransformer(use_commons_router=ctx.uses_commons_router)
    new_module = module.visit(transformer)
    if not transformer.found:
        # Not an error: this is what a second run against an already-migrated
        # tree looks like — route_line already collapsed the `/runs` handler,
        # so there is nothing left matching `@app.post("/runs")` to find. A
        # stale plan pointed at the wrong agent looks identical from here;
        # both are handled the same conservative way, by doing nothing rather
        # than guessing, with a note so the mismatch is still visible.
        ctx.manual.append(
            f'{rel_path}: route_line found no `@app.post("/runs")` handler — either '
            "already migrated, or this plan does not match this agent tree; skipped"
        )
        return
    ctx.manual.extend(transformer.manual_notes)
    ctx.write(rel_path, new_module.code)


class _RouteLineTransformer(cst.CSTTransformer):
    """Replace the ``@app.post("/runs")`` handler with the A2A shell.

    Shape A (``use_commons_router=False``): the handler collapses to
    ``app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())`` — no function left
    at all, since ``AgentBuilder.asgi_endpoint()`` already owns request
    decoding, ``SendMessage``/``SendStreamingMessage`` dispatch, and SSE
    framing.

    Shape B (``use_commons_router=True``): the handler stays a function, but
    its body shrinks to reading the raw request and delegating to
    ``commons.a2a_request.route(raw, headers, agent)`` — the hook that still
    owns this agent's skill-inference rules (a customer's shared ``commons/
    a2a_request.py``, built alongside this work but not by it; see this
    module's docstring).
    """

    def __init__(self, *, use_commons_router: bool) -> None:
        self.use_commons_router = use_commons_router
        self.found = False
        self.manual_notes: list[str] = []
        self._agent_name = "agent"
        self._app_name = "app"
        self._request_param_name = "request"
        self._request_still_used = False

    def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
        if self._targets_runs_route(node):
            self._app_name = self._decorator_receiver_name(node) or "app"
            self._request_param_name = self._first_param_name(node) or "request"
            handler = self._handle_acp_request_receiver(node)
            if handler:
                self._agent_name = handler
        return True

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        if not self.found:
            return updated_node
        new_body = self._patch_import(updated_node.body)
        if not self.use_commons_router and not self._name_used_elsewhere(new_body, "Request"):
            new_body = self._drop_fastapi_request_import(new_body)
        return updated_node.with_changes(body=new_body)

    def leave_FunctionDef(
        self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef
    ) -> cst.FunctionDef | cst.BaseStatement:
        if not self._targets_runs_route(original_node):
            return updated_node
        self.found = True
        if self.use_commons_router:
            self.manual_notes.append(
                f"{self._app_name!r} route: commons.a2a_request.route(...) was generated with "
                "START_SKILL/PROGRESS_SKILL placeholders — replace both with this agent's own "
                "start/progress SKILL_* constants (and add known_skills=... if this agent's "
                "original skills/acp_request.py gated explicit skill names to an allow-list); "
                "a generic ACP-shape analysis has no way to know an agent's own skill names"
            )
            param = self._request_param_name
            return updated_node.with_changes(
                name=cst.Name("a2a_endpoint"),
                params=original_node.params,
                returns=None,
                body=cst.IndentedBlock(
                    body=[
                        cst.parse_statement(f"raw = await {param}.body()"),
                        cst.parse_statement(f"header_dict = dict({param}.headers.items())"),
                        cst.parse_statement(
                            "return await commons.a2a_request.route(\n"
                            "    raw,\n"
                            "    header_dict,\n"
                            f"    {self._agent_name},\n"
                            "    start_skill=START_SKILL,  # TODO: this agent's own start skill\n"
                            "    progress_skill=PROGRESS_SKILL,  # TODO: this agent's own progress skill\n"
                            ")\n"
                        ),
                    ]
                ),
                decorators=[
                    cst.Decorator(
                        decorator=cst.Call(
                            func=cst.Attribute(
                                value=cst.Name(self._app_name), attr=cst.Name("post")
                            ),
                            args=[cst.Arg(cst.Name("A2A_JSONRPC_PATH"))],
                        )
                    )
                ],
            )
        return cst.parse_statement(
            f"{self._app_name}.post(A2A_JSONRPC_PATH)({self._agent_name}.asgi_endpoint())\n"
        )

    def _matching_runs_decorator(self, node: cst.FunctionDef) -> cst.Call | None:
        """The specific ``@X.post("/runs")`` decorator call, if any — never
        just "the first decorator that happens to be a call", since a route
        can carry other decorators too (``@trace.span()`` above the route,
        say) whose receiver is not the one this codemod needs to rewrite."""
        for dec in node.decorators:
            call = dec.decorator
            if not isinstance(call, cst.Call):
                continue
            func = call.func
            if not (isinstance(func, cst.Attribute) and func.attr.value == "post"):
                continue
            if not call.args:
                continue
            first = call.args[0].value
            if isinstance(first, cst.SimpleString) and first.evaluated_value == "/runs":
                return call
        return None

    def _targets_runs_route(self, node: cst.FunctionDef) -> bool:
        return self._matching_runs_decorator(node) is not None

    def _decorator_receiver_name(self, node: cst.FunctionDef) -> str | None:
        call = self._matching_runs_decorator(node)
        if call is None or not isinstance(call.func, cst.Attribute):
            return None
        value = call.func.value
        return value.value if isinstance(value, cst.Name) else None

    def _first_param_name(self, node: cst.FunctionDef) -> str | None:
        params = node.params.params
        return params[0].name.value if params else None

    def _handle_acp_request_receiver(self, node: cst.FunctionDef) -> str | None:
        class _Finder(cst.CSTVisitor):
            found: str | None = None

            def visit_Call(self, inner: cst.Call) -> None:
                if (
                    isinstance(inner.func, cst.Attribute)
                    and inner.func.attr.value == "handle_acp_request"
                    and isinstance(inner.func.value, cst.Name)
                ):
                    self.found = inner.func.value.value

        finder = _Finder()
        node.body.visit(finder)
        return finder.found

    def _patch_import(self, body: Any) -> Any:
        new_body = []
        patched = False
        for stmt in body:
            stmt = self._maybe_add_a2a_path(stmt)
            if isinstance(stmt, cst.SimpleStatementLine) and any(
                isinstance(s, cst.ImportFrom)
                and isinstance(s.module, cst.Name)
                and s.module.value == "novaos_client_sdk"
                for s in stmt.body
            ):
                patched = True
            new_body.append(stmt)
        if not patched:
            # No existing `from novaos_client_sdk import ...` — extremely
            # unlikely for an agent whose route_line action fired at all
            # (it always imported AgentBuilder to build `agent` in the first
            # place), but handled rather than silently producing code that
            # references an undefined name.
            new_body.insert(
                0, cst.parse_statement("from novaos_client_sdk import A2A_JSONRPC_PATH\n")
            )
        return new_body

    def _maybe_add_a2a_path(self, stmt: Any) -> Any:
        if not isinstance(stmt, cst.SimpleStatementLine):
            return stmt
        new_lines = []
        changed = False
        for line in stmt.body:
            if (
                isinstance(line, cst.ImportFrom)
                and isinstance(line.module, cst.Name)
                and line.module.value == "novaos_client_sdk"
                and not isinstance(line.names, cst.ImportStar)
            ):
                names = [a.name.value for a in line.names if isinstance(a.name, cst.Name)]
                if "A2A_JSONRPC_PATH" not in names:
                    all_names = sorted([*names, "A2A_JSONRPC_PATH"])
                    line = line.with_changes(
                        names=[cst.ImportAlias(name=cst.Name(n)) for n in all_names]
                    )
                    changed = True
            new_lines.append(line)
        return stmt.with_changes(body=new_lines) if changed else stmt

    def _name_used_elsewhere(self, body: Any, name: str) -> bool:
        class _Finder(cst.CSTVisitor):
            hits = 0

            def visit_Name(self, node: cst.Name) -> None:
                if node.value == name:
                    self.hits += 1

        finder = _Finder()
        for stmt in body:
            if isinstance(stmt, cst.SimpleStatementLine) and any(
                isinstance(s, (cst.Import, cst.ImportFrom)) for s in stmt.body
            ):
                continue
            stmt.visit(finder)
        return finder.hits > 0

    def _drop_fastapi_request_import(self, body: Any) -> Any:
        new_body = []
        for stmt in body:
            if isinstance(stmt, cst.SimpleStatementLine):
                new_lines = []
                for line in stmt.body:
                    if (
                        isinstance(line, cst.ImportFrom)
                        and isinstance(line.module, cst.Name)
                        and line.module.value == "fastapi"
                        and not isinstance(line.names, cst.ImportStar)
                    ):
                        remaining = [
                            a
                            for a in line.names
                            if not (isinstance(a.name, cst.Name) and a.name.value == "Request")
                        ]
                        if not remaining:
                            continue
                        line = line.with_changes(names=_without_trailing_comma(remaining))
                    new_lines.append(line)
                if not new_lines:
                    continue
                stmt = stmt.with_changes(body=new_lines)
            new_body.append(stmt)
        return new_body


# ── drop_ensure_acp_output ───────────────────────────────────────────────


def _handle_drop_ensure_acp_output(ctx: _TransformContext) -> None:
    rel_path = _find_server_file(ctx)
    source = ctx.current_text(rel_path)
    assert source is not None
    module = cst.parse_module(source)
    new_module = module.visit(_DropImportedNameTransformer(name="ensure_acp_output"))
    ctx.write(rel_path, new_module.code)


class _DropImportedNameTransformer(cst.CSTTransformer):
    """Remove ``name`` from whichever ``from X import ..., name, ...`` binds
    it, dropping the whole import statement if ``name`` was its only member.
    Used for helpers ``route_line`` already deleted every call site of."""

    def __init__(self, *, name: str) -> None:
        self.name = name

    def leave_SimpleStatementLine(
        self, original_node: cst.SimpleStatementLine, updated_node: cst.SimpleStatementLine
    ) -> cst.RemovalSentinel | cst.SimpleStatementLine:
        new_lines = []
        for line in updated_node.body:
            if isinstance(line, cst.ImportFrom) and not isinstance(line.names, cst.ImportStar):
                remaining = [
                    a
                    for a in line.names
                    if not (isinstance(a.name, cst.Name) and a.name.value == self.name)
                ]
                if len(remaining) != len(line.names):
                    if not remaining:
                        continue
                    line = line.with_changes(names=_without_trailing_comma(remaining))
            new_lines.append(line)
        if not new_lines:
            return cst.RemoveFromParent()
        return updated_node.with_changes(body=new_lines)


# ── replace_acp_request_with_commons_router ─────────────────────────────


def _handle_replace_acp_request_with_commons_router(ctx: _TransformContext) -> None:
    rel_path = _find_server_file(ctx)
    source = ctx.current_text(rel_path)
    assert source is not None
    module = cst.parse_module(source)

    routing_module, transformer = _find_and_strip_routing_import(module)
    if routing_module is None:
        # Same idempotency reasoning as route_line's "not found" case above:
        # a second run against an already-migrated tree has no
        # `from <module> import normalize_acp_body, ...` left to find.
        ctx.manual.append(
            f"{rel_path}: replace_acp_request_with_commons_router found no import of "
            f"{_ROUTING_HELPER_NAMES!r} — either already migrated, or this plan does "
            "not match this agent tree; skipped"
        )
        return
    new_module = module.visit(transformer)
    new_module = new_module.visit(_EnsureImportTransformer())
    ctx.write(rel_path, new_module.code)

    # Deferred to transform_agent's final step, after rewrite_routing_tests
    # has run — see _TransformContext.pending_module_removal's docstring.
    ctx.pending_module_removal = (routing_module, rel_path)


def _find_and_strip_routing_import(
    module: cst.Module,
) -> tuple[str | None, _StripRoutingImportTransformer]:
    found: dict[str, str] = {}

    class _Finder(cst.CSTVisitor):
        def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
            if isinstance(node.names, cst.ImportStar):
                return
            names = {a.name.value for a in node.names if isinstance(a.name, cst.Name)}
            if names & set(_ROUTING_HELPER_NAMES):
                mod = _dotted_module_name(node)
                if mod:
                    found["module"] = mod

    module.visit(_Finder())
    mod = found.get("module")
    return mod, _StripRoutingImportTransformer(module_name=mod)


def _dotted_module_name(node: cst.ImportFrom) -> str | None:
    parts: list[str] = []

    def _walk(expr: Any) -> None:
        if isinstance(expr, cst.Attribute):
            _walk(expr.value)
            parts.append(expr.attr.value)
        elif isinstance(expr, cst.Name):
            parts.append(expr.value)

    if node.module is None:
        return None
    _walk(node.module)
    return ".".join(parts) if parts else None


class _StripRoutingImportTransformer(cst.CSTTransformer):
    """Remove ``from <module_name> import normalize_acp_body, ...`` and add
    ``import commons.a2a_request`` in its place, once, at the same position."""

    def __init__(self, *, module_name: str | None) -> None:
        self.module_name = module_name
        self._inserted = False

    def leave_SimpleStatementLine(
        self, original_node: cst.SimpleStatementLine, updated_node: cst.SimpleStatementLine
    ) -> cst.RemovalSentinel | cst.SimpleStatementLine | cst.FlattenSentinel:
        is_target = any(
            isinstance(line, cst.ImportFrom) and _dotted_module_name(line) == self.module_name
            for line in updated_node.body
        )
        if not is_target:
            return updated_node
        replacement = cst.SimpleStatementLine(
            body=[
                cst.Import(
                    names=[
                        cst.ImportAlias(
                            name=cst.Attribute(
                                value=cst.Name("commons"), attr=cst.Name("a2a_request")
                            )
                        )
                    ]
                )
            ]
        )
        self._inserted = True
        return replacement


class _EnsureImportTransformer(cst.CSTTransformer):
    """No-op placeholder kept for symmetry with route_line's import patcher —
    ``_StripRoutingImportTransformer`` already inserts ``import
    commons.a2a_request`` at the strip site, so there is nothing further to
    ensure here today. Kept as a distinct pass (rather than folded into the
    strip transformer) because a future action that also needs
    ``commons.a2a_request`` imported should extend this, not re-open the
    strip logic above."""

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        return updated_node


def _maybe_remove_unreferenced_module(
    ctx: _TransformContext, dotted_module: str, *, keep: str
) -> None:
    """Delete the file ``dotted_module`` resolves to, but only if — after
    this run's own rewrite — nothing else left in the tree still imports it.
    This is the codemod's only file deletion, and it stays conservative on
    purpose: a hit anywhere else leaves the file in place and records a
    ``manual[]`` note instead of guessing that the other reference is itself
    dead code the plan's analyzer would have caught.

    Resolves every remaining file's imports the same way the plan's own
    reachability walk does (:func:`~.analyze._resolve_import`), rather than
    a plain substring search for ``dotted_module`` — a relative import
    (``from . import acp_request``, ``from .acp_request import X``) never
    contains that literal dotted string, so a substring check would silently
    miss a still-live importer and delete a module still in use.
    """
    candidate = ctx.agent_path / (dotted_module.replace(".", "/") + ".py")
    if not candidate.is_file():
        return
    rel_candidate = candidate.relative_to(ctx.agent_path).as_posix()

    tracked = ctx.all_tracked_files()
    is_package = {
        _module_name(ctx.agent_path / p, ctx.agent_path): Path(p).name == "__init__.py"
        for p in tracked
    }

    for rel_path in tracked:
        if rel_path in (rel_candidate, keep):
            continue
        text = ctx.current_text(rel_path)
        if text is None:
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        mod_name = _module_name(ctx.agent_path / rel_path, ctx.agent_path)
        imports_it = any(
            _resolve_import(raw_name, mod_name, is_package) == dotted_module
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for raw_name in _iter_import_module_names(node)
        )
        if imports_it:
            ctx.manual.append(
                f"{rel_candidate}: still imported from {rel_path} after the "
                "commons-router rewrite — left in place rather than deleted; "
                "confirm that reference is itself dead before removing it by hand"
            )
            return

    ctx.remove(rel_candidate)


# ── replace_from_acp_request ─────────────────────────────────────────────


def _handle_replace_from_acp_request(ctx: _TransformContext) -> None:
    """Always defers to ``manual[]`` — see this module's docstring for why a
    mechanical rewrite of ``from_acp_request`` call sites is not safe in
    general: ACP's ``(body, headers)`` and A2A's ``(message, params, headers,
    *, task_id=..., ...)`` are not related by a syntactic rename."""
    for surface in ctx.plan.surfaces:
        if surface.kind == "from_acp_request_call":
            ctx.manual.append(
                f"{surface.file}:{surface.line}: from_acp_request(...) call — no "
                "mechanical A2A equivalent (ACP's flat `body` dict cannot become "
                "A2A's `message`/`params` objects by renaming); port this call site "
                "by hand to AgentExecutionContext.from_a2a_request(message, params, "
                "headers, ...)"
            )


# ── rewrite_routing_tests ────────────────────────────────────────────────


def _handle_rewrite_routing_tests(ctx: _TransformContext) -> None:
    for rel_path in ctx.plan.tests.routing_inference_files:
        source = ctx.current_text(rel_path)
        if source is None:
            continue
        rewritten, ok, needs_skill_kwargs = _rewrite_routing_test_source(source)
        new_path = _renamed_test_path(rel_path)
        if ok:
            if new_path != rel_path:
                ctx.remove(rel_path)
            ctx.write(new_path, rewritten)
            if needs_skill_kwargs:
                ctx.manual.append(
                    f"{new_path}: normalize_acp_body(...) call(s) were given "
                    "start_skill=START_SKILL, progress_skill=PROGRESS_SKILL placeholders — "
                    "replace both with the same agent-specific skill names filled into "
                    "server.py's commons.a2a_request.route(...) call, same reason: a "
                    "generic ACP-shape plan cannot know an agent's own skill names"
                )
        else:
            ctx.manual.append(
                f"{rel_path}: routing-inference test uses an import/assertion shape "
                "this codemod does not recognize — rewrite it by hand against "
                "commons.a2a_request, then rename it to test_a2a_request.py"
            )


def _renamed_test_path(rel_path: str) -> str:
    p = Path(rel_path)
    if p.name == "test_acp_request.py":
        return str(p.with_name("test_a2a_request.py"))
    return rel_path


def _rewrite_routing_test_source(source: str) -> tuple[str, bool, bool]:
    """The one recognized shape: a plain ``from skills.acp_request import
    <names>`` (or any local module exposing the same routing-helper names) —
    rewritten to import the same names from ``commons.a2a_request``, which
    re-exports them for exactly this reason (see a customer's shared
    ``commons/a2a_request.py``). Every assertion in the test body is
    untouched: the inference rules themselves did not move, only where they
    live. Anything else (a test that imports ``server`` and drives ``/runs``
    through a `TestClient`, for instance) is out of this shape and goes to
    ``manual[]`` instead of a guessed rewrite.

    ``commons.a2a_request.normalize_acp_body`` takes two required
    keyword-only arguments (``start_skill``, ``progress_skill``) the old
    per-agent function never needed — they are exactly the agent-specific
    names route_line's commons-router branch also cannot know (see this
    module's docstring, Decision 2). Every recognized call to
    ``normalize_acp_body(...)`` gets the same ``START_SKILL``/
    ``PROGRESS_SKILL`` placeholders route_line already generates in
    ``server.py``, rather than a rewrite that type-errors at test collection
    with no clue why. Returns ``(new_source, matched, needs_skill_kwargs)``.
    """
    try:
        module = cst.parse_module(source)
    except cst.ParserSyntaxError:
        return source, False, False

    import_rewriter = _RoutingTestImportRewriter()
    module = module.visit(import_rewriter)
    if not import_rewriter.matched:
        return source, False, False

    kwarg_injector = _NormalizeAcpBodySkillKwargInjector()
    module = module.visit(kwarg_injector)
    return module.code, True, kwarg_injector.injected


class _RoutingTestImportRewriter(cst.CSTTransformer):
    def __init__(self) -> None:
        self.matched = False

    def leave_ImportFrom(
        self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom
    ) -> cst.ImportFrom:
        if isinstance(updated_node.names, cst.ImportStar):
            return updated_node
        names = {a.name.value for a in updated_node.names if isinstance(a.name, cst.Name)}
        if not (names & set(_ROUTING_HELPER_NAMES)):
            return updated_node
        self.matched = True
        return updated_node.with_changes(
            module=cst.Attribute(value=cst.Name("commons"), attr=cst.Name("a2a_request"))
        )


class _NormalizeAcpBodySkillKwargInjector(cst.CSTTransformer):
    """Adds ``start_skill=START_SKILL, progress_skill=PROGRESS_SKILL`` to
    every ``normalize_acp_body(...)`` call missing them — matching
    ``route_line``'s placeholders in ``server.py`` exactly, so the two files
    need the same one search-and-replace rather than two different ones."""

    def __init__(self) -> None:
        self.injected = False

    def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.Call:
        if not (
            isinstance(updated_node.func, cst.Name)
            and updated_node.func.value == "normalize_acp_body"
        ):
            return updated_node
        existing = {a.keyword.value for a in updated_node.args if a.keyword is not None}
        missing = [n for n in ("start_skill", "progress_skill") if n not in existing]
        if not missing:
            return updated_node
        self.injected = True
        placeholder = {"start_skill": "START_SKILL", "progress_skill": "PROGRESS_SKILL"}
        new_args = [
            *updated_node.args,
            *(
                cst.Arg(keyword=cst.Name(name), value=cst.Name(placeholder[name]))
                for name in missing
            ),
        ]
        return updated_node.with_changes(args=new_args)


_HANDLERS = {
    "route_line": _handle_route_line,
    "drop_ensure_acp_output": _handle_drop_ensure_acp_output,
    "replace_acp_request_with_commons_router": _handle_replace_acp_request_with_commons_router,
    "replace_from_acp_request": _handle_replace_from_acp_request,
    "rewrite_routing_tests": _handle_rewrite_routing_tests,
}
