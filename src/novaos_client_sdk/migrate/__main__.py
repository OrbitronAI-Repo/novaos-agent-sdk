"""``python -m novaos_client_sdk.migrate`` — ACP→A2A migration analyzer.

::

    python -m novaos_client_sdk.migrate analyze ./my-agent --out ./out
    python -m novaos_client_sdk.migrate analyze ./agents --fleet --out ./out

``analyze`` never imports or executes the agent it looks at — see
:mod:`~novaos_client_sdk.migrate.analyze` for why — and never writes inside
the tree it analyzes; every file this command produces goes under ``--out``.
Without ``--out``, ``analyze`` still runs and reports (JSON on stdout for a
single agent, a Markdown fleet table for ``--fleet``) but writes nothing to
disk, so it is safe to run against a read-only checkout just to see the
numbers.

**Exit codes** (single-agent and fleet mode alike):

= =============================================================
0 no blockers found
1 at least one blocker found
2 could not analyze — a target had no ``manifest.json``, or a
  ``.py`` file in it failed to parse
= =============================================================

``transform`` rewrites an agent per a plan ``analyze`` already wrote;
``verify`` then runs the rewritten agent for real and checks it
behaves the same as its pre-migration fixtures expect — see
:mod:`~novaos_client_sdk.migrate.verify` for the full contract.

::

    python -m novaos_client_sdk.migrate transform ./my-agent --plan ./out/my-agent.plan.json
    python -m novaos_client_sdk.migrate transform ./my-agent --plan ./out/my-agent.plan.json --write

``transform`` requires the ``migrate`` extra (``pip install
"novaos-client-sdk[migrate]"``) for ``libcst``; without it, this prints a
clear message on stderr and exits ``3`` rather than a bare ``ImportError``
traceback.

**Exit codes** for ``transform``:

= =============================================================
0 dry run completed, or ``--write`` applied every planned action
2 refused: the plan is not codemod-eligible (Shape C, or unknown),
  an action in ``plan.codemod.actions`` has no handler in this SDK
  version, or the plan file could not be read
= =============================================================

::

    python -m novaos_client_sdk.migrate verify ./my-agent --base-url http://127.0.0.1:8001
    python -m novaos_client_sdk.migrate verify ./my-agent --base-url http://127.0.0.1:8001 \\
        --expect screen=INPUT_REQUIRED --stream screen

``verify`` needs no extra — it uses this SDK's own :class:`Client`, already a
core dependency. Unlike ``analyze``/``transform`` it does reach the network:
``--base-url`` must point at a running instance of the migrated agent.

**Exit codes** for ``verify``:

= =============================================================
0 every skill check and the card check passed
1 at least one skill assertion failed, or the card and manifest
  disagree on the skill set
2 could not verify at all — no fixtures found, or a fixture/plan
  file could not be read
= =============================================================
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from .analyze import AnalysisError, analyze_agent
from .model import Plan, load_plan
from .report import render_fleet_table, render_plan_markdown, render_verify_markdown
from .verify import VerifyError, parse_expect_overrides, parse_stream_skills, verify_agent

#: Directory names that are never themselves an agent, even though they sit
#: alongside agent directories at a fleet root (``agents/commons``,
#: ``agents/sdk/novaos-client-sdk``, ``agents/test_kit``). Skipped by
#: presence-of-``manifest.json`` in practice, but named here too so a fleet
#: run's "skipped" report reads clearly rather than just "no manifest.json".
_NEVER_AGENT_DIR_NAMES = frozenset({"commons", "sdk", "test_kit"})


def _iter_fleet_candidates(root: Path) -> list[Path]:
    return sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith("."))


def _write_plan_files(plan: Plan, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{plan.agent}.plan.json").write_text(
        json.dumps(plan.to_dict(), indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    (out_dir / f"{plan.agent}.plan.md").write_text(render_plan_markdown(plan), encoding="utf-8")


def _run_single(path: Path, out: Path | None) -> int:
    try:
        plan = analyze_agent(path)
    except AnalysisError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(plan.to_dict(), sort_keys=False))
    if out is not None:
        _write_plan_files(plan, out)
    return plan.exit_code


def _run_fleet(root: Path, out: Path | None) -> int:
    plans: list[Plan] = []
    errors: list[str] = []
    skipped: list[str] = []

    for candidate in _iter_fleet_candidates(root):
        if not (candidate / "manifest.json").is_file():
            skipped.append(candidate.name)
            continue
        try:
            plans.append(analyze_agent(candidate))
        except AnalysisError as exc:
            errors.append(str(exc))

    if out is not None:
        for plan in plans:
            _write_plan_files(plan, out)
        out.mkdir(parents=True, exist_ok=True)
        summary_md = render_fleet_table(plans, skipped=skipped)
        (out / "fleet-summary.md").write_text(summary_md, encoding="utf-8")
        (out / "fleet-summary.json").write_text(
            json.dumps(
                {
                    "agents": [p.to_dict() for p in plans],
                    "skipped": sorted(skipped),
                    "errors": errors,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    print(render_fleet_table(plans, skipped=skipped))
    for err in errors:
        print(err, file=sys.stderr)

    if errors:
        return 2
    if any(p.blockers for p in plans):
        return 1
    return 0


def _cmd_analyze(args: argparse.Namespace) -> int:
    path = Path(args.path)
    out = Path(args.out) if args.out else None
    if args.fleet:
        return _run_fleet(path, out)
    return _run_single(path, out)


def _cmd_transform(args: argparse.Namespace) -> int:
    try:
        from . import transform as transform_mod
    except ImportError:
        print(
            "'transform' requires the 'migrate' extra: pip install " '"novaos-client-sdk[migrate]"',
            file=sys.stderr,
        )
        return 3

    path = Path(args.path)
    plan_path = Path(args.plan)
    try:
        plan = load_plan(plan_path)
    except (OSError, json.JSONDecodeError, KeyError) as exc:
        print(f"{plan_path}: could not read plan ({exc})", file=sys.stderr)
        return 2

    try:
        result = transform_mod.transform_agent(path, plan, write=args.write)
    except transform_mod.TransformError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if result.refused:
        print(f"refused: {result.refusal_reason}", file=sys.stderr)
        return 2

    if not args.write:
        print(result.diff_text(), end="")
    for note in result.manual:
        print(f"manual: {note}", file=sys.stderr)
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    path = Path(args.path)

    if args.plan:
        try:
            plan = load_plan(Path(args.plan))
        except (OSError, json.JSONDecodeError, KeyError) as exc:
            print(f"{args.plan}: could not read plan ({exc})", file=sys.stderr)
            return 2
    else:
        try:
            plan = analyze_agent(path)
        except AnalysisError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    try:
        expect_overrides = parse_expect_overrides(args.expect)
        stream_skills = parse_stream_skills(args.stream)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        report = asyncio.run(
            verify_agent(
                path,
                plan,
                base_url=args.base_url,
                expect_overrides=expect_overrides,
                stream_skills=stream_skills,
                timeout=args.timeout,
            )
        )
    except VerifyError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(json.dumps(report.to_dict(), sort_keys=False))
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{plan.agent}.verify.json").write_text(
            json.dumps(report.to_dict(), indent=2, sort_keys=False) + "\n", encoding="utf-8"
        )
        (out / f"{plan.agent}.verify.md").write_text(
            render_verify_markdown(report), encoding="utf-8"
        )

    return report.exit_code


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m novaos_client_sdk.migrate")
    sub = parser.add_subparsers(dest="command", required=True)

    analyze_parser = sub.add_parser(
        "analyze", help="Classify an ACP agent and emit a migration plan"
    )
    analyze_parser.add_argument(
        "path", help="Path to an agent directory, or a fleet root with --fleet"
    )
    analyze_parser.add_argument("--out", help="Directory to write plan.json/plan.md into")
    analyze_parser.add_argument(
        "--fleet",
        action="store_true",
        help="Treat 'path' as a directory of agent directories, analyzing each",
    )
    analyze_parser.set_defaults(func=_cmd_analyze)

    transform_parser = sub.add_parser(
        "transform", help="Rewrite an ACP agent onto the A2A shell per its migration plan"
    )
    transform_parser.add_argument("path", help="Path to the agent directory to rewrite")
    transform_parser.add_argument(
        "--plan", required=True, help="Path to the agent's <agent>.plan.json from `analyze`"
    )
    transform_parser.add_argument(
        "--write",
        action="store_true",
        help="Apply the rewrite; without this, only a unified diff is printed",
    )
    transform_parser.set_defaults(func=_cmd_transform)

    verify_parser = sub.add_parser(
        "verify", help="Run a migrated agent's own fixtures against it and check its Agent Card"
    )
    verify_parser.add_argument("path", help="Path to the agent directory to verify")
    verify_parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8001",
        help="Where the migrated agent is running (default: %(default)s)",
    )
    verify_parser.add_argument(
        "--plan",
        help="Path to the agent's <agent>.plan.json from `analyze`; re-analyzes the "
        "agent tree if omitted",
    )
    verify_parser.add_argument(
        "--expect",
        help="Comma-separated skill=STATE overrides, e.g. 'screen=INPUT_REQUIRED'",
    )
    verify_parser.add_argument(
        "--stream",
        help="Comma-separated skill names to also probe with SendStreamingMessage",
    )
    verify_parser.add_argument("--out", help="Directory to write verify.json/verify.md into")
    verify_parser.add_argument(
        "--timeout", type=float, default=30.0, help="Per-request timeout in seconds"
    )
    verify_parser.set_defaults(func=_cmd_verify)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
