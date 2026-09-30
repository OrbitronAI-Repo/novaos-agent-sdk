"""Render a :class:`~.model.Plan` (or a fleet of them) as Markdown.

The JSON a :class:`~.model.Plan` serializes to is the contract later
migration stages consume programmatically; this module is the human-facing
side of the same data — what a reviewer reads on the tracking ticket or in
``<out>/<agent>.plan.md`` before the codemod touches anything.
"""

from __future__ import annotations

from .model import Plan
from .verify import VerifyReport


def render_plan_markdown(plan: Plan) -> str:
    lines: list[str] = [f"# {plan.agent}", ""]
    lines.append(f"## Shape: {plan.shape.value}{' (mixed)' if plan.mixed else ''}")
    lines.append("")
    lines.append(plan.shape_reason)
    lines.append("")

    lines.append("## ACP surfaces")
    lines.append("")
    if plan.surfaces:
        lines.append("| Kind | File:Line | Detail |")
        lines.append("|---|---|---|")
        for s in plan.surfaces:
            detail = s.detail or ""
            lines.append(f"| {s.kind} | {s.file}:{s.line} | {detail} |")
    else:
        lines.append("_None found._")
    lines.append("")

    lines.append("## Blockers")
    lines.append("")
    if plan.blockers:
        lines.append("| Kind | File:Line | Value | Effect |")
        lines.append("|---|---|---|---|")
        for b in plan.blockers:
            lines.append(f"| {b.kind} | {b.file}:{b.line} | {b.value} | {b.effect} |")
    else:
        lines.append("_None found._")
    lines.append("")

    lines.append("## Dead code")
    lines.append("")
    if plan.dead_code:
        for d in plan.dead_code:
            lines.append(f"- `{d.file}` — {d.reason}")
    else:
        lines.append("_None found._")
    lines.append("")

    lines.append("## Tests affected")
    lines.append("")
    lines.append(f"- Total test files: {plan.tests.total_files}")
    if plan.tests.runs_route_files:
        lines.append(f"- Exercise `/runs` directly: {', '.join(plan.tests.runs_route_files)}")
    if plan.tests.make_ctx_files:
        lines.append(
            f"- Build `AgentExecutionContext` via `make_ctx`: {', '.join(plan.tests.make_ctx_files)}"
        )
    if plan.tests.routing_inference_files:
        lines.append(
            f"- Test skill-routing inference: {', '.join(plan.tests.routing_inference_files)}"
        )
    lines.append("")

    lines.append("## Codemod actions")
    lines.append("")
    if plan.codemod.eligible:
        lines.append("Eligible. Actions:")
        for action in plan.codemod.actions:
            lines.append(f"- {action}")
    else:
        lines.append(f"**Refused.** {plan.codemod.refusal_reason}")
    lines.append("")

    lines.append("## Manual work")
    lines.append("")
    if plan.manual:
        for note in plan.manual:
            lines.append(f"- {note}")
    else:
        lines.append("_None._")
    lines.append("")

    lines.append("## Verify fixtures")
    lines.append("")
    if plan.verify_fixtures:
        for f in plan.verify_fixtures:
            lines.append(f"- `{f}`")
    else:
        lines.append("_None found._")
    lines.append("")

    return "\n".join(lines)


def render_fleet_table(plans: list[Plan], *, skipped: list[str]) -> str:
    lines = [
        "# Fleet summary",
        "",
        "| Agent | Shape | Mixed | Blockers | Dead code | Codemod |",
        "|---|---|---|---|---|---|",
    ]
    for plan in sorted(plans, key=lambda p: p.agent):
        codemod = "eligible" if plan.codemod.eligible else "refused"
        lines.append(
            f"| {plan.agent} | {plan.shape.value} | {'yes' if plan.mixed else ''} | "
            f"{len(plan.blockers)} | {len(plan.dead_code)} | {codemod} |"
        )
    lines.append("")

    shape_counts: dict[str, int] = {}
    for plan in plans:
        shape_counts[plan.shape.value] = shape_counts.get(plan.shape.value, 0) + 1
    counts_str = ", ".join(f"{v} {k}" for k, v in sorted(shape_counts.items()))
    lines.append(f"**Totals:** {len(plans)} analyzed ({counts_str})")
    if skipped:
        lines.append(f"**Skipped** (no manifest.json): {', '.join(sorted(skipped))}")
    lines.append("")
    return "\n".join(lines)


def render_verify_markdown(report: VerifyReport) -> str:
    """The human-facing side of a :class:`~.verify.VerifyReport` — what
    ``verify --out`` writes as ``<agent>.verify.md``, alongside the JSON both
    this process's stdout and ``<agent>.verify.json`` carry."""
    lines: list[str] = [
        f"# {report.agent} — verify",
        "",
        f"**Result:** {'PASS' if report.ok else 'FAIL'}  ",
        f"**Base URL:** {report.base_url}",
        "",
    ]

    if report.error:
        lines.append(f"**Could not verify.** {report.error}")
        lines.append("")
        return "\n".join(lines)

    lines.append("## Skills")
    lines.append("")
    if report.skills:
        lines.append("| Skill | Fixture | Expected | Actual | Stream | OK |")
        lines.append("|---|---|---|---|---|---|")
        for s in report.skills:
            stream_col = "-"
            if s.stream_requested:
                stream_col = "ok" if s.stream_ok else f"FAIL ({s.stream_error})"
            actual = s.actual_state if s.error is None else f"ERROR: {s.error}"
            lines.append(
                f"| {s.skill} | {s.fixture} | {s.expected_state} | {actual} | "
                f"{stream_col} | {'yes' if s.ok else 'no'} |"
            )
            if s.detail:
                lines.append(f"|  |  |  | _{s.detail}_ |  |  |")
    else:
        lines.append("_None found._")
    lines.append("")

    lines.append("## Agent Card")
    lines.append("")
    if report.card is None:
        lines.append("_Not checked._")
    elif report.card.error:
        lines.append(f"**Could not check.** {report.card.error}")
    else:
        lines.append(f"Manifest skills: {', '.join(report.card.manifest_skills) or '_none_'}  ")
        lines.append(f"Card skills: {', '.join(report.card.card_skills) or '_none_'}")
        if report.card.missing_from_card:
            lines.append(f"- **Missing from card:** {', '.join(report.card.missing_from_card)}")
        if report.card.extra_on_card:
            lines.append(
                f"- **On card but not in manifest:** {', '.join(report.card.extra_on_card)}"
            )
        if report.card.ok:
            lines.append("Card matches the manifest.")
    lines.append("")

    return "\n".join(lines)
