"""``python -m novaos_client_sdk.migrate verify`` — post-migration behavioral check.

::

    python -m novaos_client_sdk.migrate verify ./example-cm-agent \\
        --base-url http://127.0.0.1:8001 --expect screen=INPUT_REQUIRED

Where :mod:`~novaos_client_sdk.migrate.analyze` only ever reads an
agent's source and :mod:`~novaos_client_sdk.migrate.transform` only
ever rewrites it, ``verify`` is the one stage of this pipeline that actually
*runs* the migrated agent: it converts each of the agent's own pre-migration
fixtures (``tests/test-data/*.json`` — an ACP-era smoke-test body, the same
shape an agent's staging smoke used before this migration) into the A2A
``SendMessage`` request an equivalent caller would send today, fires it at a
live agent process at ``--base-url``, and asserts the resulting
:class:`~novaos_client_sdk.a2a_task.Task` ended in the state that fixture is
expected to reach. It also checks the agent's live Agent Card against its
``manifest.json`` — the two must name exactly the same skills, or a caller
routing off the card can reach a skill the manifest never declared, or fail to
reach one it did.

**This never imports the agent under verification.** Unlike ``agent_card``,
which imports a ``server.py``-shaped module in-process to build a
card, everything here that talks to the agent goes over the wire, through
:class:`~novaos_client_sdk.client.Client`, exactly the way a real caller
would — a live process at ``--base-url``, not a Python object this process
happens to share memory with. The one exception is the card check itself,
which *does* import the target (see :func:`check_card`) because that is
literally what ``agent_card`` does and there is no wire equivalent to reuse;
the message checks never do.

**Fixtures are read fresh from the agent tree, not baked into a plan.** A
:class:`~novaos_client_sdk.migrate.model.Plan` supplies
``manifest.skills`` (the default skill for a fixture that names none, and the
set a card is checked against) and ``verify_fixtures`` (which files under
``tests/test-data/`` to convert) — nothing here re-derives either fact, so an
analyzer change flows straight through. A caller with a ``<agent>.plan.json``
from ``analyze`` already on hand passes ``--plan`` to reuse it as-is; without
one, ``verify`` calls :func:`~novaos_client_sdk.migrate.analyze.analyze_agent`
itself. That call is safe post-migration even though the analyzer was written
to classify ACP shells — a fully migrated ``server.py`` simply reports
``Shape.UNKNOWN`` with no ACP surfaces, which ``verify`` does not care about;
only ``manifest`` and ``verify_fixtures`` matter here.

**Per-fixture expectations, in increasing precedence.** Every fixture defaults
to expecting ``TASK_STATE_COMPLETED`` — the common case, a synchronous skill
that finishes in the same call. Three ways to say otherwise, each overriding
the last:

1. A ``verify.json`` next to the fixtures (``tests/test-data/verify.json``),
   keyed by fixture filename, e.g. ``{"pause-request.json": {"expected_state":
   "INPUT_REQUIRED", "stream": true}}`` — the durable, checked-in expectation
   for a fixture that is not the ``COMPLETED`` default, such as a pausing HITL
   skill or a background job start landing on ``SUBMITTED``/``WORKING``.
2. ``--expect skill=STATE[,skill=STATE...]`` on the command line, keyed by
   *skill* rather than fixture — a one-off override for a single run, without
   editing a checked-in file.

A state name is matched case-insensitively and with or without the
``TASK_STATE_`` prefix (``completed``, ``COMPLETED`` and
``TASK_STATE_COMPLETED`` are the same value) — see :func:`normalize_state`.

**Streaming is opt-in per skill**, via ``--stream skill[,skill...]`` or
``"stream": true`` in ``verify.json`` — most skills have nothing distinctive
to check over ``SendStreamingMessage`` beyond what ``SendMessage`` already
proved, and a caller opts a skill in once it is known to advertise
streaming. When probed, a well-formed stream must produce at least one frame
and close on a terminal or interrupt state, never simply run out of frames
(the stream-terminality shape :func:`~novaos_client_sdk.a2a_events.is_last_frame`
exists to catch); no particular *opening* frame shape is required — a skill
that returns immediately with no interim progress update legitimately opens
on its auto-synthesised output artifact rather than a status frame. See
:func:`_validate_stream`.

**A skill a fixture cannot route to is a failure, not a crash.** By design,
an agent that cannot resolve which skill to run answers with a
``TASK_STATE_REJECTED`` task rather than guessing, and a request naming a
method the peer does not implement answers a JSON-RPC ``-32601`` — both are
reported as an ordinary failed :class:`SkillCheck` (the rejected state, or the
error's code/message) alongside a wrong-state failure, not raised out of this
module.

**Exit codes:**

= =============================================================
0 every skill check and the card check passed
1 at least one skill assertion failed, or the card is missing a skill
  the manifest declares (or vice versa)
2 could not verify at all — no ``manifest.json``, no fixtures found
  under ``tests/test-data/``, or a fixture file was unreadable
= =============================================================
"""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from novaos_client_sdk.a2a_events import StreamFrame, is_last_frame
from novaos_client_sdk.a2a_task import Task, TaskState, TaskStatus
from novaos_client_sdk.a2a_types import A2AMessage, DataPart, FilePart, Role, TextPart
from novaos_client_sdk.agent_context import NOVAOS_CONTEXT_METADATA_KEY
from novaos_client_sdk.client import Client
from novaos_client_sdk.exceptions import NovaOSError

from .model import Plan

#: Filename ``verify`` looks for next to an agent's fixtures for durable,
#: checked-in expectation overrides. See the module docstring's "Per-fixture
#: expectations" section.
VERIFY_OVERRIDES_FILENAME = "verify.json"

#: What ``verify`` looks for at the conventional Galaxy-scaffold location — the
#: same ``server:agent`` spec :mod:`~novaos_client_sdk.agent_card` documents
#: and every fixture under ``tests/fixtures/migrate/`` uses.
DEFAULT_CARD_TARGET = "server:agent"


class VerifyError(Exception):
    """Refused: no manifest/fixtures to verify against, or a fixture/override
    file that could not be read. Callers map this to exit code 2."""


@dataclass
class SkillCheck:
    """The outcome of converting one fixture into a request and sending it."""

    skill: str
    fixture: str
    expected_state: str
    actual_state: str | None = None
    detail: str = ""
    error: str | None = None
    stream_requested: bool = False
    stream_ok: bool | None = None
    stream_error: str | None = None

    @property
    def ok(self) -> bool:
        if self.error is not None:
            return False
        if self.actual_state != self.expected_state:
            return False
        if self.stream_requested and not self.stream_ok:
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "skill": self.skill,
            "fixture": self.fixture,
            "expected_state": self.expected_state,
            "actual_state": self.actual_state,
            "ok": self.ok,
        }
        if self.detail:
            d["detail"] = self.detail
        if self.error is not None:
            d["error"] = self.error
        if self.stream_requested:
            d["stream_ok"] = self.stream_ok
            if self.stream_error:
                d["stream_error"] = self.stream_error
        return d


@dataclass
class CardCheck:
    """Whether the agent's live Agent Card names exactly ``manifest.json``'s
    skills — see :func:`check_card`."""

    ok: bool
    manifest_skills: list[str] = field(default_factory=list)
    card_skills: list[str] = field(default_factory=list)
    missing_from_card: list[str] = field(default_factory=list)
    extra_on_card: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "ok": self.ok,
            "manifest_skills": self.manifest_skills,
            "card_skills": self.card_skills,
        }
        if self.missing_from_card:
            d["missing_from_card"] = self.missing_from_card
        if self.extra_on_card:
            d["extra_on_card"] = self.extra_on_card
        if self.error is not None:
            d["error"] = self.error
        return d


@dataclass
class VerifyReport:
    """What ``python -m novaos_client_sdk.migrate verify`` produces — the
    ``<agent>.verify.json``/``<agent>.verify.md`` pair, and this process's own
    stdout."""

    agent: str
    base_url: str
    skills: list[SkillCheck] = field(default_factory=list)
    card: CardCheck | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        if self.error is not None:
            return False
        if not self.skills:
            return False
        if any(not s.ok for s in self.skills):
            return False
        if self.card is not None and not self.card.ok:
            return False
        return True

    @property
    def exit_code(self) -> int:
        if self.error is not None:
            return 2
        return 0 if self.ok else 1

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "agent": self.agent,
            "base_url": self.base_url,
            "ok": self.ok,
            "skills": [s.to_dict() for s in self.skills],
        }
        if self.card is not None:
            d["card"] = self.card.to_dict()
        if self.error is not None:
            d["error"] = self.error
        return d


def normalize_state(raw: str) -> str:
    """Match a user-supplied state name to a :class:`TaskState` wire string.

    Accepts the value with or without the ``TASK_STATE_`` prefix, and
    case-insensitively — ``"completed"``, ``"COMPLETED"`` and
    ``"TASK_STATE_COMPLETED"`` all normalize to the same wire string.

    Raises:
        ValueError: ``raw`` does not name any :class:`TaskState` member.
    """
    candidate = raw.strip().upper()
    if not candidate.startswith("TASK_STATE_"):
        candidate = f"TASK_STATE_{candidate}"
    try:
        return str(TaskState(candidate))
    except ValueError:
        raise ValueError(
            f"{raw!r} is not a known task state (e.g. COMPLETED, INPUT_REQUIRED, FAILED)"
        ) from None


def parse_expect_overrides(raw: str | None) -> dict[str, str]:
    """Parse ``--expect skill=STATE,skill=STATE`` into ``{skill: state}``.

    The state names are normalized eagerly (see :func:`normalize_state`) so a
    typo is reported at argument-parsing time, not lost in a per-fixture
    failure later.
    """
    overrides: dict[str, str] = {}
    for item in _split_csv(raw):
        skill, sep, state = item.partition("=")
        if not sep or not skill.strip() or not state.strip():
            raise ValueError(f"expected skill=STATE, got {item!r}")
        overrides[skill.strip()] = normalize_state(state.strip())
    return overrides


def parse_stream_skills(raw: str | None) -> frozenset[str]:
    """Parse ``--stream skill,skill`` into the set of skills to also probe
    with ``SendStreamingMessage``."""
    return frozenset(_split_csv(raw))


def _split_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _load_verify_overrides(agent_path: Path) -> dict[str, dict[str, Any]]:
    """Read ``tests/test-data/verify.json``, or ``{}`` if there is none."""
    path = agent_path / "tests" / "test-data" / VERIFY_OVERRIDES_FILENAME
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerifyError(f"{path}: could not read verify overrides ({exc})") from exc
    if not isinstance(raw, dict):
        raise VerifyError(f"{path}: expected a JSON object keyed by fixture filename")
    return raw


def _first_input_parts(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Read the ACP-era ``input[0].parts`` off a fixture body, tolerantly."""
    input_field = fixture.get("input")
    if isinstance(input_field, list) and input_field:
        first = input_field[0]
        if isinstance(first, dict):
            parts = first.get("parts")
            if isinstance(parts, list):
                return [p for p in parts if isinstance(p, dict)]
    return []


def _skill_from_fixture(fixture: dict[str, Any], manifest_skills: list[str]) -> str | None:
    """Which skill a fixture names, mirroring ``AgentBuilder._resolve_skill``'s
    own precedence for a request naming no skill at all: the fixture's own
    ``skill``, then ``config.skill`` (ACP's Temporal-worker-era placement),
    then the manifest's only skill if it has exactly one, else unresolved."""
    skill = fixture.get("skill")
    if isinstance(skill, str) and skill:
        return skill
    config = fixture.get("config")
    if isinstance(config, dict):
        config_skill = config.get("skill")
        if isinstance(config_skill, str) and config_skill:
            return config_skill
    if len(manifest_skills) == 1:
        return manifest_skills[0]
    return None


def message_from_fixture(fixture: dict[str, Any]) -> tuple[A2AMessage, dict[str, Any] | None]:
    """Convert an ACP-era fixture body into an A2A message plus a NovaOS
    context dict (or ``None``), ready for
    :meth:`~novaos_client_sdk.client.Client.send_message`.

    Each ACP ``parts[]`` entry's ``content``/``content_type`` becomes one
    :class:`TextPart`'s ``text``/``media_type`` — including a part whose
    ``content`` is itself a JSON-encoded string, since the skill handler on
    the other end is what calls ``json.loads`` on it, not this converter.
    """
    raw_parts = _first_input_parts(fixture)
    parts: list[TextPart | FilePart | DataPart] = [
        TextPart(text=p.get("content", ""), media_type=p.get("content_type") or None)
        for p in raw_parts
    ] or [TextPart(text="")]
    message = A2AMessage(parts=parts, role=Role.USER)
    context = fixture.get("context")
    return message, (context if isinstance(context, dict) else None)


def _status_message_text(status: TaskStatus) -> str:
    if status.message is None:
        return ""
    for part in status.message.parts:
        if isinstance(part, TextPart):
            return part.text
    return ""


def _validate_stream(frames: list[StreamFrame]) -> tuple[bool, str]:
    """Acceptance criterion 2: a well-formed stream produces at least one
    frame and closes on a terminal or interrupt frame — never simply runs out
    (the stream-terminality shape).

    Deliberately does not require any particular *opening* frame shape.
    ``AgentBuilder.handle_a2a_streaming_request`` does not always open on a
    ``statusUpdate``: a skill that returns immediately with no interim
    ``update_status`` calls streams its auto-synthesised output artifact
    first, so the opening frame is legitimately a ``TaskArtifactUpdateEvent``.
    :func:`~novaos_client_sdk.a2a_events.is_last_frame` already carries the
    entire invariant that actually matters here — the close, not the open.
    """
    if not frames:
        return False, "stream produced no frames at all"
    if not is_last_frame(frames[-1]):
        return False, "stream closed without a terminal or interrupt frame"
    return True, ""


async def _check_skill(
    client: Client,
    *,
    skill: str,
    fixture: str,
    message: A2AMessage,
    context: dict[str, Any] | None,
    expected_state: str,
    stream: bool,
) -> SkillCheck:
    check = SkillCheck(
        skill=skill, fixture=fixture, expected_state=expected_state, stream_requested=stream
    )
    metadata = {NOVAOS_CONTEXT_METADATA_KEY: context} if context else None

    try:
        result = await client.send_message(message, skill_id=skill, metadata=metadata)
    except NovaOSError as exc:
        check.error = f"{type(exc).__name__}: {exc}"
        return check

    if isinstance(result, Task):
        check.actual_state = str(result.status.state)
        check.detail = _status_message_text(result.status)
    else:
        # The SendMessage oneof's other member: the agent replied without
        # creating a task at all. No fixture expects this today, but it is a
        # legitimate A2A response, not a transport failure — report it as a
        # named state so a caller who *does* expect it can `--expect` it.
        check.actual_state = "MESSAGE_ONLY"

    if stream:
        try:
            frames = [
                f
                async for f in client.send_streaming_message(
                    message, skill_id=skill, metadata=metadata
                )
            ]
        except NovaOSError as exc:
            check.stream_error = f"{type(exc).__name__}: {exc}"
            check.stream_ok = False
        else:
            check.stream_ok, reason = _validate_stream(frames)
            if not check.stream_ok:
                check.stream_error = reason

    return check


@contextlib.contextmanager
def _chdir(path: Path) -> Iterator[None]:
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def check_card(
    agent_path: Path, manifest_skills: list[str], *, target: str = DEFAULT_CARD_TARGET
) -> CardCheck:
    """Import the agent's ``server:agent`` (the same spec
    :mod:`~novaos_client_sdk.agent_card` resolves) and compare
    ``to_agent_card().skills[].id`` against ``manifest.json``'s declared
    skills.

    This is the one part of ``verify`` that imports the agent under test
    rather than calling it over the wire — there is no live endpoint that
    answers "what does your Agent Card say", short of the façade's
    ``/.well-known/agent-card.json``, which is a platform concern this
    agent-directory-scoped check does not assume is running. Reuses
    ``agent_card``'s own target resolution (import-with-timeout, guarded
    against a ``server.py`` that starts a server at import time) rather than
    re-implementing it.
    """
    from .. import agent_card as agent_card_mod

    manifest_set = set(manifest_skills)
    with _chdir(agent_path):
        try:
            agent = agent_card_mod._resolve_target(target)
        except agent_card_mod._EntrypointError as exc:
            return CardCheck(
                ok=False,
                manifest_skills=sorted(manifest_set),
                error=str(exc),
            )
        card = agent.to_agent_card()

    card_skill_ids = sorted(skill.id for skill in card.skills)
    card_set = set(card_skill_ids)
    missing = sorted(manifest_set - card_set)
    extra = sorted(card_set - manifest_set)
    return CardCheck(
        ok=not missing and not extra,
        manifest_skills=sorted(manifest_set),
        card_skills=card_skill_ids,
        missing_from_card=missing,
        extra_on_card=extra,
    )


async def verify_agent(
    agent_path: Path,
    plan: Plan,
    *,
    base_url: str,
    expect_overrides: dict[str, str] | None = None,
    stream_skills: frozenset[str] | None = None,
    timeout: float | None = None,
    check_card_target: str = DEFAULT_CARD_TARGET,
) -> VerifyReport:
    """Convert every fixture named in ``plan.verify_fixtures``, send it to the
    live agent at ``base_url``, and check the card — see the module
    docstring for the full contract.

    Args:
        agent_path: The agent directory ``plan`` was built for. Fixture paths
            in ``plan.verify_fixtures`` are relative to this.
        plan: From ``analyze`` (or ``load_plan`` on its ``--out`` artifact) —
            supplies ``manifest.skills`` and ``verify_fixtures``.
        base_url: Where the migrated agent is actually running.
        expect_overrides: ``{skill: normalized_state}``, from ``--expect`` —
            see :func:`parse_expect_overrides`. Wins over a ``verify.json``
            entry for the same skill.
        stream_skills: Skill names to also probe with
            ``SendStreamingMessage``, from ``--stream`` — see
            :func:`parse_stream_skills`. A skill named here streams even if
            no ``verify.json`` entry says so, and vice versa.
        timeout: Per-request timeout passed to the underlying
            :class:`~novaos_client_sdk.client.Client`.
        check_card_target: The ``MODULE:ATTR`` spec for the card check — see
            :func:`check_card`. The Galaxy scaffold default,
            ``"server:agent"``, is right for every agent this pipeline
            targets; overridable for a differently-laid-out one.

    Raises:
        VerifyError: ``plan.verify_fixtures`` is empty, or ``verify.json``
            exists but is not valid JSON.
    """
    expect_overrides = expect_overrides or {}
    stream_skills = stream_skills or frozenset()

    if not plan.verify_fixtures:
        raise VerifyError(
            f"{agent_path}: no verify fixtures (plan.verify_fixtures is empty — "
            "nothing under tests/test-data/ to convert)"
        )

    fixture_overrides = _load_verify_overrides(agent_path)
    checks: list[SkillCheck] = []

    async with Client(base_url=base_url, timeout=timeout) as client:
        for rel in plan.verify_fixtures:
            if Path(rel).name == VERIFY_OVERRIDES_FILENAME:
                # `analyze`'s fixture glob takes every file under
                # tests/test-data/ — including this module's own overrides
                # file, once one exists. It is configuration, not a request
                # body to convert and send.
                continue

            fixture_path = agent_path / rel
            try:
                raw = json.loads(fixture_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                checks.append(
                    SkillCheck(skill="", fixture=rel, expected_state="", error=f"unreadable: {exc}")
                )
                continue
            if not isinstance(raw, dict):
                checks.append(
                    SkillCheck(
                        skill="",
                        fixture=rel,
                        expected_state="",
                        error="fixture is not a JSON object",
                    )
                )
                continue

            fixture_conf = fixture_overrides.get(Path(rel).name, {})
            skill = fixture_conf.get("skill") or _skill_from_fixture(raw, plan.manifest.skills)
            if not skill:
                checks.append(
                    SkillCheck(
                        skill="",
                        fixture=rel,
                        expected_state="",
                        error=(
                            "fixture names no skill, and the manifest has "
                            f"{len(plan.manifest.skills)} candidates — set 'skill' in "
                            "verify.json or the fixture's own 'skill'/'config.skill'"
                        ),
                    )
                )
                continue

            expected_raw = (
                expect_overrides.get(skill) or fixture_conf.get("expected_state") or "COMPLETED"
            )
            try:
                expected_state = normalize_state(expected_raw)
            except ValueError as exc:
                checks.append(
                    SkillCheck(
                        skill=skill, fixture=rel, expected_state=expected_raw, error=str(exc)
                    )
                )
                continue

            stream = skill in stream_skills or bool(fixture_conf.get("stream"))
            message, context = message_from_fixture(raw)
            checks.append(
                await _check_skill(
                    client,
                    skill=skill,
                    fixture=rel,
                    message=message,
                    context=context,
                    expected_state=expected_state,
                    stream=stream,
                )
            )

    card = check_card(agent_path, plan.manifest.skills, target=check_card_target)
    return VerifyReport(agent=plan.agent, base_url=base_url, skills=checks, card=card)
