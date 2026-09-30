"""
AgentBuilder — The main entry point for building NovaOS agents.

Usage:
    from novaos_client_sdk import AgentBuilder, AgentExecutionContext, SkillResult

    agent = AgentBuilder(name="my-agent", version="1.0.0")

    @agent.skill("greet", description="Greet a user")
    async def greet(ctx: AgentExecutionContext, input_text: str) -> SkillResult:
        return SkillResult(output=f"Hello!")

Ported from NovaOs/packages/agent-sdk/python/novaos_sdk/agent.py
Renamed from Agent to AgentBuilder to avoid conflict with the existing
novaos_client_sdk.models.Agent model class.

**Protocol: A2A v1.0.1, single-protocol.**  ``handle_a2a_request`` replaced the
ACP-era ``handle_acp_request`` outright, reflecting a deliberate migration
decision.  There is deliberately no ACP compatibility path and no
dual-protocol serving, for two reasons that decision settles: BeeAI's
successor SDK ships no ACP-compatibility shim, so an agent cannot honestly
serve both; and NovaOS took an explicit product decision not to stay backward
compatible with ACP.  A pod running this handler speaks A2A and nothing else,
which is why it must reach production in the same coordinated cutover as the
Temporal worker's A2A rewrite.

**This module serves no Agent Card.**  Agent pods are never
internet-reachable, so ``/.well-known/agent-card.json`` is hosted by the
platform's execution gateway on the agent's behalf.  Emitting one here
would only be visible to callers already inside the cluster.

**Dependency direction (DIP).**  Everything below is typed against NovaOS's own
abstractions — ``a2a_types``, ``a2a_task``, ``a2a_jsonrpc``, ``agent_context``,
``agent_types``.  No vendor A2A/ACP library is imported, and none should be: an
agent's skill handlers are business logic and must not have a vendor wire type
in their signature.  ``agentstack-sdk`` and ``a2a-sdk`` are both absent from
this module's imports by design, not by accident.

**Skill dispatch: a `Task` for cancellability, an explicit `context=` for
`ContextVar` propagation.**  Both ``_execute_skill`` and
``handle_a2a_streaming_request`` run a skill handler via
``asyncio.create_task(handler(...), context=...)`` rather than a plain
``await handler(...)``.  The ``Task`` exists solely so ``CancelTask``
has something to ``.cancel()`` — a bare ``await`` gives a concurrent
cancellation nothing to interrupt.  That ``Task`` would otherwise run inside
its *own* copy of the ambient ``contextvars`` context (``asyncio``'s default
for any new ``Task``), so a skill body's ``ContextVar.set(...)`` would mutate a
throwaway copy invisible to the caller the moment ``handle_a2a_request``
returns — exactly the kind of regression an earlier version of this module
introduced and later fixed.  The fix is
to capture ``contextvars.copy_context()`` immediately before creating the
task, pass it in as that task's ``context``, and — once the task is done,
however it finished — copy every var in that context back onto the ambient
one the caller resumes in.  Both constraints hold at once: the skill is still
cancellable (cancelling a ``Task`` is independent of which context it runs
in), and a ``ContextVar.set()`` inside the skill body is visible again once
``handle_a2a_request``/``handle_a2a_streaming_request`` returns.  Reverting to
an inline ``await`` — trading this regression back for the earlier one — is
not an acceptable fix for either problem.

**Interruption policy: which interruptions end a task, and which leave it
running.**  A skill's execution can be cut short three ways, and the
Temporal worker depends on the store answering each one truthfully, so the
contract is stated here rather than left implicit in the ``except`` clauses:

1. **An explicit ``CancelTask``** — the one and only interruption that
   *is* a decision about the task.  ``_dispatch_cancel_task`` writes
   ``TASK_STATE_CANCELED`` first, then preempts the running skill.  The
   dispatchers below never overwrite that write, however the skill's
   coroutine ends afterward.
2. **The skill's own coroutine ends in an ``asyncio.CancelledError`` no
   ``CancelTask`` asked for** — the skill raised it itself, or something it
   awaited did.  The harness writes **nothing**: the task stays at whatever
   state the skill last wrote (``WORKING``, typically), the SSE response
   closes cleanly with no terminal frame, and ``SendMessage`` answers with
   the task's current, non-terminal snapshot.  This is A2A's "stream ends
   with no terminal event" shape, which a scaffold-based
   ``a2a-sdk`` executor produces by simply returning without publishing one.
   A prior version of this module labelled it ``CANCELED`` and let the error
   propagate out of the response generator, which Starlette's task group
   swallowed as a cancellation — so the ASGI callable returned without ever
   completing its response (``uvicorn``: "ASGI callable returned without
   completing response"; the client: ``httpx.RemoteProtocolError``).  Both
   halves were wrong: the label mistook a skill giving up for a caller
   asking it to, and the abort made the shape impossible to produce on
   purpose — the fleet conformance probe exists to test the platform's
   handling of exactly this shape, and could not.
3. **The request is abandoned while the skill runs** — the client
   disconnected, the ASGI request task was cancelled, or the response
   generator was closed.  This is not a decision about the task either: an
   A2A task is a server-side resource that outlives any one request to it
   (spec §3.1.6 gives ``SubscribeToTask`` precisely so a caller can
   reconnect), and the platform's own worker never treated a dropped
   connection as a cancel signal — it stops *watching* the task, then
   resubscribes or deletes the pod outright.  So the
   skill is **not** stopped and nothing terminal is written.  The execution
   is detached (:meth:`AgentBuilder._detach_execution`): the skill's
   ``Task`` runs on, and a finisher task publishes its eventual outcome —
   ``COMPLETED``/``FAILED``/whatever the ``SkillResult`` maps to, or nothing
   under rule 2, or nothing under rule 1 if a ``CancelTask`` reaches it
   meanwhile — through the same ``TaskUpdater``/``EventQueue`` the request
   would have, then closes the queue and unregisters both.  A ``GetTask``
   in between reports ``WORKING``; a ``SubscribeToTask`` attaches to the
   still-registered queue and sees the terminal frame when it comes.
   ``CancelTask`` remains the way to actually stop a detached skill — and
   so does **re-sending on the same task id**: a ``SendMessage`` /
   ``SendStreamingMessage`` naming a task whose execution is still live
   (detached or not) supersedes it (:meth:`AgentBuilder._supersede_live_execution`)
   before re-seeding — preempting the earlier skill and revoking its
   ``TaskUpdater`` — so a caller that disconnected and retried never ends
   up with two skills writing to one task.  A prior version
   cancelled the skill and wrote ``CANCELED`` here too — which turned every
   worker restart and every client timeout into a task that claimed it had
   been cancelled by someone.

Rule 2 is the "stream ends with no terminal event" shape and the reason a
probe can now produce it; rule 3 is what keeps the store truthful while
nobody is watching.  Rule 1 is unchanged from the original cancellation
behavior.
"""

import asyncio
import contextvars
import json
import logging
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import replace
from datetime import datetime
from typing import Any, Final
from uuid import uuid4

from ._a2a import sse_data_line
from .a2a_agent_card import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentProvider,
    AgentSkill,
)
from .a2a_artifact import Artifact
from .a2a_event_queue import EventQueue, TaskEvent
from .a2a_events import TaskArtifactUpdateEvent, TaskStatusUpdateEvent
from .a2a_extensions import A2AExtension, ExtensionActivation, ExtensionRegistry
from .a2a_jsonrpc import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    METHOD_CANCEL_TASK,
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_GET_TASK,
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS,
    METHOD_LIST_TASKS,
    METHOD_NOT_FOUND,
    METHOD_SEND_MESSAGE,
    METHOD_SEND_STREAMING_MESSAGE,
    METHOD_SUBSCRIBE_TO_TASK,
    decode_request_body,
    error_response,
    parse_request,
    send_message_result,
    success_response,
)
from .a2a_push_notification import TaskPushNotificationConfig, TaskPushNotificationConfigList
from .a2a_push_notification_store import PushNotificationConfigStore
from .a2a_task import (
    INTERRUPT_TASK_STATES,
    TERMINAL_TASK_STATES,
    Task,
    TaskList,
    TaskState,
    TaskStatus,
    task_state_from_wire,
    utc_timestamp,
)
from .a2a_task_store import InMemoryTaskStore, TaskStore
from .a2a_task_updater import TaskUpdater
from .a2a_types import (
    A2AMessage,
    DataPart,
    Part,
    Role,
    TextPart,
    unsupported_version_reason,
)
from .agent_context import (
    AgentExecutionContext,
    _CancellationToken,
    as_dict,
    as_str,
    novaos_context_from_metadata,
)
from .agent_types import _SKILL_STATUS_TO_TASK_STATE, SkillResult
from .dns import validate_agent_url
from .exceptions import (
    ContentTypeNotSupportedError,
    ExtensionSupportRequiredError,
    NovaOSError,
    PushNotificationNotSupportedError,
    TaskNotCancelableError,
    TaskNotFoundError,
    UnsupportedOperationError,
    VersionNotSupportedError,
)
from .pii import mask_pii, mask_pii_in_dict
from .tracing import _HAS_OTEL, _get_tracer

logger = logging.getLogger(__name__)

# ⚠️ MUST match message_builder.NOVAOS_METADATA_CONTENT_TYPE (message_builder.py).
#    temporal-worker's tryExtractNovaosMetadata checks for this exact string —
#    any variation (e.g. "application/json+novaos.metadata") causes silent skip.
#    Defined here (not imported) to avoid the message_builder ↔ messages circular
#    import: messages.py imports message_builder at the bottom of the module, so
#    importing message_builder from here would re-enter a partially-initialised
#    module.
#    Exported publicly (re-exported from novaos_client_sdk.__init__) so downstream
#    Python consumers reference this constant instead of retyping the literal.
#    tests/test_agent_builder.py::test_matches_message_builder_constant
#    asserts the two constants stay identical — treat a failure there as a P0.
NOVAOS_METADATA_CONTENT_TYPE = "application/vnd.novaos.metadata+json"

# ⚠️ Same duplication, same reason, same enforcing test.  A2A's DataPart has no
#    content-type field of its own (unlike ACP's MessagePart), so the vendor
#    content-type above is carried as the value of this key in the part's own
#    metadata.  See message_builder.NOVAOS_METADATA_PART_KEY.
NOVAOS_METADATA_PART_KEY = "novaos.platform/content-type"

# Reserved key inside the NovaOS metadata part carrying SkillResult.error.  The
# error also appears in the status message's text when there is nothing else to
# show, but text is for humans — this is the field a caller reads to branch.
NOVAOS_SKILL_ERROR_KEY = "skill_error"

# ⚠️ Vendor content-type for a skill's own
#    result, folded in from ``SkillResult.metadata`` by ``_agent_message``.
#    Deliberately distinct from ``NOVAOS_METADATA_CONTENT_TYPE`` above: that
#    marker means "platform metadata" (HITL, error, progress), and
#    temporal-worker's ``drainParts`` never surfaces a part carrying it as
#    caller-visible ``data`` — which meant a skill returning
#    ``SkillResult(output=…, metadata={…})`` had its ``metadata`` silently
#    excluded from ``GET /api/v1/service/runs/{id}`` before this constant
#    existed, while the same data returned via a manual, untagged
#    ``ctx.task_updater.add_artifact(...)`` call surfaced normally. This marker
#    gives the fold-in path its own identity so the worker can tell "the
#    platform's business" apart from "the skill's own result".
#    Must match the platform's A2A wire contract's
#    `NOVAOS_SKILL_METADATA_CONTENT_TYPE` — tests/test_agent_builder.py has no
#    cross-repo enforcement (see the constant above's own comment on why), so
#    a value changing on either side without the other is the same P0 class.
NOVAOS_SKILL_METADATA_CONTENT_TYPE = "application/vnd.novaos.skill-metadata+json"

# Artifact id of the artifact carrying a skill's output.
#
# Stable rather than a fresh UUID per task, and that is the point: A2A identifies
# an artifact by its id, so a stable one is what lets a later revision of the
# same output be recognised as an update instead of a second, unrelated result
# (see Artifact.revised).  The spec only requires uniqueness *within* a task, so
# reusing the string across tasks is conformant.  Exported so a consumer — the
# shared agent test harness — matches on this constant instead of
# retyping the literal.
OUTPUT_ARTIFACT_ID = "output"

# Message-metadata key a caller uses to choose which skill runs.
#
# ⚠️ This literal is shared with the Galaxy agent scaffold, which resolves the
#    same key in its own SkillRegistry (galaxy-create-agent,
#    templates/server/skills.py::SKILL_ID_KEY).  A2A has no dedicated "which
#    skill" field on SendMessage, so both pod implementations — this builder and
#    the scaffold's vendor-SDK executor — must agree on where the selector
#    lives, or the Temporal worker would need to know which kind of pod it is
#    calling.  Do not rename one side alone.
SKILL_ID_METADATA_KEY = "skillId"

#: Default page size for ``ListTasks`` and ``ListTaskPushNotificationConfigs``
#: when the caller does not supply ``pageSize``.  A2A leaves the
#: default to the agent; this is small enough that an agent holding
#: :data:`~novaos_client_sdk.a2a_task_store.DEFAULT_MAX_TASKS` tasks does not
#: hand a caller its entire backlog in one unbounded response.
DEFAULT_LIST_TASKS_PAGE_SIZE = 50

#: Upper bound on ``pageSize`` for the same two methods. Before
#: this existed, any positive integer was honoured verbatim, so a single
#: ``pageSize: 1000000`` request returned the whole store — up to
#: :data:`~novaos_client_sdk.a2a_task_store.DEFAULT_MAX_TASKS` tasks, each with
#: full history — in one response: a real amplification from one small
#: request, even though it is bounded rather than unbounded.
#:
#: **Clamp, not reject.** The reference A2A Python SDK (``a2a.utils.task``'s
#: ``validate_page_size``, bounded by ``a2a.utils.constants``'s
#: ``MAX_LIST_TASKS_PAGE_SIZE = 100`` — matched here) answers an oversized
#: ``pageSize`` with ``-32602 INVALID_PARAMS`` instead of a smaller page. This
#: SDK clamps to this maximum and serves that page instead. That is a
#: deliberate divergence, not an oversight: every other malformed ``pageSize``
#: on these two methods already degrades to something usable rather than
#: failing the call — a non-int, a bool, zero, or a negative value all fall
#: back to :data:`DEFAULT_LIST_TASKS_PAGE_SIZE` above, and a stale
#: ``pageToken`` degrades to starting over rather than erroring (see
#: :meth:`AgentBuilder._dispatch_list_tasks`'s own docstring). Rejecting only
#: an oversized ``pageSize`` while every sibling bad-input case is forgiven
#: would be the one inconsistent check in an otherwise uniformly permissive
#: family, for a caller who is, if anything, asking for more help than a
#: caller who supplies garbage. A caller that asks for too much still gets a
#: valid, capped page rather than an error it has to retry around.
MAX_LIST_TASKS_PAGE_SIZE = 100

#: The output media types a skill is treated as producing when neither the
#: skill itself nor the agent declares any. A2A leaves an
#: agent free to choose its own defaults the way it leaves ``ListTasks``'
#: page size undefined (see :data:`DEFAULT_LIST_TASKS_PAGE_SIZE`'s own
#: reasoning) — this is deliberately permissive rather than restrictive,
#: since an agent that never called ``@agent.skill(..., output_modes=...)``
#: at all should not have every request start failing content negotiation
#: the moment a caller starts sending ``acceptedOutputModes``. Plain text and
#: structured JSON cover what every skill in this SDK can already produce
#: (:meth:`AgentBuilder._agent_message` always emits a ``TextPart``, plus a
#: ``DataPart`` when a skill returns ``metadata``), so declaring them
#: supported by default costs nothing and is never wrong to advertise.
_DEFAULT_OUTPUT_MODES: Final[tuple[str, ...]] = ("text/plain", "application/json")

#: The JSON-RPC methods this entry point (``handle_a2a_request_with_headers``)
#: actually serves.  ``SendStreamingMessage`` and ``SubscribeToTask`` are real
#: v1.0 operations this agent implements, but *not here* — both have a result
#: shape this method cannot return (an SSE byte stream) and are served by
#: :meth:`AgentBuilder.handle_a2a_streaming_request` instead, so they are
#: declined with A2A's own ``-32004`` rather than counted as "unknown".
_KNOWN_JSON_ONLY_METHODS: tuple[str, ...] = (
    METHOD_SEND_MESSAGE,
    METHOD_GET_TASK,
    METHOD_CANCEL_TASK,
    METHOD_LIST_TASKS,
    METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
    METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS,
    METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
)

#: The four ``*TaskPushNotificationConfig`` methods, which share one dispatch
#: method (:meth:`AgentBuilder._dispatch_push_notification_config`) since all
#: four are gated by the exact same ``-32003`` check.
_PUSH_NOTIFICATION_CONFIG_METHODS: frozenset[str] = frozenset(
    {
        METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG,
        METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
        METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS,
        METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
    }
)


def _is_nonempty_str(value: Any) -> bool:
    """Is ``value`` a non-empty string?  The shape every new task-identifier param needs.

    ``bool`` is deliberately not special-cased — a JSON boolean can never
    equal a JSON string under ``isinstance(value, str)``, so there is no
    ``isinstance(x, bool)`` trap here the way there is for the numeric params
    below (``True == 1`` is an ``int`` gotcha; it is not a ``str`` one).
    """
    return isinstance(value, str) and value != ""


def _parse_rfc3339(value: str) -> datetime | None:
    """Parse an RFC 3339 timestamp, or return ``None`` if it doesn't parse.

    Used by ``ListTasks``' ``statusTimestampAfter`` filter to compare two
    timestamps *as instants*, not as strings. A plain string comparison
    looks correct for two timestamps of identical width, and
    ``utc_timestamp()`` (``a2a_task.py``) now guarantees that width for
    every stored task — but ``statusTimestampAfter`` itself is a
    caller-supplied cutoff with no such guarantee: a caller may send RFC
    3339 in a different, still-valid width (no fractional seconds, a
    different number of fractional digits, ``+00:00`` instead of ``Z``),
    and comparing that against our own fixed-width stored timestamps
    lexicographically would reintroduce exactly the ordering bug fixing
    ``utc_timestamp()`` alone does not. Parsing both sides
    into real ``datetime`` instances compares them correctly regardless of
    either one's exact formatting.

    ``datetime.fromisoformat`` (3.11+) already accepts a trailing ``Z`` and
    any number of fractional-second digits, so no manual normalization is
    needed before parsing.

    **A timezone-less value is treated as unparseable, not as naive UTC.**
    ``fromisoformat`` happily parses ``"2024-01-01T00:00:00"`` (no ``Z`` or
    offset) into a naive ``datetime`` — but every stored task timestamp is
    timezone-*aware* (``utc_timestamp()`` always has one), and comparing a
    naive and an aware ``datetime`` with ``<``/``>`` raises ``TypeError``,
    crashing the whole ``ListTasks`` call over one caller-supplied field.
    RFC 3339 itself requires an offset, so a value missing
    one is non-conformant input; degrading it the same way a value that
    fails to parse at all degrades — as "unusable, don't filter on it" — is
    consistent rather than a special case.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


_HAS_EVAL = False
try:
    from .evaluation.context import EvaluationContext, _reset_eval_context, _set_eval_context

    _HAS_EVAL = True
except ImportError:
    pass


def _resolve_skill(
    message: A2AMessage,
    params: dict[str, Any],
    skills: dict[str, dict[str, Any]],
) -> tuple[str | None, str | None]:
    """Resolve which skill an A2A request should run.

    Returns ``(skill_name, rejection_reason)``.  Exactly one of the two is
    non-None.  A rejection is a *reason string* rather than a response object
    because the caller decides how to render it — here, as a task in
    ``TASK_STATE_REJECTED``.

    Routing rules, highest precedence first:
      1. ``message.metadata["skillId"]`` — the canonical A2A selector, and the
         one the Galaxy scaffold's own dispatcher reads.
      2. ``params["metadata"]["skillId"]`` — the same selector set at the request
         level instead of on the message.  Accepted so neither placement is a
         silent no-op.
      3. ``config.skill`` inside the NovaOS platform context — preserves the
         Temporal worker's existing per-run ``config`` plumbing, which carried
         the skill name under ACP and has no reason to move.
      4. The only registered skill, when the agent has exactly one.

    A multi-skill agent that gets no selector is **rejected, not guessed at**.
    Falling back to whichever skill was registered first has been observed
    as a real defect elsewhere in NovaOS, where a multi-skill agent silently
    answers with the wrong skill and nothing in the response says so.  A clear
    rejection is recoverable; a confident wrong answer is not.
    """
    if not skills:
        return None, "This agent has no registered skills."

    def _lookup(name: Any) -> tuple[str | None, str | None, bool]:
        """Return ``(skill, error, matched)``; ``matched`` is False to fall through."""
        if not isinstance(name, str) or not name:
            return None, None, False
        if name in skills:
            return name, None, True
        return (
            None,
            f"Unknown skill '{name}'. This agent exposes: {', '.join(skills)}.",
            True,
        )

    novaos_context = novaos_context_from_metadata(params.get("metadata"), message.metadata)
    # as_dict, not `or {}` — a truthy non-object (a list, a string) would sail
    # through `or {}` and raise AttributeError on the next .get(), surfacing as a
    # JSON-RPC INTERNAL_ERROR instead of the rejected task this function promises.
    candidates = (
        as_dict(message.metadata).get(SKILL_ID_METADATA_KEY),
        as_dict(params.get("metadata")).get(SKILL_ID_METADATA_KEY),
        as_dict(novaos_context.get("config")).get("skill"),
    )

    for candidate in candidates:
        skill, error, matched = _lookup(candidate)
        if matched:
            return skill, error

    if len(skills) == 1:
        return next(iter(skills)), None

    return None, (
        f"This agent exposes {len(skills)} skills ({', '.join(skills)}) and the request "
        f"named none. Set metadata.{SKILL_ID_METADATA_KEY} on the message to choose one."
    )


async def _maybe_prefetch_memory(ctx: AgentExecutionContext, input_text: str) -> None:
    """Pre-fetch memory context before a skill runs, at most once per turn.

    A no-op unless the platform attached a ``memory_budget`` to this
    request's ``novaos.platform/context`` — see :attr:`AgentExecutionContext.memory_budget`'s
    own docstring for why that presence, not a separate capability flag
    living in this SDK, is what gates the call. Called once, before the
    skill's handler starts, from both :meth:`AgentBuilder._send_message` and
    :meth:`AgentBuilder._stream_send_message` — never from inside a skill —
    so a retried or cancelled-and-resumed turn cannot fetch twice.

    **Never fails the turn.** A skill that would otherwise have everything it
    needs must not fail because pre-fetch adds a dependency on the Memory
    Service: a pre-fetch error (missing token, service down, request
    rejected) is logged and swallowed, leaving :attr:`AgentExecutionContext.memory`'s
    ``context`` at its default ``None`` — a skill reading it already has to
    handle "no pre-fetched context" as the common case for an agent that
    never declares the ``memory`` capability at all, so this degrades to the
    same shape.

    On success, sets :attr:`AgentExecutionContext.memory`'s ``context`` and
    emits exactly one ``memory.assembled`` platform event via
    :meth:`AgentExecutionContext._emit_platform_event` — delivered live on
    the streaming transport (the same channel ``tool.call``/``tool.result``
    already use); buffered on the non-streaming transport, which has no
    mid-execution channel of its own for either.
    """
    budget = ctx.memory_budget
    if budget is None:
        return

    try:
        async with ctx.memory.client() as mem:
            result = await mem.assemble_context(
                input_text,
                token_budget=budget.get("token_budget"),
                latency_budget_ms=budget.get("latency_budget_ms"),
                ctx=ctx,
            )
    except Exception:
        ctx.logger.warning(
            "Memory pre-fetch failed; continuing without pre-fetched context.",
            exc_info=True,
        )
        return

    ctx.memory.context = result
    ctx._emit_platform_event(
        "memory.assembled",
        {
            "retrieval_id": result.retrieval_id,
            "partial": result.partial,
            "sources_consulted": result.sources_consulted,
            "sources_skipped": result.sources_skipped,
        },
    )


class AgentBuilder:
    """
    NovaOS Agent. Register skills with @agent.skill() decorator.
    Handle A2A requests with agent.handle_a2a_request().
    """

    def __init__(
        self,
        name: str,
        version: str = "0.1.0",
        *,
        mask_output_pii: bool = False,
        extensions: Iterable[A2AExtension] | None = None,
        task_store: TaskStore | None = None,
        push_notification_store: PushNotificationConfigStore | None = None,
        default_output_modes: Iterable[str] | None = None,
    ):
        """Initialize the agent.

        Args:
            name: Agent name.
            version: Agent version.
            task_store: Where ``SendMessage``-created tasks are persisted
                between the moment a skill starts and the moment something
                (``GetTask``/``CancelTask``/``ListTasks``) looks
                one up again. Defaults to a fresh
                :class:`~novaos_client_sdk.a2a_task_store.InMemoryTaskStore`,
                which is sufficient for a pod whose lifetime is one execution —
                see that class's docstring for its eviction policy. Injectable
                so a fleet-mode pod handling many runs, or a test, can supply
                its own instance instead.
            push_notification_store: Where this agent's push notification
                registrations are persisted — the ``Create`` /
                ``Get`` / ``List`` / ``Delete`` ``TaskPushNotificationConfig``
                quartet's counterpart to ``task_store``.  Defaults to
                ``None``, and that default is load-bearing rather than a mere
                placeholder: a pod that does not pass a store does not support
                push notification configuration *at all* — every one of the
                four methods answers A2A's own ``-32003``
                (:class:`~novaos_client_sdk.exceptions.PushNotificationNotSupportedError`)
                rather than a mix of "some of the four work". See
                :attr:`supports_push_notifications`, which is what a
                card-generating caller reads to set
                ``capabilities.pushNotifications`` — this class does not
                generate or serve an Agent Card itself, only exposes
                the fact a card generator needs.  Pass
                :class:`~novaos_client_sdk.a2a_push_notification_store.InMemoryPushNotificationConfigStore`
                (or your own implementation) to opt in.
            mask_output_pii: When ``True``, run the same PII masking used by
                the structured logger (``novaos_client_sdk.pii.mask_pii`` /
                ``mask_pii_in_dict``) over the outbound A2A task —
                ``SkillResult.output``, ``.metadata``, and ``.error`` — before
                it's serialized onto the wire. Defaults to ``False`` because
                masking is not safe to apply unconditionally: an agent whose
                job is to echo back or confirm caller-supplied data (e.g. "I'll
                send the confirmation to 555-123-4567") would have that
                legitimate output redacted too. Enable this only for agents
                whose output should never legitimately contain PII patterns.
                PII masking has historically only covered structured logging —
                this flag is the SDK-provided way to extend the same
                protection to agent output, rather than each agent hand-rolling
                its own masking before returning ``SkillResult``.
            extensions: A2A protocol extensions this agent understands.
                Stating them at construction is deliberate: what an
                agent declares on its card should be a property of how it was
                built, not of which modules happened to be imported.  See
                :meth:`register_extension` for the incremental form and
                :meth:`declared_extensions` for what gets published.
            default_output_modes: The media types this agent produces when a
                skill declares no ``output_modes`` of its own.
                The single source of truth for both :meth:`to_agent_card`'s
                published ``defaultOutputModes`` and dispatch-time content
                negotiation (:meth:`_supported_output_modes`) — set it here,
                once, rather than passing it again to every ``to_agent_card()``
                call, so the two cannot silently disagree about what a given
                skill accepts. Defaults to :data:`_DEFAULT_OUTPUT_MODES`
                (``text/plain``, ``application/json``) when neither this nor a
                skill's own ``output_modes`` says otherwise.

        Raises:
            ValueError: If two extensions claim the same URI, or one declares
                no URI at all.
        """
        self.name = name
        self.version = version
        self._skills: dict[str, dict[str, Any]] = {}
        self._mask_output_pii = mask_output_pii
        self._extensions = ExtensionRegistry(extensions or ())
        self._default_output_modes: tuple[str, ...] = tuple(default_output_modes or ())
        self._task_store: TaskStore = task_store if task_store is not None else InMemoryTaskStore()
        self._push_notification_store = push_notification_store
        # The `EventQueue` currently driving each in-flight task,
        # keyed by task_id. This is what lets a concurrent `SubscribeToTask`
        # call attach to a task that is still executing inside a still-open
        # `SendMessage`/`SendStreamingMessage` call in this same process,
        # instead of seeing nothing until that call returns. Must only ever
        # hold *open* queues: once a call returns its queue is closed and
        # useless to a subscriber, so `_send_message`/`_stream_send_message`
        # pop their own entry in their `finally` block — not just on the
        # happy path, since an exception must not leave a stale, closed queue
        # registered for a subscriber to wait on forever. The one
        # exit that does *not* pop is an abandoned request whose skill is
        # still running — ownership of the entry passes to the detached
        # finisher (`_finish_detached_execution`), which pops it once the
        # skill's outcome has actually been published.
        self._active_event_queues: dict[str, EventQueue] = {}
        # The cancellation signal for each in-flight execution,
        # keyed by task_id. Registered before the skill starts (so `CancelTask`
        # can find it even in the narrow window before the skill's own task
        # exists — see `_CancellationToken`'s docstring) and popped in
        # `_send_message`/`_stream_send_message`'s `finally` block, mirroring
        # `_active_event_queues` above (and, like it, handed to the detached
        # finisher instead when the request is abandoned mid-skill — so a
        # `CancelTask` can still reach a skill nobody is watching any more).
        self._active_cancellations: dict[str, _CancellationToken] = {}
        # Strong references to every finisher task currently
        # publishing the outcome of an execution whose request was abandoned
        # (see the module docstring's "Interruption policy", rule 3). asyncio
        # holds only weak references to tasks, so without this set a finisher
        # could be garbage-collected mid-flight and the task it was finishing
        # would stay WORKING forever — the exact outcome detaching exists to
        # prevent. Entries remove themselves on completion.
        self._detached_executions: set[asyncio.Task[None]] = set()

    @property
    def task_store(self) -> TaskStore:
        """Where this agent's ``SendMessage``-created tasks are persisted."""
        return self._task_store

    @property
    def supports_push_notifications(self) -> bool:
        """Whether this agent *records* push notification config, not whether anything delivers it.

        ``True`` exactly when a
        :class:`~novaos_client_sdk.a2a_push_notification_store.PushNotificationConfigStore`
        was supplied at construction, mirroring :attr:`task_store`'s always-
        present default. That is the *entire* claim: the four
        ``*TaskPushNotificationConfig`` methods will accept, persist, and read
        back a registration instead of answering A2A's ``-32003``. It says
        nothing about whether a webhook is ever actually dialled — this SDK
        has no sender (confirmed by investigation: one does not
        exist here, deliberately: delivery is the platform gateway's job).
        Whether a given deployment delivers depends on whether
        that gateway sits in front of this pod, which this class has no way
        to know.

        The platform's gateway/card-generation layer is the
        intended reader — it sets this pod's Agent Card
        ``capabilities.pushNotifications`` from this property, so the card
        cannot drift from what ``handle_a2a_request_with_headers`` actually
        serves *at this layer*. A caller reading that flag as "push
        notifications work end-to-end" is reading more into it than this
        property, or the methods it gates, ever promised. This class never
        generates or serves a card itself; it only exposes the fact
        a card generator needs.
        """
        return self._push_notification_store is not None

    def register_extension(self, extension: A2AExtension) -> A2AExtension:
        """Register one A2A protocol extension and return it.

        The incremental counterpart of the ``extensions=`` constructor argument,
        for an agent assembled in pieces the way ``@agent.skill`` assembles its
        skills.

        Args:
            extension: The extension to add.

        Returns:
            The extension, unchanged.

        Raises:
            ValueError: If it declares no URI, or another extension already
                claimed the one it declares.
        """
        return self._extensions.register(extension)

    @property
    def extensions(self) -> ExtensionRegistry:
        """The A2A extensions this agent understands."""
        return self._extensions

    def declared_extensions(self) -> list[AgentExtension]:
        """The ``capabilities.extensions[]`` array for this agent's Agent Card.

        **This agent does not serve its own card** — pods are not
        internet-reachable, so the platform's execution gateway publishes it
        and the Galaxy scaffold generates card content at build time.  This
        returns the array for whichever of them is doing the publishing, so a
        declaration cannot drift from the registry that will actually negotiate
        it at request time.
        """
        return self._extensions.declarations()

    def skill(
        self,
        name: str,
        description: str = "",
        *,
        id: str | None = None,
        tags: Iterable[str] | None = None,
        input_modes: Iterable[str] | None = None,
        output_modes: Iterable[str] | None = None,
    ) -> Callable:
        """Register a skill handler.

        Args:
            name: The skill's display name (``AgentSkill.name`` on the card
                built by :meth:`to_agent_card`).
            description: Human-readable description of what the skill does.
            id: The routing selector a caller sends as
                :data:`SKILL_ID_METADATA_KEY` to pick this skill, and the
                value :meth:`to_agent_card` publishes as ``AgentSkill.id``.
                Defaults to ``name``, so every existing
                ``@agent.skill("name", "description")`` call keeps working
                unchanged.  This agent's skills are keyed by whatever value
                ends up here — the same dict :func:`_resolve_skill` looks up
                at request time — so a card built by :meth:`to_agent_card`
                can never advertise an id this agent would reject.
            tags: Free-form category tags, for ``AgentSkill.tags``.
            input_modes: Media types this skill accepts, for
                ``AgentSkill.inputModes``.
            output_modes: Media types this skill produces, for
                ``AgentSkill.outputModes``.
        """

        def decorator(func: Callable) -> Callable:
            skill_id = id if id is not None else name
            self._skills[skill_id] = {
                "handler": func,
                "name": name,
                "description": description,
                "tags": list(tags) if tags is not None else [],
                "input_modes": list(input_modes) if input_modes is not None else [],
                "output_modes": list(output_modes) if output_modes is not None else [],
            }
            return func

        return decorator

    def _supported_output_modes(
        self, skill_id: str, *, default_output_modes: Iterable[str] | None = None
    ) -> list[str]:
        """The output media types ``skill_id`` is treated as producing.

        The single source of truth for what :meth:`to_agent_card` publishes as
        a skill's ``outputModes`` *and* what dispatch-time content negotiation
        (``configuration.acceptedOutputModes`` on ``SendMessage`` /
        ``SendStreamingMessage``) enforces against — both now call this method
        rather than each re-deriving the same precedence independently, which
        is how a card and its enforcement drift apart in the first place (the
        same class of defect ``test_every_advertised_skill_id_actually_routes``
        already guards for skill routing).

        Precedence: the skill's own declared ``output_modes`` (:meth:`skill`'s
        ``output_modes=``) if it registered any; else ``default_output_modes``
        if the caller passed one; else :attr:`_default_output_modes`, this
        agent's own construction-time default; else
        :data:`_DEFAULT_OUTPUT_MODES`. A skill's declaration always wins
        outright rather than being unioned with any default — the same
        "explicit beats configured, never merged" rule
        :meth:`~novaos_client_sdk.client.Client._apply_config` already uses for
        the client-side ``accepted_output_modes`` precedence.

        Args:
            skill_id: The skill to resolve, keyed exactly as :meth:`skill`
                registered it.
            default_output_modes: An override for :attr:`_default_output_modes`,
                used only by :meth:`to_agent_card` so a caller of *that* method
                can still publish a card for a different default without
                touching this instance's own. Dispatch never passes one — it
                always resolves against :attr:`_default_output_modes`, which is
                what keeps the common (no-override) card and dispatch's own
                enforcement identical *by construction* rather than by both
                happening to agree today.

        Returns:
            A non-empty list of media types.
        """
        declared = list(self._skills.get(skill_id, {}).get("output_modes") or ())
        if declared:
            return declared
        defaults = list(
            default_output_modes if default_output_modes is not None else self._default_output_modes
        )
        return defaults if defaults else list(_DEFAULT_OUTPUT_MODES)

    def to_agent_card(
        self,
        *,
        description: str = "",
        supported_interfaces: Iterable[AgentInterface] | None = None,
        streaming: bool = True,
        provider: AgentProvider | None = None,
        documentation_url: str | None = None,
        icon_url: str | None = None,
        security_schemes: dict[str, Any] | None = None,
        security_requirements: list[dict[str, Any]] | None = None,
        default_input_modes: Iterable[str] | None = None,
        default_output_modes: Iterable[str] | None = None,
    ) -> AgentCard:
        """The Agent Card content for this agent.

        **This agent still serves no card of its own.**  As
        this module's own docstring explains, agent pods are never
        internet-reachable, so the platform's execution gateway publishes the
        card and the Galaxy scaffold generates card content at build time.
        This method returns an
        :class:`AgentCard` object for whichever of them is doing the
        publishing — no route is added and nothing here starts serving
        ``/.well-known/agent-card.json``.  It takes the same posture
        :meth:`declared_extensions` already takes for the
        ``capabilities.extensions[]`` slice of the same document, extended to
        the whole card.

        Every registered skill is carried through under the *same* id
        :meth:`skill` registered it as — ``self._skills`` is keyed by that id,
        and :func:`_resolve_skill` looks up requests against that same dict —
        so a card built here can never advertise a ``skillId`` this agent
        would reject at request time. That drift is exactly the class of
        defect this design exists to close.

        Args:
            description: The card's top-level ``description``.  Unlike
                ``name`` and ``version``, the builder has no attribute of its
                own to source this from, so it is a parameter.
            supported_interfaces: Where the agent answers
                (``AgentCard.supportedInterfaces``).  The builder cannot know
                this: pods are not internet-reachable, so the
                reachable URL is always the gateway's, never the pod's own.
            streaming: Whether ``capabilities.streaming`` should be
                published.  ``AgentBuilder`` always implements
                ``SendStreamingMessage``/``SubscribeToTask`` in
                :meth:`handle_a2a_streaming_request`, but that
                method has to actually be wired up in front of the pod for a
                caller to reach it — a fact only whoever is assembling this
                card (the execution gateway, or the Galaxy scaffold) knows,
                since it depends on how they route requests to
                this instance, not on anything ``AgentBuilder`` itself holds.
                Defaults to ``True``, the common case where both entry points
                are wired; pass ``False`` only when the caller deliberately
                exposes ``handle_a2a_request_with_headers`` alone, so the card
                does not promise an operation that entry point declines with
                ``-32004``.
            provider: Who publishes the agent (``AgentCard.provider``).
            documentation_url: ``AgentCard.documentationUrl``.
            icon_url: ``AgentCard.iconUrl``.
            security_schemes: ``AgentCard.securitySchemes``, carried through
                opaque (see :mod:`novaos_client_sdk.a2a_agent_card`).
            security_requirements: ``AgentCard.securityRequirements``.
            default_input_modes: The card-level default input media types
                (``AgentCard.defaultInputModes``), distinct from any given
                skill's own ``input_modes``.
            default_output_modes: The card-level default output media types
                (``AgentCard.defaultOutputModes``), distinct from any given
                skill's own ``output_modes``. Defaults to
                :attr:`_default_output_modes` — the value passed to
                :meth:`AgentBuilder.__init__` — when omitted, which is what
                keeps this the *same* list :meth:`_supported_output_modes`
                enforces at dispatch time for the common, no-override call. An
                explicit value here is a one-off override for this card only;
                see :meth:`_supported_output_modes`'s own docstring for why
                that is the one case the two can drift.

        Returns:
            A populated :class:`AgentCard`.  ``capabilities.streaming`` is the
            ``streaming`` argument above, passed through as-is.
            ``capabilities.push_notifications`` mirrors
            :attr:`supports_push_notifications` — ``True`` exactly when this
            instance was constructed with a ``push_notification_store``, so
            it can never drift from what
            ``handle_a2a_request_with_headers``'s ``_dispatch_push_notification_config``
            actually serves. Per that property's own docstring,
            ``True`` here means only that
            ``Create``/``Get``/``List``/``Delete`` work at this layer — it
            does not mean this pod delivers a webhook itself; delivery,
            where it happens, is the platform gateway's job.
        """
        # Resolved once, through the same precedence
        # `_supported_output_modes` applies at dispatch time, so this card's
        # `defaultOutputModes` and every skill's `outputModes` below cannot
        # independently drift from what a `SendMessage`/`SendStreamingMessage`
        # call against this same agent actually enforces.
        effective_default_output_modes = (
            list(default_output_modes)
            if default_output_modes is not None
            else list(self._default_output_modes)
        )
        return AgentCard(
            name=self.name,
            description=description,
            version=self.version,
            supported_interfaces=list(supported_interfaces or ()),
            capabilities=AgentCapabilities(
                streaming=streaming,
                push_notifications=self.supports_push_notifications,
                extensions=self.declared_extensions(),
            ),
            default_input_modes=list(default_input_modes or ()),
            default_output_modes=effective_default_output_modes,
            skills=[
                AgentSkill(
                    id=skill_id,
                    name=meta.get("name") or skill_id,
                    description=meta.get("description") or "",
                    tags=list(meta.get("tags") or ()),
                    input_modes=list(meta.get("input_modes") or ()),
                    output_modes=self._supported_output_modes(
                        skill_id, default_output_modes=effective_default_output_modes
                    ),
                )
                for skill_id, meta in self._skills.items()
            ],
            provider=provider,
            documentation_url=documentation_url,
            icon_url=icon_url,
            security_schemes=dict(security_schemes or {}),
            security_requirements=list(security_requirements or ()),
        )

    def _agent_message(
        self,
        result: SkillResult,
        *,
        task_id: str,
        context_id: str,
        buffered_events: tuple[tuple[str, dict[str, Any]], ...] = (),
    ) -> A2AMessage:
        """Build the agent's reply message for a task's terminal status.

        The visible text is the skill's output, falling back to its error when
        the output is empty — a terminal task must never carry an empty message,
        a failure mode that has been observed and is worth guarding against
        explicitly.  ``SkillResult.error`` is
        *additionally* recorded under :data:`NOVAOS_SKILL_ERROR_KEY`, in its own
        `DataPart` tagged :data:`NOVAOS_METADATA_CONTENT_TYPE` — the platform
        marker — so a caller can read the error structurally instead of
        string-matching the text, and so the worker's HITL/error extraction
        keeps working on it.

        ``buffered_events`` — from :attr:`AgentExecutionContext.buffered_events`
        — is folded into that same platform-marker `DataPart`, one key per
        event type (``{"memory.assembled": {...}}``, matching the streaming
        transport's own wire shape from ``_stream_send_message``'s
        ``metadata_message``). A non-streaming request has no mid-execution
        channel of its own to deliver these live — see
        :meth:`AgentExecutionContext._emit_platform_event` — so this is the
        only place they reach the caller at all; the platform side reads them
        out of ``result.metadata`` via the exact same
        ``tryExtractNovaosMetadata``/``collectToolEvents``-adjacent path
        ``skill_error`` already relies on, not a separate contract of its own.

        ``SkillResult.metadata`` gets a **separate** `DataPart`, tagged
        :data:`NOVAOS_SKILL_METADATA_CONTENT_TYPE` instead. Before
        this split, a skill's own ``metadata`` was folded into the *same*
        platform-marker part as ``skill_error`` — which meant
        ``temporal-worker``'s ``drainParts`` (a part tagged with the platform
        marker is read for platform fields and never surfaced as caller data)
        silently excluded a skill's structured result from
        ``GET /api/v1/service/runs/{id}`` whenever a skill used this fold-in
        shortcut instead of calling ``ctx.task_updater.add_artifact`` manually
        with an untagged `DataPart`. The two SDK-endorsed paths now agree.
        """
        main_content = str(result.output) if not isinstance(result.output, str) else result.output
        skill_metadata = dict(result.metadata) if result.metadata else {}
        error = result.error

        if self._mask_output_pii:
            main_content = mask_pii(main_content) or main_content
            if skill_metadata:
                skill_metadata = mask_pii_in_dict(skill_metadata)
            if error:
                error = mask_pii(error)

        parts: list[Part] = [TextPart(text=main_content or error or "")]

        if skill_metadata:
            parts.append(
                DataPart(
                    # json round-trip with default=str coerces anything the caller
                    # put in metadata that is not natively JSON-serializable, so the
                    # builder cannot emit a task that fails to serialize downstream.
                    data=json.loads(json.dumps(skill_metadata, default=str)),
                    metadata={NOVAOS_METADATA_PART_KEY: NOVAOS_SKILL_METADATA_CONTENT_TYPE},
                )
            )

        platform_metadata: dict[str, Any] = {}
        if error:
            platform_metadata[NOVAOS_SKILL_ERROR_KEY] = error
        for event_type, payload in buffered_events:
            platform_metadata[event_type] = (
                mask_pii_in_dict(payload) if self._mask_output_pii else payload
            )

        if platform_metadata:
            parts.append(
                DataPart(
                    data=platform_metadata,
                    metadata={NOVAOS_METADATA_PART_KEY: NOVAOS_METADATA_CONTENT_TYPE},
                )
            )

        return A2AMessage(
            parts=parts,
            role=Role.AGENT,
            task_id=task_id or None,
            context_id=context_id or None,
        )

    def _output_artifacts(
        self,
        result: SkillResult,
        message: A2AMessage,
        *,
        skill_name: str,
    ) -> list[Artifact]:
        """Render a skill's result as the task's output artifacts.

        Built from ``message``'s parts rather than from ``result`` a second time,
        so the artifact and the status message cannot diverge and PII masking
        runs exactly once.  The two carry the same content deliberately: A2A
        models them as different things — the artifact is the machine-readable
        *output*, the status message is the agent's account of reaching this
        state — and existing NovaOS consumers (temporal-worker's
        ``tryExtractNovaosMetadata``) read the message, so the artifact is
        additive rather than a relocation.

        **An artifact is emitted only when the skill produced output.**  A task
        that failed before producing anything carries none: its explanation is
        ``status.message``, and an "output" artifact holding nothing but an error
        string would misrepresent an error as a result.
        """
        if not str(result.output or "").strip():
            return []

        return [
            Artifact(
                parts=list(message.parts),
                artifact_id=OUTPUT_ARTIFACT_ID,
                name=skill_name,
                description=self._skills.get(skill_name, {}).get("description") or None,
            )
        ]

    def _build_task(
        self,
        state: TaskState,
        message: A2AMessage | None,
        *,
        task_id: str,
        context_id: str,
        history: list[A2AMessage] | None = None,
        artifacts: list[Artifact] | None = None,
    ) -> Task:
        """Assemble a ``Task`` in ``state``."""
        return Task(
            id=task_id,
            context_id=context_id or None,
            status=TaskStatus(state=state, message=message, timestamp=utc_timestamp()),
            artifacts=artifacts or [],
            history=history or [],
        )

    def _task(
        self,
        state: TaskState,
        message: A2AMessage,
        *,
        task_id: str,
        context_id: str,
        history: list[A2AMessage] | None = None,
        artifacts: list[Artifact] | None = None,
    ) -> dict[str, Any]:
        """Assemble a ``Task`` in ``state`` and return its wire shape."""
        return self._build_task(
            state,
            message,
            task_id=task_id,
            context_id=context_id,
            history=history,
            artifacts=artifacts,
        ).to_dict()

    async def _rejected(
        self,
        reason: str,
        *,
        task_id: str,
        context_id: str,
        history: list[A2AMessage] | None = None,
    ) -> dict[str, Any]:
        """Build a ``TASK_STATE_REJECTED`` task, persist it, and return its wire shape.

        ``REJECTED`` rather than ``FAILED`` is deliberate: it means "this request
        was never going to work", as distinct from "the agent tried and could
        not".  A caller can retry a ``FAILED`` task; retrying a ``REJECTED`` one
        without changing the request is pointless, and the state says so.

        Persisted to :attr:`task_store` like every other outcome
        this class produces, so a later ``GetTask`` for this id finds the
        rejection instead of nothing — a request that never ran is still a
        request a caller may come back to ask about.
        """
        task = self._build_task(
            TaskState.REJECTED,
            A2AMessage(
                parts=[TextPart(text=reason)],
                role=Role.AGENT,
                task_id=task_id or None,
                context_id=context_id or None,
            ),
            task_id=task_id,
            context_id=context_id,
            history=history,
        )
        await self._task_store.save(task)
        return task.to_dict()

    def _task_state_for(
        self, result: SkillResult, skill_name: str
    ) -> tuple[TaskState, SkillResult]:
        """Resolve a returned ``SkillResult`` to the ``TaskState`` and (possibly
        rewritten) result :meth:`_finalize_task` should publish.

        A ``status`` outside :data:`~novaos_client_sdk.agent_types._SKILL_STATUS_TO_TASK_STATE`
        no longer falls back to ``TaskState.COMPLETED`` — the bug found by
        analysis of a customer's shared request-routing module: a skill that
        misspells
        its own status (``QUEUED``, ``RUNNING``, ``IN_PROGRESS`` and ``ERROR``
        all showed up in real agent code, none of them a word this SDK ever
        defined) had every one of those requests reported back to the caller
        as a silent success, indistinguishable from a skill that actually
        completed. The four legacy spellings above are mapped to the A2A
        state they always meant (see ``SkillResult``'s own docstring);
        anything else fails the task outright instead, with the bad value
        quoted in both an ERROR-level log naming the skill and the task's own
        status message — visible to whoever is watching logs *and* to the
        caller itself, rather than discarded either way.

        Returns the original ``result`` unchanged for a known status; for an
        unknown one, returns a copy with ``status``/``error`` overwritten by
        the diagnostic above, so :meth:`_agent_message` renders that
        diagnostic as the task's message the same way it renders any other
        skill-reported error, while ``output``/``metadata`` are preserved.

        **``QUEUED``/``RUNNING``/``IN_PROGRESS`` are retry-hazardous by
        design, and that is documented, not silently accepted.** They
        resolve to ``SUBMITTED``/``WORKING``, neither of
        which :data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES`
        recognises — so a caller reading a ``SendStreamingMessage`` reply
        cannot tell "the skill deliberately ended here" from "the connection
        dropped mid-execution", the exact ambiguity
        :class:`~novaos_client_sdk.agent_context.AgentExecutionContext`'s
        "Delivery semantics" section already warns can make the platform
        retry a stream that ended without a terminal state. This SDK has no
        wire-level way to close that gap without inventing a new A2A state
        (out of scope here); see ``SkillResult``'s own docstring for the
        existing, sanctioned mitigation (``ctx.idempotency_key``) a skill
        using these three statuses for a non-idempotent effect must apply.
        """
        try:
            return _SKILL_STATUS_TO_TASK_STATE[result.status], result
        except KeyError:
            detail = f"unknown SkillResult.status {result.status!r}"
            logger.error("skill %s: %s", skill_name, detail)
            return TaskState.FAILED, replace(result, status="FAILED", error=detail)

    async def _finalize_task(
        self,
        updater: TaskUpdater,
        state: TaskState,
        message: A2AMessage,
        *,
        artifacts: list[Artifact],
    ) -> None:
        """Publish a ``SendMessage`` execution's outcome through its updater.

        Runs after :meth:`_send_message` has already decided ``state`` from the
        skill's returned ``SkillResult`` (or from a caught exception) — this
        method does not decide anything, only publishes that decision to
        ``updater``'s store and event queue so a ``GetTask``/``SubscribeToTask``
        call would see the same outcome this ``SendMessage`` call
        is about to return in its response body.

        ``TASK_STATE_INPUT_REQUIRED`` and
        ``TASK_STATE_AUTH_REQUIRED`` are routed to
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.requires_input`
        and :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.requires_auth`
        respectively, rather than a terminal method, matching
        :data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES`:
        a paused task must remain updatable, not sealed the way ``complete``,
        ``failed`` and ``reject`` seal one.  ``AUTH_REQUIRED`` was added here
        later than ``INPUT_REQUIRED``, for the same reason ``CANCELED`` still
        has no entry:
        ``SkillResult.status`` had no auth-required vocabulary for a skill to
        return, so there was nothing to route.  ``CANCELED`` remains
        unreachable from this table on purpose — cancellation is
        ``CancelTask``'s business, not a skill's own return value;
        see :class:`~novaos_client_sdk.agent_types.SkillResult`'s docstring for
        why that member was deliberately never added.

        **``TASK_STATE_SUBMITTED``/``TASK_STATE_WORKING`` route the same
        way.** A skill returning the legacy ``QUEUED``/
        ``RUNNING``/``IN_PROGRESS`` spellings (see ``SkillResult``'s
        docstring) is reporting that its work continues outside this
        request — the handler has already returned, so there is nothing left
        for *this* call to keep running — so these route to
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.submit` /
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.start_work`
        rather than :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.complete`,
        leaving the task open the same *lifecycle* way ``INPUT_REQUIRED``/
        ``AUTH_REQUIRED`` do — whatever later closes it (a progress skill's
        own ``SendMessage`` naming the same task id, or a background job
        calling ``ctx.task_updater`` directly) is a separate execution, not
        this one — **but not the same *stream-closing* way.**
        :data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES` (and so
        :func:`~novaos_client_sdk.a2a_events.is_last_frame`) recognises
        ``INPUT_REQUIRED``/``AUTH_REQUIRED`` as a clean pause; it does not
        recognise ``SUBMITTED``/``WORKING``, so a ``SendStreamingMessage``
        reply ending here reads exactly like a dropped connection. See
        :meth:`_task_state_for`'s docstring for why that gap is accepted
        rather than closed, and what a skill using these statuses must do
        about it.

        A ``QUEUED`` return therefore can move a task *backward*, from
        ``WORKING`` (every dispatcher already calls ``start_work()`` before
        invoking a skill) to ``SUBMITTED`` — accepted deliberately rather than
        guarded against. Nothing about that transition is unsafe (``submit``
        carries no monotonicity check, matching the reference implementation),
        and it is the honest answer for exactly the case ``QUEUED`` models:
        this call's own handler did no work at all, it only accepted a request
        that some other job now owns.

        **A task the skill already closed itself wins.**
        ``ctx.task_updater``'s terminal methods (``complete``/``failed``/
        ``reject``/``cancel``) are public, despite the docstring telling a
        skill not to call them — a skill can still call one anyway, which
        raises :class:`RuntimeError` on *this* method's own terminal call a
        moment later, since the updater is already sealed.

        That must not turn an otherwise well-formed request into a JSON-RPC
        ``INTERNAL_ERROR`` — this method's caller has nothing to do about a
        skill's own choice — so the ``RuntimeError`` is caught. What happens
        next is a precedence decision made explicit here: **the
        skill's own direct call is treated as authoritative and left in
        place**; this method makes no further write. A prior version of this
        method instead called
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater._force_update_status`
        here to make ``state`` — the harness's own, ``SkillResult``-derived
        decision — win instead. That is exactly the bug this precedence
        rule found: a skill
        that calls ``ctx.task_updater.reject(...)`` to report "this request
        was never going to work" and then returns any ``SkillResult`` (most
        naturally the default ``COMPLETED``, since it has nothing else to
        report) had its ``REJECTED`` silently overwritten by whatever the
        returned status mapped to — there was no way for a skill to make
        ``REJECTED`` stick from inside a running skill at all.

        The direct call is the more specific, more deliberate of the two
        signals — a skill that reaches for ``ctx.task_updater`` mid-execution
        is explicitly choosing when and how its task ends, which is exactly
        what that object is for (see its module docstring). A returned
        ``SkillResult`` is the *fallback* this method uses when a skill has
        **not** already closed its own task, not a second vote that
        outranks a closing call the skill made on purpose. This is symmetric
        with :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater._set_status`'s
        own terminal guard, which already refuses to downgrade a
        store-terminal task back to something transient for the same reason.

        Both callers of this method already read the task back from
        ``self._task_store`` after calling it, rather than assuming ``state``
        was what got written — that is what keeps their JSON-RPC response
        in agreement with whichever write actually won here, without this
        method needing to report back which one that was.

        ``_force_update_status`` itself is unchanged and still exists on
        :class:`~novaos_client_sdk.a2a_task_updater.TaskUpdater` as a
        low-level bypass of the terminal guard — nothing in this SDK calls it
        today, but it remains a documented, independently tested capability
        of that class for a caller with an actual reason to override a
        sealed task, which this method no longer is.

        **A self-sealed task gets none of ``artifacts`` either.**
        ``artifacts`` is rendered from the *returned*
        ``SkillResult`` — the same decision ``state`` comes from — so once a
        skill's own direct call has already won, attaching its output as an
        artifact would leave the winning, self-sealed task (say, ``REJECTED``)
        carrying an artifact that belongs to the losing, superseded outcome. A
        caller with a non-empty ``SkillResult.output`` and a skill that also
        calls ``ctx.task_updater.reject(...)`` is exactly the case this
        guards: without it, :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.add_artifact`
        would still attach that output — it has no terminal guard of its own
        — even though the status write right after it is skipped. Checking
        :attr:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.is_sealed`
        first, rather than only catching the terminal call's own
        ``RuntimeError`` afterward, is what lets this method skip the
        artifact loop too instead of just the status write.
        """
        if updater.is_sealed:
            return
        for artifact in artifacts:
            await updater.add_artifact(
                artifact.parts,
                artifact_id=artifact.artifact_id,
                name=artifact.name,
                description=artifact.description,
                last_chunk=True,
            )
        try:
            if state == TaskState.REJECTED:
                await updater.reject(message)
            elif state == TaskState.FAILED:
                await updater.failed(message)
            elif state == TaskState.INPUT_REQUIRED:
                await updater.requires_input(message)
            elif state == TaskState.AUTH_REQUIRED:
                await updater.requires_auth(message)
            elif state == TaskState.SUBMITTED:
                await updater.submit(message)
            elif state == TaskState.WORKING:
                await updater.start_work(message)
            else:
                await updater.complete(message)
        except RuntimeError:
            # Not the primary way this method detects a self-seal any more —
            # the `is_sealed` check above already covers every path reachable
            # today, since the skill's own coroutine has finished running by
            # the time this method is called and no concurrent caller holds
            # this exact `updater` instance (a concurrent `CancelTask` seals
            # the task through its own, separate updater instead — see
            # `TaskUpdater._set_status`'s docstring). Kept as a defensive
            # backstop rather than removed: if that invariant is ever wrong,
            # the alternative is an `INTERNAL_ERROR` on an otherwise
            # well-formed request, which is worse than a redundant guard.
            pass

    def _failed_message(self, error: BaseException, *, task_id: str, context_id: str) -> A2AMessage:
        """The ``FAILED`` status message for a skill that raised ``error``.

        One place for the PII-masking rule every failure path shares —
        :meth:`_send_message`, :meth:`_stream_send_message` and
        :meth:`_finish_detached_execution` all report an unhandled skill
        exception the same way, and ``mask_output_pii`` must apply to all
        three or the flag's promise ("agent output never carries a PII
        pattern") has a hole in it on whichever path forgot.
        """
        error_message = str(error)
        if self._mask_output_pii:
            error_message = mask_pii(error_message) or error_message
        return self._agent_message(
            SkillResult(output="", status="FAILED", error=error_message),
            task_id=task_id,
            context_id=context_id,
        )

    async def _canceled_snapshot(self, task_updater: TaskUpdater, task_id: str) -> Task:
        """The task as a caller whose ``CancelTask`` was honoured should see it.

        Every ``if cancellation.is_set:`` branch in :meth:`_send_message` /
        :meth:`_stream_send_message` answers with the store's current task,
        on the reasoning that :meth:`_dispatch_cancel_task` wrote ``CANCELED``
        before it ever set the token. That holds whenever the ``CancelTask``
        landed *after* this execution's own ``SUBMITTED`` seed. It does not
        hold for one that landed *between* the token's registration and the
        seed: the token is registered before the
        seed now, so a ``CancelTask`` in that window finds the task id's
        *prior* incarnation in the store — a detached execution's ``WORKING``
        task, say — writes ``CANCELED`` over it through its own updater, sets
        *this* execution's token, and answers success; then this execution's
        seed, a plain ``store.save`` with no terminal guard, overwrites that
        ``CANCELED`` with ``SUBMITTED``. Answering with the bare snapshot
        would then leave a ``SUBMITTED`` task nobody is running, contradicting
        the success ``CancelTask`` already returned.

        So: if the token is set but the store is not terminal, the
        cancellation this execution was told about is written now, through
        this execution's own ``task_updater`` — then the snapshot is taken.
        A store that is already terminal (the common case) is returned as
        is; an updater that has been revoked because a *newer* request
        superseded this one writes nothing, and the snapshot is that newer
        execution's task, which is correct too — that ``is_set`` was never a
        ``CancelTask``.
        """
        task = await self._task_store.get(task_id)
        assert task is not None
        if task.status.state in TERMINAL_TASK_STATES:
            return task
        await task_updater.cancel(
            A2AMessage(
                parts=[TextPart(text="Task canceled.")],
                role=Role.AGENT,
                task_id=task_id,
                context_id=task_updater.context_id,
            )
        )
        task = await self._task_store.get(task_id)
        assert task is not None
        return task

    def _supersede_live_execution(self, task_id: str) -> None:
        """Preempt whatever execution currently owns ``task_id`` before re-seeding it.

        A2A lets a caller continue a task by naming its id on a new
        ``SendMessage``/``SendStreamingMessage``; this SDK's continuation is a
        *restart* — the dispatchers re-seed the task ``SUBMITTED`` and run the
        skill again (see :meth:`_send_message`'s "Task id precedence" note).
        Previously, the only way an execution could still be live on
        that id was a caller racing itself with two concurrent requests. The
        "Interruption policy" rule 3 above makes it routine: a caller that
        disconnected mid-skill and
        retried on the same id — the platform worker reuses its ``run_id`` as
        the task id — arrives while the detached execution is still running.
        Two executions on one task would both publish to it, and the earlier
        one's outcome would land on the task the later one now owns.

        So the earlier execution is preempted through its cancellation token,
        exactly as :meth:`_dispatch_cancel_task` would preempt it: its skill's
        ``Task`` is cancelled, its own handler or detached finisher then sees
        ``cancellation.is_set`` and publishes nothing, and its identity-checked
        cleanup leaves the new registration alone. Its ``TaskUpdater`` is also
        revoked (:meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.revoke`), so
        a write it had *already* committed to — one that passed its
        ``is_set`` check and is now suspended inside ``_finalize_task``'s
        awaits — lands nowhere either. This is *not* a ``CancelTask``, though
        — nothing writes ``CANCELED``; the re-seed that follows is what the
        store says next. Called synchronously, with the new execution's own
        registration following before any ``await``, so two requests
        entering together on one task id cannot both miss each other.
        """
        previous = self._active_cancellations.get(task_id)
        if previous is not None:
            previous.set()
            # `set()` alone leaves a window — an execution
            # that already passed its `is_set` check and is inside
            # `_finalize_task`'s awaits would still write. Revoking its
            # updater makes every later write through it a no-op, checked
            # under the updater's own lock right before each store write.
            if previous.updater is not None:
                previous.updater.revoke()

    def _detach_execution(
        self,
        *,
        skill_task: "asyncio.Task[SkillResult]",
        cancellation: _CancellationToken,
        task_updater: TaskUpdater,
        event_queue: EventQueue,
        task_id: str,
        context_id: str,
        skill_name: str,
    ) -> "asyncio.Task[None]":
        """Hand a still-running skill to a finisher once its request is gone.

        Called from :meth:`_send_message`'s and :meth:`_stream_send_message`'s
        abandonment handlers — rule 3 of the module docstring's "Interruption
        policy" — when the request that started ``skill_task`` has been
        cancelled or its response generator closed while the skill is still
        running (or has finished, but its outcome was never published). The
        caller must **not** then close ``event_queue`` or pop this task's
        registry entries in its own ``finally``: ownership of both passes to
        :meth:`_finish_detached_execution`, which does so once the outcome is
        in the store, so a ``SubscribeToTask`` that attaches meanwhile still
        sees the terminal frame, and a ``CancelTask`` can still find the
        token to preempt the skill.

        The finisher is held in :attr:`_detached_executions` for its own
        lifetime (asyncio keeps only weak references to tasks) and removes
        itself on completion.
        """
        finisher = asyncio.create_task(
            self._finish_detached_execution(
                skill_task=skill_task,
                cancellation=cancellation,
                task_updater=task_updater,
                event_queue=event_queue,
                task_id=task_id,
                context_id=context_id,
                skill_name=skill_name,
            ),
            name=f"novaos-detached-execution:{task_id}",
        )
        self._detached_executions.add(finisher)
        finisher.add_done_callback(self._detached_executions.discard)
        return finisher

    async def _finish_detached_execution(
        self,
        *,
        skill_task: "asyncio.Task[SkillResult]",
        cancellation: _CancellationToken,
        task_updater: TaskUpdater,
        event_queue: EventQueue,
        task_id: str,
        context_id: str,
        skill_name: str,
    ) -> None:
        """Publish an abandoned execution's outcome exactly as its request would have.

        The off-request half of rule 3 (module docstring, "Interruption
        policy"). Waits for ``skill_task`` — never cancels it — and then
        applies the same three-way decision the request path applies:

        * ``skill_task`` ends in ``asyncio.CancelledError`` — either a
          ``CancelTask`` preempted it (``cancellation`` is set, the store
          already carries ``CANCELED``, rule 1) or the skill ended itself
          without an outcome (rule 2). Neither is this method's to write, so
          it writes nothing.
        * ``skill_task`` raised a plain exception — ``FAILED``, via
          :meth:`_failed_message`, unless a ``CancelTask`` already answered
          ``CANCELED`` (the same check :meth:`_send_message`'s own
          ``except Exception`` makes, for the same reason).
        * ``skill_task`` returned a ``SkillResult`` — mapped, rendered and
          published through :meth:`_finalize_task` exactly as on-request,
          including the rule that a task the skill sealed itself wins
          and gets no artifact from the returned result. Discarded instead if
          a ``CancelTask`` landed first.

        No SSE frames and no JSON-RPC response: the caller that would have
        received them is gone. What the store and the shared ``EventQueue``
        carry is the whole output — which is what a later ``GetTask`` or a
        reconnecting ``SubscribeToTask`` reads.

        ``finally`` closes ``event_queue`` and unregisters this task from
        :attr:`_active_event_queues` / :attr:`_active_cancellations` — the
        cleanup the abandoned request skipped when it handed off — but only
        while the registered entry is still *this* execution's: a caller that
        reconnected by re-sending on the same task id may have registered a
        newer one meanwhile, and that one is not this method's to remove.

        If this finisher is itself cancelled (event-loop shutdown), the
        cancellation propagates to ``skill_task`` through the ``await`` and
        is re-raised after cleanup rather than swallowed.

        **Publishes only while this execution still owns the task.** A later
        request on the same task id supersedes this one
        (:meth:`_supersede_live_execution`), which sets ``cancellation`` — so
        the ``is_set`` checks below already cover it — but ownership is also
        checked directly against :attr:`_active_cancellations`, so that even
        a registration replaced by some path that forgot to preempt could
        never have a stale finisher seal or attach artifacts to a task a
        newer execution now owns. And because both of those
        checks are separated from the actual store writes by ``await``s,
        the supersede also revokes ``task_updater`` itself, which is what
        makes a write already in flight at that moment a no-op.
        """

        def owns_task() -> bool:
            return self._active_cancellations.get(task_id) is cancellation

        try:
            try:
                result = await skill_task
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
                return
            except Exception as e:
                if cancellation.is_set or not owns_task():
                    return
                await self._finalize_task(
                    task_updater,
                    TaskState.FAILED,
                    self._failed_message(e, task_id=task_id, context_id=context_id),
                    artifacts=[],
                )
                return
            if cancellation.is_set or not owns_task():
                return
            state, result = self._task_state_for(result, skill_name)
            message = self._agent_message(result, task_id=task_id, context_id=context_id)
            artifacts = (
                []
                if task_updater.is_sealed
                else self._output_artifacts(result, message, skill_name=skill_name)
            )
            await self._finalize_task(task_updater, state, message, artifacts=artifacts)
        finally:
            await event_queue.close()
            if self._active_event_queues.get(task_id) is event_queue:
                del self._active_event_queues[task_id]
            if self._active_cancellations.get(task_id) is cancellation:
                del self._active_cancellations[task_id]

    async def _continuation_history(self, task_id: str, message: A2AMessage) -> list[A2AMessage]:
        """Build the ``Task.history`` this execution should seed ``task_id`` with.

        ``_supersede_live_execution``'s own docstring already calls a
        ``SendMessage``/``SendStreamingMessage`` naming an existing task id a
        *restart* of the skill's **execution** — the dispatchers re-seed
        ``SUBMITTED`` and run the handler again regardless of what state the
        task was in. Before this method existed, every call also reset
        **history** to ``[message]`` the same unconditional way, which
        defeated the entire point of ``INPUT_REQUIRED``/``AUTH_REQUIRED``:
        those two states exist because A2A models the
        caller's answer to a pause as the *next turn of one conversation*, not
        a new one, and a handler that reads :attr:`AgentExecutionContext.history`
        expecting the question it just asked to still be there found it gone.

        **Accumulate when continuing a non-terminal task; reset otherwise.** A
        ``task_id`` that names a task currently in a non-terminal state — the
        live ``INPUT_REQUIRED``/``AUTH_REQUIRED`` continuation this exists for,
        or a still-``SUBMITTED``/``WORKING`` one a caller happened to retry —
        gets that task's own stored history plus ``message`` appended,
        oldest-first. A ``task_id`` that is unknown (a fresh task) or already
        **terminal** (``COMPLETED``/``FAILED``/``CANCELED``/``REJECTED``)
        resets to ``[message]`` instead: restarting a finished task is a new
        invocation that happens to reuse an old id — the at-least-once
        delivery model's own retry case (see the module docstring's
        ``idempotency_key`` guidance) — not a continued dialogue, and
        accumulating there would only grow without ever being read as one.

        Args:
            task_id: The id this execution resolved for its task (existing or
                fresh — the caller does not need to know which).
            message: The inbound message this execution is about to run,
                appended as the newest turn.

        Returns:
            The full ordered history to seed this execution's task with.
        """
        existing = await self._task_store.get(task_id)
        if existing is not None and existing.status.state not in TERMINAL_TASK_STATES:
            return [*existing.history, message]
        return [message]

    async def _resolve_referenced_tasks(
        self, reference_task_ids: list[str] | None, context_id: str
    ) -> tuple[tuple[Task, ...], tuple[str, ...]]:
        """Resolve ``message.reference_task_ids`` against this pod's own task store.

        Read-only: nothing about a referenced task's own state is checked or
        changed here, this is a lookup and nothing more. Called by
        :meth:`_send_message` and :meth:`_stream_send_message` before either
        builds its :class:`~novaos_client_sdk.agent_context.AgentExecutionContext`,
        so a skill reads :attr:`~novaos_client_sdk.agent_context.AgentExecutionContext.referenced_tasks`
        as already-resolved ``Task`` objects rather than resolving them itself
        — this SDK gives a skill no other way to reach :attr:`task_store` at
        all.

        **Scoped to ``context_id``, in place of a tenant field this store's
        data model does not have** (see
        :attr:`~novaos_client_sdk.agent_context.AgentExecutionContext.referenced_tasks`'s
        own docstring for the full reasoning): a referenced id belonging to a
        *different* ``context_id`` than this request's own is treated exactly
        like an id the store has never seen — reported as unresolved, not
        resolved and not raised on.

        Args:
            reference_task_ids: ``message.reference_task_ids``, straight off
                the wire — possibly ``None``, possibly containing anything a
                caller chose to put in a string list.
            context_id: This request's own ``contextId``, already resolved by
                the caller.

        Returns:
            ``(resolved, unresolved)`` — the tasks that resolved, in the order
            requested, and the ids that did not (unknown to this store, or
            scoped to a different ``context_id``).
        """
        # A stored `Task.context_id` is never an empty string —
        # `_build_task` normalizes it to `None` (`context_id or None`) — but
        # this request's own `context_id` is passed through as `""` when
        # nothing supplied one. Comparing the two as given made `None == ""`
        # false, so the common no-`contextId` case (both sides genuinely
        # contextless) never matched and every same-conversation reference
        # landed in `unresolved` instead of `resolved`. Normalizing this
        # side the same way the store already normalizes the other closes
        # that gap without changing what "different context" means.
        normalized_context_id = context_id or None
        resolved: list[Task] = []
        unresolved: list[str] = []
        for ref_id in reference_task_ids or ():
            if not isinstance(ref_id, str) or not ref_id:
                continue
            task = await self._task_store.get(ref_id)
            if task is not None and task.context_id == normalized_context_id:
                resolved.append(task)
            else:
                unresolved.append(ref_id)
        return tuple(resolved), tuple(unresolved)

    def _content_negotiation_error(
        self, request_id: Any, skill_id: str, params: dict[str, Any]
    ) -> dict[str, Any] | None:
        """A2A ``-32005``: refuse a request whose ``acceptedOutputModes`` names nothing ``skill_id`` produces.

        Reads ``configuration.acceptedOutputModes`` — v1.0's per-call content
        negotiation, nested exactly where the client side already reads
        ``configuration.acceptedOutputModes`` — and compares
        it against :meth:`_supported_output_modes`'s answer for ``skill_id``.
        An empty or absent list means the caller declared no restriction, which
        A2A treats as "anything is acceptable"; only a *non-empty* list with
        **zero** overlap is refused. This is deliberately checked as a set
        intersection, not "every accepted mode is supported" — a caller naming
        several modes is asking for any one of them, not all.

        Called by :meth:`_send_message` and :meth:`handle_a2a_streaming_request`
        **before either creates or seeds a task**: ``-32005`` is a
        request-level refusal, and a caller retrying
        after fixing its ``acceptedOutputModes`` must not find a rejected task
        sitting at the id it named. This is why the check lives here, taking
        ``request_id``/``skill_id``/``params`` directly, rather than inside
        :meth:`_send_message` after its task-seeding has already begun.

        Args:
            request_id: The JSON-RPC request id, echoed back verbatim.
            skill_id: The already-resolved skill this request would run.
            params: The request's ``params`` object (``configuration`` is read
                off it directly; every other method reads its own nesting).

        Returns:
            A JSON-RPC ``error`` response if negotiation fails, else ``None``.
        """
        configuration = as_dict(params.get("configuration"))
        accepted = configuration.get("acceptedOutputModes")
        if not isinstance(accepted, list):
            return None
        accepted_modes = [mode for mode in accepted if isinstance(mode, str) and mode]
        if not accepted_modes:
            return None
        supported_modes = self._supported_output_modes(skill_id)
        if set(accepted_modes) & set(supported_modes):
            return None
        return error_response(
            request_id,
            ContentTypeNotSupportedError.error_code,
            f"Skill {skill_id!r} produces {supported_modes}, which shares no media "
            f"type with the requested acceptedOutputModes {accepted_modes}.",
            data={"acceptedOutputModes": accepted_modes, "supportedOutputModes": supported_modes},
        )

    async def handle_a2a_request(self, body: Any, headers: Any) -> dict[str, Any]:
        """Handle one A2A JSON-RPC 2.0 request and return the response envelope.

        The body-only form of :meth:`handle_a2a_request_with_headers`, which is
        what an agent serving no extensions needs.  An agent that *does* serve
        extensions should call that method instead and put the response headers
        it returns on the wire, so a caller can see what its
        ``A2A-Extensions`` request actually activated.

        Implements the ``SendMessage`` operation of **A2A spec v1.0.1**: builds
        the execution context, routes to a skill, runs it, and returns the
        resulting ``Task``.  Creates an OTel span around skill execution and
        wires ``EvaluationContext`` when the evaluation module is installed.

        **Two distinct failure layers**, kept distinct on purpose:

        * A malformed or unsupported *call* gets a JSON-RPC ``error`` response —
          the request never became a task, so there is no task to report a state
          for.
        * A call that was well-formed but could not be served gets a JSON-RPC
          ``result`` holding a task in ``TASK_STATE_REJECTED`` (unroutable) or
          ``TASK_STATE_FAILED`` (the skill raised).  This matches what the
          Galaxy scaffold's vendor-SDK executor produces for the same
          conditions, so the Temporal worker handles both kinds of pod
          identically.

        **Version negotiation is spec-strict, including for an absent header.**
        A2A v1.0 §7 makes ``A2A-Version``
        mandatory and defines an absent or empty header as an explicit request
        for ``0.3``, so a header-less call is answered ``-32009``
        (:class:`~novaos_client_sdk.exceptions.VersionNotSupportedError`), not
        served.  The alternative — accepting an absent header as ``1.0`` during
        the cutover — was considered and rejected: it is the exact
        looks-compliant-but-is-not shape found elsewhere, and because a
        scaffolded pod already rejects those calls, being lenient here would
        *preserve* the two-kinds-of-pod divergence this handler is meant to
        close.  It costs nothing because ``handle_a2a_request`` has no caller
        yet — the Temporal worker's A2A rewrite, the first one, is written
        against this contract.

        Args:
            body: The JSON-decoded request body
            headers: Request headers; any mapping with ``.items()``

        Returns:
            A JSON-RPC 2.0 response object, ready to serialize.
        """
        response_body, _ = await self.handle_a2a_request_with_headers(body, headers)
        return response_body

    async def handle_a2a_request_with_headers(
        self, body: Any, headers: Any
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Handle one A2A request, returning its response body *and* headers.

        The same work as :meth:`handle_a2a_request`, which delegates here.  The
        second return value exists because A2A's extension negotiation is
        two-way: a caller names the extensions it wants in the
        ``A2A-Extensions`` request header, and the agent answers with the ones
        actually in force.  That answer is a header, and a method returning only
        a JSON-RPC body has nowhere to put it — hence the second form rather
        than a changed return type on the first, which every existing pod route
        already calls.

        **Extension negotiation is read off the original ``headers`` object**,
        not the lower-cased dict the rest of this handler uses.  ``A2A-Extensions``
        is a list-valued header, so a caller may send it as several header lines;
        flattening a multi-map into a ``dict`` keeps only one of them, and the
        others would silently fail to activate.

        **An extension this agent does not implement is ignored, not refused.**
        The one hard failure is the mirror image: an extension this agent
        declared ``required`` that the caller did not activate is answered
        ``-32008``
        (:class:`~novaos_client_sdk.exceptions.ExtensionSupportRequiredError`),
        which is exactly what that code is for.  It is checked *after* version
        negotiation, since a v0.3 caller's extension list is not worth reading.

        Args:
            body: The JSON-decoded request body
            headers: Request headers; any mapping with ``.items()``

        Returns:
            ``(response_body, response_headers)``.  The headers are empty unless
            at least one extension is in force.
        """
        request, envelope_error = parse_request(body)
        if envelope_error is not None:
            return envelope_error, {}
        assert request is not None  # parse_request returns exactly one of the two

        # `SendStreamingMessage` and `SubscribeToTask` are both real v1.0
        # operations this agent implements —
        # just not *here*.  Both have a result shape this method cannot
        # return (an SSE byte stream), and both are served by
        # :meth:`handle_a2a_streaming_request` instead.  Reported with A2A's
        # own UnsupportedOperationError, not JSON-RPC's generic "method not
        # found" — the caller's next move differs.  Checked immediately after
        # method resolution, before any params/version validation — exactly
        # where the original `SendStreamingMessage`-only special case sat —
        # so moving this later would change today's -32004 into whatever a
        # later check answers first (`TestVersionCheckOrdering` and
        # `TestOrderingParity` pin the ordering this preserves).
        if request.method in (METHOD_SEND_STREAMING_MESSAGE, METHOD_SUBSCRIBE_TO_TASK):
            return (
                error_response(
                    request.id,
                    UnsupportedOperationError.error_code,
                    f"{request.method} is not implemented by this entry point. Use "
                    "handle_a2a_streaming_request instead.",
                ),
                {},
            )

        if request.method not in _KNOWN_JSON_ONLY_METHODS:
            return (
                error_response(
                    request.id,
                    METHOD_NOT_FOUND,
                    f"Unknown method {request.method!r}. This agent implements "
                    f"{', '.join(_KNOWN_JSON_ONLY_METHODS)}.",
                ),
                {},
            )

        params = request.params

        # Per-method params validation. Each of the known
        # methods has its own required-field shape; `SendMessage`'s is the
        # original check, kept verbatim. `raw_message` stays `None` for every
        # other method — there is no message to validate or to read
        # extension metadata off of.
        raw_message: dict[str, Any] | None = None
        if request.method == METHOD_SEND_MESSAGE:
            raw_message = params.get("message")
            if not isinstance(raw_message, dict):
                return (
                    error_response(
                        request.id,
                        INVALID_PARAMS,
                        f'"params.message" must be an A2A Message object, got '
                        f"{type(raw_message).__name__}.",
                    ),
                    {},
                )
        elif request.method in (METHOD_GET_TASK, METHOD_CANCEL_TASK):
            if not _is_nonempty_str(params.get("id")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.id" must be a non-empty string.'
                    ),
                    {},
                )
        elif request.method == METHOD_LIST_TASKS:
            pass  # No required params — every filter ListTasks reads is optional.
        elif request.method == METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG:
            if not _is_nonempty_str(params.get("taskId")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.taskId" must be a non-empty string.'
                    ),
                    {},
                )
            if not _is_nonempty_str(params.get("url")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.url" must be a non-empty string.'
                    ),
                    {},
                )
        elif request.method == METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS:
            if not _is_nonempty_str(params.get("taskId")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.taskId" must be a non-empty string.'
                    ),
                    {},
                )
        else:
            assert request.method in (
                METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG,
                METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG,
            )
            if not _is_nonempty_str(params.get("taskId")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.taskId" must be a non-empty string.'
                    ),
                    {},
                )
            if not _is_nonempty_str(params.get("id")):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.id" must be a non-empty string.'
                    ),
                    {},
                )

        header_dict: dict[str, str] = {}
        if hasattr(headers, "items"):
            # A non-string key can't be a header name, so it is skipped rather
            # than crashing `.lower()` — the same skip-and-continue posture
            # this handler takes with every other caller-controlled field
            # (see `requested_protocol_version`'s identical guard).
            header_dict = {k.lower(): v for k, v in headers.items() if isinstance(k, str)}

        # Version negotiation runs *here* — after the method and params checks,
        # not before them — because that is where the reference dispatcher runs
        # it (`a2a.server.routes.jsonrpc_dispatcher` validates the envelope and
        # routes the method first, and only the resolved handler carries
        # `@validate_version`).  Ordering is observable: an unknown method sent
        # without the header answers -32601, not -32009, on both kinds of pod.
        version_problem = unsupported_version_reason(header_dict)
        if version_problem is not None:
            return (
                error_response(request.id, VersionNotSupportedError.error_code, version_problem),
                {},
            )

        # Only `SendMessage` has a message to read extension metadata off of —
        # every other method's `message_metadata` is `None`, matching what
        # `handle_a2a_streaming_request` does for `SubscribeToTask`.
        message_metadata = raw_message.get("metadata") if raw_message is not None else None
        activation = self._extensions.activate(headers, params.get("metadata"), message_metadata)
        unmet = activation.unmet_requirement_reason()
        if unmet is not None:
            # No response headers: nothing was activated, so there is nothing to
            # report as in force — the error body names what is missing.
            return error_response(request.id, ExtensionSupportRequiredError.error_code, unmet), {}

        response_headers = activation.response_headers()

        try:
            if request.method == METHOD_SEND_MESSAGE:
                assert raw_message is not None  # validated above
                response_body = await self._send_message(
                    request.id, params, raw_message, header_dict, activation
                )
            elif request.method == METHOD_GET_TASK:
                response_body = await self._dispatch_get_task(request.id, params)
            elif request.method == METHOD_CANCEL_TASK:
                response_body = await self._dispatch_cancel_task(request.id, params)
            elif request.method == METHOD_LIST_TASKS:
                response_body = await self._dispatch_list_tasks(request.id, params)
            else:
                assert request.method in _PUSH_NOTIFICATION_CONFIG_METHODS
                response_body = await self._dispatch_push_notification_config(
                    request.method, request.id, params
                )
            return response_body, response_headers
        except Exception as e:  # pragma: no cover - defensive backstop
            # Anything escaping a dispatch method is an SDK bug rather than an
            # agent failure, so it is reported at the protocol layer instead of
            # being dressed up as a task the caller could retry.
            message = str(e)
            if self._mask_output_pii:
                message = mask_pii(message) or message
            return (
                error_response(request.id, INTERNAL_ERROR, f"Agent handler error: {message}"),
                response_headers,
            )

    async def handle_a2a_bytes(self, raw: bytes | str, headers: Any) -> dict[str, Any]:
        """Handle one A2A JSON-RPC 2.0 request given as an undecoded HTTP body.

        The bytes-first counterpart of :meth:`handle_a2a_request`, for a route
        that has not yet called ``request.json()`` (or equivalent) itself.  A
        hand-rolled route that decodes the body before calling
        ``handle_a2a_request`` lets that decode step raise past it — the
        framework turns a malformed body into an HTTP 500 instead of the
        JSON-RPC ``PARSE_ERROR`` (``-32700``) the spec calls for — and every
        scaffolded agent duplicated exactly that mistake. This
        method, and :meth:`asgi_endpoint` built on it, exist so the decode is
        the SDK's problem once rather than every wrapper's problem repeatedly.

        Args:
            raw: The undecoded request body, as ``bytes`` (e.g. from an ASGI
                ``await request.body()``) or ``str``.
            headers: Request headers; any mapping with ``.items()``

        Returns:
            A JSON-RPC 2.0 response object, ready to serialize. ``PARSE_ERROR``
            with ``id: null`` if ``raw`` was not valid JSON (or exceeded the
            size limit); otherwise identical to what
            :meth:`handle_a2a_request` returns for the decoded body.
        """
        response_body, _ = await self.handle_a2a_bytes_with_headers(raw, headers)
        return response_body

    async def handle_a2a_bytes_with_headers(
        self, raw: bytes | str, headers: Any
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Handle one A2A request given as an undecoded HTTP body, with headers.

        The bytes-first counterpart of :meth:`handle_a2a_request_with_headers`;
        see :meth:`handle_a2a_bytes` for why the decode belongs here rather
        than in a route.  Decoding is the only step added — a body that
        decodes successfully is handled identically to calling
        ``handle_a2a_request_with_headers(decoded_body, headers)`` directly.

        Args:
            raw: The undecoded request body, as ``bytes`` (e.g. from an ASGI
                ``await request.body()``) or ``str``.
            headers: Request headers; any mapping with ``.items()``

        Returns:
            ``(response_body, response_headers)``. On a decode failure,
            ``response_headers`` is always ``{}`` — no extension negotiation
            can have happened for a body that was never read as JSON.
        """
        body, decode_error = decode_request_body(raw)
        if decode_error is not None:
            return decode_error, {}
        return await self.handle_a2a_request_with_headers(body, headers)

    def asgi_endpoint(self) -> Callable[[Any], Any]:
        """Build a ready-made ASGI route handler for this agent's A2A endpoint.

        Turns the hand-written route every scaffolded agent used to carry —
        decode the body, branch on ``SendStreamingMessage``/``SubscribeToTask``
        vs. everything else, pick ``JSONResponse`` or ``StreamingResponse`` —
        into one line:

        ```python
        from novaos_client_sdk import A2A_JSONRPC_PATH, AgentBuilder

        agent = AgentBuilder(name="my-agent", version="1.0.0")
        app.post(A2A_JSONRPC_PATH)(agent.asgi_endpoint())
        ```

        The returned callable takes one Starlette-shaped request (anything
        with an async ``.body()`` and a ``.headers`` mapping — a plain
        ``starlette.requests.Request`` or the identical object FastAPI hands a
        route, since FastAPI's ``Request``/``Response`` *are* Starlette's) and
        returns a Starlette ``Response``, so it mounts directly on either
        framework's router.

        Requires Starlette to be installed (FastAPI pulls it in transitively,
        so any FastAPI app already has it). Importing this module never
        requires it — only calling this method does.

        Returns:
            An async ``(request) -> Response`` callable.

        Raises:
            ImportError: If Starlette is not installed.
        """
        try:
            from starlette.requests import Request
            from starlette.responses import JSONResponse, Response, StreamingResponse
        except ImportError as exc:
            raise ImportError(
                "AgentBuilder.asgi_endpoint() requires starlette. Install it "
                "directly, or via `pip install fastapi` (which depends on it)."
            ) from exc

        # The parameter *must* be annotated `Request` — not `Any` — because
        # FastAPI's `app.post(...)` decorator inspects the signature to decide
        # what to inject. An untyped parameter is treated as a query parameter
        # to parse from the URL rather than "give me the raw request", so
        # every call would 422 with "field required" instead of ever reaching
        # this body. A plain Starlette `Route(path, endpoint)` never does this
        # inspection, so the annotation is a no-op there — safe either way.
        async def endpoint(request: Request) -> Response:
            raw = await request.body()
            body, decode_error = decode_request_body(raw)
            if decode_error is not None:
                return JSONResponse(decode_error)

            if isinstance(body, dict) and body.get("method") in (
                METHOD_SEND_STREAMING_MESSAGE,
                METHOD_SUBSCRIBE_TO_TASK,
            ):
                result, response_headers = await self.handle_a2a_streaming_request(
                    body, request.headers
                )
                if isinstance(result, dict):
                    return JSONResponse(result, headers=response_headers)
                return StreamingResponse(
                    result, media_type="text/event-stream", headers=response_headers
                )

            response_body, response_headers = await self.handle_a2a_request_with_headers(
                body, request.headers
            )
            return JSONResponse(response_body, headers=response_headers)

        return endpoint

    @staticmethod
    def _apply_history_length(wire: dict[str, Any], history_length: Any) -> None:
        """Truncate a task's wire-shape ``history`` to ``historyLength``, in place.

        The one truncation rule shared by every place this SDK reads a
        caller-supplied ``historyLength``, however differently each one nests
        it: ``GetTask``'s and ``ListTasks``' flat ``params.historyLength``,
        and ``SendMessage``'s/``SendStreamingMessage``'s nested
        ``configuration.historyLength`` (following the client's own
        placement of ``acceptedOutputModes`` at the same nesting level). One
        method rather than four independent reimplementations is what keeps a
        caller's ``historyLength: 0`` meaning the same thing regardless of
        which method it sent it to.

        Omitting the param (anything that is not a non-``bool`` ``int``)
        leaves ``wire["history"]`` untouched — full history, the ``to_dict()``
        output as built. ``0`` removes the key entirely rather than leaving an
        empty list, matching what an unset repeated field looks like on the
        wire. A negative value clamps to ``0`` rather than slicing from the
        wrong end (``list[-n:]`` for a negative ``n`` is not "empty").

        **No ceiling** (this field is deliberately exempt, unlike ``pageSize``):
        ``list[-n:]`` for an ``n`` larger than the list's own length is simply
        the whole list, not an amplification — a caller cannot get back more
        history than the task actually has, however large a value it asks for.

        Args:
            wire: A task's ``to_dict()`` output, mutated in place.
            history_length: The caller-supplied value, still untyped — every
                caller of this method reads it straight off ``params`` (or a
                nested ``configuration`` object) with no validation of its own.
        """
        if isinstance(history_length, bool) or not isinstance(history_length, int):
            return
        n = max(history_length, 0)
        if n == 0:
            wire.pop("history", None)
        else:
            wire["history"] = wire.get("history", [])[-n:]

    async def _dispatch_get_task(self, request_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Serve ``GetTask`` — a snapshot lookup by task id.

        Args:
            request_id: The JSON-RPC request id, echoed back verbatim.
            params: The request's already-validated params (``params.id`` is
                a non-empty string — enforced by the caller before dispatch).

        Returns:
            A JSON-RPC response: ``error`` with
            :class:`~novaos_client_sdk.exceptions.TaskNotFoundError`'s code
            when the task is unknown, else ``result`` holding a bare
            :class:`~novaos_client_sdk.a2a_task.Task` (not wrapped in
            ``SendMessage``'s oneof — only ``SendMessage`` uses that shape).
        """
        task_id = as_str(params.get("id"))
        task = await self._task_store.get(task_id)
        if task is None:
            return error_response(
                request_id, TaskNotFoundError.error_code, f"Unknown task {task_id!r}."
            )

        wire = task.to_dict()
        self._apply_history_length(wire, params.get("historyLength"))
        return success_response(request_id, wire)

    async def _dispatch_cancel_task(
        self, request_id: Any, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Serve ``CancelTask``, including preemption of a live execution.

        Writes ``TASK_STATE_CANCELED`` to the store first — and, when a live
        ``EventQueue`` is registered for this task, notifies a concurrent
        ``SubscribeToTask`` of the cancellation — then, if an execution is
        actually running this task (registered in
        :attr:`_active_cancellations` by :meth:`_send_message` /
        :meth:`_stream_send_message`), fires both preemption mechanisms on it:
        setting the cooperative :class:`~novaos_client_sdk.agent_context._CancellationToken`
        (which also cancels the skill's ``asyncio.Task``, once it exists — see
        that class's own docstring for the narrow "before the skill has even
        started" case). The store write happens first and unconditionally, so
        by the time either mechanism reaches the skill, ``GetTask`` already
        agrees with what the caller is about to see returned from here.

        **No separate ``AgentExecutor.cancel()``-style hook.** The reference
        A2A SDK's ``AgentExecutor`` is an ABC with its own ``cancel()`` method,
        called by the framework so an agent can stop deliberately and clean
        up. This SDK's skills are plain ``@agent.skill``-decorated functions,
        not objects with a lifecycle — adding a second callback surface to
        that decorator is deliberately out of scope for this design.
        Asyncio task cancellation plus the cooperative flag on
        :class:`~novaos_client_sdk.agent_context.AgentExecutionContext` are
        the whole story here: task cancellation interrupts an ``await``;
        :attr:`~novaos_client_sdk.agent_context.AgentExecutionContext.is_cancelled`
        / :meth:`~novaos_client_sdk.agent_context.AgentExecutionContext.raise_if_cancelled`
        cover a skill doing synchronous work between awaits.

        **Whether a skill that ignores or races the cancellation can still
        overwrite this write is decided at the other end, not here.**
        :meth:`_send_message` / :meth:`_stream_send_message` check the same
        :class:`~novaos_client_sdk.agent_context._CancellationToken` after
        their skill's task resolves — however it resolves — and discard a
        result that arrives after cancellation was requested rather than
        finalize it, which is what keeps a skill that "completes anyway"
        from clobbering the ``CANCELED`` write this method makes.

        Args:
            request_id: The JSON-RPC request id, echoed back verbatim.
            params: The request's already-validated params (``params.id`` is
                a non-empty string).

        Returns:
            A JSON-RPC response: ``error`` with
            :class:`~novaos_client_sdk.exceptions.TaskNotFoundError` when the
            task is unknown,
            :class:`~novaos_client_sdk.exceptions.TaskNotCancelableError` when
            it is already terminal, else ``result`` holding the task as
            canceled.
        """
        task_id = as_str(params.get("id"))
        task = await self._task_store.get(task_id)
        if task is None:
            return error_response(
                request_id, TaskNotFoundError.error_code, f"Unknown task {task_id!r}."
            )
        if task.status.state in TERMINAL_TASK_STATES:
            return error_response(
                request_id,
                TaskNotCancelableError.error_code,
                f"Task {task_id!r} is already in a terminal state and cannot be canceled.",
            )

        message = A2AMessage(
            parts=[TextPart(text="Task canceled.")],
            role=Role.AGENT,
            task_id=task_id,
            context_id=task.context_id,
        )
        # Reuse the live queue a concurrent SubscribeToTask may already be
        # attached to (the `_active_event_queues` registry), so that
        # subscriber observes this cancellation instead of seeing nothing. A
        # task with no in-flight execution — the common case, since a
        # cancellation usually targets a paused or already-returned task —
        # gets a throwaway queue nobody is reading from.
        event_queue = self._active_event_queues.get(task_id) or EventQueue()
        updater = TaskUpdater(self._task_store, event_queue, task_id, task.context_id or "")
        await updater.cancel(message)

        # The entry check above and this write
        # read the store at two different instants — a skill can finalize
        # (through its own, separate `TaskUpdater`) in between and win the
        # race, in which case `TaskUpdater._set_status`'s terminal guard now
        # makes the write above a silent no-op rather than letting it
        # downgrade the skill's own COMPLETED/FAILED back to CANCELED. That
        # protects the store, but this method must not then still tell the
        # caller cancellation *succeeded*: answer the same
        # `TaskNotCancelableError` an entry-time check would have given if
        # it had happened to run a moment later, and don't preempt an
        # execution that is (by this point) already over.
        updated_task = await self._task_store.get(task_id)
        assert updated_task is not None  # `cancel()` just saved it, or the skill already had
        if updated_task.status.state != TaskState.CANCELED:
            return error_response(
                request_id,
                TaskNotCancelableError.error_code,
                f"Task {task_id!r} is already in a terminal state and cannot be canceled.",
            )

        # Preempt the actual execution, if one is running. The
        # store write above already happened, so `cancellation.set()` firing
        # `.task.cancel()` (or a skill's own `raise_if_cancelled()` poll)
        # always lands *after* `GetTask` would already report CANCELED —
        # there is no ordering in which the skill's own eventual write could
        # win a race against a store write that already completed.
        cancellation = self._active_cancellations.get(task_id)
        if cancellation is not None:
            cancellation.set()

        return success_response(request_id, updated_task.to_dict())

    async def _dispatch_list_tasks(self, request_id: Any, params: dict[str, Any]) -> dict[str, Any]:
        """Serve ``ListTasks`` — a filtered, paginated enumeration.

        Every filter ``params`` supplies is applied — acceptance criterion 4
        is explicit that a filter the caller gave must never be silently
        ignored — and pagination follows :class:`~novaos_client_sdk.a2a_task.TaskList`'s
        own contract: ``pageToken`` is treated as "the last task id returned
        in the previous page", found in the filtered set and continued after;
        a stale or garbage token degrades to starting over rather than
        erroring the whole call, since a listing call should not fail outright
        over a caller's bookkeeping mistake.

        Args:
            request_id: The JSON-RPC request id, echoed back verbatim.
            params: The request's already-validated params (none are
                required).

        Returns:
            A JSON-RPC ``result`` holding a bare
            :class:`~novaos_client_sdk.a2a_task.TaskList` (not wrapped in
            ``SendMessage``'s oneof).
        """
        tasks = await self._task_store.list_tasks()

        context_id = params.get("contextId")
        if isinstance(context_id, str) and context_id:
            tasks = [t for t in tasks if t.context_id == context_id]

        raw_status = params.get("status")
        if isinstance(raw_status, str) and raw_status:
            wanted_state = task_state_from_wire(raw_status)
            tasks = [t for t in tasks if t.status.state == wanted_state]

        status_timestamp_after = params.get("statusTimestampAfter")
        if isinstance(status_timestamp_after, str) and status_timestamp_after:
            # Compared as parsed instants, not as strings: a
            # plain string comparison only works when both sides share one
            # fixed width, which `utc_timestamp()` now guarantees for every
            # *stored* task timestamp, but not for this caller-supplied
            # cutoff — a caller may send RFC 3339 in a different, still-valid
            # width (no fraction, a different number of fractional digits).
            # See `_parse_rfc3339`'s own docstring for the exact ordering bug
            # this avoids.
            cutoff = _parse_rfc3339(status_timestamp_after)
            if cutoff is not None:
                # An unparseable stored timestamp can't be proven "after" a
                # valid cutoff, so it is excluded rather than crashing the
                # whole call — stored timestamps are always our own
                # `utc_timestamp()` output in practice, so this only matters
                # as a defensive fallback.
                tasks = [
                    t
                    for t in tasks
                    if t.status.timestamp is not None
                    and (parsed := _parse_rfc3339(t.status.timestamp)) is not None
                    and parsed > cutoff
                ]
            # else: an unparseable cutoff degrades to "no filter applied"
            # rather than erroring the whole listing call or dropping every
            # task — the same garbage-input posture `pageToken`'s "start
            # over" handling takes below.

        total_size = len(tasks)

        page_token = params.get("pageToken")
        start = 0
        if isinstance(page_token, str) and page_token:
            for index, candidate in enumerate(tasks):
                if candidate.id == page_token:
                    start = index + 1
                    break
            # else: token not found in the filtered set — start from the
            # beginning rather than erroring (see docstring).

        raw_page_size = params.get("pageSize")
        page_size = (
            min(raw_page_size, MAX_LIST_TASKS_PAGE_SIZE)
            if isinstance(raw_page_size, int)
            and not isinstance(raw_page_size, bool)
            and raw_page_size > 0
            else DEFAULT_LIST_TASKS_PAGE_SIZE
        )

        page = tasks[start : start + page_size]
        next_page_token = page[-1].id if page and start + page_size < len(tasks) else None

        # historyLength: shared truncation rule, see `_apply_history_length`
        # (no ceiling here either — slicing self-limits to each task's actual
        # history size, so an oversized value cannot amplify the response the
        # way an oversized `pageSize` could).
        history_length = params.get("historyLength")

        # includeArtifacts is a wire-shape decision applied to each task's
        # `to_dict()` output below, distinct from the underlying `Task`
        # object — the stored tasks themselves are never mutated.
        include_artifacts = params.get("includeArtifacts") is True

        wire_tasks = []
        for t in page:
            wire = t.to_dict()
            self._apply_history_length(wire, history_length)
            if not include_artifacts:
                wire.pop("artifacts", None)
            wire_tasks.append(wire)

        task_list = TaskList(
            tasks=page, next_page_token=next_page_token, page_size=page_size, total_size=total_size
        )
        wire = task_list.to_dict()
        wire["tasks"] = wire_tasks
        return success_response(request_id, wire)

    async def _dispatch_push_notification_config(
        self, method: str, request_id: Any, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Serve all four ``*TaskPushNotificationConfig`` methods.

        One dispatch method for all four, since every one of them is gated by
        the exact same check first: if this agent was constructed with no
        ``push_notification_store``, all four answer A2A's own ``-32003``
        (:class:`~novaos_client_sdk.exceptions.PushNotificationNotSupportedError`)
        — never a mix where some of the four "work" and others don't. When a
        store *is* configured, every one of the four
        additionally requires the named task to exist —
        :class:`~novaos_client_sdk.exceptions.TaskNotFoundError` (``-32001``)
        otherwise, matching :class:`~novaos_client_sdk.client.Client`'s own
        docstrings for these methods.

        ``Create`` additionally runs the caller-supplied ``url`` through
        :func:`~novaos_client_sdk.dns.validate_agent_url` before it is ever
        persisted: a stored callback resolving to a private,
        loopback, link-local or otherwise reserved address never reaches
        :meth:`~novaos_client_sdk.a2a_push_notification_store.PushNotificationConfigStore.save`,
        rejected instead as ``-32602`` (``INVALID_PARAMS``). This is the same
        SSRF guard already applied to agent-discovery URLs
        (:mod:`novaos_client_sdk.discovery`); no host-suffix allowlist is
        passed, since a push-notification callback is, by design, any URL the
        caller controls, not a NovaOS-known host.

        Args:
            method: One of the four ``METHOD_*_TASK_PUSH_NOTIFICATION_CONFIG*``
                constants.
            request_id: The JSON-RPC request id, echoed back verbatim.
            params: The request's already-validated params.

        Returns:
            A JSON-RPC response, shaped per method — see each method's own
            handling below.
        """
        if self._push_notification_store is None:
            return error_response(
                request_id,
                PushNotificationNotSupportedError.error_code,
                "This agent does not support task push notification configuration.",
            )

        task_id = as_str(params.get("taskId"))
        task = await self._task_store.get(task_id)
        if task is None:
            return error_response(
                request_id, TaskNotFoundError.error_code, f"Unknown task {task_id!r}."
            )

        if method == METHOD_CREATE_TASK_PUSH_NOTIFICATION_CONFIG:
            config = TaskPushNotificationConfig.from_dict(params)
            try:
                validate_agent_url(config.url)
            except NovaOSError as exc:
                return error_response(
                    request_id,
                    INVALID_PARAMS,
                    f'"params.url" failed the SSRF guard: {exc}',
                )
            if not config.id:
                # Assign one so the caller can read it back off the response —
                # most callers leave `id` empty on a first registration.
                config.id = str(uuid4())
            await self._push_notification_store.save(config)
            return success_response(request_id, config.to_dict())

        if method == METHOD_GET_TASK_PUSH_NOTIFICATION_CONFIG:
            config_id = as_str(params.get("id"))
            config = await self._push_notification_store.get(task_id, config_id)
            if config is None:
                # Known, accepted ambiguity: an unknown task and an unknown
                # config both read -32001 here, since A2A has no dedicated
                # error code for "the task exists but that config id does
                # not". Fixing the ambiguity itself is out of scope for this
                # SDK — it is a gateway-side bug tracked separately.
                return error_response(
                    request_id,
                    TaskNotFoundError.error_code,
                    f"Unknown push notification config {config_id!r} for task {task_id!r}.",
                )
            return success_response(request_id, config.to_dict())

        if method == METHOD_LIST_TASK_PUSH_NOTIFICATION_CONFIGS:
            configs = await self._push_notification_store.list(task_id)

            page_token = params.get("pageToken")
            start = 0
            if isinstance(page_token, str) and page_token:
                for index, candidate in enumerate(configs):
                    if candidate.id == page_token:
                        start = index + 1
                        break
                # else: token not found — start over, same as ListTasks.

            raw_page_size = params.get("pageSize")
            page_size = (
                min(raw_page_size, MAX_LIST_TASKS_PAGE_SIZE)
                if isinstance(raw_page_size, int)
                and not isinstance(raw_page_size, bool)
                and raw_page_size > 0
                else DEFAULT_LIST_TASKS_PAGE_SIZE
            )

            page = configs[start : start + page_size]
            next_page_token = page[-1].id if page and start + page_size < len(configs) else None
            return success_response(
                request_id,
                TaskPushNotificationConfigList(
                    configs=page, next_page_token=next_page_token
                ).to_dict(),
            )

        assert method == METHOD_DELETE_TASK_PUSH_NOTIFICATION_CONFIG
        config_id = as_str(params.get("id"))
        # Idempotent per the store's own contract — deleting an already-absent
        # config is a no-op, not a 404, so this never re-checks existence.
        await self._push_notification_store.delete(task_id, config_id)
        return success_response(request_id, {})

    async def handle_a2a_streaming_request(
        self, body: Any, headers: Any
    ) -> tuple[dict[str, Any] | AsyncIterator[bytes], dict[str, str]]:
        """Handle one A2A ``SendStreamingMessage`` or ``SubscribeToTask`` request over SSE.

        This method originally served only ``SendStreamingMessage``;
        ``SubscribeToTask`` was added here rather than as a third entry point,
        since it shares the same reason for being separate from
        :meth:`handle_a2a_request_with_headers`: both return a byte stream
        that must start flowing before anything is known about how it ends,
        not a single JSON-RPC body. ``SendStreamingMessage``'s own behaviour
        is unchanged by this addition.

        Runs the skill live rather than buffering its result, which is the
        entire point: :attr:`AgentExecutionContext.tools` calls made through
        this request emit ``tool.call`` / ``tool.result`` onto the stream *as
        the skill makes them*, not only once it returns. A pod that never
        streams still gets tool visibility through the terminal message's
        metadata part (:meth:`_agent_message`); this is what makes it live.

        **Return contract.** Exactly one of two shapes:

        * ``(response_dict, response_headers)`` — envelope/version/extension
          negotiation failed, the method was neither ``SendStreamingMessage``
          nor ``SubscribeToTask``, or a ``SendStreamingMessage`` request named
          an unroutable skill. Nothing has been sent yet, so this is answered
          as an ordinary JSON-RPC response, matching what
          :meth:`handle_a2a_request_with_headers` would have returned for the
          same failure — a caller does not need to know which entry point it
          used to get a consistent error shape.
        * ``(frame_iterator, response_headers)`` — negotiation succeeded and
          either the skill is running (or about to, for
          ``SendStreamingMessage``) or a subscription is being served (for
          ``SubscribeToTask``). ``frame_iterator`` yields already SSE-encoded
          bytes (:func:`novaos_client_sdk._a2a.sse_data_line`), ready to hand
          an ASGI ``StreamingResponse`` (or equivalent) unmodified — this
          method owns the SSE framing so no two pod implementations format it
          slightly differently.

        A route distinguishes the two with ``isinstance(result, dict)``.

        Args:
            body: The JSON-decoded request body
            headers: Request headers; any mapping with ``.items()``

        Returns:
            See "Return contract" above.
        """
        request, envelope_error = parse_request(body)
        if envelope_error is not None:
            return envelope_error, {}
        assert request is not None  # parse_request returns exactly one of the two

        if request.method not in (METHOD_SEND_STREAMING_MESSAGE, METHOD_SUBSCRIBE_TO_TASK):
            return (
                error_response(
                    request.id,
                    METHOD_NOT_FOUND,
                    f"{request.method!r} was sent to the streaming entry point; only "
                    f"{METHOD_SEND_STREAMING_MESSAGE} and {METHOD_SUBSCRIBE_TO_TASK} are "
                    "served here.",
                ),
                {},
            )

        # `SubscribeToTask` has no message to validate — its only required
        # field is the task id — so it takes a different params check than
        # `SendStreamingMessage`'s `params.message` one. `raw_message` stays
        # `None` for it, which is also what tells the extension-activation
        # call below there is no message metadata to read.
        raw_message: dict[str, Any] | None = None
        subscribe_task_id: str | None = None
        if request.method == METHOD_SEND_STREAMING_MESSAGE:
            raw_message = request.params.get("message")
            if not isinstance(raw_message, dict):
                return (
                    error_response(
                        request.id,
                        INVALID_PARAMS,
                        f'"params.message" must be an A2A Message object, got '
                        f"{type(raw_message).__name__}.",
                    ),
                    {},
                )
        else:
            assert request.method == METHOD_SUBSCRIBE_TO_TASK
            raw_task_id = request.params.get("id")
            if not _is_nonempty_str(raw_task_id):
                return (
                    error_response(
                        request.id, INVALID_PARAMS, '"params.id" must be a non-empty string.'
                    ),
                    {},
                )
            subscribe_task_id = raw_task_id

        header_dict: dict[str, str] = {}
        if hasattr(headers, "items"):
            # A non-string key can't be a header name, so it is skipped rather
            # than crashing `.lower()` — the same skip-and-continue posture
            # this handler takes with every other caller-controlled field
            # (see `requested_protocol_version`'s identical guard).
            header_dict = {k.lower(): v for k, v in headers.items() if isinstance(k, str)}

        # Same ordering as handle_a2a_request_with_headers, for the same reason
        # (matches the reference dispatcher): envelope and params first, version
        # next, extensions last.
        version_problem = unsupported_version_reason(header_dict)
        if version_problem is not None:
            return (
                error_response(request.id, VersionNotSupportedError.error_code, version_problem),
                {},
            )

        message_metadata = raw_message.get("metadata") if raw_message is not None else None
        activation = self._extensions.activate(
            headers, request.params.get("metadata"), message_metadata
        )
        unmet = activation.unmet_requirement_reason()
        if unmet is not None:
            return error_response(request.id, ExtensionSupportRequiredError.error_code, unmet), {}

        response_headers = activation.response_headers()

        if request.method == METHOD_SUBSCRIBE_TO_TASK:
            assert subscribe_task_id is not None
            return (
                self._stream_subscribe_to_task(request.id, subscribe_task_id),
                response_headers,
            )

        assert raw_message is not None
        message = A2AMessage.from_dict(raw_message)
        skill_name, rejection = _resolve_skill(message, request.params, self._skills)
        if rejection is not None:
            # Nothing has streamed yet — an unroutable request is answered the
            # same way the non-streaming path answers it, as one JSON-RPC
            # result carrying a REJECTED task, not as a one-frame stream.
            novaos_context = novaos_context_from_metadata(
                request.params.get("metadata"), message.metadata
            )
            task_id = (
                as_str(message.task_id) or as_str(novaos_context.get("run_id")) or str(uuid4())
            )
            context_id = (
                as_str(message.context_id)
                or as_str(novaos_context.get("session_id"))
                or header_dict.get("x-session-id", "")
            )
            history = await self._continuation_history(task_id, message)
            rejected_task = await self._rejected(
                rejection, task_id=task_id, context_id=context_id, history=history
            )
            # Same nested `configuration.historyLength` bound
            # `_send_message_response` applies on the non-streaming path.
            self._apply_history_length(
                rejected_task, as_dict(request.params.get("configuration")).get("historyLength")
            )
            return (
                success_response(request.id, send_message_result(task=rejected_task)),
                response_headers,
            )

        assert skill_name is not None  # `rejection is None` guarantees this (see `_resolve_skill`)

        # Same request-level refusal `_send_message` makes,
        # before anything has streamed — an unroutable-content-type refusal
        # answers the same way the rejection branch above does: one JSON-RPC
        # error, no task created, no bytes sent yet. See
        # `_content_negotiation_error`'s own docstring for why this must run
        # ahead of `_stream_send_message`'s own task seeding rather than
        # inside it.
        negotiation_error = self._content_negotiation_error(request.id, skill_name, request.params)
        if negotiation_error is not None:
            return negotiation_error, response_headers

        return (
            self._stream_send_message(request.id, request.params, message, header_dict, activation),
            response_headers,
        )

    @staticmethod
    def _task_event_frame(request_id: Any, event: TaskEvent) -> bytes:
        """Encode one ``TaskStatusUpdateEvent``/``TaskArtifactUpdateEvent`` as its SSE frame.

        The one piece of framing genuinely shared between :meth:`_stream_send_message`'s
        live relay of its own execution's ``EventQueue`` and
        :meth:`_stream_subscribe_to_task`'s relay of the same queue for a
        concurrent subscriber — both read a :data:`TaskEvent` off a
        subscription and must turn it into the exact same wire shape, since a
        caller of either method sees the same event types on the same
        methods (``statusUpdate``/``artifactUpdate``).
        """
        if isinstance(event, TaskStatusUpdateEvent):
            return sse_data_line(success_response(request_id, {"statusUpdate": event.to_dict()}))
        return sse_data_line(success_response(request_id, {"artifactUpdate": event.to_dict()}))

    async def _stream_send_message(
        self,
        request_id: Any,
        params: dict[str, Any],
        message: A2AMessage,
        header_dict: dict[str, str],
        activation: ExtensionActivation,
    ) -> AsyncIterator[bytes]:
        """Run one already-routable skill, yielding SSE frames as it goes.

        Mirrors ``_execute_skill`` inside :meth:`_send_message` — same task/
        context id resolution, same terminal-state mapping, same artifact
        construction — but as a generator instead of a single return, and with
        one addition: :attr:`AgentExecutionContext.tools` calls the skill makes
        are drained from an internal queue and republished as
        ``TaskStatusUpdateEvent`` frames *while the skill is still running*,
        which is the entire reason this method exists rather than reusing
        ``_send_message`` and wrapping its return value in a one-frame stream.

        Skill selection already happened in :meth:`handle_a2a_streaming_request`
        (a rejection is answered there, before any bytes are sent) — this method
        only ever runs an already-resolved skill.

        **This task is no longer invisible off this
        stream.** Previously, this method built its SUBMITTED/WORKING/terminal
        lifecycle purely as SSE frames, without ever touching
        :attr:`_task_store` — a task created via ``SendStreamingMessage`` had
        no way to answer a later ``GetTask``/``CancelTask``/``ListTasks``/
        ``SubscribeToTask``, which defeated the point of serving those methods
        at all for a streaming caller. This method now carries the same
        ``TaskStore``/``EventQueue``/``TaskUpdater`` plumbing
        :meth:`_send_message` already had, as a side effect of the *same*
        state transitions — the store and the shared ``EventQueue`` in
        :attr:`_active_event_queues` are a second, additional destination for
        the same information, not a replacement for *this* stream's own
        frames.

        **This stream also drains its own ``EventQueue`` live.**
        Previously, a skill's own ``ctx.task_updater.update_status(WORKING,
        ...)``/``add_artifact(...)`` calls were written to the store and
        enqueued for a *concurrent* ``SubscribeToTask``/``GetTask`` to see,
        but never surfaced on this call's own SSE response — this method built
        that response purely from local computation (the auto-synthesised
        output artifact, then the terminal ``statusUpdate``), so a skill
        reporting progress mid-run was invisible on the very stream it was
        running under. This method now holds its own subscription (via
        :meth:`~novaos_client_sdk.a2a_event_queue.EventQueue.subscribe`,
        never the shared, single-consumer
        :meth:`~novaos_client_sdk.a2a_event_queue.EventQueue.dequeue_event` —
        the same reasoning :meth:`_stream_subscribe_to_task` gives for its own
        subscription applies here too: a plain shared queue delivers each
        event to only one caller, and a concurrent ``SubscribeToTask`` on this
        same task must keep seeing every event regardless of what this
        method's own subscription happens to drain first) and relays every
        event it reads — via :meth:`_task_event_frame`, the same encoding
        :meth:`_stream_subscribe_to_task` uses — interleaved with running the
        skill, for as long as the skill's own task is not yet done. Once the
        skill returns, this method reverts to building its terminal frames
        from local computation exactly as before, rather than continuing to
        read from the subscription — ``_finalize_task`` below re-enqueues the
        very same output-artifact and terminal-status events onto this
        queue for a concurrent subscriber's benefit, and relaying those a
        second time here would double-deliver them on this stream.

        **The tool-event queue (`events`, below) is deliberately not part of
        this.** It is a NovaOS-specific side channel carried only on this
        method's own SSE stream — `tool.call`/`tool.result` frames a
        `SubscribeToTask` caller has no reason to see, since they are not part
        of the core A2A `TaskStatusUpdateEvent`/`TaskArtifactUpdateEvent`
        model at all. Only the SUBMITTED→WORKING→terminal lifecycle and
        artifacts go through the shared store/queue; `events` stays exactly
        as it was.

        **How this stream ends when the skill does not.** The
        module docstring's "Interruption policy" is the contract; this is
        where it lands for the streaming path. A skill whose own coroutine
        ends in an ``asyncio.CancelledError`` no ``CancelTask`` requested
        (rule 2) ends this generator *normally* — no terminal frame, no
        store write, a clean SSE close — instead of re-raising into the ASGI
        response and aborting it. A request abandoned mid-skill (rule 3 —
        client disconnect, the driving task cancelled, ``aclose()``) hands
        the still-running skill to :meth:`_detach_execution` and leaves the
        store untouched, rather than cancelling the skill and labelling the
        task ``CANCELED`` as before. Only a ``CancelTask`` (rule 1) produces
        ``CANCELED``, and its handling here is exactly as before.
        """
        novaos_context = novaos_context_from_metadata(params.get("metadata"), message.metadata)
        task_id = as_str(message.task_id) or as_str(novaos_context.get("run_id")) or str(uuid4())
        context_id = (
            as_str(message.context_id)
            or as_str(novaos_context.get("session_id"))
            or header_dict.get("x-session-id", "")
        )

        skill_name, rejection = _resolve_skill(message, params, self._skills)
        assert rejection is None and skill_name is not None  # resolved by the caller

        # Accumulates onto a still-open task's own stored
        # history when continuing it, resets to `[message]` for a fresh or
        # already-terminal one — see `_continuation_history`'s own docstring.
        history = await self._continuation_history(task_id, message)

        # Seed the store with a SUBMITTED task before running the
        # skill, exactly as `_send_message` does — so this task exists for a
        # concurrent `GetTask`/`CancelTask`/`ListTasks`/`SubscribeToTask` from
        # the moment this call acknowledges it.
        # Everything this execution registers is
        # created and registered *synchronously* here — supersede, queue,
        # token, updater — with the store seed's `await` only afterwards. The
        # order matters twice over. Supersede-then-register with no `await`
        # between means two requests entering on the same task id together
        # cannot both miss each other (the second always finds the first's
        # token); register-before-seed means the token exists for a
        # concurrent `CancelTask` from the very first moment the task can be
        # observed in the store at all (the original invariant, now
        # strictly stronger). `_supersede_live_execution` takes over from any
        # execution — detached after its own request was abandoned, or simply
        # still in flight — that already owns this task id, rather than
        # letting a second skill run against the same task.
        self._supersede_live_execution(task_id)
        event_queue = EventQueue()
        task_updater = TaskUpdater(self._task_store, event_queue, task_id, context_id)
        cancellation = _CancellationToken()
        cancellation.updater = task_updater
        # Registered *before* the skill runs, so a concurrent `SubscribeToTask`
        # can attach to this task the moment it exists in the store. Popped in
        # `finally` below (identity-checked) — including on the exception
        # path — since an exception here must not leave a stale, closed queue
        # registered for a subscriber to wait on forever.
        self._active_event_queues[task_id] = event_queue
        self._active_cancellations[task_id] = cancellation
        try:
            await self._task_store.save(
                self._build_task(
                    TaskState.SUBMITTED,
                    None,
                    task_id=task_id,
                    context_id=context_id,
                    history=history,
                )
            )
        except BaseException:
            # The seed failed (or this request was cancelled during it) before
            # this call's `try`/`finally` below ever began, so nothing else
            # will unregister what was registered just above — or close the
            # queue: a `SubscribeToTask` that attached in this window (the
            # task id's prior incarnation was still in the store) would
            # otherwise wait forever on an unregistered queue nobody closes.
            # Identity-checked pops, like every other.
            await event_queue.close()
            if self._active_event_queues.get(task_id) is event_queue:
                del self._active_event_queues[task_id]
            if self._active_cancellations.get(task_id) is cancellation:
                del self._active_cancellations[task_id]
            raise

        events: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()

        def emit_tool_event(event_type: str, payload: dict[str, Any]) -> None:
            events.put_nowait((event_type, payload))

        referenced_tasks, unresolved_reference_task_ids = await self._resolve_referenced_tasks(
            message.reference_task_ids, context_id
        )
        ctx = AgentExecutionContext.from_a2a_request(
            message,
            params,
            header_dict,
            task_id=task_id,
            extensions=activation,
            emit_tool_event=emit_tool_event,
            task_updater=task_updater,
            cancellation=cancellation,
            referenced_tasks=referenced_tasks,
            unresolved_reference_task_ids=unresolved_reference_task_ids,
        )

        def status_frame(state: TaskState, status_message: A2AMessage | None = None) -> bytes:
            update = TaskStatusUpdateEvent(
                status=TaskStatus(state=state, message=status_message, timestamp=utc_timestamp()),
                task_id=task_id,
                context_id=context_id,
            )
            return sse_data_line(success_response(request_id, {"statusUpdate": update.to_dict()}))

        def metadata_message(payload_key: str, payload: dict[str, Any]) -> A2AMessage:
            # One key per part, matching `collectToolEvents`'s read side
            # (the platform's A2A wire contract) exactly: it discriminates a
            # tool event by which of "tool.call"/"tool.result" is present in
            # the part's `data`, so cramming both into one part — or one part
            # per skill run instead of one per event — would collide the way
            # `drainParts`'s merge collides same-keyed parts within one message.
            return A2AMessage(
                parts=[
                    DataPart(
                        data={payload_key: payload},
                        metadata={NOVAOS_METADATA_PART_KEY: NOVAOS_METADATA_CONTENT_TYPE},
                    )
                ],
                role=Role.AGENT,
                task_id=task_id,
                context_id=context_id,
            )

        def is_terminal_status_event(event: TaskEvent) -> bool:
            # A *terminal* status event can reach
            # `own_subscription` while `skill_task` is still not `done()` —
            # concretely, `_dispatch_cancel_task` writes CANCELED onto this
            # exact `event_queue` (via `_active_event_queues`) before it ever
            # cancels `skill_task` itself, so this loop can observe the
            # CANCELED event before the cancellation has actually unwound the
            # skill's own coroutine. Forwarding it here would double-deliver
            # it: the terminal frame this generator sends is always the one
            # built below from a fresh store read, once `skill_task` actually
            # finishes — reached whether the terminal write came from a
            # concurrent `CancelTask` (racing in like this), from
            # `_finalize_task` (only ever called after this loop has already
            # exited, so never observed here at all), or from the skill
            # itself misusing `ctx.task_updater`'s terminal methods. A
            # skill's own *non-terminal* progress — `WORKING` from
            # `update_status`, `INPUT_REQUIRED`/`AUTH_REQUIRED` from
            # `requires_input`/`requires_auth` — has no such dedicated path
            # and must still be relayed.
            return isinstance(event, TaskStatusUpdateEvent) and (
                event.status.state in TERMINAL_TASK_STATES
            )

        handler = self._skills[skill_name]["handler"]
        input_text = message.text

        # Runs once, before the skill starts — see `_maybe_prefetch_memory`'s
        # own docstring for why this call site (not inside the skill) is
        # what makes "at most once per turn" hold.
        await _maybe_prefetch_memory(ctx, input_text)

        # Both bound before the outer `try` so the `except (GeneratorExit,
        # asyncio.CancelledError)` clause below can always safely check
        # them — abandonment can in principle strike before the skill even
        # starts (see that clause's own docstring), or while `get_event` is
        # the awaitable actually parked inside `asyncio.wait` below.
        skill_task: asyncio.Task[SkillResult] | None = None
        get_event: asyncio.Task[tuple[str, dict[str, Any]]] | None = None
        # `own_subscription`'s own in-flight read, bound
        # alongside `get_event` for the identical reason — the abandonment
        # handler below must be able to cancel it too.
        get_task_event: asyncio.Task[TaskEvent | None] | None = None
        # Bound alongside `skill_task` above, for
        # the same reason — `finally` below must be able to copy it back onto
        # the ambient context no matter which of this `try`'s exits was
        # taken, including one that struck before `skill_task` was ever
        # created.
        skill_context: contextvars.Context | None = None
        # `published` flips once this generator has put the
        # skill's outcome in the store (or established there is none to put —
        # a `CancelTask` already wrote CANCELED, or the skill ended without
        # one); `detached` flips once the abandonment handler below has
        # handed a still-unpublished skill to `_detach_execution`, so
        # `finally` knows the queue and registry entries are no longer its
        # to close and pop.
        published = False
        detached = False
        try:
            # A `CancelTask` can race this call all the way back
            # to the SUBMITTED store-seed's own `await` above — including
            # before `cancellation` was registered at all, in which case
            # `CancelTask` found no token to set — so this reads the store
            # itself rather than trusting `cancellation.is_set`. Starting the
            # skill now (or even marking it WORKING) would clobber whatever
            # terminal state a concurrent call already wrote.
            current = await self._task_store.get(task_id)
            if current is not None and current.status.state in TERMINAL_TASK_STATES:
                yield status_frame(current.status.state, current.status.message)
                return

            # Moved inside this `try` (it used
            # to run before it) so that a cancellation landing on this exact
            # await is caught by the same `except` clause as every later
            # one, instead of propagating straight past `finally` with the
            # task left at `TASK_STATE_SUBMITTED` and its `EventQueue` never
            # closed.
            await task_updater.start_work()

            # A `CancelTask` landing during the
            # `await` above no longer clobbers the store — `TaskUpdater.
            # _set_status`'s terminal guard now refuses that write — but
            # `start_work()` itself still returns normally either way,
            # nothing raises to say its write was silently skipped. Check
            # the flag `_dispatch_cancel_task` sets before ever starting the
            # skill, so a cancellation that arrived mid-`start_work()`
            # doesn't still run it to completion for nothing.
            if cancellation.is_set:
                canceled = await self._canceled_snapshot(task_updater, task_id)
                yield status_frame(canceled.status.state, canceled.status.message)
                return

            # This call's own subscription onto `event_queue`, so
            # the loop below can relay the skill's *own* progress calls back
            # onto this SSE stream. Taken here — after `start_work()` above,
            # before `handler` is ever scheduled — rather than back when
            # `event_queue` was created, so it captures only what the skill
            # itself reports through `ctx.task_updater` (this method's own
            # SUBMITTED→WORKING bookkeeping stays exactly as invisible on this
            # stream as it always was; that is not "the skill's own
            # progress"). A subscription rather than `event_queue.
            # dequeue_event()`'s shared single-consumer queue, for the same
            # reason `_stream_subscribe_to_task` needs one of its own: either
            # could otherwise steal an event a concurrent `SubscribeToTask`
            # call also needs to see.
            own_subscription = event_queue.subscribe()

            # Captured before the task is created and passed in
            # explicitly as its `context=`, rather than letting
            # `asyncio.create_task` default to its own fresh copy — see the
            # module docstring's "Skill dispatch" note. `skill_context` is
            # copied back onto the ambient context in `finally` below no
            # matter how this method's outer `try` exits, so a
            # `ContextVar.set()` inside the skill body survives past this
            # generator resuming from the `await`/`yield` points in the loop
            # below — abandonment mid-skill included.
            skill_context = contextvars.copy_context()
            skill_task = asyncio.create_task(handler(ctx, input_text), context=skill_context)
            # Lets `_dispatch_cancel_task` reach this exact task
            # through `cancellation` even though it lives in a different
            # request's coroutine.
            cancellation.task = skill_task
            try:
                while not skill_task.done():
                    get_event = asyncio.ensure_future(events.get())
                    # Raced in the same `asyncio.wait` as
                    # `get_event`/`skill_task` rather than polled separately —
                    # a separate `await own_subscription.get()` before or
                    # after this wait would itself block the loop, silently
                    # delaying tool events (or skill completion) behind
                    # whichever source happens to go first.
                    get_task_event = asyncio.ensure_future(own_subscription.get())
                    done, _pending = await asyncio.wait(
                        {skill_task, get_event, get_task_event},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # Both `done` memberships are read out to plain local
                    # frames *before* either is ever yielded.
                    # A `yield` suspends this generator but not the event
                    # loop — `get_task_event` can complete in the background
                    # while this generator sits suspended on the tool-event
                    # `yield` below, and checking it against `done` only
                    # *after* resuming from that yield would check a stale
                    # snapshot: `get_task_event` reads as "not in done", takes
                    # the `else` branch, and `.cancel()`s a task that already
                    # finished — a no-op that silently discards the real
                    # `TaskEvent` `.result()` would have returned, since
                    # nothing else ever reads it. Resolving both futures
                    # against the one `done` set back-to-back, with no
                    # `await`/`yield` between them, closes that window.
                    tool_frame: bytes | None = None
                    if get_event in done:
                        event_type, payload = get_event.result()
                        tool_frame = status_frame(
                            TaskState.WORKING, metadata_message(event_type, payload)
                        )
                    else:
                        get_event.cancel()
                    task_frame: bytes | None = None
                    if get_task_event in done:
                        # `own_subscription` only ever yields its closing
                        # `None` sentinel once `event_queue.close()` runs in
                        # `finally` below — which cannot happen while this
                        # loop, still inside the same `try`, is still
                        # running — so a real `TaskEvent` is the only thing
                        # this can observe here.
                        task_event = get_task_event.result()
                        assert task_event is not None
                        if not is_terminal_status_event(task_event):
                            task_frame = self._task_event_frame(request_id, task_event)
                    else:
                        get_task_event.cancel()
                    if tool_frame is not None:
                        yield tool_frame
                    if task_frame is not None:
                        yield task_frame

                # The skill finished with events still queued — e.g. its last
                # tool call resolved after its final `events.get()` had
                # already lost the `asyncio.wait` race to `skill_task`
                # completing. Drain the rest before the terminal frame so no
                # tool event — or skill-reported progress event — is silently
                # dropped.
                while not events.empty():
                    event_type, payload = events.get_nowait()
                    yield status_frame(TaskState.WORKING, metadata_message(event_type, payload))
                while not own_subscription.empty():
                    task_event = own_subscription.get_nowait()
                    assert task_event is not None
                    if not is_terminal_status_event(task_event):
                        yield self._task_event_frame(request_id, task_event)

                try:
                    result: SkillResult | None = skill_task.result()
                except asyncio.CancelledError:
                    # `skill_task` was cancelled by
                    # `_dispatch_cancel_task` (in which case `cancellation` is
                    # already set and the store already carries CANCELED) —
                    # or the skill's own coroutine ended in a
                    # `CancelledError` nobody asked for: the skill raised it
                    # itself, or something it awaited did. `asyncio.wait`
                    # above never propagates *this* generator's own
                    # cancellation into `skill_task`, so there is no third
                    # reading. The second case is rule 2 of the module
                    # docstring's "Interruption policy": the skill gave up
                    # without an outcome, so there is nothing to write and
                    # nothing to frame — end this stream cleanly, right here,
                    # with the task left at whatever the skill last wrote
                    # (WORKING, typically). Never mislabel it CANCELED,
                    # and never re-raise it into the
                    # ASGI response either: that is what made the response
                    # abort instead of close (see the module docstring).
                    if not cancellation.is_set:
                        published = True
                        return
                    result = None

                if cancellation.is_set:
                    # Covers both halves of the "skill completes anyway"
                    # race (acceptance criterion 1): `result` is `None` from
                    # the `except` above, or the skill ignored/caught its
                    # own `CancelledError` and returned a `SkillResult`
                    # regardless. Either way `_dispatch_cancel_task` already
                    # wrote CANCELED before it ever set this flag (see that
                    # method's own docstring), so that write — not whatever
                    # `result` holds — is this task's real outcome.
                    published = True
                    canceled = await self._canceled_snapshot(task_updater, task_id)
                    yield status_frame(canceled.status.state, canceled.status.message)
                    return

                assert result is not None
            except Exception as e:
                # A plain (non-`CancelledError`)
                # exception from the skill — including one caused *by* being
                # interrupted, if the skill's own code translates
                # cancellation into some other exception type — must not be
                # reported as FAILED once `CancelTask` already answered
                # CANCELED. `TaskUpdater._set_status`'s terminal guard now
                # keeps the *store* correct either way, but this method's own
                # response is built from local state below, not read back
                # from the store, so it needs the same check.
                if cancellation.is_set:
                    published = True
                    canceled = await self._canceled_snapshot(task_updater, task_id)
                    yield status_frame(canceled.status.state, canceled.status.message)
                    return

                failed_message = self._failed_message(e, task_id=task_id, context_id=context_id)
                await self._finalize_task(
                    task_updater, TaskState.FAILED, failed_message, artifacts=[]
                )
                published = True
                # Read back what `_finalize_task`
                # actually persisted rather than assuming it was FAILED — the
                # `cancellation.is_set` check above already covers the common
                # case, but a `CancelTask` landing in the narrow window
                # between that check and this write would make the write
                # above a silent no-op (the store correctly stays CANCELED),
                # and yielding a frame built from `failed_message` here would
                # still hand the caller a stale FAILED that disagrees with it.
                final_task = await self._task_store.get(task_id)
                assert final_task is not None
                yield status_frame(final_task.status.state, final_task.status.message)
                return

            state, result = self._task_state_for(result, skill_name)
            final_message = self._agent_message(
                result, task_id=task_id, context_id=context_id, buffered_events=ctx.buffered_events
            )
            # Skip building and streaming the
            # returned `SkillResult`'s artifact at all when the skill has
            # already sealed the task itself — `_finalize_task` below will
            # refuse to attach it to the store for the same reason (see its
            # own docstring), but by then this loop would already have put
            # an `artifactUpdate` frame for the *losing* outcome on the wire,
            # ahead of the terminal frame for the outcome that actually won.
            artifacts = (
                []
                if task_updater.is_sealed
                else self._output_artifacts(result, final_message, skill_name=skill_name)
            )
            for artifact in artifacts:
                update = TaskArtifactUpdateEvent(
                    artifact=artifact, task_id=task_id, context_id=context_id, last_chunk=True
                )
                yield sse_data_line(
                    success_response(request_id, {"artifactUpdate": update.to_dict()})
                )
            await self._finalize_task(task_updater, state, final_message, artifacts=artifacts)
            published = True
            # Same reasoning as the FAILED tail
            # above — read the actually-persisted state back from the store
            # rather than assuming `_finalize_task`'s write went through as
            # `state`/`final_message`.
            final_task = await self._task_store.get(task_id)
            assert final_task is not None
            yield status_frame(final_task.status.state, final_task.status.message)
            # A2A's `Task.history` — what `_send_message` returns on the
            # non-streaming path — has no frame of its own here: a streaming
            # caller accumulates its own view from the frames it already
            # received, the same asymmetry `Task.from_dict`'s docstring notes
            # for a client reading a snapshot versus a stream. This is also
            # why a nested `configuration.historyLength` has
            # nothing to bound on *this* method's own output — there is no
            # `history`-bearing frame here to truncate. It is still honoured
            # for whatever `Task.history` this task's stored `SUBMITTED` seed
            # carries (via `_continuation_history` above) and for whatever a
            # later `GetTask`/`ListTasks`/`SubscribeToTask` reads back, the
            # same as it always was — a streaming caller wanting a
            # length-bounded history reads it from one of those, not from
            # this stream's own frames.
        except (GeneratorExit, asyncio.CancelledError):
            # The caller abandoned this stream — a client disconnect, or
            # this generator's own driving task being cancelled, or the
            # generator being closed — before the skill reached a terminal
            # state. Rule 3 of the module docstring's "Interruption policy":
            # this is not a decision about the *task*.
            # `skill_task`, once it exists, is a plain `asyncio.Task`
            # independent of this generator's lifecycle, and it is left
            # running on purpose — an A2A task outlives the request that
            # started it. What must not happen is its outcome never
            # reaching the store because the code that would have published
            # it (the tail of the `try` above) is now unreachable — an
            # earlier finding, which a prior version answered by
            # cancelling the skill and writing CANCELED — the wrong label
            # for a caller that merely went away). So the skill is handed
            # to a detached finisher, which publishes whatever it eventually
            # produces through the same updater and queue, then does the
            # cleanup `finally` below skips for it. A skill that already
            # finished but whose outcome this generator had not yet
            # published (abandoned mid-`_finalize_task`, say) is detached
            # the same way — the finisher awaits an already-done task and
            # publishes from it.
            if skill_task is not None and not published:
                self._detach_execution(
                    skill_task=skill_task,
                    cancellation=cancellation,
                    task_updater=task_updater,
                    event_queue=event_queue,
                    task_id=task_id,
                    context_id=context_id,
                    skill_name=skill_name,
                )
                detached = True
            # `get_event` is this generator's own internal helper task for
            # draining `events`, not something a caller ever sees — but
            # leaving it running unawaited past this point is exactly the
            # kind of dangling task asyncio warns about at shutdown.
            if get_event is not None and not get_event.done():
                get_event.cancel()
            # `get_task_event` is this generator's counterpart
            # helper task for draining `own_subscription` — same dangling-task
            # concern as `get_event` above.
            if get_task_event is not None and not get_task_event.done():
                get_task_event.cancel()
            # Abandonment must still propagate to whoever drove this
            # generator — Starlette's response machinery, a test's
            # `aclose()` — so this never swallows it. Nothing here is a
            # `CancelTask`: `cancellation` is deliberately *not* set, so a
            # detached skill polling `ctx.is_cancelled` keeps running.
            raise
        finally:
            # Unlike `_execute_skill`'s own
            # `finally`, this one guards every exit out of the *outer* `try`
            # above — the happy path, the plain-exception path, *and* the
            # `except (GeneratorExit, asyncio.CancelledError)` abandonment
            # path — not just the inner `try` around `skill_task.result()`.
            # Copying back only after the drain loops (as a first version of
            # this fix did) skipped exactly the abandonment case: a stream
            # torn down while `skill_task` was still running raises straight
            # out of the `while`/`asyncio.wait` above, past that point
            # entirely, discarding whatever the skill had already `.set()`
            # before it was interrupted. `skill_context` reflects that
            # mutation regardless of when the task stopped running — a
            # `contextvars.Context` is mutated in place by `.set()` the
            # moment it runs, not only once the task holding it finishes —
            # so this is safe to replay even for a `skill_task` that never
            # reached `done()`.
            if skill_context is not None:
                for var in skill_context:
                    var.set(skill_context[var])
            # A detached execution's queue stays open and
            # registered — closing it here would hand every concurrent
            # subscriber the "nothing more is coming" sentinel while the
            # skill is in fact still running, and popping the registry would
            # hide it from the `CancelTask` that is now the only way to stop
            # it. `_finish_detached_execution` owns both once it has
            # published.
            if not detached:
                await event_queue.close()
                # Identity-checked: a later request on this
                # same task id may have superseded this execution and
                # registered its own queue and token — those are its to pop,
                # not this call's.
                if self._active_event_queues.get(task_id) is event_queue:
                    del self._active_event_queues[task_id]
                if self._active_cancellations.get(task_id) is cancellation:
                    del self._active_cancellations[task_id]

    async def _stream_subscribe_to_task(
        self, request_id: Any, task_id: str
    ) -> AsyncIterator[bytes]:
        """Serve ``SubscribeToTask`` — reconnect to a task's update stream.

        Per spec (§3.1.6, and :meth:`~novaos_client_sdk.client.Client.subscribe_to_task`'s
        own docstring on the client side of this same contract): **the first
        frame is always the task's current snapshot.** Everything after that
        depends on what this task is doing right now:

        * **Unknown task id** — one error frame
          (:class:`~novaos_client_sdk.exceptions.TaskNotFoundError`, ``-32001``)
          and nothing else. The spec has no notion of a mid-stream error
          distinct from an error frame, so this reads the same way a caller
          would read a JSON-RPC error arriving as the payload of an SSE line —
          the same framing every other frame in this module uses.
        * **Already terminal** — the snapshot is the whole stream (acceptance
          criterion 6: subscribing to an already-finished task must not hang).
          This is the "immediate terminal frame" option the ticket poses as a
          choice, taken over evicting the task from the store — eviction is
          :class:`~novaos_client_sdk.a2a_task_store.InMemoryTaskStore`'s own
          LRU policy's business, not this method's.
        * **Not terminal, and no live execution is driving it** — the
          snapshot is also the whole stream. This is the common case: a task
          paused in ``INPUT_REQUIRED``/``AUTH_REQUIRED`` was produced by a
          call that already returned and closed its ``EventQueue``, since
          ``AgentBuilder`` runs a skill to completion or interruption
          synchronously within one call. Waiting on nothing here would hang
          forever, so a caller gets a one-frame snapshot instead — still
          spec-conformant, since the guarantee above is only about the
          *first* frame. A ``WORKING`` task whose skill ended itself without
          an outcome ("Interruption policy" rule 2) lands here
          too: its queue was closed when that request ended, and the one-
          frame snapshot is the honest answer.
        * **Not terminal, and a *detached* execution is driving it** (rule
          3 — the request that started the skill was abandoned,
          the skill was not) — indistinguishable from the next bullet on
          purpose: the finisher keeps the ``EventQueue`` registered until it
          has published the outcome, so this is exactly the reconnect
          §3.1.6 exists for, and the terminal frame arrives when the skill
          finishes.
        * **Not terminal, and a live execution is driving it** — this method
          registers its own subscriber queue on the same
          :class:`~novaos_client_sdk.a2a_event_queue.EventQueue` the
          in-flight :meth:`_send_message` / :meth:`_stream_send_message` call
          registered in :attr:`_active_event_queues` (via
          :meth:`~novaos_client_sdk.a2a_event_queue.EventQueue.subscribe`,
          not the shared, single-consumer
          :meth:`~novaos_client_sdk.a2a_event_queue.EventQueue.dequeue_event`
          — a plain shared queue delivers each event to only one caller, so
          two concurrent ``SubscribeToTask`` calls on the same task would
          otherwise silently split the stream between them, each missing
          whatever the other happened to dequeue first, and only one of
          them ever seeing the terminal frame), and republishes every event
          it reads as its own SSE frame until a status update carries a
          terminal or interrupt state (acceptance criterion 7: a streaming
          response must always end on a terminal-or-interrupt frame, never
          close silently). This cannot hang forever, even if the producer
          abandons the task without ever reaching a terminal state itself:
          :meth:`~novaos_client_sdk.a2a_event_queue.EventQueue.close`
          delivers a closing sentinel (``None``) to every subscriber, which
          this method reads as "stop waiting, nothing more is coming" and
          ends the stream rather than forwarding it as a frame.
          An artifact update alone never ends the loop, mirroring
          :func:`~novaos_client_sdk.a2a_events.is_last_frame`'s own logic
          rather than reinventing it.

        Args:
            request_id: The JSON-RPC request id, echoed back verbatim.
            task_id: The task to subscribe to (``params.id``, already
                validated as a non-empty string by the caller).

        Yields:
            SSE-encoded bytes, per the framing above.
        """
        task = await self._task_store.get(task_id)
        if task is None:
            yield sse_data_line(
                error_response(
                    request_id, TaskNotFoundError.error_code, f"Unknown task {task_id!r}."
                )
            )
            return

        # Look up (and subscribe to) any live producer's queue *before*
        # yielding the snapshot below, not after. A `yield`
        # suspends this generator, and every `await` before it is itself a
        # point where the producer this method is trying to attach to can
        # run and finish: if the registry lookup happened after the
        # snapshot yield, a producer that completed in that gap would
        # already have popped itself out of `_active_event_queues` by the
        # time this method got around to checking it, silently truncating
        # the stream to a stale snapshot with no terminal frame at all —
        # not merely a delayed one. Subscribing here instead means this
        # method holds a direct reference to the `EventQueue` object itself;
        # once held, that reference keeps receiving every event the
        # producer enqueues regardless of whether the producer has since
        # removed its own registry entry, since `enqueue_event` fans out to
        # every subscriber it was ever given, not just whoever is still in
        # the registry.
        event_queue = self._active_event_queues.get(task_id)
        subscription = event_queue.subscribe() if event_queue is not None else None

        # Re-read the task now that a subscription (if any) is locked in,
        # so the snapshot yielded below is as fresh as it can be — anything
        # the producer does *after* this point is exactly what the
        # subscription itself reports, not something this snapshot needs to
        # anticipate.
        task = await self._task_store.get(task_id) or task
        # Read *now*, synchronously alongside `to_dict()` below and before
        # the `yield` that follows — not after resuming from it. `Task` is
        # a plain mutable dataclass and `InMemoryTaskStore.get()` hands back
        # the same object it stores, not a copy: `TaskUpdater._set_status`
        # mutates that shared object's `.status` in place before enqueuing
        # its event, so if this read happened *after* the yield, a producer
        # that finished while this generator sat suspended would have
        # already flipped the very object `task` still points to — making
        # this check see the *new* terminal state while the snapshot this
        # method already sent over the wire still shows the *old* one, and
        # then returning without ever sending a frame that actually reports
        # the terminal state at all.
        is_terminal = task.status.state in TERMINAL_TASK_STATES
        yield sse_data_line(success_response(request_id, {"task": task.to_dict()}))

        if is_terminal:
            return

        if subscription is None:
            return

        while True:
            event = await subscription.get()
            if event is None:
                # The producer closed without this subscription ever seeing
                # a terminal-or-interrupt frame — an abandoned stream, or a
                # task canceled before it reached one. Stop rather than
                # waiting for an event that will never arrive; do not
                # forward this sentinel as a frame.
                return
            yield self._task_event_frame(request_id, event)
            if isinstance(event, TaskStatusUpdateEvent) and (
                event.status.state in TERMINAL_TASK_STATES
                or event.status.state in INTERRUPT_TASK_STATES
            ):
                return

    async def _send_message(
        self,
        request_id: Any,
        params: dict[str, Any],
        raw_message: dict[str, Any],
        header_dict: dict[str, str],
        activation: ExtensionActivation = ExtensionActivation.EMPTY,
    ) -> dict[str, Any]:
        """Run the ``SendMessage`` operation for an already-validated envelope.

        **One deliberate deviation, recorded rather than hidden.**
        ``configuration.returnImmediately`` — v1.0's replacement for v0.3's
        ``blocking``, with the sense inverted — defaults to ``false``, meaning the
        server blocks until the task is terminal or interrupted.  This handler runs
        the skill inline and returns a terminal task, so it honours that default
        exactly.  An explicit ``returnImmediately: true`` asks this call to return
        a non-terminal task early and let the caller poll the rest — this handler
        still runs the skill inline and does not do that (this class gave it a
        task store to poll *against*, via ``GetTask``;
        returning early from this call itself is a separate change nothing here
        makes).  Rather than fail such a request it is answered with the finished
        task: strictly more than was asked for, never less.  The gateway façade
        made the opposite call for the opposite reason — it fronts an
        async route and cannot block at all.

        **What this call answers when the skill does not
        finish.** The module docstring's "Interruption policy" is the
        contract. A skill whose own coroutine ends in an
        ``asyncio.CancelledError`` no ``CancelTask`` requested (rule 2) gets
        no terminal write; this call answers with the task's current,
        non-terminal snapshot — ``WORKING``, typically — the same shape an
        explicit ``returnImmediately: true`` would legitimately produce. A
        request abandoned mid-skill (rule 3 — the ASGI request task
        cancelled on a client disconnect) has no caller left to answer; the
        still-running skill is handed to :meth:`_detach_execution`, which
        publishes its eventual outcome to the store, and the cancellation
        propagates. Neither writes ``CANCELED``; only a ``CancelTask`` (rule
        1) does, exactly as before.

        **Server-side content negotiation, nested
        ``historyLength``, and history accumulation.** Three gaps a
        content-negotiation probe found this method leaving unhandled, all
        closed the same way: read the same nested ``configuration`` object
        the client side already reads ``acceptedOutputModes`` off of.
        :meth:`_content_negotiation_error` runs first, before this method's
        task-seeding begins, and answers A2A's own ``-32005`` when the
        request's ``acceptedOutputModes`` shares nothing with what the
        resolved skill produces (:meth:`_supported_output_modes`) — a
        request-level refusal, so a caller that fixes its accepted modes and
        retries never finds a rejected task sitting at the id it named.
        :meth:`_continuation_history` decides whether this call's
        ``Task.history`` accumulates onto an existing, still-open task or
        resets for a fresh/terminal one — the previous unconditional reset
        silently discarded the very turn a paused
        ``INPUT_REQUIRED``/``AUTH_REQUIRED`` task exists to let a caller
        answer. And ``configuration.historyLength``, once resolved, bounds
        every ``Task`` this call can return through
        ``_send_message_response`` — the one funnel every return path below
        goes through, so none of them can forget to apply it. See each
        method's own docstring for the full reasoning; this paragraph is the
        index, not a restatement.
        """
        message = A2AMessage.from_dict(raw_message)
        novaos_context = novaos_context_from_metadata(params.get("metadata"), message.metadata)

        # Task id precedence: a caller continuing an existing task names it; a
        # platform-originated run reuses its run_id so NovaOS run and A2A task
        # correlate without a side table; anything else is a fresh task.
        task_id = as_str(message.task_id) or as_str(novaos_context.get("run_id")) or str(uuid4())
        context_id = (
            as_str(message.context_id)
            or as_str(novaos_context.get("session_id"))
            or header_dict.get("x-session-id", "")
        )

        history = await self._continuation_history(task_id, message)

        # `configuration.historyLength` bounds every `Task` this
        # call returns, wherever below it returns one — resolved once, since
        # every one of `_apply_history_length`'s callers reads the same raw,
        # untyped value, and `_send_message_response` is what every return
        # site below funnels through instead of building `success_response`/
        # `send_message_result` by hand, so none of them can forget to apply
        # it.
        history_length = as_dict(params.get("configuration")).get("historyLength")

        def _send_message_response(task_dict: dict[str, Any]) -> dict[str, Any]:
            self._apply_history_length(task_dict, history_length)
            return success_response(request_id, send_message_result(task=task_dict))

        skill_name, rejection = _resolve_skill(message, params, self._skills)
        if rejection is not None:
            return _send_message_response(
                await self._rejected(
                    rejection, task_id=task_id, context_id=context_id, history=history
                )
            )
        assert skill_name is not None

        # Content negotiation is a request-level refusal, so it
        # runs before anything below creates or seeds a task — a caller that
        # fixes its `acceptedOutputModes` and retries must not find a rejected
        # task sitting at the id it named. See `_content_negotiation_error`'s
        # own docstring for why this lives ahead of the store seed rather than
        # inside it.
        negotiation_error = self._content_negotiation_error(request_id, skill_name, params)
        if negotiation_error is not None:
            return negotiation_error

        # Seed the store before the skill starts, so the task
        # exists for a concurrent `GetTask`/`SubscribeToTask` from the
        # moment this call acknowledges it, and so `ctx.task_updater`'s own
        # progress calls have an existing entry — carrying this request's
        # `history` — to merge into rather than reconstructing it from
        # scratch. `event_queue` is this execution's alone: a fresh instance
        # per call, so two concurrent `SendMessage` calls for different tasks
        # never share one.
        # Everything this execution registers is
        # created and registered *synchronously* here — supersede, queue,
        # token, updater — with the store seed's `await` only afterwards. The
        # order matters twice over. Supersede-then-register with no `await`
        # between means two requests entering on the same task id together
        # cannot both miss each other (the second always finds the first's
        # token); register-before-seed means the token exists for a
        # concurrent `CancelTask` from the very first moment the task can be
        # observed in the store at all (the original invariant, now
        # strictly stronger). `_supersede_live_execution` takes over from any
        # execution — detached after its own request was abandoned, or simply
        # still in flight — that already owns this task id, rather than
        # letting a second skill run against the same task.
        self._supersede_live_execution(task_id)
        event_queue = EventQueue()
        task_updater = TaskUpdater(self._task_store, event_queue, task_id, context_id)
        cancellation = _CancellationToken()
        cancellation.updater = task_updater
        # Registered *before* the skill runs, so a concurrent `SubscribeToTask`
        # can attach to this task the moment it exists in the store. Popped in
        # `finally` below (identity-checked) — including on the exception
        # path — since an exception here must not leave a stale, closed queue
        # registered for a subscriber to wait on forever.
        self._active_event_queues[task_id] = event_queue
        self._active_cancellations[task_id] = cancellation
        try:
            await self._task_store.save(
                self._build_task(
                    TaskState.SUBMITTED,
                    None,
                    task_id=task_id,
                    context_id=context_id,
                    history=history,
                )
            )
        except BaseException:
            # The seed failed (or this request was cancelled during it) before
            # this call's `try`/`finally` below ever began, so nothing else
            # will unregister what was registered just above — or close the
            # queue: a `SubscribeToTask` that attached in this window (the
            # task id's prior incarnation was still in the store) would
            # otherwise wait forever on an unregistered queue nobody closes.
            # Identity-checked pops, like every other.
            await event_queue.close()
            if self._active_event_queues.get(task_id) is event_queue:
                del self._active_event_queues[task_id]
            if self._active_cancellations.get(task_id) is cancellation:
                del self._active_cancellations[task_id]
            raise

        referenced_tasks, unresolved_reference_task_ids = await self._resolve_referenced_tasks(
            message.reference_task_ids, context_id
        )
        ctx = AgentExecutionContext.from_a2a_request(
            message,
            params,
            header_dict,
            task_id=task_id,
            extensions=activation,
            task_updater=task_updater,
            cancellation=cancellation,
            referenced_tasks=referenced_tasks,
            unresolved_reference_task_ids=unresolved_reference_task_ids,
        )

        handler = self._skills[skill_name]["handler"]
        input_text = message.text

        # Runs once, before the skill starts — see `_maybe_prefetch_memory`'s
        # own docstring for why this call site (not inside the skill) is
        # what makes "at most once per turn" hold.
        await _maybe_prefetch_memory(ctx, input_text)

        eval_ctx = None
        ctx_token = None
        if _HAS_EVAL:
            eval_ctx = EvaluationContext(
                agent_name=self.name,
                agent_version=self.version,
                runtime_context=ctx.runtime_context,
                user_input=input_text,
                skill_name=skill_name,
            )
            ctx_token = _set_eval_context(eval_ctx)

        tracer = _get_tracer() if _HAS_OTEL else None

        # Bound outside `_execute_skill` so the abandonment
        # handler (`except BaseException` below) can hand a still-running
        # skill to `_detach_execution`; `published` flips once this call has
        # put the skill's outcome in the store (or established there is
        # none to put), `detached` once `finally`'s cleanup has been handed
        # off — same pair `_stream_send_message` keeps.
        skill_task: asyncio.Task[SkillResult] | None = None
        published = False
        detached = False

        async def _execute_skill() -> dict[str, Any]:
            nonlocal skill_task, published
            # Run the skill in its own task so `CancelTask` has
            # something to cancel — the direct `await handler(...)` this
            # replaced gave a concurrent cancellation nothing to interrupt.
            # `skill_context` is captured now and passed in as
            # the task's explicit `context=`, then copied back onto this
            # coroutine's own ambient context in `finally` below — see the
            # module docstring's "Skill dispatch" note — so a
            # `ContextVar.set()` inside the skill body is not silently
            # confined to the throwaway copy `asyncio.create_task` would
            # otherwise give it by default.
            skill_context = contextvars.copy_context()
            skill_task = asyncio.create_task(handler(ctx, input_text), context=skill_context)
            # Lets `_dispatch_cancel_task` reach this exact task through
            # `cancellation` even though it lives in a different request's
            # coroutine.
            cancellation.task = skill_task
            try:
                # `asyncio.shield`, so that cancelling *this*
                # coroutine (an abandoned request — rule 3 of the module
                # docstring's "Interruption policy") does not cancel the
                # skill with it. A bare `await skill_task` propagates the
                # awaiter's cancellation into the awaited task, which is
                # exactly the "a dropped connection stops the work" coupling
                # rule 3 removes. `CancelTask` is unaffected: it cancels
                # `skill_task` directly through `cancellation`, and the
                # shield surfaces that as a `CancelledError` here just as
                # the bare await did.
                result: SkillResult | None = await asyncio.shield(skill_task)
            except asyncio.CancelledError:
                if not (skill_task.done() and skill_task.cancelled()):
                    # `skill_task` is still running (or finished normally)
                    # and yet this await was cancelled: it is *this*
                    # coroutine's enclosing task that was cancelled — an
                    # abandoned request. Let the outer `except
                    # BaseException` below detach the skill; nothing to
                    # decide here.
                    raise
                if not cancellation.is_set:
                    # The skill's own task ended in a `CancelledError` no
                    # `CancelTask` asked for — the skill raised it itself,
                    # or something it awaited did. Rule 2: the skill gave up
                    # without an outcome, so nothing is written; answer with
                    # the task as it stands (non-terminal), never mislabel
                    # it CANCELED, and never
                    # let it propagate out of the ASGI request as before.
                    published = True
                    current = await self._task_store.get(task_id)
                    assert current is not None
                    return current.to_dict()
                result = None
            finally:
                # Runs on every exit from the `try` above —
                # normal completion, the CANCELED path, the unexplained
                # cancellation re-raised above, or a plain exception out of
                # the skill itself — so a partial `ContextVar.set()` made
                # before an interruption survives exactly as a completed
                # skill's would.
                for var in skill_context:
                    var.set(skill_context[var])

            if cancellation.is_set:
                # Covers both halves of the "skill completes anyway" race
                # (acceptance criterion 1): `result` is `None` from the
                # `except` above, or the skill ignored/caught its own
                # `CancelledError` and returned a `SkillResult` regardless.
                # Either way `_dispatch_cancel_task` already wrote CANCELED
                # to the store before it ever set this flag (see that
                # method's own docstring), so that write — not whatever
                # `result` holds — is this task's real outcome.
                published = True
                canceled = await self._canceled_snapshot(task_updater, task_id)
                return canceled.to_dict()

            assert result is not None
            if eval_ctx is not None:
                output_str = str(result.output) if result.output is not None else ""
                eval_ctx.set_output(output_str)

            state, result = self._task_state_for(result, skill_name)
            message = self._agent_message(
                result, task_id=task_id, context_id=context_id, buffered_events=ctx.buffered_events
            )
            artifacts = self._output_artifacts(result, message, skill_name=skill_name)
            await self._finalize_task(task_updater, state, message, artifacts=artifacts)
            published = True
            # Read back what `_finalize_task`
            # actually persisted rather than assuming it was `state`/
            # `message`. The `cancellation.is_set` check above already
            # covers the common case, but a `CancelTask` landing in the
            # narrow window *between* that check and this write would make
            # `TaskUpdater._set_status`'s terminal guard silently no-op this
            # call's write — the store would correctly stay CANCELED, but
            # returning `self._task(state, message, ...)` here would still
            # hand the caller the stale local COMPLETED/FAILED, splitting
            # the response from the store the same way the earlier
            # exception-path checks exist to prevent.
            final_task = await self._task_store.get(task_id)
            assert final_task is not None
            return final_task.to_dict()

        try:
            # A `CancelTask` can race this call all the way back
            # to the SUBMITTED store-seed's own `await` above — including
            # before `cancellation` was registered at all, in which case
            # `CancelTask` found no token to set — so this reads the store
            # itself rather than trusting `cancellation.is_set`. Starting the
            # skill now (or even marking it WORKING) would clobber whatever
            # terminal state a concurrent call already wrote.
            current = await self._task_store.get(task_id)
            if current is not None and current.status.state in TERMINAL_TASK_STATES:
                return _send_message_response(current.to_dict())

            # This used to run before the
            # `try`, so a cancellation landing on this exact await skipped
            # both the `except BaseException` clause below *and* `finally`
            # entirely — the task stayed at `TASK_STATE_SUBMITTED` forever
            # and its `EventQueue` was never closed or unregistered. Moved
            # inside for the same reason `_stream_send_message`'s own
            # `start_work()` call was.
            await task_updater.start_work()

            # A `CancelTask` landing during the
            # `await` above no longer clobbers the store — `TaskUpdater.
            # _set_status`'s terminal guard now refuses that write — but
            # `start_work()` itself still returns normally either way,
            # nothing raises to say its write was silently skipped. Check
            # the flag `_dispatch_cancel_task` sets before ever starting the
            # skill, so a cancellation that arrived mid-`start_work()`
            # doesn't still run it to completion for nothing.
            if cancellation.is_set:
                canceled = await self._canceled_snapshot(task_updater, task_id)
                return _send_message_response(canceled.to_dict())

            if tracer is not None:
                with tracer.start_as_current_span(
                    f"skill.{skill_name}",
                    attributes={
                        "novaos.agent.name": self.name,
                        "novaos.agent.version": self.version,
                        "novaos.skill.name": skill_name,
                        "novaos.run_id": ctx.run_id,
                        "novaos.a2a.task_id": task_id,
                    },
                ) as span:
                    try:
                        task = await _execute_skill()
                        span.set_attribute("novaos.a2a.task_state", task["status"]["state"])
                        return _send_message_response(task)
                    except Exception as e:
                        if _HAS_OTEL:
                            from opentelemetry.trace import StatusCode

                            span.set_status(StatusCode.ERROR, str(e))
                            span.record_exception(e)
                        raise
            else:
                return _send_message_response(await _execute_skill())
        except Exception as e:
            # A plain (non-`CancelledError`)
            # exception from the skill — including one caused *by* being
            # interrupted, if the skill's own code translates cancellation
            # into some other exception type — must not be reported as
            # FAILED once `CancelTask` already answered CANCELED.
            # `TaskUpdater._set_status`'s terminal guard now keeps the
            # *store* correct either way, but this method's own response is
            # built from local state below, not read back from the store, so
            # it needs the same check.
            if cancellation.is_set:
                published = True
                canceled = await self._canceled_snapshot(task_updater, task_id)
                return _send_message_response(canceled.to_dict())

            if eval_ctx is not None:
                eval_ctx.record_error(
                    error_type=type(e).__name__,
                    message=str(e),
                    recoverable=False,
                )
            failed_message = self._failed_message(e, task_id=task_id, context_id=context_id)
            await self._finalize_task(task_updater, TaskState.FAILED, failed_message, artifacts=[])
            published = True
            # Read back what `_finalize_task`
            # actually persisted rather than assuming it was FAILED — the
            # `cancellation.is_set` check above already covers the common
            # case, but a `CancelTask` landing in the narrow window between
            # that check and this write would make the write above a silent
            # no-op (the store correctly stays CANCELED), and constructing
            # the response from `failed_message` here would still hand the
            # caller a stale FAILED that disagrees with it.
            final_task = await self._task_store.get(task_id)
            assert final_task is not None
            return _send_message_response(final_task.to_dict())
        except BaseException:
            # Anything that isn't an `Exception` — chiefly `asyncio.CancelledError`
            # — reaches here instead of the branch above, which means
            # `_execute_skill` never got to publish an outcome: this call's
            # own caller (an ASGI request whose client disconnected, or a
            # task this call was awaited from being cancelled) abandoned it
            # first. Rule 3 of the module docstring's "Interruption policy":
            # a caller going away is not a decision about the
            # task, so nothing terminal is written and the skill — shielded
            # from this cancellation above, so still running — is not
            # stopped. What must not happen is its outcome never reaching
            # the store because the code that would have published it is
            # now unreachable — an earlier finding, which a
            # prior version answered by writing CANCELED here — the wrong
            # label for a caller that merely left). The skill is handed to
            # a detached finisher instead, which publishes whatever it
            # eventually produces and then does the cleanup `finally` below
            # skips for it. Abandoned before the skill ever started
            # (`skill_task` is `None` — cancelled during `start_work()`,
            # say): nothing is running, nothing to detach, nothing written;
            # the task stays SUBMITTED/WORKING and `finally` cleans up.
            # Cancellation must propagate either way, so this never
            # swallows it.
            if skill_task is not None and not published:
                self._detach_execution(
                    skill_task=skill_task,
                    cancellation=cancellation,
                    task_updater=task_updater,
                    event_queue=event_queue,
                    task_id=task_id,
                    context_id=context_id,
                    skill_name=skill_name,
                )
                detached = True
            raise
        finally:
            # See `_stream_send_message`'s `finally` — a
            # detached execution's queue and registry entries belong to its
            # finisher now.
            if not detached:
                await event_queue.close()
                # Identity-checked: a later request on this
                # same task id may have superseded this execution and
                # registered its own queue and token — those are its to pop,
                # not this call's.
                if self._active_event_queues.get(task_id) is event_queue:
                    del self._active_event_queues[task_id]
                if self._active_cancellations.get(task_id) is cancellation:
                    del self._active_cancellations[task_id]
            if eval_ctx is not None:
                eval_ctx.finalize()
            if ctx_token is not None:
                _reset_eval_context(ctx_token)
