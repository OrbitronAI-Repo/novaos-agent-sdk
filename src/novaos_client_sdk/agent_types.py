"""
Core types for the NovaOS Agent SDK.

Ported from NovaOs/packages/agent-sdk/python/novaos_sdk/types.py
"""

from dataclasses import dataclass, field
from typing import Any

from .a2a_task import TaskState

#: ``SkillResult.status`` -> A2A ``TaskState``. Shared between
#: :class:`~novaos_client_sdk.agent_builder.AgentBuilder` (which uses this
#: mapping to decide a task's outcome) and
#: :mod:`~novaos_client_sdk.migrate.surfaces` (whose ``KNOWN_SKILL_STATUSES``
#: is derived from these same keys, so the migration analyzer and the runtime
#: cannot silently drift apart on what counts as a valid status) rather than
#: each module keeping its own copy of the vocabulary.
#:
#: The five canonical members are ``SkillResult``'s own deliberately designed
#: vocabulary (see this dataclass's docstring). ``QUEUED``, ``RUNNING``,
#: ``IN_PROGRESS`` and ``ERROR`` are legacy spellings this SDK never defined
#: but real agent code returns anyway (analysis of a real agent fleet
#: found ``QUEUED`` six times, plus ``RUNNING``, ``IN_PROGRESS`` and
#: ``ERROR``) — each has exactly one unambiguous A2A meaning, so they are
#: accepted as synonyms rather than rejected outright. Anything else is not
#: silently treated as ``COMPLETED`` any more (that was the bug this fixes);
#: see :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._task_state_for`.
_SKILL_STATUS_TO_TASK_STATE: dict[str, TaskState] = {
    "COMPLETED": TaskState.COMPLETED,
    "FAILED": TaskState.FAILED,
    "WAITING_USER": TaskState.INPUT_REQUIRED,
    "REJECTED": TaskState.REJECTED,
    "AUTH_REQUIRED": TaskState.AUTH_REQUIRED,
    "QUEUED": TaskState.SUBMITTED,
    "RUNNING": TaskState.WORKING,
    "IN_PROGRESS": TaskState.WORKING,
    "ERROR": TaskState.FAILED,
}


@dataclass
class SkillResult:
    """Result of a skill execution.

    ``status`` stays in NovaOS's own vocabulary rather than becoming an A2A
    :class:`~novaos_client_sdk.a2a_task.TaskState`.  That is the DIP requirement
    working as intended: a skill handler is agent-authored business logic and
    should not have to know which wire protocol its result is serialized into.
    :class:`~novaos_client_sdk.agent_builder.AgentBuilder` owns the translation:

    ==================  ==============================
    ``SkillResult``     A2A ``TaskState``
    ==================  ==============================
    ``COMPLETED``       ``TASK_STATE_COMPLETED``
    ``FAILED``          ``TASK_STATE_FAILED``
    ``WAITING_USER``    ``TASK_STATE_INPUT_REQUIRED``
    ``REJECTED``        ``TASK_STATE_REJECTED``
    ``AUTH_REQUIRED``   ``TASK_STATE_AUTH_REQUIRED``
    ==================  ==============================

    ``WAITING_USER`` mapping onto a first-class protocol state is the point of
    ADR F4: under ACP the pause was a custom ``awaiting_approval`` field that
    only NovaOS understood.

    **Legacy status spellings are also accepted, deliberately.**
    ``QUEUED``, ``RUNNING``, ``IN_PROGRESS`` and ``ERROR`` are not
    part of this vocabulary — nothing ever designed them — but real agent
    code returns them anyway (analysis of a real agent fleet
    found all four in agent code that had never seen this table). Each has
    exactly one unambiguous A2A meaning, so
    :data:`~novaos_client_sdk.agent_types._SKILL_STATUS_TO_TASK_STATE` maps
    them too: ``QUEUED`` -> ``TASK_STATE_SUBMITTED``, ``RUNNING``/
    ``IN_PROGRESS`` -> ``TASK_STATE_WORKING``, ``ERROR`` -> ``TASK_STATE_FAILED``.
    A ``QUEUED``/``RUNNING``/``IN_PROGRESS`` return leaves the task **open**
    — the handler has already returned by the time this mapping runs, so
    "submitted" or "working" can only mean the work continues outside this
    request (a background job a separate progress skill or ``TaskUpdater``
    call will later close). A status outside this expanded vocabulary is no
    longer silently treated as ``COMPLETED`` (that was the bug this fixes):
    it fails the task instead, with the offending value named in both an
    ERROR-level log and the task's own status message — see
    :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._task_state_for`.

    **This is not the same shape as ``WAITING_USER``/``AUTH_REQUIRED``, and
    that difference matters.**
    :data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES` — and therefore
    :func:`~novaos_client_sdk.a2a_events.is_last_frame` — knows about
    ``INPUT_REQUIRED``/``AUTH_REQUIRED``, so a caller reading a
    ``SendStreamingMessage`` reply correctly recognises those as a clean,
    deliberate pause. ``SUBMITTED``/``WORKING`` carry no such recognition —
    to a stream reader they are indistinguishable from a connection that
    dropped mid-execution, which is *exactly* the ambiguity
    :class:`~novaos_client_sdk.agent_context.AgentExecutionContext`'s own
    "Delivery semantics" section already documents: **the platform retries a
    streaming execution whose stream ended without a terminal task state**,
    so a skill returning ``QUEUED``/``RUNNING``/``IN_PROGRESS`` can be
    invoked again for what the platform believes might be the same logical
    request. This is not a new hazard this vocabulary introduces — any skill left at
    ``WORKING`` when its stream drops already faces it — but choosing one of
    these three statuses is choosing to hit it *every time*, not
    occasionally. A skill using them for a non-idempotent effect (enqueuing
    a job, most concretely) **must** deduplicate on ``ctx.idempotency_key``,
    exactly as that section instructs for any other non-idempotent skill.

    **``REJECTED`` means "this was never going to work," not "it failed"**,
    the same distinction :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._rejected`
    already draws for a request an agent never even dispatched to a skill —
    an unroutable ``skillId``, say. A caller can retry a ``FAILED`` task;
    retrying a ``REJECTED`` one without changing the request is pointless.
    Before this member existed, a skill that discovered *mid-run* that a
    request could never succeed had no way to say so — the best it could
    reach was ``FAILED``, which is indistinguishable from "the agent tried
    and broke."

    **``AUTH_REQUIRED`` means "go authorise something out of band," not
    "answer a question"**, the same distinction A2A itself
    draws between :attr:`~novaos_client_sdk.a2a_task.TaskState.AUTH_REQUIRED`
    and :attr:`~novaos_client_sdk.a2a_task.TaskState.INPUT_REQUIRED`. A
    connector-OAuth pause or a write-confirmation gate — the shapes
    :meth:`~novaos_client_sdk.message_builder.MessageBuilder.create_oauth_required_message`
    and this SDK's A2A pause carrier exist for — is fundamentally different from
    a clarifying question: the caller cannot resolve it by simply replying
    with text, it has to go complete an out-of-band step (visit an OAuth URL,
    approve a write) first. Before this member existed, a skill reaching this
    pause had no way to say so distinctly and had to return ``WAITING_USER``
    and hope the caller inferred the difference from the message body alone.
    Like ``INPUT_REQUIRED``, this is an interrupt, not a terminal
    (:data:`~novaos_client_sdk.a2a_task.INTERRUPT_TASK_STATES`): the task
    stays updatable, and a caller resumes it with a follow-up ``SendMessage``
    naming the same task id once the out-of-band step is done.

    **Precedence when a skill also calls ``ctx.task_updater`` itself.** A
    skill may call one of ``ctx.task_updater``'s terminal methods
    (``complete``/``failed``/``reject``/``cancel``) directly instead of, or
    in addition to, returning a ``SkillResult`` — see that attribute's own
    docstring. When both happen and disagree,
    :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task`
    treats the direct call as authoritative and leaves it in place rather
    than overwriting it with whatever this table maps the returned
    ``SkillResult.status`` to. That direct call is the more specific,
    deliberate signal; a returned ``SkillResult`` is what ``_finalize_task``
    falls back to when a skill *hasn't* already closed its own task. See
    ``_finalize_task``'s docstring for the full reasoning and its history.

    **There is no ``CANCELED`` member, deliberately.**
    ``TASK_STATE_CANCELED`` never arises from a returned ``SkillResult`` —
    it is always the outcome of an external ``CancelTask`` call, handled
    entirely outside this dataclass: :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
    reads the task store directly once a skill's execution is interrupted or
    known to be cancelled, rather than synthesizing a ``SkillResult`` to
    carry it through this table. Adding a ``CANCELED`` member here would
    invite a skill to *return* one, which makes no sense for a state a skill
    never chooses for itself — cancellation is imposed on a skill by a
    caller, not decided by the skill's own return value the way ``COMPLETED``,
    ``FAILED`` and ``WAITING_USER`` are.

    **What ``metadata`` becomes on the wire.**
    :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._agent_message` folds
    this dict into its own `DataPart`, tagged
    :data:`~novaos_client_sdk.agent_builder.NOVAOS_SKILL_METADATA_CONTENT_TYPE`
    — a marker distinct from the platform one
    (:data:`~novaos_client_sdk.agent_builder.NOVAOS_METADATA_CONTENT_TYPE`,
    which carries HITL/OAuth/write-confirmation pauses and ``error``, above,
    under :data:`~novaos_client_sdk.agent_builder.NOVAOS_SKILL_ERROR_KEY`).
    The platform's part-draining (its wire contract's
    `drainParts`) reads a platform-marker part for platform fields only and
    never surfaces it as caller-visible run data; the skill-metadata marker
    is treated like any other `DataPart` instead, so this field's contents
    reach a caller through ``GET /api/v1/service/runs/{id}``'s per-artifact
    ``data``. Before the two markers were split apart, this field shared the
    platform marker with ``skill_error`` and was silently excluded from that
    response — returning structured data via ``ctx.task_updater.add_artifact``
    with a plain, untagged `DataPart` was the only way to have it surface.
    Both paths behave identically now; this field is simply the shortcut for
    a skill that has nothing else to say about its own task lifecycle.
    """

    output: Any
    # COMPLETED | FAILED | WAITING_USER | REJECTED | AUTH_REQUIRED, plus the
    # legacy synonyms QUEUED | RUNNING | IN_PROGRESS | ERROR (see docstring).
    status: str = "COMPLETED"
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConversationTurn:
    """One earlier turn of the conversation this execution belongs to.

    **Why this type exists at all.**  Under ACP the platform posted
    ``input: Message[]`` and an agent could simply read the earlier entries.
    A2A's ``SendMessage`` carries exactly *one* message, so there is nowhere in
    the request shape for prior turns to live.  Folding them into the current
    message would corrupt it — every agent written against ACP reads the last
    message as its input, and ``Message.text`` concatenates every text part, so
    sibling parts would glue the transcript onto the question with the roles
    lost.  The platform therefore carries prior turns *structurally*, in the
    NovaOS context metadata, and they surface here as
    :attr:`~novaos_client_sdk.agent_context.AgentExecutionContext.history`.

    **This is not A2A's own multi-turn mechanism, deliberately.**  A2A groups
    related tasks with ``contextId`` and expects the server to recover earlier
    turns from its own task store.  That does not work across NovaOS runs
    today: ``AgentBuilder``'s task store, like a scaffolded pod's
    ``InMemoryTaskStore``, is in-process and does not outlive the pod, and an
    ephemeral run gets a fresh pod anyway.  Until a durable, pod-external task
    store exists, ``contextId`` correlates turns but cannot reconstruct them.

    Attributes:
        role: Who produced the turn, in the conversation service's own
            vocabulary — ``"user"`` and ``"assistant"`` in practice, which is
            also what OpenAI-style chat APIs expect.  Passed through verbatim
            rather than normalised, so an unrecognised role stays visible
            instead of being silently rewritten to something plausible.
        content: The turn's plain text.  May be empty for a turn that carried
            no text (an attachment-only message, say); it is never ``None``.

    Example — prepending the transcript to an LLM call::

        from dataclasses import asdict

        messages = [{"role": "system", "content": "You are a helpful agent."}]
        messages += [asdict(turn) for turn in ctx.history]
        messages.append({"role": "user", "content": user_input})
    """

    role: str
    content: str


@dataclass
class AgentRuntimeContext:
    """Platform execution context injected at runtime via the request body/headers.

    Distinct from ``context.ExecutionContext``, which parses ``X-NovaOS-*`` auth-proxy
    headers.  This class holds the NovaOS platform fields (run_id, mcp_gateway_url,
    config, etc.) plus the A2A protocol correlators, extracted from an A2A
    ``SendMessage`` request, and is used exclusively by ``AgentBuilder`` /
    ``AgentExecutionContext``.

    **Why NovaOS ids and A2A ids both appear here** (ADR F13).  Before the A2A
    migration this class documented itself as holding "ACP protocol fields", and the
    obvious move on migrating was to rename ``run_id`` to ``task_id`` and
    ``session_id`` to ``context_id``.  That would have been wrong.  A NovaOS *run* is
    a platform record with its own lifecycle — the marketplace Agent Log Panel's Run
    ID filter and the Loki ingestion pipeline both key on it —
    whereas an A2A *task* is a protocol object owned by whichever agent serves the
    request.  They correlate; they are not the same identifier.  So both are carried
    and the correspondence is recorded rather than collapsed:

    ================  ================  ==================================================
    NovaOS field      A2A field         Relationship
    ================  ================  ==================================================
    ``run_id``        ``task_id``       ``AgentBuilder`` seeds a new task's id from
                                        ``run_id`` when the caller supplied none, so the
                                        two match for platform-originated runs.
    ``session_id``    ``context_id``    A2A's ``contextId`` groups related tasks; ACP's
                                        ``session_id`` meant the same thing (ADR F2).
    ================  ================  ==================================================
    """

    run_id: str = ""
    thread_id: str = ""
    user_id: str = ""
    tenant_id: str = ""
    instance_id: str = ""
    trace_id: str = ""
    span_id: str = ""
    #: Full W3C ``traceparent`` header value (``00-<trace-id>-<span-id>-<flags>``),
    #: kept apart from ``trace_id``/``span_id`` above so outbound propagation
    #: (which needs the whole header) and correlation (which needs the bare
    #: hex ids) each read the field shaped for their use, instead of one field
    #: doing both jobs under a name that only describes one of them.
    traceparent: str = ""
    session_id: str = ""
    execution_token: str = ""
    mcp_gateway_url: str = ""
    mcp_gateway_token: str = ""
    gateway_url: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] | None = None

    #: Stable deduplication key for this execution attempt.
    #:
    #: **Agent execution is at-least-once.** The platform retries a streaming
    #: execution whose stream ended without a terminal task state, and it cannot
    #: tell that apart from a stream that dropped *after* the agent finished its
    #: work — A2A v1.0 removed v0.3's ``final`` flag, so the close is the only
    #: signal. An agent can therefore be asked to run the same logical request
    #: twice.
    #:
    #: This key is identical across every retry of one execution and different
    #: between executions. An agent with side effects — writing to an external
    #: system, calling a paid API — should record it with the effect and skip a
    #: request whose key it has already processed.
    #:
    #: Do **not** deduplicate on :attr:`run_id`: it is stable across retries but
    #: is not unique per execution, because a human-in-the-loop continuation
    #: reuses the run id of the turn that raised the approval request. Deduping
    #: on it would skip a legitimate second execution.
    #:
    #: Empty when the caller sent none, which is the case for every platform
    #: build before this key existed — treat an empty value as "no dedup information", never as
    #: a key in its own right.
    idempotency_key: str = ""

    # ── A2A protocol correlators (see the table above) ──────────────────────
    #: A2A ``Task.id`` this execution belongs to.
    task_id: str = ""
    #: A2A ``Task.contextId`` / ``Message.contextId`` grouping related tasks.
    context_id: str = ""

    #: Earlier turns of this conversation, oldest first.
    #:
    #: Empty for a single-shot invocation, which is most of them: the platform
    #: only carries prior turns for a run that belongs to a conversation thread
    #: — the marketplace chat surface, and a service-account run that named a
    #: ``thread_id``. A task, a team step, a background run and a single-shot
    #: service-account run have no conversation to draw on and always see ``()``.
    #:
    #: Also empty when the caller is not the NovaOS platform at all — an
    #: agent-to-agent call or a hand-written ``SendMessage`` — so a skill must
    #: treat an empty history as "no prior context available", never as "this is
    #: the first turn". The current turn is **not** included; it arrives as the
    #: skill's input text, exactly as it did before.
    #:
    #: Bounded by the platform (20 turns at the time of writing) and truncated
    #: oldest-first, so a long conversation yields the most recent turns rather
    #: than an unbounded transcript.
    history: tuple[ConversationTurn, ...] = ()

    #: Mint-time signal: does this run's token carry a write-capable
    #: ``mcp:*`` scope? Coarse and run-level — a skill wanting a precise,
    #: per-tool answer should call ``ctx.tools.hitl.requires_write_hitl(tool_fqn)``
    #: instead, which classifies the exact tool synchronously with no network
    #: call. This field can never promise more than the token itself already
    #: allows, since the platform derives it from the same scopes embedded in
    #: the token's claims.
    #:
    #: Always ``False`` for a service-account run, regardless of scope: an SA
    #: has no interactive HITL resume path (a write call either succeeds via
    #: an admin-managed static per-tool grant or is rejected with nothing
    #: able to ever answer a pause), so a proactive "build a pause" signal
    #: would tell an SA-driven skill to wait for an approval that structurally
    #: cannot arrive.
    #:
    #: Also ``False`` when the platform sent no ``execution_hints`` at all —
    #: which includes every run whose token carries no ``mcp:*`` scope, and
    #: every caller that is not the NovaOS platform (an agent-to-agent call,
    #: a hand-written ``SendMessage``). Treat ``False`` as "no signal", not as
    #: a guarantee this run will never need write approval.
    mcp_write_hitl_active: bool = False

    #: Pre-fetch budget for the memory-assemble call, e.g.
    #: ``{"token_budget": 4000, "latency_budget_ms": 800}``.
    #:
    #: Set by ``temporal-worker`` only for an agent whose catalog manifest
    #: declares the ``memory`` capability — its absence is the signal an agent
    #: without that capability uses to skip the assemble call entirely, rather
    #: than a separate capability flag living in this SDK.  ``None`` for every
    #: other caller: an agent-to-agent call, a hand-written request, or a
    #: platform build before this field existed.
    memory_budget: dict[str, int] | None = None


@dataclass
class ToolResponse:
    """Standardised response from a platform tool."""

    success: bool
    data: Any = None
    error: str | None = None
