"""
Context — The core object every NovaOS agent uses.

Provides:
  - ctx.tools.*     — governed access to platform tools
  - ctx.run_id      — current execution run ID
  - ctx.idempotency_key — dedup key; execution is AT-LEAST-ONCE (see below)
  - ctx.tenant_id   — tenant context (auto-propagated)
  - ctx.history     — earlier turns of this conversation, oldest first
  - ctx.trace_id    — W3C trace ID (auto-propagated)
  - ctx.config      — agent configuration

Delivery semantics — read this if your agent has side effects
------------------------------------------------------------
Agent execution is **at-least-once**, not exactly-once. The platform retries a
streaming execution whose stream ended without a terminal task state, and it
cannot distinguish that from a stream that dropped *after* the agent finished
its work. Your skill can therefore be invoked twice for one logical request.

If your skill writes to an external system, calls a paid API, or has any other
non-idempotent effect, deduplicate on ``ctx.idempotency_key``::

    async def handle(ctx):
        key = ctx.idempotency_key
        if key and await already_processed(key):
            return await stored_result(key)
        result = await do_the_side_effecting_work()
        if key:
            await record(key, result)
        return result

Do **not** deduplicate on ``ctx.run_id``. It is stable across retries but is not
unique per execution — a human-in-the-loop continuation reuses the run id of the
turn that raised the approval — so it would silently skip a legitimate second
execution.

Ported from NovaOs/packages/agent-sdk/python/novaos_sdk/context.py
"""

import asyncio
import logging
import os
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from .a2a_extensions import ExtensionActivation
from .a2a_task import Task
from .a2a_task_updater import TaskUpdater
from .a2a_types import A2AMessage, as_dict, as_str
from .agent_types import AgentRuntimeContext, ConversationTurn
from .exceptions import SecretNotDeliveredError
from .header_safety import filter_custom_headers
from .logging import get_agent_logger
from .tools import ToolBindings
from .tracing import configure_otel, extract_parent_id, extract_trace_id

if TYPE_CHECKING:
    from .context import ExecutionContext, MemoryNamespace

_BEARER_PREFIX = "Bearer "
_AUTH_PROXY_TOKEN_HEADER = "x-novaos-execution-token"

# Namespaced key under which the NovaOS platform hands its execution context to
# an agent pod.  A2A has no field for host-platform context: the protocol's
# extension points are the request/message ``metadata`` maps, so NovaOS claims one
# vendor-namespaced key inside them rather than inventing a sibling of
# ``message``.  Under ACP this same payload was a top-level ``body["context"]``.
#
# ⚠️ This is a plain namespaced metadata key, **not** a declared A2A extension,
#    and nothing sensitive belongs in it.  That is a standing rule rather than a
#    stopgap: extension data and this key are the same plain JSON in the same
#    request body, visible to the same proxies and APM tooling (ADR F7/G1), so
#    the confidentiality does not come from the placement — it comes from the
#    value being sealed first.  Secrets therefore travel through the
#    ``user-context/v1`` extension (:mod:`novaos_client_sdk.a2a_user_context`),
#    sealed before transit, and are read via :meth:`~AgentExecutionContext.get_secret`.
NOVAOS_CONTEXT_METADATA_KEY = "novaos.platform/context"


#: Distinguishes "no default was supplied" from "the default is ``None``", so
#: :meth:`AgentExecutionContext.get_secret` can both raise on an undeclared
#: secret and return ``None`` for one a caller is happy to do without.
_NO_SECRET_DEFAULT: Any = object()


def _user_context_extension_uri() -> str | None:
    """The ``user-context/v1`` URI, or ``None`` when the ``crypto`` extra is absent.

    Deferred rather than imported at module scope because opening sealed
    envelopes needs the optional ``cryptography`` dependency, while this module
    is imported by every agent whether or not it receives secrets.  A pod
    without the extra could not have registered the extension in the first
    place, so "the import failed" and "the extension was not activated" describe
    the same situation from two directions and are answered the same way — with
    a message that says which one it was.
    """
    try:
        from .a2a_user_context import USER_CONTEXT_EXTENSION_URI
    except ImportError:  # pragma: no cover - exercised by the import-guard test
        return None
    return USER_CONTEXT_EXTENSION_URI


def _extract_bearer_token(auth_header: str) -> str:
    """Extract the token from a Bearer authorization header, or return empty string."""
    if auth_header.startswith(_BEARER_PREFIX):
        return auth_header[len(_BEARER_PREFIX) :]
    return ""


def _resolve_execution_token(headers: dict[str, str]) -> str:
    """Resolve execution token from Authorization or auth-proxy header."""
    auth_token = _extract_bearer_token(headers.get("authorization", ""))
    if auth_token:
        return auth_token
    return headers.get(_AUTH_PROXY_TOKEN_HEADER, "")


def _resolve_output_schema(novaos_context: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve a caller-supplied JSON Schema for structured extraction (Ex10).

    Under ACP this field arrived at three different depths depending on which
    gateway call path produced the request, so the lookup had to try all three.
    A2A collapses that: everything platform-side now travels inside the one
    :data:`NOVAOS_CONTEXT_METADATA_KEY` object, so only two depths remain —
    the context itself, and its ``config`` sub-object where the temporal worker
    puts per-run settings.

    Args:
        novaos_context: The decoded :data:`NOVAOS_CONTEXT_METADATA_KEY` payload

    Returns:
        The first schema dict found, or ``None`` when the caller supplied none.
    """
    for candidate in (
        novaos_context.get("schema"),
        as_dict(novaos_context.get("config")).get("schema"),
    ):
        if isinstance(candidate, dict) and candidate:
            return candidate
    return None


def _resolve_mcp_write_hitl_active(novaos_context: dict[str, Any]) -> bool:
    """Decode the mint-time write-approval hint the platform attached, if any.

    The platform writes ``execution_hints.mcp_write_hitl_active`` into the
    :data:`NOVAOS_CONTEXT_METADATA_KEY` object. Absent for any run whose token
    carries no ``mcp:*`` scope, and for any caller that is not the NovaOS
    platform — both cases fall back to ``False``, same as a malformed value,
    since this is a proactive hint, not a promise: a skill that gets
    ``False`` here should still expect a write call to fail with -32042 if it
    actually turns out to need approval, and use
    ``ctx.tools.hitl.requires_write_hitl(tool_fqn)`` for a precise, per-tool
    check instead of relying on this alone.
    """
    hints = novaos_context.get("execution_hints")
    if not isinstance(hints, dict):
        return False
    return hints.get("mcp_write_hitl_active") is True


def _resolve_memory_budget(novaos_context: dict[str, Any]) -> dict[str, int] | None:
    """Decode the pre-fetch memory budget the platform attached, if any.

    ``temporal-worker`` writes ``memory_budget`` into the
    :data:`NOVAOS_CONTEXT_METADATA_KEY` object only for an agent whose catalog
    manifest declares the ``memory`` capability — its absence here is the
    signal :class:`AgentBuilder`'s pre-fetch hook uses to skip the
    ``memory.assemble`` call entirely, so a malformed or missing value must
    resolve to "no budget", never raise.

    Args:
        novaos_context: The decoded :data:`NOVAOS_CONTEXT_METADATA_KEY` payload

    Returns:
        A dict with whichever of ``token_budget`` / ``latency_budget_ms`` the
        platform sent as integers, or ``None`` when the field was absent or
        carried nothing readable.
    """
    raw = novaos_context.get("memory_budget")
    if not isinstance(raw, dict):
        return None
    budget = {
        key: value
        for key, value in raw.items()
        if key in ("token_budget", "latency_budget_ms") and isinstance(value, int)
    }
    return budget or None


def _conversation_history(novaos_context: dict[str, Any]) -> tuple[ConversationTurn, ...]:
    """Decode the prior conversation turns the platform attached, if any.

    The platform writes ``history`` into the
    :data:`NOVAOS_CONTEXT_METADATA_KEY` object as a list of
    ``{"role": ..., "content": ...}`` objects, oldest first, for runs that
    belong to a conversation thread.  See :class:`ConversationTurn` for why the
    turns travel here rather than as A2A messages.

    **Malformed input is skipped, never raised on.**  This value crosses a
    network boundary and is decoded before the skill runs, so a parse error
    here would fail an execution that has nothing wrong with it — an agent that
    could have answered from the current turn alone would instead return
    FAILED.  Losing an unreadable turn degrades the answer; refusing to run
    destroys it.  Entries that are not objects, and entries whose ``role`` or
    ``content`` is not a string, are therefore dropped individually, so one bad
    turn cannot take the rest of the transcript with it.

    Args:
        novaos_context: The decoded :data:`NOVAOS_CONTEXT_METADATA_KEY` payload

    Returns:
        The turns in the order the platform sent them, or an empty tuple when
        no readable history arrived.
    """
    raw = novaos_context.get("history")
    if not isinstance(raw, list):
        return ()

    turns: list[ConversationTurn] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        role = entry.get("role")
        content = entry.get("content")
        if not isinstance(role, str) or not isinstance(content, str):
            continue
        turns.append(ConversationTurn(role=role, content=content))
    return tuple(turns)


class _CancellationToken:
    """Cooperative + asyncio-level cancellation signal for one execution.

    Bridges ``AgentBuilder``'s ``CancelTask`` dispatch — which runs as a
    separate, concurrent request — to two things at once: the running skill's
    :class:`AgentExecutionContext` (so a skill can poll :attr:`~AgentExecutionContext.is_cancelled`
    or call :meth:`~AgentExecutionContext.raise_if_cancelled`), and the
    ``asyncio.Task`` actually executing the skill (so ``CancelTask`` can
    interrupt an in-progress ``await``). ``AgentBuilder`` creates one per
    execution and hands the *same* instance to both places; :attr:`task` is
    filled in once the skill's task exists, which lets :meth:`set` reach it
    even when ``CancelTask`` is dispatched before the skill has actually
    started running.

    Racing ``CancelTask`` against a request too early to have registered a
    token at all — the request's own initial store write, before this class
    is even constructed — is *not* this class's job: ``_send_message`` /
    ``_stream_send_message`` guard that window by reading the task store
    directly rather than trusting this token, since a race that early may
    have found no token to set.

    One-way: nothing ever clears ``_cancelled`` once :meth:`set` has run, since
    cancellation is terminal.

    :attr:`updater` is the execution's ``TaskUpdater``, attached
    by ``AgentBuilder`` alongside :attr:`task` so that
    ``AgentBuilder._supersede_live_execution`` — which reaches a preempted
    execution only through this token — can also revoke its updater, closing
    the window in which an execution that has already passed its
    ``is_set`` check could still publish onto a task a newer request now
    owns. Typed loosely to keep this module free of a ``TaskUpdater`` import.
    """

    __slots__ = ("_cancelled", "task", "updater")

    def __init__(self) -> None:
        self._cancelled = False
        self.task: asyncio.Task[Any] | None = None
        self.updater: Any | None = None

    def set(self) -> None:
        self._cancelled = True
        if self.task is not None:
            self.task.cancel()

    @property
    def is_set(self) -> bool:
        return self._cancelled


def novaos_context_from_metadata(*metadata: dict[str, Any] | None) -> dict[str, Any]:
    """Pull the NovaOS platform context out of the first metadata map that has it.

    A2A gives a caller two places to attach metadata to a ``SendMessage`` — the
    request ``params`` and the ``message`` itself.  The platform sets it on the
    request; a caller constructing a message by hand may well attach it to the
    message.  Both are accepted, in the order given, so neither placement is a
    silent no-op.

    Args:
        *metadata: Candidate metadata maps, most-authoritative first

    Returns:
        The context object, or an empty dict when no map carries one.
    """
    for candidate in metadata:
        if not isinstance(candidate, dict):
            continue
        context = candidate.get(NOVAOS_CONTEXT_METADATA_KEY)
        if isinstance(context, dict):
            return context
    return {}


class AgentExecutionContext:
    """
    Agent execution context. Created automatically by the AgentBuilder class
    from the A2A request headers and ``SendMessage`` params.

    Access platform tools via ctx.tools:
        result = await ctx.tools.knowledge.search("compliance regulations")
        result = await ctx.tools.agents.call("agent-id", {"input": "..."})
    """

    def __init__(
        self,
        execution_context: AgentRuntimeContext,
        *,
        enable_telemetry: bool = False,
        extensions: ExtensionActivation | None = None,
        verified_tenant_id: str = "",
        emit_tool_event: Callable[[str, dict[str, Any]], None] | None = None,
        task_updater: TaskUpdater | None = None,
        cancellation: "_CancellationToken | None" = None,
        referenced_tasks: tuple[Task, ...] = (),
        unresolved_reference_task_ids: tuple[str, ...] = (),
        custom_headers: Mapping[str, str] | None = None,
    ):
        self._ctx = execution_context
        # The caller's `custom_headers`, filtered. Held on the context itself, not
        # in request-scoped storage, so a streaming skill task that starts after
        # the endpoint handler has returned still sees them. Values are
        # credential-equivalent: only ever log `len(custom_headers)`.
        self._custom_headers: dict[str, str] = filter_custom_headers(custom_headers or {})
        # Resolved by `AgentBuilder` *before* this object is
        # constructed — see `referenced_tasks`'s own docstring for why
        # resolution happens there rather than lazily here.
        self._referenced_tasks = referenced_tasks
        self._unresolved_reference_task_ids = unresolved_reference_task_ids
        self._task_updater = task_updater
        # Defaults to a fresh, never-cancelled token rather than `None` so a
        # hand-built context (a test, or a caller that skips `from_a2a_request`)
        # can still read `is_cancelled` / call `raise_if_cancelled` without a
        # `None` check of its own. `AgentBuilder` always passes
        # its own instance, shared with its `_active_cancellations` registry.
        self._cancellation = cancellation if cancellation is not None else _CancellationToken()
        # The tenant the auth-proxy sidecar vouched for, kept apart from
        # `AgentRuntimeContext.tenant_id` rather than merged into it.
        # The runtime context reads the request body first, which is
        # right for everything that only needs to know whose run this is; a
        # sealed secret's binding needs the stronger claim, because a caller who
        # writes someone else's tenant id into the body is exactly the replay
        # the binding exists to refuse.  Empty when no sidecar header was
        # present — `get_secret` falls back to the runtime context, which still
        # defeats replay *between runs*.
        self._verified_tenant_id = verified_tenant_id
        self._entity_refs: list[str] = []
        self._logger: logging.Logger | None = None
        # `is None`, not `or`: an ExtensionActivation is falsy whenever nothing
        # is *in force*, which is not the same as no negotiation having happened.
        # A request that asked for extensions this agent does not implement
        # produces a real activation with `requested` and `ignored` populated and
        # `activated` empty — `or` would throw that away and hand the skill
        # EMPTY, which says the caller asked for nothing.
        self._extensions = ExtensionActivation.EMPTY if extensions is None else extensions

        self._maybe_configure_otel(enable_telemetry)

        mcp_url = execution_context.mcp_gateway_url or os.environ.get("MCP_GATEWAY_URL", "")
        # ⚠️ D3 fix: prefer execution_token (RS256 JWT from auth-proxy sidecar via
        #    x-novaos-execution-token header) over mcp_gateway_token (opaque MCP session
        #    token from context body).  Aligns with to_execution_context() token resolution
        #    and with the header precedence in _resolve_execution_token().
        token = execution_context.execution_token or execution_context.mcp_gateway_token
        tenant_id = execution_context.tenant_id

        self.tools = ToolBindings(
            mcp_gateway_url=mcp_url,
            token=token,
            tenant_id=tenant_id,
        )
        # Propagate the per-request W3C traceparent to all tool bindings so outbound
        # MCP calls carry the correct trace context in fleet mode where TRACEPARENT
        # env var is set at pod startup and becomes stale (P4-fleet F16).
        if execution_context.traceparent:
            self.tools.set_traceparent(execution_context.traceparent)
        self.tools.set_run_id(execution_context.run_id)
        self.tools.set_custom_headers(self._custom_headers)
        # `emit_tool_event` is only ever non-None for a streaming request —
        # see the parameter's docstring above.
        if emit_tool_event is not None:
            self.tools.set_event_sink(emit_tool_event)
        # Kept apart from `self.tools`' own copy of this callback: platform
        # events like `memory.assembled` are not tool calls, so they go
        # through `_emit_platform_event` below rather than `self.tools`.
        self._emit_tool_event = emit_tool_event
        # Drains into the terminal message's metadata for a non-streaming
        # request, which has no SSE frame of its own to carry one — see
        # `_emit_platform_event`.
        self._buffered_events: list[tuple[str, dict[str, Any]]] = []
        # `MemoryNamespace` is imported under `TYPE_CHECKING` only, so the
        # quotes here are load-bearing, not stylistic: ruff's UP037 doesn't
        # know that and suggests removing them, which would evaluate the
        # name eagerly and raise `NameError` at import time.
        self._memory_ns: "MemoryNamespace | None" = None  # noqa: UP037

    def _maybe_configure_otel(self, enable_telemetry: bool) -> None:
        """Auto-bootstrap OpenTelemetry when explicitly enabled or env is set."""
        otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        if not enable_telemetry and not otel_endpoint:
            return

        service_name = (
            os.environ.get("OTEL_SERVICE_NAME", "") or self._ctx.instance_id or "novaos-agent"
        )
        try:
            configured = configure_otel(service_name=service_name)
        except Exception as exc:
            logging.getLogger("novaos.sdk.telemetry").error(
                "OpenTelemetry auto-configuration failed; telemetry disabled: %s",
                exc,
            )
            return

        if configured:
            logging.getLogger("novaos.sdk.telemetry").debug(
                "OpenTelemetry auto-configured (service_name=%s)",
                service_name,
            )
        elif enable_telemetry or otel_endpoint:
            logging.getLogger("novaos.sdk.telemetry").warning(
                "OpenTelemetry requested (enable_telemetry=%s, endpoint=%s) "
                "but configure_otel returned False — is novaos-agent-sdk[otel] installed?",
                enable_telemetry,
                otel_endpoint or "(unset)",
            )

    @property
    def runtime_context(self) -> AgentRuntimeContext:
        """The underlying runtime context injected by the platform."""
        return self._ctx

    @property
    def task_updater(self) -> TaskUpdater | None:
        """Report progress on this task mid-execution.

        Call :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.start_work`,
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.add_artifact`, or
        :meth:`~novaos_client_sdk.a2a_task_updater.TaskUpdater.update_status`
        (with :attr:`~novaos_client_sdk.a2a_task.TaskState.WORKING`) from inside
        a long-running skill instead of staying silent until it returns.

        **Prefer returning a terminal ``SkillResult`` over calling one of its
        terminal methods** (``complete``, ``failed``, ``reject``, ``cancel``)
        yourself — :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
        closes the task itself once your handler returns, from whatever
        ``SkillResult`` you return, and you must return one regardless. You
        *may* call a terminal method directly if you have your own reason to
        — most concretely, to close the task before you have
        finished building the ``SkillResult`` you still have to return. Doing
        so is safe: your direct call wins over the harness's own decision when
        the two disagree, rather than silently being overwritten by it — see
        :meth:`~novaos_client_sdk.agent_builder.AgentBuilder._finalize_task`'s
        docstring for the precedence reasoning. Calling one is still rarely
        useful, though, since you have nothing to gain that returning the
        equivalent ``SkillResult.status`` (``"REJECTED"``, and so on) does not
        already give you more simply.

        ``None`` for a request that arrived through ``SendStreamingMessage``,
        which reports progress through SSE frames directly rather than through
        this object.
        """
        return self._task_updater

    @property
    def is_cancelled(self) -> bool:
        """Whether ``CancelTask`` has requested cancellation of this execution.

        Poll this in a skill doing synchronous, non-awaiting work — a tight
        compute loop, a blocking library call between awaits — since only an
        ``await`` point can be interrupted by asyncio task cancellation alone.
        A skill that only ever awaits (I/O, ``asyncio.sleep``, an async HTTP
        client) does not need to check this itself: ``AgentBuilder`` cancels
        the ``asyncio.Task`` running the skill, which delivers
        ``asyncio.CancelledError`` at the skill's next ``await`` regardless of
        whether it ever looks at this property.

        See :meth:`raise_if_cancelled` for the raise-instead-of-check form.
        """
        return self._cancellation.is_set

    def raise_if_cancelled(self) -> None:
        """Raise ``asyncio.CancelledError`` if ``CancelTask`` has requested cancellation.

        The convenience form of :attr:`is_cancelled` for a loop that would
        otherwise have to check the flag and raise by hand on every
        iteration::

            @agent.skill("bulk-process")
            async def bulk_process(ctx, input_text):
                for item in items:
                    ctx.raise_if_cancelled()
                    process(item)  # synchronous, no `await` of its own
                return SkillResult(output="done")

        Raises the same exception type asyncio itself raises at an
        interrupted ``await``, so a skill's own cleanup does not need two
        different cases for "cancelled while awaiting" versus "cancelled
        while polling".
        """
        if self._cancellation.is_set:
            raise asyncio.CancelledError("Task canceled")

    @property
    def extensions(self) -> ExtensionActivation:
        """The A2A protocol extensions in force for this request.

        A skill asks ``ctx.extensions.is_active(uri)`` whether an agreement
        applies and ``ctx.extensions.value(uri)`` for whatever data it carried;
        the dispatch to the extension that owns the URI happens inside the
        activation, so a skill never has to know which metadata map the payload
        arrived in.

        Always an :class:`~novaos_client_sdk.a2a_extensions.ExtensionActivation`
        — :data:`ExtensionActivation.EMPTY` for a request negotiated by an agent
        with no extensions, or for a context built by hand — so a skill has no
        ``None`` to guard against.
        """
        return self._extensions

    @property
    def referenced_tasks(self) -> tuple[Task, ...]:
        """This message's ``referenceTaskIds``, resolved to ``Task`` objects.

        A2A lets a message point at other tasks it references — a follow-up
        question about a prior result, a correction to one — via
        ``message.referenceTaskIds``. ``AgentBuilder`` resolves each id
        against its own ``task_store`` *before* this context is built (see
        :meth:`from_a2a_request`'s own docstring for why resolving is
        dispatch's job, not this class's), so a skill reads already-resolved
        ``Task`` objects here rather than ids it would have to look up itself
        — this SDK gives a skill no other way to reach ``task_store`` at all.

        **Same-context only.** A pod's task store carries no tenant field of
        its own (:class:`~novaos_client_sdk.a2a_task.Task` has none), so
        ``contextId`` equality — A2A's own session/conversation grouping,
        and the closest boundary this store's data model actually has — is
        what stands in for "no cross-tenant reads" here: a reference naming a
        task from a *different* ``contextId`` than this request's own is
        treated exactly like a reference naming an id the store has never
        seen, and is skipped rather than resolved.

        **Never raises.** An id that does not resolve — unknown to this pod,
        evicted by the store's own LRU cap, or scoped to a different
        ``contextId`` — is silently omitted from this tuple and reported
        instead in :attr:`unresolved_reference_task_ids`, so one bad or
        stale reference cannot fail an otherwise-servable request. Empty when
        the message carried no ``referenceTaskIds`` at all, which is the
        common case.
        """
        return self._referenced_tasks

    @property
    def unresolved_reference_task_ids(self) -> tuple[str, ...]:
        """``referenceTaskIds`` this request named that :attr:`referenced_tasks` could not resolve.

        Every id here is either unknown to this pod's ``task_store`` or
        belongs to a different ``contextId`` than this request's own — see
        :attr:`referenced_tasks`'s docstring for why the second case is
        treated the same as the first. A skill that needs to tell a caller
        "I couldn't find what you referenced" reads this rather than
        diffing the message's own ``referenceTaskIds`` against
        :attr:`referenced_tasks` by hand.
        """
        return self._unresolved_reference_task_ids

    # ------------------------------------------------------------------
    # Sealed secret delivery — the user-context/v1 extension
    # ------------------------------------------------------------------

    def _sealed_user_context(self) -> Any:
        """This request's sealed secrets, or ``None`` if none were delivered."""
        uri = _user_context_extension_uri()
        return None if uri is None else self._extensions.value(uri)

    @property
    def secret_names(self) -> tuple[str, ...]:
        """Every secret name this request delivered, sorted; empty when none were.

        Safe to log — names travel in the clear on the wire by design, since a
        pod must be able to route a value before it can decrypt one. Worth
        logging when :meth:`get_secret` raises, because it separates "this
        instance declares no such secret" from "delivery is not working".
        """
        sealed = self._sealed_user_context()
        return () if sealed is None else sealed.secret_names

    def get_secret(self, name: str, default: Any = _NO_SECRET_DEFAULT) -> Any:
        """The value of one secret delivered with this request.

        The A2A-era replacement for ACP's ``context.secrets``. The worker seals
        each value to this pod's recipient key before placing it in the request
        (the extension mechanism provides no confidentiality of its
        own), and this is where it is opened, inside the agent process, past the
        auth-proxy sidecar.

        **Nothing is cached.** Each call opens the envelope again and hands back
        a fresh string; the plaintext lives only in whatever the caller does
        with it. A skill that needs a value more than once should keep it.

        **A default covers a missing secret, never a failed one.** If the value
        was delivered and would not open — a rotated key, a tampered payload, a
        binding that does not match this run — the exception propagates whether
        or not a default was supplied. Substituting a default there would turn a
        key or replay problem into an agent quietly running with the wrong
        credential, which is the failure this whole subsystem exists to prevent.

        Args:
            name: The secret's name, as the instance declares it.
            default: Returned when *no secret of that name was delivered*.
                Omit it to get :class:`~novaos_client_sdk.exceptions.SecretNotDeliveredError`
                instead, which is the right choice for a secret the skill cannot
                run without.

        Returns:
            The secret value, or ``default`` when nothing arrived under ``name``.

        Raises:
            SecretNotDeliveredError: Nothing arrived under ``name`` and no
                ``default`` was given. Also raised when the caller never
                activated the extension at all — an agent-to-agent call or a
                hand-written request, neither of which seals secrets.
            RecipientKeyUnavailableError: The envelope names a key this pod does
                not hold.
            SealedEnvelopeError: The envelope did not open.

        Example::

            @agent.skill("send")
            async def send(ctx, text):
                password = ctx.get_secret("SMTP_PASSWORD")
                region = ctx.get_secret("AWS_REGION", "eu-west-1")
        """
        sealed = self._sealed_user_context()
        if sealed is None:
            if default is not _NO_SECRET_DEFAULT:
                return default
            uri = _user_context_extension_uri()
            why = (
                f"the '{uri}' extension was not activated for this request"
                if uri is not None
                else "the SDK's 'crypto' extra is not installed, so sealed delivery is unavailable"
            )
            raise SecretNotDeliveredError(
                f"no secret named '{name}' is available: {why}",
                details={"secret_name": name, "delivered": [], "extension": uri},
            )

        try:
            return sealed.open(
                name,
                # The sidecar's claim wins over the request body's; see the
                # note on `_verified_tenant_id`.
                tenant_id=self._verified_tenant_id or self._ctx.tenant_id,
                instance_id=self._ctx.instance_id,
                run_id=self._ctx.run_id,
            )
        except SecretNotDeliveredError:
            if default is not _NO_SECRET_DEFAULT:
                return default
            raise

    @property
    def run_id(self) -> str:
        return self._ctx.run_id

    @property
    def idempotency_key(self) -> str:
        """Stable dedup key for this execution — see :class:`AgentRuntimeContext`.

        Agent execution is at-least-once: the same logical request can arrive
        twice. A skill with side effects should record this key alongside the
        effect and skip a request whose key it has already handled. Empty when
        the platform sent none; do not dedupe on :attr:`run_id` instead, which
        a HITL continuation reuses.
        """
        return self._ctx.idempotency_key

    @property
    def custom_headers(self) -> dict[str, str]:
        """The caller's ``custom_headers`` for this request, lower-cased and filtered.

        Service-account callers pass ``custom_headers`` on execute; the platform
        delivers them as request headers. Only names that pass
        :func:`~novaos_client_sdk.header_safety.is_safe_custom_header_name` are
        kept, so platform-owned headers (``Authorization``, ``X-NovaOS-*``,
        ``traceparent``, ...) never appear here. Every ``ctx.tools.*`` call
        already forwards them, so a skill only needs this to read a value.

        Returns a copy. Values may carry credentials (an end-user JWT): do not
        log them.
        """
        return dict(self._custom_headers)

    @property
    def mcp_write_hitl_active(self) -> bool:
        """Mint-time signal: does this run's token carry a write-capable
        ``mcp:*`` scope? See :class:`AgentRuntimeContext` for the full
        contract, including why this is always ``False`` for a
        service-account run and why it is a hint rather than a guarantee.

        For a precise, per-tool answer instead of this coarse run-level one,
        use ``ctx.tools.hitl.requires_write_hitl(tool_fqn)``.
        """
        return self._ctx.mcp_write_hitl_active

    @property
    def thread_id(self) -> str:
        return self._ctx.thread_id

    @property
    def user_id(self) -> str:
        return self._ctx.user_id

    @property
    def tenant_id(self) -> str:
        return self._ctx.tenant_id

    @property
    def instance_id(self) -> str:
        return self._ctx.instance_id

    @property
    def trace_id(self) -> str:
        """The bare 32-hex W3C trace-id, or ``""`` when none was propagated.

        Extracted from the inbound ``traceparent`` header (falling back to a
        platform-supplied ``trace_id`` when no header arrived). This is the id
        a trace backend keyed on ``trace_id`` expects — for the full header
        needed to propagate trace context on outbound calls, see
        :attr:`traceparent`.
        """
        return self._ctx.trace_id

    @property
    def traceparent(self) -> str:
        """The full inbound W3C ``traceparent`` header, or ``""`` when none arrived."""
        return self._ctx.traceparent

    @property
    def session_id(self) -> str:
        return self._ctx.session_id

    @property
    def task_id(self) -> str:
        """A2A ``Task.id`` for this execution.

        Correlates with :attr:`run_id` but is not the same identifier — see
        :class:`~novaos_client_sdk.agent_types.AgentRuntimeContext`.
        """
        return self._ctx.task_id

    @property
    def context_id(self) -> str:
        """A2A ``contextId`` grouping this task with related ones.

        A2A's name for what ACP called ``session_id`` (ADR F2); :attr:`session_id`
        returns the same value for callers that still use the older name.
        """
        return self._ctx.context_id

    @property
    def history(self) -> tuple[ConversationTurn, ...]:
        """Earlier turns of this conversation, oldest first.

        The A2A-era replacement for reading ACP's ``input: Message[]`` beyond
        its last entry.  ``SendMessage`` carries exactly one message, so the
        platform sends prior turns structurally in the NovaOS context instead;
        this is where they are read.  The current turn is **not** included — it
        still arrives as the skill's input text — so a skill that ignores this
        property behaves exactly as it did before.

        **Empty is the common case, and it does not mean "first turn."** The
        platform only carries history for a run that belongs to a conversation
        thread: the marketplace chat surface, and a service-account run that
        named a ``thread_id``. A task, a team step, a background run, a
        single-shot service-account run, an agent-to-agent call and a
        hand-written request all arrive with none. Read an empty history as *no
        prior context was supplied*, and never infer from it that the
        conversation has no past.

        **Not A2A's ``contextId`` mechanism, and deliberately so.** A2A expects
        a server to recover earlier turns from its own task store, keyed on
        ``contextId``. No NovaOS pod can: ``AgentBuilder``'s task store
        is in-process and dies with the pod exactly as a
        scaffolded pod's ``InMemoryTaskStore`` does, and an ephemeral run gets
        a fresh pod for every turn. :attr:`context_id` therefore still
        correlates the turns — it is just not something a pod can look
        anything up in today.

        Returns:
            The turns as the platform sent them, oldest first; ``()`` when none
            arrived or none were readable.

        Example::

            from dataclasses import asdict

            @agent.skill("chat")
            async def chat(ctx, user_input):
                messages = [{"role": "system", "content": SYSTEM_PROMPT}]
                messages += [asdict(turn) for turn in ctx.history]
                messages.append({"role": "user", "content": user_input})
                return SkillResult(output=await call_llm(messages))
        """
        return self._ctx.history

    @property
    def config(self) -> dict[str, Any]:
        return self._ctx.config

    @property
    def output_schema(self) -> dict[str, Any] | None:
        """Caller-supplied JSON Schema for structured extraction (Ex10), if any.

        ``None`` when the caller didn't supply one. Use with
        :func:`novaos_client_sdk.schema_validation.validate_against_schema`
        to check a skill's extracted output against it before returning.
        """
        return self._ctx.output_schema

    @property
    def memory_budget(self) -> dict[str, int] | None:
        """The pre-fetch budget ``temporal-worker`` attached, or ``None``.

        ``None`` means this agent's catalog manifest does not declare the
        ``memory`` capability — :class:`~novaos_client_sdk.agent_builder.AgentBuilder`'s
        pre-fetch hook reads this property to decide whether to call
        ``memory.assemble`` at all, so a skill does not need to check this
        itself unless it wants to know why :attr:`memory` came back empty.
        """
        return self._ctx.memory_budget

    @property
    def memory(self) -> "MemoryNamespace":
        """Pre-fetched memory context, and a factory for on-demand ``MemoryClient`` calls.

        ``ctx.memory.context`` is the :class:`~novaos_client_sdk.memory.AssembleResult`
        :class:`~novaos_client_sdk.agent_builder.AgentBuilder`'s pre-fetch hook
        assembled before your skill ran — ``None`` if the ``memory``
        capability isn't declared for this agent, or if pre-fetch failed
        (logged, never raised into your skill).

        Backed by the same :class:`~novaos_client_sdk.context.MemoryNamespace`
        the lower-level :class:`~novaos_client_sdk.context.ExecutionContext`
        uses, constructed once and cached on this instance — unlike
        :meth:`to_execution_context`, which builds a fresh
        ``ExecutionContext`` (and so a fresh, empty namespace) on every call,
        this property must return the *same* namespace every time so a value
        the pre-fetch hook sets is still there when your skill reads it.
        """
        if self._memory_ns is None:
            from .context import MemoryNamespace

            self._memory_ns = MemoryNamespace(self.to_execution_context())
        return self._memory_ns

    def _emit_platform_event(self, event_type: str, payload: dict[str, Any]) -> None:
        """Record one platform-originated event (e.g. ``memory.assembled``) for this turn.

        A streaming request has :attr:`_emit_tool_event` wired to its SSE
        queue, so the event reaches the caller immediately. A non-streaming
        request has no comparable channel mid-execution — it only ever sends
        one response — so the event is buffered in :attr:`_buffered_events`
        for ``AgentBuilder`` to fold into that response's metadata once the
        skill returns.
        """
        if self._emit_tool_event is not None:
            self._emit_tool_event(event_type, payload)
        else:
            self._buffered_events.append((event_type, payload))

    @property
    def buffered_events(self) -> tuple[tuple[str, dict[str, Any]], ...]:
        """Platform events recorded via :meth:`_emit_platform_event` on a non-streaming request.

        Always empty on a streaming request, since those events went straight
        to the SSE queue instead — see :meth:`_emit_platform_event`.
        """
        return tuple(self._buffered_events)

    @property
    def logger(self) -> logging.Logger:
        """Structured JSON logger pre-bound to this execution's run_id, agent_id, and tenant_id.

        Each line emitted via this logger is a newline-delimited JSON object
        compatible with the Loki/Promtail ingestion pipeline. The
        marketplace Agent Log Panel's Run ID filter relies on the
        ``run_id`` field being present in every log line.

        Usage::

            ctx.logger.info("Tool call completed")
            ctx.logger.error("Upstream failed", extra={"tool": "search", "status": 503})
            ctx.logger.debug("Parsed %d items", len(items))

        Returns:
            A ``logging.Logger`` writing structured JSON to stdout.
        """
        if self._logger is None:
            self._logger = get_agent_logger(
                run_id=self._ctx.run_id,
                agent_id=self._ctx.instance_id,
                tenant_id=self._ctx.tenant_id,
                # Use a unique name per run so multiple concurrent executions
                # in the same process each get their own context-bound logger.
                name=f"novaos.agent.{self._ctx.run_id or 'default'}",
            )
        return self._logger

    @property
    def entity_refs(self) -> list[str]:
        return list(self._entity_refs)

    def set_entity_context(self, refs: list[str]) -> None:
        self._entity_refs = list(refs)
        self.tools.set_entity_refs(refs)

    def to_execution_context(self) -> "ExecutionContext":
        """Convert to an ``ExecutionContext`` for use with the lower-level SDK APIs.

        ``AgentExecutionContext`` is the A2A-builder-facing context (constructed from
        an A2A ``SendMessage`` request + headers).  ``ExecutionContext`` is the
        lower-level dataclass used by ``context.py`` / ``MemoryClient`` /
        auth-header utilities.

        D3 fix: prior code had no bridge between the two context types, so agents
        using the builder pattern could not access ``ctx.memory`` via the
        ``ExecutionContext`` interface without duplicating header parsing logic.

        Returns:
            An ``ExecutionContext`` populated from this context's ``AgentRuntimeContext``.

        Example::

            # Inside a skill handler
            lower = ctx.to_execution_context()
            mem_headers = lower.to_auth_headers()
            # mem_headers == {"Authorization": "Bearer <execution_token>"}
        """
        from .context import ExecutionContext

        # ⚠️ execution_token is resolved in preference over mcp_gateway_token —
        #    the auth-proxy sidecar injects the RS256 execution token into
        #    x-novaos-execution-token (resolved to execution_token here).
        #    mcp_gateway_token is only used for direct MCP calls without the sidecar.
        token = self._ctx.execution_token or self._ctx.mcp_gateway_token or None
        return ExecutionContext(
            user_id=self._ctx.user_id or None,
            tenant_id=self._ctx.tenant_id or None,
            run_id=self._ctx.run_id or None,
            # agent_id maps to instance_id — AgentRuntimeContext has no separate agent_id field.
            # ⚠️ instance_id is the pod-level identifier in AgentRuntimeContext; agent_id in
            #    ExecutionContext is the logical agent identifier from the token sub claim.
            #    These are conceptually different but instance_id is the best available proxy.
            agent_id=self._ctx.instance_id or None,
            instance_id=self._ctx.instance_id or None,
            execution_token=token,
        )

    @classmethod
    def from_a2a_request(
        cls,
        message: A2AMessage,
        params: dict[str, Any],
        headers: dict[str, str],
        *,
        task_id: str = "",
        extensions: ExtensionActivation | None = None,
        emit_tool_event: Callable[[str, dict[str, Any]], None] | None = None,
        task_updater: TaskUpdater | None = None,
        cancellation: "_CancellationToken | None" = None,
        referenced_tasks: tuple[Task, ...] = (),
        unresolved_reference_task_ids: tuple[str, ...] = (),
    ) -> "AgentExecutionContext":
        """Build a Context from an A2A ``SendMessage`` request.

        Replaces the ACP-era ``from_acp_request``.  The platform context no longer
        arrives as a top-level ``body["context"]`` object — A2A's request shape has
        no room for one — so it is read out of the request and message ``metadata``
        maps under :data:`NOVAOS_CONTEXT_METADATA_KEY`.

        A2A's own protocol correlators are read from the message itself, not from the
        NovaOS context, so they stay authoritative even when a caller sends no NovaOS
        context at all: ``message.context_id`` is the A2A ``contextId``, and
        ``task_id`` is supplied by the caller of this method (``AgentBuilder`` resolves
        it, since choosing between a continued task and a fresh one is dispatch's
        business, not the context's).

        Args:
            message: The already-parsed inbound A2A ``Message``
            params: The ``SendMessage`` params object, for its ``metadata`` map
            headers: Lower-cased HTTP request headers
            task_id: The A2A ``Task.id`` this execution belongs to
            extensions: The outcome of this request's ``A2A-Extensions``
                negotiation, which ``AgentBuilder`` performs before dispatch —
                the same division of labour as ``task_id``: negotiating is the
                handler's business, and the context only carries the result so a
                skill can read it.  Defaults to nothing activated.
            emit_tool_event: Forwarded to :meth:`__init__` — see its docstring.
                Only ``AgentBuilder``'s streaming handler passes one.
            task_updater: Forwarded to :meth:`__init__` — see
                :attr:`task_updater`. Only ``AgentBuilder``'s ``SendMessage``
                handler passes one.
            cancellation: Forwarded to :meth:`__init__` — see
                :attr:`is_cancelled` / :meth:`raise_if_cancelled`.
                ``AgentBuilder`` passes the same instance it also
                registers for ``CancelTask`` to find.
            referenced_tasks: Forwarded to :meth:`__init__` — see
                :attr:`referenced_tasks`. ``AgentBuilder``
                resolves ``message.reference_task_ids`` against its own
                ``task_store`` *before* calling this method — the same
                division of labour as ``task_id`` and ``extensions`` above:
                resolving is dispatch's business (it needs store access this
                context has no reason to hold), and the context only carries
                the result so a skill can read it.
            unresolved_reference_task_ids: Forwarded to :meth:`__init__` —
                see :attr:`unresolved_reference_task_ids`.

        Returns:
            A populated ``AgentExecutionContext``.
        """
        novaos_context = novaos_context_from_metadata(params.get("metadata"), message.metadata)

        # A2A contextId is authoritative for session grouping; the NovaOS context and
        # the legacy header are fallbacks for callers that have not moved over yet.
        context_id = (
            as_str(message.context_id)
            or as_str(novaos_context.get("session_id"))
            or headers.get("x-session-id", "")
        )

        # The inbound `traceparent` header carries the trace-id and span-id
        # encoded together (`00-<32hex trace-id>-<16hex span-id>-<flags>`); it is
        # not itself a trace-id.  Decode the bare ids out of it here so `trace_id`/
        # `span_id` hold what their names say, and keep the header intact in
        # `traceparent` for outbound propagation (see `AgentExecutionContext.__init__`).
        traceparent = headers.get("traceparent", "")
        decoded_trace_id = extract_trace_id(traceparent) if traceparent else None
        decoded_span_id = extract_parent_id(traceparent) if traceparent else None

        instance = cls(
            AgentRuntimeContext(
                run_id=novaos_context.get("run_id", ""),
                idempotency_key=novaos_context.get("idempotency_key", ""),
                thread_id=novaos_context.get("thread_id", ""),
                user_id=novaos_context.get("user_id", ""),
                tenant_id=novaos_context.get("tenant_id", headers.get("x-novaos-tenant-id", "")),
                instance_id=novaos_context.get("instance_id", ""),
                trace_id=decoded_trace_id or novaos_context.get("trace_id", ""),
                span_id=decoded_span_id or novaos_context.get("span_id", ""),
                traceparent=traceparent,
                session_id=context_id,
                execution_token=_resolve_execution_token(headers),
                mcp_gateway_url=novaos_context.get(
                    "mcp_gateway_url", os.environ.get("MCP_GATEWAY_URL", "")
                ),
                mcp_gateway_token=novaos_context.get("mcp_gateway_token", ""),
                gateway_url=novaos_context.get("gateway_url", ""),
                config=as_dict(novaos_context.get("config")),
                output_schema=_resolve_output_schema(novaos_context),
                task_id=task_id,
                context_id=context_id,
                history=_conversation_history(novaos_context),
                mcp_write_hitl_active=_resolve_mcp_write_hitl_active(novaos_context),
                memory_budget=_resolve_memory_budget(novaos_context),
            ),
            extensions=extensions,
            verified_tenant_id=headers.get("x-novaos-tenant-id", ""),
            emit_tool_event=emit_tool_event,
            task_updater=task_updater,
            cancellation=cancellation,
            referenced_tasks=referenced_tasks,
            unresolved_reference_task_ids=unresolved_reference_task_ids,
            custom_headers=headers,
        )
        raw_entity_refs = novaos_context.get("entity_refs", [])
        if isinstance(raw_entity_refs, list) and raw_entity_refs:
            instance.set_entity_context([r for r in raw_entity_refs if isinstance(r, str)])
        return instance
