"""Structured JSON logger for NovaOS agents.

Provides a stdlib-only, zero-dependency JSON logger that emits log lines
compatible with the Loki/Promtail ingestion pipeline introduced for
developer log access and observability on published agents.

Each log line written to stdout has the form:

    {"timestamp": "2026-07-25T14:00:00.123456Z", "level": "info",
     "message": "Tool call completed",
     "run_id": "run-abc...", "agent_id": "agent-xyz...", "tenant_id": "t-...",
     "tool": "search"}

This makes every line filterable in the marketplace Agent Log Panel by level
and run_id, and queryable in Grafana/Loki with:

    {namespace="novaos-execution"} | json | run_id="<run-uuid>"

Usage via AgentExecutionContext (recommended):
    @agent.skill("run", description="...")
    async def run(ctx: AgentExecutionContext, input: str) -> SkillResult:
        ctx.logger.info("Starting run")
        ctx.logger.error("Tool failed", extra={"tool": "search", "error": str(e)})

Standalone usage (outside an A2A handler):
    from novaos_client_sdk import get_agent_logger
    logger = get_agent_logger(run_id="run-abc", agent_id="agent-xyz", tenant_id="t-1")
    logger.info("Processing started")

Every line also carries ``span_id`` (and ``trace_id``, if not bound) of the span
active when it was written, so a line links to one span of the run's trace.

Lines from any other logger (the SDK's own modules, third-party libraries) carry
the run's ids too while a skill runs: when an agent runs its first skill,
:func:`install_structured_root_logging` gives the root logger a JSON stdout
handler with :class:`RunContextFilter`, unless the application configured the
root logger itself or ``NOVAOS_STRUCTURED_ROOT_LOGS=false`` is set.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, TextIO

from .pii import mask_pii, mask_pii_in_dict
from .run_context import current_run
from .tracing import current_span_ids

# Names that collide with a stdlib LogRecord's own attributes (plus "asctime",
# which logging.Formatter computes lazily). Passing any of these inside `extra=`
# makes Logger.makeRecord() raise `KeyError: Attempt to overwrite '<name>' in
# LogRecord` — this is the same reserved set _NovaOSJsonFormatter filters out
# when reading a record's extra fields back out; kept as one shared constant so
# the two can't drift out of sync.
_RESERVED_LOG_RECORD_ATTRS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",  # Python 3.12+ asyncio task name
        "thread",
        "threadName",
    }
)

# Keyword arguments a stdlib Logger's _log() actually accepts. Anything else
# passed as a keyword (the structlog convention, e.g. logger.info("msg", tool="x"))
# raises TypeError against a bare stdlib Logger instead of being treated as a
# structured field.
_LOGGER_METHOD_KWARGS = frozenset({"exc_info", "extra", "stack_info", "stacklevel"})

# Methods wrapped by _make_structlog_tolerant() below.
_LOGGER_METHODS_TO_WRAP = ("debug", "info", "warning", "error", "critical", "exception")

# The run's ids RunContextFilter stamps onto a record, in the order they are written.
_RUN_LOG_FIELDS = ("run_id", "agent_id", "tenant_id", "trace_id", "instance_id")

# Set to false/0/no/off to keep the root logger as the application left it.
STRUCTURED_ROOT_LOGS_ENV = "NOVAOS_STRUCTURED_ROOT_LOGS"
_root_logging_installed = False


def _make_structlog_tolerant(bound_method: Callable[..., None]) -> Callable[..., None]:
    """Wrap a Logger method so unrecognized keyword args are folded into ``extra``.

    Without this, ``logger.info("msg", tool="x")`` raises
    ``TypeError: Logger._log() got an unexpected keyword argument 'tool'`` against a
    bare stdlib Logger. That's a common call shape borrowed from structured-logging
    libraries (structlog, etc.) that this SDK's logger doesn't natively support.

    The failure mode is worse than a normal bug: if this call sits inside an
    ``except`` handler, the TypeError *replaces* the exception being handled, and
    the original error is lost — the caller sees an unrelated logging crash instead
    of whatever actually went wrong.

    A keyword that happens to collide with a reserved ``LogRecord`` attribute name
    (``name``, ``message``, ``module``, ...) is renamed with a ``_field`` suffix
    rather than passed through as-is — ``extra={"name": ...}`` raises ``KeyError``
    from stdlib's ``makeRecord`` for the exact same reason the plain keyword raised
    ``TypeError`` in the first place. This wrapper must never raise, full stop.
    """

    @functools.wraps(bound_method)
    def wrapper(msg: object, *args: object, **kwargs: Any) -> None:
        unexpected = {k: v for k, v in kwargs.items() if k not in _LOGGER_METHOD_KWARGS}
        if unexpected:
            for key in unexpected:
                del kwargs[key]
            safe_fields = {
                (f"{k}_field" if k in _RESERVED_LOG_RECORD_ATTRS else k): v
                for k, v in unexpected.items()
            }
            existing_extra = kwargs.get("extra") or {}
            kwargs["extra"] = {**existing_extra, **safe_fields}
        return bound_method(msg, *args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class _NovaOSJsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects.

    Injects context fields (run_id, agent_id, tenant_id, trace_id, instance_id) from a fixed mapping
    bound at logger-creation time, then merges any extra fields the caller
    passes via ``extra={}``.
    """

    # Note: this formatter's own reserved set intentionally excludes "asctime" —
    # it's not a real LogRecord attribute vars(record) would ever surface, only
    # something Formatter.format() computes on demand — so it's harmless to keep
    # this filter scoped to what vars(record) can actually contain, while the
    # shared constant (used by _make_structlog_tolerant) also covers "asctime"
    # since that one CAN collide when passed through extra=.
    _RESERVED = _RESERVED_LOG_RECORD_ATTRS - {"asctime"}

    def __init__(
        self, context_fields: dict[str, str], *, include_logger_name: bool = False
    ) -> None:
        super().__init__()
        # Only include non-empty context values to keep lines clean.
        self._ctx: dict[str, str] = {k: v for k, v in context_fields.items() if v}
        self._include_logger_name = include_logger_name

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        masked_message = mask_pii(record.message) or record.message

        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )
            + "Z",
            "level": record.levelname.lower(),
            "message": masked_message,
        }

        if self._include_logger_name:
            payload["logger"] = record.name

        # Inject run_id / agent_id / tenant_id / trace_id / instance_id from the bound context.
        payload.update(self._ctx)
        # Then the run's ids RunContextFilter stamped on the record (lines from
        # loggers not bound to a run), and the active span, so the line links to
        # the span it was written in.
        for field in (*_RUN_LOG_FIELDS, "span_id"):
            value = getattr(record, field, "")
            if value and field not in payload:
                payload[field] = value
        trace_id, span_id = current_span_ids()
        if span_id:
            payload.setdefault("trace_id", trace_id)
            payload.setdefault("span_id", span_id)

        # Merge caller-supplied extra fields (skip internal logging attrs).
        extra_fields: dict[str, Any] = {}
        for key, value in vars(record).items():
            if key not in self._RESERVED and not key.startswith("_") and key not in payload:
                extra_fields[key] = value
        if extra_fields:
            payload.update(mask_pii_in_dict(extra_fields))

        # Append exception info if present.
        if record.exc_info:
            exc_text = self.formatException(record.exc_info)
            payload["exc_info"] = mask_pii(exc_text) or exc_text

        return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


def get_agent_logger(
    *,
    run_id: str = "",
    agent_id: str = "",
    tenant_id: str = "",
    trace_id: str = "",
    instance_id: str = "",
    name: str = "novaos.agent",
) -> logging.Logger:
    """Return a structured JSON logger bound to the given execution context fields.

    The logger writes newline-delimited JSON to stdout.  Each line includes
    ``run_id``, ``agent_id``, ``tenant_id``, ``trace_id`` and ``instance_id`` (each
    when non-empty) so that Loki can correlate entries across the execution
    namespace and join them to the run's trace.

    The returned ``logging.Logger`` is a standard Python logger — all normal
    logging methods (``info``, ``warning``, ``error``, ``debug``, ``exception``)
    work as expected.  Pass additional fields via ``extra``:

        logger.info("Tool result", extra={"tool": "search", "hit_count": 5})

    Structured-logging-style keyword calls also work, e.g. ``logger.info("Tool
    result", tool="search")`` — any keyword the method doesn't otherwise recognize
    is folded into ``extra`` automatically instead of raising ``TypeError``.

    Args:
        run_id: Execution run ID (from ``ctx.run_id``).
        agent_id: Agent ID (from ``ctx.agent_id``).
        tenant_id: Tenant ID (from ``ctx.tenant_id``).
        trace_id: W3C trace id of the run (from ``ctx.trace_id``).
        instance_id: Agent instance ID (from ``ctx.instance_id``).
        name: Logger name (useful when multiple agents share a process).

    Returns:
        A ``logging.Logger`` configured with a JSON stdout handler.

    Example::

        from novaos_client_sdk import get_agent_logger

        logger = get_agent_logger(
            run_id="run-abc123",
            agent_id="agent-xyz",
            tenant_id="tenant-1",
        )
        logger.info("Processing started")
    """
    logger = logging.getLogger(name)

    new_formatter = _NovaOSJsonFormatter(
        {
            "run_id": run_id,
            "agent_id": agent_id,
            "tenant_id": tenant_id,
            "trace_id": trace_id,
            "instance_id": instance_id,
        }
    )

    if not logger.handlers:
        # First time this named logger is requested: add a stdout handler.
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(new_formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        # Prevent records propagating to the root logger and producing duplicate
        # unstructured output when the root logger has its own handlers.
        logger.propagate = False
        # Tolerate structlog-style keyword calls (see _make_structlog_tolerant).
        # Wrapping instance attributes, not the class, so this only affects
        # loggers vended by this factory. Guarded by `not logger.handlers` above
        # so a logger already wrapped on a prior call isn't wrapped twice.
        for method_name in _LOGGER_METHODS_TO_WRAP:
            setattr(logger, method_name, _make_structlog_tolerant(getattr(logger, method_name)))
    else:
        # Logger already exists (same name requested again, possibly with different
        # context fields).  Replace the formatter on every existing handler so the
        # caller always gets log lines with the correct run_id / agent_id / tenant_id.
        for handler in logger.handlers:
            handler.setFormatter(new_formatter)

    return logger


# ---------------------------------------------------------------------------
# Run identity on every logger
# ---------------------------------------------------------------------------


class RunContextFilter(logging.Filter):
    """Stamp the current run's ids and the active span's id onto every record.

    While a skill runs, ``run_id``, ``agent_id``, ``tenant_id``, ``trace_id`` and
    ``instance_id`` come from the run :class:`~novaos_client_sdk.agent_builder.AgentBuilder`
    bound for it (see :mod:`novaos_client_sdk.run_context`); ``span_id`` is the
    active OpenTelemetry span's, whenever one is active. A field the caller
    already set (``extra=``) is left alone. Outside a run only ``span_id`` (and
    the span's ``trace_id``) is added.

    The SDK attaches it to the root handler it installs
    (:func:`install_structured_root_logging`). An application that configures
    logging itself can add it to its own handlers to get the same fields::

        handler.addFilter(RunContextFilter())

    Never rejects a record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        run = current_run()
        fields: dict[str, str] = {}
        if run is not None:
            fields = {field: getattr(run, field) for field in _RUN_LOG_FIELDS}
        trace_id, span_id = current_span_ids()
        if span_id:
            fields["span_id"] = span_id
            fields["trace_id"] = fields.get("trace_id") or trace_id
        for field, value in fields.items():
            if value and not getattr(record, field, ""):
                setattr(record, field, value)
        return True


class _StdoutHandler(logging.StreamHandler):
    """A stream handler that writes to whatever ``sys.stdout`` is at the time of each line."""

    def __init__(self) -> None:
        super().__init__(sys.stdout)

    @property
    def stream(self) -> TextIO:
        return sys.stdout

    @stream.setter
    def stream(self, value: TextIO) -> None:
        pass


def install_structured_root_logging() -> bool:
    """Give the root logger a JSON stdout handler that stamps the run's ids, once per process.

    Lines from the SDK's own module loggers and from third-party libraries then
    reach stdout as JSON with ``run_id``, ``trace_id``, ``span_id`` and the
    other run fields while a skill runs, so ``agent logs --run-id`` shows them
    too. ``ctx.logger`` and :func:`get_agent_logger` loggers do not propagate to
    the root logger, so their lines are not written twice. The root logger's
    level is not changed (``WARNING`` unless the application set another).

    :class:`~novaos_client_sdk.agent_builder.AgentBuilder` calls this when it
    runs its first skill, not on import. It does nothing when the root logger
    already has a handler (the application configured logging itself; add
    :class:`RunContextFilter` to that handler instead), when
    ``NOVAOS_STRUCTURED_ROOT_LOGS`` is ``false``/``0``/``no``/``off``, or on any
    call after the first.

    Returns:
        ``True`` when this call added the handler.
    """
    global _root_logging_installed
    if _root_logging_installed:
        return False
    _root_logging_installed = True
    if os.environ.get(STRUCTURED_ROOT_LOGS_ENV, "").strip().lower() in {"false", "0", "no", "off"}:
        return False
    root = logging.getLogger()
    if root.handlers:
        return False
    handler = _StdoutHandler()
    handler.setFormatter(_NovaOSJsonFormatter({}, include_logger_name=True))
    handler.addFilter(RunContextFilter())
    root.addHandler(handler)
    return True
