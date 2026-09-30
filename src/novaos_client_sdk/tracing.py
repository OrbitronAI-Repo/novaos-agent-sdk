"""
OpenTelemetry instrumentation for NovaOS Agent Client SDK.

ERD: SDK-05 (FR-26)

Provides distributed tracing for all tool calls and gateway operations.
Propagates W3C Trace Context (traceparent) on all outbound HTTP calls.

Quick-start (env-var driven)::

    OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 \\
    OTEL_SERVICE_NAME=my-agent \\
    python agent.py

Or programmatically::

    from novaos_client_sdk.tracing import configure_otel
    configure_otel(
        endpoint="http://otel-collector:4318",
        service_name="my-agent",
    )
"""

import os
import re
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

_OTEL_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
_OTEL_SERVICE_NAME_ENV = "OTEL_SERVICE_NAME"
_OTEL_HEADERS_ENV = "OTEL_EXPORTER_OTLP_HEADERS"

_otel_configured = False

_tracer = None
_meter = None
_tool_call_duration_histogram = None
_tool_call_errors_counter = None
_HAS_OTEL = False
_HAS_OTEL_METRICS = False

try:
    from opentelemetry import trace
    from opentelemetry.propagate import inject
    from opentelemetry.trace import StatusCode

    _HAS_OTEL = True
except ImportError:
    pass

try:
    from opentelemetry import metrics

    _HAS_OTEL_METRICS = True
except ImportError:
    pass


def _get_tracer():
    """Lazily initialize the OpenTelemetry tracer."""
    global _tracer
    if _tracer is None and _HAS_OTEL:
        from novaos_client_sdk import __version__

        _tracer = trace.get_tracer("novaos-agent-client-sdk", __version__)
    return _tracer


def _get_meter():
    """Lazily initialize the OpenTelemetry meter."""
    global _meter
    if _meter is None and _HAS_OTEL_METRICS:
        from novaos_client_sdk import __version__

        _meter = metrics.get_meter("novaos-agent-client-sdk", __version__)
    return _meter


def _get_tool_call_metrics():
    """Return (duration_histogram, errors_counter) instruments, or (None, None)."""
    global _tool_call_duration_histogram, _tool_call_errors_counter
    meter = _get_meter()
    if meter is None:
        return None, None
    if _tool_call_duration_histogram is None:
        _tool_call_duration_histogram = meter.create_histogram(
            "sdk.tool_call.duration_ms",
            unit="ms",
            description="Duration of MCP tool calls in milliseconds",
        )
        _tool_call_errors_counter = meter.create_counter(
            "sdk.tool_call.errors",
            description="Count of failed MCP tool calls",
        )
    return _tool_call_duration_histogram, _tool_call_errors_counter


_TRACEPARENT_RE = re.compile(r"^[\da-f]{2}-[\da-f]{32}-[\da-f]{16}-[\da-f]{2}$")


def get_traceparent() -> str:
    """Return the current traceparent from env or active OTel context."""
    if _HAS_OTEL:
        carrier: dict[str, str] = {}
        inject(carrier)
        tp = carrier.get("traceparent")
        if tp:
            return tp
    return os.environ.get("TRACEPARENT", "")


def is_valid_traceparent(value: str) -> bool:
    """Validate a W3C traceparent header value."""
    return bool(_TRACEPARENT_RE.match(value))


def extract_trace_id(traceparent: str) -> str | None:
    """Extract the 32-hex trace-id from a traceparent string."""
    if not is_valid_traceparent(traceparent):
        return None
    return traceparent.split("-")[1]


def extract_parent_id(traceparent: str) -> str | None:
    """Extract the 16-hex parent-id (span-id) from a traceparent string."""
    if not is_valid_traceparent(traceparent):
        return None
    return traceparent.split("-")[2]


def propagate_trace(headers: dict[str, str], traceparent: str = "") -> dict[str, str]:
    """Return a copy of *headers* with traceparent added (if available)."""
    result = headers.copy()
    tp = traceparent or get_traceparent()
    if tp:
        result["traceparent"] = tp
    return result


@contextmanager
def traced_tool_call(
    tool_name: str,
    args: dict[str, Any] | None = None,
    run_id: str = "",
) -> Generator[Any | None, None, None]:
    """Context manager that creates an OTel span for a tool call.

    Usage:
        with traced_tool_call("novaos.knowledge.query", {"query": "..."}) as span:
            result = await self._call_tool("novaos.knowledge.query", args)
    """
    tracer = _get_tracer()

    if tracer is None:
        yield None
        return

    duration_histogram, errors_counter = _get_tool_call_metrics()
    metric_attrs = {"novaos.tool.name": tool_name}
    start = time.perf_counter()

    attributes: dict[str, Any] = {
        "novaos.tool.name": tool_name,
        "novaos.tool.args_count": len(args) if args else 0,
    }
    if run_id:
        attributes["novaos.run_id"] = run_id

    with tracer.start_as_current_span(
        f"tool.{tool_name}",
        attributes=attributes,
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            if errors_counter is not None:
                errors_counter.add(1, metric_attrs)
            raise
        finally:
            if duration_histogram is not None:
                duration_ms = (time.perf_counter() - start) * 1000
                duration_histogram.record(duration_ms, metric_attrs)


def inject_trace_headers(headers: dict[str, str]) -> dict[str, str]:
    """Inject W3C Trace Context headers into outbound HTTP request headers."""
    if _HAS_OTEL:
        inject(headers)
    else:
        traceparent = os.environ.get("TRACEPARENT")
        if traceparent:
            headers["traceparent"] = traceparent
    return headers


@contextmanager
def traced_gateway_call(
    method: str,
    endpoint: str,
) -> Generator[Any | None, None, None]:
    """Context manager that creates an OTel span for a gateway HTTP call."""
    tracer = _get_tracer()

    if tracer is None:
        yield None
        return

    with tracer.start_as_current_span(
        f"gateway.{method} {endpoint}",
        attributes={
            "http.method": method,
            "http.url": endpoint,
            "novaos.component": "gateway-client",
        },
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise


def configure_otel(
    endpoint: str | None = None,
    service_name: str | None = None,
    headers: dict[str, str] | None = None,
) -> bool:
    """Bootstrap an OTLP HTTP exporter and register it as the global TracerProvider.

    Call once at agent startup.  Reads from environment variables when
    keyword arguments are omitted:

    - ``OTEL_EXPORTER_OTLP_ENDPOINT`` — collector HTTP endpoint
      (e.g. ``http://otel-collector:4318``)
    - ``OTEL_SERVICE_NAME`` — service name emitted on every span
    - ``OTEL_EXPORTER_OTLP_HEADERS`` — comma-separated ``key=value`` pairs
      forwarded to the collector (e.g. for auth tokens)

    Returns ``True`` when the provider was successfully registered, ``False``
    when the ``otel`` extra dependencies are not installed (no-op, not an
    error).

    Example::

        from novaos_client_sdk.tracing import configure_otel
        configure_otel(
            endpoint="http://otel-collector:4318",
            service_name="echo-agent",
        )
    """
    global _otel_configured
    if _otel_configured:
        return True

    try:
        from opentelemetry import metrics as otel_metrics
        from opentelemetry import trace as otel_trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        return False

    resolved_endpoint = endpoint or os.environ.get(_OTEL_ENDPOINT_ENV)
    if not resolved_endpoint:
        return False

    resolved_service = service_name or os.environ.get(_OTEL_SERVICE_NAME_ENV, "novaos-agent")

    resolved_headers: dict[str, str] = {}
    if headers:
        resolved_headers = headers
    else:
        raw = os.environ.get(_OTEL_HEADERS_ENV, "")
        for pair in raw.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, _, v = pair.partition("=")
                resolved_headers[k.strip()] = v.strip()

    # Normalise: if the caller already supplied the full path, don't append again.
    base = resolved_endpoint.rstrip("/")
    if base.endswith("/v1/traces"):
        otlp_traces_endpoint = base
        otlp_metrics_endpoint = base.replace("/v1/traces", "/v1/metrics")
    elif base.endswith("/v1/metrics"):
        otlp_metrics_endpoint = base
        otlp_traces_endpoint = base.replace("/v1/metrics", "/v1/traces")
    else:
        otlp_traces_endpoint = f"{base}/v1/traces"
        otlp_metrics_endpoint = f"{base}/v1/metrics"

    resource = Resource.create({SERVICE_NAME: resolved_service})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=otlp_traces_endpoint,
        headers=resolved_headers,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    otel_trace.set_tracer_provider(provider)

    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(
            endpoint=otlp_metrics_endpoint,
            headers=resolved_headers,
        )
    )
    otel_metrics.set_meter_provider(
        MeterProvider(resource=resource, metric_readers=[metric_reader])
    )

    global _tracer, _meter, _tool_call_duration_histogram, _tool_call_errors_counter
    from novaos_client_sdk import __version__

    _tracer = provider.get_tracer("novaos-agent-client-sdk", __version__)
    _meter = otel_metrics.get_meter("novaos-agent-client-sdk", __version__)
    _tool_call_duration_histogram = None
    _tool_call_errors_counter = None
    _otel_configured = True

    return True
