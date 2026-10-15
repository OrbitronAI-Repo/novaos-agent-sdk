"""Custom header name safety validation for caller-supplied ``custom_headers``.

A service-account caller can pass ``custom_headers`` on execute and the platform
forwards them to the agent pod. The gateway is the enforcement point; this filter
is a second layer so an agent never re-emits a platform-owned or hop-by-hop
header on an outbound tool call.

The rules mirror the platform gateway's custom-header name check.
``tests/data/custom-header-vectors.json`` holds the vectors both
implementations are tested against.
"""

import re
from collections.abc import Mapping

MAX_CUSTOM_HEADER_NAME_LENGTH = 128

FORBIDDEN_CUSTOM_HEADER_PREFIXES: tuple[str, ...] = (
    "x-novaos-",
    "x-connector-secret-",
    "x-accel-",
    "x-forwarded-",
)

FORBIDDEN_CUSTOM_HEADER_NAMES: frozenset[str] = frozenset(
    {
        "authorization",
        "cookie",
        "host",
        "content-type",
        "content-length",
        "transfer-encoding",
        "traceparent",
        "tracestate",
        "x-idempotency-key",
        "x-session-id",
        "x-novaos-tenant-id",
        "x-novaos-user-id",
        "x-tenant-id",
        "mcp-session-id",
        "x-internal-token",
        "x-real-ip",
    }
)

# Headers every inbound A2A request carries as part of the HTTP exchange itself,
# not because a caller chose to send them. The shared denylist above covers
# platform-owned names only, so without this set `a2a-version`, `user-agent`,
# `connection` and the like would be taken for caller headers and replayed on
# outbound tool calls. Hop-by-hop headers must never be forwarded at all.
REQUEST_ENVELOPE_HEADER_NAMES: frozenset[str] = frozenset(
    {
        "a2a-version",
        "a2a-extensions",
        "accept",
        "accept-charset",
        "accept-encoding",
        "accept-language",
        "cache-control",
        "connection",
        "content-encoding",
        "expect",
        "forwarded",
        "keep-alive",
        "origin",
        "pragma",
        "proxy-authenticate",
        "proxy-authorization",
        "referer",
        "te",
        "trailer",
        "upgrade",
        "user-agent",
        "via",
    }
)
REQUEST_ENVELOPE_HEADER_PREFIXES: tuple[str, ...] = ("sec-", "proxy-")

# RFC 7230 token characters. `fullmatch` rather than `$`, which would let a
# trailing newline through.
_RFC7230_TOKEN_RE = re.compile(r"[a-zA-Z0-9!#$%&'*+\-.^_`|~]+")


def is_safe_custom_header_name(name: str) -> bool:
    """Return whether ``name`` may be forwarded as a custom header."""
    if not name or len(name) > MAX_CUSTOM_HEADER_NAME_LENGTH:
        return False
    if _RFC7230_TOKEN_RE.fullmatch(name) is None:
        return False
    lower = name.lower()
    if lower in FORBIDDEN_CUSTOM_HEADER_NAMES:
        return False
    return not lower.startswith(FORBIDDEN_CUSTOM_HEADER_PREFIXES)


def filter_custom_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Keep only the entries of ``headers`` that are caller custom headers.

    ``headers`` is typically a whole inbound request's headers. An entry is kept
    when its name passes :func:`is_safe_custom_header_name` and is not part of
    the request envelope (``user-agent``, ``a2a-version``, hop-by-hop headers...).
    Names are lower-cased so the result is stable regardless of how the transport
    cased them.
    """
    kept: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(value, str) or not is_safe_custom_header_name(name):
            continue
        lower = name.lower()
        if lower in REQUEST_ENVELOPE_HEADER_NAMES or lower.startswith(
            REQUEST_ENVELOPE_HEADER_PREFIXES
        ):
            continue
        kept[lower] = value
    return kept
