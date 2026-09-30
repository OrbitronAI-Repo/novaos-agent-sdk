"""Local HITL classification for MCP tool calls.

Mirrors the platform's own ``mcpToolActionClass`` classifier so a skill can answer "will this specific tool call
need write approval?" synchronously and without a network call, by running
the exact same name-based classification Gate A (``enforceMcpHitl`` in
mcp-gateway) uses to decide whether to return -32042. Keeping the regexes and
fail-closed default identical to that classifier is what lets this
never drift from what the platform will actually decide — see
``HITLTool.requires_write_hitl`` for the primary entry point skills use.

Unclassified tools default to write (fail-closed). The ``database`` category
is always a write.
"""

import re
from typing import Literal

MCP_TOOLS_CALL_READ = "mcp.tools.call.read"
MCP_TOOLS_CALL_WRITE = "mcp.tools.call.write"

_WRITE_CATEGORIES = {"database"}

# Leaf has both a read-ish token and a mutating token joined by
# or/and/from/then/with (get_or_create, update_from_query, search_and_replace).
# Checked before plain READ_NAME so those stay fail-closed write / -32042.
_MIXED_MUTATING_NAME = re.compile(
    r"(?:^|[^a-z0-9])(?:list|get|read|search|query|fetch|describe|find)(?:[^a-z0-9]+)"
    r"(?:or|and|from|then|with)(?:[^a-z0-9]+)"
    r"(?:create|update|delete|write|put|post|patch|insert|upsert|remove|set|send|"
    r"execute|invoke|mutate|replace|destroy|drop|alter|store|save)(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)
_MIXED_MUTATING_NAME_REVERSE = re.compile(
    r"(?:^|[^a-z0-9])"
    r"(?:create|update|delete|write|put|post|patch|insert|upsert|remove|set|send|"
    r"execute|invoke|mutate|replace|destroy|drop|alter|store|save)(?:[^a-z0-9]+)"
    r"(?:or|and|from|then|with)(?:[^a-z0-9]+)"
    r"(?:list|get|read|search|query|fetch|describe|find)(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)
# `read` and `write` compounded with no joiner (read_write_file, read-write-object,
# readWriteFile) is unambiguously a mutating leaf — unlike get_post / get_update /
# get_result_set, where the trailing token is a noun, not the verb "write" itself.
_READ_WRITE_COMPOUND_NAME = re.compile(
    r"(?:^|[^a-z0-9])(?:read[-_]?write|write[-_]?read)(?:[^a-z0-9]|$)", re.IGNORECASE
)
_WRITE_NAME = re.compile(
    r"(?:^|[^a-z0-9])(create|update|delete|write|put|post|patch|insert|upsert|remove|"
    r"set|send|execute|invoke|mutate|replace|destroy|drop|alter|grant|revoke|store|save)"
    r"(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)
_READ_NAME = re.compile(
    r"(?:^|[^a-z0-9])(list|get|read|search|query|fetch|describe)(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)


def mcp_tool_name_leaf(tool_name: str) -> str:
    """Leaf segment of an MCP tool id (after the last '.').

    Used so READ_NAME heuristics never fire on publisher/service path
    segments in an FQN (e.g. the "search" in "acme.search-api.create-payment-intent"
    must not force a read classification and skip -32042).
    """
    trimmed = tool_name.strip()
    if not trimmed:
        return trimmed
    parts = [p for p in trimmed.split(".") if p]
    return parts[-1] if parts else trimmed


def mcp_tool_action_class(
    tool_name: str,
    *,
    categories: list[str] | None = None,
    rag_action_class: Literal["read", "write"] | None = None,
) -> str:
    """Classify an MCP tool call as read or write for HITL purposes.

    ``rag_action_class`` takes priority over the name-regex fallback when the
    caller already knows the real RAG OPA action authoritatively (tools with
    no read/write verb in their name, like parse_document or graph_status,
    would otherwise fail-close to write).
    """
    cats = {c.lower() for c in (categories or [])}
    if cats & _WRITE_CATEGORIES:
        return MCP_TOOLS_CALL_WRITE
    if rag_action_class == "read":
        return MCP_TOOLS_CALL_READ
    if rag_action_class == "write":
        return MCP_TOOLS_CALL_WRITE

    leaf = mcp_tool_name_leaf(tool_name)
    if _MIXED_MUTATING_NAME.search(leaf) or _MIXED_MUTATING_NAME_REVERSE.search(leaf):
        return MCP_TOOLS_CALL_WRITE
    if _READ_WRITE_COMPOUND_NAME.search(leaf):
        return MCP_TOOLS_CALL_WRITE
    if _READ_NAME.search(leaf):
        return MCP_TOOLS_CALL_READ
    if _WRITE_NAME.search(leaf):
        return MCP_TOOLS_CALL_WRITE
    # Fail-closed: unclassified tools are treated as writes (require HITL / -32042).
    return MCP_TOOLS_CALL_WRITE
