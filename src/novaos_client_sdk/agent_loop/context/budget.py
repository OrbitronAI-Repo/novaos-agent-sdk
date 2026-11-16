"""Token estimates for a prompt. The SDK has no tokenizer, so a count is an estimate."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from typing import Any

CHARS_PER_TOKEN = 4
SAFETY_MARGIN = 1.2
MESSAGE_OVERHEAD_TOKENS = 4


def estimate_tokens(text: str) -> int:
    """Characters over four, plus a 20 percent margin, rounded up."""
    return math.ceil(len(text) / CHARS_PER_TOKEN * SAFETY_MARGIN)


def text_of(content: Any) -> str:
    if content is None:
        return ""
    return content if isinstance(content, str) else json.dumps(content, default=str)


def message_tokens(message: Mapping[str, Any]) -> int:
    tokens = estimate_tokens(text_of(message.get("content"))) + MESSAGE_OVERHEAD_TOKENS
    if message.get("tool_calls"):
        tokens += estimate_tokens(text_of(message["tool_calls"]))
    return tokens


def count_prompt(messages: Iterable[Mapping[str, Any]]) -> int:
    return sum(message_tokens(m) for m in messages)


def chars_for(tokens: int) -> int:
    """The most characters that still estimate to ``tokens`` or fewer."""
    return int(tokens * CHARS_PER_TOKEN / SAFETY_MARGIN)
