"""What the LLM Gateway refuses before a call reaches the provider, and how to read it.

The gateway refuses a prompt that is too large: above the model's input limit (the router's
pre-call check, which never truncates), above its cap on message text, or above the tenant's
tokens-per-minute limit (one request that needs more than that can never succeed). All come back
as HTTP 400. A refusal for any other reason (a guardrail, a budget, an outage) is not a size problem and
is not recognised here.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass

from .budget import CHARS_PER_TOKEN, SAFETY_MARGIN

# The gateway's default cap on the text of all messages in one request (its MAX_INPUT_CHARS).
PLATFORM_MAX_INPUT_CHARS = 500_000
# The same cap in estimated tokens. The estimate is above the real count for text, so a prompt
# kept under this stays under the character cap.
PLATFORM_MAX_PROMPT_TOKENS = math.ceil(PLATFORM_MAX_INPUT_CHARS / CHARS_PER_TOKEN * SAFETY_MARGIN)

_MARKERS = (
    "input_too_long",
    "context_length_exceeded",
    "contextwindowexceeded",
    "maximum context length",
    "max_input_tokens",
    "max input tokens",
    "tokens-per-minute limit",
)
_TOKEN_LIMITS = (
    re.compile(r"maximum context length is (\d[\d,]*)"),
    re.compile(r"max(?:imum)?[ _]input[ _]tokens\D{0,12}(\d[\d,]*)"),
)
# "Request needs ~111005 tokens, more than the tokens-per-minute limit (100000) for tenant ..."
_TENANT_LIMIT = re.compile(
    r"needs ~(\d[\d,]*) tokens, more than the tokens-per-minute limit \((\d[\d,]*)\)"
)
_CHAR_LIMIT = re.compile(r"maximum allowed \((\d[\d,]*) chars\)")


@dataclass(frozen=True)
class SizeRejection:
    """A refusal for size. ``limit_tokens`` is the limit the gateway named, if it named one, and
    ``needed_tokens`` what it counted in the request (in its own units, so use the two as a ratio).
    """

    limit_tokens: int | None
    needed_tokens: int | None = None


def _number(text: str) -> int:
    return int(text.replace(",", ""))


def size_rejection(exc: BaseException) -> SizeRejection | None:
    """``exc`` read as a size refusal from the gateway, or None if it is anything else."""
    status = getattr(exc, "status_code", None) or getattr(
        getattr(exc, "response", None), "status_code", None
    )
    if status is not None and status not in (400, 413):
        return None
    text = " ".join(str(part) for part in (exc, getattr(exc, "code", ""), getattr(exc, "body", "")))
    text = text.lower()
    if not any(marker in text for marker in _MARKERS):
        return None
    if found := _TENANT_LIMIT.search(text):
        return SizeRejection(_number(found.group(2)), _number(found.group(1)))
    for pattern in _TOKEN_LIMITS:
        if found := pattern.search(text):
            return SizeRejection(_number(found.group(1)))
    if found := _CHAR_LIMIT.search(text):
        return SizeRejection(math.ceil(_number(found.group(1)) / CHARS_PER_TOKEN * SAFETY_MARGIN))
    return SizeRejection(None)
