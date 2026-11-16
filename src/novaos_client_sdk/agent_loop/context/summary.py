"""Summaries of the turns trimmed from the history."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from .budget import chars_for, text_of

OMITTED = "[older turns omitted]"
MIN_LINE_CHARS = 12


class Summarizer(Protocol):
    async def summarize(
        self, previous: str, dropped: Sequence[dict[str, Any]], *, max_tokens: int
    ) -> str:
        """Fold the ``dropped`` messages into ``previous``, in at most ``max_tokens``."""
        ...


def _turns(messages: Sequence[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    turns: list[list[dict[str, Any]]] = []
    for message in messages:
        if message.get("role") == "user" or not turns:
            turns.append([])
        turns[-1].append(message)
    return turns


def _clip(text: str, chars: int) -> str:
    text = " ".join(text.split())
    return text if len(text) <= chars else text[: max(chars - 1, 0)] + "…"


def fit_summary(summary: str, max_tokens: int) -> str:
    """``summary`` cut to ``max_tokens``: the newest part is kept, behind a marker."""
    limit = chars_for(max_tokens)
    if len(summary) <= limit:
        return summary
    keep = max(limit - len(OMITTED) - 1, 0)
    tail = summary[len(summary) - keep :] if keep else ""
    head, sep, rest = tail.partition("\n")
    return f"{OMITTED}\n{rest if sep else head}"


class ExtractiveSummarizer:
    """One line per dropped turn: the start of each message in it. No model call.

    Past the size limit every line is shortened, and the oldest lines go last behind a marker.
    """

    async def summarize(
        self, previous: str, dropped: Sequence[dict[str, Any]], *, max_tokens: int
    ) -> str:
        old = [line for line in previous.split("\n") if line and line != OMITTED]
        turns = _turns(dropped)
        limit = chars_for(max_tokens)
        count = len(old) + len(turns)
        if not count or limit <= 0:
            return ""
        # Every line gets an equal share, less its newline and the room for the marker.
        per_line = max((limit - len(OMITTED) - 1) // count - 1, MIN_LINE_CHARS)
        out = [_clip(line, per_line) for line in old]
        for turn in turns:
            parts = [
                text_of(m.get("content")) if m.get("role") != "tool" else f"[{m.get('name')}]"
                for m in turn
                if m.get("content") or m.get("role") == "tool"
            ]
            share = max(per_line // max(len(parts), 1) - 3, MIN_LINE_CHARS // 2)
            out.append(" / ".join(_clip(p, share) for p in parts))
        omitted = False
        while sum(len(line) + 1 for line in out) + len(OMITTED) + 1 > limit and len(out) > 1:
            out.pop(0)
            omitted = True
        return "\n".join([OMITTED, *out] if omitted else out)
