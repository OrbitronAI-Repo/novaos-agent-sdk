"""The stall guard. The write key lives in ``novaos_client_sdk.write_keys`` and is re-exported."""

from __future__ import annotations

import json
from typing import Any

from novaos_client_sdk.write_keys import idempotency_key

from .ports import ToolSpec

__all__ = ["StallGuard", "idempotency_key"]


class StallGuard:
    """Skips a tool call that already ran in this run with the same arguments.

    Only a call that succeeded is recorded, so a failed call can be retried. Each skip is
    counted; ``max_skips`` skips mean the loop is going round in circles.
    """

    def __init__(self, max_skips: int = 2) -> None:
        self.max_skips = max_skips

    @staticmethod
    def signature(tool: ToolSpec, args: dict[str, Any]) -> str:
        """The tool and its arguments in a stable form, so equal calls compare equal."""
        return f"{tool.name}|{json.dumps(args, sort_keys=True, default=str)}"

    def should_skip(self, tool: ToolSpec, args: dict[str, Any], log: list[str]) -> bool:
        """True when this call already ran with these arguments (see :meth:`record`)."""
        return not tool.repeatable and self.signature(tool, args) in log

    def record(self, tool: ToolSpec, args: dict[str, Any], log: list[str]) -> None:
        """Record a call that ran, so an identical later call is skipped."""
        if not tool.repeatable:
            log.append(self.signature(tool, args))

    def stalled(self, skips: int) -> bool:
        return skips >= self.max_skips
