"""Helpers that turn an ``httpx.HTTPStatusError`` into a useful error message.

The raw httpx message only carries the status line.  The service's response
body usually states the real cause, so a short, safe excerpt of it is appended.
Nothing here ever raises: a malformed, empty or non-JSON body simply yields no
excerpt.
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

MAX_EXCERPT_CHARS = 300
_MAX_RAW_BODY_CHARS = 4096
_MORE_INFO_RE = re.compile(r"\n?For more information check:.*", re.DOTALL)


def _truncate(text: str, limit: int = MAX_EXCERPT_CHARS) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 3].rstrip() + "..."


def _extract_detail(parsed: Any) -> str | None:
    if not isinstance(parsed, dict):
        return None
    errors = parsed.get("errors")
    if isinstance(errors, list):
        details = [
            str(item["detail"]) for item in errors if isinstance(item, dict) and item.get("detail")
        ]
        if details:
            return "; ".join(details)
    error = parsed.get("error")
    if isinstance(error, dict) and error.get("message"):
        return str(error["message"])
    if isinstance(error, str) and error:
        return error
    detail = parsed.get("detail")
    if isinstance(detail, str) and detail:
        return detail
    if detail:
        return json.dumps(detail, default=str)
    return None


def response_body(response: Any) -> Any:
    """Return the parsed JSON body, else a bounded raw-text body, else ``None``."""
    try:
        text = response.text
        if not isinstance(text, str) or not text.strip():
            return None
        try:
            return json.loads(text)
        except ValueError:
            return text[:_MAX_RAW_BODY_CHARS]
    except Exception:  # noqa: BLE001 - formatting must never raise
        return None


def body_excerpt(response: Any) -> str:
    """Short human-readable excerpt of the response body ('' when none)."""
    try:
        body = response_body(response)
        if body is None:
            return ""
        detail = _extract_detail(body) if not isinstance(body, str) else None
        if detail is None:
            raw = body if isinstance(body, str) else json.dumps(body, default=str)
            detail = raw
        return _truncate(detail)
    except Exception:  # noqa: BLE001
        return ""


def status_error_message(service: str, exc: httpx.HTTPStatusError, *, context: str = "") -> str:
    """Build ``"<service> returned HTTP <code>[ context]: <status line> - <detail>"``."""
    try:
        code = exc.response.status_code
        status_line = _MORE_INFO_RE.sub("", str(exc)).strip()
        message = f"{service} returned HTTP {code}{context}: {status_line}"
        excerpt = body_excerpt(exc.response)
        return f"{message} - {excerpt}" if excerpt else message
    except Exception:  # noqa: BLE001
        return f"{service} returned HTTP error{context}: {exc}"


def status_error_details(exc: httpx.HTTPStatusError) -> dict[str, Any]:
    """``details`` dict for the raised error, including ``response_body`` if any."""
    details: dict[str, Any] = {"original_error": type(exc).__name__}
    body = response_body(exc.response)
    if body is not None:
        details["response_body"] = body
    return details
