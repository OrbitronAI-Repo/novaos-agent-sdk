"""
PII Masking for NovaOS Agent SDK.

Detects and masks common PII patterns in strings before they are logged.

Patterns detected:
  - Email addresses
  - Phone numbers (flexible grouping/length — see note below)
  - SSN (XXX-XX-XXXX, with optional space/dot/dash separators)
  - Credit card numbers (13-19 digits, flexible grouping — covers Visa/MC 16,
    Amex 15, Diners 14, and other network lengths)
  - IP addresses (IPv4)

⚠️ Pattern-based detection is inherently best-effort, especially for phone
numbers: valid formats vary enormously by locale (grouping, separators,
country-code presence) and no fixed set of regexes can be complete. The
patterns below are deliberately broad — favoring masking something that
wasn't actually PII over missing something that was, since under-masking is
the more dangerous failure mode for a safety utility. For any field whose
*name* is known to carry sensitive data regardless of its format (e.g. a
field literally called "phone" or "ssn"), prefer key-aware masking — see
``mask_pii_in_dict``'s ``_SENSITIVE_KEY_SUBSTRINGS`` — over relying on the
value happening to match one of these patterns.

Ported from NovaOs/packages/agent-sdk/python/novaos_sdk/pii.py
"""

import re
from typing import Any

_PII_PATTERNS = [
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"), "***EMAIL***"),
    # SSN: XXX-XX-XXXX with '-', '.', or space as the separator (or none).
    (re.compile(r"\b\d{3}[-.\s]?\d{2}[-.\s]?\d{4}\b"), "***SSN***"),
    # IP must run before CARD/PHONE below: a dotted-quad like "192.168.1.100"
    # has 12 digits and dot separators, which would otherwise also satisfy
    # PHONE's much broader pattern. Matching the precise, well-defined IP
    # format first and consuming it means the greedier patterns never see it.
    (re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"), "***IP***"),
    # Card numbers: 13-19 total digits (covers Amex 15, Diners 14, Visa/MC 16,
    # and other network lengths) with an optional space/dash before each digit
    # after the first, so any grouping (4-4-4-4, 4-6-5, ungrouped, ...) matches.
    (re.compile(r"\b\d(?:[\s-]?\d){12,18}\b"), "***CARD***"),
    # Phone: an optional leading '+', then 7-15 total digits with an optional
    # space/dot/dash/parenthesis before each digit after the first. Deliberately
    # broad rather than assuming one national grouping convention — see module
    # docstring on why format-based phone detection can't be exhaustive. Must
    # run last: it's the greediest pattern and would otherwise swallow IPs/cards.
    (re.compile(r"\b\+?\(?\d\)?(?:[\s.\-()]?\d){6,14}\b"), "***PHONE***"),
]

# Key-name substrings (case-insensitive) that mark a dict value as sensitive
# regardless of whether its content matches any pattern above. This is the
# defense-in-depth backstop for exactly the failure mode pattern-matching
# can't fully close: a value in a format the regexes above don't anticipate,
# sitting in a field whose *name* nonetheless tells you it's sensitive.
_SENSITIVE_KEY_SUBSTRINGS = frozenset(
    {
        "ssn",
        "social_security",
        "phone",
        "mobile",
        "telephone",
        "credit_card",
        "card_number",
        "cvv",
        "cvc",
        "dob",
        "date_of_birth",
        # Added for `TaskPushNotificationConfig.token` and
        # `AuthenticationInfo.credentials` — a webhook receiver's callback
        # secret, held in memory only, that must never reach a log line. Also
        # catches any other bearer token or cursor named "token" (e.g. a
        # `page_token`) or "credential(s)", which is the safe direction to
        # over-mask per the module docstring above.
        "token",
        "credential",
    }
)

_REDACTED = "***REDACTED***"


def _is_sensitive_key(key: str) -> bool:
    key_lower = key.lower()
    return any(substr in key_lower for substr in _SENSITIVE_KEY_SUBSTRINGS)


def _redact_all_strings(value: Any) -> Any:
    """Recursively replace every string leaf with a fixed redaction marker.

    Used for values under a sensitive key name — unlike ``mask_pii``, this
    does not depend on the string matching any known format, since the whole
    point is to protect values whose format we can't anticipate.
    """
    if isinstance(value, str):
        return _REDACTED
    if isinstance(value, dict):
        return {k: _redact_all_strings(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_all_strings(v) for v in value]
    return value


def mask_pii(text: str | None) -> str | None:
    """Mask PII patterns in the given text."""
    if not text:
        return text
    result = text
    for pattern, replacement in _PII_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def mask_pii_in_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Recursively mask PII in all string values of a dictionary.

    Values under a key whose name matches a known-sensitive substring (see
    ``_SENSITIVE_KEY_SUBSTRINGS``) are fully redacted regardless of their
    content — this is the key-aware backstop for values whose format doesn't
    match any pattern in ``mask_pii``. Every other value is masked by pattern
    as before.
    """
    result = {}
    for key, value in data.items():
        if _is_sensitive_key(key):
            result[key] = _redact_all_strings(value)
        elif isinstance(value, str):
            result[key] = mask_pii(value)
        elif isinstance(value, dict):
            result[key] = mask_pii_in_dict(value)
        elif isinstance(value, list):
            result[key] = [
                (
                    mask_pii(item)
                    if isinstance(item, str)
                    else mask_pii_in_dict(item) if isinstance(item, dict) else item
                )
                for item in value
            ]
        else:
            result[key] = value
    return result
