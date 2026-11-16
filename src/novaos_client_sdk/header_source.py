"""Headers given to a client either as a fixed mapping or as a provider.

A fixed mapping is read once, when the client is built. A provider (a callable
returning a mapping, such as ``ctx.to_auth_headers`` without the call) is read
for every request, so a run that outlives its first execution token keeps
sending a valid one.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

HeaderSource = Mapping[str, str] | Callable[[], Mapping[str, str]]


def resolve_headers(source: HeaderSource | None) -> dict[str, str]:
    """A copy of the headers, calling the provider when one was given."""
    if source is None:
        return {}
    return dict(source() if callable(source) else source)
