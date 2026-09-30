"""``python -m novaos_client_sdk.agent_card`` — emit a live agent's Agent Card as JSON.

This module is the emitter half of the platform's agent-card contract.
:meth:`~novaos_client_sdk.agent_builder.AgentBuilder.to_agent_card` builds a
card whose ``skills[]`` comes from the exact registry
:func:`~novaos_client_sdk.agent_builder._resolve_skill` routes against — but
that method is a Python call, and its two intended consumers are
TypeScript: ``novaos-cli publish`` (the platform's agent CLI) and
the activation pipeline's ``resolveAgentCard``
(the platform's agent-card resolver). Neither can call a Python method
directly. This module is the process boundary between them: it imports a
``server.py``-shaped module, finds the :class:`AgentBuilder` instance inside
it, and prints the card it builds.

Usage::

    python -m novaos_client_sdk.agent_card server:agent            # full card
    python -m novaos_client_sdk.agent_card server:agent --skills   # skills[] only

``server:agent`` resolves the same way ``uvicorn``/``gunicorn`` resolve their
``module:attr`` target: :func:`importlib.import_module` the module, then
:func:`getattr` the attribute off it. The current directory is added to
``sys.path`` first, so this works unmodified from inside a built agent image
(``/app`` on the ``PYTHONPATH``, ``server.py`` at its root) the same way it
would from a shell in that same directory.

**Output contract.** On success this prints exactly one line of JSON to
stdout — the proto3-JSON wire shape :meth:`AgentCard.to_dict()
<novaos_client_sdk.a2a_agent_card.AgentCard.to_dict>` produces (or, with
``--skills``, just its ``skills[]`` array) — and exits ``0``. A TypeScript
consumer validates that shape with the existing ``AgentCardSchema``. On any
failure (the module doesn't import, the attribute doesn't exist, the
attribute isn't an :class:`AgentBuilder`) this prints one line of *reason* to
stderr, nothing to stdout, and exits non-zero — so a caller can always trust
that a non-empty stdout is JSON and never has to guess which stream carries
the error.

**What this can't know, and who fills it in.** ``to_agent_card()`` builds a
card from what a running agent process actually holds — its registered
skills and the capabilities it truthfully supports (``streaming``,
``push_notifications``) — and nothing else. Fields describing where the card
is published rather than what the agent does — ``supportedInterfaces``,
``provider``, ``securitySchemes``, ``documentationUrl``/``iconUrl``, the
card-level default input/output modes — have no runtime source and stay at
their defaults here. This is the contract the publish pipeline builds its merge on:
the publish-time manifest is authoritative for those, and this command's
output is authoritative for ``skills[]`` and ``capabilities`` — the two
fields only a live :class:`AgentBuilder` actually knows.

**Importing the target must not start the server.** A ``server.py`` written
against the Galaxy scaffold template guards its ``uvicorn.run()`` call behind
``if __name__ == "__main__":``, so importing it as a module is inert — but an
agent module that binds a port at import time would hang this command
forever. Import runs on a background thread with a bounded timeout
(:data:`IMPORT_TIMEOUT_SECONDS`) precisely so that failure mode is a clear,
timely error instead of a silent hang.

Not to be confused with
:mod:`novaos_client_sdk.a2a_agent_card`, which defines the ``AgentCard``
*type* this module serializes — that module is the data shape; this one is
the process that produces an instance of it from a live agent definition.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import threading
from typing import Any, Final

from .agent_builder import AgentBuilder

#: How long import of the target module may run before this command gives up
#: and reports a likely import-time server start instead of hanging forever.
#: Generous on purpose: a real agent module's own imports (ML/tooling
#: dependencies) can be slow, and every finite bound still catches the actual
#: failure mode this guards — ``uvicorn.run()`` blocks forever, not for 30s.
IMPORT_TIMEOUT_SECONDS: Final[float] = 30.0


class _EntrypointError(Exception):
    """A user-facing reason the target ``module:attr`` could not be resolved."""


def _import_with_timeout(module_name: str) -> Any:
    """Import ``module_name``, failing clearly instead of hanging.

    Runs the import on a daemon thread so a module that starts a server at
    import time (e.g. an un-guarded ``uvicorn.run()``) times out here rather
    than blocking this process forever. The thread is left running if it
    never finishes — this process exits non-zero right after, so there is
    nothing to clean up.
    """
    outcome: dict[str, Any] = {}

    def _do_import() -> None:
        try:
            outcome["module"] = importlib.import_module(module_name)
        except BaseException as exc:  # noqa: BLE001 - reraised on the main thread below
            outcome["error"] = exc

    thread = threading.Thread(target=_do_import, daemon=True)
    thread.start()
    thread.join(IMPORT_TIMEOUT_SECONDS)

    if thread.is_alive():
        raise _EntrypointError(
            f"Importing {module_name!r} did not finish within "
            f"{IMPORT_TIMEOUT_SECONDS:g}s. This usually means the module starts a "
            "server at import time (e.g. calls uvicorn.run() outside "
            "`if __name__ == '__main__':`) instead of only defining objects — "
            "guard that call so importing the module is safe."
        )
    if "error" in outcome:
        raise _EntrypointError(
            f"Could not import module {module_name!r}: {outcome['error']}"
        ) from outcome["error"]
    return outcome["module"]


def _resolve_target(spec: str) -> AgentBuilder:
    """Resolve ``MODULE:ATTR`` to the :class:`AgentBuilder` it names.

    Raises :class:`_EntrypointError` — never anything else — for every way
    this can fail: a malformed spec, a module that won't import, a missing
    attribute, or an attribute that isn't an :class:`AgentBuilder`.
    """
    module_name, sep, attr_name = spec.partition(":")
    if not sep or not module_name or not attr_name:
        raise _EntrypointError(f"Expected MODULE:ATTR (e.g. 'server:agent'), got {spec!r}.")

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    module = _import_with_timeout(module_name)

    try:
        target = getattr(module, attr_name)
    except AttributeError:
        raise _EntrypointError(f"Module {module_name!r} has no attribute {attr_name!r}.") from None

    if not isinstance(target, AgentBuilder):
        raise _EntrypointError(f"{spec!r} is a {type(target).__qualname__}, not an AgentBuilder.")
    return target


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m novaos_client_sdk.agent_card",
        description="Emit an AgentBuilder's Agent Card as JSON.",
    )
    parser.add_argument("target", metavar="MODULE:ATTR", help="e.g. 'server:agent'")
    parser.add_argument(
        "--skills",
        action="store_true",
        help="Print only the card's skills[] array instead of the full card.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        agent = _resolve_target(args.target)
    except _EntrypointError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    card = agent.to_agent_card().to_dict()
    print(json.dumps(card["skills"] if args.skills else card))
    return 0


if __name__ == "__main__":
    sys.exit(main())
