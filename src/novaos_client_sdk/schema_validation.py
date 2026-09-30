"""Structured-extraction validation against a caller-supplied JSON Schema (Ex10).

Pairs with :attr:`novaos_client_sdk.agent_context.AgentExecutionContext.output_schema`:
a skill that performs structured extraction (e.g. via an LLM call or BAML)
can check its own result against the caller's schema before returning,
instead of trusting the extraction blindly.

This module does not perform extraction itself — that remains the skill's
own business logic (whatever LLM call or parsing approach it already uses).
It only validates structural correctness of an already-produced result.

Requires the optional ``jsonschema>=4.18.0`` dependency (``pip install
novaos-agent-sdk[schema]``) — 4.18.0 specifically, not just any
``jsonschema``, because this module relies on the ``referencing``-based
``Registry``/``registry=`` API to block remote ``$ref`` resolution (see
below); that API doesn't exist in jsonschema 4.0–4.17. The import is
deferred to :func:`validate_against_schema` itself so importing this
module — or the rest of the SDK — never requires ``jsonschema`` to be
installed; only calling the function does.

.. admonition:: The schema itself is untrusted input — never resolved remotely

   ``ctx.output_schema`` comes straight from the caller's request body
   (Ex10). Older ``jsonschema`` releases (pre ``referencing`` migration)
   fetch remote ``$ref`` targets over the network by default — a caller
   could embed a ``$ref`` pointing at an internal service or a cloud
   metadata endpoint (e.g. ``http://169.254.169.254/...``) and turn
   validation into an SSRF primitive. This module always validates against
   an explicit registry that refuses to resolve *any* external reference
   (network or filesystem) — a ``$ref`` that would require fetching fails
   validation with a clear error instead of ever being retrieved.
"""

from __future__ import annotations

from typing import Any

_INSTALL_HINT = (
    "validate_against_schema() requires the optional 'schema' extra "
    "(jsonschema>=4.18.0): pip install novaos-agent-sdk[schema]"
)


def _deny_external_refs(uri: str) -> Any:
    """Registry `retrieve` callback that refuses every external `$ref`.

    Passed explicitly to the validator (rather than relying on whichever
    default a given jsonschema/referencing version ships) so remote-ref
    resolution is blocked deterministically — see the module docstring.
    """
    from referencing.exceptions import NoSuchResource

    # mypy misresolves NoSuchResource's attrs-generated __init__ (verified at
    # runtime: NoSuchResource(ref=uri) is the documented, correct referencing API).
    raise NoSuchResource(ref=uri)  # type: ignore[call-arg]


def validate_against_schema(data: Any, schema: dict[str, Any]) -> list[str]:
    """Validate ``data`` against a caller-supplied JSON Schema.

    Args:
        data: The value to validate — typically a skill's extracted output.
        schema: A JSON Schema document, e.g. from
            :attr:`~novaos_client_sdk.agent_context.AgentExecutionContext.output_schema`.

    Returns:
        A list of human-readable validation error messages, in the order
        ``jsonschema`` discovers them. Empty list means ``data`` is valid.
        A malformed ``schema`` (e.g. an invalid regex in a ``pattern``
        keyword, or a ``$ref`` this function refuses to resolve) also
        produces an error message here rather than raising — the schema is
        caller-supplied, untrusted input, so a skill following the
        documented ``if errors:`` pattern must never crash on it.

    Raises:
        ImportError: If the optional ``jsonschema>=4.18.0`` dependency (or
            its ``referencing`` dependency) isn't installed — including an
            allowed-but-too-old ``jsonschema`` 4.0–4.17 install that lacks
            the ``referencing``-based ``Registry`` API this function needs.

    Example:
        ```python
        if ctx.output_schema:
            extracted = await extract_with_llm(ctx, ctx.output_schema)
            errors = validate_against_schema(extracted, ctx.output_schema)
            if errors:
                return SkillResult(output=None, error="; ".join(errors))
        ```
    """
    try:
        import jsonschema
        from referencing import Registry
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc

    # Same mypy/attrs stub-resolution quirk as _deny_external_refs above.
    locked_registry = Registry(retrieve=_deny_external_refs)  # type: ignore[call-arg]

    try:
        jsonschema.Draft202012Validator.check_schema(schema)
        validator = jsonschema.Draft202012Validator(schema, registry=locked_registry)
        return [error.message for error in validator.iter_errors(data)]
    except jsonschema.exceptions.SchemaError as exc:
        return [f"Invalid JSON Schema: {exc.message}"]
    except (
        Exception
    ) as exc:  # noqa: BLE001 - schema is untrusted input; must never crash the caller
        return [f"Schema validation error: {exc}"]
