"""Configuration helpers for the NovaOS Client SDK."""

from typing import Literal

Environment = Literal["dev", "staging", "prod"]


def get_environment() -> Environment:
    """Get the current environment from environment variables.

    Checks `ENVIRONMENT` or `ENV` environment variables (case-insensitive).
    Defaults to "dev" if not set.

    Returns:
        Environment name: "dev", "staging", or "prod".

    Example:
        ```python
        from novaos_client_sdk.config import get_environment

        env = get_environment()  # "dev", "staging", or "prod"
        ```
    """
    import os

    env = os.getenv("ENVIRONMENT") or os.getenv("ENV")
    if env:
        env_lower = env.lower()
        if env_lower in ("dev", "staging", "prod"):
            return env_lower  # type: ignore[return-value]
        # If invalid value, default to dev
    return "dev"


def get_default_tool_timeout_s() -> int:
    """Get the default per-tool MCP call timeout in seconds.

    Checks the ``NOVAOS_TOOL_TIMEOUT_S`` environment variable; defaults to 128s
    (chosen to exceed the MCP Gateway's own upstream ceilings — 120s for
    built-in/service-registry-routed tools, 30s by default for T1
    manifest-registered tools unless the manifest overrides it — with an 8s
    margin, so a caller sees the Gateway's own timeout error rather than a
    premature SDK-side timeout). See ``docs/timeout-hierarchy.md`` for the
    full breakdown; there is no single platform-wide "the gateway timeout" to
    validate against, since it varies by which dispatch path a tool takes.

    Returns:
        Default timeout in seconds for MCP tool calls that don't specify
        their own ``timeout_s``.

    Raises:
        ValueError: If ``NOVAOS_TOOL_TIMEOUT_S`` is set but not a positive integer.

    Example:
        ```bash
        export NOVAOS_TOOL_TIMEOUT_S=180
        ```
    """
    import os

    raw = os.getenv("NOVAOS_TOOL_TIMEOUT_S")
    if raw is None or not raw.strip():
        return 128
    try:
        value = int(raw.strip())
    except ValueError as e:
        raise ValueError(
            f"NOVAOS_TOOL_TIMEOUT_S must be an integer number of seconds, got {raw!r}"
        ) from e
    if value <= 0:
        raise ValueError(f"NOVAOS_TOOL_TIMEOUT_S must be positive, got {value}")
    return value


def validate_tool_timeout(timeout_s: int, gateway_timeout_s: int | None) -> None:
    """Reject a per-tool timeout that is shorter than a caller-declared gateway ceiling.

    The SDK's timeout is meant to sit *above* the MCP Gateway's own ceiling —
    see ``docs/timeout-hierarchy.md`` — so the Gateway times out first and the
    caller sees its (often more informative) error instead of a generic
    client-side timeout while the tool keeps running server-side. A
    ``timeout_s`` shorter than ``gateway_timeout_s`` reintroduces exactly that
    bug. There is no single platform-wide Gateway timeout the SDK can assume —
    it ranges from 30s to 120s depending on which dispatch path a given tool
    takes, and manifest-registered tools can override it per-tool — so
    validation is opt-in: pass ``gateway_timeout_s`` (the actual ceiling for
    the tools you call — when in doubt, the smallest one, 30s, is always
    safe) to enforce it; leave it ``None`` (the default) to skip validation
    entirely.

    Args:
        timeout_s: The effective per-tool timeout about to be used.
        gateway_timeout_s: The caller-declared Gateway ceiling to validate
            against, or ``None`` to skip validation.

    Raises:
        ValueError: If ``gateway_timeout_s`` is set and ``timeout_s`` is
            shorter than it.
    """
    if gateway_timeout_s is not None and timeout_s < gateway_timeout_s:
        raise ValueError(
            f"Per-tool timeout {timeout_s}s is shorter than the configured "
            f"gateway_timeout_s ({gateway_timeout_s}s) — the SDK would time out "
            "before the MCP Gateway does, producing a generic client-side "
            "timeout instead of the Gateway's own error while the tool keeps "
            f"running server-side. Raise timeout_s to at least {gateway_timeout_s}s "
            "(plus margin), lower gateway_timeout_s to match your deployment, "
            "or leave gateway_timeout_s unset to skip this check."
        )


def get_dns_url_for_environment(environment: Environment | None = None) -> str:
    """Raise, naming the environment variables that configure a DNS URL.

    There is no hardcoded default DNS host for any environment — every
    deployment must set one of the environment variables documented on
    :func:`get_dns_url_from_env`. This function stays exported for callers
    that want an environment-scoped lookup, but it always raises: a coarse
    per-environment fallback host would otherwise mask a missing platform
    configuration with a URL that resolves nowhere.

    Args:
        environment: Environment name ("dev", "staging", or "prod").
            If None, uses `get_environment()` to detect the current environment.

    Raises:
        ValueError: Always — no default exists. The message names
            `AGENT_DNS_URL`, `ACP_DNS_URL`, `DNS_SERVICE_HOST`
            (+ `DNS_SERVICE_PORT`), and the environment-specific
            `ACP_DNS_URL_<ENV>` variable as the ways to configure a DNS URL.

    Example:
        ```python
        from novaos_client_sdk.config import get_dns_url_for_environment

        # Raises ValueError unless a DNS variable is configured
        url = get_dns_url_for_environment("staging")
        ```
    """
    if environment is None:
        environment = get_environment()

    raise ValueError(
        f"No agent DNS URL is configured for environment '{environment}'. "
        "Set AGENT_DNS_URL (platform convention), ACP_DNS_URL, "
        "DNS_SERVICE_HOST[+DNS_SERVICE_PORT] or "
        f"ACP_DNS_URL_{environment.upper()}."
    )


def get_allowed_hosts_from_env() -> list[str] | None:
    """Read the outbound-host allowlist from the ``NOVAOS_ALLOWED_HOSTS`` env var.

    Returns:
        A list of hostname suffixes (e.g. ``["novaos.example.com", "localhost"]``),
        or ``None`` if the env var is unset or empty — meaning no restriction.

    Example:
        ```bash
        export NOVAOS_ALLOWED_HOSTS="novaos.example.com,localhost"
        ```
    """
    import os

    raw = os.getenv("NOVAOS_ALLOWED_HOSTS")
    if not raw or not raw.strip():
        return None
    hosts = [h.strip() for h in raw.split(",") if h.strip()]
    return hosts or None


def validate_host_allowed(url: str, allowed_hosts: list[str] | None = None) -> None:
    """Reject ``url`` if its hostname isn't covered by an outbound-host allowlist.

    A caller-supplied SDK config value (``gateway_url``, ``base_url``, etc.) is
    trusted configuration, not attacker-controlled data — this is a coarse
    suffix check, not the DNS-resolution + private-IP-blocking check
    :func:`novaos_client_sdk.dns.validate_agent_url` does for dynamically
    *discovered* agent URLs (a different, higher-risk trust boundary).

    No-ops when no allowlist is configured (neither ``allowed_hosts`` nor the
    ``NOVAOS_ALLOWED_HOSTS`` env var) — this is an opt-in restriction, not a
    default-deny, so existing deployments are unaffected until they opt in.

    Args:
        url: The URL the SDK is about to call.
        allowed_hosts: Explicit hostname-suffix allowlist. When ``None``
            (default), falls back to :func:`get_allowed_hosts_from_env`.

    Raises:
        ValueError: If an allowlist is configured and ``url``'s hostname
            doesn't end with any entry in it.
    """
    from urllib.parse import urlparse

    if allowed_hosts is None:
        allowed_hosts = get_allowed_hosts_from_env()
    if not allowed_hosts:
        return

    hostname = urlparse(url).hostname
    if not hostname or not any(
        hostname == suffix or hostname.endswith(f".{suffix}") for suffix in allowed_hosts
    ):
        raise ValueError(
            f"Host '{hostname}' for URL '{url}' is not in the configured allowlist "
            f"{allowed_hosts} (NOVAOS_ALLOWED_HOSTS)"
        )


def _validate_url(url: str, source: str) -> str:
    """Validate a URL and return it if valid.

    Args:
        url: URL string to validate.
        source: Description of the source (for error messages).

    Returns:
        The validated URL string.

    Raises:
        ValueError: If the URL is empty or has an invalid format.
    """
    from urllib.parse import urlparse

    if not url.strip():
        raise ValueError(f"{source} is set but empty")
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid URL format in {source}: {url}")
    return url


def get_dns_url_from_env(
    custom_url: str | None = None,
    env_var: str | None = None,
    environment: Environment | None = None,
) -> str:
    """Get DNS URL from environment variables or defaults.

    Priority order:
    1. Explicit `custom_url` parameter (highest priority)
    2. Custom `env_var` parameter
    3. `AGENT_DNS_URL` — matches NovaOS control-plane / Temporal worker convention
    4. `ACP_DNS_URL` — backward-compatible generic variable
    5. `DNS_SERVICE_HOST` + `DNS_SERVICE_PORT` — matches agent-registrar convention
    6. Environment-specific variable (e.g., `ACP_DNS_URL_DEV`)
    7. Raises if none of the above is set — there is no environment default

    Args:
        custom_url: Explicit URL to use (highest priority). If provided, all other
            options are ignored.
        env_var: Custom environment variable name to check (default: None, uses standard names).
            If provided, checks this variable before platform-standard variables.
        environment: Environment name to use for environment-specific variables and defaults.
            If None, uses `get_environment()` to detect the current environment.

    Returns:
        DNS URL string.

    Raises:
        ValueError: If a URL is provided but is invalid (empty or not a valid URL format),
            or if none of the DNS variables above are set — there is no hardcoded
            fallback host for any environment.

    Environment Variables (checked in order):
        - `AGENT_DNS_URL`: Full URL used by NovaOS control-plane components
        - `ACP_DNS_URL`: Generic DNS URL (backward-compatible)
        - `DNS_SERVICE_HOST`: Agent DNS hostname (used with `DNS_SERVICE_PORT`)
        - `DNS_SERVICE_PORT`: Agent DNS port (default: 8000, used with `DNS_SERVICE_HOST`)
        - `ACP_DNS_URL_DEV` / `ACP_DNS_URL_STAGING` / `ACP_DNS_URL_PROD`: Per-environment URLs

    Example:
        ```python
        import os
        from novaos_client_sdk.config import get_dns_url_from_env

        # Platform convention (recommended)
        os.environ["AGENT_DNS_URL"] = "http://agent-dns.example.internal:8000"
        url = get_dns_url_from_env()

        # Agent-registrar convention (host + port)
        os.environ["DNS_SERVICE_HOST"] = "agent-dns.example.internal"
        os.environ["DNS_SERVICE_PORT"] = "8000"
        url = get_dns_url_from_env()

        # Backward-compatible
        os.environ["ACP_DNS_URL"] = "http://localhost:8021"
        url = get_dns_url_from_env()

        # Explicit override (highest priority)
        url = get_dns_url_from_env(custom_url="http://custom:8000")
        ```
    """
    import os

    # Priority 1: Explicit custom_url parameter
    if custom_url is not None:
        return _validate_url(custom_url, "custom_url")

    # Detect environment if not provided
    if environment is None:
        environment = get_environment()

    # Priority 2: Custom env_var parameter
    if env_var:
        url = os.getenv(env_var)
        if url is not None:
            return _validate_url(url, f"Environment variable '{env_var}'")

    # Priority 3: AGENT_DNS_URL (NovaOS platform convention)
    url = os.getenv("AGENT_DNS_URL")
    if url is not None:
        return _validate_url(url, "Environment variable 'AGENT_DNS_URL'")

    # Priority 4: ACP_DNS_URL (backward-compatible)
    url = os.getenv("ACP_DNS_URL")
    if url is not None:
        return _validate_url(url, "Environment variable 'ACP_DNS_URL'")

    # Priority 5: DNS_SERVICE_HOST + DNS_SERVICE_PORT (agent-registrar convention)
    dns_host = os.getenv("DNS_SERVICE_HOST")
    if dns_host is not None:
        if not dns_host.strip():
            raise ValueError("Environment variable 'DNS_SERVICE_HOST' is set but empty")
        dns_port = os.getenv("DNS_SERVICE_PORT", "8000")
        constructed_url = f"http://{dns_host.strip()}:{dns_port.strip()}"
        return _validate_url(constructed_url, "DNS_SERVICE_HOST + DNS_SERVICE_PORT")

    # Priority 6: Environment-specific variable (e.g., ACP_DNS_URL_DEV)
    env_specific_var = f"ACP_DNS_URL_{environment.upper()}"
    url = os.getenv(env_specific_var)
    if url is not None:
        return _validate_url(url, f"Environment variable '{env_specific_var}'")

    # Priority 7: Environment default
    return get_dns_url_for_environment(environment)
