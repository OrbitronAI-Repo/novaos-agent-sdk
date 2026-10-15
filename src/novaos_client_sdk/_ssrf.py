"""Connect-time SSRF guard: check the address that is actually dialled.

:func:`~novaos_client_sdk.dns.validate_agent_url` resolves a host once, before
the client exists. The HTTP and gRPC stacks then resolve it again, so a
short-TTL answer can be public for the check and private for the connection.
The guard here closes that gap by resolving once more, at connect time,
refusing a blocked address, and connecting to the very address it checked.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Iterable
from typing import Any

import httpcore
import httpx

from novaos_client_sdk.exceptions import NovaOSError


def is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Whether ``ip`` is private, loopback, link-local or reserved."""
    return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved


def pick_checked_address(
    addr_infos: Iterable[tuple[Any, ...]], host: str, *, allow_private_ips: bool
) -> str:
    """Return the first resolved address, raising if *any* of them is blocked.

    Matches :func:`~novaos_client_sdk.dns.validate_agent_url`: a name that
    resolves to a blocked address anywhere in its answer is refused outright,
    not merely skipped.
    """
    chosen: str | None = None
    for info in addr_infos:
        ip_str = info[4][0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if not allow_private_ips and is_blocked_ip(ip):
            raise NovaOSError(f"Resolved IP {ip_str} for host '{host}' is in a blocked range")
        if chosen is None:
            chosen = ip_str
    if chosen is None:
        raise NovaOSError(f"Cannot resolve host '{host}' — blocking to prevent SSRF bypass")
    return chosen


async def resolve_checked(host: str, port: int, *, allow_private_ips: bool) -> str:
    """Resolve ``host`` now and return an address that passed the range check."""
    try:
        ipaddress.ip_address(host)
        literal = True
    except ValueError:
        literal = False
    if literal:
        infos: list[tuple[Any, ...]] = [(None, None, None, "", (host, port))]
    else:
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(
                host, port, type=socket.SOCK_STREAM
            )
        except socket.gaierror as exc:
            raise NovaOSError(
                f"Cannot resolve host '{host}' — blocking to prevent SSRF bypass"
            ) from exc
    return pick_checked_address(infos, host, allow_private_ips=allow_private_ips)


class _CheckedBackend(httpcore.AsyncNetworkBackend):
    """Network backend that dials only an address it has just range-checked.

    The origin's hostname still drives TLS server-name verification and the
    ``Host`` header — httpcore passes it separately from the TCP address — so
    only the dialled address is pinned, not the identity being verified.
    """

    def __init__(self, inner: httpcore.AsyncNetworkBackend, *, allow_private_ips: bool) -> None:
        self._inner = inner
        self._allow_private_ips = allow_private_ips

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        address = await resolve_checked(host, port, allow_private_ips=self._allow_private_ips)
        return await self._inner.connect_tcp(
            address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(self, *args: Any, **kwargs: Any) -> httpcore.AsyncNetworkStream:
        raise NovaOSError("Unix sockets are not allowed on an address-guarded connection")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


def checked_async_transport(*, allow_private_ips: bool = False) -> httpx.AsyncHTTPTransport:
    """An ``httpx`` transport that connects only to range-checked addresses.

    httpx exposes no public hook for the TCP backend, so this swaps the one on
    the transport's own connection pool. If a future httpx moves it, this
    raises instead of silently returning an unguarded transport.
    """
    transport = httpx.AsyncHTTPTransport()
    pool = getattr(transport, "_pool", None)
    inner = getattr(pool, "_network_backend", None)
    if inner is None:
        raise NovaOSError(
            "The installed httpx no longer exposes its network backend; "
            "cannot enforce the connect-time address guard"
        )
    pool._network_backend = _CheckedBackend(inner, allow_private_ips=allow_private_ips)
    return transport
