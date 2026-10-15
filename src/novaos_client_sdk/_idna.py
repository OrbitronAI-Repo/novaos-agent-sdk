"""Hostname normalisation shared by URL validation and the transports."""

from __future__ import annotations

import ipaddress

import idna

from novaos_client_sdk.exceptions import NovaOSError


def wire_hostname(hostname: str) -> str:
    """Return the hostname in the exact ASCII form the HTTP transport connects to.

    ``socket.getaddrinfo`` encodes a ``str`` host with Python's IDNA 2003
    codec, whereas httpx encodes it with the ``idna`` package (IDNA 2008 plus
    UTS 46 mapping). The two disagree for some labels — ``faß.example`` becomes
    ``fass.example`` in the former and ``xn--fa-hia.example`` in the latter —
    so validating the Unicode form would check one destination and dial
    another. Normalising up front with the transport's own encoder makes the
    suffix check and the resolution below act on the host that is actually
    reached.
    """
    try:
        ipaddress.ip_address(hostname)
        return hostname
    except ValueError:
        pass
    try:
        return idna.encode(hostname, uts46=True).decode("ascii")
    except (idna.IDNAError, UnicodeError) as exc:
        raise NovaOSError(f"Invalid hostname {hostname!r}: {exc}") from exc


def wire_suffix(suffix: str) -> str:
    """Normalise an allowlist suffix like :func:`wire_hostname`, keeping a leading dot."""
    bare = suffix.lstrip(".")
    return suffix[: len(suffix) - len(bare)] + wire_hostname(bare) if bare else suffix
