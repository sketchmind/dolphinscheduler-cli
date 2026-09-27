from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HttpAuthoringSurface:
    """HTTP fields and template variants safe to expose for typed authoring."""

    request_body: bool
    native_socket_timeout: int | None


_HTTP_WITHOUT_BODY = HttpAuthoringSurface(
    request_body=False,
    native_socket_timeout=60000,
)
_HTTP_LEGACY_WITH_BODY = HttpAuthoringSurface(
    request_body=True,
    native_socket_timeout=60000,
)
_HTTP_MODERN = HttpAuthoringSurface(
    request_body=True,
    native_socket_timeout=None,
)
