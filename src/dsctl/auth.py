from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.config import ClusterProfile


def build_auth_headers(profile: ClusterProfile) -> dict[str, str]:
    """Build the standard authenticated headers for DS REST requests."""
    return {
        "accept": "application/json",
        "token": profile.api_token,
    }
