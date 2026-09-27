from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.errors import InvalidStateError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.errors import ApiResultError


STORAGE_NOT_STARTUP = 60002


def resource_storage_unavailable_error(
    error: ApiResultError,
    *,
    details: Mapping[str, str | int | None],
) -> InvalidStateError | None:
    """Explain an explicit native storage prerequisite without guessing a backend."""
    if error.result_code != STORAGE_NOT_STARTUP:
        return None
    return InvalidStateError(
        "DolphinScheduler resource storage is not enabled.",
        details=details,
        source=error.source,
        suggestion=(
            "Ask a DolphinScheduler administrator to check the server's effective "
            "resource storage configuration and enable the intended backend. "
            "Retry after the server prerequisite is resolved; changing the CLI "
            "resource path or version does not enable storage."
        ),
    )
