from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.cli_surface import PROJECT_RESOURCE
from dsctl.errors import ApiResultError, ApiTransportError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import TaskOperations


_PREVIEW_TASK_CODE_BASE = 8_500_000_000_000_000_000


def preview_task_codes(count: int) -> Sequence[int]:
    """Return deterministic, non-persistent task codes for local previews."""
    return range(_PREVIEW_TASK_CODE_BASE, _PREVIEW_TASK_CODE_BASE + count)


def allocate_server_task_codes(
    count: int,
    *,
    adapter: TaskOperations,
    project_code: int,
    action: str,
) -> Sequence[int]:
    """Allocate persistent task codes and attach mutation context to failures."""
    details = {
        "resource": PROJECT_RESOURCE,
        "project_code": project_code,
        "action": action,
        "task_code_count": count,
    }
    try:
        return adapter.generate_codes(project_code=project_code, count=count)
    except ApiResultError as error:
        message = "DolphinScheduler could not allocate task codes."
        raise ApiTransportError(
            message,
            details={
                **details,
                "result_code": error.result_code,
                "result_message": error.result_message,
            },
            source=error.source,
            suggestion="Retry the workflow mutation after checking server health.",
        ) from error
    except ApiTransportError as error:
        raise ApiTransportError(
            error.message,
            details={**error.details, **details},
            source=error.source,
            suggestion=(
                error.suggestion
                or "Verify DolphinScheduler API health and version, then retry."
            ),
        ) from error
