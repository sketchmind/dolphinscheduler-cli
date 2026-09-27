from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_dinky_address,
    validate_dinky_task_id,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)
from dsctl.upstream.task_parameter_projection.types import (
    ProjectedTask,
    ProjectionDirection,
    TaskParameterProjectionError,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject

_DINKY_OWNED_FIELDS = frozenset({"address", "taskId", "online"})


def _encode_dinky(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate canonical Dinky intent on its exact identity wire."""
    return _project_dinky(payload, version=version, direction="encode")


def _decode_dinky(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate native Dinky state and restore its explicit online default."""
    return _project_dinky(payload, version=version, direction="decode")


def _project_opaque_dinky(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> ProjectedTask:
    """Canonicalize safe Dinky evidence while preserving unsafe native state."""
    _require_dinky_available(version=version, direction=direction)
    try:
        projected = _project_dinky(
            payload,
            version=version,
            direction=direction,
        )
    except TaskParameterProjectionError:
        return ProjectedTask("DINKY", payload)
    return ProjectedTask("DINKY", projected)


def _project_dinky(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate the safe Dinky job-trigger subset on its identity wire."""
    # Native projection historically accepts surrogate-bearing URL paths that the
    # canonical model's length constraint rejects; retain the shared leaf rules.
    _require_dinky_available(version=version, direction=direction)
    unexpected = sorted(set(payload) - _DINKY_OWNED_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _dinky_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-job-trigger-subset",
            message=f"DINKY job-trigger projection does not own fields: {names}",
        )
    projected = deepcopy(payload)
    validators = {
        "address": validate_dinky_address,
        "taskId": validate_dinky_task_id,
    }
    for key, validator in validators.items():
        value = projected.get(key)
        if not isinstance(value, str):
            reason = (
                "missing-required-field" if key not in projected else "invalid-type"
            )
            raise _dinky_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason=reason,
                message=f"DINKY requires one literal string task_params.{key}",
            )
        try:
            validator(value)
        except ValueError as exc:
            raise _dinky_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason="unsafe-value",
                message=f"DINKY task_params.{key} is outside the safe literal subset",
            ) from exc
    online = projected.get("online", False)
    if not isinstance(online, bool):
        raise _dinky_projection_error(
            version=version,
            direction=direction,
            field="task_params.online",
            reason="invalid-strict-boolean",
            message="DINKY task_params.online must be one strict boolean",
        )
    projected["online"] = online
    return projected


def _require_dinky_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).dinky.available:
        return
    raise _dinky_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DINKY does not exist in DolphinScheduler {version}",
    )


def _dinky_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type="DINKY",
        field=field,
        reason=reason,
        message=message,
    )
