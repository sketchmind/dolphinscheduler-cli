from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
    _reject_field,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
    )


def _encode_emr(payload: JsonObject, *, version: str) -> JsonObject:
    """Remove the discriminator implied by the legacy EMR task class."""
    encoded = deepcopy(payload)
    surface = get_task_authoring_surface(version).emr
    _require_emr_available(version=version, direction="encode")
    _validate_emr_program_mode(
        encoded,
        version=version,
        direction="encode",
    )
    if not surface.native_program_type:
        del encoded["programType"]
    return encoded


def _decode_emr(payload: JsonObject, *, version: str) -> JsonObject:
    """Restore the canonical discriminator implied by the legacy EMR wire."""
    decoded = deepcopy(payload)
    surface = get_task_authoring_surface(version).emr
    _require_emr_available(version=version, direction="decode")
    if not surface.native_program_type:
        _reject_field(
            decoded,
            "programType",
            version=version,
            direction="decode",
            task_type="EMR",
            field="task_params.programType",
            reason="field-absent-in-version",
        )
        _reject_field(
            decoded,
            "stepsDefineJson",
            version=version,
            direction="decode",
            task_type="EMR",
            field="task_params.stepsDefineJson",
            reason="field-absent-in-version",
        )
        decoded["programType"] = "RUN_JOB_FLOW"
    _validate_emr_program_mode(
        decoded,
        version=version,
        direction="decode",
    )
    return decoded


def _require_emr_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).emr.available:
        return
    message = f"EMR does not exist in DolphinScheduler {version}"
    raise _projection_error(
        version=version,
        direction=direction,
        task_type="EMR",
        field="task.type",
        reason="task-type-absent-in-version",
        message=message,
    )


def _validate_emr_program_mode(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    surface = get_task_authoring_surface(version).emr
    program_type = payload.get("programType")
    if program_type not in surface.program_types:
        message = f"EMR {program_type!r} is not available in DolphinScheduler {version}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="EMR",
            field="task_params.programType",
            reason="unsupported-program-type",
            message=message,
        )
    active_field, inactive_field = _emr_mode_json_fields(program_type)
    if active_field not in payload:
        message = f"EMR {program_type} requires task_params.{active_field}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="EMR",
            field=f"task_params.{active_field}",
            reason="missing-active-program-type-field",
            message=message,
        )
    _reject_field(
        payload,
        inactive_field,
        version=version,
        direction=direction,
        task_type="EMR",
        field=f"task_params.{inactive_field}",
        reason="inactive-program-type-field",
    )


def _emr_mode_json_fields(program_type: JsonValue) -> tuple[str, str]:
    """Return the active and inactive JSON fields for one validated EMR mode."""
    if program_type == "RUN_JOB_FLOW":
        return "jobFlowDefineJson", "stepsDefineJson"
    return "stepsDefineJson", "jobFlowDefineJson"
