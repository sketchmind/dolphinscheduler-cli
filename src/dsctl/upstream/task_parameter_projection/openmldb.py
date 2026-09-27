from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_openmldb_sql,
    validate_openmldb_zk,
    validate_openmldb_zk_path,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_OPENMLDB_OWNED_FIELDS = frozenset({"zk", "zkPath", "executeMode", "sql"})


def _encode_openmldb(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate canonical OpenMLDB intent on its exact identity wire."""
    return _project_openmldb(payload, version=version, direction="encode")


def _decode_openmldb(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact reviewed OpenMLDB identity wire."""
    return _project_openmldb(payload, version=version, direction="decode")


def _project_openmldb(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Validate one literal single statement without changing wire spelling."""
    # Native projection historically accepts surrogate-bearing SQL that the
    # canonical model's length constraint rejects; retain the shared leaf rules.
    _require_openmldb_available(version=version, direction=direction)
    unexpected = sorted(set(payload) - _OPENMLDB_OWNED_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _openmldb_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-literal-single-statement-subset",
            message=(
                "OPENMLDB literal single-statement projection does not own "
                f"fields: {names}"
            ),
        )
    projected = deepcopy(payload)
    validators = {
        "zk": validate_openmldb_zk,
        "zkPath": validate_openmldb_zk_path,
        "sql": validate_openmldb_sql,
    }
    for key, validator in validators.items():
        value = projected.get(key)
        if not isinstance(value, str):
            reason = (
                "missing-required-field" if key not in projected else "invalid-type"
            )
            raise _openmldb_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason=reason,
                message=f"OPENMLDB requires one literal string task_params.{key}",
            )
        try:
            validator(value)
        except ValueError as exc:
            raise _openmldb_projection_error(
                version=version,
                direction=direction,
                field=f"task_params.{key}",
                reason="unsafe-value",
                message=(
                    f"OPENMLDB task_params.{key} is outside the safe literal subset"
                ),
            ) from exc
    execute_mode = projected.get("executeMode")
    if not isinstance(execute_mode, str):
        reason = (
            "missing-required-field"
            if "executeMode" not in projected
            else "invalid-type"
        )
        raise _openmldb_projection_error(
            version=version,
            direction=direction,
            field="task_params.executeMode",
            reason=reason,
            message="OPENMLDB requires one string task_params.executeMode",
        )
    if execute_mode not in {"offline", "online"}:
        raise _openmldb_projection_error(
            version=version,
            direction=direction,
            field="task_params.executeMode",
            reason="unsupported-execute-mode",
            message="OPENMLDB task_params.executeMode must be offline or online",
        )
    return projected


def _require_openmldb_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    surface = get_task_authoring_surface(version).openmldb
    if surface.available:
        return
    if surface.exclusion_reason is not None:
        raise _openmldb_projection_error(
            version=version,
            direction=direction,
            field="task.type",
            reason=surface.exclusion_reason,
            message=(
                f"OPENMLDB typed authoring is unavailable in DolphinScheduler {version}"
            ),
        )
    raise _openmldb_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"OPENMLDB does not exist in DolphinScheduler {version}",
    )


def _require_openmldb_plugin_present(
    *, version: str, direction: ProjectionDirection
) -> None:
    """Keep registered runtime holes eligible for unchanged native preservation."""
    if get_task_authoring_surface(version).openmldb.exclusion_reason is None:
        _require_openmldb_available(version=version, direction=direction)


def _openmldb_projection_error(
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
        task_type="OPENMLDB",
        field=field,
        reason=reason,
        message=message,
    )
