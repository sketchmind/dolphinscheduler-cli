from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    validate_flink_inline_local_sql,
    validate_flink_inline_local_sql_ascii,
)
from dsctl.upstream.task_authoring_surface import (
    FlinkInlineSqlAuthoringSurface,
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskParameterProjectionError,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def _decode_opaque_flink_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
) -> DecodedTaskParameters:
    """Canonicalize only one exact safe native Flink inline-SQL package."""
    if not _flink_inline_sql_surface_for_task_type(
        version=version,
        task_type=task_type,
    ).available:
        return DecodedTaskParameters(
            ProjectedTask(task_type, payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    try:
        projected = _decode_flink_family_inline_local_sql(
            payload,
            version=version,
            task_type=task_type,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask(task_type, payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask(task_type, projected),
        ProjectionSource.TYPED_AUTHORING,
    )


_FLINK_CANONICAL_FIELDS = frozenset({"rawScript"})
_FLINK_NATIVE_VALUES = {
    "programType": "SQL",
    "deployMode": "local",
    "initScript": "",
}


def _encode_flink_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project FLINK literal SQL through the shared Flink-family seam."""
    return _encode_flink_family_inline_local_sql(
        payload,
        version=version,
        task_type="FLINK",
    )


def _encode_flink_stream_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Project FLINK_STREAM literal SQL through the shared Flink-family seam."""
    return _encode_flink_family_inline_local_sql(
        payload,
        version=version,
        task_type="FLINK_STREAM",
    )


def _encode_flink_family_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
) -> JsonObject:
    """Project one portable literal SQL script onto an exact Flink-family wire."""
    _require_flink_inline_sql_available(
        version=version,
        direction="encode",
        task_type=task_type,
    )
    _reject_flink_unowned_fields(
        payload,
        allowed=_FLINK_CANONICAL_FIELDS,
        version=version,
        direction="encode",
        task_type=task_type,
    )
    return {
        **_FLINK_NATIVE_VALUES,
        "rawScript": _flink_safe_raw_script(
            payload,
            version=version,
            direction="encode",
            task_type=task_type,
        ),
    }


def _decode_flink_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode FLINK literal SQL through the shared Flink-family seam."""
    return _decode_flink_family_inline_local_sql(
        payload,
        version=version,
        task_type="FLINK",
    )


def _decode_flink_stream_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
) -> JsonObject:
    """Decode FLINK_STREAM literal SQL through the shared Flink-family seam."""
    return _decode_flink_family_inline_local_sql(
        payload,
        version=version,
        task_type="FLINK_STREAM",
    )


def _decode_flink_family_inline_local_sql(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
) -> JsonObject:
    """Decode exact worker-local inline SQL evidence into canonical intent."""
    _require_flink_inline_sql_available(
        version=version,
        direction="decode",
        task_type=task_type,
    )
    allowed = _FLINK_CANONICAL_FIELDS | frozenset(_FLINK_NATIVE_VALUES)
    _reject_flink_unowned_fields(
        payload,
        allowed=allowed,
        version=version,
        direction="decode",
        task_type=task_type,
    )
    for key, expected in _FLINK_NATIVE_VALUES.items():
        if payload.get(key) == expected:
            continue
        reason = "missing-native-discriminator" if key not in payload else "wrong-mode"
        raise _flink_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{key}",
            reason=reason,
            message=f"{task_type} inline local SQL requires {key}={expected!r}",
            task_type=task_type,
        )
    return {
        "rawScript": _flink_safe_raw_script(
            payload,
            version=version,
            direction="decode",
            task_type=task_type,
        )
    }


def _flink_safe_raw_script(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> str:
    value = payload.get("rawScript")
    if not isinstance(value, str):
        reason = (
            "missing-required-field" if "rawScript" not in payload else "invalid-type"
        )
        raise _flink_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason=reason,
            message=(
                f"{task_type} inline local SQL requires one literal rawScript string"
            ),
            task_type=task_type,
        )
    surface = _flink_inline_sql_surface_for_task_type(
        version=version,
        task_type=task_type,
    )
    validator = (
        validate_flink_inline_local_sql_ascii
        if surface.script_encoding == "platform-default"
        else validate_flink_inline_local_sql
    )
    try:
        return validator(value)
    except ValueError as exc:
        raise _flink_projection_error(
            version=version,
            direction=direction,
            field="task_params.rawScript",
            reason="unsafe-inline-sql",
            message=f"{task_type} rawScript is outside the safe literal SQL subset",
            task_type=task_type,
        ) from exc


def _reject_flink_unowned_fields(
    payload: JsonObject,
    *,
    allowed: frozenset[str],
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    unexpected = sorted(set(payload) - allowed)
    if not unexpected:
        return
    names = ", ".join(unexpected)
    raise _flink_projection_error(
        version=version,
        direction=direction,
        field=f"task_params.{unexpected[0]}",
        reason="outside-reviewed-inline-local-sql-subset",
        message=(
            f"{task_type} inline local SQL projection does not own fields: {names}"
        ),
        task_type=task_type,
    )


def _require_flink_inline_sql_available(
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    surface = _flink_inline_sql_surface_for_task_type(
        version=version,
        task_type=task_type,
    )
    if surface.available:
        return
    reason = surface.exclusion_reason or "typed-facet-absent-in-version"
    raise _flink_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason=reason,
        message=(
            f"{task_type} inline local SQL is unavailable in DolphinScheduler {version}"
        ),
        task_type=task_type,
    )


def _flink_inline_sql_surface_for_task_type(
    *,
    version: str,
    task_type: str,
) -> FlinkInlineSqlAuthoringSurface:
    surface = get_task_authoring_surface(version)
    if task_type == "FLINK":
        return surface.flink_inline_sql
    if task_type == "FLINK_STREAM":
        return surface.flink_stream_inline_sql
    message = f"Unsupported Flink-family task type: {task_type}"
    raise ValueError(message)


def _flink_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
    reason: str,
    message: str,
    task_type: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type=task_type,
        field=field,
        reason=reason,
        message=message,
    )
