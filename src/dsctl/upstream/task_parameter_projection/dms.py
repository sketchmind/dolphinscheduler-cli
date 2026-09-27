from __future__ import annotations

from copy import deepcopy
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, cast, get_args, get_origin

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DmsResumeExistingFullLoadTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _projection_error,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskParameterProjectionError,
    )

_DMS_OWNED_FIELDS = frozenset(
    field.alias or name
    for name, field in DmsResumeExistingFullLoadTaskParamsSpec.model_fields.items()
)
_DMS_LITERAL_ERROR_VALUES: Mapping[str, JsonValue] = MappingProxyType(
    {
        field.alias or name: cast("JsonValue", get_args(field.annotation)[0])
        for name, field in DmsResumeExistingFullLoadTaskParamsSpec.model_fields.items()
        if get_origin(field.annotation) is Literal
    }
)


def _encode_dms(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate canonical DMS resume intent on its exact identity wire."""
    return _project_dms(payload, version=version, direction="encode")


def _decode_dms(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact reviewed five-field DMS resume identity."""
    return _project_dms(payload, version=version, direction="decode")


def _project_dms(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Keep one explicit existing full-load resume request unchanged on the wire."""
    _require_dms_available(version=version, direction=direction)
    unexpected = sorted(set(payload) - _DMS_OWNED_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _dms_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-existing-full-load-resume-subset",
            message=f"DMS resume projection does not own fields: {names}",
        )
    try:
        DmsResumeExistingFullLoadTaskParamsSpec.model_validate(payload, strict=True)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        key = str(error["loc"][0])
        if key in _DMS_LITERAL_ERROR_VALUES:
            if error["type"] == "missing":
                reason = "missing-required-field"
                message = f"DMS resume requires task_params.{key}"
            else:
                reason = "wrong-resume-mode"
                message = (
                    f"DMS resume requires task_params.{key}="
                    f"{_DMS_LITERAL_ERROR_VALUES[key]!r}"
                )
        elif error["type"] in {"missing", "string_type"}:
            reason = (
                "missing-required-field"
                if error["type"] == "missing"
                else "invalid-type"
            )
            message = "DMS resume requires one literal replicationTaskArn string"
        else:
            reason = "unsafe-replication-task-arn"
            message = "DMS replicationTaskArn is outside the safe literal task subset"
        raise _dms_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{key}",
            reason=reason,
            message=message,
        ) from exc
    return deepcopy(payload)


def _require_dms_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).dms.available:
        return
    raise _dms_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DMS does not exist in DolphinScheduler {version}",
    )


def _dms_projection_error(
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
        task_type="DMS",
        field=field,
        reason=reason,
        message=message,
    )
