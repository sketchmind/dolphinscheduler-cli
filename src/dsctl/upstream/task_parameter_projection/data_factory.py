from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DataFactoryPipelineTriggerTaskParamsSpec,
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

_DATA_FACTORY_OWNED_FIELDS = frozenset(
    field.alias or name
    for name, field in DataFactoryPipelineTriggerTaskParamsSpec.model_fields.items()
)


def _encode_data_factory(payload: JsonObject, *, version: str) -> JsonObject:
    """Validate one canonical Azure Data Factory pipeline identity."""
    return _project_data_factory(payload, version=version, direction="encode")


def _decode_data_factory(payload: JsonObject, *, version: str) -> JsonObject:
    """Decode only the exact reviewed three-field native identity."""
    return _project_data_factory(payload, version=version, direction="decode")


def _project_data_factory(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    """Keep one literal three-field pipeline trigger unchanged on the wire."""
    _require_data_factory_available(version=version, direction=direction)
    unexpected = sorted(set(payload) - _DATA_FACTORY_OWNED_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _data_factory_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-pipeline-trigger-subset",
            message=(
                f"DATA_FACTORY pipeline-trigger projection does not own fields: {names}"
            ),
        )
    try:
        DataFactoryPipelineTriggerTaskParamsSpec.model_validate(payload, strict=True)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        key = str(error["loc"][0])
        if error["type"] in {"missing", "string_type"}:
            reason = (
                "missing-required-field"
                if error["type"] == "missing"
                else "invalid-type"
            )
            message = f"DATA_FACTORY requires one literal string task_params.{key}"
        else:
            reason = "unsafe-value"
            message = (
                f"DATA_FACTORY task_params.{key} is outside the safe literal "
                "identity subset"
            )
        raise _data_factory_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{key}",
            reason=reason,
            message=message,
        ) from exc
    return deepcopy(payload)


def _require_data_factory_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).data_factory.available:
        return
    raise _data_factory_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason="task-type-absent-in-version",
        message=f"DATA_FACTORY does not exist in DolphinScheduler {version}",
    )


def _data_factory_projection_error(
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
        task_type="DATA_FACTORY",
        field=field,
        reason=reason,
        message=message,
    )
