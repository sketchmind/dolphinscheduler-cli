from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType
from typing import cast

from pydantic import ValidationError

from dsctl.models.task_spec import (
    DYNAMIC_MAX_WORKFLOW_CODE,
    DynamicLiteralSingleDimensionFanoutTaskParamsSpec,
)
from dsctl.support.json_types import JsonObject, JsonValue, require_json_object
from dsctl.upstream.task_authoring_surface import (
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
    TaskWorkflowRefIndex,
)

_DYNAMIC_CANONICAL_FIELDS = frozenset(
    {
        "childWorkflowName",
        "parameterName",
        "values",
        "degreeOfParallelism",
    }
)
_DYNAMIC_NATIVE_FIELDS = frozenset(
    {
        "processDefinitionCode",
        "maxNumOfSubWorkflowInstances",
        "degreeOfParallelism",
        "filterCondition",
        "listParameters",
    }
)
_DYNAMIC_EMPTY_UI_FIELDS = frozenset({"localParams", "resourceList"})
_DYNAMIC_MODEL_FIELD_ALIASES: Mapping[str, str] = MappingProxyType(
    {
        "child_workflow_name": "childWorkflowName",
        "parameter_name": "parameterName",
        "degree_of_parallelism": "degreeOfParallelism",
    }
)


def _encode_dynamic_literal_single_dimension_fanout(
    payload: JsonObject,
    *,
    version: str,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> JsonObject:
    """Project one bounded literal dimension onto the exact 3.2.2 native wire."""
    validated = _validate_dynamic_canonical(
        payload,
        version=version,
        direction="encode",
    )
    values = list(validated.values)
    if workflow_refs is None:
        raise _dynamic_projection_error(
            version=version,
            direction="encode",
            field="task_params.childWorkflowName",
            reason="missing-workflow-reference-index",
            message="DYNAMIC childWorkflowName requires same-project resolution",
        )
    workflow_code = workflow_refs.code_by_name.get(validated.child_workflow_name)
    if workflow_code is None:
        raise _dynamic_projection_error(
            version=version,
            direction="encode",
            field="task_params.childWorkflowName",
            reason="unresolved-child-workflow-name",
            message=(
                f"DYNAMIC child workflow {validated.child_workflow_name!r} was not "
                "resolved in the selected project"
            ),
        )
    return {
        "processDefinitionCode": workflow_code,
        "maxNumOfSubWorkflowInstances": len(values),
        "degreeOfParallelism": validated.degree_of_parallelism,
        "filterCondition": "",
        "listParameters": [
            {
                "name": validated.parameter_name,
                "value": ",".join(values),
                "separator": ",",
            }
        ],
    }


def _decode_dynamic_literal_single_dimension_fanout(
    payload: JsonObject,
    *,
    version: str,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> JsonObject:
    """Decode only the compiler-owned fixed point; reject every richer shape."""
    _require_dynamic_typed_available(version=version, direction="decode")
    raw_parameter = _dynamic_native_parameter(payload, version=version)
    raw_value = _dynamic_native_parameter_value(raw_parameter, version=version)
    values = raw_value.split(",")
    native_max = payload["maxNumOfSubWorkflowInstances"]
    if (
        not isinstance(native_max, int)
        or isinstance(native_max, bool)
        or native_max != len(values)
    ):
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.maxNumOfSubWorkflowInstances",
            reason="noncanonical-native-maximum",
            message=(
                "DYNAMIC native maximum must equal the exact generated value count"
            ),
        )
    workflow_code = payload["processDefinitionCode"]
    if (
        not isinstance(workflow_code, int)
        or isinstance(workflow_code, bool)
        or not 0 < workflow_code <= DYNAMIC_MAX_WORKFLOW_CODE
    ):
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.processDefinitionCode",
            reason="invalid-native-child-workflow-code",
            message="DYNAMIC native child workflow code must be a positive int64",
        )
    if workflow_refs is None:
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.processDefinitionCode",
            reason="missing-workflow-reference-index",
            message="DYNAMIC native child workflow code requires name resolution",
        )
    child_workflow_name = workflow_refs.name_by_code.get(workflow_code)
    if child_workflow_name is None:
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.processDefinitionCode",
            reason="unresolved-child-workflow-code",
            message=(
                f"DYNAMIC child workflow code {workflow_code} was not resolved in "
                "the selected project"
            ),
        )
    canonical: JsonObject = {
        "childWorkflowName": child_workflow_name,
        "parameterName": deepcopy(raw_parameter.get("name")),
        "values": values,
        "degreeOfParallelism": deepcopy(payload["degreeOfParallelism"]),
    }
    validated = _validate_dynamic_canonical(
        canonical,
        version=version,
        direction="decode",
    )
    return require_json_object(
        validated.to_payload(),
        label="validated DYNAMIC task_params",
    )


def _dynamic_native_parameter(
    payload: JsonObject,
    *,
    version: str,
) -> Mapping[str, JsonValue]:
    """Validate the fixed native envelope and return its only dimension."""
    unexpected = sorted(
        set(payload) - _DYNAMIC_NATIVE_FIELDS - _DYNAMIC_EMPTY_UI_FIELDS
    )
    if unexpected:
        names = ", ".join(unexpected)
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-single-dimension-subset",
            message=f"DYNAMIC projection does not own fields: {names}",
        )
    for key in _DYNAMIC_EMPTY_UI_FIELDS:
        if key not in payload:
            continue
        if payload[key] != []:
            raise _dynamic_projection_error(
                version=version,
                direction="decode",
                field=f"task_params.{key}",
                reason="nonempty-ui-only-field",
                message=f"DYNAMIC native {key} is typed only as []",
            )
    missing = sorted(_DYNAMIC_NATIVE_FIELDS - set(payload))
    if missing:
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field=f"task_params.{missing[0]}",
            reason="missing-required-wire-field",
            message=f"DYNAMIC native wire requires task_params.{missing[0]}",
        )
    if payload["filterCondition"] != "":
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.filterCondition",
            reason="nonempty-filter-outside-reviewed-subset",
            message="DYNAMIC typed wire requires an empty filterCondition",
        )
    raw_parameters = payload["listParameters"]
    if not isinstance(raw_parameters, list) or len(raw_parameters) != 1:
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.listParameters",
            reason="not-one-native-dimension",
            message="DYNAMIC typed wire requires exactly one listParameters item",
        )
    raw_parameter = raw_parameters[0]
    if not isinstance(raw_parameter, Mapping):
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.listParameters[0]",
            reason="invalid-native-dimension-shape",
            message="DYNAMIC typed listParameters item must be an object",
        )
    return cast("Mapping[str, JsonValue]", raw_parameter)


def _dynamic_native_parameter_value(
    raw_parameter: Mapping[str, JsonValue],
    *,
    version: str,
) -> str:
    """Validate and return one exact comma-delimited dimension value."""
    required_parameter_fields = frozenset({"name", "value", "separator"})
    parameter_fields = frozenset(raw_parameter)
    if parameter_fields not in {
        required_parameter_fields,
        frozenset((*required_parameter_fields, "disabled")),
    } or ("disabled" in raw_parameter and raw_parameter["disabled"] is not True):
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.listParameters[0]",
            reason="invalid-native-dimension-shape",
            message=(
                "DYNAMIC typed listParameters item owns exactly name, value, and "
                "separator; exact disabled=true UI residue is decode-only"
            ),
        )
    if raw_parameter.get("separator") != ",":
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.listParameters[0].separator",
            reason="noncanonical-native-separator",
            message="DYNAMIC typed wire requires the compiler-owned comma separator",
        )
    raw_value = raw_parameter.get("value")
    if not isinstance(raw_value, str):
        raise _dynamic_projection_error(
            version=version,
            direction="decode",
            field="task_params.listParameters[0].value",
            reason="invalid-native-value-type",
            message="DYNAMIC native dimension value must be one string",
        )
    return raw_value


def _decode_opaque_dynamic_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    workflow_refs: TaskWorkflowRefIndex | None,
) -> DecodedTaskParameters:
    """Preserve runtime holes/richer state and recognize only the 3.2.2 fixed point."""
    surface = get_task_authoring_surface(version).dynamic
    if not surface.typed_available:
        return DecodedTaskParameters(
            ProjectedTask("DYNAMIC", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    try:
        projected = _decode_dynamic_literal_single_dimension_fanout(
            payload,
            version=version,
            workflow_refs=workflow_refs,
        )
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask("DYNAMIC", payload),
            ProjectionSource.OPAQUE_PRESERVE,
        )
    return DecodedTaskParameters(
        ProjectedTask("DYNAMIC", projected),
        ProjectionSource.TYPED_AUTHORING,
    )


def _validate_dynamic_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> DynamicLiteralSingleDimensionFanoutTaskParamsSpec:
    """Apply the closed canonical DYNAMIC model with stable projection errors."""
    _require_dynamic_typed_available(version=version, direction=direction)
    unexpected = sorted(set(payload) - _DYNAMIC_CANONICAL_FIELDS)
    if unexpected:
        names = ", ".join(unexpected)
        raise _dynamic_projection_error(
            version=version,
            direction=direction,
            field=f"task_params.{unexpected[0]}",
            reason="outside-reviewed-single-dimension-subset",
            message=f"DYNAMIC canonical projection does not own fields: {names}",
        )
    try:
        return DynamicLiteralSingleDimensionFanoutTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params"
        for part in location:
            if isinstance(part, int):
                field += f"[{part}]"
            else:
                field += f".{_DYNAMIC_MODEL_FIELD_ALIASES.get(part, part)}"
        raise _dynamic_projection_error(
            version=version,
            direction=direction,
            field=field,
            reason="invalid-canonical-value",
            message=f"DYNAMIC typed task params are invalid: {error['msg']}",
        ) from exc


def _require_dynamic_typed_available(
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    surface = get_task_authoring_surface(version).dynamic
    if surface.registered and surface.typed_available:
        return
    reason = surface.exclusion_reason or "upstream-absent"
    raise _dynamic_projection_error(
        version=version,
        direction=direction,
        field="task.type",
        reason=reason,
        message=(
            f"DYNAMIC typed execution is unavailable in DolphinScheduler "
            f"{version}: {reason}"
        ),
    )


def _dynamic_projection_error(
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
        task_type="DYNAMIC",
        field=field,
        reason=reason,
        message=message,
    )
