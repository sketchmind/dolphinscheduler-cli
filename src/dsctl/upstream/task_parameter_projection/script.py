from __future__ import annotations

from copy import deepcopy
from functools import partial
from typing import TYPE_CHECKING

from pydantic import ValidationError

from dsctl.models.task_spec import (
    ScriptTaskParamsSpec,
)
from dsctl.upstream.parameter_semantics import get_parameter_semantics
from dsctl.upstream.task_parameter_projection.resource_info import (
    decode_resource_info,
    encode_resource_info,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _decode_canonical_native,
    _projection_error,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection.types import (
        DecodedTaskParameters,
        ProjectionDirection,
        TaskParameterProjectionError,
        TaskResourceRefIndex,
    )


def _decode_opaque_script_with_provenance(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    resource_refs: TaskResourceRefIndex | None,
) -> DecodedTaskParameters:
    """Recognize the legacy script package only in its reviewed wire epoch."""
    return _decode_canonical_native(
        payload,
        version=version,
        task_type=task_type,
        decoder=partial(
            _decode_legacy_script, task_type=task_type, resource_refs=resource_refs
        ),
    )


_LEGACY_SCRIPT_139_FIELDS = frozenset(
    {
        "rawScript",
        "localParams",
        "resourceList",
    }
)


def _encode_legacy_python(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Project one canonical PYTHON script onto its exact legacy resource wire."""
    return _encode_legacy_script(
        payload, version=version, task_type="PYTHON", resource_refs=resource_refs
    )


def _encode_legacy_shell(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Project one canonical SHELL script onto its exact legacy resource wire."""
    return _encode_legacy_script(
        payload, version=version, task_type="SHELL", resource_refs=resource_refs
    )


def _encode_legacy_script(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Validate the no-resource 1.3.9 typed script subset."""
    if version != "1.3.9":
        return _project_script_resources(
            payload,
            version=version,
            task_type=task_type,
            resource_refs=resource_refs,
            direction="encode",
        )
    _validate_legacy_script_139_canonical(
        payload,
        version=version,
        direction="encode",
        task_type=task_type,
    )
    return payload


def _decode_legacy_python(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Decode one exact legacy PYTHON resource package."""
    return _decode_legacy_script(
        payload, version=version, task_type="PYTHON", resource_refs=resource_refs
    )


def _decode_legacy_shell(
    payload: JsonObject, *, version: str, resource_refs: TaskResourceRefIndex | None
) -> JsonObject:
    """Decode one exact legacy SHELL resource package."""
    return _decode_legacy_script(
        payload, version=version, task_type="SHELL", resource_refs=resource_refs
    )


def _decode_legacy_script(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    resource_refs: TaskResourceRefIndex | None,
) -> JsonObject:
    """Decode only the no-resource 1.3.9 typed script subset."""
    if version != "1.3.9":
        return _project_script_resources(
            payload,
            version=version,
            task_type=task_type,
            resource_refs=resource_refs,
            direction="decode",
        )
    unexpected = sorted(set(payload) - _LEGACY_SCRIPT_139_FIELDS)
    if unexpected:
        field = unexpected[0]
        raise _legacy_script_projection_error(
            version=version,
            direction="decode",
            task_type=task_type,
            field=f"task_params.{field}",
            reason="outside-reviewed-script-subset",
            message=(
                f"{task_type} 1.3.9 typed projection does not own field {field!r}"
            ),
        )
    projected = deepcopy(payload)
    _validate_legacy_script_139_canonical(
        projected,
        version=version,
        direction="decode",
        task_type=task_type,
    )
    return projected


def _project_script_resources(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    resource_refs: TaskResourceRefIndex | None,
    direction: ProjectionDirection,
) -> JsonObject:
    projected = deepcopy(payload)
    resources = projected.get("resourceList", [])
    if not isinstance(resources, list):
        raise _projection_error(
            version=version,
            task_type=task_type,
            direction=direction,
            field="task_params.resourceList",
            reason="invalid-resource-list",
            message="resourceList must be a list",
        )
    result: list[JsonObject] = []
    for index, resource in enumerate(resources):
        field = f"task_params.resourceList[{index}]"
        if direction == "encode":
            if (
                not isinstance(resource, dict)
                or set(resource) != {"resourceName"}
                or not isinstance(resource["resourceName"], str)
            ):
                raise _projection_error(
                    version=version,
                    task_type=task_type,
                    direction=direction,
                    field=field,
                    reason="invalid-canonical-resource-name",
                    message="Script resources require one canonical resourceName",
                )
            result.append(
                encode_resource_info(
                    resource["resourceName"],
                    version=version,
                    task_type=task_type,
                    field=field,
                    resource_refs=resource_refs,
                )
            )
        else:
            result.append(
                {
                    "resourceName": decode_resource_info(
                        resource,
                        version=version,
                        task_type=task_type,
                        field=field,
                        resource_refs=resource_refs,
                    )
                }
            )
    if "resourceList" in projected or result:
        projected["resourceList"] = result
    return projected


def _validate_legacy_script_139_canonical(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    """Validate the closed script and reversible resource subset for DS 1.3.9."""
    validated = _validate_legacy_script_139_shape(
        payload,
        version=version,
        direction=direction,
        task_type=task_type,
    )
    _validate_legacy_script_139_parameters(
        validated,
        version=version,
        direction=direction,
        task_type=task_type,
    )
    _validate_legacy_script_139_resources(
        payload,
        version=version,
        direction=direction,
        task_type=task_type,
    )


def _validate_legacy_script_139_shape(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> ScriptTaskParamsSpec:
    """Validate the closed top-level legacy script shape."""
    unexpected = sorted(set(payload) - _LEGACY_SCRIPT_139_FIELDS)
    if unexpected:
        field = unexpected[0]
        raise _legacy_script_projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field=f"task_params.{field}",
            reason="outside-reviewed-script-subset",
            message=(
                f"{task_type} 1.3.9 typed projection does not own field {field!r}"
            ),
        )
    try:
        validated = ScriptTaskParamsSpec.model_validate(payload)
    except ValidationError as exc:
        error = exc.errors(include_url=False)[0]
        location = error["loc"]
        field = "task_params" + "".join(
            f"[{part}]" if isinstance(part, int) else f".{part}" for part in location
        )
        raise _legacy_script_projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field=field,
            reason=(
                "missing-required-field"
                if error["type"] == "missing"
                else "invalid-canonical-value"
            ),
            message=(
                f"{task_type} 1.3.9 typed task params are invalid: {error['msg']}"
            ),
        ) from exc
    if direction == "decode" and validated.to_payload() != payload:
        raise _legacy_script_projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field="task_params",
            reason="non-canonical-native-spelling",
            message=(
                f"{task_type} 1.3.9 native task params would change under typed "
                "normalization and must remain opaque"
            ),
        )
    return validated


def _validate_legacy_script_139_parameters(
    validated: ScriptTaskParamsSpec,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    """Reject lossy duplicate and unsupported local parameter semantics."""
    seen_names: set[str] = set()
    for index, parameter in enumerate(validated.local_params):
        if parameter.prop in seen_names:
            raise _legacy_script_projection_error(
                version=version,
                direction=direction,
                task_type=task_type,
                field=f"task_params.localParams[{index}].prop",
                reason="duplicate-local-param-name",
                message=f"{task_type} localParams prop names must be unique",
            )
        seen_names.add(parameter.prop)
    parameter_semantics = get_parameter_semantics(version)
    for index, parameter in enumerate(validated.local_params):
        if parameter.direct.value == "OUT":
            raise _legacy_script_projection_error(
                version=version,
                direction=direction,
                task_type=task_type,
                field=f"task_params.localParams[{index}].direct",
                reason="unsupported-parameter-direction",
                message=f"{task_type} 1.3.9 typed localParams accept IN only",
            )
        if parameter.type.value not in parameter_semantics.allowed_property_types:
            raise _legacy_script_projection_error(
                version=version,
                direction=direction,
                task_type=task_type,
                field=f"task_params.localParams[{index}].type",
                reason="unsupported-parameter-data-type",
                message=(
                    f"{task_type} 1.3.9 localParams use a data type absent from "
                    "the exact profile"
                ),
            )


def _validate_legacy_script_139_resources(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    """Reject 1.3.9 typed resources because old-name wires bypass ID checks."""
    resources = payload.get("resourceList")
    if resources is None:
        return
    if not isinstance(resources, list):
        raise _legacy_script_projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field="task_params.resourceList",
            reason="invalid-resource-list",
            message=f"{task_type} resourceList must be a list",
        )
    if resources:
        raise _legacy_script_projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field="task_params.resourceList",
            reason="unsafe-legacy-resource-authoring",
            message=(
                f"{task_type} 1.3.9 typed authoring requires resourceList to be "
                "empty because the old full-name resource wire bypasses positive-ID "
                "permission checks"
            ),
        )


def _legacy_script_projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return _projection_error(
        version=version,
        direction=direction,
        task_type=task_type,
        field=field,
        reason=reason,
        message=message,
    )
