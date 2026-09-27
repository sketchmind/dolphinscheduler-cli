from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.task_spec import (
    DEPENDENT_BASE_MONTH_DATE_VALUES,
    DEPENDENT_EXTENDED_MONTH_DATE_VALUES,
)
from dsctl.support.json_types import JsonObject, JsonValue, require_json_object
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.types import (
    DecodedTaskParameters,
    ProjectedTask,
    ProjectionDirection,
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    _ParameterCodec,
)
from dsctl.versioning import normalize_version

_LEGACY_VERSIONS = frozenset(
    {
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "2.0.9",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
    }
)
_DEPENDENT_MONTH_EXTENSION = frozenset(DEPENDENT_EXTENDED_MONTH_DATE_VALUES) - set(
    DEPENDENT_BASE_MONTH_DATE_VALUES
)
_CONDITION_STRING_RESULT_VERSIONS = frozenset(
    {
        "2.0.0",
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
    }
)
_NESTED_WORKFLOW_TYPES = frozenset({"SUB_PROCESS", "SUB_WORKFLOW"})


def _decode_canonical_native(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
    decoder: _ParameterCodec,
) -> DecodedTaskParameters:
    """Preserve richer native input when an explicitly selected typed decode fails."""
    try:
        projected = decoder(payload, version=version)
    except TaskParameterProjectionError:
        return DecodedTaskParameters(
            ProjectedTask(task_type, payload), ProjectionSource.OPAQUE_PRESERVE
        )
    return DecodedTaskParameters(
        ProjectedTask(task_type, projected), ProjectionSource.TYPED_AUTHORING
    )


def _opaque_projection_identity(*, version: str, task_type: str) -> bool:
    """Keep native shapes whose canonical discriminator has no provenance."""
    authoring_surface = get_task_authoring_surface(version)
    linkis = (
        task_type == "LINKIS"
        and authoring_surface.linkis.registered
        and not authoring_surface.linkis.typed_available
    )
    surface = authoring_surface.emr
    legacy_emr = (
        task_type == "EMR" and surface.available and not surface.native_program_type
    )
    zeppelin = task_type == "ZEPPELIN" and authoring_surface.zeppelin.available
    return linkis or legacy_emr or zeppelin


def _exact_version(version: str) -> str:
    normalized = normalize_version(version)
    if normalized not in TARGET_DS_VERSIONS:
        supported = ", ".join(TARGET_DS_VERSIONS)
        message = (
            f"No exact task parameter projector for {version!r}; "
            f"supported versions: {supported}"
        )
        raise ValueError(message)
    return normalized


def _nested_items_have_key(payload: JsonObject, key: str) -> bool:
    return _nested_items_have_any_key(payload, {key})


def _nested_items_have_any_key(payload: JsonObject, keys: set[str]) -> bool:
    dependence = payload.get("dependence")
    if not isinstance(dependence, Mapping):
        return False
    groups = dependence.get("dependTaskList")
    if not isinstance(groups, Sequence) or isinstance(groups, (str, bytes, bytearray)):
        return False
    return any(
        isinstance(item, Mapping) and bool(keys.intersection(item))
        for group in groups
        if isinstance(group, Mapping)
        for item in _mapping_sequence(group.get("dependItemList"))
    )


def _nested_items_have_value(
    payload: JsonObject,
    key: str,
    value: JsonValue,
) -> bool:
    dependence = payload.get("dependence")
    if not isinstance(dependence, Mapping):
        return False
    groups = dependence.get("dependTaskList")
    if not isinstance(groups, Sequence) or isinstance(groups, (str, bytes, bytearray)):
        return False
    return any(
        isinstance(item, Mapping) and item.get(key) == value
        for group in groups
        if isinstance(group, Mapping)
        for item in _mapping_sequence(group.get("dependItemList"))
    )


def _mapping_sequence(value: JsonValue) -> Sequence[JsonValue]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return ()


def _copy_json_object(value: JsonValue, *, label: str) -> JsonObject:
    return deepcopy(require_json_object(value, label=label))


def _json_value_exact_equal(left: JsonValue, right: JsonValue) -> bool:
    """Compare JSON values without Python's bool/int equality aliasing."""
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return set(left) == set(right) and all(
            _json_value_exact_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _json_value_exact_equal(left_value, right_value)
            for left_value, right_value in zip(left, right, strict=True)
        )
    return type(left) is type(right) and left == right


def _compact_json_array(value: JsonValue) -> str:
    if not isinstance(value, list):
        message = "Validated K8S argv must be an array"
        raise TypeError(message)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _projection_error(
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    field: str,
    reason: str,
    message: str,
) -> TaskParameterProjectionError:
    return TaskParameterProjectionError(
        message,
        version=version,
        direction=direction,
        task_type=task_type,
        field=field,
        reason=reason,
    )


def _reject_139_code_projection(
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    if version != "1.3.9" or task_type not in {
        "CONDITIONS",
        "DEPENDENT",
        "SUB_PROCESS",
        "SUB_WORKFLOW",
        "SWITCH",
    }:
        return
    message = (
        f"{task_type} cannot be projected through local task codes for "
        "DolphinScheduler 1.3.9"
    )
    raise _projection_error(
        version=version,
        direction=direction,
        task_type=task_type,
        field="task_params",
        reason="no-code-based-projection",
        message=message,
    )


def _object_field(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    field: str,
) -> JsonObject:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        message = f"{task_type} requires {field} to be a JSON object"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field=field,
            reason="invalid-object-shape",
            message=message,
        )
    return deepcopy(dict(value))


def _list_field(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    field: str,
) -> list[JsonValue]:
    value = payload.get(key)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        message = f"{task_type} requires {field} to be a JSON array"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type=task_type,
            field=field,
            reason="invalid-array-shape",
            message=message,
        )
    return deepcopy(list(value))


def _encode_task_ref(
    value: JsonValue,
    *,
    version: str,
    task_type: str,
    field: str,
    refs: TaskRefIndex,
) -> int:
    if not isinstance(value, str):
        message = f"{task_type} expects a canonical task name in {field}"
        raise _projection_error(
            version=version,
            direction="encode",
            task_type=task_type,
            field=field,
            reason="expected-task-name",
            message=message,
        )
    name = value.strip()
    code = refs.code_by_name.get(name)
    if code is None:
        message = f"{task_type} references unknown local task {name!r} in {field}"
        raise _projection_error(
            version=version,
            direction="encode",
            task_type=task_type,
            field=field,
            reason="unknown-task-name",
            message=message,
        )
    return code


def _decode_task_ref(
    value: JsonValue,
    *,
    version: str,
    task_type: str,
    field: str,
    refs: TaskRefIndex,
) -> str:
    code = _native_task_code(
        value,
        version=version,
        task_type=task_type,
        field=field,
    )
    name = refs.name_by_code.get(code)
    if name is None:
        message = f"{task_type} contains unknown local task code {code} in {field}"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type=task_type,
            field=field,
            reason="unknown-task-code",
            message=message,
        )
    return name


def _native_task_code(
    value: JsonValue,
    *,
    version: str,
    task_type: str,
    field: str,
) -> int:
    candidate = value
    if isinstance(candidate, Sequence) and not isinstance(
        candidate,
        (str, bytes, bytearray),
    ):
        if len(candidate) != 1:
            message = f"{task_type} cannot represent multiple task codes in {field}"
            raise _projection_error(
                version=version,
                direction="decode",
                task_type=task_type,
                field=field,
                reason="unrepresentable-reference-shape",
                message=message,
            )
        candidate = candidate[0]
    if isinstance(candidate, str):
        stripped = candidate.strip()
        if stripped.isdecimal():
            candidate = int(stripped)
    if not isinstance(candidate, int) or isinstance(candidate, bool) or candidate <= 0:
        message = f"{task_type} requires one positive native task code in {field}"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type=task_type,
            field=field,
            reason="invalid-task-code",
            message=message,
        )
    return candidate


def _reject_field(
    payload: Mapping[str, JsonValue],
    key: str,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    field: str,
    reason: str,
) -> None:
    if key not in payload:
        return
    message = f"{task_type} {field} is not authorable for DolphinScheduler {version}"
    raise _projection_error(
        version=version,
        direction=direction,
        task_type=task_type,
        field=field,
        reason=reason,
        message=message,
    )


def _drop_empty_compatibility_list(
    payload: JsonObject,
    *,
    key: str,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
) -> None:
    if key not in payload:
        return
    value = payload[key]
    if isinstance(value, list) and not value:
        del payload[key]
        return
    field = f"task_params.{key}"
    message = f"{task_type} does not consume non-empty {field}"
    raise _projection_error(
        version=version,
        direction=direction,
        task_type=task_type,
        field=field,
        reason="compatibility-only-field",
        message=message,
    )


def _project_parameter_passing(
    item: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> None:
    if not get_task_authoring_surface(version).dependent.parameter_passing:
        _reject_field(
            item,
            "parameterPassing",
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.parameterPassing",
            reason="field-absent-in-version",
        )
        return
    if "parameterPassing" not in item:
        item["parameterPassing"] = False
        return
    value = item["parameterPassing"]
    if not isinstance(value, bool):
        message = f"DEPENDENT requires a boolean in {field}.parameterPassing"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.parameterPassing",
            reason="invalid-parameter-passing",
            message=message,
        )
