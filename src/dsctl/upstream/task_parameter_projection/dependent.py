from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    contains_ds_parameter_placeholder,
)
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _DEPENDENT_MONTH_EXTENSION,
    _LEGACY_VERSIONS,
    _drop_empty_compatibility_list,
    _list_field,
    _object_field,
    _project_parameter_passing,
    _projection_error,
    _reject_field,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
    )


def _encode_dependent(payload: JsonObject, *, version: str) -> JsonObject:
    encoded = deepcopy(payload)
    _drop_empty_compatibility_list(
        encoded,
        key="resourceList",
        version=version,
        direction="encode",
        task_type="DEPENDENT",
    )
    dependence = _dependent_tree(encoded, version=version, direction="encode")
    encoded["dependence"] = dependence
    return encoded


def _decode_dependent(payload: JsonObject, *, version: str) -> JsonObject:
    decoded = deepcopy(payload)
    _drop_empty_compatibility_list(
        decoded,
        key="resourceList",
        version=version,
        direction="decode",
        task_type="DEPENDENT",
    )
    dependence = _dependent_tree(decoded, version=version, direction="decode")
    decoded["dependence"] = dependence
    return decoded


def _dependent_tree(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> JsonObject:
    dependence = _object_field(
        payload,
        "dependence",
        version=version,
        direction=direction,
        task_type="DEPENDENT",
        field="task_params.dependence",
    )
    _validate_dependent_policy_fields(
        dependence,
        version=version,
        direction=direction,
    )
    groups = _list_field(
        dependence,
        "dependTaskList",
        version=version,
        direction=direction,
        task_type="DEPENDENT",
        field="task_params.dependence.dependTaskList",
    )
    projected_groups: list[JsonValue] = []
    for group_index, raw_group in enumerate(groups):
        projected_groups.append(
            _dependent_group(
                raw_group,
                version=version,
                direction=direction,
                index=group_index,
            )
        )
    dependence["dependTaskList"] = projected_groups
    return dependence


def _validate_dependent_policy_fields(
    dependence: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
) -> None:
    if get_task_authoring_surface(version).dependent.failure_control:
        return
    for key in ("checkInterval", "failurePolicy", "failureWaitingTime"):
        _reject_field(
            dependence,
            key,
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"task_params.dependence.{key}",
            reason="field-absent-in-version",
        )


def _dependent_group(
    raw_group: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
    index: int,
) -> JsonObject:
    field = f"task_params.dependence.dependTaskList[{index}]"
    if not isinstance(raw_group, Mapping):
        message = f"DEPENDENT requires {field} to be a JSON object"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="invalid-object-shape",
            message=message,
        )
    group = deepcopy(dict(raw_group))
    items = _list_field(
        group,
        "dependItemList",
        version=version,
        direction=direction,
        task_type="DEPENDENT",
        field=f"{field}.dependItemList",
    )
    group["dependItemList"] = [
        _dependent_item(
            item,
            version=version,
            direction=direction,
            field=f"{field}.dependItemList[{item_index}]",
        )
        for item_index, item in enumerate(items)
    ]
    return group


def _dependent_item(
    raw_item: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> JsonObject:
    if not isinstance(raw_item, Mapping):
        message = f"DEPENDENT requires {field} to be a JSON object"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="invalid-object-shape",
            message=message,
        )
    item = deepcopy(dict(raw_item))
    for runtime_key in ("dependResult", "status"):
        _reject_field(
            item,
            runtime_key,
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.{runtime_key}",
            reason="runtime-only",
        )
    _validate_dependent_date_epoch(
        item,
        version=version,
        direction=direction,
        field=field,
    )
    code = _dependent_task_code(
        item.get("depTaskCode"),
        version=version,
        direction=direction,
        field=f"{field}.depTaskCode",
    )
    if direction == "encode":
        dependent_type = _canonical_dependent_type(
            item.get("dependentType"),
            code=code,
            version=version,
            direction=direction,
            field=f"{field}.dependentType",
        )
        if version in _LEGACY_VERSIONS:
            del item["dependentType"]
        else:
            item["dependentType"] = dependent_type
    elif version in _LEGACY_VERSIONS:
        _reject_field(
            item,
            "dependentType",
            version=version,
            direction="decode",
            task_type="DEPENDENT",
            field=f"{field}.dependentType",
            reason="field-absent-in-version",
        )
        item["dependentType"] = _dependent_type_for_code(
            code,
            version=version,
            direction=direction,
            field=f"{field}.depTaskCode",
        )
    else:
        item["dependentType"] = _canonical_dependent_type(
            item.get("dependentType"),
            code=code,
            version=version,
            direction=direction,
            field=f"{field}.dependentType",
        )
    _project_parameter_passing(
        item,
        version=version,
        direction=direction,
        field=field,
    )
    return item


def _validate_dependent_date_epoch(
    item: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> None:
    """Validate one literal cycle/dateValue pair against its exact runtime switch."""
    for key in ("cycle", "dateValue"):
        value = item.get(key)
        if isinstance(value, str) and contains_ds_parameter_placeholder(value):
            message = f"DEPENDENT {key} does not support DS placeholders"
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="DEPENDENT",
                field=f"{field}.{key}",
                reason="parameter-placeholder-not-supported",
                message=message,
            )
    surface = get_task_authoring_surface(version).dependent
    values_by_cycle = dict(surface.date_values_by_cycle)
    cycle = item.get("cycle")
    if not isinstance(cycle, str) or cycle not in values_by_cycle:
        message = f"DEPENDENT cycle {cycle!r} is not supported"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.cycle",
            reason="unsupported-dependent-cycle",
            message=message,
        )
    date_value = item.get("dateValue")
    profile_values = frozenset(
        value for values in values_by_cycle.values() for value in values
    )
    if date_value in _DEPENDENT_MONTH_EXTENSION and date_value not in profile_values:
        message = f"DEPENDENT dateValue {date_value!r} is absent in {version}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.dateValue",
            reason="field-absent-in-version",
            message=message,
        )
    if not isinstance(date_value, str) or date_value not in profile_values:
        message = f"DEPENDENT dateValue {date_value!r} is not supported"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.dateValue",
            reason="unsupported-dependent-date-value",
            message=message,
        )
    if date_value not in values_by_cycle[cycle]:
        message = (
            f"DEPENDENT dateValue {date_value!r} does not belong to cycle {cycle!r}"
        )
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=f"{field}.dateValue",
            reason="dependent-date-cycle-mismatch",
            message=message,
        )


def _dependent_task_code(
    value: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"DEPENDENT requires an integer task selector in {field}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="invalid-dependent-task-code",
            message=message,
        )
    if value == -1:
        message = "The native all-tasks sentinel cannot be represented by typed YAML"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="unrepresentable-all-tasks-sentinel",
            message=message,
        )
    if value < 0:
        message = f"DEPENDENT requires depTaskCode >= 0 in {field}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="invalid-dependent-task-code",
            message=message,
        )
    return value


def _dependent_type_for_code(
    code: int,
    *,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> str:
    del version, direction, field
    return "DEPENDENT_ON_WORKFLOW" if code == 0 else "DEPENDENT_ON_TASK"


def _canonical_dependent_type(
    value: JsonValue,
    *,
    code: int,
    version: str,
    direction: ProjectionDirection,
    field: str,
) -> str:
    if not isinstance(value, str) or value not in {
        "DEPENDENT_ON_WORKFLOW",
        "DEPENDENT_ON_TASK",
    }:
        message = f"DEPENDENT requires a canonical dependentType in {field}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="invalid-dependent-type",
            message=message,
        )
    expected = "DEPENDENT_ON_WORKFLOW" if code == 0 else "DEPENDENT_ON_TASK"
    if value != expected:
        message = f"DEPENDENT {value} is inconsistent with depTaskCode {code}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="DEPENDENT",
            field=field,
            reason="dependent-type-code-mismatch",
            message=message,
        )
    return value
