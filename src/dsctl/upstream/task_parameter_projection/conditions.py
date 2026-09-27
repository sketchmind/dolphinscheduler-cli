from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.upstream.task_parameter_projection.shared import (
    _CONDITION_STRING_RESULT_VERSIONS,
    _decode_task_ref,
    _encode_task_ref,
    _list_field,
    _object_field,
    _projection_error,
    _reject_field,
)
from dsctl.upstream.task_references import (
    CONDITION_RESULTS,
    PREDICATE_TASK,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskRefIndex,
    )


def _encode_conditions(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    dependence = _conditions_dependence(
        payload,
        version=version,
        direction="encode",
        refs=refs,
    )
    condition_result = _conditions_result(
        payload,
        version=version,
        direction="encode",
        refs=refs,
    )
    encoded = deepcopy(payload)
    encoded["dependence"] = dependence
    encoded["conditionResult"] = condition_result
    return encoded


def _decode_conditions(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    dependence = _conditions_dependence(
        payload,
        version=version,
        direction="decode",
        refs=refs,
    )
    condition_result = _conditions_result(
        payload,
        version=version,
        direction="decode",
        refs=refs,
    )
    decoded = deepcopy(payload)
    decoded["dependence"] = dependence
    decoded["conditionResult"] = condition_result
    return decoded


def _conditions_dependence(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
) -> JsonObject:
    _reject_field(
        payload,
        "conditionSuccess",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.conditionSuccess",
        reason="runtime-only",
    )
    dependence = _object_field(
        payload,
        "dependence",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.dependence",
    )
    _reject_field(
        dependence,
        "conditionSuccess",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.dependence.conditionSuccess",
        reason="runtime-only",
    )
    groups = _list_field(
        dependence,
        "dependTaskList",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.dependence.dependTaskList",
    )
    projected_groups: list[JsonValue] = []
    for group_index, raw_group in enumerate(groups):
        group_field = f"task_params.dependence.dependTaskList[{group_index}]"
        if not isinstance(raw_group, Mapping):
            message = f"CONDITIONS requires {group_field} to be a JSON object"
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="CONDITIONS",
                field=group_field,
                reason="invalid-object-shape",
                message=message,
            )
        group = deepcopy(dict(raw_group))
        item_list_field = f"{group_field}.dependItemList"
        items = _list_field(
            group,
            "dependItemList",
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=item_list_field,
        )
        projected_items: list[JsonValue] = []
        for item_index, raw_item in enumerate(items):
            item_field = f"{item_list_field}[{item_index}]"
            projected_items.append(
                _condition_predicate(
                    raw_item,
                    version=version,
                    direction=direction,
                    refs=refs,
                    field=item_field,
                )
            )
        group["dependItemList"] = projected_items
        projected_groups.append(group)
    dependence["dependTaskList"] = projected_groups
    return dependence


def _condition_predicate(
    raw_item: JsonValue,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
    field: str,
) -> JsonObject:
    if not isinstance(raw_item, Mapping):
        message = f"CONDITIONS requires {field} to be a JSON object"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=field,
            reason="invalid-object-shape",
            message=message,
        )
    item = deepcopy(dict(raw_item))
    status = item.get("status")
    if status not in {"SUCCESS", "FAILURE"}:
        message = f"CONDITIONS supports only SUCCESS or FAILURE in {field}.status"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=f"{field}.status",
            reason="unsupported-condition-status",
            message=message,
        )
    ref_key = PREDICATE_TASK.key(native=direction == "decode")
    allowed = {ref_key, "status"}
    unexpected = sorted(set(item).difference(allowed))
    if unexpected:
        names = ", ".join(unexpected)
        message = f"CONDITIONS predicate {field} has unsupported fields: {names}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=field,
            reason="unsupported-predicate-field",
            message=message,
        )
    if ref_key not in item:
        message = f"CONDITIONS requires {field}.{ref_key}"
        raise _projection_error(
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=f"{field}.{ref_key}",
            reason="missing-task-reference",
            message=message,
        )
    if direction == "encode":
        return {
            PREDICATE_TASK.key(native=True): _encode_task_ref(
                item[ref_key],
                version=version,
                task_type="CONDITIONS",
                field=f"{field}.{ref_key}",
                refs=refs,
            ),
            "status": status,
        }
    return {
        PREDICATE_TASK.key(): _decode_task_ref(
            item[ref_key],
            version=version,
            task_type="CONDITIONS",
            field=f"{field}.{ref_key}",
            refs=refs,
        ),
        "status": status,
    }


def _conditions_result(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
) -> JsonObject:
    result = _object_field(
        payload,
        "conditionResult",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.conditionResult",
    )
    _reject_field(
        result,
        "conditionSuccess",
        version=version,
        direction=direction,
        task_type="CONDITIONS",
        field="task_params.conditionResult.conditionSuccess",
        reason="runtime-only",
    )
    for reference_path in CONDITION_RESULTS:
        outcome = reference_path.key()
        nodes = _list_field(
            result,
            outcome,
            version=version,
            direction=direction,
            task_type="CONDITIONS",
            field=f"task_params.conditionResult.{outcome}",
        )
        projected: list[JsonValue] = []
        for index, node in enumerate(nodes):
            field = reference_path.field(index)
            if direction == "encode":
                code = _encode_task_ref(
                    node,
                    version=version,
                    task_type="CONDITIONS",
                    field=field,
                    refs=refs,
                )
                projected.append(
                    str(code) if version in _CONDITION_STRING_RESULT_VERSIONS else code
                )
            else:
                projected.append(
                    _decode_task_ref(
                        node,
                        version=version,
                        task_type="CONDITIONS",
                        field=field,
                        refs=refs,
                    )
                )
        result[outcome] = projected
    return result
