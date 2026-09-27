from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _decode_task_ref,
    _encode_task_ref,
    _list_field,
    _mapping_sequence,
    _object_field,
    _projection_error,
    _reject_field,
)
from dsctl.upstream.task_references import (
    SWITCH_BRANCH,
    SWITCH_DEFAULT,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream.task_parameter_projection.types import (
        ProjectionDirection,
        TaskRefIndex,
    )

_STRING_NEXT_NODE_VERSIONS = frozenset(
    {"2.0.0", "2.0.1", "2.0.2", "2.0.3", "2.0.4", "2.0.5", "2.0.6", "2.0.7"}
)


def _try_project_opaque_modern_switch(
    payload: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    task_type: str,
    refs: TaskRefIndex,
) -> JsonObject | None:
    """Project representable modern SWITCH refs while retaining runtime evidence."""
    if (
        task_type != "SWITCH"
        or get_task_authoring_surface(version).nested_workflow.native_task_type
        != "SUB_WORKFLOW"
        or not {"nextBranch", "resultConditionLocation"}.intersection(payload)
    ):
        return None
    runtime_fields = {
        key: deepcopy(payload[key])
        for key in ("nextBranch", "resultConditionLocation")
        if key in payload
    }
    projected_input = {
        key: deepcopy(value)
        for key, value in payload.items()
        if key not in runtime_fields
    }
    projected = (
        _encode_switch(projected_input, version=version, refs=refs)
        if direction == "encode"
        else _decode_switch(projected_input, version=version, refs=refs)
    )
    if "nextBranch" in runtime_fields:
        projector = _encode_task_ref if direction == "encode" else _decode_task_ref
        projected["nextBranch"] = projector(
            runtime_fields["nextBranch"],
            version=version,
            task_type="SWITCH",
            field="task_params.nextBranch",
            refs=refs,
        )
    if "resultConditionLocation" in runtime_fields:
        projected["resultConditionLocation"] = runtime_fields["resultConditionLocation"]
    return projected


def _switch_has_name_reference(payload: JsonObject, *, refs: TaskRefIndex) -> bool:
    switch_result = payload.get("switchResult")
    if not isinstance(switch_result, Mapping):
        return False
    candidates: list[JsonValue] = [
        switch_result.get("nextNode"),
        payload.get("nextBranch"),
    ]
    branches = switch_result.get("dependTaskList")
    candidates.extend(
        branch.get("nextNode")
        for branch in _mapping_sequence(branches)
        if isinstance(branch, Mapping)
    )
    return any(
        isinstance(candidate, str)
        and (
            candidate.strip() in refs.code_by_name or not candidate.strip().isdecimal()
        )
        for candidate in candidates
        if candidate is not None
    )


def _encode_switch(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    _reject_field(
        payload,
        "resultConditionLocation",
        version=version,
        direction="encode",
        task_type="SWITCH",
        field="task_params.resultConditionLocation",
        reason="runtime-only",
    )
    _reject_field(
        payload,
        "nextBranch",
        version=version,
        direction="encode",
        task_type="SWITCH",
        field="task_params.nextBranch",
        reason="runtime-only",
    )
    switch_result = _object_field(
        payload,
        "switchResult",
        version=version,
        direction="encode",
        task_type="SWITCH",
        field="task_params.switchResult",
    )
    switch_result = _transform_switch_result(
        switch_result,
        version=version,
        direction="encode",
        refs=refs,
    )
    encoded = deepcopy(payload)
    encoded["switchResult"] = switch_result
    return encoded


def _decode_switch(
    payload: JsonObject,
    *,
    version: str,
    refs: TaskRefIndex,
) -> JsonObject:
    _reject_field(
        payload,
        "resultConditionLocation",
        version=version,
        direction="decode",
        task_type="SWITCH",
        field="task_params.resultConditionLocation",
        reason="runtime-only",
    )
    _reject_field(
        payload,
        "nextBranch",
        version=version,
        direction="decode",
        task_type="SWITCH",
        field="task_params.nextBranch",
        reason="runtime-only",
    )
    switch_result = _object_field(
        payload,
        "switchResult",
        version=version,
        direction="decode",
        task_type="SWITCH",
        field="task_params.switchResult",
    )
    switch_result = _transform_switch_result(
        switch_result,
        version=version,
        direction="decode",
        refs=refs,
    )
    decoded = deepcopy(payload)
    decoded["switchResult"] = switch_result
    return decoded


def _transform_switch_result(
    switch_result: JsonObject,
    *,
    version: str,
    direction: ProjectionDirection,
    refs: TaskRefIndex,
) -> JsonObject:
    branches = (
        _list_field(
            switch_result,
            "dependTaskList",
            version=version,
            direction=direction,
            task_type="SWITCH",
            field="task_params.switchResult.dependTaskList",
        )
        if "dependTaskList" in switch_result
        else []
    )
    projected_branches: list[JsonValue] = []
    for index, raw_branch in enumerate(branches):
        field_prefix = f"task_params.switchResult.dependTaskList[{index}]"
        if not isinstance(raw_branch, Mapping):
            message = f"SWITCH requires {field_prefix} to be a JSON object"
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="SWITCH",
                field=field_prefix,
                reason="invalid-object-shape",
                message=message,
            )
        branch = deepcopy(dict(raw_branch))
        ref_field = SWITCH_BRANCH.field(index)
        ref_key = SWITCH_BRANCH.key()
        if ref_key not in branch:
            message = f"SWITCH requires one branch target in {ref_field}"
            raise _projection_error(
                version=version,
                direction=direction,
                task_type="SWITCH",
                field=ref_field,
                reason="missing-task-reference",
                message=message,
            )
        if direction == "encode":
            code = _encode_task_ref(
                branch[ref_key],
                version=version,
                task_type="SWITCH",
                field=ref_field,
                refs=refs,
            )
            branch[ref_key] = (
                str(code) if version in _STRING_NEXT_NODE_VERSIONS else code
            )
        else:
            branch[ref_key] = _decode_task_ref(
                branch[ref_key],
                version=version,
                task_type="SWITCH",
                field=ref_field,
                refs=refs,
            )
        projected_branches.append(branch)
    projected = deepcopy(switch_result)
    projected["dependTaskList"] = projected_branches
    ref_key = SWITCH_DEFAULT.key()
    if ref_key in projected:
        field = SWITCH_DEFAULT.field()
        if direction == "encode":
            code = _encode_task_ref(
                projected[ref_key],
                version=version,
                task_type="SWITCH",
                field=field,
                refs=refs,
            )
            projected[ref_key] = (
                str(code) if version in _STRING_NEXT_NODE_VERSIONS else code
            )
        else:
            projected[ref_key] = _decode_task_ref(
                projected[ref_key],
                version=version,
                task_type="SWITCH",
                field=field,
                refs=refs,
            )
    return projected
