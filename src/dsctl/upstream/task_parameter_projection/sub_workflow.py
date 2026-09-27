from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _drop_empty_compatibility_list,
    _projection_error,
    _reject_field,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject


def _encode_sub_workflow(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
) -> tuple[str, JsonObject]:
    if task_type != "SUB_WORKFLOW":
        message = (
            "Typed nested-workflow authoring requires the canonical SUB_WORKFLOW type"
        )
        raise _projection_error(
            version=version,
            direction="encode",
            task_type=task_type,
            field="task.type",
            reason="expected-canonical-task-type",
            message=message,
        )
    encoded = deepcopy(payload)
    _drop_empty_compatibility_list(
        encoded,
        key="resourceList",
        version=version,
        direction="encode",
        task_type="SUB_WORKFLOW",
    )
    _reject_field(
        encoded,
        "processDefinitionCode",
        version=version,
        direction="encode",
        task_type="SUB_WORKFLOW",
        field="task_params.processDefinitionCode",
        reason="native-only-field",
    )
    surface = get_task_authoring_surface(version).nested_workflow
    if surface.native_task_type == "SUB_PROCESS":
        if "workflowDefinitionCode" not in encoded:
            message = "SUB_WORKFLOW requires task_params.workflowDefinitionCode"
            raise _projection_error(
                version=version,
                direction="encode",
                task_type="SUB_WORKFLOW",
                field="task_params.workflowDefinitionCode",
                reason="missing-workflow-reference",
                message=message,
            )
        encoded[surface.native_code_field] = encoded.pop("workflowDefinitionCode")
        return surface.native_task_type, encoded
    return "SUB_WORKFLOW", encoded


def _decode_sub_workflow(
    payload: JsonObject,
    *,
    version: str,
    task_type: str,
) -> tuple[str, JsonObject]:
    decoded = deepcopy(payload)
    _drop_empty_compatibility_list(
        decoded,
        key="resourceList",
        version=version,
        direction="decode",
        task_type=task_type,
    )
    surface = get_task_authoring_surface(version).nested_workflow
    if surface.native_task_type == "SUB_PROCESS":
        _reject_field(
            decoded,
            "workflowDefinitionCode",
            version=version,
            direction="decode",
            task_type=task_type,
            field="task_params.workflowDefinitionCode",
            reason="field-absent-in-version",
        )
        if task_type != "SUB_PROCESS" or "processDefinitionCode" not in decoded:
            message = (
                "Legacy nested-workflow wire requires SUB_PROCESS and "
                "task_params.processDefinitionCode"
            )
            raise _projection_error(
                version=version,
                direction="decode",
                task_type=task_type,
                field="task_params.processDefinitionCode",
                reason="invalid-native-sub-process-shape",
                message=message,
            )
        decoded["workflowDefinitionCode"] = decoded.pop("processDefinitionCode")
        return "SUB_WORKFLOW", decoded
    _reject_field(
        decoded,
        "processDefinitionCode",
        version=version,
        direction="decode",
        task_type=task_type,
        field="task_params.processDefinitionCode",
        reason="field-absent-in-version",
    )
    if task_type != "SUB_WORKFLOW":
        message = f"{task_type} is not the modern nested-workflow task type"
        raise _projection_error(
            version=version,
            direction="decode",
            task_type=task_type,
            field="task.type",
            reason="invalid-native-sub-workflow-type",
            message=message,
        )
    return "SUB_WORKFLOW", decoded
