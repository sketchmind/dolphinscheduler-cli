from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.models.procedure_call import ProcedureCall
from dsctl.upstream.task_authoring_surface import (
    get_task_authoring_surface,
)
from dsctl.upstream.task_parameter_projection.emr import (
    _emr_mode_json_fields,
)
from dsctl.upstream.task_parameter_projection.shared import (
    _LEGACY_VERSIONS,
    _nested_items_have_any_key,
    _nested_items_have_key,
    _nested_items_have_value,
)
from dsctl.upstream.task_parameter_projection.switch import (
    _switch_has_name_reference,
)

if TYPE_CHECKING:
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.task_parameter_projection.types import (
        TaskRefIndex,
    )


def _looks_canonical(
    *,
    version: str,
    task_type: str,
    payload: JsonObject,
    refs: TaskRefIndex,
) -> bool:
    """Recognize authored markers without guessing opaque native state."""
    if task_type == "CONDITIONS":
        return _nested_items_have_key(payload, "task")
    if task_type == "SWITCH":
        return not {"nextBranch", "resultConditionLocation"}.intersection(
            payload
        ) and _switch_has_name_reference(payload, refs=refs)
    if task_type == "SUB_WORKFLOW":
        return version in _LEGACY_VERSIONS or "workflowDefinitionCode" in payload
    if task_type == "DEPENDENT":
        return (
            _nested_items_have_key(payload, "dependentType")
            and not _nested_items_have_any_key(payload, {"dependResult", "status"})
            and not _nested_items_have_value(payload, "depTaskCode", -1)
        )
    if task_type == "EMR":
        surface = get_task_authoring_surface(version).emr
        program_type = payload.get("programType")
        if not surface.available or program_type not in surface.program_types:
            return False
        active_field, inactive_field = _emr_mode_json_fields(program_type)
        return active_field in payload and inactive_field not in payload
    if task_type == "HTTP":
        return version in _LEGACY_VERSIONS and "socketTimeout" not in payload
    if task_type == "PROCEDURE":
        method = payload.get("method")
        if not isinstance(method, str):
            return False
        call = ProcedureCall.parse(method)
        return call is not None and call.uses_positional_placeholders
    return False
