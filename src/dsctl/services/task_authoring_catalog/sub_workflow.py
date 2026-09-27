from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject


def _validate_sub_workflow_identity_fields(
    task_params: YamlObject,
    *,
    profile_version: str,
) -> None:
    """Keep the 1.3.9 database id behind a same-project name selector."""
    has_name = "childWorkflowName" in task_params
    has_code = "workflowDefinitionCode" in task_params
    if profile_version == "1.3.9":
        unsupported = sorted(
            {
                "workflowDefinitionCode",
                "localParams",
                "resourceList",
                "varPool",
            }.intersection(task_params)
        )
        if unsupported:
            fields = ", ".join(unsupported)
            message = (
                "SUB_WORKFLOW 1.3.9 typed authoring owns only childWorkflowName; "
                f"unsupported fields: {fields}"
            )
            raise ValueError(message)
        if not has_name:
            message = (
                "SUB_WORKFLOW 1.3.9 requires childWorkflowName so the service can "
                "resolve the exact same-project processDefinitionId"
            )
            raise ValueError(message)
        return
    if has_name or not has_code:
        message = (
            f"SUB_WORKFLOW {profile_version} requires workflowDefinitionCode; "
            "childWorkflowName is currently the exact 1.3.9 identity selector"
        )
        raise ValueError(message)
