from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsctl.upstream.parameter_semantics import ParameterSemanticsProfile


def switch_uses_local_params(version: str) -> bool:
    """SWITCH uses the prepared input map starting with exact DS 3.2.2."""
    return tuple(int(part) for part in version.split(".")) >= (3, 2, 2)


def switch_parameter_guidance(version: str) -> str:
    """Project the reviewed evaluator input boundary for schema and templates."""
    if switch_uses_local_params(version):
        return (
            "SWITCH expressions use the prepared parameter map, including "
            "task localParams; set route=A there or supply workflow "
            "globals/upstream outputs."
        )
    return (
        "SWITCH expressions read workflow globals and incoming runtime varPool, "
        "not this task's localParams. Set workflow.global_params.route: A in the "
        "enclosing workflow, or provide route as an upstream OUT with depends_on."
    )


def nested_workflow_parameter_rules(
    semantics: ParameterSemanticsProfile,
) -> list[str]:
    """Describe child inputs and outputs from the exact reviewed parameter record."""
    nested = semantics.nested_workflow
    version = semantics.version
    if version == "1.3.9":
        return [
            (
                "Canonical SUB_WORKFLOW childWorkflowName is resolved within the "
                "same project to native SUB_PROCESS processDefinitionId."
            ),
            (
                "SUB_PROCESS child defaults are filled by same-name parent "
                "workflow globals; parent values do not override an already "
                "declared child global."
            ),
            (
                "Canonical SUB_WORKFLOW does not author task localParams; native "
                "SUB_PROCESS localParams do not participate in child input passing."
            ),
        ]
    if nested.child_input_precedence == ("child-global", "parent-global"):
        return [
            (
                "SUB_PROCESS child globals are overridden by same-name parent "
                "workflow globals only when that name is selected in task "
                "localParams; other parent globals only fill names absent from "
                "the child."
            )
        ]
    if nested.task_local_params_role == "select-parent-global":
        output_rule = (
            "Task localParams OUT entries recover child values only when the "
            "nested task is cancelled; normal completion does not publish them."
            if nested.child_output_contract == "task-local-out-on-cancel"
            else "Child output parameters are not returned to the parent."
        )
        return [
            (
                "SUB_PROCESS child globals are overridden by same-name parent "
                "workflow globals."
            ),
            output_rule,
        ]
    if nested.task_type == "SUB_PROCESS":
        output_rule = (
            "task localParams OUT entries select child outputs returned to the parent."
            if nested.child_output_contract == "task-local-out"
            else "child workflow global OUT parameters are returned to the parent."
        )
        return [
            (
                "SUB_PROCESS receives parent workflow globals and the parent "
                "workflow-instance varPool; parent values override matching child "
                "global defaults."
            ),
            (
                "SUB_PROCESS localParams select parent task varPool inputs; "
                f"{output_rule}"
            ),
        ]
    if nested.child_input_sources == ("parent-startup",):
        return [
            (
                f"SUB_WORKFLOW receives only parent startup parameters in "
                f"DolphinScheduler {version}; upstream varPool and task "
                "localParams are not child inputs."
            )
        ]
    return [
        (
            "SUB_WORKFLOW receives parent workflow globals, startup parameters, "
            "and the parent workflow-instance varPool; matching child global "
            "defaults are overridden."
        ),
        (
            f"SUB_WORKFLOW localParams do not become child inputs in "
            f"DolphinScheduler {version}; set values on the parent workflow or "
            "pass them when starting it. Keep standalone defaults on the "
            "child workflow.global_params."
        ),
    ]
