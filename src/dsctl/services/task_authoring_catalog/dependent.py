from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from dsctl.models.task_spec import (
    Dependent139TaskParamsSpec,
    DependentTaskParamsSpec,
)
from dsctl.upstream.task_authoring_surface import (
    DependentAuthoringSurface,
    get_task_authoring_surface,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject

from dsctl.services.task_authoring_catalog.templates import (
    task_template_with_runtime_controls,
)
from dsctl.services.task_authoring_catalog.types import (
    TaskAuthoringFacetContract,
    TaskAuthoringFacetMembership,
    TaskAuthoringField,
    TaskAuthoringStateRule,
    TaskAuthoringTemplate,
    TaskTypeAuthoringProfile,
    model_field,
)

DEPENDENT_DEPENDENCY_FACET = "DEPENDENT/dependency"


def _dependent_fields(
    surface: DependentAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    """Describe one exact name-routed or code-routed DEPENDENT contract."""
    dependence_path = (
        "processDefinitionJson.tasks[].dependence"
        if surface.identity_wire == "legacy-name-to-id"
        else "taskDefinitionJson[].taskParams.dependence"
    )
    item_path = f"{dependence_path}.dependTaskList[].dependItemList[]"
    fields: list[TaskAuthoringField] = [
        model_field(
            "task_params.dependence.relation",
            compile_path=f"{dependence_path}.relation",
            description="Top-level dependency relation.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[]",
            compile_path=f"{dependence_path}.dependTaskList",
            description="One or more nonempty dependency branch groups.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].relation",
            compile_path=f"{dependence_path}.dependTaskList[].relation",
            description="Relation inside one dependency branch group.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[]",
            compile_path=f"{dependence_path}.dependTaskList[].dependItemList",
            description="One exact upstream workflow or task dependency.",
        ),
        model_field(
            ("task_params.dependence.dependTaskList[].dependItemList[].dependentType"),
            description="Whether the item targets a whole workflow or one task.",
        ),
    ]
    if surface.identity_wire == "legacy-name-to-id":
        fields.extend(
            (
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "projectName"
                    ),
                    choice_source="dsctl project list",
                    related_commands=("dsctl project list",),
                    compile_path=f"{item_path}.projectId",
                    description=(
                        "Literal project name resolved to compiler-owned projectId; "
                        "the 1.3.9 executor itself does not consume projectId."
                    ),
                ),
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "workflowName"
                    ),
                    choice_source="dsctl workflow list --project PROJECT",
                    related_commands=(
                        "dsctl workflow list --project PROJECT",
                        "dsctl workflow get WORKFLOW --project PROJECT",
                    ),
                    compile_path=f"{item_path}.definitionId",
                    description=(
                        "Literal workflow name resolved to the exact 1.3.9 "
                        "process-definition database ID."
                    ),
                ),
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "taskName"
                    ),
                    active_when="dependentType == DEPENDENT_ON_TASK",
                    choice_source=(
                        "dsctl task list --project PROJECT --workflow WORKFLOW"
                    ),
                    related_commands=(
                        "dsctl task list --project PROJECT --workflow WORKFLOW",
                    ),
                    compile_path=f"{item_path}.depTasks",
                    description=(
                        "Literal historical task name. Omit for a workflow target; "
                        "the compiler then sends the reserved ALL sentinel."
                    ),
                ),
            )
        )
    else:
        fields.extend(
            (
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "projectCode"
                    ),
                    choice_source="dsctl project list",
                    related_commands=(
                        "dsctl project list",
                        "dsctl project get PROJECT",
                    ),
                    description="Upstream project code.",
                ),
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "definitionCode"
                    ),
                    choice_source="dsctl workflow list --project PROJECT",
                    related_commands=(
                        "dsctl workflow list --project PROJECT",
                        "dsctl workflow get WORKFLOW --project PROJECT",
                    ),
                    description="Upstream workflow definition code.",
                ),
                model_field(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[]."
                        "depTaskCode"
                    ),
                    choice_source=(
                        "dsctl task list --project PROJECT --workflow WORKFLOW"
                    ),
                    related_commands=(
                        "dsctl task list --project PROJECT --workflow WORKFLOW",
                    ),
                    description="Upstream task code, or 0 for the whole workflow.",
                ),
            )
        )
    fields.extend(
        (
            model_field(
                ("task_params.dependence.dependTaskList[].dependItemList[].cycle"),
                compile_path=f"{item_path}.cycle",
                description=(
                    "Dependency cycle label; dsctl also enforces its exact "
                    "dateValue branch although upstream runtime keys only on it."
                ),
            ),
            model_field(
                ("task_params.dependence.dependTaskList[].dependItemList[].dateValue"),
                "enum",
                choices=tuple(
                    dict.fromkeys(
                        value
                        for _cycle, values in surface.date_values_by_cycle
                        for value in values
                    )
                ),
                active_when="valid values depend on cycle; see state_rules",
                compile_path=f"{item_path}.dateValue",
                description="Exact runtime date-window selector.",
            ),
        )
    )
    if surface.parameter_passing:
        fields.append(
            model_field(
                (
                    "task_params.dependence.dependTaskList[].dependItemList[]."
                    "parameterPassing"
                ),
                default=False,
                compile_path=f"{item_path}.parameterPassing",
                description=(
                    "Pass the successful target workflow's varPool; this does not "
                    "substitute identity, cycle, or dateValue fields."
                ),
            )
        )
    if surface.failure_control:
        fields.extend(
            (
                model_field(
                    "task_params.dependence.checkInterval",
                    default=10,
                    compile_path=f"{dependence_path}.checkInterval",
                    description="Dependency polling interval in seconds.",
                ),
                model_field(
                    "task_params.dependence.failurePolicy",
                    compile_path=f"{dependence_path}.failurePolicy",
                    description="Failure behavior while waiting on dependencies.",
                ),
                model_field(
                    "task_params.dependence.failureWaitingTime",
                    active_when=("failurePolicy == DEPENDENT_FAILURE_WAITING"),
                    compile_path=f"{dependence_path}.failureWaitingTime",
                    description="Waiting duration used by the waiting failure policy.",
                ),
            )
        )
    return tuple(fields)


def _dependent_state_rules(
    surface: DependentAuthoringSurface,
) -> tuple[TaskAuthoringStateRule, ...]:
    """Expose target and exact date-window invariants to LLM-facing discovery."""
    item_prefix = "task_params.dependence.dependTaskList[].dependItemList[]"
    rules = [
        TaskAuthoringStateRule(
            when=f"dependItem.cycle == {cycle}",
            condition_paths=(f"{item_prefix}.cycle",),
            active_paths=(f"{item_prefix}.dateValue",),
            compile_policy=(("dateValue choices", ", ".join(values)),),
            description=f"Exact {cycle} dependency windows for this release.",
        )
        for cycle, values in surface.date_values_by_cycle
    ]
    if surface.identity_wire == "legacy-name-to-id":
        rules.extend(
            (
                TaskAuthoringStateRule(
                    when="dependItem.dependentType == DEPENDENT_ON_WORKFLOW",
                    condition_paths=(f"{item_prefix}.dependentType",),
                    active_paths=(
                        f"{item_prefix}.projectName",
                        f"{item_prefix}.workflowName",
                    ),
                    inactive_paths=(f"{item_prefix}.taskName",),
                    compile_policy=(("depTasks", "send compiler-owned ALL"),),
                    description="Resolve one whole-workflow dependency by names.",
                ),
                TaskAuthoringStateRule(
                    when="dependItem.dependentType == DEPENDENT_ON_TASK",
                    condition_paths=(f"{item_prefix}.dependentType",),
                    active_paths=(
                        f"{item_prefix}.projectName",
                        f"{item_prefix}.workflowName",
                        f"{item_prefix}.taskName",
                    ),
                    compile_policy=(("depTasks", "send exact literal taskName"),),
                    description="Resolve and verify one historical task name.",
                ),
            )
        )
    return tuple(rules)


def _dependent_runtime_guidance(surface: DependentAuthoringSurface) -> str:
    identity = (
        "Names are resolved online to exact 1.3.9 database IDs before mutation. "
        if surface.identity_wire == "legacy-name-to-id"
        else "Project, workflow, and task identities use exact native codes. "
    )
    return (
        f"{identity}DEPENDENT runs on the master and reads historical workflow/task "
        "state; identity, cycle, and dateValue are literal and never placeholder-"
        "substituted. It has no durable remote app id or worker resume target. "
        "Scheduled parents anchor windows to scheduleTime; manual retry or master "
        "reinitialization can anchor them to a later current time."
    )


def _dependent_templates(
    surface: DependentAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _dependent_runtime_guidance(surface)
    comments = f"# Runtime semantics: {guidance}\n"
    if surface.identity_wire == "legacy-name-to-id":
        workflow_identity = """            projectName: upstream-project
            workflowName: upstream-daily
"""
        task_identity = workflow_identity + "            taskName: extract\n"
    else:
        workflow_identity = """            projectCode: 1
            definitionCode: 1000000000001
            depTaskCode: 0
"""
        task_identity = """            projectCode: 1
            definitionCode: 1000000000001
            depTaskCode: 1000000000002
"""
    failure_controls = (
        "    checkInterval: 10\n    failurePolicy: DEPENDENT_FAILURE_FAILURE\n"
        if surface.failure_control
        else ""
    )
    parameter_passing = (
        "            parameterPassing: false\n" if surface.parameter_passing else ""
    )

    def render(*, task_target: bool) -> str:
        dependent_type = "DEPENDENT_ON_TASK" if task_target else "DEPENDENT_ON_WORKFLOW"
        identity = task_identity if task_target else workflow_identity
        body = f"""# Task template for an exact DEPENDENT target
name: dependent-task
type: DEPENDENT
description: Wait for an upstream {"task" if task_target else "workflow"}
task_params:
  dependence:
    relation: AND
{failure_controls}    dependTaskList:
      - relation: AND
        dependItemList:
          - dependentType: {dependent_type}
{identity}            cycle: day
            dateValue: last1Days
{parameter_passing}worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
        return f"{comments}{task_template_with_runtime_controls(body)}"

    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Wait for one exact upstream workflow dependency.",
            yaml=render(task_target=False),
            payload_modes=("task_params",),
        ),
        TaskAuthoringTemplate(
            name="task-dependency",
            summary=(
                "Wait for one exact task name in an upstream workflow."
                if surface.identity_wire == "legacy-name-to-id"
                else "Wait for one exact task code in an upstream workflow."
            ),
            yaml=render(task_target=True),
            payload_modes=("task_params",),
        ),
    )


def _dependent_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    """Materialize DEPENDENT so identity and date epochs stay in one deep seam."""
    surface = get_task_authoring_surface(profile_version).dependent
    legacy = surface.identity_wire == "legacy-name-to-id"
    contract = TaskAuthoringFacetContract(
        facet_id=DEPENDENT_DEPENDENCY_FACET,
        family=(
            "dependent-name-routed-legacy-v1" if legacy else "dependent-code-routed-v1"
        ),
        review="dependent-exact-identity-and-date-window-subset",
        params_model=(
            Dependent139TaskParamsSpec if legacy else DependentTaskParamsSpec
        ),
        fields=_dependent_fields(surface),
        state_rules=_dependent_state_rules(surface),
        templates=_dependent_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=not legacy,
        opaque_edit=not legacy,
        opaque_preserve=True,
        constraint=(
            "Exact 1.3.9 outer TaskNode.dependence can only be authored through "
            "literal names resolved by the service."
            if legacy
            else None
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="DEPENDENT",
        category="Logic",
        kind="typed",
        default_facet=DEPENDENT_DEPENDENCY_FACET,
        facets={DEPENDENT_DEPENDENCY_FACET: membership},
    )


def validate_semantics(
    task_params: YamlObject,
    *,
    version: str,
    surface: DependentAuthoringSurface,
) -> None:
    """Require the exact runtime dateValue table selected by this release."""
    dependence = task_params.get("dependence")
    if not isinstance(dependence, Mapping):
        return
    groups = dependence.get("dependTaskList")
    if not isinstance(groups, Sequence) or isinstance(
        groups,
        (bytes, bytearray, str),
    ):
        return
    for group_index, raw_group in enumerate(groups):
        if not isinstance(raw_group, Mapping):
            continue
        items = raw_group.get("dependItemList")
        if not isinstance(items, Sequence) or isinstance(
            items,
            (bytes, bytearray, str),
        ):
            continue
        for item_index, raw_item in enumerate(items):
            if not isinstance(raw_item, Mapping):
                continue
            cycle = raw_item.get("cycle")
            date_value = raw_item.get("dateValue")
            allowed = surface.date_values(cycle) if isinstance(cycle, str) else ()
            if isinstance(date_value, str) and date_value in allowed:
                continue
            choices = ", ".join(allowed)
            message = (
                "DEPENDENT dateValue "
                f"{date_value!r} is unsupported for cycle {cycle!r} on "
                f"DolphinScheduler {version}; expected one of: "
                f"{choices} (item {group_index}:{item_index})"
            )
            raise ValueError(message)
