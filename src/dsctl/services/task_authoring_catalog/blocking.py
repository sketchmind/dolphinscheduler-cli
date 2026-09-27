from __future__ import annotations

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
    _family_model,
    model_field,
)
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

BLOCKING_SAME_WORKFLOW_STATE_GATE_FACET = "BLOCKING/same_workflow_state_gate"


def _blocking_runtime_guidance(profile_version: str) -> str:
    """Describe the exact master-local gate and its standby-task epoch."""
    surface = get_task_authoring_surface(profile_version).blocking
    if not surface.available:
        return "BLOCKING is absent from this exact DolphinScheduler profile."
    standby_transition = (
        "marks not-yet-submitted tasks KILL"
        if surface.standby_transition == "KILL"
        else "marks not-yet-submitted tasks PAUSE"
    )
    cancel_behavior = (
        "Its task-specific pause/kill hook changes only the local task state"
        if surface.pause_kill_behavior == "task-local-state"
        else (
            "Its inherited pause/kill hook warns and performs no task-specific action"
        )
    )
    return (
        "The reviewed exact upstream releases expose no BLOCKING authoring form; "
        "this reviewed typed surface is REST-only and grounded in parameter, "
        "runtime, and programmatic-test evidence. The master evaluates final "
        "states of same-workflow task codes. The BLOCKING task itself completes "
        "SUCCESS. A matching gate moves the workflow through READY_BLOCK to "
        "terminal BLOCK; a nonmatching gate lets the workflow continue and does "
        "not trigger task retry. READY_BLOCK normally waits for active and retry "
        f"work to drain before the final transition, which {standby_transition}. "
        "alertWhenBlocking only asks the master to add a blocking alert record "
        "for the workflow warningGroupId. Delivery requires a valid configured "
        "alert group and alert infrastructure; this facet does not validate "
        "either. Infrastructure replay or workflow rerun may re-evaluate the gate "
        "and repeat the alert. Upstream logs task codes, expected and actual "
        "states, aggregate results, and the blocking opportunity at INFO; task "
        "fields are not secret storage. The task is master-local and has no worker "
        "resource, datasource, structured output, durable application id, or "
        f"dedicated failover resume. {cancel_behavior}; it has no remote "
        "cancellation target."
    )


def _blocking_fields(profile_version: str) -> tuple[TaskAuthoringField, ...]:
    runtime = _blocking_runtime_guidance(profile_version)
    return (
        model_field(
            "task_params.blockingOpportunity",
            compile_path="taskDefinitionJson[].taskParams.blockingOpportunity",
            description=f"Aggregate state that triggers workflow blocking. {runtime}",
        ),
        model_field(
            "task_params.alertWhenBlocking",
            model_default=True,
            compile_path="taskDefinitionJson[].taskParams.alertWhenBlocking",
            description=(
                "Strict boolean selecting the optional blocking alert; it does not "
                "change the gate decision."
            ),
        ),
        model_field(
            "task_params.dependence.relation",
            compile_path="taskDefinitionJson[].taskParams.dependence.relation",
            description="Top-level relation across same-workflow predicate groups.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[]",
            description="One nonempty group of same-workflow state predicates.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].relation",
            compile_path=(
                "taskDefinitionJson[].taskParams.dependence.dependTaskList[].relation"
            ),
            description="Relation across predicates inside this group.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[]",
            description="One expected final state for a local predecessor task.",
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[].task",
            choice_source="other tasks in the same workflow YAML",
            compile_path=(
                "taskDefinitionJson[].taskParams.dependence.dependTaskList[]."
                "dependItemList[].depTaskCode"
            ),
            description=(
                "Literal same-workflow task name; compiled to a positive code and "
                "added as a real DAG predecessor."
            ),
        ),
        model_field(
            "task_params.dependence.dependTaskList[].dependItemList[].status",
            compile_path=(
                "taskDefinitionJson[].taskParams.dependence.dependTaskList[]."
                "dependItemList[].status"
            ),
            description="Expected final state of the referenced local task.",
        ),
    )


def _blocking_templates(profile_version: str) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _blocking_runtime_guidance(profile_version)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Block the workflow when one local predecessor succeeds.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime behavior: {guidance}\n"
                "# Richer inherited or future native state survives only through "
                "an unchanged existing-server baseline. Standalone export carries "
                "raw opaque evidence but cannot be reapplied through the closed raw "
                "opaque create/edit path.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one same-workflow blocking gate
name: block-on-upstream-success
type: BLOCKING
description: End the workflow as BLOCK when upstream succeeds
task_params:
  blockingOpportunity: BlockingOnSuccess
  alertWhenBlocking: false
  dependence:
    relation: AND
    dependTaskList:
      - relation: AND
        dependItemList:
          - task: upstream
            status: SUCCESS
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0  # Minutes; disabled. Set a deliberate timeout and WARN/FAILED strategy.
"""
                )
            ),
        ),
    )


def _blocking_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    if not get_task_authoring_surface(profile_version).blocking.available:
        message = f"BLOCKING is absent from DolphinScheduler {profile_version}"
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=BLOCKING_SAME_WORKFLOW_STATE_GATE_FACET,
        family="blocking-same-workflow-state-gate-v1",
        review="blocking-same-workflow-state-gate-exact-subset",
        params_model=_family_model("BLOCKING"),
        fields=_blocking_fields(profile_version),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed BLOCKING same-workflow state-gate authoring",
                condition_paths=(),
                active_paths=(
                    "task_params.blockingOpportunity",
                    "task_params.alertWhenBlocking",
                    "task_params.dependence",
                ),
                compile_policy=(
                    (
                        "task_params.dependence.dependTaskList[].dependItemList[].task",
                        "resolve to depTaskCode and add a DAG predecessor edge",
                    ),
                    (
                        "task_params.alertWhenBlocking",
                        "send strict boolean false by default",
                    ),
                ),
                description=(
                    "The compiler owns the complete closed native taskParams wire; "
                    "all richer state remains preservation-only."
                ),
            ),
        ),
        templates=_blocking_templates(profile_version),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
    )
    return TaskTypeAuthoringProfile(
        task_type="BLOCKING",
        category="Logic",
        kind="typed",
        default_facet=BLOCKING_SAME_WORKFLOW_STATE_GATE_FACET,
        facets={BLOCKING_SAME_WORKFLOW_STATE_GATE_FACET: membership},
    )
