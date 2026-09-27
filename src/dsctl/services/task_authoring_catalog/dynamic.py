from __future__ import annotations

from typing import TYPE_CHECKING

from dsctl.upstream.task_authoring_surface import get_task_authoring_surface

if TYPE_CHECKING:
    from dsctl.upstream.task_authoring_surface import (
        DynamicAuthoringSurface,
    )

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

DYNAMIC_LITERAL_SINGLE_DIMENSION_FANOUT_FACET = (
    "DYNAMIC/literal_single_dimension_fanout"
)
_DYNAMIC_RUNTIME_EXCLUSION_FACET = "DYNAMIC/runtime_exclusion"


def _dynamic_runtime_guidance(surface: DynamicAuthoringSurface) -> str:
    """Describe the fixed 3.2.2 fan-out and its durable-state boundaries."""
    if not surface.typed_available:
        reason = surface.exclusion_reason or "upstream-absent"
        return f"DYNAMIC typed execution is unavailable: {reason}."
    if (
        not surface.registered
        or not surface.child_tenant_forwarded
        or not surface.child_start_param_guarded
        or not surface.pending_children_cancelled
        or not surface.persisted_child_relations
        or not surface.workflow_recovery_reuses_child_instances
        or surface.ordinary_task_retry_resets_failed_children
        or not surface.failover_recovery_supported
        or not surface.cancellation_best_effort
        or surface.pause_cascades
        or surface.schedule_context_forwarded
    ):
        message = "Available DYNAMIC surface lacks required 3.2.2 runtime fixes"
        raise ValueError(message)
    return (
        "Before live create/edit, dsctl resolves childWorkflowName in the selected "
        "project and proves that its reachable DYNAMIC/SUB_PROCESS closure is "
        "non-recursive. The compiler emits one literal dimension, fixes an empty "
        "filter, uses comma splitting, and sets the native maximum to the exact value "
        "count. It rejects parent-global collisions and retry.times other than 0; "
        "runtime start parameters must also avoid parameterName because parent values "
        "win. Upstream does not forward the parent's schedule time or timezone. INFO "
        "logs every generated group and dynamic.out(taskName), including child "
        "varPool and definition globals, so values are not secret storage. Exact "
        "3.2.2 persists relations for workflow recovery/failover, but ordinary task "
        "retry does not reset failed children and partial materialization can orphan "
        "or duplicate child work. Cancellation is best-effort for running/queued "
        "children; pause does not cascade. Child workflow runtime, permissions, "
        "resources, datasources, and post-authoring graph drift remain operator "
        "prerequisites."
    )


def _dynamic_fields(
    surface: DynamicAuthoringSurface,
) -> tuple[TaskAuthoringField, ...]:
    guidance = _dynamic_runtime_guidance(surface)
    return (
        model_field(
            "task_params.childWorkflowName",
            choice_source="dsctl workflow list",
            related_commands=(
                "dsctl workflow list --project PROJECT",
                "dsctl workflow get WORKFLOW --project PROJECT",
            ),
            compile_path="taskDefinitionJson[].taskParams.processDefinitionCode",
            description="Literal same-project child workflow name. " + guidance,
        ),
        model_field(
            "task_params.parameterName",
            compile_path="taskDefinitionJson[].taskParams.listParameters[0].name",
            description=(
                "Literal startup-parameter key outside the reserved system.* "
                "namespace for every generated child workflow. " + guidance
            ),
        ),
        model_field(
            "task_params.values",
            compile_path=("taskDefinitionJson[].taskParams.listParameters[0].value"),
            description=(
                "One to 1,024 ordered unique literal values defining the bounded "
                "fan-out; their comma-joined native representation must not exceed "
                "256 UTF-16 code units."
            ),
        ),
        model_field(
            "task_params.values[]",
            required=False,
            compile_path=("taskDefinitionJson[].taskParams.listParameters[0].value"),
            description=(
                "One nonblank literal value without comma, edge whitespace, "
                "controls, surrogates, or DolphinScheduler placeholders; each item "
                "and the complete comma-joined representation are bounded to 256 "
                "UTF-16 code units."
            ),
        ),
        model_field(
            "task_params.degreeOfParallelism",
            model_default=True,
            compile_path=("taskDefinitionJson[].taskParams.degreeOfParallelism"),
            description=(
                "Positive concurrent-child limit no greater than the number of values."
            ),
        ),
    )


def _dynamic_templates(
    surface: DynamicAuthoringSurface,
) -> tuple[TaskAuthoringTemplate, ...]:
    guidance = _dynamic_runtime_guidance(surface)
    return (
        TaskAuthoringTemplate(
            name="minimal",
            summary="Fan out one child workflow over one literal value list.",
            payload_modes=("task_params",),
            yaml=(
                f"# Runtime boundary: {guidance}\n"
                "# Keep parameterName distinct from parent globals and run params.\n"
                + task_template_with_runtime_controls(
                    """# Task template for one bounded dynamic fan-out
name: fanout-region
type: DYNAMIC
description: Run one child workflow for each literal region
task_params:
  childWorkflowName: child-orders
  parameterName: region
  values:
    - east
    - west
  degreeOfParallelism: 1
worker_group: default
priority: MEDIUM
retry:
  times: 0
  interval: 0
timeout: 0
"""
                )
            ),
        ),
    )


def _dynamic_authoring_profile(profile_version: str) -> TaskTypeAuthoringProfile:
    surface = get_task_authoring_surface(profile_version).dynamic
    if not surface.typed_available:
        if surface.registered and surface.exclusion_reason is not None:
            return _dynamic_runtime_exclusion_profile(profile_version)
        reason = surface.exclusion_reason or "upstream-absent"
        message = (
            f"DYNAMIC typed authoring is unavailable in DolphinScheduler "
            f"{profile_version}: {reason}"
        )
        raise ValueError(message)
    contract = TaskAuthoringFacetContract(
        facet_id=DYNAMIC_LITERAL_SINGLE_DIMENSION_FANOUT_FACET,
        family="dynamic-literal-single-dimension-fanout-v1",
        review="dynamic-322-literal-single-dimension-fanout",
        params_model=_family_model("DYNAMIC"),
        fields=_dynamic_fields(surface),
        state_rules=(
            TaskAuthoringStateRule(
                when="typed DYNAMIC literal single-dimension fan-out",
                condition_paths=(),
                active_paths=(
                    "task_params.childWorkflowName",
                    "task_params.parameterName",
                    "task_params.values",
                    "task_params.degreeOfParallelism",
                ),
                compile_policy=(
                    (
                        "task_params.maxNumOfSubWorkflowInstances",
                        "send the exact compiler-derived value count",
                    ),
                    (
                        "task_params.filterCondition",
                        "send compiler-owned empty string",
                    ),
                    (
                        "task_params.listParameters",
                        "send one compiler-owned comma-delimited dimension",
                    ),
                ),
                description=(
                    "Multidimensional products, filters, custom separators, "
                    "placeholders, nonempty localParams/resources, and future state "
                    "remain unchanged/export preservation only. Exact empty "
                    "localParams/resourceList and listParameters.disabled=true UI "
                    "residue are tolerated only while decoding; ordinary task "
                    "retry is rejected."
                ),
            ),
        ),
        templates=_dynamic_templates(surface),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=True,
        typed_edit=True,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            "DYNAMIC/literal_single_dimension_fanout owns only the closed exact "
            "3.2.2 wire; richer native state is preserve-only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="DYNAMIC",
        category="Logic",
        kind="typed",
        default_facet=DYNAMIC_LITERAL_SINGLE_DIMENSION_FANOUT_FACET,
        facets={DYNAMIC_LITERAL_SINGLE_DIMENSION_FANOUT_FACET: membership},
    )


def _dynamic_runtime_exclusion_profile(
    profile_version: str,
) -> TaskTypeAuthoringProfile:
    """Materialize exact 3.2.0/3.2.1 as preserve-only runtime holes."""
    surface = get_task_authoring_surface(profile_version).dynamic
    if (
        profile_version not in {"3.2.0", "3.2.1"}
        or not surface.registered
        or surface.typed_available
        or surface.child_tenant_forwarded
        or surface.exclusion_reason is None
    ):
        message = f"DYNAMIC {profile_version} is not a reviewed runtime hole"
        raise ValueError(message)
    defect = (
        "The dynamic child command omits the parent tenant, so ordinary child "
        "workflows whose reachable closure contains a worker-dispatched task cannot "
        "execute."
    )
    if not surface.child_start_param_guarded:
        defect += (
            " This release also dereferences missing child start parameters "
            "without a membership guard."
        )
    contract = TaskAuthoringFacetContract(
        facet_id=_DYNAMIC_RUNTIME_EXCLUSION_FACET,
        family="dynamic-runtime-exclusion-v1",
        review="dynamic-child-workflow-runtime-exclusion",
        params_model=None,
        fields=(),
        state_rules=(
            TaskAuthoringStateRule(
                when=f"existing exact {profile_version} DYNAMIC server state",
                condition_paths=(),
                active_paths=("task_params",),
                compile_policy=(("task_params", "preserve unchanged only"),),
                description=(
                    defect
                    + " Typed and raw opaque create/edit are closed because a task "
                    "payload cannot repair the master runtime defect."
                ),
            ),
        ),
        templates=(),
    )
    membership = TaskAuthoringFacetMembership(
        profile_version=profile_version,
        contract=contract,
        typed_create=False,
        typed_edit=False,
        opaque_create=False,
        opaque_edit=False,
        opaque_preserve=True,
        constraint=(
            f"Exact {profile_version} DYNAMIC has a reviewed child-workflow "
            "runtime defect; existing state is unchanged/export preservation only."
        ),
    )
    return TaskTypeAuthoringProfile(
        task_type="DYNAMIC",
        category="Logic",
        kind="generic",
        default_facet=_DYNAMIC_RUNTIME_EXCLUSION_FACET,
        facets={_DYNAMIC_RUNTIME_EXCLUSION_FACET: membership},
    )
