from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from dsctl.errors import UserInputError
from dsctl.models.workflow_patch import WorkflowPatchSpec
from dsctl.services._workflow.compile import preserved_workflow_edges
from dsctl.services._workflow.mutation import (
    WorkflowFileEditRiskData,
    workflow_file_edit_risk_data,
)
from dsctl.services._workflow.patch import (
    WorkflowPatchDiffData,
    apply_workflow_patch,
    patch_has_changes,
    reconcile_workflow_spec,
)
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyTask,
    DecodedLegacyWorkflowGraph,
    LegacyDependentRefIndex,
    LegacyWorkflowGraphError,
    LegacyWorkflowRefIndex,
    PreparedLegacyWorkflowGraph,
    prepare_legacy_workflow_graph,
)
from dsctl.upstream.task_parameter_projection import ProjectionSource

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.output import JsonValue
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.task_parameter_projection import TaskResourceRefIndex


LegacyWorkflowMutationInputMode = Literal["patch", "file"]


@dataclass(frozen=True, slots=True)
class LegacyWorkflowMutationPlan:
    """Pure DS 1.3.9 desired-state edit and its frozen wire compilation."""

    merged_spec: WorkflowSpec
    diff: WorkflowPatchDiffData
    compilation: PreparedLegacyWorkflowGraph
    has_changes: bool
    input_mode: LegacyWorkflowMutationInputMode
    confirmation: WorkflowFileEditRiskData | None = None


@dataclass(frozen=True, slots=True)
class LegacyWorkflowMutationDraft:
    """Merged DS 1.3.9 edit intent awaiting service-level reference binding."""

    merged_spec: WorkflowSpec
    diff: WorkflowPatchDiffData
    compilation_baseline: DecodedLegacyWorkflowGraph
    has_changes: bool
    input_mode: LegacyWorkflowMutationInputMode
    confirmation: WorkflowFileEditRiskData | None
    risk_type: str


def prepare_legacy_workflow_mutation_plan(
    graph: DecodedLegacyWorkflowGraph,
    *,
    workflow_name: str,
    project_name: str,
    description: str | None,
    release_state: str | None,
    mutation: WorkflowPatchSpec | WorkflowSpec,
    catalog: TaskAuthoringCatalog,
    task_id_factory: Callable[[str], str] | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    risk_type: str = "workflow_full_edit_destructive_change",
) -> LegacyWorkflowMutationPlan:
    """Merge one patch or full file against a decoded DS 1.3.9 graph.

    Existing native ids and opaque fields are retained through ``graph``.  Patch
    renames deliberately rebind the old native task to its new canonical name;
    full-file edits keep the established exact-name matching semantics.
    """
    draft = prepare_legacy_workflow_mutation_draft(
        graph,
        workflow_name=workflow_name,
        project_name=project_name,
        description=description,
        release_state=release_state,
        mutation=mutation,
        catalog=catalog,
        risk_type=risk_type,
    )
    return compile_legacy_workflow_mutation_draft(
        draft,
        task_id_factory=task_id_factory,
        resource_refs=resource_refs,
    )


def prepare_legacy_workflow_mutation_draft(
    graph: DecodedLegacyWorkflowGraph,
    *,
    workflow_name: str,
    project_name: str,
    description: str | None,
    release_state: str | None,
    mutation: WorkflowPatchSpec | WorkflowSpec,
    catalog: TaskAuthoringCatalog,
    risk_type: str = "workflow_full_edit_destructive_change",
) -> LegacyWorkflowMutationDraft:
    """Merge a legacy edit before resolving name-based remote identities."""
    baseline_spec = graph.to_workflow_spec(
        name=workflow_name,
        project=project_name,
        description=description,
        release_state=release_state or "OFFLINE",
    )
    if isinstance(mutation, WorkflowPatchSpec):
        merged_spec, diff = apply_workflow_patch(
            baseline_spec,
            mutation,
            edge_builder=preserved_workflow_edges,
            catalog=catalog,
        )
        compilation_baseline = _baseline_with_patch_renames(graph, diff=diff)
        input_mode: LegacyWorkflowMutationInputMode = "patch"
    else:
        merged_spec, diff = reconcile_workflow_spec(
            baseline_spec,
            mutation,
            edge_builder=preserved_workflow_edges,
            catalog=catalog,
        )
        compilation_baseline = graph
        input_mode = "file"

    _require_opaque_conditions_metadata_only(
        graph,
        baseline=baseline_spec,
        desired=merged_spec,
        diff=diff,
    )
    if input_mode == "file":
        _require_opaque_sub_process_preservation_only(
            baseline=baseline_spec,
            desired=merged_spec,
        )
    confirmation = (
        workflow_file_edit_risk_data(
            baseline=baseline_spec,
            desired=merged_spec,
            diff=diff,
            risk_type=risk_type,
        )
        if input_mode == "file"
        else None
    )
    return LegacyWorkflowMutationDraft(
        merged_spec=merged_spec,
        diff=diff,
        compilation_baseline=compilation_baseline,
        has_changes=patch_has_changes(diff),
        input_mode=input_mode,
        confirmation=confirmation,
        risk_type=risk_type,
    )


def compile_legacy_workflow_mutation_draft(
    draft: LegacyWorkflowMutationDraft,
    *,
    workflow_refs: LegacyWorkflowRefIndex | None = None,
    dependent_refs: LegacyDependentRefIndex | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    task_id_factory: Callable[[str], str] | None = None,
) -> LegacyWorkflowMutationPlan:
    """Compile one merged draft using already-frozen workflow bindings."""
    try:
        compilation = prepare_legacy_workflow_graph(
            draft.merged_spec,
            task_id_factory=task_id_factory,
            baseline=draft.compilation_baseline,
            workflow_refs=(
                LegacyWorkflowRefIndex.empty()
                if workflow_refs is None
                else workflow_refs
            ),
            dependent_refs=(
                LegacyDependentRefIndex.empty()
                if dependent_refs is None
                else dependent_refs
            ),
            resource_refs=resource_refs,
        )
    except LegacyWorkflowGraphError as error:
        edit_command = (
            "workflow-instance edit"
            if draft.risk_type == "workflow_instance_full_edit_destructive_change"
            else "workflow edit"
        )
        raise UserInputError(
            str(error),
            suggestion=(
                "Fix legacy task names and graph references, then retry "
                f"`dsctl {edit_command} ... --dry-run`."
            ),
        ) from error
    return LegacyWorkflowMutationPlan(
        merged_spec=draft.merged_spec,
        diff=draft.diff,
        compilation=compilation,
        has_changes=draft.has_changes,
        input_mode=draft.input_mode,
        confirmation=draft.confirmation,
    )


def _require_opaque_conditions_metadata_only(
    graph: DecodedLegacyWorkflowGraph,
    *,
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
    diff: WorkflowPatchDiffData,
) -> None:
    """Prevent edits that cannot safely rewrite split opaque TaskNode state."""
    opaque_names = {
        task.name
        for task in graph.tasks
        if task.type == "CONDITIONS"
        and task.task_params_reencode_source is ProjectionSource.OPAQUE_PRESERVE
    }
    if not opaque_names:
        return

    topology_changed = any(
        (
            diff["added_tasks"],
            diff["renamed_tasks"],
            diff["deleted_tasks"],
            diff["added_edges"],
            diff["removed_edges"],
        )
    )
    if topology_changed:
        message = (
            "Server-originated opaque CONDITIONS topology cannot be changed "
            "because its split outer TaskNode references are not represented "
            "in standalone YAML"
        )
        raise UserInputError(
            message,
            suggestion=(
                "Keep task names and dependencies unchanged and apply only "
                "metadata edits."
            ),
        )

    baseline_by_name = {task.name: task for task in baseline.tasks}
    desired_by_name = {task.name: task for task in desired.tasks}
    for task_name in opaque_names:
        before = baseline_by_name[task_name]
        after = desired_by_name[task_name]
        if any(
            (
                before.type != after.type,
                before.task_params != after.task_params,
                before.command != after.command,
            )
        ):
            message = (
                f"Server-originated opaque CONDITIONS task '{task_name}' payload "
                "cannot be changed because its split outer TaskNode state is not "
                "represented in standalone YAML"
            )
            raise UserInputError(
                message,
                suggestion="Apply only metadata edits to this task.",
            )


def _require_opaque_sub_process_preservation_only(
    *,
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
) -> None:
    """Allow native SUB_PROCESS only as unchanged server-baseline preservation."""
    baseline_by_name = {task.name: task for task in baseline.tasks}
    for task in desired.tasks:
        if task.type != "SUB_PROCESS":
            continue
        before = baseline_by_name.get(task.name)
        if (
            before is not None
            and before.type == "SUB_PROCESS"
            and before.task_params == task.task_params
            and before.command == task.command
        ):
            continue
        message = (
            f"Native SUB_PROCESS task '{task.name}' is preservation-only on "
            "DolphinScheduler 1.3.9"
        )
        raise UserInputError(
            message,
            suggestion=(
                "Keep an exported native SUB_PROCESS payload unchanged, or replace "
                "it with canonical SUB_WORKFLOW.childWorkflowName authoring."
            ),
        )


def _baseline_with_patch_renames(
    graph: DecodedLegacyWorkflowGraph,
    *,
    diff: WorkflowPatchDiffData,
) -> DecodedLegacyWorkflowGraph:
    rename_map = {
        rename["from_name"]: rename["to_name"] for rename in diff["renamed_tasks"]
    }
    if not rename_map:
        return graph

    tasks = tuple(_renamed_task(task, rename_map=rename_map) for task in graph.tasks)
    edges = tuple(
        (
            rename_map.get(predecessor, predecessor),
            rename_map.get(successor, successor),
        )
        for predecessor, successor in graph.edges
    )
    return replace(graph, tasks=tasks, edges=edges)


def _renamed_task(
    task: DecodedLegacyTask,
    *,
    rename_map: Mapping[str, str],
) -> DecodedLegacyTask:
    current_name = rename_map.get(task.name, task.name)
    predecessors = tuple(
        rename_map.get(predecessor, predecessor) for predecessor in task.depends_on
    )
    document = dict(task.document)
    document["name"] = current_name
    document["depends_on"] = list(predecessors)
    native_fields = dict(task.native_fields)
    native_fields["name"] = current_name
    native_fields["preTasks"] = list(predecessors)
    return replace(
        task,
        name=current_name,
        depends_on=predecessors,
        document=_frozen_json_mapping(document),
        native_fields=_frozen_json_mapping(native_fields),
    )


def _frozen_json_mapping(
    value: Mapping[str, JsonValue],
) -> Mapping[str, JsonValue]:
    return MappingProxyType(dict(value))


__all__ = [
    "LegacyWorkflowMutationDraft",
    "LegacyWorkflowMutationPlan",
    "compile_legacy_workflow_mutation_draft",
    "prepare_legacy_workflow_mutation_draft",
    "prepare_legacy_workflow_mutation_plan",
]
