from __future__ import annotations

import shlex
from collections import Counter, deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.cli_surface import SCHEDULE_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import ConflictError, UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_patch import load_workflow_patch
from dsctl.models.workflow_spec import load_workflow_spec
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import (
    PreparedWorkflowCompilation,
    prepare_preserved_workflow_update_compilation,
    preserved_workflow_edges,
)
from dsctl.services._workflow.identity import (
    WorkflowTaskIdentity,
    patch_task_identities,
)
from dsctl.services._workflow.patch import (
    WORKFLOW_TASK_AUTHORING_FIELDS,
    WorkflowPatchDiffData,
    apply_workflow_patch,
    patch_has_changes,
    reconcile_workflow_spec,
    workflow_task_authoring_changed,
)
from dsctl.services._workflow.render import workflow_live_baseline
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.upstream.definition_models import schedule_has_missed_fire_policy
from dsctl.upstream.protocols.design import ScheduleMissedFireRecord
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.task_references import task_references

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec, WorkflowPatchTasksSpec
    from dsctl.models.workflow_spec import WorkflowScheduleSpec, WorkflowSpec
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog
    from dsctl.upstream.protocol import ScheduleRecord, WorkflowDagRecord
    from dsctl.upstream.resolver import ResolvedProject
    from dsctl.upstream.task_parameter_projection import (
        ProjectionSource,
        TaskResourceRefIndex,
        TaskWorkflowRefIndex,
    )
    from dsctl.upstream.workflow_graph import WorkflowUpdatePayload


WorkflowMutationInputMode = Literal["patch", "file"]


class WorkflowFileEditTaskTypeChangeData(TypedDict):
    """One same-name task type change detected in a full-file workflow edit."""

    task: str
    from_type: str
    to_type: str


class WorkflowFileEditRiskData(TypedDict):
    """Risk metadata for full-file workflow edits that need confirmation."""

    risk_type: str
    risk_level: str
    deleted_tasks: list[str]
    renamed_workflow: bool
    old_workflow_name: str
    new_workflow_name: str
    task_type_changes: list[WorkflowFileEditTaskTypeChangeData]


class WorkflowPatchIntrinsicIssueData(TypedDict):
    """One patch problem that can be proved without a live workflow baseline."""

    code: str
    path: str
    message: str


WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS = frozenset(
    {"global_params", "timeout"}
)


@dataclass(frozen=True)
class WorkflowMutationPlan:
    """Compiled workflow mutation plan shared by definition and instance edits."""

    merged_spec: WorkflowSpec
    diff: WorkflowPatchDiffData
    compilation: PreparedWorkflowCompilation[WorkflowUpdatePayload]
    has_changes: bool
    input_mode: WorkflowMutationInputMode
    confirmation: WorkflowFileEditRiskData | None = None


def require_supported_workflow_relation_edit(
    plan: WorkflowMutationPlan,
    *,
    profile_version: str,
) -> None:
    """Reject relation edits that DS 2.0.3 can silently discard on definition update."""
    if profile_version != "2.0.3":
        return
    renames = {
        item["from_name"]: item["to_name"] for item in plan.diff["renamed_tasks"]
    }
    removed = {
        (
            renames.get(edge["from_task"], edge["from_task"]),
            renames.get(edge["to_task"], edge["to_task"]),
        )
        for edge in plan.diff["removed_edges"]
    }
    added = {(edge["from_task"], edge["to_task"]) for edge in plan.diff["added_edges"]}
    if added == removed:
        return
    message = "DolphinScheduler 2.0.3 cannot reliably apply workflow dependency edits."
    raise UnsupportedFeatureError(
        message,
        details={
            "resource": WORKFLOW_RESOURCE,
            "ds_version": profile_version,
            "reason": "upstream_relation_update_defect",
            "mutation_applied": False,
        },
        suggestion=(
            "Keep the existing dependencies when editing this workflow, or "
            "upgrade the server before changing its dependency graph. "
            "Creating a separate workflow remains available."
        ),
    )


_WORKFLOW_PATCH_PARSE_SUGGESTION = (
    "Fix the patch YAML, then retry the same command with `--dry-run` to "
    "inspect the compiled diff before apply."
)
_WORKFLOW_PATCH_OPERATION_SUGGESTION = (
    "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
    "inspect the compiled diff before applying it."
)


def workflow_patch_intrinsic_issues(
    patch: WorkflowPatchSpec,
    *,
    workflow_instance: bool = False,
) -> list[WorkflowPatchIntrinsicIssueData]:
    """Collect patch conflicts whose truth does not depend on live DS state."""
    issues = _workflow_instance_patch_field_issues(
        patch,
        enabled=workflow_instance,
    )

    task_patch = patch.tasks
    if task_patch is None:
        return issues
    issues.extend(_workflow_patch_task_operation_issues(task_patch))
    issues.extend(_workflow_patch_created_graph_issues(task_patch))
    return issues


def _workflow_instance_patch_field_issues(
    patch: WorkflowPatchSpec,
    *,
    enabled: bool,
) -> list[WorkflowPatchIntrinsicIssueData]:
    if not enabled or patch.workflow is None:
        return []
    unsupported = sorted(
        set(patch.workflow.set.model_fields_set).difference(
            WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
        )
    )
    return [
        {
            "code": "workflow_instance_patch_field_unsupported",
            "path": f"patch.workflow.set.{field_name}",
            "message": (
                "workflow-instance patches only support workflow.set."
                "global_params and workflow.set.timeout"
            ),
        }
        for field_name in unsupported
    ]


def _workflow_patch_task_operation_issues(
    task_patch: WorkflowPatchTasksSpec,
) -> list[WorkflowPatchIntrinsicIssueData]:
    rename_sources = [item.from_name for item in task_patch.rename]
    rename_targets = [item.to_name for item in task_patch.rename]
    update_matches = [item.match.name for item in task_patch.update]
    create_names = [item.name for item in task_patch.create]
    delete_names = set(task_patch.delete)

    issues = _duplicate_patch_operation_issues(
        rename_sources,
        code="workflow_patch_rename_source_duplicate",
        path="patch.tasks.rename",
        message_template="Patch renames task '{name}' more than once",
    )
    issues.extend(
        _duplicate_patch_operation_issues(
            rename_targets,
            code="workflow_patch_rename_target_duplicate",
            path="patch.tasks.rename",
            message_template=(
                "Patch cannot rename multiple tasks to the same target name '{name}'"
            ),
        )
    )
    issues.extend(
        _duplicate_patch_operation_issues(
            update_matches,
            code="workflow_patch_update_match_duplicate",
            path="patch.tasks.update",
            message_template="Patch cannot update task '{name}' more than once",
        )
    )
    issues.extend(
        _duplicate_patch_operation_issues(
            create_names,
            code="workflow_patch_create_name_duplicate",
            path="patch.tasks.create",
            message_template="Patch cannot create multiple tasks named '{name}'",
        )
    )

    issues.extend(
        _overlapping_patch_operation_issues(
            left=set(rename_sources),
            right=delete_names,
            code="workflow_patch_rename_delete_conflict",
            message_template=(
                "Patch cannot rename and delete task '{name}' in the same edit"
            ),
        )
    )
    issues.extend(
        _overlapping_patch_operation_issues(
            left=set(update_matches),
            right=delete_names,
            code="workflow_patch_update_delete_conflict",
            message_template=(
                "Patch cannot update and delete task '{name}' in the same edit"
            ),
        )
    )
    issues.extend(
        _overlapping_patch_operation_issues(
            left=set(create_names),
            right=set(rename_targets),
            code="workflow_patch_create_rename_target_conflict",
            message_template=(
                "Patch cannot create and rename another task to '{name}' in the "
                "same edit"
            ),
        )
    )
    issues.extend(_workflow_patch_payload_issues(task_patch))
    return issues


def _workflow_patch_payload_issues(
    task_patch: WorkflowPatchTasksSpec,
) -> list[WorkflowPatchIntrinsicIssueData]:
    issues: list[WorkflowPatchIntrinsicIssueData] = []
    for index, update in enumerate(task_patch.update):
        provided = update.set.model_fields_set
        command = update.set.command
        task_params = update.set.task_params
        if {"command", "task_params"}.issubset(provided):
            if (command is None) == (task_params is None):
                issues.append(
                    {
                        "code": "workflow_patch_task_payload_conflict",
                        "path": f"patch.tasks.update[{index}].set",
                        "message": (
                            "A task update must leave exactly one of command or "
                            "task_params populated"
                        ),
                    }
                )
        elif "command" in provided and command is None:
            issues.append(
                {
                    "code": "workflow_patch_task_payload_missing",
                    "path": f"patch.tasks.update[{index}].set.command",
                    "message": (
                        "Setting command to null without task_params removes both "
                        "task payload sources"
                    ),
                }
            )
        elif "task_params" in provided and task_params is None:
            issues.append(
                {
                    "code": "workflow_patch_task_payload_missing",
                    "path": f"patch.tasks.update[{index}].set.task_params",
                    "message": (
                        "Setting task_params to null without command removes both "
                        "task payload sources"
                    ),
                }
            )
    return issues


def _workflow_patch_created_graph_issues(
    task_patch: WorkflowPatchTasksSpec,
) -> list[WorkflowPatchIntrinsicIssueData]:
    """Validate the part of the final graph formed wholly by created tasks."""
    name_counts = Counter(task.name for task in task_patch.create)
    if not name_counts:
        return []
    created_names = set(name_counts)
    edges: set[tuple[str, str]] = set()
    issues: list[WorkflowPatchIntrinsicIssueData] = []
    for index, task in enumerate(task_patch.create):
        for dependency_index, predecessor in enumerate(task.depends_on):
            _append_created_patch_edge(
                predecessor,
                task.name,
                task_name=task.name,
                path=(f"patch.tasks.create[{index}].depends_on[{dependency_index}]"),
                created_names=created_names,
                edges=edges,
                issues=issues,
            )
        if task.task_params is None:
            continue
        for reference in task_references(
            task.type.upper(),
            task.task_params,
        ):
            if not isinstance(reference.value, str):
                continue
            predecessor, successor = (
                (reference.value, task.name)
                if reference.role == "predecessor"
                else (task.name, reference.value)
            )
            _append_created_patch_edge(
                predecessor,
                successor,
                task_name=task.name,
                path=f"patch.tasks.create[{index}].{reference.field}",
                created_names=created_names,
                edges=edges,
                issues=issues,
            )
    if all(count == 1 for count in name_counts.values()) and _created_graph_has_cycle(
        created_names,
        edges,
    ):
        issues.append(
            {
                "code": "workflow_patch_created_task_cycle",
                "path": "patch.tasks.create",
                "message": "Created patch tasks contain a dependency cycle",
            }
        )
    return issues


def _append_created_patch_edge(
    predecessor: str,
    successor: str,
    *,
    task_name: str,
    path: str,
    created_names: set[str],
    edges: set[tuple[str, str]],
    issues: list[WorkflowPatchIntrinsicIssueData],
) -> None:
    if predecessor == successor:
        issues.append(
            {
                "code": "workflow_patch_created_task_self_reference",
                "path": path,
                "message": f"Created task '{task_name}' cannot reference itself",
            }
        )
        return
    if predecessor in created_names and successor in created_names:
        edges.add((predecessor, successor))


def _created_graph_has_cycle(
    task_names: set[str],
    edges: set[tuple[str, str]],
) -> bool:
    indegree = dict.fromkeys(task_names, 0)
    downstream: dict[str, list[str]] = {name: [] for name in task_names}
    for predecessor, successor in edges:
        downstream[predecessor].append(successor)
        indegree[successor] += 1
    ready = deque(name for name, count in indegree.items() if count == 0)
    visited = 0
    while ready:
        current = ready.popleft()
        visited += 1
        for successor in downstream[current]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                ready.append(successor)
    return visited != len(task_names)


def _overlapping_patch_operation_issues(
    *,
    left: set[str],
    right: set[str],
    code: str,
    message_template: str,
) -> list[WorkflowPatchIntrinsicIssueData]:
    return [
        {
            "code": code,
            "path": "patch.tasks",
            "message": message_template.format(name=name),
        }
        for name in sorted(left & right)
    ]


def _duplicate_patch_operation_issues(
    names: list[str],
    *,
    code: str,
    path: str,
    message_template: str,
) -> list[WorkflowPatchIntrinsicIssueData]:
    counts = Counter(names)
    return [
        {
            "code": code,
            "path": path,
            "message": message_template.format(name=name),
        }
        for name in sorted(value for value, count in counts.items() if count > 1)
    ]


def require_workflow_patch_intrinsic_valid(
    patch: WorkflowPatchSpec,
    *,
    workflow_instance: bool = False,
) -> None:
    """Keep mutation commands fail-fast over the shared intrinsic patch facts."""
    issues = workflow_patch_intrinsic_issues(
        patch,
        workflow_instance=workflow_instance,
    )
    if not issues:
        return
    first = issues[0]
    if first["code"] == "workflow_instance_patch_field_unsupported":
        unsupported_fields = sorted(
            issue["path"].rsplit(".", maxsplit=1)[-1]
            for issue in issues
            if issue["code"] == "workflow_instance_patch_field_unsupported"
        )
        message = (
            "workflow-instance edit only supports workflow.set.global_params and "
            "workflow.set.timeout"
        )
        raise UserInputError(
            message,
            details={
                "unsupported_fields": unsupported_fields,
                "supported_fields": sorted(
                    WORKFLOW_INSTANCE_PATCH_SUPPORTED_WORKFLOW_FIELDS
                ),
            },
            suggestion=(
                "Use `dsctl workflow edit --patch ...` for definition-level fields "
                "such as name, description, or release_state."
            ),
        )
    raise UserInputError(
        first["message"],
        details={"field": first["path"], "code": first["code"]},
        suggestion=_WORKFLOW_PATCH_OPERATION_SUGGESTION,
    )


def _workflow_file_parse_suggestion(path: Path) -> str:
    lint_command = shlex.join(("dsctl", "lint", "workflow", str(path)))
    return (
        f"Fix the workflow YAML and validate this same file with `{lint_command}`, "
        "then retry the original edit command with --dry-run."
    )


def load_workflow_patch_or_error(
    path: Path,
    *,
    catalog: TaskAuthoringCatalog | None = None,
    workflow_instance: bool = False,
) -> WorkflowPatchSpec:
    """Load one workflow patch file and normalize parse errors to user input."""
    try:
        patch = load_workflow_patch(
            path,
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.TYPED_EDIT,
            ),
        )
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_WORKFLOW_PATCH_PARSE_SUGGESTION,
        ) from exc
    require_workflow_patch_intrinsic_valid(
        patch,
        workflow_instance=workflow_instance,
    )
    return patch


def load_workflow_edit_spec_or_error(
    path: Path,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> WorkflowSpec:
    """Load one full workflow edit YAML file and normalize parse errors."""
    try:
        spec = load_workflow_spec(
            path,
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
            ),
        )
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_workflow_file_parse_suggestion(path),
        ) from exc
    return spec


def prepare_workflow_file_edit(
    spec: WorkflowSpec,
    *,
    attached_schedule: ScheduleRecord | None,
    workflow_release_state: str | None,
) -> WorkflowSpec:
    """Verify an optional read-only schedule snapshot and return definition state."""
    schedule_spec = spec.schedule
    if schedule_spec is None:
        return spec
    if attached_schedule is None:
        message = (
            "Workflow edit was not sent because the file contains a schedule "
            "snapshot but the workflow has no attached schedule."
        )
        raise ConflictError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "dependency_resource": SCHEDULE_RESOURCE,
                "reason": "attached_schedule_missing",
                "mutation_applied": False,
            },
            suggestion=_WORKFLOW_SCHEDULE_SNAPSHOT_CONFLICT_SUGGESTION,
        )

    document_values = _workflow_schedule_spec_snapshot(schedule_spec)
    current_values = _workflow_schedule_record_snapshot(attached_schedule)
    mismatches: dict[str, dict[str, str | None]] = {}
    for field_name, document_value in document_values.items():
        current_value = current_values.get(field_name)
        if document_value == current_value:
            continue
        if (
            field_name == "release_state"
            and document_value == "ONLINE"
            and current_value == "OFFLINE"
            and workflow_release_state == "OFFLINE"
            and spec.workflow.release_state.value == "ONLINE"
        ):
            continue
        mismatches[field_name] = {
            "file": document_value,
            "current": current_value,
        }

    if mismatches:
        message = (
            "Workflow edit was not sent because the file's read-only schedule "
            "snapshot differs from the attached schedule."
        )
        raise ConflictError(
            message,
            details={
                "resource": WORKFLOW_RESOURCE,
                "dependency_resource": SCHEDULE_RESOURCE,
                "reason": "schedule_snapshot_mismatch",
                "schedule_id": attached_schedule.id,
                "mismatched_fields": sorted(mismatches),
                "mismatches": mismatches,
                "mutation_applied": False,
            },
            suggestion=_WORKFLOW_SCHEDULE_SNAPSHOT_CONFLICT_SUGGESTION,
        )
    return spec.model_copy(update={"schedule": None})


_WORKFLOW_SCHEDULE_SNAPSHOT_CONFLICT_SUGGESTION = (
    "Export the workflow again and reapply only definition edits, or remove the "
    "schedule block to preserve its current state without snapshot validation. "
    "Use `dsctl schedule update|online|offline` for schedule changes."
)


def _workflow_schedule_spec_snapshot(
    schedule: WorkflowScheduleSpec,
) -> dict[str, str | None]:
    release_state = (
        None
        if schedule.release_state is None and schedule.enabled is None
        else schedule.desired_release_state().value
    )
    return {
        "cron": schedule.cron,
        "timezone": schedule.timezone,
        "start": schedule.start,
        "end": schedule.end,
        "failure_strategy": (
            None
            if schedule.failure_strategy is None
            else schedule.failure_strategy.value
        ),
        "priority": None if schedule.priority is None else schedule.priority.value,
        **(
            {"missed_fire_policy": schedule.missed_fire_policy}
            if schedule.missed_fire_policy is not None
            else {}
        ),
        "release_state": release_state,
    }


def _workflow_schedule_record_snapshot(
    schedule: ScheduleRecord,
) -> dict[str, str | None]:
    return {
        "cron": schedule.crontab,
        "timezone": schedule.timezoneId,
        "start": schedule.startTime,
        "end": schedule.endTime,
        "failure_strategy": enum_value(schedule.failureStrategy),
        "priority": enum_value(schedule.workflowInstancePriority),
        **(
            {"missed_fire_policy": enum_value(schedule.missedFirePolicy)}
            if schedule_has_missed_fire_policy(schedule)
            and isinstance(schedule, ScheduleMissedFireRecord)
            and schedule.missedFirePolicy is not None
            else {}
        ),
        "release_state": enum_value(schedule.releaseState),
    }


def load_workflow_instance_edit_spec_or_error(
    path: Path,
    *,
    catalog: TaskAuthoringCatalog | None = None,
) -> WorkflowSpec:
    """Load one full workflow-instance edit YAML file and normalize parse errors."""
    try:
        spec = load_workflow_spec(
            path,
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
            ),
        )
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_workflow_file_parse_suggestion(path),
        ) from exc
    if spec.schedule is not None:
        message = (
            "workflow-instance edit --file does not mutate schedule blocks; remove "
            "`schedule:` and use schedule commands separately."
        )
        raise UserInputError(
            message,
            details={"file": str(path), "unsupported_block": "schedule"},
            suggestion=(
                "Remove the schedule block. Instance edit repairs one finished "
                "workflow-instance DAG; schedule lifecycle remains under "
                "`dsctl schedule`."
            ),
        )
    return spec


def prepare_workflow_mutation_plan(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
    patch: WorkflowPatchSpec,
    release_state: str | None,
    catalog: TaskAuthoringCatalog | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> WorkflowMutationPlan:
    """Apply one patch and prepare a reusable DS update compilation."""
    live_baseline = workflow_live_baseline(
        dag,
        project=project,
        catalog=catalog,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    merged_spec, diff = apply_workflow_patch(
        live_baseline.spec,
        patch,
        edge_builder=preserved_workflow_edges,
        catalog=catalog,
    )
    active_task_identities = patch_task_identities(
        live_baseline.task_identities,
        diff=diff,
    )
    projection_sources = _patch_projection_sources(
        live_baseline.projection_sources,
        diff=diff,
    )
    compilation = prepare_preserved_workflow_update_compilation(
        merged_spec,
        release_state=release_state,
        active_task_identities=active_task_identities,
        unavailable_task_identities=_unavailable_task_identities(
            live_baseline.task_identities,
            active=active_task_identities,
        ),
        authored_task_names=_patch_authored_task_names(patch),
        runtime_authored_task_names=_patch_runtime_authored_task_names(patch),
        workflow_global_params_authored=_patch_global_params_authored(patch),
        workflow_runtime_authored=(
            _patch_workflow_runtime_authored(patch)
            or _patch_task_runtime_context_authored(patch)
        ),
        preserved_projection_sources=projection_sources,
        catalog=catalog,
    )
    return WorkflowMutationPlan(
        merged_spec=merged_spec,
        diff=diff,
        compilation=compilation,
        has_changes=patch_has_changes(diff),
        input_mode="patch",
    )


def prepare_workflow_file_mutation_plan(
    dag: WorkflowDagRecord,
    *,
    project: ResolvedProject,
    desired: WorkflowSpec,
    release_state: str | None,
    risk_type: str = "workflow_full_edit_destructive_change",
    catalog: TaskAuthoringCatalog | None = None,
    resource_refs: TaskResourceRefIndex | None = None,
    workflow_refs: TaskWorkflowRefIndex | None = None,
) -> WorkflowMutationPlan:
    """Prepare one full workflow YAML desired-state edit compilation."""
    live_baseline = workflow_live_baseline(
        dag,
        project=project,
        catalog=catalog,
        resource_refs=resource_refs,
        workflow_refs=workflow_refs,
    )
    merged_spec, diff = reconcile_workflow_spec(
        live_baseline.spec,
        desired,
        edge_builder=preserved_workflow_edges,
        catalog=catalog,
    )
    active_task_identities = _desired_task_identities(
        live_baseline.task_identities,
        desired=merged_spec,
    )
    compilation = prepare_preserved_workflow_update_compilation(
        merged_spec,
        release_state=release_state,
        active_task_identities=active_task_identities,
        unavailable_task_identities=_unavailable_task_identities(
            live_baseline.task_identities,
            active=active_task_identities,
        ),
        authored_task_names=_file_authored_task_names(
            live_baseline.spec,
            merged_spec,
        ),
        runtime_authored_task_names=_file_runtime_authored_task_names(
            live_baseline.spec,
            merged_spec,
        ),
        workflow_global_params_authored=(
            live_baseline.spec.workflow.global_params
            != merged_spec.workflow.global_params
        ),
        workflow_runtime_authored=_file_runtime_context_authored(
            live_baseline.spec,
            merged_spec,
            diff=diff,
        ),
        preserved_projection_sources=_desired_projection_sources(
            live_baseline.projection_sources,
            desired=merged_spec,
        ),
        catalog=catalog,
    )
    return WorkflowMutationPlan(
        merged_spec=merged_spec,
        diff=diff,
        compilation=compilation,
        has_changes=patch_has_changes(diff),
        input_mode="file",
        confirmation=workflow_file_edit_risk_data(
            baseline=live_baseline.spec,
            desired=merged_spec,
            diff=diff,
            risk_type=risk_type,
        ),
    )


def _desired_task_identities(
    baseline: dict[str, WorkflowTaskIdentity],
    *,
    desired: WorkflowSpec,
) -> dict[str, WorkflowTaskIdentity]:
    desired_names = {task.name for task in desired.tasks}
    return {
        name: identity for name, identity in baseline.items() if name in desired_names
    }


def _patch_projection_sources(
    baseline: dict[str, ProjectionSource],
    *,
    diff: WorkflowPatchDiffData,
) -> dict[str, ProjectionSource]:
    """Align decoded task provenance with patch rename and delete operations."""
    sources = dict(baseline)
    for rename in diff["renamed_tasks"]:
        source = sources.pop(rename["from_name"], None)
        if source is not None:
            sources[rename["to_name"]] = source
    for name in diff["deleted_tasks"]:
        sources.pop(name, None)
    return sources


def _desired_projection_sources(
    baseline: dict[str, ProjectionSource],
    *,
    desired: WorkflowSpec,
) -> dict[str, ProjectionSource]:
    """Retain decoded provenance for same-name tasks in one full-file edit."""
    desired_names = {task.name for task in desired.tasks}
    return {name: source for name, source in baseline.items() if name in desired_names}


def _patch_authored_task_names(patch: WorkflowPatchSpec) -> frozenset[str]:
    """Return tasks whose type or parameter intent was explicitly authored."""
    if patch.tasks is None:
        return frozenset()
    renamed = {rename.from_name: rename.to_name for rename in patch.tasks.rename}
    authored = {task.name for task in patch.tasks.create}
    authored.update(
        renamed.get(update.match.name, update.match.name)
        for update in patch.tasks.update
        if WORKFLOW_TASK_AUTHORING_FIELDS.intersection(update.set.model_fields_set)
    )
    return frozenset(authored)


_RUNTIME_CONSTRAINT_TASK_FIELDS = WORKFLOW_TASK_AUTHORING_FIELDS | frozenset(
    {
        "cpu_quota",
        "delay",
        "depends_on",
        "environment_code",
        "flag",
        "memory_max",
        "priority",
        "retry",
        "task_group_id",
        "task_group_priority",
        "timeout",
        "timeout_notify_strategy",
        "worker_group",
    }
)

_RUNTIME_CONSTRAINT_WORKFLOW_FIELDS = frozenset(
    {
        "execution_type",
        "global_params",
        "release_state",
        "timeout",
    }
)


def _patch_runtime_authored_task_names(
    patch: WorkflowPatchSpec,
) -> frozenset[str]:
    """Return tasks whose authored changes can alter guarded runtime semantics."""
    if patch.tasks is None:
        return frozenset()
    renamed = {rename.from_name: rename.to_name for rename in patch.tasks.rename}
    authored = {task.name for task in patch.tasks.create}
    authored.update(
        renamed.get(update.match.name, update.match.name)
        for update in patch.tasks.update
        if _RUNTIME_CONSTRAINT_TASK_FIELDS.intersection(update.set.model_fields_set)
    )
    return frozenset(authored)


def _patch_global_params_authored(patch: WorkflowPatchSpec) -> bool:
    """Return whether this patch explicitly changes workflow-global parameters."""
    return (
        patch.workflow is not None
        and "global_params" in patch.workflow.set.model_fields_set
    )


def _patch_workflow_runtime_authored(patch: WorkflowPatchSpec) -> bool:
    """Return whether a patch authors workflow-level execution semantics."""
    return patch.workflow is not None and bool(
        _RUNTIME_CONSTRAINT_WORKFLOW_FIELDS.intersection(
            patch.workflow.set.model_fields_set
        )
    )


def _patch_task_runtime_context_authored(patch: WorkflowPatchSpec) -> bool:
    """Return whether any task or graph operation changes workflow execution."""
    if patch.tasks is None:
        return False
    return bool(
        patch.tasks.create
        or patch.tasks.delete
        or any(
            _RUNTIME_CONSTRAINT_TASK_FIELDS.intersection(update.set.model_fields_set)
            for update in patch.tasks.update
        )
    )


def _file_authored_task_names(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
) -> frozenset[str]:
    """Return full-file tasks with newly authored type or parameter intent."""
    baseline_by_name = {task.name: task for task in baseline.tasks}
    return frozenset(
        task.name
        for task in desired.tasks
        if workflow_task_authoring_changed(baseline_by_name.get(task.name), task)
    )


def _file_runtime_authored_task_names(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
) -> frozenset[str]:
    """Return full-file tasks whose changes affect guarded runtime semantics."""
    baseline_by_name = {task.name: task for task in baseline.tasks}
    return frozenset(
        task.name
        for task in desired.tasks
        if (baseline_task := baseline_by_name.get(task.name)) is None
        or any(
            getattr(baseline_task, field_name) != getattr(task, field_name)
            for field_name in _RUNTIME_CONSTRAINT_TASK_FIELDS
        )
    )


def _file_runtime_context_authored(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
    *,
    diff: WorkflowPatchDiffData,
) -> bool:
    """Return whether a full file changes workflow execution semantics."""
    return bool(
        diff["added_tasks"]
        or diff["deleted_tasks"]
        or diff["renamed_tasks"]
        or _file_runtime_authored_task_names(baseline, desired)
        or any(
            getattr(baseline.workflow, field_name)
            != getattr(desired.workflow, field_name)
            for field_name in _RUNTIME_CONSTRAINT_WORKFLOW_FIELDS
        )
    )


def _unavailable_task_identities(
    baseline: dict[str, WorkflowTaskIdentity],
    *,
    active: dict[str, WorkflowTaskIdentity],
) -> tuple[WorkflowTaskIdentity, ...]:
    active_codes = {identity.code for identity in active.values()}
    return tuple(
        identity for identity in baseline.values() if identity.code not in active_codes
    )


def workflow_file_edit_risk_data(
    *,
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
    diff: WorkflowPatchDiffData,
    risk_type: str,
) -> WorkflowFileEditRiskData | None:
    task_type_changes = _task_type_changes(baseline, desired)
    renamed_workflow = baseline.workflow.name != desired.workflow.name
    if not diff["deleted_tasks"] and not renamed_workflow and not task_type_changes:
        return None
    return {
        "risk_type": risk_type,
        "risk_level": "high",
        "deleted_tasks": diff["deleted_tasks"],
        "renamed_workflow": renamed_workflow,
        "old_workflow_name": baseline.workflow.name,
        "new_workflow_name": desired.workflow.name,
        "task_type_changes": task_type_changes,
    }


def _task_type_changes(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
) -> list[WorkflowFileEditTaskTypeChangeData]:
    baseline_by_name = {task.name: task for task in baseline.tasks}
    changes: list[WorkflowFileEditTaskTypeChangeData] = []
    for task in desired.tasks:
        baseline_task = baseline_by_name.get(task.name)
        if baseline_task is None or baseline_task.type == task.type:
            continue
        changes.append(
            {
                "task": task.name,
                "from_type": baseline_task.type,
                "to_type": task.type,
            }
        )
    return sorted(changes, key=lambda item: item["task"])
