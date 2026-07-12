from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.cli_surface import SCHEDULE_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import ConflictError, UserInputError
from dsctl.models.workflow_patch import load_workflow_patch
from dsctl.models.workflow_spec import load_workflow_spec
from dsctl.services._serialization import enum_value
from dsctl.services._workflow_compile import (
    WorkflowUpdatePayload,
    compile_workflow_update_payload,
    workflow_edges,
)
from dsctl.services._workflow_identity import (
    WorkflowTaskIdentity,
    patch_task_identities,
)
from dsctl.services._workflow_patch import (
    WorkflowPatchDiffData,
    apply_workflow_patch,
    patch_has_changes,
    reconcile_workflow_spec,
)
from dsctl.services._workflow_render import workflow_live_baseline

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import WorkflowScheduleSpec, WorkflowSpec
    from dsctl.services.resolver import ResolvedProject
    from dsctl.upstream.protocol import ScheduleRecord, WorkflowDagRecord


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


@dataclass(frozen=True)
class WorkflowMutationPlan:
    """Compiled workflow mutation plan shared by definition and instance edits."""

    merged_spec: WorkflowSpec
    diff: WorkflowPatchDiffData
    payload: WorkflowUpdatePayload
    has_changes: bool
    input_mode: WorkflowMutationInputMode
    confirmation: WorkflowFileEditRiskData | None = None


_WORKFLOW_PATCH_PARSE_SUGGESTION = (
    "Fix the patch YAML, then retry the same command with `--dry-run` to "
    "inspect the compiled diff before apply."
)
_WORKFLOW_FILE_PARSE_SUGGESTION = (
    "Fix the workflow YAML, then retry `dsctl workflow edit --file FILE --dry-run` "
    "to inspect the compiled diff before apply."
)
_WORKFLOW_INSTANCE_FILE_PARSE_SUGGESTION = (
    "Fix the workflow YAML, then retry `dsctl workflow-instance edit ID --file "
    "FILE --dry-run` to inspect the compiled diff before apply."
)


def load_workflow_patch_or_error(path: Path) -> WorkflowPatchSpec:
    """Load one workflow patch file and normalize parse errors to user input."""
    try:
        return load_workflow_patch(path)
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_WORKFLOW_PATCH_PARSE_SUGGESTION,
        ) from exc


def load_workflow_edit_spec_or_error(path: Path) -> WorkflowSpec:
    """Load one full workflow edit YAML file and normalize parse errors."""
    try:
        spec = load_workflow_spec(path)
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_WORKFLOW_FILE_PARSE_SUGGESTION,
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
        current_value = current_values[field_name]
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
        "release_state": enum_value(schedule.releaseState),
    }


def load_workflow_instance_edit_spec_or_error(path: Path) -> WorkflowSpec:
    """Load one full workflow-instance edit YAML file and normalize parse errors."""
    try:
        spec = load_workflow_spec(path)
    except (TypeError, ValueError) as exc:
        raise UserInputError(
            str(exc),
            details={"file": str(path)},
            suggestion=_WORKFLOW_INSTANCE_FILE_PARSE_SUGGESTION,
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


def compile_workflow_mutation_plan(
    dag: WorkflowDagRecord,
    *,
    allocate_task_codes: Callable[[int], Sequence[int]],
    project: ResolvedProject,
    patch: WorkflowPatchSpec,
    release_state: str | None,
) -> WorkflowMutationPlan:
    """Apply one patch to a live DAG snapshot and compile the DS update payload."""
    live_baseline = workflow_live_baseline(dag, project=project)
    merged_spec, diff = apply_workflow_patch(
        live_baseline.spec,
        patch,
        edge_builder=workflow_edges,
    )
    payload = compile_workflow_update_payload(
        merged_spec,
        allocate_task_codes=allocate_task_codes,
        release_state=release_state,
        task_identities=patch_task_identities(live_baseline.task_identities, diff=diff),
        reserved_task_codes={
            identity.code for identity in live_baseline.task_identities.values()
        },
    )
    return WorkflowMutationPlan(
        merged_spec=merged_spec,
        diff=diff,
        payload=payload,
        has_changes=patch_has_changes(diff),
        input_mode="patch",
    )


def compile_workflow_file_mutation_plan(
    dag: WorkflowDagRecord,
    *,
    allocate_task_codes: Callable[[int], Sequence[int]],
    project: ResolvedProject,
    desired: WorkflowSpec,
    release_state: str | None,
    risk_type: str = "workflow_full_edit_destructive_change",
) -> WorkflowMutationPlan:
    """Compile one full workflow YAML desired-state edit payload."""
    live_baseline = workflow_live_baseline(dag, project=project)
    merged_spec, diff = reconcile_workflow_spec(
        live_baseline.spec,
        desired,
        edge_builder=workflow_edges,
    )
    payload = compile_workflow_update_payload(
        merged_spec,
        allocate_task_codes=allocate_task_codes,
        release_state=release_state,
        task_identities=_desired_task_identities(
            live_baseline.task_identities,
            desired=merged_spec,
        ),
        reserved_task_codes={
            identity.code for identity in live_baseline.task_identities.values()
        },
    )
    return WorkflowMutationPlan(
        merged_spec=merged_spec,
        diff=diff,
        payload=payload,
        has_changes=patch_has_changes(diff),
        input_mode="file",
        confirmation=_workflow_file_edit_risk_data(
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


def _workflow_file_edit_risk_data(
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
