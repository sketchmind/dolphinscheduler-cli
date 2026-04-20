from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypedDict

from dsctl.errors import UserInputError
from dsctl.models.workflow_patch import load_workflow_patch
from dsctl.models.workflow_spec import load_workflow_spec
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
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services.resolver import ResolvedProject
    from dsctl.upstream.protocol import WorkflowDagRecord


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
    if spec.schedule is not None:
        message = (
            "workflow edit --file does not mutate schedule blocks; remove "
            "`schedule:` and use schedule commands separately."
        )
        raise UserInputError(
            message,
            details={"file": str(path), "unsupported_block": "schedule"},
            suggestion=(
                "Remove the schedule block, then use `dsctl schedule update|online|"
                "offline` for schedule lifecycle changes."
            ),
        )
    return spec


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
        release_state=release_state,
        task_identities=patch_task_identities(live_baseline.task_identities, diff=diff),
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
        release_state=release_state,
        task_identities=_desired_task_identities(
            live_baseline.task_identities,
            desired=merged_spec,
        ),
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
