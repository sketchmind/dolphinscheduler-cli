from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.errors import UserInputError
from dsctl.models.workflow_patch import load_workflow_patch
from dsctl.services._workflow_compile import (
    WorkflowUpdatePayload,
    compile_workflow_update_payload,
    workflow_edges,
)
from dsctl.services._workflow_identity import patch_task_identities
from dsctl.services._workflow_patch import (
    WorkflowPatchDiffData,
    apply_workflow_patch,
    patch_has_changes,
)
from dsctl.services._workflow_render import workflow_live_baseline

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models.workflow_patch import WorkflowPatchSpec
    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services.resolver import ResolvedProject
    from dsctl.upstream.protocol import WorkflowDagRecord


@dataclass(frozen=True)
class WorkflowMutationPlan:
    """Compiled workflow patch plan shared by definition and instance edits."""

    merged_spec: WorkflowSpec
    diff: WorkflowPatchDiffData
    payload: WorkflowUpdatePayload
    has_changes: bool


_WORKFLOW_PATCH_PARSE_SUGGESTION = (
    "Fix the patch YAML, then retry the same command with `--dry-run` to "
    "inspect the compiled diff before apply."
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
    )
