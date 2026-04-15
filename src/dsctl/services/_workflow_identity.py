from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dsctl.cli_surface import WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.services._serialization import optional_text, require_resource_int

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.models.workflow_spec import WorkflowSpec
    from dsctl.services._workflow_patch import WorkflowPatchDiffData
    from dsctl.upstream.protocol import WorkflowDagRecord


@dataclass(frozen=True)
class WorkflowTaskIdentity:
    """Stable DS task identity carried across whole-definition workflow edits."""

    code: int
    version: int


@dataclass(frozen=True)
class WorkflowLiveBaseline:
    """Live workflow edit baseline with authoring spec and DS task identities."""

    spec: WorkflowSpec
    task_identities: dict[str, WorkflowTaskIdentity]


def task_identities_by_name(
    dag: WorkflowDagRecord,
) -> dict[str, WorkflowTaskIdentity]:
    """Extract current DS task identities from one workflow DAG payload."""
    identities: dict[str, WorkflowTaskIdentity] = {}
    for task in dag.taskDefinitionList or []:
        name = optional_text(task.name)
        if name is None:
            message = "Workflow DAG payload was missing task.name"
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        if name in identities:
            message = f"Workflow DAG payload contained duplicate task name '{name}'"
            raise ApiTransportError(message, details={"resource": WORKFLOW_RESOURCE})
        identities[name] = WorkflowTaskIdentity(
            code=require_resource_int(
                task.code,
                resource=WORKFLOW_RESOURCE,
                field_name="task.code",
            ),
            version=require_resource_int(
                task.version,
                resource=WORKFLOW_RESOURCE,
                field_name="task.version",
            ),
        )
    return identities


def patch_task_identities(
    baseline: Mapping[str, WorkflowTaskIdentity],
    *,
    diff: WorkflowPatchDiffData,
) -> dict[str, WorkflowTaskIdentity]:
    """Rewrite live task identities to match one applied workflow patch diff."""
    task_identities = dict(baseline)
    for rename in diff["renamed_tasks"]:
        source = rename["from_name"]
        target = rename["to_name"]
        identity = task_identities.pop(source, None)
        if identity is None:
            message = f"Workflow patch diff referenced unknown live task '{source}'"
            raise RuntimeError(message)
        task_identities[target] = identity
    for name in diff["deleted_tasks"]:
        task_identities.pop(name, None)
    return task_identities
