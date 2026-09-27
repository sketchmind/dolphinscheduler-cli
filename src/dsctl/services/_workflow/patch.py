from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, TypedDict, cast

from pydantic import ValidationError

from dsctl.errors import UserInputError
from dsctl.models.common import (
    YamlObject,
    first_validation_error_message,
)
from dsctl.models.task_spec import normalize_task_params
from dsctl.models.workflow_spec import (
    WorkflowAuthoringContext,
    WorkflowMetadataSpec,
    WorkflowSpec,
    WorkflowTaskSpec,
    validate_workflow_document,
    validate_workflow_task_document,
)
from dsctl.output import require_json_object, require_json_value
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.task_authoring_catalog import TaskAuthoringIntent
from dsctl.upstream.task_references import rename_task_references
from dsctl.upstream.task_settings import (
    task_environment_code_value,
    task_flag_value,
    task_group_values,
    task_resource_limit_value,
    task_timeout_settings,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.models.workflow_patch import (
        WorkflowPatchSpec,
        WorkflowPatchTaskRenameSpec,
        WorkflowPatchTaskSetSpec,
        WorkflowPatchTaskUpdateSpec,
        WorkflowPatchWorkflowSetSpec,
    )
    from dsctl.output import JsonObject, JsonValue
    from dsctl.services.task_authoring_catalog import TaskAuthoringCatalog


class WorkflowRenameDiffData(TypedDict):
    """One explicit task rename emitted by workflow edit dry runs."""

    from_name: str
    to_name: str


class WorkflowEdgeDiffData(TypedDict):
    """One DAG edge delta emitted by workflow edit dry runs."""

    from_task: str
    to_task: str


class WorkflowFieldChangeData(TypedDict):
    """One canonical authoring field change with its live and desired values."""

    field: str
    before: JsonValue
    after: JsonValue


class WorkflowTaskChangeData(TypedDict):
    """Canonical field changes for one existing workflow task."""

    task: str
    changes: list[WorkflowFieldChangeData]


class WorkflowPatchDiffData(TypedDict):
    """Internal workflow patch facts shared by validation and dry runs."""

    workflow_updated_fields: list[str]
    workflow_changes: list[WorkflowFieldChangeData]
    added_tasks: list[str]
    updated_tasks: list[str]
    task_changes: list[WorkflowTaskChangeData]
    renamed_tasks: list[WorkflowRenameDiffData]
    deleted_tasks: list[str]
    added_edges: list[WorkflowEdgeDiffData]
    removed_edges: list[WorkflowEdgeDiffData]
    dag_valid: bool


class WorkflowPatchDiffOutputData(TypedDict):
    """Value-level workflow patch diff exposed by authoring dry runs."""

    workflow_changes: list[WorkflowFieldChangeData]
    added_tasks: list[str]
    task_changes: list[WorkflowTaskChangeData]
    renamed_tasks: list[WorkflowRenameDiffData]
    deleted_tasks: list[str]
    added_edges: list[WorkflowEdgeDiffData]
    removed_edges: list[WorkflowEdgeDiffData]
    dag_valid: bool


_WORKFLOW_PATCH_DRY_RUN_SUGGESTION = (
    "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
    "inspect the compiled diff before applying it."
)
WORKFLOW_TASK_AUTHORING_FIELDS = frozenset({"type", "task_params", "command"})


def apply_workflow_patch(
    baseline: WorkflowSpec,
    patch: WorkflowPatchSpec,
    *,
    edge_builder: Callable[[list[WorkflowTaskSpec]], list[tuple[str, str]]],
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[WorkflowSpec, WorkflowPatchDiffData]:
    """Apply one validated workflow patch to a live workflow spec snapshot."""
    typed_context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    preserve_context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    rename_context = replace(
        preserve_context,
        validate_task_identity=typed_context.validate_task_identity,
    )
    baseline = _validate_workflow_spec(
        baseline.model_dump(mode="python", exclude_none=False),
        authoring_context=preserve_context,
    )
    workflow = baseline.workflow.model_copy(deep=True)
    tasks = [task.model_copy(deep=True) for task in baseline.tasks]
    original_by_name = {
        task.name: task.model_copy(deep=True) for task in baseline.tasks
    }
    live_names = set(original_by_name)

    task_patch = patch.tasks
    rename_ops = [] if task_patch is None else task_patch.rename
    update_ops = [] if task_patch is None else task_patch.update
    delete_names = [] if task_patch is None else task_patch.delete
    create_tasks = [] if task_patch is None else task_patch.create
    create_tasks = [
        _validate_task_spec(
            task.model_dump(mode="python", exclude_none=False),
            authoring_context=typed_context,
        )
        for task in create_tasks
    ]

    rename_map = _rename_map(rename_ops)
    _validate_task_operations(
        live_names=live_names,
        rename_ops=rename_ops,
        update_ops=update_ops,
        delete_names=delete_names,
        create_tasks=create_tasks,
    )

    if patch.workflow is not None:
        workflow = _apply_workflow_set(workflow, patch.workflow.set)

    renamed_tasks = [
        _rename_task(
            task,
            rename_map,
            authoring_context=rename_context,
        )
        for task in tasks
    ]
    tasks_by_name = {task.name: task for task in renamed_tasks}

    updated_task_names: list[str] = []
    for update_op in update_ops:
        live_name = update_op.match.name
        current_name = rename_map.get(live_name, live_name)
        current_task = tasks_by_name[current_name]
        updated_task = _apply_task_set(
            current_task,
            update_op.set,
            typed_authoring_context=typed_context,
            preserve_authoring_context=preserve_context,
        )
        tasks_by_name[current_name] = updated_task
        updated_task_names.append(current_name)

    deleted_current_names = {rename_map.get(name, name) for name in delete_names}
    tasks_after_delete = [
        tasks_by_name[name]
        for name in (task.name for task in renamed_tasks)
        if name not in deleted_current_names
    ]
    tasks_by_name = {task.name: task for task in tasks_after_delete}

    for create_task in create_tasks:
        if create_task.name in tasks_by_name:
            message = (
                f"Patch creates task '{create_task.name}', but that name already exists"
            )
            raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)
        tasks_after_delete.append(create_task.model_copy(deep=True))
        tasks_by_name[create_task.name] = tasks_after_delete[-1]

    rewritten_tasks = [
        _rewrite_task_refs(
            task,
            rename_map,
            authoring_context=preserve_context,
        )
        for task in tasks_after_delete
    ]
    merged = _validate_workflow_spec(
        {
            "workflow": workflow.model_dump(mode="python"),
            "tasks": [task.model_dump(mode="python") for task in rewritten_tasks],
            "schedule": (
                None
                if baseline.schedule is None
                else baseline.schedule.model_dump(mode="python")
            ),
        },
        authoring_context=preserve_context,
    )

    workflow_updated_fields = _workflow_updated_fields(baseline, merged, patch=patch)
    task_changes = _patched_task_changes(
        original_by_name=original_by_name,
        merged=merged,
        rename_map=rename_map,
        updated_task_names=sorted(
            rename_map.get(update.match.name, update.match.name)
            for update in update_ops
        ),
        preserve_authoring_context=preserve_context,
    )
    updated_tasks = [item["task"] for item in task_changes]
    before_edges = set(edge_builder(baseline.tasks))
    after_edges = set(edge_builder(merged.tasks))
    added_edges = sorted(after_edges - before_edges)
    removed_edges = sorted(before_edges - after_edges)

    diff: WorkflowPatchDiffData = {
        "workflow_updated_fields": workflow_updated_fields,
        "workflow_changes": _workflow_field_changes(
            baseline,
            merged,
            field_names=workflow_updated_fields,
        ),
        "added_tasks": sorted(task.name for task in create_tasks),
        "updated_tasks": updated_tasks,
        "task_changes": task_changes,
        "renamed_tasks": [
            {
                "from_name": rename.from_name,
                "to_name": rename.to_name,
            }
            for rename in rename_ops
        ],
        "deleted_tasks": sorted(delete_names),
        "added_edges": [_edge_diff_item(edge) for edge in added_edges],
        "removed_edges": [_edge_diff_item(edge) for edge in removed_edges],
        "dag_valid": True,
    }
    return merged, diff


def reconcile_workflow_spec(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
    *,
    edge_builder: Callable[[list[WorkflowTaskSpec]], list[tuple[str, str]]],
    catalog: TaskAuthoringCatalog | None = None,
) -> tuple[WorkflowSpec, WorkflowPatchDiffData]:
    """Build a desired-state workflow edit diff against one live baseline.

    Full-file edits intentionally match task identity by exact task name only.
    Rename preservation remains an explicit `workflow edit --patch` operation.
    """
    typed_context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    preserve_context = workflow_authoring_context(
        catalog=catalog,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    baseline = _validate_workflow_spec(
        baseline.model_dump(mode="python", exclude_none=False),
        authoring_context=preserve_context,
    )
    desired = _validate_workflow_spec(
        desired.model_dump(mode="python", exclude_none=False),
        authoring_context=preserve_context,
    )
    baseline_by_name = {task.name: task for task in baseline.tasks}
    desired_tasks = [
        _validate_task_spec(
            task.model_dump(mode="python", exclude_none=False),
            authoring_context=typed_context,
        )
        if workflow_task_authoring_changed(baseline_by_name.get(task.name), task)
        else task.model_copy(deep=True)
        for task in desired.tasks
    ]
    desired = _validate_workflow_spec(
        {
            "workflow": desired.workflow.model_dump(mode="python"),
            "tasks": [task.model_dump(mode="python") for task in desired_tasks],
            "schedule": (
                None
                if desired.schedule is None
                else desired.schedule.model_dump(mode="python")
            ),
        },
        authoring_context=preserve_context,
    )
    before_edges = set(edge_builder(baseline.tasks))
    after_edges = set(edge_builder(desired.tasks))
    desired_by_name = {task.name: task for task in desired.tasks}
    shared_names = set(baseline_by_name) & set(desired_by_name)
    added_edges = sorted(after_edges - before_edges)
    removed_edges = sorted(before_edges - after_edges)

    workflow_updated_fields = _desired_workflow_updated_fields(baseline, desired)
    task_changes: list[WorkflowTaskChangeData] = []
    for name in sorted(shared_names):
        field_changes = _task_field_changes(
            baseline_by_name[name],
            desired_by_name[name],
        )
        if field_changes:
            task_changes.append({"task": name, "changes": field_changes})
    updated_tasks = [item["task"] for item in task_changes]
    diff: WorkflowPatchDiffData = {
        "workflow_updated_fields": workflow_updated_fields,
        "workflow_changes": _workflow_field_changes(
            baseline,
            desired,
            field_names=workflow_updated_fields,
        ),
        "added_tasks": sorted(set(desired_by_name) - set(baseline_by_name)),
        "updated_tasks": updated_tasks,
        "task_changes": task_changes,
        "renamed_tasks": [],
        "deleted_tasks": sorted(set(baseline_by_name) - set(desired_by_name)),
        "added_edges": [_edge_diff_item(edge) for edge in added_edges],
        "removed_edges": [_edge_diff_item(edge) for edge in removed_edges],
        "dag_valid": True,
    }
    return desired.model_copy(deep=True), diff


def workflow_patch_diff_output(
    diff: WorkflowPatchDiffData,
) -> WorkflowPatchDiffOutputData:
    """Project internal change facts into the non-redundant public diff shape."""
    return {
        "workflow_changes": diff["workflow_changes"],
        "added_tasks": diff["added_tasks"],
        "task_changes": diff["task_changes"],
        "renamed_tasks": diff["renamed_tasks"],
        "deleted_tasks": diff["deleted_tasks"],
        "added_edges": diff["added_edges"],
        "removed_edges": diff["removed_edges"],
        "dag_valid": diff["dag_valid"],
    }


def patch_has_changes(diff: WorkflowPatchDiffData) -> bool:
    """Return whether one workflow patch diff changes any persistent state."""
    return any(
        (
            diff["workflow_updated_fields"],
            diff["added_tasks"],
            diff["updated_tasks"],
            diff["renamed_tasks"],
            diff["deleted_tasks"],
            diff["added_edges"],
            diff["removed_edges"],
        )
    )


def _desired_workflow_updated_fields(
    baseline: WorkflowSpec,
    desired: WorkflowSpec,
) -> list[str]:
    compared_fields = (
        "name",
        "description",
        "timeout",
        "global_params",
        "execution_type",
        "release_state",
    )
    return [
        field_name
        for field_name in compared_fields
        if getattr(baseline.workflow, field_name)
        != getattr(desired.workflow, field_name)
    ]


def _workflow_field_changes(
    baseline: WorkflowSpec,
    merged: WorkflowSpec,
    *,
    field_names: list[str],
) -> list[WorkflowFieldChangeData]:
    before = baseline.workflow.model_dump(mode="json", exclude_none=False)
    after = merged.workflow.model_dump(mode="json", exclude_none=False)
    return [
        {
            "field": field_name,
            "before": require_json_value(
                before[field_name],
                label=f"workflow diff before {field_name}",
            ),
            "after": require_json_value(
                after[field_name],
                label=f"workflow diff after {field_name}",
            ),
        }
        for field_name in field_names
    ]


def _patched_task_changes(
    *,
    original_by_name: Mapping[str, WorkflowTaskSpec],
    merged: WorkflowSpec,
    rename_map: Mapping[str, str],
    updated_task_names: list[str],
    preserve_authoring_context: WorkflowAuthoringContext,
) -> list[WorkflowTaskChangeData]:
    merged_by_name = {task.name: task for task in merged.tasks}
    original_name_by_current = {
        rename_map.get(original_name, original_name): original_name
        for original_name in original_by_name
    }
    changes: list[WorkflowTaskChangeData] = []
    for current_name in updated_task_names:
        original_name = original_name_by_current[current_name]
        baseline_task = _normalized_original_task(
            original_by_name[original_name],
            current_name=current_name,
            rename_map=rename_map,
            authoring_context=preserve_authoring_context,
        )
        field_changes = _task_field_changes(
            baseline_task,
            merged_by_name[current_name],
        )
        if field_changes:
            changes.append({"task": current_name, "changes": field_changes})
    return changes


def _task_field_changes(
    baseline: WorkflowTaskSpec,
    merged: WorkflowTaskSpec,
) -> list[WorkflowFieldChangeData]:
    before = _task_dump(baseline)
    after = _task_dump(merged)
    omitted_fields = {"name"}
    return [
        {
            "field": field_name,
            "before": require_json_value(
                before[field_name],
                label=f"task diff before {field_name}",
            ),
            "after": require_json_value(
                after[field_name],
                label=f"task diff after {field_name}",
            ),
        }
        for field_name in before
        if field_name not in omitted_fields and before[field_name] != after[field_name]
    ]


def workflow_task_authoring_changed(
    baseline: WorkflowTaskSpec | None,
    desired: WorkflowTaskSpec,
) -> bool:
    if baseline is None:
        return True
    return any(
        (
            baseline.type != desired.type,
            baseline.task_params != desired.task_params,
            baseline.command != desired.command,
        )
    )


def _rename_map(
    rename_ops: list[WorkflowPatchTaskRenameSpec],
) -> dict[str, str]:
    rename_map: dict[str, str] = {}
    for rename in rename_ops:
        if rename.from_name in rename_map:
            message = f"Patch renames task '{rename.from_name}' more than once"
            raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)
        rename_map[rename.from_name] = rename.to_name
    return rename_map


def _validate_task_operations(
    *,
    live_names: set[str],
    rename_ops: list[WorkflowPatchTaskRenameSpec],
    update_ops: list[WorkflowPatchTaskUpdateSpec],
    delete_names: list[str],
    create_tasks: list[WorkflowTaskSpec],
) -> None:
    rename_sources = {rename.from_name for rename in rename_ops}
    rename_targets = {rename.to_name for rename in rename_ops}
    update_matches = {update.match.name for update in update_ops}
    create_names = [task.name for task in create_tasks]

    _validate_unique_task_operations(
        rename_ops=rename_ops,
        update_ops=update_ops,
        create_names=create_names,
    )
    _ensure_live_task_refs_exist(
        live_names=live_names,
        referenced_names=rename_sources | update_matches | set(delete_names),
    )
    _ensure_non_conflicting_task_operations(
        live_names=live_names,
        rename_sources=rename_sources,
        rename_targets=rename_targets,
        update_matches=update_matches,
        delete_names=set(delete_names),
    )
    _ensure_create_names_available(
        create_names=create_names,
        live_names=live_names,
        rename_targets=rename_targets,
    )


def _validate_unique_task_operations(
    *,
    rename_ops: list[WorkflowPatchTaskRenameSpec],
    update_ops: list[WorkflowPatchTaskUpdateSpec],
    create_names: list[str],
) -> None:
    rename_targets = {rename.to_name for rename in rename_ops}
    update_matches = {update.match.name for update in update_ops}
    if len(rename_targets) != len(rename_ops):
        message = "Patch cannot rename multiple tasks to the same target name"
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)
    if len(update_matches) != len(update_ops):
        message = "Patch cannot update the same task more than once"
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)
    if len(set(create_names)) != len(create_names):
        message = "Patch cannot create multiple tasks with the same name"
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)


def _ensure_live_task_refs_exist(
    *,
    live_names: set[str],
    referenced_names: set[str],
) -> None:
    for name in referenced_names:
        if name not in live_names:
            message = f"Patch references unknown live task '{name}'"
            raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)


def _ensure_non_conflicting_task_operations(
    *,
    live_names: set[str],
    rename_sources: set[str],
    rename_targets: set[str],
    update_matches: set[str],
    delete_names: set[str],
) -> None:
    duplicate_targets = live_names & rename_targets
    if duplicate_targets:
        duplicate = sorted(duplicate_targets)[0]
        message = (
            f"Patch renames a task to '{duplicate}', but that name already exists "
            "in the live workflow"
        )
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)

    if rename_sources & delete_names:
        duplicate = sorted(rename_sources & delete_names)[0]
        message = f"Patch cannot rename and delete task '{duplicate}' in the same edit"
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)
    if update_matches & delete_names:
        duplicate = sorted(update_matches & delete_names)[0]
        message = f"Patch cannot update and delete task '{duplicate}' in the same edit"
        raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)


def _ensure_create_names_available(
    *,
    create_names: list[str],
    live_names: set[str],
    rename_targets: set[str],
) -> None:
    for name in create_names:
        if name in live_names or name in rename_targets:
            message = f"Patch creates task '{name}', but that name is already reserved"
            raise UserInputError(message, suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION)


def _apply_workflow_set(
    workflow: WorkflowMetadataSpec,
    patch_set: WorkflowPatchWorkflowSetSpec,
) -> WorkflowMetadataSpec:
    payload = workflow.model_dump(mode="python", exclude_none=False)
    for field_name in patch_set.model_fields_set:
        payload[field_name] = getattr(patch_set, field_name)
    try:
        return WorkflowMetadataSpec.model_validate(payload)
    except ValidationError as exc:
        raise UserInputError(
            first_validation_error_message(exc),
            suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION,
        ) from exc


def _rename_task(
    task: WorkflowTaskSpec,
    rename_map: Mapping[str, str],
    *,
    authoring_context: WorkflowAuthoringContext,
) -> WorkflowTaskSpec:
    current_name = rename_map.get(task.name)
    if current_name is None:
        return task.model_copy(deep=True)
    payload = task.model_dump(mode="python", exclude_none=False)
    payload["name"] = current_name
    return _validate_task_spec(payload, authoring_context=authoring_context)


def _apply_task_set(
    task: WorkflowTaskSpec,
    patch_set: WorkflowPatchTaskSetSpec,
    *,
    typed_authoring_context: WorkflowAuthoringContext,
    preserve_authoring_context: WorkflowAuthoringContext,
) -> WorkflowTaskSpec:
    payload = task.model_dump(mode="python", exclude_none=False)
    provided_fields = set(patch_set.model_fields_set)
    if "command" in provided_fields and "task_params" not in provided_fields:
        payload["task_params"] = None
    if "task_params" in provided_fields and "command" not in provided_fields:
        payload["command"] = None
    for field_name in provided_fields:
        payload[field_name] = getattr(patch_set, field_name)
    authoring_context = (
        typed_authoring_context
        if provided_fields.intersection(WORKFLOW_TASK_AUTHORING_FIELDS)
        else preserve_authoring_context
    )
    return _validate_task_spec(payload, authoring_context=authoring_context)


def _rewrite_task_refs(
    task: WorkflowTaskSpec,
    rename_map: Mapping[str, str],
    *,
    authoring_context: WorkflowAuthoringContext,
) -> WorkflowTaskSpec:
    if not rename_map:
        return task.model_copy(deep=True)
    payload = task.model_dump(mode="python", exclude_none=False)
    payload["depends_on"] = [
        rename_map.get(dependency, dependency) for dependency in task.depends_on
    ]
    if task.task_params is not None:
        payload["task_params"] = rename_task_references(
            task.type.upper(), cast("JsonObject", task.task_params), rename_map
        )
    return _validate_task_spec(payload, authoring_context=authoring_context)


def _workflow_updated_fields(
    baseline: WorkflowSpec,
    merged: WorkflowSpec,
    *,
    patch: WorkflowPatchSpec,
) -> list[str]:
    if patch.workflow is None:
        return []
    return [
        field_name
        for field_name in sorted(patch.workflow.set.model_fields_set)
        if getattr(baseline.workflow, field_name)
        != getattr(merged.workflow, field_name)
    ]


def _normalized_original_task(
    task: WorkflowTaskSpec,
    *,
    current_name: str,
    rename_map: Mapping[str, str],
    authoring_context: WorkflowAuthoringContext,
) -> WorkflowTaskSpec:
    renamed = task
    if task.name != current_name:
        payload = task.model_dump(mode="python", exclude_none=False)
        payload["name"] = current_name
        renamed = _validate_task_spec(
            payload,
            authoring_context=authoring_context,
        )
    return _rewrite_task_refs(
        renamed,
        rename_map,
        authoring_context=authoring_context,
    )


def _task_dump(task: WorkflowTaskSpec) -> JsonObject:
    payload = task.model_dump(mode="json", exclude_none=False)
    if task.command is not None:
        payload["task_params"] = normalize_task_params(
            task.type,
            {"rawScript": task.command},
        )
        payload["command"] = None
    payload["description"] = "" if task.description is None else task.description
    payload["flag"] = task_flag_value(task.flag)
    payload["worker_group"] = (
        "default" if task.worker_group is None else task.worker_group
    )
    payload["environment_code"] = task_environment_code_value(task.environment_code)
    task_group_id, task_group_priority = task_group_values(
        task.task_group_id,
        task.task_group_priority,
    )
    payload["task_group_id"] = 0 if task_group_id is None else task_group_id
    payload["task_group_priority"] = (
        0 if task_group_priority is None else task_group_priority
    )
    timeout_notify_strategy = (
        None
        if task.timeout_notify_strategy is None
        else task.timeout_notify_strategy.value
    )
    payload["timeout_notify_strategy"] = task_timeout_settings(
        task.timeout,
        notify_strategy=timeout_notify_strategy,
    )[1]
    payload["cpu_quota"] = task_resource_limit_value(task.cpu_quota)
    payload["memory_max"] = task_resource_limit_value(task.memory_max)
    return require_json_object(payload, label="normalized workflow task")


def _validate_task_spec(
    payload: YamlObject,
    *,
    authoring_context: WorkflowAuthoringContext,
) -> WorkflowTaskSpec:
    try:
        return validate_workflow_task_document(
            payload,
            authoring_context=authoring_context,
        )
    except ValidationError as exc:
        raise UserInputError(
            first_validation_error_message(exc),
            suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION,
        ) from exc


def _validate_workflow_spec(
    payload: YamlObject,
    *,
    authoring_context: WorkflowAuthoringContext,
) -> WorkflowSpec:
    try:
        return validate_workflow_document(
            payload,
            authoring_context=authoring_context,
        )
    except ValidationError as exc:
        raise UserInputError(
            first_validation_error_message(exc),
            suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION,
        ) from exc


def _edge_diff_item(edge: tuple[str, str]) -> WorkflowEdgeDiffData:
    predecessor, successor = edge
    return {
        "from_task": predecessor,
        "to_task": successor,
    }
