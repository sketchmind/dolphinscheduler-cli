from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, TypedDict, cast

from pydantic import ValidationError

from dsctl.errors import UserInputError
from dsctl.models.common import YamlObject, first_validation_error_message
from dsctl.models.workflow_spec import (
    WorkflowMetadataSpec,
    WorkflowSpec,
    WorkflowTaskSpec,
)
from dsctl.services._task_settings import (
    task_environment_code_value,
    task_flag_value,
    task_group_values,
    task_resource_limit_value,
    task_timeout_settings,
)

if TYPE_CHECKING:
    from dsctl.models.workflow_patch import (
        WorkflowPatchSpec,
        WorkflowPatchTaskRenameSpec,
        WorkflowPatchTaskSetSpec,
        WorkflowPatchTaskUpdateSpec,
        WorkflowPatchWorkflowSetSpec,
    )


class WorkflowRenameDiffData(TypedDict):
    """One explicit task rename emitted by workflow edit dry runs."""

    from_name: str
    to_name: str


class WorkflowEdgeDiffData(TypedDict):
    """One DAG edge delta emitted by workflow edit dry runs."""

    from_task: str
    to_task: str


class WorkflowPatchDiffData(TypedDict):
    """Stable workflow patch diff emitted by workflow edit dry runs."""

    workflow_updated_fields: list[str]
    added_tasks: list[str]
    updated_tasks: list[str]
    renamed_tasks: list[WorkflowRenameDiffData]
    deleted_tasks: list[str]
    added_edges: list[WorkflowEdgeDiffData]
    removed_edges: list[WorkflowEdgeDiffData]
    dag_valid: bool


_WORKFLOW_PATCH_DRY_RUN_SUGGESTION = (
    "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
    "inspect the compiled diff before applying it."
)


def apply_workflow_patch(
    baseline: WorkflowSpec,
    patch: WorkflowPatchSpec,
    *,
    edge_builder: Callable[[list[WorkflowTaskSpec]], list[tuple[str, str]]],
) -> tuple[WorkflowSpec, WorkflowPatchDiffData]:
    """Apply one validated workflow patch to a live workflow spec snapshot."""
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

    renamed_tasks = [_rename_task(task, rename_map) for task in tasks]
    tasks_by_name = {task.name: task for task in renamed_tasks}

    updated_task_names: list[str] = []
    for update_op in update_ops:
        live_name = update_op.match.name
        current_name = rename_map.get(live_name, live_name)
        current_task = tasks_by_name[current_name]
        updated_task = _apply_task_set(current_task, update_op.set)
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
        _rewrite_task_refs(task, rename_map) for task in tasks_after_delete
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
        }
    )

    workflow_updated_fields = _workflow_updated_fields(baseline, merged, patch=patch)
    updated_tasks = _updated_task_names(
        original_by_name=original_by_name,
        merged=merged,
        rename_map=rename_map,
        update_ops=update_ops,
    )
    before_edges = set(edge_builder(baseline.tasks))
    after_edges = set(edge_builder(merged.tasks))
    added_edges = sorted(after_edges - before_edges)
    removed_edges = sorted(before_edges - after_edges)

    diff: WorkflowPatchDiffData = {
        "workflow_updated_fields": workflow_updated_fields,
        "added_tasks": sorted(task.name for task in create_tasks),
        "updated_tasks": updated_tasks,
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
) -> WorkflowTaskSpec:
    current_name = rename_map.get(task.name)
    if current_name is None:
        return task.model_copy(deep=True)
    payload = task.model_dump(mode="python", exclude_none=False)
    payload["name"] = current_name
    return _validate_task_spec(payload)


def _apply_task_set(
    task: WorkflowTaskSpec,
    patch_set: WorkflowPatchTaskSetSpec,
) -> WorkflowTaskSpec:
    payload = task.model_dump(mode="python", exclude_none=False)
    provided_fields = set(patch_set.model_fields_set)
    if "command" in provided_fields and "task_params" not in provided_fields:
        payload["task_params"] = None
    if "task_params" in provided_fields and "command" not in provided_fields:
        payload["command"] = None
    for field_name in provided_fields:
        payload[field_name] = getattr(patch_set, field_name)
    return _validate_task_spec(payload)


def _rewrite_task_refs(
    task: WorkflowTaskSpec,
    rename_map: Mapping[str, str],
) -> WorkflowTaskSpec:
    if not rename_map:
        return task.model_copy(deep=True)
    payload = task.model_dump(mode="python", exclude_none=False)
    payload["depends_on"] = [
        rename_map.get(dependency, dependency) for dependency in task.depends_on
    ]
    if task.task_params is not None:
        payload["task_params"] = _rewrite_task_params_refs(
            task_type=task.type.upper(),
            payload=task.task_params,
            rename_map=rename_map,
        )
    return _validate_task_spec(payload)


def _rewrite_task_params_refs(
    *,
    task_type: str,
    payload: YamlObject,
    rename_map: Mapping[str, str],
) -> YamlObject:
    if task_type == "SWITCH":
        return _rewrite_switch_task_params_refs(payload, rename_map=rename_map)
    if task_type == "CONDITIONS":
        return _rewrite_conditions_task_params_refs(payload, rename_map=rename_map)
    return dict(payload)


def _rewrite_switch_task_params_refs(
    payload: YamlObject,
    *,
    rename_map: Mapping[str, str],
) -> YamlObject:
    rewritten: dict[str, object] = dict(payload)
    switch_result = payload.get("switchResult")
    if isinstance(switch_result, Mapping):
        rewritten_switch_result: dict[str, object] = dict(switch_result)
        depend_task_list = switch_result.get("dependTaskList")
        if isinstance(depend_task_list, list):
            rewritten_depend_task_list: list[dict[str, object]] = []
            for branch in depend_task_list:
                if not isinstance(branch, Mapping):
                    continue
                rewritten_branch: dict[str, object] = dict(branch)
                next_node = branch.get("nextNode")
                if isinstance(next_node, str):
                    rewritten_branch["nextNode"] = rename_map.get(next_node, next_node)
                rewritten_depend_task_list.append(rewritten_branch)
            rewritten_switch_result["dependTaskList"] = cast(
                "YamlObject",
                {"items": rewritten_depend_task_list},
            )["items"]
        default_next_node = switch_result.get("nextNode")
        if isinstance(default_next_node, str):
            rewritten_switch_result["nextNode"] = rename_map.get(
                default_next_node,
                default_next_node,
            )
        rewritten["switchResult"] = rewritten_switch_result
    next_branch = payload.get("nextBranch")
    if isinstance(next_branch, str):
        rewritten["nextBranch"] = rename_map.get(next_branch, next_branch)
    return cast("YamlObject", rewritten)


def _rewrite_conditions_task_params_refs(
    payload: YamlObject,
    *,
    rename_map: Mapping[str, str],
) -> YamlObject:
    rewritten: dict[str, object] = dict(payload)
    condition_result = payload.get("conditionResult")
    if not isinstance(condition_result, Mapping):
        return cast("YamlObject", rewritten)
    rewritten_condition_result: dict[str, object] = dict(condition_result)
    for key in ("successNode", "failedNode"):
        nodes = condition_result.get(key)
        if not isinstance(nodes, list):
            continue
        rewritten_condition_result[key] = [
            rename_map.get(node, node) if isinstance(node, str) else node
            for node in nodes
        ]
    rewritten["conditionResult"] = rewritten_condition_result
    return cast("YamlObject", rewritten)


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


def _updated_task_names(
    *,
    original_by_name: Mapping[str, WorkflowTaskSpec],
    merged: WorkflowSpec,
    rename_map: Mapping[str, str],
    update_ops: list[WorkflowPatchTaskUpdateSpec],
) -> list[str]:
    merged_by_name = {task.name: task for task in merged.tasks}
    updated_names: list[str] = []
    for update in update_ops:
        live_name = update.match.name
        current_name = rename_map.get(live_name, live_name)
        baseline_task = _normalized_original_task(
            original_by_name[live_name],
            current_name=current_name,
            rename_map=rename_map,
        )
        if _task_dump(baseline_task) != _task_dump(merged_by_name[current_name]):
            updated_names.append(current_name)
    return sorted(updated_names)


def _normalized_original_task(
    task: WorkflowTaskSpec,
    *,
    current_name: str,
    rename_map: Mapping[str, str],
) -> WorkflowTaskSpec:
    renamed = task
    if task.name != current_name:
        payload = task.model_dump(mode="python", exclude_none=False)
        payload["name"] = current_name
        renamed = _validate_task_spec(payload)
    return _rewrite_task_refs(renamed, rename_map)


def _task_dump(task: WorkflowTaskSpec) -> dict[str, object]:
    payload = task.model_dump(mode="python", exclude_none=False)
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
    return payload


def _validate_task_spec(payload: YamlObject) -> WorkflowTaskSpec:
    try:
        return WorkflowTaskSpec.model_validate(payload)
    except ValidationError as exc:
        raise UserInputError(
            first_validation_error_message(exc),
            suggestion=_WORKFLOW_PATCH_DRY_RUN_SUGGESTION,
        ) from exc


def _validate_workflow_spec(payload: YamlObject) -> WorkflowSpec:
    try:
        return WorkflowSpec.model_validate(payload)
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
