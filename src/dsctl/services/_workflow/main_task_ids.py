"""DS 3.1.0 main-table task identities for synchronized graph edits."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
)

# Exact DS 3.1.0 Status values returned by queryTaskDefinitionDetail.
_TASK_PERMISSION_RESULT_CODES = frozenset({30001, 30002})
_TASK_NOT_FOUND_RESULT_CODES = frozenset({50030})

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence

    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.upstream.protocol import TaskPayloadRecord, WorkflowDagRecord
    from dsctl.upstream.task_definition_wire import TaskDefinitionWire


def changed_task_names_by_code(
    mutation: WorkflowMutationPlan,
    allocated_task_codes: Sequence[int],
) -> dict[int, str]:
    """Identify tasks whose substantive native detail should advance."""
    names = set(mutation.diff["updated_tasks"])
    names.update(mutation.diff["added_tasks"])
    names.update(item["to_name"] for item in mutation.diff["renamed_tasks"])
    codes_by_name = mutation.compilation.task_codes_by_name(allocated_task_codes)
    return {codes_by_name[name]: name for name in names}


def read_main_task_ids(
    wire: TaskDefinitionWire | None,
    *,
    project_code: int,
    task_codes: Collection[int],
    resource: str,
) -> dict[int, int]:
    """Read ids from task_get's main table, never from the DAG's log rows."""
    if wire is None:
        raise _identity_error(resource, "DS 3.1.0 task detail is unavailable")
    result: dict[int, int] = {}
    for code in sorted(set(task_codes)):
        if type(code) is not int or code <= 0:
            raise _identity_error(resource, "Existing task code is invalid")
        task = _get_main_task(
            wire, project_code=project_code, code=code, resource=resource
        )
        if task.projectCode != project_code or task.code != code:
            raise _identity_error(resource, "Task detail returned a different identity")
        if type(task.id) is not int or task.id <= 0:
            raise _identity_error(resource, "Task detail omitted a positive main id")
        result[code] = task.id
    if len(set(result.values())) != len(result):
        raise _identity_error(resource, "Task details reused one main id")
    return result


def verify_main_task_views(
    wire: TaskDefinitionWire | None,
    *,
    project_code: int,
    dag: WorkflowDagRecord | None,
    changed_names_by_code: Mapping[int, str],
    original_ids: Mapping[int, int],
    resource: str,
) -> None:
    """Check the changed tasks in both the saved DAG and main-table detail."""
    if not changed_names_by_code:
        return
    if wire is None or dag is None:
        raise _identity_error(resource, "Task readback is unavailable", applied=True)
    dag_by_code = {}
    for task in dag.taskDefinitionList or ():
        if type(task.code) is int and task.code in changed_names_by_code:
            if task.code in dag_by_code:
                raise _identity_error(
                    resource, "Task DAG repeated a code", applied=True
                )
            dag_by_code[task.code] = task
    for code, expected_name in changed_names_by_code.items():
        dag_task = dag_by_code.get(code)
        if dag_task is None or dag_task.name != expected_name:
            raise _identity_error(resource, "Task DAG readback differs", applied=True)
        main = _get_main_task(
            wire, project_code=project_code, code=code, resource=resource
        )
        if (
            main.projectCode != project_code
            or main.code != code
            or type(main.id) is not int
            or main.id <= 0
            or (code in original_ids and main.id != original_ids[code])
            or main.name != dag_task.name
            or main.version != dag_task.version
            or main.taskType != dag_task.taskType
            or _normalized_params(main) != _normalized_params(dag_task)
        ):
            raise _identity_error(
                resource, "Task main detail differs from DAG", applied=True
            )


def _normalized_params(task: TaskPayloadRecord) -> str:
    """Compare typed task parameters as canonical JSON without exposing JSON types."""
    value = task.taskParams
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _get_main_task(
    wire: TaskDefinitionWire,
    *,
    project_code: int,
    code: int,
    resource: str,
) -> TaskPayloadRecord:
    try:
        return wire.get(project_code=project_code, task_code=code).payload
    except ApiResultError as exc:
        details = {
            "resource": resource,
            "selected_version": "3.1.0",
            "project_code": project_code,
            "task_code": code,
            "upstream_result_code": exc.result_code,
        }
        if exc.result_code in _TASK_PERMISSION_RESULT_CODES:
            message = "Task detail requires project permission"
            raise PermissionDeniedError(
                message,
                details=details,
                suggestion=(
                    "Ask a DolphinScheduler project owner to grant task read "
                    "permission, then retry the edit."
                ),
            ) from exc
        if exc.result_code in {10190, 10018}:
            message = "Task project was not found"
            raise NotFoundError(
                message,
                details=details,
                suggestion=(
                    "Run `dsctl project get` for the selected project, then "
                    "retry the edit if it still exists."
                ),
            ) from exc
        if exc.result_code in _TASK_NOT_FOUND_RESULT_CODES:
            message = "Task detail was not found"
            raise NotFoundError(
                message,
                details=details,
                suggestion=(
                    "Inspect `dsctl task list` for the selected project and "
                    "workflow, then retry with its current task codes."
                ),
            ) from exc
        message = "Task detail read failed"
        raise ApiTransportError(
            message,
            details=details,
            suggestion="Inspect the task detail endpoint before retrying the edit.",
        ) from exc


def _identity_error(
    resource: str, message: str, *, applied: bool = False
) -> ApiTransportError:
    return ApiTransportError(
        message,
        details={
            "resource": resource,
            "selected_version": "3.1.0",
            "mutation_applied": applied,
        },
    )
