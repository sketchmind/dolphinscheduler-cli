from __future__ import annotations

import shlex
from collections.abc import Callable, Mapping, Sequence
from typing import TypedDict, TypeGuard

from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.support.json_types import JsonObject, JsonValue

MAX_NEXT_ACTIONS = 3
MAX_TASK_LOG_ACTIONS = 2
NEXT_ACTION_ITEM_FIELDS = ("action", "command", "mutates")

_FAILED_TASK_STATES = frozenset({"FAILURE", "KILL", "NEED_FAULT_TOLERANCE"})
_SUCCESS_TASK_STATES = frozenset({"FORCED_SUCCESS", "SUCCESS"})


class NextActionData(TypedDict):
    """One complete, bounded invocation derived from a successful result."""

    action: str
    command: str
    mutates: bool


CommandPrefix = tuple[str, ...]
NavigationRule = Callable[
    [JsonObject, JsonValue, CommandPrefix],
    list[NextActionData],
]


def next_actions_for(
    action: str,
    *,
    resolved: JsonObject,
    data: JsonValue,
    env_file: str | None = None,
) -> list[NextActionData]:
    """Derive complete lifecycle navigation without I/O or guessed facts."""
    rule = _NAVIGATION_RULES.get(action)
    if rule is None:
        return []
    command_prefix = _command_prefix(env_file)
    if command_prefix is None:
        return []
    try:
        candidates = rule(resolved, data, command_prefix)
    except CommandBindingError:
        return []
    actions: list[NextActionData] = []
    seen_commands: set[str] = set()
    for candidate in candidates:
        command = candidate["command"]
        if command in seen_commands:
            continue
        seen_commands.add(command)
        actions.append(candidate)
        if len(actions) == MAX_NEXT_ACTIONS:
            break
    return actions


def _workflow_release_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    data_object = _object(data)
    if data_object is None or data_object.get("dry_run") is True:
        return []
    project_code = _nested_positive_int(resolved, "project", "code")
    workflow_code = _nested_positive_int(resolved, "workflow", "code")
    release_state = _upper_string(data_object.get("releaseState"))
    if project_code is None or workflow_code is None or release_state is None:
        return []
    if release_state == "OFFLINE":
        return [
            _action(
                "workflow.online",
                [
                    *command_prefix,
                    "--compact",
                    "--columns",
                    "code,name,releaseState",
                    "workflow",
                    "online",
                    str(workflow_code),
                    "--project",
                    str(project_code),
                ],
                mutates=True,
            )
        ]
    if release_state == "ONLINE":
        return [
            _action(
                "workflow.run",
                [
                    *command_prefix,
                    "--compact",
                    "workflow",
                    "run",
                    str(workflow_code),
                    "--project",
                    str(project_code),
                ],
                mutates=True,
            )
        ]
    return []


def _workflow_create_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    data_object = _object(data)
    if data_object is None:
        return []
    if data_object.get("dry_run") is not True:
        return _workflow_release_navigation(resolved, data, command_prefix)

    project_code = _nested_positive_int(resolved, "project", "code")
    file = _non_empty_opaque_string(resolved.get("file"))
    if project_code is None or file is None:
        return []

    confirmation_args = _workflow_create_confirmation_args(data_object)
    if confirmation_args is None:
        return []
    global_values: dict[str, str | bool] = {
        "compact": True,
        "columns": "code,name,releaseState",
    }
    if len(command_prefix) == 3:
        global_values["env-file"] = command_prefix[2]
    values: dict[str, str | int] = {
        "file": file,
        "project": str(project_code),
    }
    if confirmation_args:
        values["confirm-risk"] = confirmation_args[1]
    return [
        {
            "action": "workflow.create",
            "command": COMMAND_CATALOG.render(
                "workflow.create",
                global_values=global_values,
                values=values,
            ),
            "mutates": True,
        }
    ]


def _workflow_create_confirmation_args(
    data: Mapping[str, JsonValue],
) -> list[str] | None:
    has_schedule_preview = "schedule_preview" in data
    has_schedule_confirmation = "schedule_confirmation" in data
    if has_schedule_preview or has_schedule_confirmation:
        if not has_schedule_preview or not has_schedule_confirmation:
            return None
        if _object(data.get("schedule_preview")) is None:
            return None
        schedule_confirmation = _object(data.get("schedule_confirmation"))
        if schedule_confirmation is None:
            return None
        confirmation_required = schedule_confirmation.get("required")
        if confirmation_required is True:
            confirmation_token = _non_blank_opaque_string(
                schedule_confirmation.get("token")
            )
            if confirmation_token is None:
                return None
            return ["--confirm-risk", confirmation_token]
        if confirmation_required is not False:
            return None
    return []


def _workflow_run_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    del resolved
    data_object = _object(data)
    if data_object is None or data_object.get("dry_run") is True:
        return []
    raw_ids = data_object.get("workflowInstanceIds")
    if not _is_sequence(raw_ids):
        return []
    instance_ids = [_positive_int(item) for item in raw_ids]
    if len(instance_ids) != 1 or instance_ids[0] is None:
        return []
    instance_id = instance_ids[0]
    return [
        _action(
            "workflow-instance.watch",
            [
                *command_prefix,
                "--compact",
                "--columns",
                "id,name,state,startTime,endTime,duration",
                "workflow-instance",
                "watch",
                str(instance_id),
            ],
            mutates=False,
        )
    ]


def _workflow_watch_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    del resolved
    data_object = _object(data)
    if data_object is None:
        return []
    instance_id = _positive_int(data_object.get("id"))
    state = _upper_string(data_object.get("state"))
    if instance_id is None or state is None:
        return []
    if state == "SUCCESS":
        return [_task_list_action(instance_id, command_prefix=command_prefix)]
    if state == "FAILURE":
        return [
            _action(
                "workflow-instance.digest",
                [
                    *command_prefix,
                    "--compact",
                    "--columns",
                    "taskCount,taskStateCounts,progress,failedTasks",
                    "workflow-instance",
                    "digest",
                    str(instance_id),
                ],
                mutates=False,
            ),
        ]
    return []


def _workflow_digest_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    del resolved
    data_object = _object(data)
    if data_object is None:
        return []
    rows = data_object.get("failedTasks")
    if not _is_sequence(rows):
        return []
    failed = [
        row
        for row in rows
        if _task_state(row) in _FAILED_TASK_STATES and _log_available(row)
    ]
    return [
        _task_log_action(
            task_id,
            command_prefix=command_prefix,
        )
        for task_id in _ranked_task_ids(failed)[:MAX_TASK_LOG_ACTIONS]
    ]


def _task_list_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    del resolved
    data_object = _object(data)
    if data_object is None:
        return []
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return []
    tasks = [row for row in rows if _object(row) is not None]
    failed = [
        row
        for row in tasks
        if _task_state(row) in _FAILED_TASK_STATES and _has_log(row)
    ]
    if failed:
        return [
            _task_log_action(task_id, command_prefix=command_prefix)
            for task_id in _ranked_task_ids(failed)[:MAX_TASK_LOG_ACTIONS]
        ]
    successful = [
        row
        for row in tasks
        if _task_state(row) in _SUCCESS_TASK_STATES and _has_log(row)
    ]
    ranked_successful = _ranked_task_ids(successful)
    if not ranked_successful:
        return []
    return [
        _task_log_action(
            ranked_successful[0],
            command_prefix=command_prefix,
            tail=30,
        )
    ]


def _task_list_action(
    instance_id: int,
    *,
    command_prefix: CommandPrefix,
) -> NextActionData:
    argv = [
        *command_prefix,
        "--compact",
        "--columns",
        "id,name,state,taskType,endTime,logPath",
        "task-instance",
        "list",
        "--workflow-instance",
        str(instance_id),
        "--page-size",
        "20",
    ]
    return _action("task-instance.list", argv, mutates=False)


def _task_log_action(
    task_id: int,
    *,
    command_prefix: CommandPrefix,
    tail: int = 80,
) -> NextActionData:
    return _action(
        "task-instance.log",
        [
            *command_prefix,
            "task-instance",
            "log",
            str(task_id),
            "--tail",
            str(tail),
            "--raw",
        ],
        mutates=False,
    )


def _action(action: str, argv: Sequence[str], *, mutates: bool) -> NextActionData:
    return {
        "action": action,
        "command": shlex.join(argv),
        "mutates": mutates,
    }


def _command_prefix(env_file: str | None) -> CommandPrefix | None:
    if env_file is None:
        return ("dsctl",)
    if not env_file:
        return None
    try:
        normalized = COMMAND_CATALOG.validate_global_values({"env-file": env_file})[
            "env-file"
        ]
    except CommandBindingError:
        return None
    if not isinstance(normalized, str):
        return None
    return ("dsctl", "--env-file", normalized)


def _object(value: JsonValue) -> Mapping[str, JsonValue] | None:
    if not isinstance(value, Mapping):
        return None
    if not all(isinstance(key, str) for key in value):
        return None
    return value


def _is_sequence(value: JsonValue) -> TypeGuard[Sequence[JsonValue]]:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _positive_int(value: JsonValue) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _nested_positive_int(root: JsonValue, *path: str) -> int | None:
    value: JsonValue = root
    for key in path:
        mapping = _object(value)
        if mapping is None:
            return None
        value = mapping.get(key)
    return _positive_int(value)


def _upper_string(value: JsonValue) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized or None


def _non_empty_opaque_string(value: JsonValue) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value


def _non_blank_opaque_string(value: JsonValue) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def _task_state(row: JsonValue) -> str | None:
    mapping = _object(row)
    return None if mapping is None else _upper_string(mapping.get("state"))


def _has_log(row: JsonValue) -> bool:
    mapping = _object(row)
    if mapping is None:
        return False
    log_path = mapping.get("logPath")
    return isinstance(log_path, str) and bool(log_path.strip())


def _log_available(row: JsonValue) -> bool:
    mapping = _object(row)
    return mapping is not None and mapping.get("logAvailable") is True


def _ranked_task_ids(rows: Sequence[JsonValue]) -> list[int]:
    ranked: list[tuple[str, int]] = []
    for row in rows:
        mapping = _object(row)
        if mapping is None:
            continue
        task_id = _positive_int(mapping.get("id"))
        if task_id is None:
            continue
        raw_end_time = mapping.get("endTime")
        end_time = raw_end_time if isinstance(raw_end_time, str) else ""
        ranked.append((end_time, task_id))
    ranked.sort(reverse=True)
    return [task_id for _, task_id in ranked]


_NAVIGATION_RULES: dict[str, NavigationRule] = {
    "workflow.create": _workflow_create_navigation,
    "workflow.online": _workflow_release_navigation,
    "workflow.run": _workflow_run_navigation,
    "workflow.run-task": _workflow_run_navigation,
    "workflow-instance.watch": _workflow_watch_navigation,
    "workflow-instance.digest": _workflow_digest_navigation,
    "task-instance.list": _task_list_navigation,
}
