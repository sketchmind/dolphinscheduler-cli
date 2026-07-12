from __future__ import annotations

import shlex
from collections.abc import Callable, Mapping, Sequence
from typing import Literal, TypedDict, TypeGuard

from dsctl.cli_surface import stable_leaf_actions
from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.support.json_types import JsonObject, JsonValue

MAX_NEXT_ACTIONS = 3
MAX_TASK_LOG_ACTIONS = 2
MAX_ACTION_INDEX_TARGETS = 100
NEXT_ACTION_ITEM_FIELDS = ("action", "command", "mutates")
ACTION_INDEX_FIELDS = (
    "scope",
    "target",
    "authorization",
    "eligibility",
    "groups",
    "schema_command",
    "group_command",
    "target_count",
    "indexed_target_count",
    "truncated",
)
ACTION_INDEX_TARGET_FIELDS = ("resource", "field")
ACTION_INDEX_GROUP_FIELDS = (
    "targets",
    "read",
    "read_needs_input",
    "mutate",
    "mutate_needs_input",
)

_FAILED_TASK_STATES = frozenset({"FAILURE", "KILL", "NEED_FAULT_TOLERANCE"})
_SUCCESS_TASK_STATES = frozenset({"FORCED_SUCCESS", "SUCCESS"})
_FINAL_WORKFLOW_STATES = frozenset({"FAILURE", "PAUSE", "STOP", "SUCCESS"})
_STOPPABLE_WORKFLOW_STATES = frozenset(
    {"READY_PAUSE", "READY_STOP", "RUNNING_EXECUTION", "SERIAL_WAIT"}
)
_ACTIVE_TASK_STATES = frozenset(
    {"DELAY_EXECUTION", "DISPATCH", "RUNNING_EXECUTION", "SUBMITTED_SUCCESS"}
)
_STABLE_LEAF_ACTIONS = stable_leaf_actions()


class NextActionData(TypedDict):
    """One complete, bounded invocation derived from a successful result."""

    action: str
    command: str
    mutates: bool


class ActionIndexTargetData(TypedDict):
    """Stable selector metadata for one list action index."""

    resource: str
    field: str


class _ActionCandidateData(TypedDict):
    """Internal action facts before candidates with equal targets are grouped."""

    action: str
    mutates: bool
    needs_input: bool
    targets: Literal["all"] | list[int | str]


class _ActionIndexGroupRequiredData(TypedDict):
    """Required selector facts for one public action group."""

    targets: Literal["all"] | list[int | str]


class ActionIndexGroupData(_ActionIndexGroupRequiredData, total=False):
    """Actions sharing one exact set of locally eligible selectors."""

    read: list[str]
    read_needs_input: list[str]
    mutate: list[str]
    mutate_needs_input: list[str]


class ActionIndexData(TypedDict):
    """Compact positive action discovery for one row-oriented result."""

    scope: str
    target: ActionIndexTargetData
    authorization: Literal["not_evaluated"]
    eligibility: Literal["row_facts_only"]
    groups: list[ActionIndexGroupData]
    schema_command: str
    group_command: str
    target_count: int
    indexed_target_count: int
    truncated: bool


class ResultNavigationData(TypedDict, total=False):
    """All optional navigation derived from one successful result."""

    next_actions: list[NextActionData]
    action_index: ActionIndexData


CommandPrefix = tuple[str, ...]
NavigationRule = Callable[
    [JsonObject, JsonValue, CommandPrefix],
    list[NextActionData],
]


def navigation_for(
    action: str,
    *,
    resolved: JsonObject,
    data: JsonValue,
    env_file: str | None = None,
) -> ResultNavigationData:
    """Derive bounded result navigation without I/O or guessed facts."""
    navigation = ResultNavigationData()
    next_actions = next_actions_for(
        action,
        resolved=resolved,
        data=data,
        env_file=env_file,
    )
    if next_actions:
        navigation["next_actions"] = next_actions
    action_index = _action_index_for(action, resolved=resolved, data=data)
    if action_index is not None:
        navigation["action_index"] = action_index
    return navigation


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


def _action_index_for(
    action: str,
    *,
    resolved: JsonObject,
    data: JsonValue,
) -> ActionIndexData | None:
    if action == "workflow.list":
        return _workflow_action_index(resolved, data)
    if action == "schedule.list":
        return _schedule_action_index(data)
    if action == "task-instance.list":
        return _task_instance_action_index(data)
    if action == "workflow-instance.list":
        return _workflow_instance_action_index(data)
    return None


def _workflow_instance_action_index(data: JsonValue) -> ActionIndexData | None:
    """Index runtime-instance actions from returned execution states."""
    data_object = _object(data)
    if data_object is None:
        return None
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return None

    target_count, indexed, truncated = _bounded_indexed_rows(rows, target_field="id")
    all_targets = [target for target, _ in indexed]
    indexed_rows = [
        (target, _upper_string(row.get("state"))) for target, row in indexed
    ]
    if not indexed_rows:
        return None

    final_targets = [
        target for target, state in indexed_rows if state in _FINAL_WORKFLOW_STATES
    ]
    failure_targets = [target for target, state in indexed_rows if state == "FAILURE"]
    stoppable_targets = [
        target for target, state in indexed_rows if state in _STOPPABLE_WORKFLOW_STATES
    ]
    items = [
        _action_index_item("workflow-instance.get", mutates=False, targets="all"),
        _action_index_item("workflow-instance.digest", mutates=False, targets="all"),
        _action_index_item("workflow-instance.watch", mutates=False, targets="all"),
        _action_index_item("workflow-instance.export", mutates=False, targets="all"),
        _action_index_item(
            "workflow-instance.stop",
            mutates=True,
            targets=_all_or_targets(stoppable_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.rerun",
            mutates=True,
            targets=_all_or_targets(final_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.recover-failed",
            mutates=True,
            targets=_all_or_targets(failure_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.edit",
            mutates=True,
            needs_input=True,
            targets=_all_or_targets(final_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.execute-task",
            mutates=True,
            needs_input=True,
            targets=_all_or_targets(final_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="workflow-instance",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_target_count=len(indexed_rows),
        truncated=truncated,
    )


def _workflow_action_index(
    resolved: JsonObject,
    data: JsonValue,
) -> ActionIndexData | None:
    if _nested_positive_int(resolved, "project", "code") is None:
        return None
    data_object = _object(data)
    if data_object is None:
        return None
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return None
    target_count, indexed, truncated = _bounded_indexed_rows(
        rows,
        target_field="code",
    )
    if not indexed:
        return None

    indexed_targets = [target for target, _ in indexed]
    online_targets = [
        target
        for target, row in indexed
        if _upper_string(row.get("releaseState")) == "ONLINE"
    ]
    offline_targets = [
        target
        for target, row in indexed
        if _upper_string(row.get("releaseState")) == "OFFLINE"
    ]
    deletable_targets = [
        target for target, row in indexed if _workflow_row_is_deletable(row)
    ]
    read_actions = (
        "workflow.get",
        "workflow.digest",
        "workflow.describe",
        "workflow.export",
        "task.list",
        "schedule.list",
        "workflow-instance.list",
        "workflow.lineage.get",
        "workflow.lineage.dependent-tasks",
    )
    items = [
        _action_index_item(action, mutates=False, targets="all")
        for action in read_actions
    ]
    items.extend(
        [
            _action_index_item(
                "workflow.run",
                mutates=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.run-task",
                mutates=True,
                needs_input=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.backfill",
                mutates=True,
                needs_input=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.online",
                mutates=True,
                targets=_all_or_targets(offline_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.offline",
                mutates=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.edit",
                mutates=True,
                needs_input=True,
                targets=_all_or_targets(offline_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.delete",
                mutates=True,
                needs_input=True,
                targets=_all_or_targets(deletable_targets, indexed_targets),
            ),
        ]
    )
    return _build_action_index(
        resource="workflow",
        target_field="code",
        candidates=items,
        target_count=target_count,
        indexed_target_count=len(indexed_targets),
        truncated=truncated,
    )


def _workflow_row_is_deletable(row: Mapping[str, JsonValue]) -> bool:
    if _upper_string(row.get("releaseState")) != "OFFLINE":
        return False
    schedule_state = _upper_string(row.get("scheduleReleaseState"))
    if schedule_state == "OFFLINE":
        return True
    return schedule_state is None and _positive_int(row.get("scheduleId")) is None


def _schedule_action_index(data: JsonValue) -> ActionIndexData | None:
    data_object = _object(data)
    if data_object is None:
        return None
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return None

    target_count, indexed, truncated = _bounded_indexed_rows(rows, target_field="id")
    all_targets = [target for target, _ in indexed]
    indexed_rows = [
        (target, _upper_string(row.get("releaseState"))) for target, row in indexed
    ]
    if not indexed_rows:
        return None

    offline_targets = [target for target, state in indexed_rows if state == "OFFLINE"]
    online_targets = [target for target, state in indexed_rows if state == "ONLINE"]
    items = [
        _action_index_item("schedule.get", mutates=False, targets="all"),
        _action_index_item("schedule.preview", mutates=False, targets="all"),
        _action_index_item(
            "schedule.explain",
            mutates=False,
            needs_input=True,
            targets="all",
        ),
        _action_index_item(
            "schedule.online",
            mutates=True,
            targets=_all_or_targets(offline_targets, all_targets),
        ),
        _action_index_item(
            "schedule.offline",
            mutates=True,
            targets=_all_or_targets(online_targets, all_targets),
        ),
        _action_index_item(
            "schedule.update",
            mutates=True,
            needs_input=True,
            targets="all",
        ),
        _action_index_item(
            "schedule.delete",
            mutates=True,
            needs_input=True,
            targets=_all_or_targets(offline_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="schedule",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_target_count=len(indexed_rows),
        truncated=truncated,
    )


def _task_instance_action_index(data: JsonValue) -> ActionIndexData | None:
    data_object = _object(data)
    if data_object is None:
        return None
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return None

    target_count, indexed_rows, truncated = _bounded_indexed_rows(
        rows,
        target_field="id",
    )
    if not indexed_rows:
        return None

    all_targets = [target for target, _ in indexed_rows]
    scoped_targets = [
        target
        for target, row in indexed_rows
        if _positive_int(row.get("workflowInstanceId")) is not None
    ]
    log_targets = [target for target, row in indexed_rows if _has_log(row)]
    sub_workflow_targets = [
        target
        for target, row in indexed_rows
        if _positive_int(row.get("workflowInstanceId")) is not None
        and _upper_string(row.get("taskType")) == "SUB_WORKFLOW"
    ]
    active_targets = [
        target
        for target, row in indexed_rows
        if _positive_int(row.get("workflowInstanceId")) is not None
        and _upper_string(row.get("state")) in _ACTIVE_TASK_STATES
        and _upper_string(row.get("taskExecuteType")) == "STREAM"
        and _non_blank_opaque_string(row.get("host")) is not None
    ]
    force_success_targets = [
        target
        for target, row in indexed_rows
        if _positive_int(row.get("workflowInstanceId")) is not None
        and _upper_string(row.get("state")) in _FAILED_TASK_STATES
    ]
    items = [
        _action_index_item(
            "task-instance.get",
            mutates=False,
            targets=_all_or_targets(scoped_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.watch",
            mutates=False,
            targets=_all_or_targets(scoped_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.log",
            mutates=False,
            targets=_all_or_targets(log_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.sub-workflow",
            mutates=False,
            targets=_all_or_targets(sub_workflow_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.force-success",
            mutates=True,
            targets=_all_or_targets(force_success_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.savepoint",
            mutates=True,
            targets=_all_or_targets(active_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.stop",
            mutates=True,
            targets=_all_or_targets(active_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="task-instance",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_target_count=len(indexed_rows),
        truncated=truncated,
    )


def _bounded_indexed_rows(
    rows: Sequence[JsonValue],
    *,
    target_field: str,
) -> tuple[int, list[tuple[int, Mapping[str, JsonValue]]], bool]:
    rows_by_target: dict[int, Mapping[str, JsonValue]] = {}
    target_order: list[int] = []
    ambiguous_targets: set[int] = set()
    for row in rows:
        row_object = _object(row)
        if row_object is None:
            continue
        target = _positive_int(row_object.get(target_field))
        if target is None:
            continue
        if target in rows_by_target:
            ambiguous_targets.add(target)
            continue
        rows_by_target[target] = row_object
        target_order.append(target)

    eligible_targets = [
        target for target in target_order if target not in ambiguous_targets
    ]
    indexed_targets = eligible_targets[:MAX_ACTION_INDEX_TARGETS]
    return (
        len(rows),
        [(target, rows_by_target[target]) for target in indexed_targets],
        len(eligible_targets) > MAX_ACTION_INDEX_TARGETS,
    )


def _all_or_targets(
    eligible_targets: Sequence[int | str],
    all_targets: Sequence[int | str],
) -> Literal["all"] | list[int | str]:
    if eligible_targets and eligible_targets == all_targets:
        return "all"
    return list(eligible_targets)


def _action_index_item(
    action: str,
    *,
    mutates: bool,
    targets: Literal["all"] | Sequence[int | str],
    needs_input: bool = False,
) -> _ActionCandidateData | None:
    if action not in _STABLE_LEAF_ACTIONS or not targets:
        return None
    normalized_targets: Literal["all"] | list[int | str] = (
        "all" if targets == "all" else list(targets)
    )
    return _ActionCandidateData(
        action=action,
        mutates=mutates,
        needs_input=needs_input,
        targets=normalized_targets,
    )


def _build_action_index(
    *,
    resource: str,
    target_field: str,
    candidates: Sequence[_ActionCandidateData | None],
    target_count: int,
    indexed_target_count: int,
    truncated: bool,
) -> ActionIndexData:
    """Build the shared bounded index envelope around resource-specific rules."""
    groups = _group_action_candidates(candidates)
    return {
        "scope": "data.totalList",
        "target": {"resource": resource, "field": target_field},
        "authorization": "not_evaluated",
        "eligibility": "row_facts_only",
        "groups": groups,
        "schema_command": "dsctl schema --command ACTION",
        "group_command": f"dsctl schema --group {resource}",
        "target_count": target_count,
        "indexed_target_count": indexed_target_count,
        "truncated": truncated,
    }


def _group_action_candidates(
    candidates: Sequence[_ActionCandidateData | None],
) -> list[ActionIndexGroupData]:
    """Intern identical target lists so selectors are emitted only once."""
    groups: list[ActionIndexGroupData] = []
    group_indexes: dict[tuple[int | str, ...], int] = {}
    for candidate in candidates:
        if candidate is None:
            continue
        targets = candidate["targets"]
        key = ("all",) if targets == "all" else ("targets", *targets)
        group_index = group_indexes.get(key)
        if group_index is None:
            group_index = len(groups)
            group_indexes[key] = group_index
            groups.append(
                ActionIndexGroupData(
                    targets="all" if targets == "all" else list(targets)
                )
            )
        _append_group_action(groups[group_index], candidate)
    return groups


def _append_group_action(
    group: ActionIndexGroupData,
    candidate: _ActionCandidateData,
) -> None:
    action = candidate["action"]
    if candidate["mutates"]:
        if candidate["needs_input"]:
            group.setdefault("mutate_needs_input", []).append(action)
        else:
            group.setdefault("mutate", []).append(action)
    elif candidate["needs_input"]:
        group.setdefault("read_needs_input", []).append(action)
    else:
        group.setdefault("read", []).append(action)


_NAVIGATION_RULES: dict[str, NavigationRule] = {
    "workflow.create": _workflow_create_navigation,
    "workflow.online": _workflow_release_navigation,
    "workflow.run": _workflow_run_navigation,
    "workflow.run-task": _workflow_run_navigation,
    "workflow-instance.watch": _workflow_watch_navigation,
    "workflow-instance.digest": _workflow_digest_navigation,
    "task-instance.list": _task_list_navigation,
}
