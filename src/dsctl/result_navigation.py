from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import Literal, TypedDict, TypeGuard

from dsctl.cli_surface import stable_leaf_actions
from dsctl.command_contract import COMMAND_CATALOG, CommandBindingError
from dsctl.execution_states import (
    TASK_EXECUTION_FAILED_STATES,
    TASK_EXECUTION_QUEUED_STATES,
    TASK_EXECUTION_RUNNING_STATES,
    TASK_EXECUTION_SUCCESS_STATES,
    WORKFLOW_EXECUTION_FINISHED_STATES,
    WORKFLOW_EXECUTION_STOPPABLE_STATES,
)
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
    "schema_command_pattern",
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
    schema_command_pattern: str
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
    available_actions: Collection[str] | None = None,
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
    command_prefix = _command_prefix(env_file, resolved=resolved)
    if action_index is not None and command_prefix is not None:
        globals_: dict[str, str] = {}
        if len(command_prefix) == 3:
            globals_[command_prefix[1].removeprefix("--")] = command_prefix[2]
        schema_command = COMMAND_CATALOG.render(
            "schema", global_values=globals_, values={"command": "ACTION"}
        )
        action_index["schema_command_pattern"] = schema_command
        action_index["group_command"] = COMMAND_CATALOG.render(
            "schema",
            global_values=globals_,
            values={"group": action_index["target"]["resource"]},
        )
        navigation["action_index"] = action_index
    return _filter_available_actions(navigation, available_actions)


def error_navigation_for(
    action: str,
    *,
    error_type: str,
    resolved: JsonObject,
    env_file: str | None = None,
) -> ResultNavigationData:
    """Derive bounded reconciliation reads for one stable error outcome."""
    if action != "workflow-instance.stop" or error_type != "mutation_outcome_unknown":
        return ResultNavigationData()
    command_prefix = _command_prefix(env_file, resolved=resolved)
    instance_id = _nested_positive_int(resolved, "workflowInstance", "id")
    project_selector = _definition_selector(resolved, "project")
    if command_prefix is None or instance_id is None or project_selector is None:
        return ResultNavigationData()
    values: dict[str, str | int | bool] = {
        "workflow_instance": instance_id,
        "project": str(project_selector),
    }
    return ResultNavigationData(
        next_actions=[
            _action(
                "workflow-instance.get",
                values,
                command_prefix=command_prefix,
            ),
            _action(
                "workflow-instance.watch",
                values,
                command_prefix=command_prefix,
                columns="id,name,state,startTime,endTime,duration",
            ),
        ]
    )


def _filter_available_actions(
    navigation: ResultNavigationData,
    available_actions: Collection[str] | None,
) -> ResultNavigationData:
    """Remove suggestions the selected exact-version profile cannot execute."""
    if available_actions is None:
        return navigation
    allowed = frozenset(available_actions)
    _filter_next_actions(navigation, allowed)
    _filter_action_index(navigation, allowed)
    return navigation


def _filter_next_actions(
    navigation: ResultNavigationData,
    allowed: frozenset[str],
) -> None:
    """Keep only executable lifecycle suggestions."""
    next_actions = navigation.get("next_actions")
    if next_actions is None:
        return
    filtered = [item for item in next_actions if item["action"] in allowed]
    if filtered:
        navigation["next_actions"] = filtered
    else:
        navigation.pop("next_actions", None)


def _filter_action_index(
    navigation: ResultNavigationData,
    allowed: frozenset[str],
) -> None:
    """Keep only executable actions in a list discovery index."""
    action_index = navigation.get("action_index")
    if action_index is None:
        return
    filtered_groups = [
        filtered
        for group in action_index["groups"]
        if (filtered := _filter_action_index_group(group, allowed)) is not None
    ]
    if filtered_groups:
        action_index["groups"] = filtered_groups
    else:
        navigation.pop("action_index", None)


def _filter_action_index_group(
    group: ActionIndexGroupData,
    allowed: frozenset[str],
) -> ActionIndexGroupData | None:
    """Filter one group while preserving its target set."""
    filtered = ActionIndexGroupData(targets=group["targets"])
    read = _allowed_group_actions(group.get("read"), allowed)
    read_needs_input = _allowed_group_actions(
        group.get("read_needs_input"),
        allowed,
    )
    mutate = _allowed_group_actions(group.get("mutate"), allowed)
    mutate_needs_input = _allowed_group_actions(
        group.get("mutate_needs_input"),
        allowed,
    )
    if read:
        filtered["read"] = read
    if read_needs_input:
        filtered["read_needs_input"] = read_needs_input
    if mutate:
        filtered["mutate"] = mutate
    if mutate_needs_input:
        filtered["mutate_needs_input"] = mutate_needs_input
    return filtered if len(filtered) > 1 else None


def _allowed_group_actions(
    actions: list[str] | None,
    allowed: frozenset[str],
) -> list[str]:
    if actions is None:
        return []
    return [action for action in actions if action in allowed]


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
    command_prefix = _command_prefix(env_file, resolved=resolved)
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
    project_selector = _definition_selector(resolved, "project")
    workflow_selector = _definition_selector(resolved, "workflow")
    if not _workflow_identity_matches(resolved, data_object):
        return []
    release_state = _upper_string(data_object.get("releaseState"))
    if project_selector is None or workflow_selector is None or release_state is None:
        return []
    if release_state == "OFFLINE":
        return [
            _action(
                "workflow.online",
                {"workflow": str(workflow_selector), "project": str(project_selector)},
                command_prefix=command_prefix,
                columns="code,name,releaseState",
            )
        ]
    if release_state == "ONLINE":
        return [
            _action(
                "workflow.run",
                {"workflow": str(workflow_selector), "project": str(project_selector)},
                command_prefix=command_prefix,
            ),
            _workflow_schedule_action(
                data_object,
                project_selector=project_selector,
                workflow_selector=workflow_selector,
                command_prefix=command_prefix,
            ),
        ]
    return []


def _workflow_schedule_action(
    data: Mapping[str, JsonValue],
    *,
    project_selector: str,
    workflow_selector: str,
    command_prefix: CommandPrefix,
) -> NextActionData:
    schedule_id = _nested_positive_int(data.get("schedule"), "id")
    if schedule_id is not None:
        return _action(
            "schedule.get",
            {"schedule_id": schedule_id, "project": project_selector},
            command_prefix=command_prefix,
        )
    return _action(
        "schedule.list",
        {"workflow": workflow_selector, "project": project_selector},
        command_prefix=command_prefix,
    )


def _workflow_change_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    """Inspect the edited definition and any known independent schedule."""
    row = _object(data)
    project = _definition_selector(resolved, "project")
    workflow = _definition_selector(resolved, "workflow")
    if row is None or row.get("dry_run") is True or project is None or workflow is None:
        return []
    if not _workflow_identity_matches(resolved, row):
        return []
    if _nested_positive_int(resolved, "workflow", "code") is None:
        workflow = _non_blank_opaque_string(row.get("name")) or workflow
    actions = [
        _action(
            "workflow.describe",
            {"workflow": workflow, "project": project},
            command_prefix=command_prefix,
        )
    ]
    if _nested_positive_int(row.get("schedule"), "id") is not None:
        actions.append(
            _workflow_schedule_action(
                row,
                project_selector=project,
                workflow_selector=workflow,
                command_prefix=command_prefix,
            )
        )
    return actions


def _workflow_identity_matches(
    resolved: JsonObject, row: Mapping[str, JsonValue]
) -> bool:
    selected_code = _nested_positive_int(resolved, "workflow", "code")
    if selected_code is None or "code" not in row:
        return True
    return _positive_int(row.get("code")) == selected_code


def _schedule_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    """Preview a persisted schedule; an online schedule can lead to its runs."""
    row = _object(data)
    if row is None or row.get("dry_run") is True:
        return []
    schedule_id = _positive_int(row.get("id"))
    selected = _object(resolved.get("schedule"))
    if schedule_id is None or (
        selected is not None and _positive_int(selected.get("id")) != schedule_id
    ):
        return []
    # ID-scoped schedule services may return only the selector's source/value.
    # The snapshot's real project name also works for legacy project identities.
    project = _definition_selector(resolved, "project") or _non_blank_opaque_string(
        row.get("projectName")
    )
    if project is None:
        return []
    if _upper_string(row.get("releaseState")) == "ONLINE":
        workflow = _definition_selector(
            resolved, "workflow"
        ) or _non_blank_opaque_string(row.get("workflowDefinitionName"))
        if workflow is not None:
            return [
                _action(
                    "workflow-instance.list",
                    {"workflow": workflow, "project": project},
                    command_prefix=command_prefix,
                )
            ]
    return [
        _action(
            "schedule.preview",
            {"schedule_id": schedule_id, "project": project},
            command_prefix=command_prefix,
        )
    ]


def _task_group_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    group_id = _nested_positive_int(data, "id")
    selected = _object(resolved.get("taskGroup"))
    if group_id is None or (
        selected is not None and _positive_int(selected.get("id")) != group_id
    ):
        return []
    return [
        _action(
            "task-group.queue.list",
            {"task_group": str(group_id)},
            command_prefix=command_prefix,
        )
    ]


def _task_group_queue_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    """Follow one returned queue row; do not select a task from a mixed page."""
    row = _object(data)
    rows = None if row is None else row.get("totalList")
    if not _is_sequence(rows) or len(rows) != 1:
        return []
    queue = _object(rows[0])
    group_id = _nested_positive_int(resolved, "taskGroup", "id")
    if queue is None or group_id is None or _positive_int(queue.get("id")) is None:
        return []
    if _positive_int(queue.get("groupId")) != group_id:
        return []
    task_id = _positive_int(queue.get("taskId"))
    instance_id = _positive_int(queue.get("workflowInstanceId"))
    project = _non_blank_opaque_string(queue.get("projectName"))
    if instance_id is None or project is None:
        return []
    if task_id is None:
        return [
            _action(
                "task-instance.list",
                {"workflow-instance": instance_id, "project": project},
                command_prefix=command_prefix,
            )
        ]
    return [
        _action(
            "task-instance.get",
            {
                "task_instance": task_id,
                "workflow-instance": instance_id,
                "project": project,
            },
            command_prefix=command_prefix,
        )
    ]


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

    project_selector = _definition_selector(resolved, "project")
    file = _non_empty_opaque_string(resolved.get("file"))
    if project_selector is None or file is None:
        return []

    confirmation_args = _workflow_create_confirmation_args(data_object)
    if confirmation_args is None:
        return []
    global_values: dict[str, str | bool] = {
        "format": "json-compact",
        "columns": "code,name,releaseState",
    }
    if len(command_prefix) == 3:
        global_values[command_prefix[1].removeprefix("--")] = command_prefix[2]
    values: dict[str, str | int] = {
        "file": file,
        "project": str(project_selector),
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
            "mutates": _command_mutates("workflow.create", values),
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
    data_object = _object(data)
    if data_object is None or data_object.get("dry_run") is True:
        return []
    project_selector = _definition_selector(resolved, "project")
    if project_selector is None:
        return []
    raw_ids = data_object.get("workflowInstanceIds")
    if not _is_sequence(raw_ids):
        return []
    instance_ids = [_positive_int(item) for item in raw_ids]
    if len(instance_ids) != 1 or instance_ids[0] is None:
        trigger_code = _positive_int(data_object.get("triggerCode"))
        if trigger_code is not None:
            return [
                _action(
                    "workflow-instance.list",
                    {"trigger-code": trigger_code, "project": str(project_selector)},
                    command_prefix=command_prefix,
                )
            ]
        return []
    instance_id = instance_ids[0]
    return [
        _action(
            "workflow-instance.watch",
            {"workflow_instance": instance_id, "project": str(project_selector)},
            command_prefix=command_prefix,
            columns="id,name,state,startTime,endTime,duration",
        )
    ]


def _workflow_replay_navigation(
    resolved: JsonObject, _data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    baseline = _nested_positive_int(resolved, "execution_baseline", "run_times")
    instance_id = _nested_positive_int(resolved, "workflowInstance", "id")
    project_selector = _definition_selector(resolved, "project")
    if baseline is None or instance_id is None or project_selector is None:
        return []
    return [
        _action(
            "workflow-instance.watch",
            {
                "workflow_instance": instance_id,
                "project": str(project_selector),
                "after-run-times": baseline,
            },
            command_prefix=command_prefix,
        )
    ]


def _trigger_instances_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    data_object = _object(data)
    if data_object is None or data_object.get("instanceResolution") != "resolved":
        return []
    rows = data_object.get("totalList")
    if not _is_sequence(rows):
        return []
    return _workflow_run_navigation(
        resolved,
        {"workflowInstanceIds": [_nested_positive_int(row, "id") for row in rows]},
        command_prefix,
    )


def _task_log_navigation(
    resolved: JsonObject, data: JsonValue, command_prefix: CommandPrefix
) -> list[NextActionData]:
    task_id = _nested_positive_int(resolved, "taskInstance", "id")
    start = _nested_positive_int(data, "window", "next_start_line")
    limit = _nested_positive_int(data, "window", "requested_limit")
    if task_id is None or start is None or limit is None:
        return []
    return [
        _action(
            "task-instance.log",
            {"task_instance": task_id, "start-line": start, "limit": limit},
            command_prefix=command_prefix,
        )
    ]


def _workflow_watch_navigation(
    resolved: JsonObject,
    data: JsonValue,
    command_prefix: CommandPrefix,
) -> list[NextActionData]:
    data_object = _object(data)
    if data_object is None:
        return []
    project_selector = _definition_selector(resolved, "project")
    instance_id = _positive_int(data_object.get("id"))
    state = _upper_string(data_object.get("state"))
    if project_selector is None or instance_id is None or state is None:
        return []
    if state == "SUCCESS":
        return [
            _task_list_action(
                instance_id,
                project_selector=project_selector,
                command_prefix=command_prefix,
            )
        ]
    if state == "FAILURE":
        return [
            _action(
                "workflow-instance.digest",
                {"workflow_instance": instance_id, "project": str(project_selector)},
                command_prefix=command_prefix,
                columns="taskCount,taskStateCounts,progress,failedTasks",
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
        if _task_state(row) in TASK_EXECUTION_FAILED_STATES and _log_available(row)
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
        if _task_state(row) in TASK_EXECUTION_FAILED_STATES and _has_log(row)
    ]
    if failed:
        return [
            _task_log_action(task_id, command_prefix=command_prefix)
            for task_id in _ranked_task_ids(failed)[:MAX_TASK_LOG_ACTIONS]
        ]
    successful = [
        row
        for row in tasks
        if _task_state(row) in TASK_EXECUTION_SUCCESS_STATES and _has_log(row)
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
    project_selector: str,
    command_prefix: CommandPrefix,
) -> NextActionData:
    return _action(
        "task-instance.list",
        {
            "project": str(project_selector),
            "workflow-instance": instance_id,
            "page-size": 20,
        },
        command_prefix=command_prefix,
        columns="id,name,state,taskType,endTime,logPath",
    )


def _task_log_action(
    task_id: int,
    *,
    command_prefix: CommandPrefix,
    tail: int = 80,
) -> NextActionData:
    return _action(
        "task-instance.log",
        {"task_instance": task_id, "tail": tail, "raw": True},
        command_prefix=command_prefix,
        compact=False,
    )


def _action(
    action: str,
    values: Mapping[str, str | int | bool],
    *,
    command_prefix: CommandPrefix,
    columns: str | None = None,
    compact: bool = True,
) -> NextActionData:
    globals_: dict[str, str | bool] = {}
    if len(command_prefix) == 3:
        globals_[command_prefix[1].removeprefix("--")] = command_prefix[2]
    if compact:
        globals_["format"] = "json-compact"
    if columns is not None:
        globals_["columns"] = columns
    return {
        "action": action,
        "command": COMMAND_CATALOG.render(
            action, global_values=globals_, values=values
        ),
        "mutates": _command_mutates(action, values),
    }


def _command_mutates(
    action: str, values: Mapping[str, str | int | bool] | None = None
) -> bool:
    effects = COMMAND_CATALOG.command(action).effects
    if (
        values is not None
        and values.get("dry-run") is True
        and effects.dry_run is not None
    ):
        return effects.dry_run.remote == "write" or effects.dry_run.local != "none"
    return effects.remote == "write" or effects.local != "none"


def _command_prefix(
    env_file: str | None, *, resolved: JsonObject
) -> CommandPrefix | None:
    selection = _object(resolved.get("selection"))
    context_name = None if selection is None else selection.get("context")
    value: JsonValue
    if isinstance(context_name, str):
        name, value = "context", context_name
    else:
        selected_file = None if selection is None else selection.get("env_file")
        value = env_file if env_file is not None else selected_file
        name = "env-file"
    if value is None:
        return ("dsctl",)
    if not isinstance(value, str) or not value:
        return None
    try:
        normalized = COMMAND_CATALOG.validate_global_values({name: value})[name]
    except CommandBindingError:
        return None
    if not isinstance(normalized, str):
        return None
    return ("dsctl", f"--{name}", normalized)


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


def _definition_selector(root: JsonObject, resource: str) -> str | None:
    """Use an actual code or name; a legacy native ID is never relabeled a code."""
    selected = _object(root.get(resource))
    if selected is None:
        return None
    code = _positive_int(selected.get("code"))
    if code is not None:
        return str(code)
    return _non_blank_opaque_string(selected.get("name"))


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
        target
        for target, state in indexed_rows
        if state in WORKFLOW_EXECUTION_FINISHED_STATES
    ]
    failure_targets = [target for target, state in indexed_rows if state == "FAILURE"]
    stoppable_targets = [
        target
        for target, state in indexed_rows
        if state in WORKFLOW_EXECUTION_STOPPABLE_STATES
    ]
    items = [
        _action_index_item("workflow-instance.get", targets="all"),
        _action_index_item("workflow-instance.digest", targets="all"),
        _action_index_item("workflow-instance.watch", targets="all"),
        _action_index_item("workflow-instance.export", targets="all"),
        _action_index_item(
            "workflow-instance.stop",
            targets=_all_or_targets(stoppable_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.rerun",
            targets=_all_or_targets(final_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.recover-failed",
            targets=_all_or_targets(failure_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.edit",
            needs_input=True,
            targets=_all_or_targets(final_targets, all_targets),
        ),
        _action_index_item(
            "workflow-instance.execute-task",
            needs_input=True,
            targets=_all_or_targets(final_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="workflow-instance",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_targets=all_targets,
        truncated=truncated,
    )


def _workflow_action_index(
    resolved: JsonObject,
    data: JsonValue,
) -> ActionIndexData | None:
    if _definition_selector(resolved, "project") is None:
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
    items = [_action_index_item(action, targets="all") for action in read_actions]
    items.extend(
        [
            _action_index_item(
                "workflow.run",
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.run-task",
                needs_input=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.backfill",
                needs_input=True,
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.online",
                targets=_all_or_targets(offline_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.offline",
                targets=_all_or_targets(online_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.edit",
                needs_input=True,
                targets=_all_or_targets(offline_targets, indexed_targets),
            ),
            _action_index_item(
                "workflow.delete",
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
        indexed_targets=indexed_targets,
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
        _action_index_item("schedule.get", targets="all"),
        _action_index_item("schedule.preview", targets="all"),
        _action_index_item(
            "schedule.explain",
            needs_input=True,
            targets="all",
        ),
        _action_index_item(
            "schedule.online",
            targets=_all_or_targets(offline_targets, all_targets),
        ),
        _action_index_item(
            "schedule.offline",
            targets=_all_or_targets(online_targets, all_targets),
        ),
        _action_index_item(
            "schedule.update",
            needs_input=True,
            targets=_all_or_targets(offline_targets, all_targets),
        ),
        _action_index_item(
            "schedule.delete",
            needs_input=True,
            targets=_all_or_targets(offline_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="schedule",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_targets=all_targets,
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
        if _upper_string(row.get("state"))
        in (TASK_EXECUTION_RUNNING_STATES | TASK_EXECUTION_QUEUED_STATES)
        and _upper_string(row.get("taskExecuteType")) == "STREAM"
        and _non_blank_opaque_string(row.get("host")) is not None
    ]
    force_success_targets = [
        target
        for target, row in indexed_rows
        if _positive_int(row.get("workflowInstanceId")) is not None
        and _upper_string(row.get("state")) in TASK_EXECUTION_FAILED_STATES
    ]
    items = [
        _action_index_item(
            "task-instance.get",
            targets="all",
        ),
        _action_index_item(
            "task-instance.watch",
            targets="all",
        ),
        _action_index_item(
            "task-instance.log",
            targets=_all_or_targets(log_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.sub-workflow",
            targets=_all_or_targets(sub_workflow_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.force-success",
            targets=_all_or_targets(force_success_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.savepoint",
            targets=_all_or_targets(active_targets, all_targets),
        ),
        _action_index_item(
            "task-instance.stop",
            targets=_all_or_targets(active_targets, all_targets),
        ),
    ]
    return _build_action_index(
        resource="task-instance",
        target_field="id",
        candidates=items,
        target_count=target_count,
        indexed_targets=all_targets,
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
        mutates=_command_mutates(action),
        needs_input=needs_input,
        targets=normalized_targets,
    )


def _build_action_index(
    *,
    resource: str,
    target_field: str,
    candidates: Sequence[_ActionCandidateData | None],
    target_count: int,
    indexed_targets: Sequence[int | str],
    truncated: bool,
) -> ActionIndexData:
    """Build the shared bounded index envelope around resource-specific rules."""
    groups = _group_action_candidates(
        candidates,
        indexed_targets=indexed_targets,
        all_rows_indexed=len(indexed_targets) == target_count and not truncated,
    )
    return {
        "scope": "data.totalList",
        "target": {"resource": resource, "field": target_field},
        "authorization": "not_evaluated",
        "eligibility": "row_facts_only",
        "groups": groups,
        "schema_command_pattern": "dsctl schema --command ACTION",
        "group_command": f"dsctl schema --group {resource}",
        "target_count": target_count,
        "indexed_target_count": len(indexed_targets),
        "truncated": truncated,
    }


def _group_action_candidates(
    candidates: Sequence[_ActionCandidateData | None],
    *,
    indexed_targets: Sequence[int | str],
    all_rows_indexed: bool,
) -> list[ActionIndexGroupData]:
    """Intern identical target lists so selectors are emitted only once."""
    groups: list[ActionIndexGroupData] = []
    group_indexes: dict[tuple[int | str, ...], int] = {}
    for candidate in candidates:
        if candidate is None:
            continue
        targets = candidate["targets"]
        if targets == "all" and not all_rows_indexed:
            targets = list(indexed_targets)
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
    "workflow.edit": _workflow_change_navigation,
    "workflow.offline": _workflow_change_navigation,
    "schedule.create": _schedule_navigation,
    "schedule.update": _schedule_navigation,
    "schedule.online": _schedule_navigation,
    "schedule.offline": _schedule_navigation,
    "task-group.get": _task_group_navigation,
    "task-group.create": _task_group_navigation,
    "task-group.update": _task_group_navigation,
    "task-group.start": _task_group_navigation,
    "task-group.queue.list": _task_group_queue_navigation,
    "workflow.run": _workflow_run_navigation,
    "workflow.run-task": _workflow_run_navigation,
    "workflow.backfill": _workflow_run_navigation,
    "workflow-instance.list": _trigger_instances_navigation,
    "workflow-instance.rerun": _workflow_replay_navigation,
    "workflow-instance.recover-failed": _workflow_replay_navigation,
    "workflow-instance.execute-task": _workflow_replay_navigation,
    "workflow-instance.watch": _workflow_watch_navigation,
    "workflow-instance.digest": _workflow_digest_navigation,
    "task-instance.list": _task_list_navigation,
    "task-instance.log": _task_log_navigation,
}
