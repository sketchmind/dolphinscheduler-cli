from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from tests.live.execution_support import _execution_instance_id
from tests.live.project_lifecycle_cleanup import (
    ProjectLifecycleCleanup,
    ProjectNativeIdentity,
)
from tests.live.support import (
    DsctlCommandResult,
    cleanup_live_resources,
    require_error_payload,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    require_text_value,
    run_dsctl,
    wait_for_result,
)
from tests.live.task_group_support import (
    require_task_group_get_absent,
    require_task_group_list_absent,
    require_task_group_project_cleanup_version,
)
from tests.live.workflow_support import (
    delete_project_eventually,
    delete_workflow_eventually,
    write_parallel_task_group_workflow_spec,
    write_single_shell_workflow_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]

TASK_CONTROL_VERSIONS = frozenset(
    version
    for version in TARGET_DS_VERSIONS
    if version.startswith(("3.1.", "3.2.", "3.3.", "3.4."))
)
EXECUTE_TASK_API_VERSIONS = frozenset(
    version
    for version in TARGET_DS_VERSIONS
    if version.startswith(("3.2.", "3.3.", "3.4."))
)
EXECUTE_TASK_RUN_TIMES_VERSIONS = frozenset({"3.2.0", "3.2.1", "3.2.2"})
EXECUTE_TASK_REUSED_ID_VERSIONS = frozenset({"3.2.0", "3.2.1"})
EXECUTE_TASK_VERSIONS = EXECUTE_TASK_RUN_TIMES_VERSIONS | {"3.4.2", "3.4.3"}
EXECUTE_TASK_NO_MASTER_HANDLER_VERSIONS = (
    EXECUTE_TASK_API_VERSIONS - EXECUTE_TASK_VERSIONS
)
TASK_STOP_SIGINT_FAILURE_VERSIONS = frozenset(
    {"3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2"}
)
# These exact upstream families map only exit 137 to KILL. A graceful TERM
# can therefore yield FAILURE; accept it only with a matching native exit log.
TASK_STOP_TERM_FAILURE_VERSIONS = frozenset(
    {f"3.1.{patch}" for patch in range(10)} | {"3.2.0", "3.2.1", "3.2.2"}
)
TASK_STOP_VERIFIED_FAILURE_VERSIONS = frozenset(
    version for version in TARGET_DS_VERSIONS if version.startswith(("3.1.", "3.2."))
)
TASK_STOP_VERIFIED_STOP_VERSIONS = frozenset(
    version for version in TARGET_DS_VERSIONS if version.startswith(("3.3.", "3.4."))
)


def _require_exact_version(
    repo_root: Path, env_file: Path, selected: str | None
) -> str:
    if selected is None or selected.lower() == "auto":
        payload = require_ok_payload(
            run_dsctl(repo_root, ["version"], env_file=env_file),
            expected_action="version",
            label="runtime-control version preflight",
        )
        data = require_mapping(payload["data"], label="runtime-control version data")
        assert data.get("version_source") not in {None, "offline_default"}
        selected = require_text_value(
            data.get("selected_ds_version"), label="runtime-control exact version"
        )
    assert selected in TARGET_DS_VERSIONS, f"unreviewed DS version: {selected}"
    return selected


def _require_action_version(version: str, *, action: str) -> None:
    if action == "workflow-instance.execute-task" and version in (
        EXECUTE_TASK_NO_MASTER_HANDLER_VERSIONS
    ):
        pytest.skip(
            f"{action} API exists in DS {version}, but its master lacks an "
            "EXECUTE_TASK command handler"
        )
    supported = (
        TASK_CONTROL_VERSIONS
        if action in {"task-instance.savepoint", "task-instance.stop"}
        else EXECUTE_TASK_VERSIONS
    )
    if version not in supported:
        pytest.skip(f"{action} has no upstream REST operation in DS {version}")


def _task_data(result: DsctlCommandResult, *, action: str) -> dict[str, object] | None:
    payload = result.payload
    if result.exit_code != 0 or payload.get("ok") is not True:
        return None
    if payload.get("action") != action:
        return None
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def _assert_task_identity(
    task: dict[str, object], *, task_id: int, workflow_instance_id: int, name: str
) -> None:
    assert task.get("id") == task_id
    assert task.get("workflowInstanceId") == workflow_instance_id
    assert task.get("name") == name


def _require_signal_task_stop_log(
    result: DsctlCommandResult, *, task_id: int, exit_code: int
) -> None:
    assert exit_code in {130, 143}
    payload = require_ok_payload(
        result, expected_action="task-instance.log", label="stopped task log"
    )
    resolved = require_mapping(
        payload.get("resolved"), label="stopped task log resolved"
    )
    selected_task = require_mapping(
        resolved.get("taskInstance"), label="stopped task log task selection"
    )
    assert selected_task.get("id") == task_id
    data = require_mapping(payload.get("data"), label="stopped task log data")
    window = require_mapping(data.get("window"), label="stopped task log window")
    assert window.get("mode") == "tail"
    assert window.get("scope_complete") is True
    log_text = require_text_value(data.get("text"), label="stopped task log text")
    # The native executor records both the task exit status and process exit
    # value in one line. A failed task state alone could be unrelated to stop.
    exit_line = re.compile(
        r"process has exited[.,] execute path:.*processId:\s*[1-9]\d*\s*,"
        rf"exitStatusCode:\s*{exit_code}\s*,processWaitForStatus:\s*(?:true|false)\s*,"
        rf"processExitValue:\s*{exit_code}(?!\d)"
    )
    assert any(exit_line.search(line) for line in log_text.splitlines()), (
        f"task {task_id} FAILURE lacks a native exit-{exit_code} executor log line"
    )


def _expected_workflow_stop_state(version: str, task_state: str) -> str:
    assert task_state in {"KILL", "FAILURE"}
    if task_state == "FAILURE":
        assert version in (
            TASK_STOP_TERM_FAILURE_VERSIONS | TASK_STOP_SIGINT_FAILURE_VERSIONS
        )
        return "FAILURE"
    return "FAILURE" if version in TASK_STOP_VERIFIED_FAILURE_VERSIONS else "STOP"


def _expected_stopped_task_states(version: str) -> str | frozenset[str]:
    if version in (TASK_STOP_TERM_FAILURE_VERSIONS | TASK_STOP_SIGINT_FAILURE_VERSIONS):
        return frozenset({"KILL", "FAILURE"})
    return "KILL"


def _expected_workflow_instance_stop_state(version: str, task_state: str) -> str:
    assert task_state in {"KILL", "FAILURE"}
    if task_state == "FAILURE":
        assert version in (
            TASK_STOP_TERM_FAILURE_VERSIONS | TASK_STOP_SIGINT_FAILURE_VERSIONS
        )
        # In 3.1/3.2 READY_STOP, either KILL or FAILURE task ends in STOP.
        # In 3.3/3.4, the finished task chain selects STOP or FAILURE.
        return "STOP" if version in TASK_STOP_VERIFIED_FAILURE_VERSIONS else "FAILURE"
    return "STOP"


def _assert_action_warning(
    payload: dict[str, object], *, action: str, target_state: str | None = None
) -> None:
    data = require_mapping(payload["data"], label=f"{action} readback")
    state = require_text_value(data.get("state"), label=f"{action} readback state")
    warnings = require_list(payload.get("warnings", []), label=f"{action} warnings")
    assert len(warnings) <= 1
    if warnings:
        detail = require_mapping(warnings[0], label=f"{action} warning detail")
        assert detail["code"] == "workflow_instance_action_state_after_request"
        assert detail["action"] == action
        assert detail["message"] == (
            f"{action} requested; current workflow instance state is "
            f"{detail['current_state']}"
        )
        assert detail["current_state"] == state
        assert detail["target_state"] == target_state
        expect_non_final = target_state is None
        assert detail["expect_non_final"] is expect_non_final
        if expect_non_final:
            assert state in {"PAUSE", "STOP", "FAILURE", "SUCCESS"}
        else:
            assert state != target_state
    elif target_state is None:
        assert state not in {"PAUSE", "STOP", "FAILURE", "SUCCESS"}
    else:
        assert state == target_state


def _task_instance_rows(result: DsctlCommandResult) -> list[object] | None:
    payload = result.payload
    if result.exit_code != 0 or payload.get("ok") is not True:
        return None
    if payload.get("action") != "task-instance.list":
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    rows = data.get("totalList")
    if not isinstance(rows, list):
        return None
    return rows


def _queue_rows(result: DsctlCommandResult) -> list[object] | None:
    payload = result.payload
    if result.exit_code != 0 or payload.get("ok") is not True:
        return None
    if payload.get("action") != "task-group.queue.list":
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    rows = data.get("totalList")
    if not isinstance(rows, list):
        return None
    return rows


def _workflow_state(result: DsctlCommandResult) -> str | None:
    payload = result.payload
    if result.exit_code != 0 or payload.get("ok") is not True:
        return None
    if payload.get("action") != "workflow-instance.get":
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    state = data.get("state")
    if not isinstance(state, str):
        return None
    return state


def _workflow_run_times(result: DsctlCommandResult) -> int | None:
    payload = result.payload
    if result.exit_code != 0 or payload.get("ok") is not True:
        return None
    if payload.get("action") != "workflow-instance.get":
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    run_times = data.get("runTimes")
    if not isinstance(run_times, int):
        return None
    return run_times


def _poll_runtime(
    repo_root: Path,
    argv: list[str],
    *,
    env_file: Path,
    accept: Callable[[DsctlCommandResult], bool],
    timeout_seconds: float,
    interval_seconds: float = 2.0,
) -> DsctlCommandResult:
    def accept_or_fail_input(current: DsctlCommandResult) -> bool:
        error = current.payload.get("error")
        if (
            current.exit_code != 0
            and isinstance(error, dict)
            and error.get("type") == "user_input_error"
        ):
            message = f"runtime poll rejected input: {error.get('message')}"
            raise AssertionError(message)
        return accept(current)

    return wait_for_result(
        repo_root,
        argv,
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=interval_seconds,
        accept=accept_or_fail_input,
    )


def _wait_for_task_rows(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    timeout_seconds: float = 30.0,
) -> list[dict[str, object]]:
    result = _poll_runtime(
        repo_root,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            str(workflow_instance_id),
            "--project",
            project,
            "--page-size",
            "20",
        ],
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=2.0,
        accept=lambda current: bool(_task_instance_rows(current)),
    )
    payload = require_ok_payload(
        result,
        expected_action="task-instance.list",
        label="task-instance list",
    )
    data = require_mapping(payload["data"], label="task-instance list data")
    rows = require_list(data["totalList"], label="task-instance list rows")
    return [require_mapping(row, label="task-instance row") for row in rows]


def _wait_for_queue_rows(
    repo_root: Path,
    env_file: Path,
    *,
    task_group: str,
    task_group_id: int,
    project: str,
    workflow_instance_id: int,
    task_names: frozenset[str],
    queue_id: int | None = None,
    priority: int | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, object]:
    result = _poll_runtime(
        repo_root,
        [
            "task-group",
            "queue",
            "list",
            task_group,
            "--status",
            "WAIT_QUEUE",
            "--all",
        ],
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=2.0,
        accept=lambda current: (
            _owned_waiting_queue_row(
                _queue_rows(current) or [],
                task_group_id=task_group_id,
                project=project,
                workflow_instance_id=workflow_instance_id,
                task_names=task_names,
                queue_id=queue_id,
                priority=priority,
            )
            is not None
        ),
    )
    payload = require_ok_payload(
        result,
        expected_action="task-group.queue.list",
        label="task-group queue list",
    )
    data = require_mapping(payload["data"], label="task-group queue data")
    rows = require_list(data["totalList"], label="task-group queue rows")
    coverage = require_mapping(data.get("coverage"), label="task-group queue coverage")
    assert coverage.get("scope_complete") is True
    assert coverage.get("totals_changed") is False
    assert data.get("total") == len(rows)
    owned = _owned_waiting_queue_row(
        rows,
        task_group_id=task_group_id,
        project=project,
        workflow_instance_id=workflow_instance_id,
        task_names=task_names,
        queue_id=queue_id,
        priority=priority,
    )
    assert owned is not None, "owned waiting task-group queue row disappeared"
    return owned


def _owned_waiting_queue_row(
    rows: list[object],
    *,
    task_group_id: int,
    project: str,
    workflow_instance_id: int,
    task_names: frozenset[str],
    queue_id: int | None = None,
    priority: int | None = None,
) -> dict[str, object] | None:
    owned = [
        row
        for value in rows
        if isinstance(value, dict)
        for row in [value]
        if row.get("groupId") == task_group_id
        and row.get("projectName") == project
        and row.get("workflowInstanceId") == workflow_instance_id
        and row.get("taskName") in task_names
        and row.get("status") == "WAIT_QUEUE"
        and isinstance(row.get("id"), int)
        and not isinstance(row.get("id"), bool)
        and row["id"] > 0
        and (queue_id is None or row["id"] == queue_id)
        and (priority is None or row.get("priority") == priority)
    ]
    return owned[0] if len(owned) == 1 else None


def _wait_for_contested_task_group(
    repo_root: Path,
    env_file: Path,
    *,
    task_group: str,
    task_group_id: int,
) -> None:
    result = _poll_runtime(
        repo_root,
        ["task-group", "get", task_group],
        env_file=env_file,
        timeout_seconds=15.0,
        accept=lambda current: (
            (data := _task_data(current, action="task-group.get")) is not None
            and data.get("id") == task_group_id
            and data.get("name") == task_group
            and data.get("groupSize") == 1
            and data.get("useSize") == 1
        ),
    )
    payload = require_ok_payload(
        result, expected_action="task-group.get", label="contested task group"
    )
    data = require_mapping(payload["data"], label="contested task group data")
    assert data.get("id") == task_group_id
    assert data.get("name") == task_group
    assert data.get("groupSize") == 1
    assert data.get("useSize") == 1


def _fresh_owned_queue_task_group(
    repo_root: Path,
    env_file: Path,
    *,
    task_group: str,
    project_code: int,
    expected_id: int | None,
) -> dict[str, object] | None:
    result = run_dsctl(repo_root, ["task-group", "get", task_group], env_file=env_file)
    if result.exit_code != 0:
        require_error_payload(
            result,
            expected_action="task-group.get",
            expected_type="not_found",
            label="owned queue task-group cleanup get",
        )
        return None
    payload = require_ok_payload(
        result,
        expected_action="task-group.get",
        label="owned queue task-group cleanup get",
    )
    group = require_mapping(payload.get("data"), label="owned queue task-group")
    group_id = require_int_value(group.get("id"), label="owned queue task-group id")
    assert group_id > 0
    assert not isinstance(group_id, bool)
    assert group.get("name") == task_group
    assert group.get("projectCode") == project_code
    assert group.get("description") == "live task-group queue control group"
    assert group.get("groupSize") == 1
    if expected_id is not None:
        assert group_id == expected_id, "task-group cleanup found a different identity"
    return group


def _close_owned_queue_task_group(
    repo_root: Path,
    env_file: Path,
    *,
    task_group: str,
    project_code: int,
    expected_id: int | None,
) -> None:
    group = _fresh_owned_queue_task_group(
        repo_root,
        env_file,
        task_group=task_group,
        project_code=project_code,
        expected_id=expected_id,
    )
    # With an uncertain create receipt, the owned project's verified cascade
    # removes the group; a global name cannot authorize a direct close.
    if group is None or expected_id is None:
        return
    close_payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["task-group", "close", str(expected_id)],
            env_file=env_file,
        ),
        expected_action="task-group.close",
        label="owned task-group cleanup close",
    )
    closed = require_mapping(close_payload.get("data"), label="closed task-group data")
    assert closed.get("id") == expected_id


def _require_only_owned_project_task_group(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    project_code: int,
    task_group: str,
    expected_id: int | None,
    create_attempted: bool,
) -> None:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["task-group", "list", "--project", project, "--all"],
            env_file=env_file,
        ),
        expected_action="task-group.list",
        label="owned project task-group cleanup list",
    )
    data = require_mapping(payload.get("data"), label="owned project task groups")
    coverage = require_mapping(data.get("coverage"), label="owned group coverage")
    assert coverage.get("scope_complete") is True
    assert coverage.get("totals_changed") is False
    rows = require_list(data.get("totalList"), label="owned project group rows")
    assert data.get("total") == len(rows)
    assert len(rows) <= 1, "owned project contains an unexpected task group"
    if not rows:
        return
    assert create_attempted, "task group appeared without an owned create attempt"
    group = require_mapping(rows[0], label="owned project task-group row")
    group_id = require_int_value(group.get("id"), label="owned project group id")
    assert group_id > 0
    assert not isinstance(group_id, bool)
    assert group.get("name") == task_group
    assert group.get("projectCode") == project_code
    assert group.get("description") == "live task-group queue control group"
    assert group.get("groupSize") == 1
    if expected_id is not None:
        assert group_id == expected_id, "project cascade found a different group"


def _wait_for_slot_holder_running(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    task_group_id: int,
    holder_task_name: str,
) -> None:
    result = _poll_runtime(
        repo_root,
        [
            "task-instance",
            "list",
            "--project",
            project,
            "--workflow-instance",
            str(workflow_instance_id),
            "--task",
            holder_task_name,
            "--state",
            "RUNNING_EXECUTION",
            "--all",
        ],
        env_file=env_file,
        timeout_seconds=15.0,
        accept=lambda current: any(
            isinstance(row, dict)
            and row.get("name") == holder_task_name
            and row.get("workflowInstanceId") == workflow_instance_id
            and row.get("taskGroupId") == task_group_id
            and row.get("state") == "RUNNING_EXECUTION"
            for row in (_task_instance_rows(current) or [])
        ),
    )
    payload = require_ok_payload(
        result, expected_action="task-instance.list", label="slot holder list"
    )
    data = require_mapping(payload["data"], label="slot holder list data")
    rows = require_list(data.get("totalList"), label="slot holder task rows")
    matching = [
        row
        for value in rows
        for row in [require_mapping(value, label="slot holder task")]
        if row.get("name") == holder_task_name
        and row.get("workflowInstanceId") == workflow_instance_id
        and row.get("taskGroupId") == task_group_id
        and row.get("state") == "RUNNING_EXECUTION"
    ]
    assert len(matching) == 1, "slot holder was not running before force-start"


def _wait_for_workflow_state(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    target_state: str,
    timeout_seconds: float = 30.0,
) -> dict[str, object]:
    result = _poll_runtime(
        repo_root,
        ["workflow-instance", "get", str(workflow_instance_id), "--project", project],
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=2.0,
        accept=lambda current: _workflow_state(current) == target_state,
    )
    payload = require_ok_payload(
        result,
        expected_action="workflow-instance.get",
        label="workflow-instance get",
    )
    data = require_mapping(payload["data"], label="workflow-instance get data")
    assert data["state"] == target_state
    return data


def _wait_for_task_state(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    task_id: int,
    workflow_instance_id: int,
    name: str,
    target_state: str | frozenset[str],
    timeout_seconds: float = 60.0,
) -> dict[str, object]:
    expected_states = (
        frozenset({target_state}) if isinstance(target_state, str) else target_state
    )
    assert expected_states
    result = _poll_runtime(
        repo_root,
        [
            "task-instance",
            "get",
            str(task_id),
            "--workflow-instance",
            str(workflow_instance_id),
            "--project",
            project,
        ],
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=2.0,
        accept=lambda current: (
            (data := _task_data(current, action="task-instance.get")) is not None
            and data.get("id") == task_id
            and data.get("workflowInstanceId") == workflow_instance_id
            and data.get("name") == name
            and data.get("state") in expected_states
        ),
    )
    payload = require_ok_payload(
        result, expected_action="task-instance.get", label="task-instance get"
    )
    data = require_mapping(payload["data"], label="task-instance get data")
    _assert_task_identity(
        data, task_id=task_id, workflow_instance_id=workflow_instance_id, name=name
    )
    assert data["state"] in expected_states
    return data


def _run_workflow(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow: str,
    attempt: dict[str, bool],
) -> int:
    # A lost receipt can follow server acceptance. Keep the exclusive fixture
    # if its instance identity cannot be reconciled from the native list.
    attempt["sent"] = True
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["workflow", "run", workflow, "--project", project],
            env_file=env_file,
        ),
        expected_action="workflow.run",
        label="workflow run",
    )
    data = require_mapping(payload["data"], label="workflow run data")
    return _execution_instance_id(
        data,
        repo_root=repo_root,
        env_file=env_file,
        project=project,
        fresh_workflow=workflow,
        label="runtime-control workflow run",
    )


def _wait_for_cleanup_terminal(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    after_run_times: int | None = None,
    timeout_seconds: int = 180,
) -> None:
    argv = [
        "workflow-instance",
        "watch",
        str(workflow_instance_id),
        "--project",
        project,
    ]
    if after_run_times is not None:
        argv.extend(("--after-run-times", str(after_run_times)))
    argv.extend(("--interval-seconds", "2", "--timeout-seconds", str(timeout_seconds)))
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            argv,
            env_file=env_file,
            timeout_seconds=float(timeout_seconds + 10),
        ),
        expected_action="workflow-instance.watch",
        label="workflow instance cleanup watch",
    )
    data = require_mapping(payload["data"], label="cleanup terminal instance")
    assert data["id"] == workflow_instance_id
    assert data["state"] in {"SUCCESS", "FAILURE", "STOP", "PAUSE"}
    if after_run_times is not None:
        assert (
            require_int_value(data.get("runTimes"), label="cleanup runTimes")
            > after_run_times
        )


def _require_run_identity_before_cleanup(
    *, attempt: dict[str, bool], workflow_instance_id: int | None
) -> None:
    if attempt["sent"] and workflow_instance_id is None:
        message = (
            "Workflow run was attempted but its instance identity is unresolved; "
            "owned fixture retained for reconciliation"
        )
        raise AssertionError(message)


def _task_time_advanced(current: object, previous: object) -> bool:
    if not isinstance(current, str) or not isinstance(previous, str):
        return False
    try:
        return datetime.fromisoformat(current) > datetime.fromisoformat(previous)
    except (TypeError, ValueError):
        return False


def _assert_force_started_task_overlap(
    rows: list[dict[str, object]],
    *,
    workflow_instance_id: int,
    queued_task_name: str,
    holder_task_name: str,
) -> None:
    names = {queued_task_name, holder_task_name}
    assert len(names) == 2
    owned = [
        row
        for row in rows
        if row.get("workflowInstanceId") == workflow_instance_id
        and row.get("name") in names
    ]
    assert len(owned) == 2, "expected exactly two owned group task instances"
    tasks = {
        require_text_value(row.get("name"), label="group task name"): row
        for row in owned
    }
    assert set(tasks) == names
    assert all(row.get("state") == "SUCCESS" for row in tasks.values())
    task_ids = [
        require_int_value(row.get("id"), label="group task id")
        for row in tasks.values()
    ]
    assert all(not isinstance(task_id, bool) and task_id > 0 for task_id in task_ids)
    assert len(set(task_ids)) == 2
    # A completed pair could have run sequentially when the holder naturally
    # released the only slot. Require the forced task's actual start to overlap.
    assert _task_time_advanced(
        tasks[holder_task_name].get("endTime"),
        tasks[queued_task_name].get("startTime"),
    ), "force-started task did not begin before the slot holder finished"


def _observe_execute_task_round_terminal(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    prior_run_times: int,
    prior_task_id: int,
    prior_task_start_time: str | None,
    prior_task_end_time: str | None,
    version: str,
) -> tuple[dict[str, object], dict[str, object]]:
    # 3.2.0/.1 can upsert the original task ID: clearDataIfExecuteTask omits
    # taskCodeInstanceMap until 3.2.2. Prove that reused ID actually ran again.
    reuses_task_id = version in EXECUTE_TASK_REUSED_ID_VERSIONS
    if reuses_task_id:
        assert prior_task_start_time is not None
        assert prior_task_end_time is not None

    def expected_task_rows(current: DsctlCommandResult) -> bool:
        rows = _task_instance_rows(current) or []
        return any(
            isinstance(row, dict)
            and row.get("name") == "echo-task"
            and isinstance(row.get("id"), int)
            and (
                row.get("id") == prior_task_id
                if reuses_task_id
                else row.get("id") != prior_task_id
            )
            and row.get("workflowInstanceId") == workflow_instance_id
            for row in rows
        )

    rows_result = _poll_runtime(
        repo_root,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            str(workflow_instance_id),
            "--project",
            project,
            "--page-size",
            "20",
        ],
        env_file=env_file,
        timeout_seconds=90.0,
        interval_seconds=2.0,
        accept=expected_task_rows,
    )
    rows_payload = require_ok_payload(
        rows_result,
        expected_action="task-instance.list",
        label="task-instance list after execute-task",
    )
    rows_data = require_mapping(rows_payload["data"], label="execute-task rows data")
    rows = require_list(rows_data["totalList"], label="execute-task rows")
    task_ids = set()
    for item in rows:
        row = require_mapping(item, label="execute-task row")
        if row.get("name") == "echo-task":
            assert row.get("workflowInstanceId") == workflow_instance_id
            task_ids.add(require_int_value(row.get("id"), label="echo task id"))
    if reuses_task_id:
        assert task_ids == {prior_task_id}, "execute-task reused-ID scope changed"
        round_task_id = prior_task_id
    else:
        new_task_ids = task_ids - {prior_task_id}
        assert len(new_task_ids) == 1, "execute-task must create one new task"
        round_task_id = next(iter(new_task_ids))

    def current_round_is_terminal(current: DsctlCommandResult) -> bool:
        data = _task_data(current, action="workflow-instance.get")
        return (
            data is not None
            and data.get("id") == workflow_instance_id
            and data.get("commandType") == "EXECUTE_TASK"
            and data.get("state") in {"SUCCESS", "FAILURE", "STOP", "PAUSE"}
            and (
                version not in EXECUTE_TASK_RUN_TIMES_VERSIONS
                or (
                    isinstance(run_times := data.get("runTimes"), int)
                    and not isinstance(run_times, bool)
                    and run_times > prior_run_times
                )
            )
        )

    workflow_result = _poll_runtime(
        repo_root,
        ["workflow-instance", "get", str(workflow_instance_id), "--project", project],
        env_file=env_file,
        timeout_seconds=180.0,
        interval_seconds=2.0,
        accept=current_round_is_terminal,
    )
    workflow_payload = require_ok_payload(
        workflow_result,
        expected_action="workflow-instance.get",
        label="execute-task terminal workflow",
    )
    workflow = require_mapping(
        workflow_payload["data"], label="execute-task terminal workflow data"
    )
    assert current_round_is_terminal(workflow_result)

    def round_task_is_terminal(current: DsctlCommandResult) -> bool:
        task = _task_data(current, action="task-instance.get")
        return (
            task is not None
            and task.get("id") == round_task_id
            and task.get("workflowInstanceId") == workflow_instance_id
            and task.get("name") == "echo-task"
            and task.get("state") in {"SUCCESS", "FAILURE", "KILL"}
            and (
                not reuses_task_id
                or (
                    _task_time_advanced(task.get("startTime"), prior_task_start_time)
                    and _task_time_advanced(task.get("endTime"), prior_task_end_time)
                )
            )
        )

    task_result = _poll_runtime(
        repo_root,
        [
            "task-instance",
            "get",
            str(round_task_id),
            "--workflow-instance",
            str(workflow_instance_id),
            "--project",
            project,
        ],
        env_file=env_file,
        timeout_seconds=30.0,
        interval_seconds=2.0,
        accept=round_task_is_terminal,
    )
    task_payload = require_ok_payload(
        task_result,
        expected_action="task-instance.get",
        label="execute-task terminal task",
    )
    task = require_mapping(
        task_payload["data"], label="execute-task terminal task data"
    )
    assert round_task_is_terminal(task_result)
    return task, workflow


def test_etl_running_runtime_control_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_ds_version: str | None,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    version = _require_exact_version(
        live_repo_root, live_etl_env_file, live_etl_ds_version
    )
    _require_action_version(version, action="task-instance.stop")
    assert version in (
        TASK_STOP_VERIFIED_FAILURE_VERSIONS | TASK_STOP_VERIFIED_STOP_VERSIONS
    )
    project_name = live_name_factory("runtime-control-project")
    workflow_name = live_name_factory("runtime-control-workflow")
    stop_workflow_name = live_name_factory("runtime-stop-workflow")
    workflow_spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="sleep-task",
        command='echo "runtime-control" && sleep 120',
        description="live runtime control surfaces",
    )
    stop_workflow_spec = write_single_shell_workflow_spec(
        tmp_path / f"{stop_workflow_name}.yaml",
        project_name=project_name,
        workflow_name=stop_workflow_name,
        task_name="workflow-stop-task",
        command='echo "runtime-stop" && sleep 120',
        description="live workflow stop control surface",
    )

    project_created = False
    workflow_created = False
    stop_workflow_created = False
    workflow_instance_id: int | None = None
    run_attempt = {"sent": False}
    workflow_stop_instance_id: int | None = None
    stop_run_attempt = {"sent": False}

    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "live runtime control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="workflow create",
        )
        workflow_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="workflow online",
        )

        workflow_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            attempt=run_attempt,
        )

        task_rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
        )
        task_instance_id = require_int_value(
            task_rows[0].get("id"),
            label="task instance id",
        )
        _assert_task_identity(
            task_rows[0],
            task_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            name="sleep-task",
        )
        _wait_for_task_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            task_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            name="sleep-task",
            target_state="RUNNING_EXECUTION",
        )

        savepoint_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "savepoint",
                    str(task_instance_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.savepoint",
            label="task-instance savepoint",
        )
        savepoint_data = require_mapping(
            savepoint_payload["data"],
            label="task-instance savepoint data",
        )
        assert savepoint_data["requested"] is True
        _assert_task_identity(
            require_mapping(savepoint_data["taskInstance"], label="savepoint task"),
            task_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            name="sleep-task",
        )

        task_stop_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "stop",
                    str(task_instance_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.stop",
            label="task-instance stop",
        )
        task_stop_data = require_mapping(
            task_stop_payload["data"],
            label="task-instance stop data",
        )
        assert task_stop_data["requested"] is True
        _assert_task_identity(
            require_mapping(task_stop_data["taskInstance"], label="stopped task"),
            task_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            name="sleep-task",
        )
        stopped_task = _wait_for_task_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            task_id=task_instance_id,
            workflow_instance_id=workflow_instance_id,
            name="sleep-task",
            target_state=_expected_stopped_task_states(version),
        )
        if stopped_task["state"] == "FAILURE":
            exit_code = 143 if version in TASK_STOP_TERM_FAILURE_VERSIONS else 130
            assert version in (
                TASK_STOP_TERM_FAILURE_VERSIONS | TASK_STOP_SIGINT_FAILURE_VERSIONS
            )
            _require_signal_task_stop_log(
                run_dsctl(
                    live_repo_root,
                    ["task-instance", "log", str(task_instance_id), "--tail", "500"],
                    env_file=live_etl_env_file,
                ),
                task_id=task_instance_id,
                exit_code=exit_code,
            )
        _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            target_state=_expected_workflow_stop_state(
                version,
                require_text_value(stopped_task["state"], label="stopped task state"),
            ),
            timeout_seconds=90.0,
        )

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(stop_workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="stop workflow create",
        )
        stop_workflow_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", stop_workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="stop workflow online",
        )

        workflow_stop_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=stop_workflow_name,
            attempt=stop_run_attempt,
        )
        stop_task_rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_stop_instance_id,
        )
        stop_task_id = require_int_value(
            stop_task_rows[0].get("id"), label="stop task id"
        )
        _assert_task_identity(
            stop_task_rows[0],
            task_id=stop_task_id,
            workflow_instance_id=workflow_stop_instance_id,
            name="workflow-stop-task",
        )
        _wait_for_task_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            task_id=stop_task_id,
            workflow_instance_id=workflow_stop_instance_id,
            name="workflow-stop-task",
            target_state="RUNNING_EXECUTION",
        )

        workflow_stop_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "stop",
                    str(workflow_stop_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.stop",
            label="workflow-instance stop",
        )
        workflow_stop_data = require_mapping(
            workflow_stop_payload["data"], label="workflow stop data"
        )
        assert workflow_stop_data["id"] == workflow_stop_instance_id
        _assert_action_warning(
            workflow_stop_payload, action="stop", target_state="STOP"
        )

        workflow_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_stop_instance_id),
                    "--project",
                    project_name,
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "60",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=70.0,
            ),
            expected_action="workflow-instance.watch",
            label="workflow-instance watch",
        )
        workflow_watch_data = require_mapping(
            workflow_watch_payload["data"],
            label="workflow-instance watch data",
        )
        assert workflow_watch_data.get("id") == workflow_stop_instance_id
        workflow_final_state = require_text_value(
            workflow_watch_data.get("state"), label="workflow-instance final state"
        )
        stopped_workflow_task = _wait_for_task_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            task_id=stop_task_id,
            workflow_instance_id=workflow_stop_instance_id,
            name="workflow-stop-task",
            target_state=_expected_stopped_task_states(version),
        )
        stopped_task_state = require_text_value(
            stopped_workflow_task["state"], label="workflow-stopped task state"
        )
        if stopped_task_state == "FAILURE":
            exit_code = 143 if version in TASK_STOP_TERM_FAILURE_VERSIONS else 130
            _require_signal_task_stop_log(
                run_dsctl(
                    live_repo_root,
                    ["task-instance", "log", str(stop_task_id), "--tail", "500"],
                    env_file=live_etl_env_file,
                ),
                task_id=stop_task_id,
                exit_code=exit_code,
            )
        assert workflow_final_state == _expected_workflow_instance_stop_state(
            version, stopped_task_state
        )
    finally:

        def cleanup_workflow(
            name: str,
            *,
            created: bool,
            attempt: dict[str, bool],
            instance_id: int | None,
        ) -> None:
            _require_run_identity_before_cleanup(
                attempt=attempt, workflow_instance_id=instance_id
            )
            if instance_id is not None:
                _wait_for_cleanup_terminal(
                    live_repo_root,
                    live_etl_env_file,
                    project=project_name,
                    workflow_instance_id=instance_id,
                )
            if created:
                delete_workflow_eventually(
                    live_repo_root,
                    live_etl_env_file,
                    project=project_name,
                    workflow=name,
                )

        # Each independent workflow gets its own cleanup attempt. Keep the
        # shared project whenever either workflow cannot be proved quiescent.
        cleanup_live_resources(
            [
                lambda: cleanup_workflow(
                    workflow_name,
                    created=workflow_created,
                    attempt=run_attempt,
                    instance_id=workflow_instance_id,
                ),
                lambda: cleanup_workflow(
                    stop_workflow_name,
                    created=stop_workflow_created,
                    attempt=stop_run_attempt,
                    instance_id=workflow_stop_instance_id,
                ),
            ]
        )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )


def _assert_replay_round(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow_instance_id: int,
    previous_run_times: int,
    action: str,
    on_dispatch_attempted: Callable[[], None] | None = None,
) -> dict[str, object]:
    if on_dispatch_attempted is not None:
        # A lost or malformed response may follow an accepted request. Cleanup
        # must not treat the previous terminal round as proof of quiescence.
        on_dispatch_attempted()
    if action == "rerun":
        response = run_dsctl(
            repo_root,
            [
                "workflow-instance",
                "rerun",
                str(workflow_instance_id),
                "--project",
                project,
            ],
            env_file=env_file,
        )
    elif action == "recover-failed":
        response = run_dsctl(
            repo_root,
            [
                "workflow-instance",
                "recover-failed",
                str(workflow_instance_id),
                "--project",
                project,
            ],
            env_file=env_file,
        )
    else:
        message = f"unreviewed replay action: {action}"
        raise AssertionError(message)
    result = require_ok_payload(
        response,
        expected_action=f"workflow-instance.{action}",
        label=f"workflow-instance {action}",
    )
    data = require_mapping(result["data"], label=f"{action} readback")
    assert data["id"] == workflow_instance_id
    _assert_action_warning(result, action=action)
    resolved = require_mapping(result["resolved"], label=f"{action} resolved")
    baseline = require_mapping(
        resolved["execution_baseline"], label=f"{action} execution baseline"
    )
    assert baseline["run_times"] == previous_run_times

    watched = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "workflow-instance",
                "watch",
                str(workflow_instance_id),
                "--project",
                project,
                "--after-run-times",
                str(previous_run_times),
                "--interval-seconds",
                "2",
                "--timeout-seconds",
                "90",
            ],
            env_file=env_file,
            timeout_seconds=100.0,
        ),
        expected_action="workflow-instance.watch",
        label=f"workflow-instance watch after {action}",
    )
    final = require_mapping(watched["data"], label=f"{action} final instance")
    assert final["id"] == workflow_instance_id
    assert (
        require_int_value(final.get("runTimes"), label=f"{action} runTimes")
        > previous_run_times
    )
    assert final["state"] == "FAILURE"
    assert final["commandType"] == (
        "REPEAT_RUNNING" if action == "rerun" else "START_FAILURE_TASK_PROCESS"
    )
    return final


@pytest.mark.parametrize("action", ["recover-failed", "rerun"])
def test_etl_replay_control_surfaces_round_trip(
    action: str,
    live_repo_root: Path,
    live_etl_ds_version: str | None,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    _require_exact_version(live_repo_root, live_etl_env_file, live_etl_ds_version)
    project_name = live_name_factory(f"runtime-{action}-project")
    workflow_name = live_name_factory(f"runtime-{action}-workflow")
    spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="fail-task",
        command='echo "intentional replay failure" && exit 1',
        description=f"live {action} control surface",
    )
    project_created = False
    workflow_created = False
    workflow_instance_id: int | None = None
    run_attempt = {"sent": False}
    first_run_times: int | None = None
    replay_dispatch_attempted = False

    def mark_replay_dispatch_attempted() -> None:
        nonlocal replay_dispatch_attempted
        replay_dispatch_attempted = True

    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    f"live {action} control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="replay project create",
        )
        project_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="replay workflow create",
        )
        workflow_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="replay workflow online",
        )
        workflow_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            attempt=run_attempt,
        )
        first = _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            target_state="FAILURE",
            timeout_seconds=90.0,
        )
        first_run_times = require_int_value(
            first.get("runTimes"), label="first runTimes"
        )
        assert first_run_times > 0
        _assert_replay_round(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            previous_run_times=first_run_times,
            action=action,
            on_dispatch_attempted=mark_replay_dispatch_attempted,
        )
    finally:
        _require_run_identity_before_cleanup(
            attempt=run_attempt, workflow_instance_id=workflow_instance_id
        )
        if workflow_instance_id is not None:
            _wait_for_cleanup_terminal(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow_instance_id=workflow_instance_id,
                after_run_times=first_run_times if replay_dispatch_attempted else None,
            )
        if workflow_created:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root, live_etl_env_file, project=project_name
            )


def test_etl_force_success_control_surface_round_trip(
    live_repo_root: Path,
    live_etl_ds_version: str | None,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    version = _require_exact_version(
        live_repo_root, live_etl_env_file, live_etl_ds_version
    )
    if version == "1.3.9":
        pytest.skip(
            "task-instance.force-success has no upstream REST operation in DS 1.3.9"
        )
    project_name = live_name_factory("runtime-force-project")
    workflow_name = live_name_factory("runtime-force-workflow")
    spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="fail-task",
        command='echo "intentional force-success failure" && exit 1',
        description="live force-success control surface",
    )
    project_created = False
    workflow_created = False
    workflow_instance_id: int | None = None
    run_attempt = {"sent": False}
    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "live force-success control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="force-success project create",
        )
        project_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="force-success workflow create",
        )
        workflow_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="force-success workflow online",
        )
        workflow_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            attempt=run_attempt,
        )
        _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            target_state="FAILURE",
            timeout_seconds=90.0,
        )
        rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
        )
        assert len(rows) == 1
        task_id = require_int_value(rows[0].get("id"), label="failed task id")
        _assert_task_identity(
            rows[0],
            task_id=task_id,
            workflow_instance_id=workflow_instance_id,
            name="fail-task",
        )
        result = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "force-success",
                    str(task_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.force-success",
            label="task-instance force-success",
        )
        data = require_mapping(result["data"], label="force-success task")
        _assert_task_identity(
            data,
            task_id=task_id,
            workflow_instance_id=workflow_instance_id,
            name="fail-task",
        )
        assert data["state"] == "FORCED_SUCCESS"
    finally:
        _require_run_identity_before_cleanup(
            attempt=run_attempt, workflow_instance_id=workflow_instance_id
        )
        if workflow_instance_id is not None:
            _wait_for_cleanup_terminal(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow_instance_id=workflow_instance_id,
            )
        if workflow_created:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )


def test_etl_execute_task_control_surface_round_trip(
    live_repo_root: Path,
    live_etl_ds_version: str | None,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    version = _require_exact_version(
        live_repo_root, live_etl_env_file, live_etl_ds_version
    )
    _require_action_version(version, action="workflow-instance.execute-task")
    project_name = live_name_factory("runtime-execute-project")
    workflow_name = live_name_factory("runtime-execute-workflow")
    # This covers TASK_ONLY for an independent SHELL task. Dependency subgraphs
    # are a separate upstream behavior and are not claimed by this scenario.
    spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="echo-task",
        command='echo "execute-task-self"',
        description="live independent execute-task self scope",
    )
    project_created = False
    workflow_created = False
    workflow_instance_id: int | None = None
    run_attempt = {"sent": False}
    execute_attempt = {"sent": False, "quiescent": False}
    prior_task_id: int | None = None
    prior_run_times: int | None = None
    prior_task_start_time: str | None = None
    prior_task_end_time: str | None = None
    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "live execute-task control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="execute-task project create",
        )
        project_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="execute-task workflow create",
        )
        workflow_created = True
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="execute-task workflow online",
        )
        workflow_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            attempt=run_attempt,
        )
        prior_workflow = _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            target_state="SUCCESS",
            timeout_seconds=90.0,
        )
        prior_run_times = require_int_value(
            prior_workflow.get("runTimes"), label="prior workflow runTimes"
        )
        prior_tasks = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
        )
        assert len(prior_tasks) == 1
        assert prior_tasks[0].get("name") == "echo-task"
        prior_task_id = require_int_value(
            prior_tasks[0].get("id"), label="prior echo task id"
        )
        _assert_task_identity(
            prior_tasks[0],
            task_id=prior_task_id,
            workflow_instance_id=workflow_instance_id,
            name="echo-task",
        )
        assert prior_tasks[0]["state"] == "SUCCESS"
        if version in EXECUTE_TASK_REUSED_ID_VERSIONS:
            prior_task_start_time = require_text_value(
                prior_tasks[0].get("startTime"), label="prior echo startTime"
            )
            prior_task_end_time = require_text_value(
                prior_tasks[0].get("endTime"), label="prior echo endTime"
            )
        execute_attempt["sent"] = True
        result = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "execute-task",
                    str(workflow_instance_id),
                    "--task",
                    "echo-task",
                    "--scope",
                    "self",
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.execute-task",
            label="workflow-instance execute-task",
        )
        readback = require_mapping(result["data"], label="execute-task readback")
        assert readback["id"] == workflow_instance_id
        resolved = require_mapping(result["resolved"], label="execute-task resolved")
        assert resolved["scope"] == "self"
        task = require_mapping(resolved["task"], label="execute-task task")
        assert task["name"] == "echo-task"
        _assert_action_warning(result, action="execute-task")

        observed_task, observed_workflow = _observe_execute_task_round_terminal(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            prior_run_times=prior_run_times,
            prior_task_id=prior_task_id,
            prior_task_start_time=prior_task_start_time,
            prior_task_end_time=prior_task_end_time,
            version=version,
        )
        execute_attempt["quiescent"] = True
        assert observed_task["state"] == "SUCCESS"
        assert observed_workflow["state"] == "SUCCESS"
    finally:
        _require_run_identity_before_cleanup(
            attempt=run_attempt, workflow_instance_id=workflow_instance_id
        )
        if execute_attempt["sent"] and not execute_attempt["quiescent"]:
            assert workflow_instance_id is not None
            assert prior_run_times is not None
            assert prior_task_id is not None
            _observe_execute_task_round_terminal(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow_instance_id=workflow_instance_id,
                prior_run_times=prior_run_times,
                prior_task_id=prior_task_id,
                prior_task_start_time=prior_task_start_time,
                prior_task_end_time=prior_task_end_time,
                version=version,
            )
        if workflow_instance_id is not None:
            _wait_for_cleanup_terminal(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow_instance_id=workflow_instance_id,
            )
        if workflow_created:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root, live_etl_env_file, project=project_name
            )


def test_etl_task_group_queue_control_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_ds_version: str | None,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    require_task_group_project_cleanup_version(
        live_repo_root,
        live_etl_env_file,
        live_etl_ds_version,
        execute=run_dsctl,
    )

    project_name = live_name_factory("task-group-queue-project")
    workflow_name = live_name_factory("task-group-queue-workflow")
    task_group_name = live_name_factory("task-group-queue-group")

    project_created = False
    workflow_created = False
    task_group_create_attempted = False
    task_group_id: int | None = None
    project_code: int | None = None
    workflow_instance_id: int | None = None
    run_attempt = {"sent": False}
    task_names = frozenset({"slot-one", "slot-two"})

    def before_project_delete(
        identity: ProjectNativeIdentity, _project: Mapping[str, object]
    ) -> None:
        assert identity.kind == "code", "task-group project needs native code"
        group = _fresh_owned_queue_task_group(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
            project_code=identity.value,
            expected_id=task_group_id,
        )
        if group is not None:
            assert task_group_create_attempted, (
                "task-group appeared without an owned create attempt"
            )
        _require_only_owned_project_task_group(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            project_code=identity.value,
            task_group=task_group_name,
            expected_id=task_group_id,
            create_attempted=task_group_create_attempted,
        )

    project_cleanup = ProjectLifecycleCleanup(
        live_repo_root,
        live_etl_env_file,
        initial_name=project_name,
        updated_name=None,
        owned_descriptions=("live task-group queue control project",),
        execute=run_dsctl,
        before_delete=before_project_delete,
    )
    project_cleanup.prepare()
    require_task_group_get_absent(
        live_repo_root,
        live_etl_env_file,
        task_group=task_group_name,
        execute=run_dsctl,
    )
    require_task_group_list_absent(
        live_repo_root,
        live_etl_env_file,
        task_group=task_group_name,
        execute=run_dsctl,
    )

    try:
        project_cleanup.mark_create_attempted()
        project_created = True
        project_result = run_dsctl(
            live_repo_root,
            [
                "project",
                "create",
                "--name",
                project_name,
                "--description",
                "live task-group queue control project",
            ],
            env_file=live_etl_env_file,
        )
        project_payload = require_ok_payload(
            project_result,
            expected_action="project.create",
            label="project create",
        )
        project_identity = project_cleanup.remember_identity(
            project_payload, label="queue project create"
        )
        assert project_identity.kind == "code"
        project_code = project_identity.value

        task_group_create_attempted = True
        task_group_result = run_dsctl(
            live_repo_root,
            [
                "task-group",
                "create",
                "--project",
                project_name,
                "--name",
                task_group_name,
                "--group-size",
                "1",
                "--description",
                "live task-group queue control group",
            ],
            env_file=live_etl_env_file,
        )
        task_group_payload = require_ok_payload(
            task_group_result,
            expected_action="task-group.create",
            label="task-group create",
        )
        task_group_data = require_mapping(
            task_group_payload["data"],
            label="task-group create data",
        )
        task_group_id = require_int_value(
            task_group_data.get("id"),
            label="task-group id",
        )
        workflow_spec = write_parallel_task_group_workflow_spec(
            tmp_path / f"{workflow_name}.yaml",
            project_name=project_name,
            workflow_name=workflow_name,
            task_group_id=task_group_id,
            first_command='echo "slot one" && sleep 180',
            second_command='echo "slot two" && sleep 180',
        )
        workflow_created = True
        workflow_result = run_dsctl(
            live_repo_root,
            ["workflow", "create", "--file", str(workflow_spec)],
            env_file=live_etl_env_file,
        )
        require_ok_payload(
            workflow_result,
            expected_action="workflow.create",
            label="workflow create",
        )

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "online", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="workflow online",
        )

        workflow_instance_id = _run_workflow(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            attempt=run_attempt,
        )

        queue_row = _wait_for_queue_rows(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
            task_group_id=task_group_id,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            task_names=task_names,
        )
        queue_id = require_int_value(queue_row.get("id"), label="queue id")
        queued_task_name = require_text_value(
            queue_row.get("taskName"), label="queued task name"
        )
        holder_task_name = next(iter(task_names - {queued_task_name}))

        set_priority_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-group",
                    "queue",
                    "set-priority",
                    str(queue_id),
                    "--priority",
                    "5",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.queue.set-priority",
            label="task-group queue set-priority",
        )
        set_priority_data = require_mapping(
            set_priority_payload["data"],
            label="task-group queue set-priority data",
        )
        assert set_priority_data["queueId"] == queue_id
        assert set_priority_data["priority"] == 5
        _wait_for_queue_rows(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
            task_group_id=task_group_id,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            task_names=frozenset({queued_task_name}),
            queue_id=queue_id,
            priority=5,
        )
        _wait_for_contested_task_group(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
            task_group_id=task_group_id,
        )
        _wait_for_slot_holder_running(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            task_group_id=task_group_id,
            holder_task_name=holder_task_name,
        )
        _wait_for_queue_rows(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
            task_group_id=task_group_id,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
            task_names=frozenset({queued_task_name}),
            queue_id=queue_id,
            priority=5,
        )

        force_start_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["task-group", "queue", "force-start", str(queue_id)],
                env_file=live_etl_env_file,
            ),
            expected_action="task-group.queue.force-start",
            label="task-group queue force-start",
        )
        force_start_data = require_mapping(
            force_start_payload["data"],
            label="task-group queue force-start data",
        )
        assert force_start_data["queueId"] == queue_id
        assert force_start_data["accepted"] is True

        workflow_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
                    "--project",
                    project_name,
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "420",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=430.0,
            ),
            expected_action="workflow-instance.watch",
            label="workflow-instance watch",
        )
        workflow_watch_data = require_mapping(
            workflow_watch_payload["data"],
            label="workflow-instance watch data",
        )
        assert workflow_watch_data["state"] == "SUCCESS"
        task_rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow_instance_id=workflow_instance_id,
        )
        _assert_force_started_task_overlap(
            task_rows,
            workflow_instance_id=workflow_instance_id,
            queued_task_name=queued_task_name,
            holder_task_name=holder_task_name,
        )
    finally:
        _require_run_identity_before_cleanup(
            attempt=run_attempt, workflow_instance_id=workflow_instance_id
        )
        if workflow_instance_id is not None:
            _wait_for_cleanup_terminal(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow_instance_id=workflow_instance_id,
                timeout_seconds=420,
            )
        if workflow_created:
            delete_workflow_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
                workflow=workflow_name,
            )
        if task_group_create_attempted and project_code is not None:
            _close_owned_queue_task_group(
                live_repo_root,
                live_etl_env_file,
                task_group=task_group_name,
                project_code=project_code,
                expected_id=task_group_id,
            )
        if project_created:
            project_cleanup.cleanup()
            cleanup_live_resources(
                [
                    lambda: require_task_group_get_absent(
                        live_repo_root,
                        live_etl_env_file,
                        task_group=task_group_name,
                        execute=run_dsctl,
                    ),
                    lambda: require_task_group_list_absent(
                        live_repo_root,
                        live_etl_env_file,
                        task_group=task_group_name,
                        execute=run_dsctl,
                    ),
                ]
            )
