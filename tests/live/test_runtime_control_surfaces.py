from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    DsctlCommandResult,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    require_text_value,
    run_dsctl,
    wait_for_result,
)
from tests.live.workflow_support import (
    delete_project_eventually,
    write_parallel_task_group_workflow_spec,
    write_shell_workflow_spec,
    write_single_shell_workflow_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]


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


def _wait_for_task_rows(
    repo_root: Path,
    env_file: Path,
    *,
    workflow_instance_id: int,
    timeout_seconds: float = 30.0,
) -> list[dict[str, object]]:
    result = wait_for_result(
        repo_root,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            str(workflow_instance_id),
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
    timeout_seconds: float = 30.0,
) -> list[dict[str, object]]:
    result = wait_for_result(
        repo_root,
        ["task-group", "queue", "list", task_group, "--page-size", "20"],
        env_file=env_file,
        timeout_seconds=timeout_seconds,
        interval_seconds=2.0,
        accept=lambda current: bool(_queue_rows(current)),
    )
    payload = require_ok_payload(
        result,
        expected_action="task-group.queue.list",
        label="task-group queue list",
    )
    data = require_mapping(payload["data"], label="task-group queue data")
    rows = require_list(data["totalList"], label="task-group queue rows")
    return [require_mapping(row, label="task-group queue row") for row in rows]


def _wait_for_workflow_state(
    repo_root: Path,
    env_file: Path,
    *,
    workflow_instance_id: int,
    target_state: str,
    timeout_seconds: float = 30.0,
) -> dict[str, object]:
    result = wait_for_result(
        repo_root,
        ["workflow-instance", "get", str(workflow_instance_id)],
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


def test_etl_running_runtime_control_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("runtime-control-project")
    workflow_name = live_name_factory("runtime-control-workflow")
    workflow_spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="sleep-task",
        command='echo "runtime-control" && sleep 20',
        description="live runtime control surfaces",
    )

    project_created = False
    workflow_created = False
    workflow_deleted = False
    workflow_instance_id: int | None = None

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

        run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "run", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="workflow run",
        )
        run_data = require_mapping(run_payload["data"], label="workflow run data")
        workflow_instance_id = require_int_value(
            require_list(
                run_data["workflowInstanceIds"],
                label="workflow instance ids",
            )[0],
            label="workflow instance id",
        )

        task_rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            workflow_instance_id=workflow_instance_id,
        )
        task_instance_id = require_int_value(
            task_rows[0].get("id"),
            label="task instance id",
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

        task_stop_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "stop",
                    str(task_instance_id),
                    "--workflow-instance",
                    str(workflow_instance_id),
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

        workflow_stop_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow-instance", "stop", str(workflow_instance_id)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.stop",
            label="workflow-instance stop",
        )
        workflow_stop_warning_details = require_list(
            workflow_stop_payload["warning_details"],
            label="workflow-instance stop warning details",
        )
        workflow_stop_warning = require_mapping(
            workflow_stop_warning_details[0],
            label="workflow-instance stop warning detail",
        )
        assert workflow_stop_warning["code"] == (
            "workflow_instance_action_state_after_request"
        )
        assert workflow_stop_warning["action"] == "stop"
        assert workflow_stop_warning["current_state"] in {
            "RUNNING_EXECUTION",
            "READY_STOP",
        }
        workflow_stop_warnings = require_list(
            workflow_stop_payload["warnings"],
            label="workflow-instance stop warnings",
        )
        assert len(workflow_stop_warnings) == 1

        workflow_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
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
        assert (
            require_text_value(
                workflow_watch_data.get("state"),
                label="workflow-instance final state",
            )
            == "SUCCESS"
        )
    finally:
        if workflow_instance_id is not None:
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "60",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=70.0,
            )
        if workflow_created and not workflow_deleted:
            run_dsctl(
                live_repo_root,
                ["workflow", "offline", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            )
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    workflow_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
            workflow_deleted = True
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )


def test_etl_recovery_execute_task_and_rerun_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fail_project_name = live_name_factory("runtime-fail-project")
    fail_workflow_name = live_name_factory("runtime-fail-workflow")
    fail_workflow_spec = write_single_shell_workflow_spec(
        tmp_path / f"{fail_workflow_name}.yaml",
        project_name=fail_project_name,
        workflow_name=fail_workflow_name,
        task_name="fail-task",
        command='echo "fail" && exit 1',
        description="live failure control surfaces",
    )

    success_project_name = live_name_factory("runtime-success-project")
    success_workflow_name = live_name_factory("runtime-success-workflow")
    success_workflow_spec = write_shell_workflow_spec(
        tmp_path / f"{success_workflow_name}.yaml",
        project_name=success_project_name,
        workflow_name=success_workflow_name,
        extract_marker=f"{success_workflow_name}-extract",
        load_marker=f"{success_workflow_name}-load",
    )

    fail_project_created = False
    fail_workflow_created = False
    fail_workflow_deleted = False
    success_project_created = False
    success_workflow_created = False
    success_workflow_deleted = False

    try:
        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    fail_project_name,
                    "--description",
                    "live failure control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="failure project create",
        )
        fail_project_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(fail_workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="failure workflow create",
        )
        fail_workflow_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    fail_workflow_name,
                    "--project",
                    fail_project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="failure workflow online",
        )

        fail_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "run", fail_workflow_name, "--project", fail_project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="failure workflow run",
        )
        fail_run_data = require_mapping(
            fail_run_payload["data"],
            label="failure workflow run data",
        )
        fail_workflow_instance_id = require_int_value(
            require_list(
                fail_run_data["workflowInstanceIds"],
                label="failure workflow instance ids",
            )[0],
            label="failure workflow instance id",
        )

        fail_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(fail_workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "60",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=70.0,
            ),
            expected_action="workflow-instance.watch",
            label="failure workflow watch",
        )
        fail_watch_data = require_mapping(
            fail_watch_payload["data"],
            label="failure workflow watch data",
        )
        assert fail_watch_data["state"] == "FAILURE"

        fail_task_rows = _wait_for_task_rows(
            live_repo_root,
            live_etl_env_file,
            workflow_instance_id=fail_workflow_instance_id,
        )
        fail_task_instance_id = require_int_value(
            fail_task_rows[0].get("id"),
            label="failure task instance id",
        )

        force_success_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "force-success",
                    str(fail_task_instance_id),
                    "--workflow-instance",
                    str(fail_workflow_instance_id),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.force-success",
            label="task-instance force-success",
        )
        force_success_data = require_mapping(
            force_success_payload["data"],
            label="task-instance force-success data",
        )
        assert force_success_data["state"] == "FORCED_SUCCESS"

        recover_failed_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "recover-failed",
                    str(fail_workflow_instance_id),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.recover-failed",
            label="workflow-instance recover-failed",
        )
        recover_failed_data = require_mapping(
            recover_failed_payload["data"],
            label="workflow-instance recover-failed data",
        )
        assert recover_failed_data["state"] == "SUBMITTED_SUCCESS"

        recovered_data = _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            workflow_instance_id=fail_workflow_instance_id,
            target_state="SUCCESS",
        )
        assert recovered_data["commandType"] == "START_FAILURE_TASK_PROCESS"

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    success_project_name,
                    "--description",
                    "live execute-task rerun project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="success project create",
        )
        success_project_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(success_workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="success workflow create",
        )
        success_workflow_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    success_workflow_name,
                    "--project",
                    success_project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="success workflow online",
        )

        success_run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "run",
                    success_workflow_name,
                    "--project",
                    success_project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="success workflow run",
        )
        success_run_data = require_mapping(
            success_run_payload["data"],
            label="success workflow run data",
        )
        success_workflow_instance_id = require_int_value(
            require_list(
                success_run_data["workflowInstanceIds"],
                label="success workflow instance ids",
            )[0],
            label="success workflow instance id",
        )

        success_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(success_workflow_instance_id),
                    "--interval-seconds",
                    "2",
                    "--timeout-seconds",
                    "60",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=70.0,
            ),
            expected_action="workflow-instance.watch",
            label="success workflow watch",
        )
        success_watch_data = require_mapping(
            success_watch_payload["data"],
            label="success workflow watch data",
        )
        assert success_watch_data["state"] == "SUCCESS"

        execute_task_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "execute-task",
                    str(success_workflow_instance_id),
                    "--task",
                    "load",
                    "--scope",
                    "self",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.execute-task",
            label="workflow-instance execute-task",
        )
        execute_task_resolved = require_mapping(
            execute_task_payload["resolved"],
            label="workflow-instance execute-task resolved",
        )
        execute_task_task = require_mapping(
            execute_task_resolved["task"],
            label="workflow-instance execute-task task",
        )
        assert execute_task_resolved["scope"] == "self"
        assert execute_task_task["name"] == "load"
        assert execute_task_payload["warnings"] == [
            "execute-task requested; current workflow instance state is SUCCESS"
        ]

        failure_after_execute = _wait_for_workflow_state(
            live_repo_root,
            live_etl_env_file,
            workflow_instance_id=success_workflow_instance_id,
            target_state="FAILURE",
        )
        assert failure_after_execute["commandType"] == "START_PROCESS"

        rerun_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow-instance", "rerun", str(success_workflow_instance_id)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.rerun",
            label="workflow-instance rerun",
        )
        assert rerun_payload["warnings"] == [
            "rerun requested; current workflow instance state is FAILURE"
        ]

        rerun_result = wait_for_result(
            live_repo_root,
            ["workflow-instance", "get", str(success_workflow_instance_id)],
            env_file=live_etl_env_file,
            timeout_seconds=30.0,
            interval_seconds=2.0,
            accept=lambda current: (
                _workflow_state(current) == "SUCCESS"
                and (_workflow_run_times(current) or 0) >= 2
            ),
        )
        rerun_get_payload = require_ok_payload(
            rerun_result,
            expected_action="workflow-instance.get",
            label="workflow-instance get after rerun",
        )
        rerun_get_data = require_mapping(
            rerun_get_payload["data"],
            label="workflow-instance get after rerun data",
        )
        assert rerun_get_data["commandType"] == "REPEAT_RUNNING"
        assert (
            require_int_value(
                rerun_get_data.get("runTimes"),
                label="workflow-instance runTimes after rerun",
            )
            >= 2
        )
    finally:
        if fail_workflow_created and not fail_workflow_deleted:
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "offline",
                    fail_workflow_name,
                    "--project",
                    fail_project_name,
                ],
                env_file=live_etl_env_file,
            )
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    fail_workflow_name,
                    "--project",
                    fail_project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
            fail_workflow_deleted = True
        if fail_project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=fail_project_name,
            )
        if success_workflow_created and not success_workflow_deleted:
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "offline",
                    success_workflow_name,
                    "--project",
                    success_project_name,
                ],
                env_file=live_etl_env_file,
            )
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    success_workflow_name,
                    "--project",
                    success_project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
            success_workflow_deleted = True
        if success_project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=success_project_name,
            )


def test_etl_task_group_queue_control_surfaces_round_trip(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("task-group-queue-project")
    workflow_name = live_name_factory("task-group-queue-workflow")
    task_group_name = live_name_factory("task-group-queue-group")

    project_created = False
    workflow_created = False
    workflow_deleted = False
    task_group_created = False
    task_group_id: int | None = None

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
                    "live task-group queue control project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_created = True

        task_group_payload = require_ok_payload(
            run_dsctl(
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
            ),
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
        task_group_created = True

        workflow_spec = write_parallel_task_group_workflow_spec(
            tmp_path / f"{workflow_name}.yaml",
            project_name=project_name,
            workflow_name=workflow_name,
            task_group_id=task_group_id,
            first_command='echo "slot one" && sleep 15',
            second_command='echo "slot two" && sleep 15',
        )
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

        run_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "run", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.run",
            label="workflow run",
        )
        run_data = require_mapping(run_payload["data"], label="workflow run data")
        workflow_instance_id = require_int_value(
            require_list(
                run_data["workflowInstanceIds"],
                label="workflow instance ids",
            )[0],
            label="workflow instance id",
        )

        queue_rows = _wait_for_queue_rows(
            live_repo_root,
            live_etl_env_file,
            task_group=task_group_name,
        )
        queue_id = require_int_value(queue_rows[0].get("id"), label="queue id")
        assert (
            require_text_value(
                queue_rows[0].get("status"),
                label="queue status",
            )
            == "WAIT_QUEUE"
        )

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
        assert force_start_data["forceStarted"] is True

        workflow_watch_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow-instance",
                    "watch",
                    str(workflow_instance_id),
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
        assert workflow_watch_data["state"] == "SUCCESS"
    finally:
        if workflow_created and not workflow_deleted:
            run_dsctl(
                live_repo_root,
                ["workflow", "offline", workflow_name, "--project", project_name],
                env_file=live_etl_env_file,
            )
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "delete",
                    workflow_name,
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            )
            workflow_deleted = True
        if task_group_created:
            run_dsctl(
                live_repo_root,
                ["task-group", "close", task_group_name],
                env_file=live_etl_env_file,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )
