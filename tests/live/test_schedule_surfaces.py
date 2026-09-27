from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pytest

from tests.live.runtime_journey import emitted_marker
from tests.live.support import (
    require_error_payload,
    require_int_value,
    require_list,
    require_mapping,
    require_ok_payload,
    require_text_value,
    run_dsctl,
    wait_for_result,
)
from tests.live.workflow_support import (
    SHANGHAI_TIMEZONE,
    delete_project_eventually,
    delete_workflow_eventually,
    near_future_schedule_window,
    refresh_schedule_window,
    write_shell_workflow_spec,
    write_single_shell_workflow_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]


def _default_worker_group_name(
    repo_root: Path,
    admin_env_file: Path,
) -> str:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["worker-group", "list", "--all"],
            env_file=admin_env_file,
        ),
        expected_action="worker-group.list",
        label="worker-group list for schedule env",
    )
    data = require_mapping(payload["data"], label="worker-group list data")
    rows = require_list(data["totalList"], label="worker-group list rows")
    if not rows:
        message = "Live cluster did not return any worker groups"
        raise AssertionError(message)
    names = {
        require_text_value(
            require_mapping(row, label="worker-group row").get("name"),
            label="worker-group name",
        )
        for row in rows
    }
    if "default" not in names:
        pytest.skip("Schedule environment test requires the default worker group.")
    return "default"


def _schedule_create_option_flags(repo_root: Path, env_file: Path) -> set[str]:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["schema", "--command", "schedule.create"],
            env_file=env_file,
        ),
        expected_action="schema",
        label="schedule.create schema",
    )
    data = require_mapping(payload["data"], label="schedule.create schema data")
    command = require_mapping(data["command"], label="schedule.create command")
    options = require_list(command["options"], label="schedule.create options")
    return {
        require_text_value(
            require_mapping(option, label="schedule option").get("flag"),
            label="schedule option flag",
        )
        for option in options
    }


def _schedule_environment_inheritance(repo_root: Path, env_file: Path) -> bool:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["capabilities", "--section", "schedule"],
            env_file=env_file,
        ),
        expected_action="capabilities",
        label="schedule capabilities",
    )
    data = require_mapping(payload["data"], label="schedule capabilities data")
    schedule = require_mapping(data["schedule"], label="schedule capabilities section")
    inherited = schedule.get("environment_inheritance")
    assert isinstance(inherited, bool), (
        "schedule inheritance capability must be boolean"
    )
    return inherited


def _workflow_instance_rows(
    result_payload: object,
) -> list[object] | None:
    if not isinstance(result_payload, dict):
        return None
    if result_payload.get("ok") is not True:
        return None
    if result_payload.get("action") != "workflow-instance.list":
        return None
    data = result_payload.get("data")
    if not isinstance(data, dict):
        return None
    rows = data.get("totalList")
    if not isinstance(rows, list):
        return None
    return rows


def _scheduled_workflow_instance_rows(
    result_payload: object,
    *,
    baseline_ids: set[int],
) -> list[dict[str, object]]:
    rows = _workflow_instance_rows(result_payload) or []
    selected: list[dict[str, object]] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        row_id = raw_row.get("id")
        schedule_time = raw_row.get("scheduleTime")
        if (
            isinstance(row_id, int)
            and not isinstance(row_id, bool)
            and row_id > 0
            and row_id not in baseline_ids
            and raw_row.get("commandType") == "SCHEDULER"
            and isinstance(schedule_time, str)
            and bool(schedule_time)
        ):
            selected.append(raw_row)
    return selected


def _task_log_has_emitted_marker(result_payload: object, *, marker: str) -> bool:
    if not isinstance(result_payload, dict) or result_payload.get("ok") is not True:
        return False
    if result_payload.get("action") != "task-instance.log":
        return False
    data = result_payload.get("data")
    return (
        isinstance(data, dict)
        and isinstance(data.get("text"), str)
        and emitted_marker(data["text"], marker)
    )


def _cleanup_owned_schedule(
    repo_root: Path,
    env_file: Path,
    *,
    project: str,
    workflow: str,
    known_id: int | None,
) -> None:
    """Only remove schedules attached to this test's uniquely named workflow."""
    ids = {known_id} if known_id is not None else set()
    list_payload = require_ok_payload(
        run_dsctl(
            repo_root,
            [
                "schedule",
                "list",
                "--project",
                project,
                "--workflow",
                workflow,
                "--all",
            ],
            env_file=env_file,
        ),
        expected_action="schedule.list",
        label="owned schedule cleanup list",
    )
    list_data = require_mapping(list_payload["data"], label="owned schedule list data")
    for raw_row in require_list(
        list_data["totalList"], label="owned schedule list rows"
    ):
        value = require_mapping(raw_row, label="owned schedule row").get("id")
        ids.add(require_int_value(value, label="owned schedule id"))
    for schedule_id in ids:
        scope = ["--project", project]
        get_result = run_dsctl(
            repo_root,
            ["schedule", "get", str(schedule_id), *scope],
            env_file=env_file,
        )
        error = get_result.payload.get("error")
        if (
            get_result.payload.get("ok") is False
            and isinstance(error, dict)
            and error.get("type") == "not_found"
        ):
            continue
        get_payload = require_ok_payload(
            get_result,
            expected_action="schedule.get",
            label="owned schedule cleanup get",
        )
        state = require_mapping(
            get_payload["data"], label="owned schedule cleanup get data"
        ).get("releaseState")
        if state == "ONLINE":
            require_ok_payload(
                run_dsctl(
                    repo_root,
                    ["schedule", "offline", str(schedule_id), *scope],
                    env_file=env_file,
                ),
                expected_action="schedule.offline",
                label="owned schedule cleanup offline",
            )
        delete_payload = require_ok_payload(
            run_dsctl(
                repo_root,
                ["schedule", "delete", str(schedule_id), *scope, "--force"],
                env_file=env_file,
            ),
            expected_action="schedule.delete",
            label="owned schedule cleanup delete",
        )
        assert (
            require_mapping(
                delete_payload["data"], label="owned schedule cleanup delete data"
            )["deleted"]
            is True
        )


def _report_cleanup_errors(errors: list[Exception]) -> None:
    if not errors:
        return
    original = sys.exception()
    if original is not None:
        for error in errors:
            original.add_note(f"Cleanup also failed: {type(error).__name__}: {error}")
        return
    message = "schedule test cleanup failed"
    raise ExceptionGroup(message, errors)


def _cleanup_schedule_fixture(
    repo_root: Path,
    etl_env_file: Path,
    *,
    project: str,
    workflow: str,
    schedule_id: int | None,
    project_created: bool,
    workflow_created: bool,
    workflow_deleted: bool,
    schedule_deleted: bool,
    environment: tuple[Path, str] | None = None,
) -> None:
    errors: list[Exception] = []
    if workflow_created and not schedule_deleted:
        try:
            _cleanup_owned_schedule(
                repo_root,
                etl_env_file,
                project=project,
                workflow=workflow,
                known_id=schedule_id,
            )
        except Exception as error:
            errors.append(error)
    workflow_cleanup_ok = not workflow_created or workflow_deleted
    if workflow_created and not workflow_deleted:
        try:
            delete_workflow_eventually(
                repo_root, etl_env_file, project=project, workflow=workflow
            )
            workflow_cleanup_ok = True
        except Exception as error:
            errors.append(error)
    if project_created and workflow_cleanup_ok:
        try:
            delete_project_eventually(repo_root, etl_env_file, project=project)
        except Exception as error:
            errors.append(error)
    # Older DS versions retain task definitions until project cleanup. Remove
    # their environment only after those references have been released.
    if environment is not None:
        admin_env_file, environment_selector = environment
        try:
            require_ok_payload(
                run_dsctl(
                    repo_root,
                    ["environment", "delete", environment_selector, "--force"],
                    env_file=admin_env_file,
                ),
                expected_action="environment.delete",
                label="schedule environment cleanup",
            )
        except Exception as error:
            errors.append(error)
    _report_cleanup_errors(errors)


@pytest.mark.live_admin
def test_etl_schedule_lifecycle_round_trips_and_triggers_runtime(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    options = _schedule_create_option_flags(live_repo_root, live_etl_env_file)
    if "--environment-code" not in options:
        pytest.skip("Exact profile has no schedule environments (DS 1.3.9).")
    schedule_inherits_environment = _schedule_environment_inheritance(
        live_repo_root, live_etl_env_file
    )
    project_name = live_name_factory("schedule-project")
    workflow_name = live_name_factory("schedule-workflow")
    environment_marker = live_name_factory("schedule-environment-marker")
    workflow_spec = write_single_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        task_name="check-schedule-environment",
        command='printf "%s\\n" "${DSCTL_SCHEDULE_MARKER:-missing}"',
        description="prove scheduled environment reaches shell task",
    )
    schedule_cron = "0 * * * * ?"
    # Match the runtime journey: the bootstrap tenant owns API resources but
    # need not have a runnable worker OS account.
    tenant_option = ["--tenant-code", "default"] if "--tenant-code" in options else []
    environment_name = live_name_factory("schedule-env")
    worker_group = _default_worker_group_name(live_repo_root, live_admin_env_file)

    environment_created = False
    project_created = False
    workflow_created = False
    workflow_deleted = False
    schedule_deleted = False
    environment_code: int | None = None
    schedule_id: int | None = None

    try:
        environment_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "environment",
                    "create",
                    "--name",
                    environment_name,
                    "--config",
                    f"export DSCTL_SCHEDULE_MARKER='{environment_marker}'",
                    "--description",
                    "schedule live test environment",
                    "--worker-group",
                    worker_group,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="environment.create",
            label="schedule environment create",
        )
        environment_created = True
        environment_create_data = require_mapping(
            environment_create_payload["data"],
            label="schedule environment create data",
        )
        environment_code = require_int_value(
            environment_create_data.get("code"),
            label="schedule environment code",
        )
        assert environment_code > 0
        if not schedule_inherits_environment:
            workflow_yaml = workflow_spec.read_text(encoding="utf-8")
            task_worker_group = "    worker_group: default\n"
            assert workflow_yaml.count(task_worker_group) == 1
            workflow_spec.write_text(
                workflow_yaml.replace(
                    task_worker_group,
                    f"{task_worker_group}    environment_code: {environment_code}\n",
                    1,
                ),
                encoding="utf-8",
            )

        project_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "project",
                    "create",
                    "--name",
                    project_name,
                    "--description",
                    "schedule live test project",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="project create",
        )
        project_created = True
        project_create_data = require_mapping(
            project_create_payload["data"],
            label="project create data",
        )
        require_int_value(project_create_data.get("code"), label="project code")

        workflow_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "create",
                    "--file",
                    str(workflow_spec),
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="workflow create",
        )
        workflow_created = True
        workflow_create_data = require_mapping(
            workflow_create_payload["data"],
            label="workflow create data",
        )
        workflow_code = require_int_value(
            workflow_create_data.get("code"),
            label="workflow code",
        )
        assert workflow_create_data["name"] == workflow_name
        assert workflow_create_data["releaseState"] == "OFFLINE"

        workflow_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="workflow online before schedule create",
        )
        workflow_online_data = require_mapping(
            workflow_online_payload["data"],
            label="workflow online before schedule create data",
        )
        assert workflow_online_data["releaseState"] == "ONLINE"

        instance_list_argv = [
            "workflow-instance",
            "list",
            "--project",
            project_name,
            "--workflow",
            workflow_name,
            "--page-size",
            "100",
        ]
        baseline_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                instance_list_argv,
                env_file=live_etl_env_file,
            ),
            expected_action="workflow-instance.list",
            label="workflow-instance baseline before schedule",
        )
        baseline_ids = {
            require_int_value(
                require_mapping(row, label="baseline workflow-instance row").get("id"),
                label="baseline workflow-instance id",
            )
            for row in require_list(
                require_mapping(baseline_payload["data"], label="baseline data")[
                    "totalList"
                ],
                label="baseline workflow-instance rows",
            )
        }

        schedule_start, schedule_end = near_future_schedule_window(
            start_offset_minutes=10, end_offset_minutes=30
        )
        schedule_create_argv = [
            "schedule",
            "create",
            "--project",
            project_name,
            "--workflow",
            workflow_name,
            "--cron",
            schedule_cron,
            "--start",
            schedule_start,
            "--end",
            schedule_end,
            "--timezone",
            SHANGHAI_TIMEZONE,
            *tenant_option,
            "--environment-code",
            str(environment_code if schedule_inherits_environment else 0),
        ]
        schedule_explain_argv = ["schedule", "explain", *schedule_create_argv[2:]]
        if not schedule_inherits_environment:
            positive_create_argv = [*schedule_create_argv[:-1], str(environment_code)]
            require_error_payload(
                run_dsctl(
                    live_repo_root, positive_create_argv, env_file=live_etl_env_file
                ),
                expected_action="schedule.create",
                expected_type="unsupported_feature",
                label="unsupported positive schedule environment create",
            )
            require_error_payload(
                run_dsctl(
                    live_repo_root,
                    ["schedule", "explain", *positive_create_argv[2:]],
                    env_file=live_etl_env_file,
                ),
                expected_action="schedule.explain",
                expected_type="unsupported_feature",
                label="unsupported positive schedule environment explain",
            )
            rejected_list = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    [
                        "schedule",
                        "list",
                        "--project",
                        project_name,
                        "--workflow",
                        workflow_name,
                        "--page-size",
                        "20",
                    ],
                    env_file=live_etl_env_file,
                ),
                expected_action="schedule.list",
                label="schedule list after rejected create",
            )
            assert not require_list(
                require_mapping(
                    rejected_list["data"], label="rejected schedule list data"
                )["totalList"],
                label="rejected schedule list rows",
            )

        confirmation_error = require_error_payload(
            run_dsctl(
                live_repo_root,
                schedule_create_argv,
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.create",
            expected_type="confirmation_required",
            label="schedule create without confirmation",
        )
        confirmation_details = require_mapping(
            confirmation_error["details"],
            label="schedule create confirmation details",
        )
        assert confirmation_details["risk_type"] == "high_frequency_schedule"

        schedule_explain_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                schedule_explain_argv,
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.explain",
            label="schedule explain create",
        )
        schedule_explain_data = require_mapping(
            schedule_explain_payload["data"],
            label="schedule explain create data",
        )
        assert schedule_explain_data["mutationAction"] == "schedule.create"
        schedule_confirmation = require_mapping(
            schedule_explain_data["confirmation"],
            label="schedule explain create confirmation",
        )
        assert schedule_confirmation["required"] is True
        create_confirm_token = require_text_value(
            schedule_confirmation.get("token"),
            label="schedule create confirmation token",
        )

        schedule_create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [*schedule_create_argv, "--confirm-risk", create_confirm_token],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.create",
            label="schedule create",
        )
        schedule_create_data = require_mapping(
            schedule_create_payload["data"],
            label="schedule create data",
        )
        schedule_id = require_int_value(
            schedule_create_data.get("id"),
            label="schedule id",
        )
        assert schedule_create_data["crontab"] == schedule_cron
        assert schedule_create_data["releaseState"] == "OFFLINE"
        assert schedule_create_data["workflowDefinitionCode"] == workflow_code
        if schedule_inherits_environment:
            assert schedule_create_data["environmentCode"] == environment_code
        else:
            assert schedule_create_data["environmentCode"] in (None, -1, 0)

        schedule_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.get",
            label="schedule get",
        )
        schedule_get_data = require_mapping(
            schedule_get_payload["data"],
            label="schedule get data",
        )
        assert schedule_get_data["id"] == schedule_id
        assert schedule_get_data["workflowDefinitionCode"] == workflow_code
        assert schedule_get_data["timezoneId"] == SHANGHAI_TIMEZONE
        if schedule_inherits_environment:
            assert schedule_get_data["environmentCode"] == environment_code
        else:
            assert schedule_get_data["environmentCode"] in (None, -1, 0)

            for action in ("explain", "update"):
                require_error_payload(
                    run_dsctl(
                        live_repo_root,
                        [
                            "schedule",
                            action,
                            str(schedule_id),
                            "--project",
                            project_name,
                            "--environment-code",
                            str(environment_code),
                        ],
                        env_file=live_etl_env_file,
                    ),
                    expected_action=f"schedule.{action}",
                    expected_type="unsupported_feature",
                    label=f"unsupported positive schedule environment {action}",
                )
            rejected_update_get = require_ok_payload(
                run_dsctl(
                    live_repo_root,
                    ["schedule", "get", str(schedule_id), "--project", project_name],
                    env_file=live_etl_env_file,
                ),
                expected_action="schedule.get",
                label="schedule get after rejected positive update",
            )
            assert (
                require_mapping(
                    rejected_update_get["data"],
                    label="schedule get after rejected positive update data",
                )
                == schedule_get_data
            )

        schedule_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "list",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                    "--page-size",
                    "20",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.list",
            label="schedule list",
        )
        schedule_list_data = require_mapping(
            schedule_list_payload["data"],
            label="schedule list data",
        )
        schedule_rows = require_list(
            schedule_list_data["totalList"],
            label="schedule list rows",
        )
        assert any(
            require_mapping(item, label="schedule list row").get("id") == schedule_id
            for item in schedule_rows
        )

        schedule_preview_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "preview", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.preview",
            label="schedule preview",
        )
        schedule_preview_data = require_mapping(
            schedule_preview_payload["data"],
            label="schedule preview data",
        )
        preview_times = require_list(
            schedule_preview_data["times"],
            label="schedule preview times",
        )
        assert preview_times
        schedule_preview_analysis = require_mapping(
            schedule_preview_data["analysis"],
            label="schedule preview analysis",
        )
        assert schedule_preview_analysis["requires_confirmation"] is True

        update_explain_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "explain",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--warning-type",
                    "SUCCESS",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.explain",
            label="schedule explain update",
        )
        update_explain_data = require_mapping(
            update_explain_payload["data"],
            label="schedule explain update data",
        )
        assert update_explain_data["mutationAction"] == "schedule.update"
        update_confirmation = require_mapping(
            update_explain_data["confirmation"],
            label="schedule explain update confirmation",
        )
        assert update_confirmation["required"] is True
        update_confirm_token = require_text_value(
            update_confirmation.get("token"),
            label="schedule update confirmation token",
        )

        schedule_update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "update",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--warning-type",
                    "SUCCESS",
                    "--confirm-risk",
                    update_confirm_token,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.update",
            label="schedule update",
        )
        schedule_update_data = require_mapping(
            schedule_update_payload["data"],
            label="schedule update data",
        )
        assert schedule_update_data["id"] == schedule_id
        assert schedule_update_data["warningType"] == "SUCCESS"
        if schedule_inherits_environment:
            assert schedule_update_data["environmentCode"] == environment_code
        else:
            assert schedule_update_data["environmentCode"] in (None, -1, 0)

        activation_data = refresh_schedule_window(
            live_repo_root,
            live_etl_env_file,
            schedule_id=schedule_id,
            project=project_name,
        )
        assert activation_data["id"] == schedule_id
        assert activation_data["warningType"] == "SUCCESS"
        assert (
            activation_data["environmentCode"]
            == schedule_update_data["environmentCode"]
        )

        schedule_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "online", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.online",
            label="schedule online",
        )
        schedule_online_data = require_mapping(
            schedule_online_payload["data"],
            label="schedule online data",
        )
        assert schedule_online_data["releaseState"] == "ONLINE"

        workflow_instance_list_result = wait_for_result(
            live_repo_root,
            instance_list_argv,
            env_file=live_etl_env_file,
            timeout_seconds=240.0,
            interval_seconds=5.0,
            accept=lambda current: bool(
                _scheduled_workflow_instance_rows(
                    current.payload,
                    baseline_ids=baseline_ids,
                )
            ),
        )
        workflow_instance_list_payload = require_ok_payload(
            workflow_instance_list_result,
            expected_action="workflow-instance.list",
            label="workflow-instance list from schedule",
        )
        workflow_instance_rows = _scheduled_workflow_instance_rows(
            workflow_instance_list_payload,
            baseline_ids=baseline_ids,
        )
        assert workflow_instance_rows, "no new scheduler-originated instance found"
        workflow_instance_row = workflow_instance_rows[0]
        assert workflow_instance_row["commandType"] == "SCHEDULER"
        assert workflow_instance_row["workflowDefinitionCode"] == workflow_code
        assert require_text_value(
            workflow_instance_row.get("scheduleTime"),
            label="scheduled workflow-instance scheduleTime",
        )
        workflow_instance_id = require_int_value(
            workflow_instance_row.get("id"),
            label="scheduled workflow-instance id",
        )

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
                    "180",
                ],
                env_file=live_etl_env_file,
                timeout_seconds=200.0,
            ),
            expected_action="workflow-instance.watch",
            label="scheduled workflow-instance watch",
        )
        workflow_watch_data = require_mapping(
            workflow_watch_payload["data"],
            label="scheduled workflow-instance watch data",
        )
        assert workflow_watch_data["id"] == workflow_instance_id
        assert workflow_watch_data["state"] == "SUCCESS"

        task_list_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "task-instance",
                    "list",
                    "--project",
                    project_name,
                    "--workflow-instance",
                    str(workflow_instance_id),
                    "--all",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="task-instance.list",
            label="scheduled task-instance list",
        )
        task_rows = require_list(
            require_mapping(task_list_payload["data"], label="task list data")[
                "totalList"
            ],
            label="scheduled task-instance rows",
        )
        assert len(task_rows) == 1
        task_row = require_mapping(task_rows[0], label="scheduled task-instance row")
        assert task_row["state"] == "SUCCESS"
        task_id = require_int_value(task_row.get("id"), label="scheduled task id")
        task_log_result = wait_for_result(
            live_repo_root,
            ["task-instance", "log", str(task_id), "--tail", "100"],
            env_file=live_etl_env_file,
            timeout_seconds=45.0,
            interval_seconds=2.0,
            accept=lambda current: _task_log_has_emitted_marker(
                current.payload,
                marker=environment_marker,
            ),
        )
        task_log_payload = require_ok_payload(
            task_log_result,
            expected_action="task-instance.log",
            label="scheduled task log with environment marker",
        )
        assert _task_log_has_emitted_marker(
            task_log_payload,
            marker=environment_marker,
        )

        schedule_offline_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "offline", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.offline",
            label="schedule offline",
        )
        schedule_offline_data = require_mapping(
            schedule_offline_payload["data"],
            label="schedule offline data",
        )
        assert schedule_offline_data["releaseState"] == "OFFLINE"

        refreshed_schedule_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.get",
            label="schedule get after offline",
        )
        refreshed_schedule_data = require_mapping(
            refreshed_schedule_payload["data"],
            label="schedule get after offline data",
        )
        assert refreshed_schedule_data["releaseState"] == "OFFLINE"
        if schedule_inherits_environment:
            assert refreshed_schedule_data["environmentCode"] == environment_code
        else:
            assert refreshed_schedule_data["environmentCode"] in (None, -1, 0)

        clear_explain_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "explain",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--environment-code",
                    "0",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.explain",
            label="schedule explain environment clear",
        )
        clear_explain_data = require_mapping(
            clear_explain_payload["data"],
            label="schedule environment clear explain data",
        )
        clear_confirmation = require_mapping(
            clear_explain_data["confirmation"],
            label="schedule environment clear confirmation",
        )
        clear_argv = [
            "schedule",
            "update",
            str(schedule_id),
            "--project",
            project_name,
            "--environment-code",
            "0",
        ]
        if clear_confirmation["required"] is True:
            clear_argv.extend(
                [
                    "--confirm-risk",
                    require_text_value(
                        clear_confirmation.get("token"),
                        label="schedule environment clear confirmation token",
                    ),
                ]
            )
        cleared_payload = require_ok_payload(
            run_dsctl(live_repo_root, clear_argv, env_file=live_etl_env_file),
            expected_action="schedule.update",
            label="schedule environment clear",
        )
        cleared_data = require_mapping(
            cleared_payload["data"],
            label="schedule environment clear data",
        )
        assert cleared_data.get("environmentCode") in (None, -1, 0)
        cleared_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.get",
            label="schedule get after environment clear",
        )
        assert require_mapping(
            cleared_get_payload["data"],
            label="schedule get after environment clear data",
        ).get("environmentCode") in (None, -1, 0)

        schedule_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "delete",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.delete",
            label="schedule delete",
        )
        schedule_delete_data = require_mapping(
            schedule_delete_payload["data"],
            label="schedule delete data",
        )
        assert schedule_delete_data["deleted"] is True
        schedule_deleted = True

        workflow_offline_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "offline",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.offline",
            label="workflow offline after schedule",
        )
        workflow_offline_data = require_mapping(
            workflow_offline_payload["data"],
            label="workflow offline after schedule data",
        )
        assert workflow_offline_data["releaseState"] == "OFFLINE"

        workflow_delete_payload = require_ok_payload(
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
            ),
            expected_action="workflow.delete",
            label="workflow delete after schedule",
        )
        workflow_delete_data = require_mapping(
            workflow_delete_payload["data"],
            label="workflow delete after schedule data",
        )
        assert workflow_delete_data["deleted"] is True
        workflow_deleted = True
    finally:
        _cleanup_schedule_fixture(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            schedule_id=schedule_id,
            project_created=project_created,
            workflow_created=workflow_created,
            workflow_deleted=workflow_deleted,
            schedule_deleted=schedule_deleted,
            environment=(
                live_admin_env_file,
                str(environment_code or environment_name),
            )
            if environment_created
            else None,
        )


def test_etl_schedule_without_environment_round_trips(
    live_repo_root: Path,
    live_etl_env_file: Path,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    if "--environment-code" not in _schedule_create_option_flags(
        live_repo_root, live_etl_env_file
    ):
        pytest.skip("DS 1.3.9 no-environment scheduling is proven by schedule journey.")
    project_name = live_name_factory("schedule-no-env-project")
    workflow_name = live_name_factory("schedule-no-env-workflow")
    workflow_spec = write_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        extract_marker=f"{workflow_name}-extract-marker",
        load_marker=f"{workflow_name}-load-marker",
    )
    schedule_id: int | None = None
    project_created = False
    workflow_created = False
    workflow_deleted = False
    schedule_deleted = False

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
                    "schedule without environment live test",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="project.create",
            label="no-environment schedule project create",
        )
        project_created = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["workflow", "create", "--file", str(workflow_spec)],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.create",
            label="no-environment schedule workflow create",
        )
        workflow_created = True

        online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "online",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.online",
            label="no-environment schedule workflow online",
        )
        assert (
            require_mapping(
                online_payload["data"],
                label="no-environment workflow online data",
            )["releaseState"]
            == "ONLINE"
        )

        schedule_start, schedule_end = near_future_schedule_window(
            start_offset_minutes=10, end_offset_minutes=30
        )
        create_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "create",
                    "--project",
                    project_name,
                    "--workflow",
                    workflow_name,
                    "--cron",
                    "0 0/10 * * * ?",
                    "--start",
                    schedule_start,
                    "--end",
                    schedule_end,
                    "--timezone",
                    SHANGHAI_TIMEZONE,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.create",
            label="schedule create without environment",
        )
        create_data = require_mapping(
            create_payload["data"],
            label="schedule create without environment data",
        )
        schedule_id = require_int_value(
            create_data.get("id"),
            label="schedule without environment id",
        )
        assert create_data.get("environmentCode") in (None, -1, 0)
        created_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.get",
            label="schedule get without environment",
        )
        assert require_mapping(
            created_get_payload["data"],
            label="schedule get without environment data",
        ).get("environmentCode") in (None, -1, 0)

        update_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "update",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--warning-type",
                    "SUCCESS",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.update",
            label="schedule update without environment",
        )
        update_data = require_mapping(
            update_payload["data"],
            label="schedule update without environment data",
        )
        assert update_data["warningType"] == "SUCCESS"
        assert update_data.get("environmentCode") in (None, -1, 0)
        updated_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id), "--project", project_name],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.get",
            label="schedule get after no-environment update",
        )
        updated_get_data = require_mapping(
            updated_get_payload["data"],
            label="schedule get after no-environment update data",
        )
        assert updated_get_data["warningType"] == "SUCCESS"
        assert updated_get_data.get("environmentCode") in (None, -1, 0)

        delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "schedule",
                    "delete",
                    str(schedule_id),
                    "--project",
                    project_name,
                    "--force",
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="schedule.delete",
            label="schedule without environment delete",
        )
        assert (
            require_mapping(
                delete_payload["data"],
                label="schedule without environment delete data",
            )["deleted"]
            is True
        )
        schedule_deleted = True

        require_ok_payload(
            run_dsctl(
                live_repo_root,
                [
                    "workflow",
                    "offline",
                    workflow_name,
                    "--project",
                    project_name,
                ],
                env_file=live_etl_env_file,
            ),
            expected_action="workflow.offline",
            label="no-environment schedule workflow offline",
        )
        delete_workflow_payload = require_ok_payload(
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
            ),
            expected_action="workflow.delete",
            label="no-environment schedule workflow delete",
        )
        assert (
            require_mapping(
                delete_workflow_payload["data"],
                label="no-environment schedule workflow delete data",
            )["deleted"]
            is True
        )
        workflow_deleted = True
    finally:
        _cleanup_schedule_fixture(
            live_repo_root,
            live_etl_env_file,
            project=project_name,
            workflow=workflow_name,
            schedule_id=schedule_id,
            project_created=project_created,
            workflow_created=workflow_created,
            workflow_deleted=workflow_deleted,
            schedule_deleted=schedule_deleted,
        )
