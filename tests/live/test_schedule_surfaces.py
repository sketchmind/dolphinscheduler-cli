from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live.support import (
    LiveBootstrapState,
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
    near_future_schedule_window,
    write_shell_workflow_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


pytestmark = [pytest.mark.live, pytest.mark.destructive]


def _first_worker_group_name(
    repo_root: Path,
    admin_env_file: Path,
) -> str:
    payload = require_ok_payload(
        run_dsctl(
            repo_root,
            ["worker-group", "list", "--page-size", "20"],
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
    first_row = require_mapping(rows[0], label="worker-group row")
    return require_text_value(first_row.get("name"), label="worker-group name")


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


@pytest.mark.live_admin
def test_etl_schedule_lifecycle_round_trips_and_triggers_runtime(
    live_repo_root: Path,
    live_admin_env_file: Path,
    live_etl_env_file: Path,
    live_bootstrap_state: LiveBootstrapState,
    live_name_factory: Callable[[str], str],
    tmp_path: Path,
) -> None:
    project_name = live_name_factory("schedule-project")
    workflow_name = live_name_factory("schedule-workflow")
    workflow_spec = write_shell_workflow_spec(
        tmp_path / f"{workflow_name}.yaml",
        project_name=project_name,
        workflow_name=workflow_name,
        extract_marker=f"{workflow_name}-extract-marker",
        load_marker=f"{workflow_name}-load-marker",
    )
    schedule_cron = "0 * * * * ?"
    schedule_start, schedule_end = near_future_schedule_window()
    tenant_code = live_bootstrap_state.tenant_code
    if tenant_code is None:
        pytest.skip("Schedule live test requires one ETL tenant code.")
    environment_name = live_name_factory("schedule-env")
    worker_group = _first_worker_group_name(live_repo_root, live_admin_env_file)

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
                    "env",
                    "create",
                    "--name",
                    environment_name,
                    "--config",
                    '{"JAVA_HOME":"/usr/lib/jvm/java-17-openjdk"}',
                    "--description",
                    "schedule live test environment",
                    "--worker-group",
                    worker_group,
                ],
                env_file=live_admin_env_file,
            ),
            expected_action="env.create",
            label="schedule environment create",
        )
        environment_create_data = require_mapping(
            environment_create_payload["data"],
            label="schedule environment create data",
        )
        environment_code = require_int_value(
            environment_create_data.get("code"),
            label="schedule environment code",
        )
        environment_created = True

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
        project_create_data = require_mapping(
            project_create_payload["data"],
            label="project create data",
        )
        require_int_value(project_create_data.get("code"), label="project code")
        project_created = True

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
        workflow_created = True

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

        confirmation_error = require_error_payload(
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
                    schedule_cron,
                    "--start",
                    schedule_start,
                    "--end",
                    schedule_end,
                    "--timezone",
                    SHANGHAI_TIMEZONE,
                    "--tenant-code",
                    tenant_code,
                    "--environment-code",
                    str(environment_code),
                ],
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
                [
                    "schedule",
                    "explain",
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
                    "--tenant-code",
                    tenant_code,
                    "--environment-code",
                    str(environment_code),
                ],
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
                [
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
                    "--tenant-code",
                    tenant_code,
                    "--environment-code",
                    str(environment_code),
                    "--confirm-risk",
                    create_confirm_token,
                ],
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

        schedule_get_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "get", str(schedule_id)],
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
                ["schedule", "preview", str(schedule_id)],
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

        schedule_online_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "online", str(schedule_id)],
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
            [
                "workflow-instance",
                "list",
                "--project",
                project_name,
                "--workflow",
                workflow_name,
                "--page-size",
                "20",
            ],
            env_file=live_etl_env_file,
            timeout_seconds=180.0,
            interval_seconds=5.0,
            accept=lambda current: bool(_workflow_instance_rows(current.payload)),
        )
        workflow_instance_list_payload = require_ok_payload(
            workflow_instance_list_result,
            expected_action="workflow-instance.list",
            label="workflow-instance list from schedule",
        )
        workflow_instance_list_data = require_mapping(
            workflow_instance_list_payload["data"],
            label="workflow-instance list from schedule data",
        )
        workflow_instance_rows = require_list(
            workflow_instance_list_data["totalList"],
            label="workflow-instance list from schedule rows",
        )
        workflow_instance_row = require_mapping(
            workflow_instance_rows[0],
            label="workflow-instance row from schedule",
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

        schedule_offline_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "offline", str(schedule_id)],
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
                ["schedule", "get", str(schedule_id)],
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

        schedule_delete_payload = require_ok_payload(
            run_dsctl(
                live_repo_root,
                ["schedule", "delete", str(schedule_id), "--force"],
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
        if schedule_id is not None and not schedule_deleted:
            run_dsctl(
                live_repo_root,
                ["schedule", "offline", str(schedule_id)],
                env_file=live_etl_env_file,
            )
            run_dsctl(
                live_repo_root,
                ["schedule", "delete", str(schedule_id), "--force"],
                env_file=live_etl_env_file,
            )
        if workflow_created and not workflow_deleted:
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
        if environment_created and environment_code is not None:
            run_dsctl(
                live_repo_root,
                ["env", "delete", str(environment_code), "--force"],
                env_file=live_admin_env_file,
            )
        if project_created:
            delete_project_eventually(
                live_repo_root,
                live_etl_env_file,
                project=project_name,
            )
