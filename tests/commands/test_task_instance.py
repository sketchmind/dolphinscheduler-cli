import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services.selection import ResourceDefaults
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
)
from tests.runtime_instance_domain_fakes import install_runtime_instance_domain_runtime
from tests.support import make_profile, normalize_cli_help

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_task_instance_service(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                name="daily-sync-901",
            ),
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=102,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                name="daily-sync-902",
            ),
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=201,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                name="child-workflow-903",
            ),
        ],
        sub_workflow_instance_ids_by_task_id={3003: 903},
    )
    task_instance_adapter = FakeTaskInstanceAdapter(
        task_instances=[
            FakeTaskInstance(
                id=3001,
                name="extract",
                task_type_value="SHELL",
                workflow_instance_id_value=901,
                workflow_instance_name_value="daily-sync-901",
                project_code_value=7,
                task_code_value=201,
                task_definition_version_value=1,
                process_definition_name_value="daily-sync",
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                start_time_value="2026-04-11 10:00:00",
                host="worker-1",
                executor_name_value="alice",
                task_execute_type_value=FakeEnumValue("BATCH"),
            ),
            FakeTaskInstance(
                id=3002,
                name="repair-load",
                task_type_value="SHELL",
                workflow_instance_id_value=902,
                workflow_instance_name_value="daily-sync-902",
                project_code_value=7,
                task_code_value=202,
                task_definition_version_value=1,
                process_definition_name_value="daily-sync",
                state_value=FakeEnumValue("FAILURE"),
                start_time_value="2026-04-11 10:05:00",
                host="worker-1",
                executor_name_value="bob",
                task_execute_type_value=FakeEnumValue("BATCH"),
            ),
            FakeTaskInstance(
                id=3003,
                name="run-child",
                task_type_value="SUB_WORKFLOW",
                workflow_instance_id_value=902,
                workflow_instance_name_value="daily-sync-902",
                project_code_value=7,
                task_code_value=203,
                task_definition_version_value=1,
                process_definition_name_value="daily-sync",
                state_value=FakeEnumValue("SUCCESS"),
                start_time_value="2026-04-11 10:10:00",
                executor_name_value="alice",
                task_execute_type_value=FakeEnumValue("BATCH"),
            ),
        ],
        log_messages_by_task_instance_id={3001: ["line-1", "line-2", "line-3"]},
    )
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        workflow_instance_adapter=workflow_instance_adapter,
        task_instance_adapter=task_instance_adapter,
    )


def test_task_instance_list_command_returns_page_payload() -> None:
    result = runner.invoke(app, ["task-instance", "list", "--workflow-instance", "901"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["id"] == 3001


def test_task_instance_list_command_supports_all_pages() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            "902",
            "--page-size",
            "1",
            "--all",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.list"
    assert payload["resolved"]["all"] is True
    assert payload["data"]["total"] == 2
    assert len(payload["data"]["totalList"]) == 2


def test_task_instance_list_command_supports_project_filters() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "list",
            "--project",
            "etl-prod",
            "--host",
            "worker-1",
            "--executor",
            "bob",
            "--start",
            "2026-04-11 10:00:00",
            "--end",
            "2026-04-11 10:10:00",
            "--execute-type",
            "BATCH",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.list"
    assert payload["resolved"]["project"]["code"] == 7
    assert payload["resolved"]["host"] == "worker-1"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["id"] == 3002


def test_task_instance_list_command_requires_project_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="etl-prod")]
        ),
        context=ResourceDefaults(),
    )
    result = runner.invoke(app, ["task-instance", "list"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass --project NAME, or configure a project in the selected context."
    )


def test_task_instance_list_command_rejects_workflow_definition_filter() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "list",
            "--project",
            "etl-prod",
            "--workflow",
            "daily-sync",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["details"]["upstream_filter"] == "workflowDefinitionName"
    assert "workflow-instance list" in payload["error"]["suggestion"]


def test_task_instance_list_help_points_to_filter_discovery() -> None:
    result = runner.invoke(app, ["task-instance", "list", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "workflow-instance" in help_text
    assert "project" in help_text
    assert "task-execution-status" in help_text
    assert "BATCH" in help_text
    assert "STREAM" in help_text
    assert "task-execute-type" in help_text
    assert "list" in help_text


def test_task_instance_get_command_returns_one_instance() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "get",
            "3001",
            "--project",
            "etl-prod",
            "--workflow-instance",
            "901",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.get"
    assert payload["data"]["workflowInstanceId"] == 901
    assert payload["resolved"]["project"]["source"] == "flag"


def test_task_instance_get_command_accepts_project_only_scope() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "get", "3001", "--project", "etl-prod"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["id"] == 3001
    assert payload["resolved"]["taskInstance"] == {"id": 3001}
    assert "workflowInstance" not in payload["resolved"]


def test_task_instance_get_command_emits_stable_not_found_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                name="daily-sync-901",
            )
        ]
    )
    task_instance_adapter = FakeTaskInstanceAdapter(task_instances=[])

    def missing_get(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id

    monkeypatch.setattr(task_instance_adapter, "get", missing_get)
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        workflow_instance_adapter=workflow_instance_adapter,
        task_instance_adapter=task_instance_adapter,
    )

    result = runner.invoke(
        app,
        ["task-instance", "get", "999999", "--workflow-instance", "901"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.get"
    assert payload["error"]["type"] == "not_found"
    assert payload["error"]["details"] == {
        "resource": "task-instance",
        "id": 999999,
        "workflow_instance_id": 901,
    }
    assert payload["error"]["suggestion"] == (
        "Run `dsctl task-instance list --project etl-prod --workflow-instance "
        "901` to inspect available task instance ids."
    )
    assert "retryable" not in payload["error"]["details"]


@pytest.mark.parametrize(
    ("result_code", "result_message", "error_type"),
    [
        (10008, "task instance not found", "not_found"),
        (
            10103,
            (
                "TaskInstanceLogPath is empty, maybe the taskInstance doesn't "
                "be dispatched"
            ),
            "task_not_dispatched",
        ),
    ],
)
def test_task_instance_log_command_emits_stable_error_envelope(
    monkeypatch: pytest.MonkeyPatch,
    result_code: int,
    result_message: str,
    error_type: str,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    task_instance_adapter = FakeTaskInstanceAdapter(task_instances=[])

    def missing_log(
        *,
        task_instance_id: int,
    ) -> None:
        del task_instance_id
        raise ApiResultError(
            result_code=result_code,
            result_message=result_message,
        )

    monkeypatch.setattr(task_instance_adapter, "task_log_lines", missing_log)
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        task_instance_adapter=task_instance_adapter,
    )

    result = runner.invoke(app, ["task-instance", "log", "999999"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.log"
    assert payload["error"]["type"] == error_type
    assert payload["error"]["source"]["result_code"] == result_code
    assert payload["error"]["suggestion"] == (
        "Use `dsctl workflow-instance list` in the relevant project to find the "
        "owning workflow instance, then inspect it with `dsctl task-instance "
        "list --workflow-instance`."
    )
    assert "retryable" not in payload["error"]["details"]


def test_task_instance_get_help_points_to_instance_discovery() -> None:
    result = runner.invoke(app, ["task-instance", "get", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "task-instance" in help_text
    assert "workflow-instance" in help_text
    assert "list" in help_text


def test_task_instance_watch_command_returns_finished_instance() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "watch", "3002", "--workflow-instance", "902"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.watch"
    assert payload["data"]["id"] == 3002
    assert payload["data"]["state"] == "FAILURE"
    assert payload["resolved"]["workflowInstance"] == {"id": 902}
    assert payload["resolved"]["taskInstance"] == {"id": 3002}
    assert payload["resolved"]["project"]["source"] == "context"


def test_task_instance_watch_exit_status_preserves_failed_instance() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "watch",
            "3002",
            "--workflow-instance",
            "902",
            "--exit-status",
        ],
    )
    assert result.exit_code == 1
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "execution_failed"
    assert payload["data"]["state"] == "FAILURE"
    assert payload["data"]["id"] == 3002


def test_task_instance_sub_workflow_command_returns_child_relation() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "sub-workflow", "3003", "--workflow-instance", "902"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.sub-workflow"
    assert payload["data"] == {"subWorkflowInstanceId": 903}
    assert payload["resolved"]["workflowInstance"] == {"id": 902}
    assert payload["resolved"]["taskInstance"] == {"id": 3003}
    assert payload["resolved"]["project"]["source"] == "context"


def test_task_instance_sub_workflow_command_reports_task_type_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                name="daily-sync-901",
            )
        ]
    )
    task_instance_adapter = FakeTaskInstanceAdapter(
        task_instances=[
            FakeTaskInstance(
                id=3001,
                name="extract",
                task_type_value="SHELL",
                workflow_instance_id_value=901,
                workflow_instance_name_value="daily-sync-901",
                project_code_value=7,
                task_code_value=201,
                task_definition_version_value=1,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
            )
        ]
    )

    def not_sub_workflow(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise ApiResultError(
            result_code=10021,
            result_message="task instance is not sub workflow instance",
        )

    monkeypatch.setattr(
        workflow_instance_adapter,
        "sub_workflow_instance_by_task",
        not_sub_workflow,
    )
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        workflow_instance_adapter=workflow_instance_adapter,
        task_instance_adapter=task_instance_adapter,
    )

    result = runner.invoke(
        app,
        ["task-instance", "sub-workflow", "3001", "--workflow-instance", "901"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.sub-workflow"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl task-instance get 3001 --project etl-prod "
        "--workflow-instance 901` to inspect the task type. Only SUB_WORKFLOW "
        "task instances have a child workflow instance."
    )


def test_task_instance_log_command_returns_tail_lines() -> None:
    result = runner.invoke(app, ["task-instance", "log", "3001", "--tail", "2"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.log"
    assert payload["data"]["lineCount"] == 2
    assert payload["data"]["text"] == "line-2\nline-3"


def test_task_instance_log_command_can_emit_raw_text() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "log", "3001", "--tail", "2", "--raw"],
    )

    assert result.exit_code == 0
    assert result.stdout == "line-2\nline-3"
    assert '"ok": true' not in result.stdout


def test_task_instance_force_success_command_returns_forced_result_payload() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "force-success",
            "3002",
            "--workflow-instance",
            "902",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.force-success"
    assert payload["data"]["id"] == 3002
    assert payload["data"]["state"] == "FORCED_SUCCESS"


def test_task_instance_force_success_command_reports_workflow_state_suggestion() -> (
    None
):
    result = runner.invoke(
        app,
        [
            "task-instance",
            "force-success",
            "3001",
            "--workflow-instance",
            "901",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.force-success"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow-instance get 901 --project etl-prod` to inspect the "
        "owning workflow instance. Wait for it to reach a final state, then "
        "retry `task-instance force-success`."
    )


def test_task_instance_savepoint_command_returns_requested_wrapper() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "savepoint",
            "3001",
            "--workflow-instance",
            "901",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.savepoint"
    assert payload["data"]["requested"] is True
    assert payload["data"]["taskInstance"]["id"] == 3001


def test_task_instance_stop_command_returns_requested_wrapper() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "stop",
            "3001",
            "--workflow-instance",
            "901",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.stop"
    assert payload["data"]["requested"] is True
    assert payload["data"]["taskInstance"]["id"] == 3001


def test_task_instance_list_command_reports_supported_state_names() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            "901",
            "--state",
            "not-a-real-state",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl enum list task-execution-status` to inspect the supported DS "
        "task-instance states."
    )


def test_task_instance_list_command_reports_supported_execute_types() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "list",
            "--workflow-instance",
            "901",
            "--execute-type",
            "not-real",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl enum list task-execute-type` to inspect the supported DS "
        "task execute-type names."
    )


def test_task_instance_stop_command_reports_running_state_suggestion() -> None:
    result = runner.invoke(
        app,
        [
            "task-instance",
            "stop",
            "3002",
            "--workflow-instance",
            "902",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.stop"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl task-instance get 3002 --project etl-prod "
        "--workflow-instance 902` to inspect the current task state. "
        "`task-instance stop` only applies while the task instance is still "
        "running."
    )


def test_task_instance_savepoint_command_preserves_raw_remote_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                name="daily-sync-901",
            )
        ]
    )
    task_instance_adapter = FakeTaskInstanceAdapter(
        task_instances=[
            FakeTaskInstance(
                id=3001,
                name="extract",
                task_type_value="SHELL",
                workflow_instance_id_value=901,
                workflow_instance_name_value="daily-sync-901",
                project_code_value=7,
                task_code_value=201,
                task_definition_version_value=1,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
            )
        ]
    )

    def broken_savepoint(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise ApiResultError(
            result_code=10196,
            result_message="task savepoint error",
        )

    monkeypatch.setattr(task_instance_adapter, "savepoint", broken_savepoint)
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        workflow_instance_adapter=workflow_instance_adapter,
        task_instance_adapter=task_instance_adapter,
    )

    result = runner.invoke(
        app,
        [
            "task-instance",
            "savepoint",
            "3001",
            "--workflow-instance",
            "901",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "task-instance.savepoint"
    assert payload["error"]["type"] == "api_result_error"
    assert payload["error"]["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10196,
        "result_message": "task savepoint error",
    }


def test_task_instance_log_window_exposes_source_coordinates_and_continuation() -> None:
    result = runner.invoke(
        app, ["task-instance", "log", "3001", "--start-line", "2", "--limit", "1"]
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)["data"]
    assert data["text"] == "line-2"
    assert data["window"]["start_line"] == 2
    assert data["window"]["end_line"] == 2
    assert data["window"]["next_start_line"] == 3
    assert data["window"]["lines_scanned"] == 2


def test_task_instance_log_window_preserves_raw_and_rejects_explicit_tail() -> None:
    raw = runner.invoke(
        app,
        ["task-instance", "log", "3001", "--start-line", "2", "--limit", "1", "--raw"],
    )
    assert raw.exit_code == 0
    assert raw.stdout == "line-2"
    invalid = runner.invoke(
        app, ["task-instance", "log", "3001", "--tail", "200", "--limit", "1"]
    )
    assert invalid.exit_code != 0
    payload = json.loads(invalid.stderr or invalid.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert "cannot be combined" in payload["error"]["message"]
