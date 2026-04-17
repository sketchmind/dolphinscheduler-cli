import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

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
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_instance_adapter=task_instance_adapter,
        ),
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


def test_task_instance_list_command_requires_project_without_workflow_instance() -> (
    None
):
    result = runner.invoke(app, ["task-instance", "list"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass --project NAME or run `dsctl use project NAME`."
    )


def test_task_instance_get_command_returns_one_instance() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "get", "3001", "--workflow-instance", "901"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.get"
    assert payload["data"]["workflowInstanceId"] == 901


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
    assert payload["resolved"] == {
        "workflowInstance": {"id": 902},
        "taskInstance": {"id": 3002},
    }


def test_task_instance_sub_workflow_command_returns_child_relation() -> None:
    result = runner.invoke(
        app,
        ["task-instance", "sub-workflow", "3003", "--workflow-instance", "902"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.sub-workflow"
    assert payload["data"] == {"subWorkflowInstanceId": 903}
    assert payload["resolved"] == {
        "workflowInstance": {"id": 902},
        "taskInstance": {"id": 3003},
    }


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
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_instance_adapter=task_instance_adapter,
        ),
    )

    result = runner.invoke(
        app,
        ["task-instance", "sub-workflow", "3001", "--workflow-instance", "901"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.sub-workflow"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl task-instance get 3001 --workflow-instance 901` to inspect "
        "the task type. Only SUB_WORKFLOW task instances have a child workflow "
        "instance."
    )


def test_task_instance_log_command_returns_tail_lines() -> None:
    result = runner.invoke(app, ["task-instance", "log", "3001", "--tail", "2"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.log"
    assert payload["data"]["lineCount"] == 2
    assert payload["data"]["text"] == "line-2\nline-3"


def test_task_instance_force_success_command_returns_forced_success_payload() -> None:
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
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.force-success"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow-instance get 901` to inspect the owning workflow "
        "instance. Wait for it to reach a final state, then retry "
        "`task-instance force-success`."
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
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl enum list task_execution_status` to inspect the supported DS "
        "task-instance states."
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
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.stop"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl task-instance get 3002 --workflow-instance 902` to inspect "
        "the current task state. `task-instance stop` only applies while the "
        "task instance is still running."
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
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_instance_adapter=task_instance_adapter,
        ),
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
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-instance.savepoint"
    assert payload["error"]["type"] == "api_result_error"
    assert payload["error"]["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 10196,
        "result_message": "task savepoint error",
    }
