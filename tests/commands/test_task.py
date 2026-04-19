import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_task_service(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                user_id_value=11,
            )
        ],
        dags={
            101: FakeDag(
                workflow_definition_value=FakeWorkflow(
                    code=101,
                    name="daily-sync",
                    project_code_value=7,
                    user_id_value=11,
                ),
                task_definition_list_value=[
                    FakeTaskDefinition(
                        code=201,
                        name="extract",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo extract"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                    FakeTaskDefinition(
                        code=202,
                        name="load",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo load"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                ],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
    )
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo extract"}',
                    flag_value=FakeEnumValue("YES"),
                ),
                FakeTaskDefinition(
                    code=202,
                    name="load",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo load"}',
                    flag_value=FakeEnumValue("YES"),
                ),
            ]
        }
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
        ),
    )


def test_task_list_command_returns_filtered_tasks() -> None:
    result = runner.invoke(app, ["task", "list", "--search", "extract"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task.list"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["data"] == [{"code": 201, "name": "extract", "version": 1}]


def test_task_list_help_points_to_project_and_workflow_discovery() -> None:
    result = runner.invoke(app, ["task", "list", "--help"])

    assert result.exit_code == 0
    assert "project list" in result.stdout
    assert "workflow list" in result.stdout


def test_task_get_command_returns_task_payload() -> None:
    result = runner.invoke(app, ["task", "get", "extract"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task.get"
    assert payload["data"]["code"] == 201
    assert payload["data"]["taskType"] == "SHELL"


def test_task_get_help_points_to_task_list() -> None:
    result = runner.invoke(app, ["task", "get", "--help"])

    assert result.exit_code == 0
    assert "task list" in result.stdout


def test_task_update_help_points_to_command_schema() -> None:
    result = runner.invoke(app, ["task", "update", "--help"])

    assert result.exit_code == 0
    assert "schema --command" in result.stdout
    assert "task.update" in result.stdout


def test_task_update_command_can_dry_run_native_update() -> None:
    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "command=echo load v2",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task.update"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["request"]["method"] == "PUT"
    assert (
        payload["data"]["request"]["path"]
        == "/projects/7/task-definition/202/with-upstream"
    )
    assert payload["data"]["updated_fields"] == ["command"]
    assert payload["warnings"] == ["dry run: no request was sent"]
    assert payload["warning_details"] == [
        {
            "code": "dry_run_no_request_sent",
            "message": "dry run: no request was sent",
            "request_sent": False,
        }
    ]


def test_task_update_command_can_dry_run_extended_execution_fields() -> None:
    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "flag=NO",
            "--set",
            "environment_code=42",
            "--set",
            "timeout=15",
            "--set",
            "timeout_notify_strategy=FAILED",
            "--set",
            "cpu_quota=50",
            "--set",
            "memory_max=1024",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    request = payload["data"]["request"]
    form = request["form"]
    task_definition = json.loads(form["taskDefinitionJsonObj"])

    assert payload["data"]["updated_fields"] == [
        "flag",
        "environment_code",
        "timeout",
        "timeout_notify_strategy",
        "cpu_quota",
        "memory_max",
    ]
    assert task_definition["flag"] == "NO"
    assert task_definition["environmentCode"] == 42
    assert task_definition["timeout"] == 15
    assert task_definition["timeoutFlag"] == "OPEN"
    assert task_definition["timeoutNotifyStrategy"] == "FAILED"
    assert task_definition["cpuQuota"] == 50
    assert task_definition["memoryMax"] == 1024


def test_task_update_command_reports_schema_suggestion_for_unsupported_set_key() -> (
    None
):
    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "unknown=1",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
    )


def test_task_update_command_suggests_schema_for_invalid_timeout_notify_strategy() -> (
    None
):
    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "timeout_notify_strategy=FAILED",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "timeout_notify_strategy requires timeout > 0"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
    )


def test_task_update_command_reports_schema_suggestion_for_remote_no_change_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                user_id_value=11,
            )
        ],
        dags={
            101: FakeDag(
                workflow_definition_value=FakeWorkflow(
                    code=101,
                    name="daily-sync",
                    project_code_value=7,
                    user_id_value=11,
                ),
                task_definition_list_value=[
                    FakeTaskDefinition(
                        code=201,
                        name="extract",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo extract"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                    FakeTaskDefinition(
                        code=202,
                        name="load",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo load"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                ],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
    )
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo extract"}',
                    flag_value=FakeEnumValue("YES"),
                ),
                FakeTaskDefinition(
                    code=202,
                    name="load",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo load"}',
                    flag_value=FakeEnumValue("YES"),
                ),
            ]
        },
        update_errors_by_code={
            202: ApiResultError(
                result_code=50057,
                result_message="no persisted change",
            )
        },
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
        ),
    )

    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "command=echo load v2",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Task update did not change any persisted fields"
    )
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
    )


def test_task_update_command_reports_invalid_state_suggestion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
                user_id_value=11,
            )
        ],
        dags={
            101: FakeDag(
                workflow_definition_value=FakeWorkflow(
                    code=101,
                    name="daily-sync",
                    project_code_value=7,
                    user_id_value=11,
                ),
                task_definition_list_value=[
                    FakeTaskDefinition(
                        code=201,
                        name="extract",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo extract"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                    FakeTaskDefinition(
                        code=202,
                        name="load",
                        project_code_value=7,
                        task_type_value="SHELL",
                        task_params_value='{"rawScript":"echo load"}',
                        project_name_value="etl-prod",
                        flag_value=FakeEnumValue("YES"),
                    ),
                ],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
    )
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo extract"}',
                    flag_value=FakeEnumValue("YES"),
                ),
                FakeTaskDefinition(
                    code=202,
                    name="load",
                    project_code_value=7,
                    task_type_value="SHELL",
                    project_name_value="etl-prod",
                    task_params_value='{"rawScript":"echo load"}',
                    flag_value=FakeEnumValue("YES"),
                ),
            ]
        },
        update_errors_by_code={
            202: ApiResultError(
                result_code=50056,
                result_message="task state does not support modification",
            )
        },
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
        ),
    )

    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "command=echo load v2",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["message"] == "task state does not support modification"
    assert payload["error"]["suggestion"] == (
        "Inspect the containing workflow definition state; if the workflow is "
        "online, bring it offline before retrying `task update`."
    )


def test_task_update_command_emits_warning_details_for_no_op_update() -> None:
    result = runner.invoke(
        app,
        [
            "task",
            "update",
            "load",
            "--set",
            "command=echo load",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task.update"
    assert payload["warnings"] == ["task update: no persistent changes detected"]
    assert payload["warning_details"] == [
        {
            "code": "task_update_no_persistent_change",
            "message": "task update: no persistent changes detected",
            "no_change": True,
            "request_sent": False,
        }
    ]
