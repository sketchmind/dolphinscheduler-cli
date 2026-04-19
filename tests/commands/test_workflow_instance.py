import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflow,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    FakeWorkflowTaskRelation,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_workflow_instance_service(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_definition = FakeWorkflow(
        code=101,
        name="daily-sync",
        version=1,
        project_code_value=7,
        project_name_value="etl-prod",
        global_params_value='[{"prop":"env","value":"prod"}]',
        global_param_map_value={"env": "prod"},
        timeout=30,
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    workflow_dag = FakeDag(
        workflow_definition_value=workflow_definition,
        task_definition_list_value=[
            FakeTaskDefinition(
                code=201,
                name="extract",
                version=1,
                project_code_value=7,
                task_type_value="SHELL",
                task_params_value='{"rawScript":"echo extract"}',
                worker_group_value="default",
                project_name_value="etl-prod",
            ),
            FakeTaskDefinition(
                code=202,
                name="load",
                version=1,
                project_code_value=7,
                task_type_value="SHELL",
                task_params_value='{"rawScript":"echo load"}',
                worker_group_value="default",
                project_name_value="etl-prod",
            ),
        ],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=201,
                post_task_code_value=202,
            )
        ],
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                host="master-1",
                executor_id_value=11,
                executor_name_value="alice",
                worker_group_value="default",
            ),
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="child-workflow-903",
                host="master-2",
                executor_id_value=12,
                executor_name_value="bob",
                worker_group_value="default",
            ),
        ],
        parent_workflow_instance_ids_by_sub_id={903: 901},
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_instance_adapter=FakeTaskInstanceAdapter(
                task_instances=[
                    FakeTaskInstance(
                        id=3001,
                        name="extract",
                        task_type_value="SHELL",
                        workflow_instance_id_value=901,
                        workflow_instance_name_value="daily-sync-901",
                        project_code_value=7,
                        task_code_value=201,
                        state_value=FakeEnumValue("RUNNING_EXECUTION"),
                        host="worker-1",
                    ),
                    FakeTaskInstance(
                        id=3002,
                        name="load",
                        task_type_value="SQL",
                        workflow_instance_id_value=901,
                        workflow_instance_name_value="daily-sync-901",
                        project_code_value=7,
                        task_code_value=202,
                        state_value=FakeEnumValue("FAILURE"),
                        retry_times_value=1,
                    ),
                ]
            ),
        ),
    )


def test_workflow_instance_list_command_returns_page_payload() -> None:
    result = runner.invoke(app, ["workflow-instance", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["id"] == 901


def test_workflow_instance_list_command_supports_all_pages() -> None:
    result = runner.invoke(
        app,
        ["workflow-instance", "list", "--page-size", "1", "--all"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.list"
    assert payload["resolved"]["all"] is True
    assert payload["data"]["total"] == 2
    assert len(payload["data"]["totalList"]) == 2


def test_workflow_instance_list_command_accepts_project_scoped_filters() -> None:
    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "list",
            "--project",
            "etl-prod",
            "--search",
            "daily",
            "--executor",
            "alice",
            "--host",
            "master",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.list"
    assert payload["resolved"]["project"] == "etl-prod"
    assert payload["resolved"]["project_code"] == 7
    assert payload["resolved"]["search"] == "daily"
    assert payload["resolved"]["executor"] == "alice"
    assert payload["resolved"]["host"] == "master"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["id"] == 901


def test_workflow_instance_list_command_reports_supported_state_names() -> None:
    result = runner.invoke(
        app,
        ["workflow-instance", "list", "--state", "running"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Workflow instance state must be one of the DS execution status names"
    )
    assert payload["error"]["suggestion"] == (
        "Run `dsctl enum list workflow-execution-status` to inspect the "
        "supported state names."
    )


def test_workflow_instance_list_help_points_to_filter_discovery() -> None:
    result = runner.invoke(app, ["workflow-instance", "list", "--help"])

    assert result.exit_code == 0
    assert "project" in result.stdout
    assert "workflow" in result.stdout
    assert "workflow-execution-status" in result.stdout
    assert "list" in result.stdout


def test_workflow_instance_get_help_points_to_instance_discovery() -> None:
    result = runner.invoke(app, ["workflow-instance", "get", "--help"])

    assert result.exit_code == 0
    assert "workflow-instance" in result.stdout
    assert "list" in result.stdout


def test_workflow_instance_get_command_returns_one_instance() -> None:
    result = runner.invoke(app, ["workflow-instance", "get", "901"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.get"
    assert payload["data"]["state"] == "RUNNING_EXECUTION"


def test_workflow_instance_parent_command_returns_parent_relation() -> None:
    result = runner.invoke(app, ["workflow-instance", "parent", "903"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.parent"
    assert payload["data"] == {"parentWorkflowInstance": 901}
    assert payload["resolved"] == {"subWorkflowInstance": {"id": 903}}


def test_workflow_instance_parent_command_rejects_regular_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_definition = FakeWorkflow(
        code=101,
        name="daily-sync",
        version=1,
        project_code_value=7,
    )
    workflow_dag = FakeDag(
        workflow_definition_value=workflow_definition,
        task_definition_list_value=[],
        workflow_task_relation_list_value=[],
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=901,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                executor_id_value=11,
            )
        ]
    )

    def not_subworkflow(
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> object:
        del project_code, sub_workflow_instance_id
        raise ApiResultError(
            result_code=50010,
            result_message="workflow instance is not sub workflow instance",
        )

    monkeypatch.setattr(
        workflow_instance_adapter,
        "parent_instance_by_sub_workflow",
        not_subworkflow,
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
        ),
    )

    result = runner.invoke(app, ["workflow-instance", "parent", "901"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.parent"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Use `dsctl workflow-instance get ID` for regular workflow instances; "
        "`parent` only applies to sub-workflow instances."
    )


def test_workflow_instance_digest_command_returns_runtime_summary() -> None:
    result = runner.invoke(app, ["workflow-instance", "digest", "901"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.digest"
    assert payload["data"]["taskCount"] == 2
    assert payload["data"]["progress"] == {
        "running": 1,
        "queued": 0,
        "paused": 0,
        "failed": 1,
        "success": 0,
        "other": 0,
        "finished": 1,
        "active": 1,
    }
    assert payload["data"]["runningTasks"] == [
        {
            "id": 3001,
            "taskCode": 201,
            "name": "extract",
            "taskType": "SHELL",
            "state": "RUNNING_EXECUTION",
            "retryTimes": 0,
            "host": "worker-1",
            "startTime": None,
            "endTime": None,
            "duration": None,
        }
    ]


def test_workflow_instance_update_command_supports_dry_run(tmp_path: Path) -> None:
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    update:
      - match:
          name: extract
        set:
          command: echo extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "update",
            "903",
            "--patch",
            str(patch_file),
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.update"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["request"]["path"] == "/projects/7/workflow-instances/903"
    assert payload["resolved"]["syncDefine"] is False


def test_workflow_instance_update_command_updates_finished_instance(
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
  tasks:
    rename:
      - from: extract
        to: extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "update",
            "903",
            "--patch",
            str(patch_file),
            "--sync-definition",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.update"
    assert payload["data"]["workflowDefinitionVersion"] == 2
    assert payload["data"]["timeout"] == 45
    assert payload["resolved"]["syncDefine"] is True


def test_workflow_instance_update_command_suggests_workflow_edit_for_definition_fields(
    tmp_path: Path,
) -> None:
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      name: renamed-instance-workflow
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "update",
            "903",
            "--patch",
            str(patch_file),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Use `dsctl workflow edit --patch ...` for definition-level fields "
        "such as name, description, or release_state."
    )


def test_workflow_instance_stop_command_returns_refreshed_instance() -> None:
    result = runner.invoke(app, ["workflow-instance", "stop", "901"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.stop"
    assert payload["data"]["state"] == "READY_STOP"
    assert payload["warnings"] == [
        "stop requested; current workflow instance state is READY_STOP"
    ]
    assert payload["warning_details"] == [
        {
            "code": "workflow_instance_action_state_after_request",
            "action": "stop",
            "message": "stop requested; current workflow instance state is READY_STOP",
            "current_state": "READY_STOP",
            "expect_non_final": False,
            "target_state": "STOP",
        }
    ]


def test_workflow_instance_watch_command_waits_for_final_state(
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
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                executor_id_value=11,
            )
        ],
        workflow_instance_sequences_by_id={
            901: [
                FakeWorkflowInstance(
                    id=901,
                    workflow_definition_code_value=101,
                    workflow_definition_version_value=1,
                    project_code_value=7,
                    state_value=FakeEnumValue("RUNNING_EXECUTION"),
                    run_times_value=1,
                    name="daily-sync-901",
                    executor_id_value=11,
                ),
                FakeWorkflowInstance(
                    id=901,
                    workflow_definition_code_value=101,
                    workflow_definition_version_value=1,
                    project_code_value=7,
                    state_value=FakeEnumValue("SUCCESS"),
                    run_times_value=1,
                    name="daily-sync-901",
                    executor_id_value=11,
                ),
            ]
        },
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
        ),
    )
    monkeypatch.setattr("dsctl.services.workflow_instance.time.sleep", lambda _: None)

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "watch",
            "901",
            "--interval-seconds",
            "1",
            "--timeout-seconds",
            "5",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.watch"
    assert payload["data"]["state"] == "SUCCESS"


def test_workflow_instance_watch_command_reports_timeout_suggestion(
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
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                run_times_value=1,
                name="daily-sync-901",
                executor_id_value=11,
            )
        ]
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
        ),
    )
    monotonic_values = iter((0.0, 5.1))
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("dsctl.services.workflow_instance.time.sleep", lambda _: None)

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "watch",
            "901",
            "--interval-seconds",
            "1",
            "--timeout-seconds",
            "5",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.watch"
    assert payload["error"]["type"] == "timeout"
    assert payload["error"]["suggestion"] == (
        "Retry with a larger --timeout-seconds value or inspect the current "
        "state with `workflow-instance get 901`."
    )


def test_workflow_instance_rerun_command_returns_refreshed_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="daily-sync-902",
                executor_id_value=11,
            )
        ]
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
        ),
    )

    result = runner.invoke(app, ["workflow-instance", "rerun", "902"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.rerun"
    assert payload["data"]["state"] == "RUNNING_EXECUTION"


def test_workflow_instance_recover_failed_command_returns_refreshed_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("FAILURE"),
                run_times_value=1,
                name="daily-sync-903",
                executor_id_value=11,
            )
        ]
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
        ),
    )

    result = runner.invoke(app, ["workflow-instance", "recover-failed", "903"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.recover-failed"
    assert payload["data"]["state"] == "RUNNING_EXECUTION"


def test_workflow_instance_recover_failed_command_reports_failure_requirement() -> None:
    result = runner.invoke(app, ["workflow-instance", "recover-failed", "903"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.recover-failed"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["message"] == (
        "This workflow instance must be in FAILURE state before recover-failed."
    )
    assert payload["error"]["suggestion"] == (
        "Use `dsctl workflow-instance get ID` or "
        "`dsctl workflow-instance watch ID` to confirm the instance is in "
        "FAILURE before retrying `recover-failed`."
    )


def test_workflow_instance_execute_task_command_returns_resolved_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_instance_adapter = FakeWorkflowInstanceAdapter(
        workflow_instances=[
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="daily-sync-902",
                executor_id_value=11,
            )
        ]
    )
    task_adapter = FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                )
            ]
        }
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_adapter=task_adapter,
        ),
    )

    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "execute-task",
            "902",
            "--task",
            "extract",
            "--scope",
            "pre",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.execute-task"
    assert payload["data"]["state"] == "RUNNING_EXECUTION"
    assert payload["resolved"]["scope"] == "pre"
    assert payload["resolved"]["task"]["code"] == 201


def test_workflow_instance_execute_task_command_reports_scope_choices() -> None:
    result = runner.invoke(
        app,
        [
            "workflow-instance",
            "execute-task",
            "902",
            "--task",
            "extract",
            "--scope",
            "before",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow-instance.execute-task"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Task execution scope must be one of: self, pre, post"
    )
    assert payload["error"]["suggestion"] == (
        "Pass `--scope self`, `--scope pre`, or `--scope post`."
    )


def test_workflow_instance_execute_task_help_points_to_task_discovery() -> None:
    result = runner.invoke(app, ["workflow-instance", "execute-task", "--help"])

    assert result.exit_code == 0
    assert "task-instance" in result.stdout
    assert "workflow-instance" in result.stdout
    assert "list" in result.stdout
