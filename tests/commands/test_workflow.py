import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.errors import ApiResultError
from dsctl.services import _workflow_compile as workflow_compile_service
from dsctl.services import runtime as runtime_service
from dsctl.services import workflow as workflow_service
from tests.fakes import (
    FakeDag,
    FakeDependentLineageTask,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowLineage,
    FakeWorkflowLineageAdapter,
    FakeWorkflowLineageDetail,
    FakeWorkflowLineageRelation,
    FakeWorkflowTaskRelation,
    fake_service_runtime,
)
from tests.support import make_profile, normalize_cli_help

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_workflow_service(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        description="Daily ETL workflow",
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("ONLINE"),
        schedule_release_state_value=FakeEnumValue("ONLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
        schedule_value=FakeSchedule(
            crontab_value="0 0 0 * * ?",
            release_state_value=FakeEnumValue("ONLINE"),
        ),
    )
    tasks = [
        FakeTaskDefinition(
            code=201,
            name="extract",
            project_code_value=7,
            task_type_value="SHELL",
            task_params_value='{"rawScript":"echo extract"}',
            project_name_value="etl-prod",
        ),
        FakeTaskDefinition(
            code=202,
            name="load",
            project_code_value=7,
            task_type_value="SHELL",
            task_params_value='{"rawScript":"echo load"}',
            project_name_value="etl-prod",
        ),
    ]
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=tasks,
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
        run_results_by_code={101: [901]},
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: tasks})
    workflow_lineage_adapter = FakeWorkflowLineageAdapter(
        project_lineages={
            7: FakeWorkflowLineage(
                work_flow_relation_list_value=[
                    FakeWorkflowLineageRelation(
                        source_work_flow_code_value=101,
                        target_work_flow_code_value=102,
                    )
                ],
                work_flow_relation_detail_list_value=[
                    FakeWorkflowLineageDetail(
                        work_flow_code_value=101,
                        work_flow_name_value="daily-sync",
                        work_flow_publish_status_value="ONLINE",
                        schedule_start_time_value="2026-01-01 00:00:00",
                        schedule_end_time_value="2026-12-31 23:59:59",
                        crontab_value="0 0 0 * * ?",
                        schedule_publish_status_value=1,
                    ),
                    FakeWorkflowLineageDetail(
                        work_flow_code_value=102,
                        work_flow_name_value="quality-check",
                        work_flow_publish_status_value="ONLINE",
                        source_work_flow_code_value="101",
                    ),
                ],
            )
        },
        workflow_lineages={
            (
                7,
                101,
            ): FakeWorkflowLineage(
                work_flow_relation_list_value=[
                    FakeWorkflowLineageRelation(
                        source_work_flow_code_value=101,
                        target_work_flow_code_value=102,
                    )
                ],
                work_flow_relation_detail_list_value=[
                    FakeWorkflowLineageDetail(
                        work_flow_code_value=101,
                        work_flow_name_value="daily-sync",
                        work_flow_publish_status_value="ONLINE",
                    ),
                    FakeWorkflowLineageDetail(
                        work_flow_code_value=102,
                        work_flow_name_value="quality-check",
                        work_flow_publish_status_value="ONLINE",
                        source_work_flow_code_value="101",
                    ),
                ],
            )
        },
        dependent_tasks_by_target={
            (
                7,
                101,
                None,
            ): [
                FakeDependentLineageTask(
                    project_code_value=7,
                    workflow_definition_code_value=102,
                    workflow_definition_name_value="quality-check",
                    task_definition_code_value=301,
                    task_definition_name_value="depends-on-daily-sync",
                )
            ],
            (
                7,
                101,
                201,
            ): [
                FakeDependentLineageTask(
                    project_code_value=7,
                    workflow_definition_code_value=102,
                    workflow_definition_name_value="quality-check",
                    task_definition_code_value=302,
                    task_definition_name_value="depends-on-extract",
                )
            ],
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
            workflow_lineage_adapter=workflow_lineage_adapter,
            task_adapter=task_adapter,
        ),
    )


def test_workflow_list_command_returns_filtered_workflows() -> None:
    result = runner.invoke(app, ["workflow", "list", "--search", "daily"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.list"
    assert payload["resolved"]["project"]["source"] == "context"
    assert payload["data"] == [{"code": 101, "name": "daily-sync", "version": 1}]


def test_workflow_export_command_emits_yaml() -> None:
    result = runner.invoke(app, ["workflow", "export"])

    assert result.exit_code == 0
    assert result.stdout.startswith("workflow:\n")
    assert "name: daily-sync" in result.stdout
    assert "tasks:" in result.stdout


def test_workflow_list_help_points_to_project_discovery() -> None:
    result = runner.invoke(app, ["workflow", "list", "--help"])

    assert result.exit_code == 0
    assert "project list" in result.stdout


def test_workflow_get_help_points_to_workflow_discovery() -> None:
    result = runner.invoke(app, ["workflow", "get", "--help"])

    assert result.exit_code == 0
    assert "workflow list" in result.stdout
    assert "--raw" not in result.stdout
    assert "--format" not in result.stdout


def test_workflow_export_help_points_to_workflow_discovery() -> None:
    result = runner.invoke(app, ["workflow", "export", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "workflow list" in help_text
    assert "--project" in help_text
    assert "--raw" not in help_text


def test_workflow_digest_command_returns_compact_graph_summary() -> None:
    result = runner.invoke(app, ["workflow", "digest"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.digest"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["data"]["taskCount"] == 2
    assert payload["data"]["taskTypeCounts"] == {"SHELL": 2}
    assert payload["data"]["rootTasks"] == [{"code": 201, "name": "extract"}]


def test_workflow_lineage_list_command_returns_project_graph() -> None:
    result = runner.invoke(app, ["workflow", "lineage", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.lineage.list"
    assert payload["resolved"]["project"]["source"] == "context"
    assert payload["data"]["workFlowRelationList"] == [
        {"sourceWorkFlowCode": 101, "targetWorkFlowCode": 102}
    ]
    assert payload["data"]["workFlowRelationDetailList"][0]["workFlowName"] == (
        "daily-sync"
    )


def test_workflow_lineage_get_command_uses_workflow_context() -> None:
    result = runner.invoke(app, ["workflow", "lineage", "get"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.lineage.get"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["data"]["workFlowRelationDetailList"][1]["sourceWorkFlowCode"] == (
        "101"
    )


def test_workflow_lineage_dependent_tasks_command_can_filter_by_task() -> None:
    result = runner.invoke(
        app,
        ["workflow", "lineage", "dependent-tasks", "--task", "extract"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.lineage.dependent-tasks"
    assert payload["resolved"]["task"]["source"] == "flag"
    assert payload["data"] == [
        {
            "projectCode": 7,
            "workflowDefinitionCode": 102,
            "workflowDefinitionName": "quality-check",
            "taskDefinitionCode": 302,
            "taskDefinitionName": "depends-on-extract",
        }
    ]


def test_workflow_lineage_dependent_tasks_help_points_to_task_discovery() -> None:
    result = runner.invoke(app, ["workflow", "lineage", "dependent-tasks", "--help"])

    assert result.exit_code == 0
    assert "task list" in result.stdout


def test_workflow_create_command_can_dry_run_yaml_spec(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    codes = iter([7201])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "create", "--file", str(spec_path), "--dry-run"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.create"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["request"]["path"] == "/projects/7/workflow-definition"
    assert len(payload["data"]["requests"]) == 1


def test_workflow_create_command_can_dry_run_schedule_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    codes = iter([7202])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: true
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "create", "--file", str(spec_path), "--dry-run"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload["data"]["requests"]) == 4
    assert payload["data"]["requests"][2]["path"] == "/v2/schedules"
    assert payload["data"]["requests"][3]["path"] == (
        "/projects/7/schedules/<nightly-sync:created_schedule_id>/online"
    )
    assert payload["data"]["schedule_preview"]["count"] == 5
    assert payload["data"]["schedule_confirmation"]["required"] is False


def test_workflow_create_command_requires_confirmation_for_high_frequency_schedule(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(workflows=[], dags={})
    task_adapter = FakeTaskAdapter(workflow_tasks={})
    schedule_adapter = FakeScheduleAdapter(
        schedules=[],
        preview_times_value=[
            "2024-01-01 00:00:00",
            "2024-01-01 00:05:00",
            "2024-01-01 00:10:00",
            "2024-01-01 00:15:00",
            "2024-01-01 00:20:00",
        ],
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 */5 * * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "create", "--file", str(spec_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["type"] == "confirmation_required"
    assert payload["error"]["details"]["risk_type"] == "high_frequency_schedule"
    assert payload["error"]["details"]["confirm_flag"].startswith("--confirm-risk ")
    assert payload["error"]["suggestion"].startswith(
        "Retry the same command with --confirm-risk "
    )


def test_workflow_create_command_rejects_five_field_schedule_cron(
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: "0 2 * * *"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["workflow", "create", "--file", str(spec_path)])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "schedule.cron must be a Quartz cron expression with 6 or 7 fields "
        "(seconds first); got 5"
    )
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template workflow` to inspect the stable YAML surface, "
        "then run `dsctl lint workflow PATH` before retrying create."
    )


def test_workflow_create_command_suggests_review_for_remote_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[],
        dags={},
        create_errors_by_name={
            "nightly-sync": ApiResultError(
                result_code=workflow_service.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={})
    schedule_adapter = FakeScheduleAdapter(schedules=[])
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["workflow", "create", "--file", str(spec_path)])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "workflow task relation invalid"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl lint workflow FILE` and `dsctl workflow create --file FILE "
        "--dry-run` to inspect the workflow spec and compiled DS-native payload "
        "before retrying."
    )


def test_workflow_run_command_returns_created_instance_ids() -> None:
    result = runner.invoke(app, ["workflow", "run"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["resolved"]["worker_group"]["source"] == "default"
    assert payload["resolved"]["tenant"]["source"] == "default"
    assert payload["data"]["workflowInstanceIds"] == [901]


def test_workflow_run_help_points_to_runtime_selector_discovery() -> None:
    result = runner.invoke(app, ["workflow", "run", "--help"])

    assert result.exit_code == 0
    assert "workflow" in result.stdout
    assert "project" in result.stdout
    assert "worker-group" in result.stdout
    assert "tenant" in result.stdout
    assert "alert-group" in result.stdout
    assert "environment" in result.stdout
    assert "list" in result.stdout


def test_workflow_run_task_command_returns_created_instance_ids_and_warning() -> None:
    result = runner.invoke(app, ["workflow", "run-task", "--task", "extract"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run-task"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["resolved"]["task"]["code"] == 201
    assert payload["resolved"]["scope"] == "self"
    assert payload["data"]["workflowInstanceIds"] == [901]
    assert payload["warning_details"][0]["code"] == (
        "workflow_run_task_dependent_context"
    )


def test_workflow_run_task_help_points_to_task_discovery() -> None:
    result = runner.invoke(app, ["workflow", "run-task", "--help"])

    assert result.exit_code == 0
    assert "task" in result.stdout
    assert "list" in result.stdout


def test_workflow_run_command_can_dry_run_runtime_options() -> None:
    result = runner.invoke(
        app,
        [
            "workflow",
            "run",
            "--dry-run",
            "--execution-dry-run",
            "--param",
            "bizdate=20260415",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run"
    assert payload["data"]["dry_run"] is True
    form = payload["data"]["request"]["form"]
    assert form["dryRun"] == 1
    assert form["startParams"] == '{"bizdate":"20260415"}'
    assert [item["code"] for item in payload["warning_details"]] == [
        "dry_run_no_request_sent",
        "workflow_execution_dry_run",
    ]


def test_workflow_backfill_command_can_dry_run_task_scope() -> None:
    result = runner.invoke(
        app,
        [
            "workflow",
            "backfill",
            "--date",
            "2026-04-01 00:00:00",
            "--task",
            "extract",
            "--scope",
            "self",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.backfill"
    assert payload["data"]["dry_run"] is True
    assert payload["resolved"]["scope"] == "self"
    form = payload["data"]["request"]["form"]
    assert form["execType"] == "COMPLEMENT_DATA"
    assert form["startNodeList"] == "201"
    assert form["taskDependType"] == "TASK_ONLY"
    assert form["runMode"] == "RUN_MODE_SERIAL"


def test_workflow_backfill_help_points_to_task_and_runtime_discovery() -> None:
    result = runner.invoke(app, ["workflow", "backfill", "--help"])

    assert result.exit_code == 0
    assert "task" in result.stdout
    assert "list" in result.stdout
    assert "worker-group" in result.stdout
    assert "tenant" in result.stdout
    assert "environment" in result.stdout


def test_workflow_backfill_command_reports_missing_time_selection() -> None:
    result = runner.invoke(app, ["workflow", "backfill"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.backfill"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Workflow backfill requires --start and --end, or --date"
    )


def test_workflow_run_task_command_reports_scope_choices() -> None:
    result = runner.invoke(
        app,
        ["workflow", "run-task", "--task", "extract", "--scope", "down"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run-task"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Task execution scope must be one of: self, pre, post"
    )


def test_workflow_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["workflow", "delete"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "Workflow deletion requires --force"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_workflow_delete_command_returns_deleted_payload() -> None:
    result = runner.invoke(app, ["workflow", "delete", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["resolved"]["workflow"]["source"] == "context"
    assert payload["data"]["deleted"] is True
    assert payload["data"]["workflow"]["name"] == "daily-sync"


def test_workflow_delete_command_suggests_offline_before_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        },
        delete_errors_by_code={
            101: ApiResultError(
                result_code=50021,
                result_message="workflow definition [daily-sync] is already online",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: []})
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

    result = runner.invoke(app, ["workflow", "delete", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
        "then retry `dsctl workflow delete --force`."
    )


def test_workflow_delete_command_suggests_schedule_cleanup_for_online_schedule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        },
        delete_errors_by_code={
            101: ApiResultError(
                result_code=50023,
                result_message="workflow definition [daily-sync] has online schedule",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: []})
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

    result = runner.invoke(app, ["workflow", "delete", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl schedule list --workflow WORKFLOW --project PROJECT` to find "
        "the attached schedule, take it offline with `dsctl schedule offline "
        "SCHEDULE_ID`, then retry `dsctl workflow delete --force`."
    )


def test_workflow_delete_command_suggests_instance_inspection_for_running_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        },
        delete_errors_by_code={
            101: ApiResultError(
                result_code=10163,
                result_message=(
                    "workflow definition [daily-sync] has running instances"
                ),
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: []})
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

    result = runner.invoke(app, ["workflow", "delete", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow-instance list --workflow WORKFLOW --project PROJECT` "
        "to inspect active instances, stop or wait for them to finish, then "
        "retry deletion."
    )


def test_workflow_delete_command_suggests_lineage_for_referenced_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        },
        delete_errors_by_code={
            101: ApiResultError(
                result_code=10193,
                result_message=(
                    "delete workflow definition fail, cause used by other tasks"
                ),
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: []})
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

    result = runner.invoke(app, ["workflow", "delete", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "conflict"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect references before retrying deletion."
    )


def test_workflow_online_command_returns_refreshed_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    offline_workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
        schedule_value=FakeSchedule(
            crontab_value="0 0 0 * * ?",
            release_state_value=FakeEnumValue("OFFLINE"),
        ),
    )
    tasks = [
        FakeTaskDefinition(
            code=201,
            name="extract",
            project_code_value=7,
            task_type_value="SHELL",
            task_params_value='{"rawScript":"echo extract"}',
            project_name_value="etl-prod",
        )
    ]
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[offline_workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=offline_workflow,
                task_definition_list_value=tasks,
                workflow_task_relation_list_value=[],
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: tasks})
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

    result = runner.invoke(app, ["workflow", "online"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.online"
    assert payload["data"]["releaseState"] == "ONLINE"
    assert payload["warnings"] == [
        "workflow brought online; any attached schedule remains offline until "
        "`schedule online` is requested"
    ]
    assert payload["warning_details"] == [
        {
            "code": "workflow_online_leaves_schedule_offline",
            "message": (
                "workflow brought online; any attached schedule remains offline "
                "until `schedule online` is requested"
            ),
            "action": "online",
            "workflow_release_state": "OFFLINE",
            "schedule_release_state": "OFFLINE",
        }
    ]


def test_workflow_online_command_suggests_bringing_subworkflows_online(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    offline_workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[offline_workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=offline_workflow,
                task_definition_list_value=[],
                workflow_task_relation_list_value=[],
            )
        },
        online_errors_by_code={
            101: ApiResultError(
                result_code=50004,
                result_message="exist sub workflow definition not online",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: []})
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

    result = runner.invoke(app, ["workflow", "online"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.online"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry `dsctl workflow online`."
    )


def test_workflow_offline_command_returns_refreshed_payload_and_warning() -> None:
    result = runner.invoke(app, ["workflow", "offline"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.offline"
    assert payload["data"]["releaseState"] == "OFFLINE"
    assert payload["data"]["scheduleReleaseState"] == "OFFLINE"
    assert payload["warnings"] == [
        "workflow brought offline; any attached schedule is also taken offline"
    ]
    assert payload["warning_details"] == [
        {
            "code": "workflow_offline_also_offlines_schedule",
            "message": (
                "workflow brought offline; any attached schedule is also taken offline"
            ),
            "action": "offline",
            "workflow_release_state": "ONLINE",
            "schedule_release_state": "ONLINE",
        }
    ]


def test_workflow_edit_command_can_dry_run_patch_diff(tmp_path: Path) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow v2
  tasks:
    rename:
      - from: extract
        to: extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path), "--dry-run"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["request"]["path"] == "/projects/7/workflow-definition/101"
    assert payload["data"]["diff"]["renamed_tasks"] == [
        {
            "from_name": "extract",
            "to_name": "extract-v2",
        }
    ]
    assert payload["data"]["workflow_state_constraints"] == [
        (
            "workflow is currently online; DolphinScheduler only allows "
            "whole-definition edits while offline"
        ),
        (
            "taking this workflow offline before apply will also take the "
            "attached schedule offline"
        ),
    ]
    assert payload["data"]["workflow_state_constraint_details"] == [
        {
            "code": "workflow_must_be_offline",
            "message": (
                "workflow is currently online; DolphinScheduler only allows "
                "whole-definition edits while offline"
            ),
            "blocking": True,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": "ONLINE",
        },
        {
            "code": "offline_also_offlines_attached_schedule",
            "message": (
                "taking this workflow offline before apply will also take the "
                "attached schedule offline"
            ),
            "blocking": False,
            "current_release_state": "ONLINE",
            "required_release_state": "OFFLINE",
            "current_schedule_release_state": "ONLINE",
        },
    ]


def test_workflow_edit_command_can_dry_run_full_file_diff(tmp_path: Path) -> None:
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow v2
  timeout: 0
  execution_type: PARALLEL
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "daily-sync", "--file", str(workflow_path), "--dry-run"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["resolved"]["input_mode"] == "file"
    assert payload["resolved"]["file"] == str(workflow_path.resolve())
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["diff"]["deleted_tasks"] == ["load"]
    assert payload["data"]["diff"]["updated_tasks"] == []


def test_workflow_edit_command_requires_one_edit_input() -> None:
    result = runner.invoke(app, ["workflow", "edit", "daily-sync", "--dry-run"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "Pass exactly one of --patch or --file."


def test_workflow_edit_command_suggests_offline_before_apply(tmp_path: Path) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow v2
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, then "
        "retry `dsctl workflow edit`. Review `schedule_impact_detail` before "
        "taking an attached schedule offline."
    )


def test_workflow_edit_command_rejects_invalid_patch_yaml_with_dry_run_suggestion(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
- patch:
    workflow:
      set:
        description: invalid
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "Workflow patch YAML root must be a mapping"
    assert payload["error"]["suggestion"] == (
        "Fix the patch YAML, then retry the same command with `--dry-run` to "
        "inspect the compiled diff before apply."
    )


def test_workflow_edit_command_suggests_dry_run_for_remote_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    offline_workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        description="Daily ETL workflow",
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
        schedule_value=FakeSchedule(
            crontab_value="0 0 0 * * ?",
            release_state_value=FakeEnumValue("OFFLINE"),
        ),
    )
    tasks = [
        FakeTaskDefinition(
            code=201,
            name="extract",
            project_code_value=7,
            task_type_value="SHELL",
            task_params_value='{"rawScript":"echo extract"}',
            project_name_value="etl-prod",
        ),
        FakeTaskDefinition(
            code=202,
            name="load",
            project_code_value=7,
            task_type_value="SHELL",
            task_params_value='{"rawScript":"echo load"}',
            project_name_value="etl-prod",
        ),
    ]
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[offline_workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=offline_workflow,
                task_definition_list_value=tasks,
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
        update_errors_by_code={
            101: ApiResultError(
                result_code=workflow_service.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: tasks})
    schedule_adapter = FakeScheduleAdapter(schedules=[])
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
            schedule_adapter=schedule_adapter,
        ),
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow v2
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "workflow task relation invalid"
    assert payload["error"]["suggestion"] == (
        "Retry with `dsctl workflow edit --dry-run` to inspect the compiled diff "
        "and DS-native payload before sending it again."
    )


def test_workflow_edit_command_reports_invalid_extended_task_patch(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          timeout_notify_strategy: FAILED
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path), "--dry-run"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert "requires timeout > 0" in payload["error"]["message"]
    assert payload["error"]["suggestion"] == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_workflow_edit_command_reports_patch_operation_conflict_suggestion(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - from: extract
        to: extract-v2
      - from: extract
        to: extract-v3
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path), "--dry-run"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Patch renames task 'extract' more than once"
    )
    assert payload["error"]["suggestion"] == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_workflow_edit_command_emits_warning_details_for_no_op_patch(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  workflow:
    set:
      description: Daily ETL workflow
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "edit", "--patch", str(patch_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert payload["warnings"] == [
        "patch produced no persistent workflow change; no update request was sent",
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately",
    ]
    assert payload["warning_details"] == [
        {
            "code": "workflow_edit_no_persistent_change",
            "message": (
                "patch produced no persistent workflow change; "
                "no update request was sent"
            ),
            "no_change": True,
            "request_sent": False,
        },
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "ONLINE",
        },
    ]
