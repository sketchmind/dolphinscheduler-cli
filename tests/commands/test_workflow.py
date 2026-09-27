import json
import shlex
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from dsctl.services.selection import ResourceDefaults
from dsctl.services.workflow import _types as workflow_types
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
    fake_read_service_runtime,
)
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile, normalize_cli_help
from tests.workflow_domain_fakes import install_workflow_domain_runtime

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
            id=23,
            start_time_value="2026-01-01 00:00:00",
            end_time_value="2026-12-31 23:59:59",
            timezone_id_value="UTC",
            crontab_value="0 0 0 * * ?",
            release_state_value=FakeEnumValue("ONLINE"),
        ),
    )
    adhoc_workflow = FakeWorkflow(
        code=102,
        name="adhoc-backfill",
        project_code_value=7,
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
        workflows=[workflow, adhoc_workflow],
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

    def read_runtime_factory(
        *,
        env_file: str | None = None,
        cwd: object = None,
    ) -> object:
        del env_file, cwd
        return fake_read_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=ResourceDefaults(project="etl-prod"),
            workflow_adapter=workflow_adapter,
        )

    monkeypatch.setattr(
        runtime_service,
        "open_read_service_runtime",
        read_runtime_factory,
    )
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        workflow_lineage_adapter=workflow_lineage_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )


def test_workflow_list_command_returns_filtered_workflows() -> None:
    result = runner.invoke(app, ["workflow", "list", "--search", "daily"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.list"
    assert payload["resolved"]["project"]["source"] == "context"
    page_without_coverage = {
        key: value for key, value in payload["data"].items() if key != "coverage"
    }
    assert page_without_coverage == {
        "totalList": [
            {
                "code": 101,
                "name": "daily-sync",
                "version": 1,
                "releaseState": "ONLINE",
                "scheduleReleaseState": "ONLINE",
                "scheduleId": 23,
            }
        ],
        "total": 1,
        "totalPage": 1,
        "pageSize": 100,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert payload["data"]["coverage"]["scope"] == "requested_page"
    assert payload["data"]["coverage"]["pages_read"] == 1


def test_workflow_list_command_supports_standard_paging_controls() -> None:
    result = runner.invoke(
        app,
        ["workflow", "list", "--page-size", "1", "--all"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert [item["name"] for item in payload["data"]["totalList"]] == [
        "daily-sync",
        "adhoc-backfill",
    ]
    assert payload["data"]["total"] == 2
    assert payload["data"]["pageSize"] == 2
    assert payload["data"]["coverage"]["scope"] == "initial_page_range"
    assert payload["data"]["coverage"]["initial_total"] == 2
    assert payload["data"]["coverage"]["initial_total_pages"] == 2
    assert payload["data"]["coverage"]["pages_read"] == 2
    assert payload["data"]["coverage"]["rows_read"] == 2
    assert payload["resolved"]["page_size"] == 1
    assert payload["resolved"]["all"] is True


def test_workflow_export_command_emits_yaml() -> None:
    result = runner.invoke(app, ["workflow", "export", "daily-sync"])

    assert result.exit_code == 0
    assert result.stdout.startswith("workflow:\n")
    assert "name: daily-sync" in result.stdout
    assert "tasks:" in result.stdout


def test_workflow_list_help_points_to_project_discovery() -> None:
    result = runner.invoke(app, ["workflow", "list", "--help"])

    assert result.exit_code == 0
    assert "project list" in normalize_cli_help(result.stdout)


def test_workflow_get_help_points_to_workflow_discovery() -> None:
    result = runner.invoke(app, ["workflow", "get", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "workflow list" in help_text
    assert "Pass WORKFLOW explicitly" in help_text
    assert "--raw" not in help_text
    assert "--format" in help_text


def test_workflow_export_help_points_to_workflow_discovery() -> None:
    result = runner.invoke(app, ["workflow", "export", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "workflow list" in help_text
    assert "--project" in help_text
    assert "raw YAML" in help_text
    assert "read-only schedule-aware edit" in help_text
    assert "display options do not alter" in help_text
    assert "--raw" not in help_text

    edit_result = runner.invoke(app, ["workflow", "edit", "--help"])
    assert edit_result.exit_code == 0
    edit_help = normalize_cli_help(edit_result.stdout)
    assert "read-only snapshot" in edit_help
    assert "--columns diff,no_change" in edit_help


def test_workflow_digest_command_returns_compact_graph_summary() -> None:
    result = runner.invoke(app, ["workflow", "digest", "daily-sync"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.digest"
    assert payload["resolved"]["workflow"]["source"] == "flag"
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


def test_workflow_lineage_get_command_uses_explicit_workflow() -> None:
    result = runner.invoke(app, ["workflow", "lineage", "get", "daily-sync"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.lineage.get"
    assert payload["resolved"]["workflow"]["source"] == "flag"
    assert payload["data"]["workFlowRelationDetailList"][1]["sourceWorkFlowCode"] == (
        "101"
    )


def test_workflow_lineage_dependent_tasks_command_can_filter_by_task() -> None:
    result = runner.invoke(
        app,
        ["workflow", "lineage", "dependent-tasks", "daily-sync", "--task", "extract"],
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
    tmp_path: Path,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
tasks:
  - name: orbital-check
    type: SHELL
    command: echo orbit=stable
  - name: payload-check
    type: SHELL
    command: echo payload=ready
  - name: mission-summary
    type: SHELL
    command: echo LUNA_CLI_EVAL_OK
    depends_on: [orbital-check, payload-check]
""".strip(),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["workflow", "create", "--file", str(spec_path), "--dry-run", "--columns", "*"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.create"
    assert payload["data"]["dry_run"] is True
    assert (
        first_dry_run_request(payload["data"])["path"]
        == "/projects/7/workflow-definition"
    )
    assert "request" not in payload["data"]
    assert shlex.split(payload["next_actions"][0]["command"]) == [
        "dsctl",
        "--format",
        "json-compact",
        "--columns",
        "code,name,releaseState",
        "workflow",
        "create",
        "--file",
        str(spec_path),
        "--project",
        "7",
    ]


@pytest.mark.parametrize("content", ["", "- not-a-workflow\n"])
def test_workflow_create_command_translates_non_mapping_yaml_root(
    tmp_path: Path,
    content: str,
) -> None:
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(content, encoding="utf-8")

    result = runner.invoke(
        app,
        ["workflow", "create", "--file", str(spec_path), "--dry-run"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.create"
    assert payload["error"] == {
        "type": "user_input_error",
        "message": "Workflow YAML root must be a mapping",
        "details": {"file": str(spec_path)},
        "suggestion": (
            "Run `dsctl template workflow` to inspect the stable YAML surface, "
            "then run `dsctl lint workflow PATH` before retrying create."
        ),
    }


def test_workflow_create_command_can_dry_run_schedule_plan(
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
        ["workflow", "create", "--file", str(spec_path), "--dry-run", "--columns", "*"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload["data"]["requests"]) == 4
    schedule_request = payload["data"]["requests"][2]
    assert schedule_request["path"] == "/projects/7/schedules"
    assert schedule_request["form"]["workflowDefinitionCode"] == (
        "<nightly-sync:created_workflow_code>"
    )
    assert "environmentCode" not in schedule_request["form"]
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        schedule_adapter=schedule_adapter,
        profile=make_profile(),
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
    payload = json.loads(result.stderr)
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
    payload = json.loads(result.stderr)
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
                result_code=workflow_types.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={})
    schedule_adapter = FakeScheduleAdapter(schedules=[])
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        schedule_adapter=schedule_adapter,
        profile=make_profile(),
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
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "workflow task relation invalid"
    assert payload["error"]["suggestion"] == (
        "Lint the same workflow file and repeat the create command with --dry-run "
        "to inspect the workflow spec and compiled DS-native payload before "
        "retrying."
    )


def test_workflow_run_command_returns_created_instance_ids() -> None:
    result = runner.invoke(app, ["workflow", "run", "daily-sync"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run"
    assert payload["resolved"]["workflow"]["source"] == "flag"
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
    result = runner.invoke(
        app, ["workflow", "run-task", "daily-sync", "--task", "extract"]
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run-task"
    assert payload["resolved"]["workflow"]["source"] == "flag"
    assert payload["resolved"]["task"]["code"] == 201
    assert payload["resolved"]["scope"] == "self"
    assert payload["data"]["workflowInstanceIds"] == [901]
    assert payload["warnings"][0]["code"] == ("workflow_run_task_dependent_context")


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
            "daily-sync",
            "--dry-run",
            "--columns",
            "*",
            "--execution-dry-run",
            "--param",
            "bizdate=20260415",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.run"
    assert payload["data"]["dry_run"] is True
    form = first_dry_run_request(payload["data"])["form"]
    assert isinstance(form, dict)
    assert form["dryRun"] == 1
    assert form["startParams"] == '{"bizdate":"20260415"}'
    assert [item["code"] for item in payload["warnings"]] == [
        "dry_run_no_mutation_sent",
        "workflow_execution_dry_run",
    ]


def test_workflow_backfill_command_can_dry_run_task_scope() -> None:
    result = runner.invoke(
        app,
        [
            "workflow",
            "backfill",
            "daily-sync",
            "--date",
            "2026-04-01 00:00:00",
            "--task",
            "extract",
            "--scope",
            "self",
            "--dry-run",
            "--columns",
            "*",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.backfill"
    assert payload["data"]["dry_run"] is True
    assert payload["resolved"]["scope"] == "self"
    form = first_dry_run_request(payload["data"])["form"]
    assert isinstance(form, dict)
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


def test_workflow_backfill_command_reports_missing_time_before_version_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preflight_called = False

    def unexpected_preflight(*args: object, **kwargs: object) -> None:
        nonlocal preflight_called
        del args, kwargs
        preflight_called = True
        message = "version preflight must not run for invalid local input"
        raise AssertionError(message)

    monkeypatch.setattr(
        "dsctl.app.preflight_selected_action",
        unexpected_preflight,
    )

    result = runner.invoke(
        app,
        ["workflow", "backfill", "daily-sync", "--start", "2026-04-01 00:00:00"],
    )

    assert result.exit_code == 1
    assert preflight_called is False
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.backfill"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Workflow backfill requires --start and --end, or --date"
    )


def test_workflow_run_task_command_reports_scope_choices() -> None:
    result = runner.invoke(
        app,
        ["workflow", "run-task", "daily-sync", "--task", "extract", "--scope", "down"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.run-task"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == (
        "Task execution scope must be one of: self, pre, post"
    )


def test_workflow_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["workflow", "delete", "daily-sync"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "Workflow deletion requires --force"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_workflow_delete_command_returns_deleted_payload() -> None:
    result = runner.invoke(app, ["workflow", "delete", "daily-sync", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.delete"
    assert payload["resolved"]["workflow"]["source"] == "flag"
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "delete", "daily-sync", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "delete", "daily-sync", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "delete", "daily-sync", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "delete", "daily-sync", "--force"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
            id=23,
            start_time_value="2026-01-01 00:00:00",
            end_time_value="2026-12-31 23:59:59",
            timezone_id_value="UTC",
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "online", "daily-sync"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.online"
    assert payload["data"]["releaseState"] == "ONLINE"
    assert [item["message"] for item in payload.get("warnings", [])] == [
        "workflow brought online; any attached schedule remains offline until "
        "`schedule online` is requested"
    ]
    assert payload["warnings"] == [
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
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
    )

    result = runner.invoke(app, ["workflow", "online", "daily-sync"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.online"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl workflow lineage dependent-tasks 101 --project 7` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry the original workflow online command."
    )


def test_workflow_offline_command_returns_refreshed_payload_and_warning() -> None:
    result = runner.invoke(app, ["workflow", "offline", "daily-sync"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.offline"
    assert payload["data"]["releaseState"] == "OFFLINE"
    assert payload["data"]["scheduleReleaseState"] == "OFFLINE"
    assert [item["message"] for item in payload.get("warnings", [])] == [
        "workflow brought offline; any attached schedule is also taken offline"
    ]


def test_workflow_offline_column_error_preserves_completed_result() -> None:
    result = runner.invoke(
        app,
        [
            "workflow",
            "offline",
            "daily-sync",
            "--format",
            "json-compact",
            "--columns",
            "nonexistent_display_field",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    payload = json.loads(result.stderr)
    assert payload["data"]["code"] == 101
    assert payload["data"]["releaseState"] == "OFFLINE"
    assert payload["resolved"]["workflow"] == {
        "code": 101,
        "name": "daily-sync",
        "source": "flag",
        "version": 1,
    }
    details = payload["error"]["details"]
    assert details["phase"] == "output_render"
    assert details["result_available"] is True
    assert details["operation_returned_success"] is True
    assert "mutation_applied" not in details
    suggestion = payload["error"]["suggestion"]
    assert "Do not repeat the command" in suggestion
    assert "retry" not in suggestion.lower()
    assert payload["warnings"] == [
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
        [
            "workflow",
            "edit",
            "daily-sync",
            "--patch",
            str(patch_path),
            "--dry-run",
            "--columns",
            "*",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["data"]["dry_run"] is True
    assert (
        first_dry_run_request(payload["data"])["path"]
        == "/projects/7/workflow-definition/101"
    )
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


def test_workflow_edit_command_supports_bounded_dry_run_projection(
    tmp_path: Path,
) -> None:
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
        [
            "workflow",
            "edit",
            "daily-sync",
            "--patch",
            str(patch_path),
            "--dry-run",
            "--columns",
            "*",
            "--columns",
            "diff,no_change,workflow_state_constraints,schedule_impacts",
            "--format",
            "json-compact",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["error"]["type"] == "invalid_state"
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["workflow_state_constraint_details"][0]["blocking"] is True
    assert payload["data"]["diff"]["workflow_changes"] == [
        {
            "field": "description",
            "before": "Daily ETL workflow",
            "after": "Daily ETL workflow v2",
        }
    ]
    assert payload["data"]["execution_order"] == [
        {"method": "PUT", "path": "/projects/7/workflow-definition/101"}
    ]
    assert "--columns requests" in payload["data"]["request_details"]


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
        [
            "workflow",
            "edit",
            "daily-sync",
            "--file",
            str(workflow_path),
            "--dry-run",
            "--columns",
            "*",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["resolved"]["input_mode"] == "file"
    assert payload["resolved"]["file"] == str(workflow_path.resolve())
    assert payload["data"]["dry_run"] is True
    assert payload["data"]["diff"]["deleted_tasks"] == ["load"]
    assert payload["data"]["diff"]["task_changes"] == []


def test_workflow_edit_command_requires_one_edit_input() -> None:
    result = runner.invoke(
        app, ["workflow", "edit", "daily-sync", "--dry-run", "--columns", "*"]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
        ["workflow", "edit", "daily-sync", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
        ["workflow", "edit", "daily-sync", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
            id=23,
            start_time_value="2026-01-01 00:00:00",
            end_time_value="2026-12-31 23:59:59",
            timezone_id_value="UTC",
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
                result_code=workflow_types.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: tasks})
    schedule_adapter = FakeScheduleAdapter(schedules=[])
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        schedule_adapter=schedule_adapter,
        context=ResourceDefaults(project="etl-prod"),
        profile=make_profile(),
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
        ["workflow", "edit", "daily-sync", "--patch", str(patch_path)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "workflow.edit"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["message"] == "workflow task relation invalid"
    assert payload["error"]["suggestion"] == (
        "Retry the original workflow edit command with --dry-run to inspect the "
        "compiled diff and DS-native payload before sending it again."
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
        [
            "workflow",
            "edit",
            "daily-sync",
            "--patch",
            str(patch_path),
            "--dry-run",
            "--columns",
            "*",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
        [
            "workflow",
            "edit",
            "daily-sync",
            "--patch",
            str(patch_path),
            "--dry-run",
            "--columns",
            "*",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
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
        ["workflow", "edit", "daily-sync", "--patch", str(patch_path)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "workflow.edit"
    assert [item["message"] for item in payload.get("warnings", [])] == [
        "patch produced no persistent workflow change; no update request was sent",
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately",
    ]
    assert payload["warnings"] == [
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


@pytest.mark.parametrize("route", [("get",), ("run",), ("lineage", "get")])
def test_workflow_identity_is_a_required_parser_argument(
    route: tuple[str, ...],
) -> None:
    result = runner.invoke(app, ["workflow", *route])

    assert result.exit_code == 2
    assert "Missing argument 'WORKFLOW'" in result.stderr
