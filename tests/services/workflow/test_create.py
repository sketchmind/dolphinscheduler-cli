"""Workflow creation plans, confirmation, application, and public errors."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeWorkflowAdapter,
)
from tests.request_assertions import first_dry_run_request
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    ConfirmationRequiredError,
    ConflictError,
    DsctlError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.services import workflow as workflow_service
from dsctl.services.workflow import _types as workflow_types
from dsctl.upstream.wire import WireRequest


def test_create_workflow_result_can_dry_run_selected_version_request(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  description: Nightly workflow
  timeout: 45
  global_params:
    env: prod
  execution_type: PARALLEL
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
    priority: HIGH
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    requests = _sequence(data["requests"])
    form = _mapping(request["form"])

    assert data["dry_run"] is True
    assert request["method"] == "POST"
    assert request["path"] == "/projects/7/workflow-definition"
    assert len(requests) == 2
    assert _mapping(requests[1])["path"] == (
        "/projects/7/workflow-definition/<nightly-sync:created_workflow_code>/release"
    )
    assert _mapping(result.resolved["project"])["source"] == "file"
    assert _mapping(result.resolved["workflow"]) == {
        "name": "nightly-sync",
        "source": "file",
    }
    expected_extract_params = json.dumps(
        {
            "rawScript": "echo extract",
            "localParams": [],
            "resourceList": [],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    expected_load_params = json.dumps(
        {
            "rawScript": "echo load",
            "localParams": [],
            "resourceList": [],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert json.loads(str(form["globalParams"])) == [
        {
            "prop": "env",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "prod",
        }
    ]
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    extract_code = task_definitions[0]["code"]
    load_code = task_definitions[1]["code"]
    assert isinstance(extract_code, int)
    assert isinstance(load_code, int)
    assert extract_code > 0
    assert load_code > 0
    assert extract_code != load_code
    assert task_definitions == [
        {
            "code": extract_code,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": expected_extract_params,
            "flag": "YES",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": -1,
            "taskGroupId": 0,
            "taskGroupPriority": 0,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
        },
        {
            "code": load_code,
            "version": 1,
            "name": "load",
            "description": "",
            "taskType": "SHELL",
            "taskParams": expected_load_params,
            "flag": "YES",
            "taskPriority": "HIGH",
            "workerGroup": "default",
            "environmentCode": -1,
            "taskGroupId": 0,
            "taskGroupPriority": 0,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
        },
    ]
    assert json.loads(str(form["taskRelationJson"])) == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": extract_code,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": extract_code,
            "preTaskVersion": 1,
            "postTaskCode": load_code,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_dry_run_resolves_datasource_name_to_verified_wire_id(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        datasource_adapter=FakeDataSourceAdapter(
            [
                FakeDataSource(
                    id=17,
                    name="analytics-mysql",
                    type_value=FakeEnumValue("MYSQL"),
                )
            ]
        )
    )
    spec_path = tmp_path / "sql-workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: daily-query
  project: etl-prod
tasks:
  - name: query-orders
    type: SQL
    task_params:
      type: MYSQL
      datasource: analytics-mysql
      sql: select 1
      sqlType: 0
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    task_definitions = json.loads(str(_mapping(request["form"])["taskDefinitionJson"]))
    assert json.loads(task_definitions[0]["taskParams"])["datasource"] == 17
    assert _sequence(result.resolved["task_datasources"]) == [
        {
            "task": "query-orders",
            "task_type": "SQL",
            "requested": "analytics-mysql",
            "id": 17,
            "name": "analytics-mysql",
            "type": "MYSQL",
        }
    ]


def test_create_workflow_dry_run_projects_the_domain_prepared_request(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    operations = workflow_harness.install()
    prepared_calls: list[dict[str, object]] = []
    applied: list[object] = []

    def prepare_create(_project: object, **values: object) -> object:
        prepared_calls.append(values)
        return SimpleNamespace(
            request=WireRequest(
                method="POST",
                path="/selected-version/process-definition",
                query=None,
                form={"exactField": "exact-value"},
                json=None,
                content=None,
            )
        )

    monkeypatch.setattr(operations, "prepare_create", prepare_create)
    monkeypatch.setattr(
        operations,
        "apply_create",
        applied.append,
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

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    request = _mapping(first_dry_run_request(_mapping(result.data)))
    assert request == {
        "method": "POST",
        "path": "/selected-version/process-definition",
        "form": {"exactField": "exact-value"},
    }
    assert len(prepared_calls) == 1
    assert applied == []


@pytest.mark.parametrize(
    (
        "result_code",
        "result_message",
        "error_type",
        "error_match",
        "expected_suggestion",
    ),
    [
        (
            workflow_types.PROJECT_NOT_FOUND,
            "project not found",
            NotFoundError,
            "Project 'etl-prod' was not found",
            (
                "Run `dsctl project list` to find an existing project, select it "
                "with --project or workflow.project, then retry create."
            ),
        ),
        (
            workflow_types.WORKFLOW_DEFINITION_NAME_EXIST,
            "workflow definition name exists",
            ConflictError,
            "Workflow 'nightly-sync' already exists",
            (
                "Choose a unique workflow.name and retry create. To update the "
                "existing workflow instead, run `dsctl workflow edit nightly-sync "
                "--file FILE --project 7 --dry-run` after replacing FILE with the "
                "intended full YAML path."
            ),
        ),
        (
            30001,
            "current user has no operation permission",
            PermissionDeniedError,
            "cannot create workflow 'nightly-sync'",
            (
                "Ask a project owner or administrator for workflow create permission "
                "in project 'etl-prod', then retry create."
            ),
        ),
    ],
)
def test_create_workflow_dry_run_translates_prepare_rejection_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    result_code: int,
    result_message: str,
    error_type: type[DsctlError],
    error_match: str,
    expected_suggestion: str,
) -> None:
    operations = workflow_harness.install()
    applied: list[object] = []

    def prepare_create(_project: object, **_values: object) -> object:
        raise ApiResultError(
            result_code=result_code,
            result_message=result_message,
        )

    monkeypatch.setattr(operations, "prepare_create", prepare_create)
    monkeypatch.setattr(
        operations,
        "apply_create",
        applied.append,
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

    with pytest.raises(error_type, match=error_match) as exc_info:
        workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert exc_info.value.details == {
        "resource": "workflow",
        "project": "etl-prod",
        "project_code": 7,
        "name": "nightly-sync",
        "mutation_applied": False,
    }
    assert exc_info.value.suggestion == expected_suggestion
    assert applied == []
    assert fake_workflow_adapter.create_calls == []


def test_create_workflow_result_dry_run_warns_on_risky_time_parameter_format(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: nightly-sync
  project: etl-prod
  global_params:
    week_key: "$[yyyyww]"
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert result.warnings == [
        "dry run: no mutation was sent; lookup and verification reads may occur",
        "workflow.global_params.week_key contains $[yyyyww]: combining "
        "calendar-year tokens such as yyyy with week tokens such as ww can be "
        "wrong near year boundaries.",
    ]
    assert [detail["code"] for detail in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "parameter_time_format_calendar_year_with_week",
    ]


def test_create_workflow_dry_run_warns_on_sub_workflow_local_params(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
) -> None:
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: parent
  project: etl-prod
tasks:
  - name: invoke-child
    type: SUB_WORKFLOW
    task_params:
      workflowDefinitionCode: 123456789
      localParams:
        - prop: run_label
          value: FROM_PARENT
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)

    assert [detail["code"] for detail in result.warning_details] == [
        "dry_run_no_mutation_sent",
        "sub_workflow_local_params_not_child_inputs",
    ]


def test_create_workflow_result_creates_and_can_online_workflow(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.generated_codes = [7101]
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
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
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path)
    data = _mapping(result.data)

    assert data["name"] == "nightly-sync"
    assert data["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.create_calls[0]["name"] == "nightly-sync"
    assert fake_task_adapter.generate_code_calls == [{"project_code": 7, "count": 1}]
    created_task_definitions = json.loads(
        str(fake_workflow_adapter.create_calls[0]["task_definition_json"])
    )
    assert [task["code"] for task in created_task_definitions] == [7101]
    assert fake_workflow_adapter.release_calls[-1][1] == "ONLINE"
    assert _mapping(result.resolved["workflow"])["source"] == "file"


def test_create_workflow_result_translates_task_code_allocation_error(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.generate_codes_error = ApiResultError(
        result_code=12345,
        result_message="code service unavailable",
    )
    workflow_harness.install()
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

    with pytest.raises(
        ApiTransportError,
        match="could not allocate task codes",
    ) as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.details == {
        "resource": "project",
        "project_code": 7,
        "action": "workflow.create",
        "task_code_count": 1,
        "result_code": 12345,
        "result_message": "code service unavailable",
    }
    assert fake_workflow_adapter.create_calls == []


def test_create_workflow_result_suggests_review_for_remote_validation_error(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    failing_workflow_adapter = replace(
        fake_workflow_adapter,
        create_errors_by_name={
            "nightly-sync": ApiResultError(
                result_code=workflow_types.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=failing_workflow_adapter,
        schedule_adapter=workflow_harness.schedule_adapter,
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

    with pytest.raises(
        UserInputError,
        match="workflow task relation invalid",
    ) as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        "Lint the same workflow file and repeat the create command with --dry-run "
        "to inspect the workflow spec and compiled DS-native payload before "
        "retrying."
    )


def test_create_workflow_result_accepts_confirmation_and_emits_warning_details(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
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

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_service.create_workflow_result(file=spec_path)

    confirmation = _mapping(captured.value.details)["confirmation_token"]
    assert isinstance(confirmation, str)

    result = workflow_service.create_workflow_result(
        file=spec_path,
        confirm_risk=confirmation,
    )

    assert result.warnings
    assert result.warning_details == [
        {
            "code": "confirmed_high_frequency_schedule",
            "message": result.warnings[0],
            "risk_type": "high_frequency_schedule",
            "min_interval_seconds": 300,
            "threshold_seconds": 600,
        }
    ]


def test_create_workflow_result_requires_confirmation_before_any_mutation(
    workflow_harness: _WorkflowServiceHarness,
    tmp_path: Path,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
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

    with pytest.raises(ConfirmationRequiredError):
        workflow_service.create_workflow_result(file=spec_path)

    assert fake_workflow_adapter.create_calls == []
    assert fake_schedule_adapter.schedules == []


@pytest.mark.parametrize("dry_run", [False, True])
def test_create_rejects_existing_name_before_task_allocation_or_mutation(
    workflow_harness: _WorkflowServiceHarness, tmp_path: Path, *, dry_run: bool
) -> None:
    workflow_harness.install()
    source = tmp_path / "duplicate.yaml"
    source.write_text(
        "workflow: {name: daily-sync, project: etl-prod}\n"
        "tasks: [{name: run, type: SHELL, command: echo}]\n"
    )
    with pytest.raises(ConflictError) as captured:
        workflow_service.create_workflow_result(file=source, dry_run=dry_run)
    assert captured.value.details["mutation_applied"] is False
    assert "workflow get daily-sync --project etl-prod" in (
        captured.value.suggestion or ""
    )
    assert workflow_harness.workflow_adapter.create_calls == []
