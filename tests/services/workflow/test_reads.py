"""Workflow selection, inspection, export, hydration, and public read errors."""

import json
from collections.abc import Callable
from dataclasses import replace

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProjectAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowPage,
    FakeWorkflowTaskRelation,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence
from tests.workflow_domain_fakes import (
    install_workflow_domain_runtime,
)

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services import workflow as workflow_service
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services.selection import ResourceDefaults
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.read_models import WorkflowReference


def test_342_workflow_describe_uses_inspection_runtime(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.describe_workflow_result(
        "daily-sync",
        project="etl-prod",
    )

    data = _mapping(result.data)
    assert [_mapping(task)["name"] for task in _sequence(data["tasks"])] == [
        "extract",
        "load",
    ]
    assert len(_sequence(data["relations"])) == 1


def test_342_workflow_digest_uses_inspection_runtime(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.digest_workflow_result(
        "daily-sync",
        project="etl-prod",
    )

    data = _mapping(result.data)
    assert data["taskCount"] == 2
    assert data["relationCount"] == 1


def test_342_workflow_export_uses_inspection_runtime(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        profile=make_profile(ds_version="3.4.2"),
    )

    result = workflow_service.export_workflow_yaml_result(
        "daily-sync",
        project="etl-prod",
    )

    document = yaml.safe_load(str(_mapping(result.data)["yaml"]))
    assert document["workflow"]["name"] == "daily-sync"
    assert [task["name"] for task in document["tasks"]] == ["extract", "load"]


@pytest.mark.parametrize(
    "operation",
    [
        workflow_service.describe_workflow_result,
        workflow_service.digest_workflow_result,
        workflow_service.export_workflow_yaml_result,
    ],
)
def test_workflow_inspection_translates_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    operation: Callable[..., object],
) -> None:
    def fail_describe(*, project_code: int, code: int) -> FakeDag:
        del project_code, code
        raise ApiResultError(
            result_code=30002,
            result_message="user has no workflow permission in this project",
        )

    monkeypatch.setattr(fake_workflow_adapter, "describe", fail_describe)
    workflow_harness.install()

    with pytest.raises(PermissionDeniedError) as exc_info:
        operation("daily-sync", project="etl-prod")

    assert exc_info.value.details == {
        "resource": "workflow",
        "project_code": 7,
        "code": 101,
    }


@pytest.mark.parametrize(
    "operation",
    [
        workflow_service.describe_workflow_result,
        workflow_service.digest_workflow_result,
        workflow_service.export_workflow_yaml_result,
    ],
)
def test_workflow_inspection_translates_not_found_error(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
    operation: Callable[..., object],
) -> None:
    def fail_describe(*, project_code: int, code: int) -> FakeDag:
        del project_code, code
        raise ApiResultError(
            result_code=50003,
            result_message="workflow definition does not exist",
        )

    monkeypatch.setattr(fake_workflow_adapter, "describe", fail_describe)
    workflow_harness.install()

    with pytest.raises(NotFoundError, match="Workflow code 101 was not found"):
        operation("daily-sync", project="etl-prod")


def test_list_workflows_result_uses_project_context_and_filters(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.list_workflows_result(
        search="daily",
        page_no=1,
        page_size=25,
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert result.resolved["page_no"] == 1
    assert result.resolved["page_size"] == 25
    assert result.resolved["all"] is False
    assert list(items) == [
        {
            "code": 101,
            "name": "daily-sync",
            "version": 1,
            "releaseState": "ONLINE",
            "scheduleReleaseState": "ONLINE",
            "scheduleId": 23,
        }
    ]
    assert data["total"] == 1
    assert data["totalPage"] == 1
    assert data["pageSize"] == 25
    assert data["currentPage"] == 1
    assert data["pageNo"] == 1


def test_list_workflows_result_translates_project_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    def fail_list_page(
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeWorkflowPage:
        del project_code, page_no, page_size, search
        raise ApiResultError(
            result_code=30002,
            result_message="user has no workflow permission in this project",
        )

    monkeypatch.setattr(fake_workflow_adapter, "list_page", fail_list_page)
    workflow_harness.install()

    with pytest.raises(PermissionDeniedError) as exc_info:
        workflow_service.list_workflows_result(project="etl-prod")

    assert exc_info.value.details == {
        "resource": "workflow",
        "project_code": 7,
    }
    assert "grant" in (exc_info.value.suggestion or "")


def test_list_workflows_result_translates_deleted_project_error(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    def fail_list_page(
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> FakeWorkflowPage:
        del project_code, page_no, page_size, search
        raise ApiResultError(
            result_code=10190,
            result_message="project does not exist",
        )

    monkeypatch.setattr(fake_workflow_adapter, "list_page", fail_list_page)
    workflow_harness.install()

    with pytest.raises(NotFoundError, match="Project code 7 was not found"):
        workflow_service.list_workflows_result(project="etl-prod")


def test_get_workflow_reuses_detail_loaded_for_versionless_name_reference(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    def versionless_refs(*, project_code: int) -> list[WorkflowReference]:
        assert project_code == 7
        return [WorkflowReference(code=101, name="daily-sync")]

    monkeypatch.setattr(fake_workflow_adapter, "list_refs", versionless_refs)
    workflow_harness.install(
        schedule_adapter=workflow_harness.schedule_adapter,
    )

    result = workflow_service.get_workflow_result(
        "daily-sync",
        project="etl-prod",
    )

    workflow_data = _mapping(result.data)
    resolved_workflow = _mapping(result.resolved["workflow"])
    assert fake_workflow_adapter.get_calls == [101]
    assert resolved_workflow["version"] == workflow_data["version"] == 1


def test_get_workflow_translates_permission_error_during_detail_refresh(
    monkeypatch: pytest.MonkeyPatch,
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    def fail_get(*, project_code: int, code: int) -> FakeWorkflow:
        del project_code, code
        raise ApiResultError(
            result_code=30002,
            result_message="user has no workflow permission in this project",
        )

    monkeypatch.setattr(fake_workflow_adapter, "get", fail_get)
    workflow_harness.install()

    with pytest.raises(PermissionDeniedError) as exc_info:
        workflow_service.get_workflow_result(
            "daily-sync",
            project="etl-prod",
        )

    assert exc_info.value.details == {
        "resource": "workflow",
        "project_code": 7,
        "code": 101,
    }


def test_workflow_read_surfaces_hydrate_attached_schedule_from_schedule_resource(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    raw_workflow = replace(
        fake_workflow_adapter.workflows[0],
        schedule_value=None,
        schedule_release_state_value=None,
    )
    fake_workflow_adapter.workflows[0] = raw_workflow
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=raw_workflow,
    )
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            FakeSchedule(
                id=23,
                workflow_definition_code_value=101,
                workflow_definition_name_value="daily-sync",
                project_code_value=7,
                start_time_value="2026-01-01 00:00:00",
                end_time_value="2026-12-31 23:59:59",
                timezone_id_value="UTC",
                crontab_value="0 0 0 * * ?",
                release_state_value=FakeEnumValue("OFFLINE"),
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    get_result = workflow_service.get_workflow_result("daily-sync")
    describe_result = workflow_service.describe_workflow_result("daily-sync")
    digest_result = workflow_service.digest_workflow_result("daily-sync")
    export_result = workflow_service.export_workflow_yaml_result("daily-sync")

    for data in (
        _mapping(get_result.data),
        _mapping(_mapping(describe_result.data)["workflow"]),
        _mapping(_mapping(digest_result.data)["workflow"]),
    ):
        assert data["scheduleReleaseState"] == "OFFLINE"
        assert _mapping(data["schedule"])["id"] == 23
    exported = yaml.safe_load(str(_mapping(export_result.data)["yaml"]))
    assert exported["schedule"] == {
        "cron": "0 0 0 * * ?",
        "timezone": "UTC",
        "start": "2026-01-01 00:00:00",
        "end": "2026-12-31 23:59:59",
        "release_state": "OFFLINE",
    }
    assert len(schedule_adapter.list_calls) == 4
    assert {call["page_size"] for call in schedule_adapter.list_calls} == {2}


def test_342_workflow_get_schedule_failure_suggests_supported_inspection(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    schedule_adapter = FakeScheduleAdapter(
        schedules=[],
        list_error=ApiResultError(
            result_code=99999,
            result_message="schedule lookup failed",
        ),
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
        profile=make_profile(ds_version="3.4.2"),
    )

    with pytest.raises(ApiTransportError) as captured:
        workflow_service.get_workflow_result(
            "daily-sync",
            project="etl-prod",
        )

    suggestion = captured.value.suggestion
    assert suggestion is not None
    assert "dsctl schedule list --project 7 --workflow 101" in suggestion
    assert "dsctl workflow get" not in suggestion


def test_workflow_export_fails_closed_for_malformed_attached_schedule(
    workflow_harness: _WorkflowServiceHarness,
    fake_workflow_adapter: FakeWorkflowAdapter,
) -> None:
    embedded_schedule = fake_workflow_adapter.workflows[0].schedule
    assert embedded_schedule is not None
    schedule_adapter = FakeScheduleAdapter(
        schedules=[
            replace(
                embedded_schedule,
                workflow_definition_code_value=101,
                project_code_value=7,
                crontab_value=None,
            )
        ]
    )
    workflow_harness.install(
        schedule_adapter=schedule_adapter,
    )

    with pytest.raises(ApiTransportError) as captured:
        workflow_service.export_workflow_yaml_result(
            "daily-sync",
            project="etl-prod",
        )

    assert captured.value.details["invalid_fields"] == ["crontab"]


def test_export_workflow_yaml_result_can_render_yaml_export(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.export_workflow_yaml_result("daily-sync")
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))

    assert _mapping(result.resolved["workflow"])["source"] == "flag"
    assert "workflow:" in str(data["yaml"])
    assert "tasks:" in str(data["yaml"])
    assert all("code" not in task for task in document["tasks"])
    assert all("version" not in task for task in document["tasks"])


def test_export_workflow_yaml_result_rewrites_logical_branch_codes_to_task_names(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    routed = FakeWorkflow(
        code=301,
        name="route-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
    )
    switch_task = FakeTaskDefinition(
        code=401,
        name="route",
        project_code_value=7,
        task_type_value="SWITCH",
        task_params_value=json.dumps(
            {
                "switchResult": {
                    "dependTaskList": [
                        {"condition": '${route} == "A"', "nextNode": 402}
                    ],
                    "nextNode": 403,
                },
                "nextBranch": 402,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        project_name_value="etl-prod",
    )
    task_a = FakeTaskDefinition(
        code=402,
        name="task-a",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo A"}',
        project_name_value="etl-prod",
    )
    task_default = FakeTaskDefinition(
        code=403,
        name="task-default",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo default"}',
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[routed],
        dags={
            301: FakeDag(
                workflow_definition_value=routed,
                task_definition_list_value=[switch_task, task_a, task_default],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=401,
                    ),
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=401,
                        post_task_code_value=402,
                    ),
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=401,
                        post_task_code_value=403,
                    ),
                ],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
    )

    result = workflow_service.export_workflow_yaml_result(
        "route-workflow",
        project="etl-prod",
    )
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))
    switch_params = document["tasks"][0]["task_params"]

    assert switch_params["switchResult"]["dependTaskList"][0]["nextNode"] == "task-a"
    assert switch_params["switchResult"]["nextNode"] == "task-default"
    assert switch_params["nextBranch"] == "task-a"


def test_export_workflow_yaml_result_round_trips_native_opaque_task_params(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    generic_workflow = FakeWorkflow(
        code=302,
        name="spark-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
    )
    spark_task = FakeTaskDefinition(
        code=410,
        name="spark-job",
        project_code_value=7,
        task_type_value="SPARK",
        task_params_value=json.dumps(
            {
                "programType": "JAVA",
                "mainClass": "com.example.jobs.SparkJob",
                "mainJar": {"id": 9},
                "deployMode": "cluster",
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[generic_workflow],
        dags={
            302: FakeDag(
                workflow_definition_value=generic_workflow,
                task_definition_list_value=[spark_task],
                workflow_task_relation_list_value=[],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.export_workflow_yaml_result("spark-workflow")
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))
    spec = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=get_task_authoring_catalog("3.4.1"),
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert spec.tasks[0].type == "SPARK"
    assert spec.tasks[0].task_params == {
        "programType": "JAVA",
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }


def test_describe_workflow_result_returns_tasks_and_relations(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install()

    result = workflow_service.describe_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)
    tasks = _sequence(data["tasks"])
    relations = _sequence(data["relations"])

    assert _mapping(data["workflow"])["code"] == 101
    assert [_mapping(task)["name"] for task in tasks] == ["extract", "load"]
    assert list(relations) == [
        {
            "preTaskCode": 201,
            "preTaskName": "extract",
            "postTaskCode": 202,
            "postTaskName": "load",
        }
    ]


def test_digest_workflow_result_returns_compact_graph_summary(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install()

    result = workflow_service.digest_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)
    tasks = _sequence(data["tasks"])

    assert _mapping(data["workflow"]) == {
        "code": 101,
        "name": "daily-sync",
        "version": 1,
        "projectCode": 7,
        "projectName": "etl-prod",
        "description": "Daily ETL workflow",
        "releaseState": "ONLINE",
        "scheduleReleaseState": "ONLINE",
        "executionType": "PARALLEL",
        "timeout": 30,
        "schedule": {
            "id": 23,
            "startTime": "2026-01-01 00:00:00",
            "endTime": "2026-12-31 23:59:59",
            "timezoneId": "UTC",
            "crontab": "0 0 0 * * ?",
            "failureStrategy": None,
            "workflowInstancePriority": None,
            "releaseState": "ONLINE",
        },
    }
    assert data["taskCount"] == 2
    assert data["relationCount"] == 1
    assert data["taskTypeCounts"] == {"SHELL": 2}
    assert data["globalParamNames"] == ["env"]
    assert data["rootTasks"] == [{"code": 201, "name": "extract"}]
    assert data["leafTasks"] == [{"code": 202, "name": "load"}]
    assert data["isolatedTasks"] == []
    assert list(tasks) == [
        {
            "code": 201,
            "name": "extract",
            "taskType": "SHELL",
            "upstreamTasks": [],
            "downstreamTasks": [{"code": 202, "name": "load"}],
            "isRoot": True,
            "isLeaf": False,
        },
        {
            "code": 202,
            "name": "load",
            "taskType": "SHELL",
            "upstreamTasks": [{"code": 201, "name": "extract"}],
            "downstreamTasks": [],
            "isRoot": False,
            "isLeaf": True,
        },
    ]


def test_export_workflow_yaml_result_exports_task_group_settings(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    grouped = FakeWorkflow(
        code=311,
        name="grouped-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
    )
    grouped_task = FakeTaskDefinition(
        code=411,
        name="grouped-task",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo grouped"}',
        task_group_id_value=12,
        task_group_priority_value=3,
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[grouped],
        dags={
            311: FakeDag(
                workflow_definition_value=grouped,
                task_definition_list_value=[grouped_task],
                workflow_task_relation_list_value=[],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.export_workflow_yaml_result("grouped-workflow")
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))

    assert document["tasks"][0]["task_group_id"] == 12
    assert document["tasks"][0]["task_group_priority"] == 3


def test_export_workflow_yaml_result_exports_extended_task_execution_fields(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow = FakeWorkflow(
        code=312,
        name="runtime-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
    )
    task = FakeTaskDefinition(
        code=412,
        name="runtime-task",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo runtime"}',
        task_group_id_value=12,
        task_group_priority_value=3,
        environment_code_value=42,
        timeout=15,
        timeout_notify_strategy_value=FakeEnumValue("FAILED"),
        flag_value=FakeEnumValue("NO"),
        cpu_quota_value=50,
        memory_max_value=1024,
        project_name_value="etl-prod",
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            312: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[task],
                workflow_task_relation_list_value=[],
            )
        },
    )
    workflow_harness.install(
        workflow_adapter=workflow_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = workflow_service.export_workflow_yaml_result("runtime-workflow")
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))

    assert document["tasks"][0]["flag"] == "NO"
    assert document["tasks"][0]["environment_code"] == 42
    assert document["tasks"][0]["task_group_id"] == 12
    assert document["tasks"][0]["task_group_priority"] == 3
    assert document["tasks"][0]["timeout"] == 15
    assert document["tasks"][0]["timeout_notify_strategy"] == "FAILED"
    assert document["tasks"][0]["cpu_quota"] == 50
    assert document["tasks"][0]["memory_max"] == 1024


def test_list_workflows_result_requires_project_selection(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(),
    )

    with pytest.raises(UserInputError, match="Project is required") as exc_info:
        workflow_service.list_workflows_result()
    assert exc_info.value.suggestion == (
        "Pass --project NAME, or configure a project in the selected context."
    )


def test_get_workflow_result_reports_missing_workflows(
    workflow_harness: _WorkflowServiceHarness,
) -> None:
    workflow_harness.install(
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(NotFoundError, match="was not found"):
        workflow_service.get_workflow_result("missing")
