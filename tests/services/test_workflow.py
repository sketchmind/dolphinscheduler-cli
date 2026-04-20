import json
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreference,
    FakeProjectPreferenceAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeUser,
    FakeUserAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.config import ClusterProfile
from dsctl.context import SessionContext
from dsctl.errors import (
    ApiResultError,
    ConfirmationRequiredError,
    ConflictError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
)
from dsctl.models import WorkflowSpec
from dsctl.services import _workflow_compile as workflow_compile_service
from dsctl.services import runtime as runtime_service
from dsctl.services import workflow as workflow_service


def _install_workflow_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    schedule_adapter: FakeScheduleAdapter | None = None,
    user_adapter: FakeUserAdapter | None = None,
    context: SessionContext | None = None,
    profile: ClusterProfile | None = None,
    project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile() if profile is None else profile,
            context=context,
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
            schedule_adapter=schedule_adapter,
            user_adapter=user_adapter,
            project_preference_adapter=project_preference_adapter,
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod", description="daily jobs")]
    )


@pytest.fixture
def fake_workflow_adapter() -> FakeWorkflowAdapter:
    daily = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        description="Daily ETL workflow",
        global_params_value='[{"prop":"env","value":"prod"}]',
        global_param_map_value={"env": "prod"},
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        timeout=30,
        release_state_value=FakeEnumValue("ONLINE"),
        schedule_release_state_value=FakeEnumValue("ONLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
        schedule_value=FakeSchedule(
            timezone_id_value="UTC",
            crontab_value="0 0 0 * * ?",
            release_state_value=FakeEnumValue("ONLINE"),
        ),
    )
    adhoc = FakeWorkflow(
        code=102,
        name="adhoc-backfill",
        project_code_value=7,
        description="Adhoc ETL workflow",
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
    )
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
    )
    dag = FakeDag(
        workflow_definition_value=daily,
        task_definition_list_value=[extract, load],
        workflow_task_relation_list_value=[
            FakeWorkflowTaskRelation(
                pre_task_code_value=201,
                post_task_code_value=202,
            )
        ],
    )
    return FakeWorkflowAdapter(
        workflows=[daily, adhoc],
        dags={101: dag},
        run_results_by_code={101: [901]},
    )


@pytest.fixture
def fake_task_adapter() -> FakeTaskAdapter:
    return FakeTaskAdapter(
        workflow_tasks={
            101: [
                FakeTaskDefinition(
                    code=201,
                    name="extract",
                    project_code_value=7,
                ),
                FakeTaskDefinition(
                    code=202,
                    name="load",
                    project_code_value=7,
                ),
            ]
        }
    )


@pytest.fixture
def fake_schedule_adapter() -> FakeScheduleAdapter:
    return FakeScheduleAdapter(schedules=[])


def test_list_workflows_result_uses_project_context_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = workflow_service.list_workflows_result(search="daily")
    items = _sequence(result.data)

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert list(items) == [
        {
            "code": 101,
            "name": "daily-sync",
            "version": 1,
        }
    ]


def test_export_workflow_yaml_result_can_render_yaml_export(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.export_workflow_yaml_result(None)
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))

    assert _mapping(result.resolved["workflow"])["source"] == "context"
    assert "workflow:" in str(data["yaml"])
    assert "tasks:" in str(data["yaml"])
    assert all("code" not in task for task in document["tasks"])
    assert all("version" not in task for task in document["tasks"])


def test_export_workflow_yaml_result_rewrites_logical_branch_codes_to_task_names(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
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
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
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


def test_export_workflow_yaml_result_round_trips_generic_task_params(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
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
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="spark-workflow"),
    )

    result = workflow_service.export_workflow_yaml_result(None)
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))
    spec = WorkflowSpec.model_validate(document)

    assert spec.tasks[0].type == "SPARK"
    assert spec.tasks[0].task_params == {
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }


def test_describe_workflow_result_returns_tasks_and_relations(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

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
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

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
            "startTime": None,
            "endTime": None,
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


def test_run_workflow_result_uses_context_selection_and_ui_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            profile=make_profile(),
            context=SessionContext(
                project="etl-prod",
                workflow="daily-sync",
            ),
            workflow_adapter=fake_workflow_adapter,
            task_adapter=fake_task_adapter,
            user_adapter=FakeUserAdapter(
                users=[
                    FakeUser(
                        id=11,
                        user_name_value="alice",
                        email="alice@example.com",
                        tenant_id_value=7,
                        tenant_code_value="tenant-current-user",
                    )
                ]
            ),
        ),
    )

    result = workflow_service.run_workflow_result(None)
    data = _mapping(result.data)

    assert data["workflowInstanceIds"] == [901]
    assert _mapping(result.resolved["project"])["source"] == "context"
    assert _mapping(result.resolved["workflow"])["source"] == "context"
    assert _mapping(result.resolved["worker_group"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["failure_strategy"]) == {
        "value": "CONTINUE",
        "source": "default",
    }
    assert _mapping(result.resolved["warning_type"]) == {
        "value": "NONE",
        "source": "default",
    }
    assert _mapping(result.resolved["workflow_instance_priority"]) == {
        "value": "MEDIUM",
        "source": "default",
    }
    assert _mapping(result.resolved["warning_group_id"]) == {
        "value": None,
        "source": "default",
    }
    assert _mapping(result.resolved["environment_code"]) == {
        "value": None,
        "source": "default",
    }
    assert _mapping(result.resolved["start_params"]) == {
        "names": [],
        "count": 0,
        "source": "default",
    }
    assert result.resolved["execution_dry_run"] is False


def test_run_workflow_result_uses_default_worker_group_without_remote_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            profile=make_profile(),
            context=SessionContext(
                project="etl-prod",
                workflow="daily-sync",
            ),
            workflow_adapter=fake_workflow_adapter,
            task_adapter=fake_task_adapter,
        ),
    )

    result = workflow_service.run_workflow_result(None)

    assert _mapping(result.resolved["worker_group"]) == {
        "value": "default",
        "source": "default",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "default",
        "source": "default",
    }


def test_run_workflow_result_prefers_enabled_project_preference_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        profile=make_profile(),
        context=SessionContext(
            project="etl-prod",
            workflow="daily-sync",
        ),
        user_adapter=FakeUserAdapter(
            users=[
                FakeUser(
                    id=11,
                    user_name_value="alice",
                    email="alice@example.com",
                    tenant_id_value=7,
                    tenant_code_value="tenant-current-user",
                )
            ]
        ),
        project_preference_adapter=FakeProjectPreferenceAdapter(
            project_preferences=[
                FakeProjectPreference(
                    id=5,
                    code=5,
                    project_code_value=7,
                    state=1,
                    preferences_value=(
                        '{"workerGroup":"pref-group","tenant":"tenant-pref",'
                        '"taskPriority":"HIGH","warningType":"FAILURE",'
                        '"alertGroups":12,"environmentCode":44}'
                    ),
                )
            ]
        ),
    )

    result = workflow_service.run_workflow_result(None)

    assert _mapping(result.resolved["worker_group"]) == {
        "value": "pref-group",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["tenant"]) == {
        "value": "tenant-pref",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["workflow_instance_priority"]) == {
        "value": "HIGH",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["warning_type"]) == {
        "value": "FAILURE",
        "source": "project_preference",
    }
    assert _mapping(result.resolved["warning_group_id"]) == {
        "value": 12,
        "source": "project_preference",
    }
    assert _mapping(result.resolved["environment_code"]) == {
        "value": 44,
        "source": "project_preference",
    }


def test_run_workflow_result_applies_explicit_runtime_options(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.run_workflow_result(
        None,
        worker_group="analytics",
        tenant="tenant-prod",
        failure_strategy="end",
        priority="highest",
        warning_type="all",
        warning_group_id=18,
        environment_code=33,
        params=["bizdate=20260415", "region=cn"],
        execution_dry_run=True,
    )

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert fake_workflow_adapter.run_calls[-1] == {
        "project_code": 7,
        "workflow_code": 101,
        "worker_group": "analytics",
        "tenant_code": "tenant-prod",
        "start_node_list": None,
        "task_scope": None,
        "failure_strategy": "END",
        "warning_type": "ALL",
        "workflow_instance_priority": "HIGHEST",
        "warning_group_id": 18,
        "environment_code": 33,
        "start_params": '{"bizdate":"20260415","region":"cn"}',
        "dry_run": True,
    }
    assert _mapping(result.resolved["start_params"]) == {
        "names": ["bizdate", "region"],
        "count": 2,
        "source": "flag",
    }
    assert result.warnings == [
        "DS execution dry-run is enabled; DolphinScheduler will create dry-run "
        "workflow/task instances and skip task plugin trigger execution."
    ]
    assert _mapping(result.warning_details[0])["code"] == "workflow_execution_dry_run"


def test_run_workflow_task_result_starts_selected_task_with_dependency_warning(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.run_workflow_task_result(None, task="extract")

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert _mapping(result.resolved["task"]) == {
        "code": 201,
        "name": "extract",
        "version": 1,
    }
    assert result.resolved["scope"] == "self"
    assert fake_workflow_adapter.run_calls[-1]["start_node_list"] == [201]
    assert fake_workflow_adapter.run_calls[-1]["task_scope"] == "self"
    assert result.warnings == [
        "Dependent downstream nodes may fail if their referenced task, whole "
        "workflow, or scheduled dependency instance has not produced a "
        "successful run; this request starts only the selected task."
    ]
    warning_detail = _mapping(result.warning_details[0])
    assert warning_detail["code"] == "workflow_run_task_dependent_context"
    assert warning_detail["blocking"] is False


def test_run_workflow_result_dry_run_emits_start_request_without_run_call(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.run_workflow_result(
        None,
        params=["bizdate=20260415"],
        dry_run=True,
        execution_dry_run=True,
    )

    data = _mapping(result.data)
    request = _mapping(data["request"])
    form = _mapping(request["form"])
    assert data["dry_run"] is True
    assert request["method"] == "POST"
    assert request["path"] == "/projects/7/executors/start-workflow-instance"
    assert form["workflowDefinitionCode"] == 101
    assert form["taskDependType"] == "TASK_POST"
    assert form["dryRun"] == 1
    assert form["startParams"] == '{"bizdate":"20260415"}'
    assert fake_workflow_adapter.run_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_request_sent",
        "workflow_execution_dry_run",
    ]


def test_run_workflow_task_result_dry_run_uses_selected_task_scope(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.run_workflow_task_result(
        None,
        task="extract",
        scope="self",
        dry_run=True,
    )

    form = _mapping(_mapping(_mapping(result.data)["request"])["form"])
    assert form["startNodeList"] == "201"
    assert form["taskDependType"] == "TASK_ONLY"
    assert form["dryRun"] == 0
    assert fake_workflow_adapter.run_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_request_sent",
        "workflow_run_task_dependent_context",
    ]


def test_run_workflow_task_result_reports_scope_choices(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(UserInputError, match="Task execution scope") as exc_info:
        workflow_service.run_workflow_task_result(None, task="extract", scope="down")

    assert exc_info.value.suggestion == (
        "Pass `--scope self`, `--scope pre`, or `--scope post`."
    )


def test_backfill_workflow_result_uses_range_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.backfill_workflow_result(
        None,
        start="2026-04-01 00:00:00",
        end="2026-04-03 00:00:00",
    )

    assert _mapping(result.data)["workflowInstanceIds"] == [901]
    assert fake_workflow_adapter.backfill_calls[-1] == {
        "project_code": 7,
        "workflow_code": 101,
        "schedule_time": (
            '{"complementStartDate":"2026-04-01 00:00:00",'
            '"complementEndDate":"2026-04-03 00:00:00"}'
        ),
        "run_mode": "RUN_MODE_SERIAL",
        "expected_parallelism_number": 2,
        "complement_dependent_mode": "OFF_MODE",
        "all_level_dependent": False,
        "execution_order": "DESC_ORDER",
        "worker_group": "default",
        "tenant_code": "default",
        "start_node_list": None,
        "task_scope": None,
        "failure_strategy": "CONTINUE",
        "warning_type": "NONE",
        "workflow_instance_priority": "MEDIUM",
        "warning_group_id": None,
        "environment_code": None,
        "start_params": None,
        "dry_run": False,
    }
    backfill_resolved = _mapping(result.resolved["backfill"])
    assert _mapping(backfill_resolved["run_mode"]) == {
        "value": "RUN_MODE_SERIAL",
        "source": "default",
    }
    assert backfill_resolved["schedule_time_mode"] == "range"


def test_backfill_workflow_result_dry_run_can_target_task_and_date_list(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = workflow_service.backfill_workflow_result(
        None,
        dates=["2026-04-01 00:00:00", "2026-04-02 00:00:00"],
        task="extract",
        scope="self",
        run_mode="parallel",
        expected_parallelism_number=4,
        complement_dependent_mode="all",
        all_level_dependent=True,
        execution_order="asc",
        params=["bizdate=20260401"],
        dry_run=True,
        execution_dry_run=True,
    )

    data = _mapping(result.data)
    form = _mapping(_mapping(data["request"])["form"])
    assert data["dry_run"] is True
    assert form["execType"] == "COMPLEMENT_DATA"
    assert form["scheduleTime"] == (
        '{"complementScheduleDateList":"2026-04-01 00:00:00,2026-04-02 00:00:00"}'
    )
    assert form["startNodeList"] == "201"
    assert form["taskDependType"] == "TASK_ONLY"
    assert form["runMode"] == "RUN_MODE_PARALLEL"
    assert form["expectedParallelismNumber"] == 4
    assert form["complementDependentMode"] == "ALL_DEPENDENT"
    assert form["allLevelDependent"] is True
    assert form["executionOrder"] == "ASC_ORDER"
    assert form["startParams"] == '{"bizdate":"20260401"}'
    assert form["dryRun"] == 1
    assert fake_workflow_adapter.backfill_calls == []
    assert [item["code"] for item in result.warning_details] == [
        "dry_run_no_request_sent",
        "workflow_execution_dry_run",
        "workflow_run_task_dependent_context",
    ]


def test_backfill_workflow_result_requires_time_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(UserInputError, match="Workflow backfill requires"):
        workflow_service.backfill_workflow_result(None)


def test_run_workflow_result_maps_offline_definition_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.run_errors_by_code = {
        101: ApiResultError(
            result_code=50014,
            result_message=(
                "start workflow instance error:The workflowDefinition should be online"
            ),
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(InvalidStateError, match="must be online") as exc_info:
        workflow_service.run_workflow_result(None)

    assert exc_info.value.details["resource"] == "workflow"
    assert exc_info.value.details["code"] == 101
    assert exc_info.value.details["required_release_state"] == "ONLINE"
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow online WORKFLOW --project PROJECT`, then retry "
        "`dsctl workflow run`."
    )


def test_create_workflow_result_can_dry_run_compiled_legacy_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7001, 7002])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
    request = _mapping(data["request"])
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
    assert json.loads(str(form["taskDefinitionJson"])) == [
        {
            "code": 7001,
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
            "code": 7002,
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
            "postTaskCode": 7001,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7001,
            "preTaskVersion": 1,
            "postTaskCode": 7002,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_workflow_result_dry_run_warns_on_risky_time_parameter_format(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7101])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
        "dry run: no request was sent",
        "workflow.global_params.week_key contains $[yyyyww]: combining "
        "calendar-year tokens such as yyyy with week tokens such as ww can be "
        "wrong near year boundaries.",
    ]
    assert [detail["code"] for detail in result.warning_details] == [
        "dry_run_no_request_sent",
        "parameter_time_format_calendar_year_with_week",
    ]


def test_create_workflow_result_compiles_switch_branch_names_to_codes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7501, 7502, 7503])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: switch-flow
  project: etl-prod
tasks:
  - name: route
    type: SWITCH
    task_params:
      switchResult:
        dependTaskList:
          - condition: ${route} == "A"
            nextNode: task-a
        nextNode: task-default
  - name: task-a
    type: SHELL
    command: echo A
  - name: task-default
    type: SHELL
    command: echo default
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(_mapping(result.data)["request"])["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    relations = json.loads(str(form["taskRelationJson"]))

    assert json.loads(task_definitions[0]["taskParams"]) == {
        "switchResult": {
            "dependTaskList": [{"condition": '${route} == "A"', "nextNode": 7502}],
            "nextNode": 7503,
        }
    }
    assert relations == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": 7501,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7501,
            "preTaskVersion": 1,
            "postTaskCode": 7502,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7501,
            "preTaskVersion": 1,
            "postTaskCode": 7503,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_workflow_result_compiles_task_group_settings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7551])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: task-group-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    task_group_id: 21
    task_group_priority: 7
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(_mapping(result.data)["request"])["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))

    assert task_definitions == [
        {
            "code": 7551,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {
                    "rawScript": "echo extract",
                    "localParams": [],
                    "resourceList": [],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "YES",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": -1,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "CLOSE",
            "timeoutNotifyStrategy": None,
            "timeout": 0,
            "delayTime": 0,
            "resourceIds": "",
            "taskExecuteType": "BATCH",
            "taskGroupId": 21,
            "taskGroupPriority": 7,
            "cpuQuota": -1,
            "memoryMax": -1,
        }
    ]


def test_create_workflow_result_compiles_extended_task_execution_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7561])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: task-runtime-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
    flag: NO
    environment_code: 42
    task_group_id: 21
    task_group_priority: 7
    timeout: 15
    timeout_notify_strategy: FAILED
    cpu_quota: 50
    memory_max: 1024
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(_mapping(result.data)["request"])["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))

    assert task_definitions == [
        {
            "code": 7561,
            "version": 1,
            "name": "extract",
            "description": "",
            "taskType": "SHELL",
            "taskParams": json.dumps(
                {
                    "rawScript": "echo extract",
                    "localParams": [],
                    "resourceList": [],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "flag": "NO",
            "taskPriority": "MEDIUM",
            "workerGroup": "default",
            "environmentCode": 42,
            "failRetryTimes": 0,
            "failRetryInterval": 0,
            "timeoutFlag": "OPEN",
            "timeoutNotifyStrategy": "FAILED",
            "timeout": 15,
            "delayTime": 0,
            "resourceIds": "",
            "taskExecuteType": "BATCH",
            "taskGroupId": 21,
            "taskGroupPriority": 7,
            "cpuQuota": 50,
            "memoryMax": 1024,
        }
    ]


def test_export_workflow_yaml_result_exports_task_group_settings(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
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
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="grouped-workflow"),
    )

    result = workflow_service.export_workflow_yaml_result(None)
    data = _mapping(result.data)
    document = yaml.safe_load(str(data["yaml"]))

    assert document["tasks"][0]["task_group_id"] == 12
    assert document["tasks"][0]["task_group_priority"] == 3


def test_export_workflow_yaml_result_exports_extended_task_execution_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
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
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod", workflow="runtime-workflow"),
    )

    result = workflow_service.export_workflow_yaml_result(None)
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


def test_create_workflow_result_compiles_conditions_branch_names_to_codes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7601, 7602, 7603, 7604])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: conditions-flow
  project: etl-prod
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: route
    type: CONDITIONS
    depends_on: [extract]
    task_params:
      dependence:
        relation: AND
        dependTaskList:
          - relation: AND
            dependItemList:
              - dependentType: DEPENDENT_ON_TASK
                projectCode: 1
                definitionCode: 1000000000001
                depTaskCode: 1000000000002
                cycle: day
                dateValue: today
                status: SUCCESS
      conditionResult:
        successNode: [on-success]
        failedNode: [on-failed]
  - name: on-success
    type: SHELL
    command: echo success
  - name: on-failed
    type: SHELL
    command: echo failed
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    form = _mapping(_mapping(_mapping(result.data)["request"])["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    relations = json.loads(str(form["taskRelationJson"]))

    assert json.loads(task_definitions[1]["taskParams"]) == {
        "dependence": {
            "relation": "AND",
            "dependTaskList": [
                {
                    "relation": "AND",
                    "dependItemList": [
                        {
                            "dependentType": "DEPENDENT_ON_TASK",
                            "projectCode": 1,
                            "definitionCode": 1000000000001,
                            "depTaskCode": 1000000000002,
                            "cycle": "day",
                            "dateValue": "today",
                            "status": "SUCCESS",
                        }
                    ],
                }
            ],
        },
        "conditionResult": {
            "successNode": [7603],
            "failedNode": [7604],
        },
    }
    assert relations == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": 7601,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7601,
            "preTaskVersion": 1,
            "postTaskCode": 7602,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7602,
            "preTaskVersion": 1,
            "postTaskCode": 7603,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 7602,
            "preTaskVersion": 1,
            "postTaskCode": 7604,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_create_workflow_result_creates_and_can_online_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7101])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
    assert fake_workflow_adapter.release_calls[-1][1] == "ONLINE"
    assert _mapping(result.resolved["workflow"])["source"] == "file"


def test_create_workflow_result_suggests_review_for_remote_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    failing_workflow_adapter = replace(
        fake_workflow_adapter,
        create_errors_by_name={
            "nightly-sync": ApiResultError(
                result_code=workflow_service.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=failing_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
        "Run `dsctl lint workflow FILE` and `dsctl workflow create --file FILE "
        "--dry-run` to inspect the workflow spec and compiled DS-native payload "
        "before retrying."
    )


def test_create_workflow_result_rejects_offline_workflow_schedule_block(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
schedule:
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match=r"workflow\.release_state=ONLINE"):
        workflow_service.create_workflow_result(file=spec_path)


def test_create_workflow_result_rejects_five_field_schedule_cron(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
  cron: "0 2 * * *"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        UserInputError,
        match="Quartz cron expression with 6 or 7 fields",
    ) as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        "Run `dsctl template workflow` to inspect the stable YAML surface, "
        "then run `dsctl lint workflow PATH` before retrying create."
    )


def test_create_workflow_result_can_dry_run_schedule_plan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7301])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: true
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path, dry_run=True)
    data = _mapping(result.data)
    requests = _sequence(data["requests"])

    assert len(requests) == 4
    assert _mapping(requests[2])["path"] == "/v2/schedules"
    assert _mapping(_mapping(requests[2])["json"])["workflowDefinitionCode"] == (
        "<nightly-sync:created_workflow_code>"
    )
    assert _mapping(requests[3])["path"] == (
        "/projects/7/schedules/<nightly-sync:created_schedule_id>/online"
    )
    schedule_preview = _mapping(data["schedule_preview"])
    schedule_confirmation = _mapping(data["schedule_confirmation"])

    assert schedule_preview["count"] == 5
    assert _mapping(schedule_preview["analysis"])["risk_level"] == "none"
    assert schedule_confirmation["required"] is False
    assert schedule_confirmation["token"] is None
    assert result.warnings == ["dry run: no request was sent"]


def test_create_workflow_result_dry_run_returns_confirmed_schedule_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
        dry_run=True,
        confirm_risk=confirmation,
    )
    data = _mapping(result.data)
    schedule_confirmation = _mapping(data["schedule_confirmation"])

    assert schedule_confirmation["required"] is True
    assert schedule_confirmation["token"] == confirmation
    assert schedule_confirmation["confirmFlag"] == f"--confirm-risk {confirmation}"


def test_create_workflow_result_accepts_confirmation_and_emits_warning_details(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7402])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    fake_schedule_adapter.preview_times_value = [
        "2024-01-01 00:00:00",
        "2024-01-01 00:05:00",
        "2024-01-01 00:10:00",
        "2024-01-01 00:15:00",
        "2024-01-01 00:20:00",
    ]
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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


def test_create_workflow_result_can_create_and_online_schedule(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> None:
    codes = iter([7401])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
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
  cron: "0 0 0 * * ?"
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
  enabled: true
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.create_workflow_result(file=spec_path)

    assert _mapping(result.resolved["workflow"])["source"] == "file"
    assert fake_workflow_adapter.release_calls[-1] == (103, "ONLINE")
    assert len(fake_schedule_adapter.schedules) == 1
    assert fake_schedule_adapter.schedules[0].workflowDefinitionCode == 103
    assert fake_schedule_adapter.schedules[0].releaseState is not None
    assert fake_schedule_adapter.schedules[0].releaseState.value == "ONLINE"


def test_create_workflow_result_rejects_dependency_cycles_locally(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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
    depends_on: [load]
  - name: load
    type: SHELL
    command: echo load
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="dependency cycle") as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        "Review task names and task references, then retry with workflow dry-run "
        "before applying the change."
    )


def test_create_workflow_result_reports_unknown_task_dependency_with_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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
    depends_on: [missing]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="depends on unknown task") as exc_info:
        workflow_service.create_workflow_result(file=spec_path)

    assert exc_info.value.suggestion == (
        "Review task names and task references, then retry with workflow dry-run "
        "before applying the change."
    )


def test_online_workflow_result_warns_when_attached_schedule_remains_offline(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    daily = fake_workflow_adapter.workflows[0]
    schedule = daily.schedule
    assert schedule is not None
    offline_workflow = replace(
        daily,
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
        schedule_value=replace(
            schedule,
            release_state_value=FakeEnumValue("OFFLINE"),
        ),
    )
    fake_workflow_adapter.workflows[0] = offline_workflow
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_workflow,
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    result = workflow_service.online_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["releaseState"] == "ONLINE"
    assert fake_workflow_adapter.release_calls[-1] == (101, "ONLINE")
    assert result.warnings == [
        "workflow brought online; any attached schedule remains offline until "
        "`schedule online` is requested"
    ]
    assert result.warning_details == [
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


def test_offline_workflow_result_warns_when_schedule_is_also_taken_offline(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    result = workflow_service.offline_workflow_result(
        "daily-sync",
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["releaseState"] == "OFFLINE"
    assert data["scheduleReleaseState"] == "OFFLINE"
    assert fake_workflow_adapter.release_calls[-1] == (101, "OFFLINE")
    assert result.warnings == [
        "workflow brought offline; any attached schedule is also taken offline"
    ]
    assert result.warning_details == [
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


def test_delete_workflow_result_requires_force() -> None:
    with pytest.raises(UserInputError, match="Workflow deletion requires --force"):
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=False,
        )


def test_delete_workflow_result_returns_deleted_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    result = workflow_service.delete_workflow_result(
        "daily-sync",
        project="etl-prod",
        force=True,
    )
    data = _mapping(result.data)

    assert data["deleted"] is True
    workflow_data = _mapping(data["workflow"])
    assert workflow_data["name"] == "daily-sync"
    assert [workflow.name for workflow in fake_workflow_adapter.workflows] == [
        "adhoc-backfill"
    ]
    assert 101 not in fake_workflow_adapter.dags
    assert result.resolved == {
        "project": {
            "code": 7,
            "name": "etl-prod",
            "description": "daily jobs",
            "source": "flag",
        },
        "workflow": {
            "code": 101,
            "name": "daily-sync",
            "source": "flag",
            "version": 1,
        },
    }


def test_delete_workflow_result_maps_online_state_to_invalid_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=50021,
            result_message="workflow definition [daily-sync] is already online",
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="must be offline before deletion",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, "
        "then retry `dsctl workflow delete --force`."
    )


def test_delete_workflow_result_suggests_schedule_cleanup_for_online_schedule(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=50023,
            result_message="workflow definition [daily-sync] has online schedule",
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="online schedule",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schedule list --workflow WORKFLOW --project PROJECT` to find "
        "the attached schedule, take it offline with `dsctl schedule offline "
        "SCHEDULE_ID`, then retry `dsctl workflow delete --force`."
    )


def test_delete_workflow_result_suggests_instance_inspection_for_running_instances(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=10163,
            result_message="workflow definition [daily-sync] has running instances",
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="running workflow instances",
    ) as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow-instance list --workflow WORKFLOW --project PROJECT` "
        "to inspect active instances, stop or wait for them to finish, then "
        "retry deletion."
    )


def test_delete_workflow_result_maps_dependencies_to_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.delete_errors_by_code = {
        101: ApiResultError(
            result_code=10193,
            result_message="delete workflow definition fail, cause used by other tasks",
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(ConflictError, match="referenced by other tasks") as exc_info:
        workflow_service.delete_workflow_result(
            "daily-sync",
            project="etl-prod",
            force=True,
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect references before retrying deletion."
    )


def test_online_workflow_result_maps_subworkflow_release_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.online_errors_by_code = {
        101: ApiResultError(
            result_code=50004,
            result_message="exist sub workflow definition not online",
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="sub-workflows are already online",
    ) as exc_info:
        workflow_service.online_workflow_result("daily-sync", project="etl-prod")
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry `dsctl workflow online`."
    )


def test_online_workflow_result_maps_wrapped_subworkflow_release_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.online_errors_by_code = {
        101: ApiResultError(
            result_code=10000,
            result_message=(
                "Internal Server Error: SubWorkflowDefinition child is not online"
            ),
        )
    }
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="sub-workflows are already online",
    ) as exc_info:
        workflow_service.online_workflow_result("daily-sync", project="etl-prod")
    assert exc_info.value.suggestion == (
        "Run `dsctl workflow lineage dependent-tasks WORKFLOW --project PROJECT` "
        "to inspect sub-workflow references, bring those sub-workflows online, "
        "then retry `dsctl workflow online`."
    )


def test_list_workflows_result_requires_project_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(),
    )

    with pytest.raises(UserInputError, match="Project is required") as exc_info:
        workflow_service.list_workflows_result()
    assert exc_info.value.suggestion == (
        "Pass --project NAME or run `dsctl use project NAME`."
    )


def test_get_workflow_result_reports_missing_workflows(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(NotFoundError, match="was not found"):
        workflow_service.get_workflow_result("missing")


def test_edit_workflow_result_dry_run_emits_diff_and_update_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    codes = iter([8101, 8102, 8103])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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
    update:
      - match:
          name: load
        set:
          command: echo load v2
    create:
      - name: verify
        type: SHELL
        command: echo verify
        depends_on: [load]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(data["request"])
    form = _mapping(request["form"])
    diff = _mapping(data["diff"])

    assert data["dry_run"] is True
    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/workflow-definition/101"
    assert diff["workflow_updated_fields"] == ["description"]
    assert diff["added_tasks"] == ["verify"]
    assert diff["updated_tasks"] == ["load"]
    assert diff["renamed_tasks"] == [
        {
            "from_name": "extract",
            "to_name": "extract-v2",
        }
    ]
    assert diff["added_edges"] == [
        {
            "from_task": "extract-v2",
            "to_task": "load",
        },
        {
            "from_task": "load",
            "to_task": "verify",
        },
    ]
    assert diff["removed_edges"] == [
        {
            "from_task": "extract",
            "to_task": "load",
        }
    ]
    assert data["workflow_state_constraints"] == [
        (
            "workflow is currently online; DolphinScheduler only allows "
            "whole-definition edits while offline"
        ),
        (
            "taking this workflow offline before apply will also take the "
            "attached schedule offline"
        ),
    ]
    assert data["workflow_state_constraint_details"] == [
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
    assert data["schedule_impacts"] == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately"
    ]
    assert data["schedule_impact_details"] == [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "ONLINE",
        }
    ]
    assert data["no_change"] is False
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    assert [
        (item["name"], item["code"], item["version"]) for item in task_definitions
    ] == [
        ("extract-v2", 201, 1),
        ("load", 202, 1),
        ("verify", 8101, 1),
    ]
    assert json.loads(str(form["taskRelationJson"])) == [
        {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": 201,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 201,
            "preTaskVersion": 1,
            "postTaskCode": 202,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
        {
            "name": "",
            "preTaskCode": 202,
            "preTaskVersion": 1,
            "postTaskCode": 8101,
            "postTaskVersion": 1,
            "conditionType": 0,
            "conditionParams": "{}",
        },
    ]


def test_edit_workflow_result_full_file_dry_run_reconciles_desired_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    codes = iter([8101])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow v2
  timeout: 30
  global_params:
    env: prod
  execution_type: PARALLEL
  release_state: ONLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract v2
  - name: verify
    type: SHELL
    command: echo verify
    depends_on: [extract]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(data["request"])
    form = _mapping(request["form"])
    diff = _mapping(data["diff"])

    assert data["dry_run"] is True
    assert result.resolved["input_mode"] == "file"
    assert result.resolved["file"] == str(workflow_path)
    assert diff["workflow_updated_fields"] == ["description"]
    assert diff["added_tasks"] == ["verify"]
    assert diff["updated_tasks"] == ["extract"]
    assert diff["renamed_tasks"] == []
    assert diff["deleted_tasks"] == ["load"]
    assert diff["added_edges"] == [
        {
            "from_task": "extract",
            "to_task": "verify",
        }
    ]
    assert diff["removed_edges"] == [
        {
            "from_task": "extract",
            "to_task": "load",
        }
    ]
    task_definitions = json.loads(str(form["taskDefinitionJson"]))
    assert [
        (item["name"], item["code"], item["version"]) for item in task_definitions
    ] == [
        ("extract", 201, 1),
        ("verify", 8101, 1),
    ]
    assert form["releaseState"] == "ONLINE"


def test_edit_workflow_result_full_file_requires_confirmation_for_deletion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    offline_workflow = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    workflow_adapter = replace(
        fake_workflow_adapter,
        workflows=[offline_workflow],
        dags={
            **fake_workflow_adapter.dags,
            101: replace(
                fake_workflow_adapter.dags[101],
                workflow_definition_value=offline_workflow,
            ),
        },
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
  project: etl-prod
  description: Daily ETL workflow
  timeout: 30
  execution_type: PARALLEL
  release_state: OFFLINE
tasks:
  - name: extract
    type: SHELL
    command: echo extract
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmationRequiredError) as captured:
        workflow_service.edit_workflow_result(
            "daily-sync",
            file=workflow_path,
            project="etl-prod",
        )

    assert captured.value.details["risk_type"] == (
        "workflow_full_edit_destructive_change"
    )
    assert captured.value.details["deleted_tasks"] == ["load"]
    confirmation = str(captured.value.details["confirmation_token"])

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        file=workflow_path,
        project="etl-prod",
        confirm_risk=confirmation,
    )

    assert _mapping(result.data)["description"] == "Daily ETL workflow"
    assert len(workflow_adapter.update_calls) == 1
    task_definitions = json.loads(
        str(workflow_adapter.update_calls[0]["task_definition_json"])
    )
    assert [(item["name"], item["code"]) for item in task_definitions] == [
        ("extract", 201)
    ]


def test_edit_workflow_result_full_file_rejects_schedule_block(
    tmp_path: Path,
) -> None:
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        """
workflow:
  name: daily-sync
tasks:
  - name: extract
    type: SHELL
    command: echo extract
schedule:
  cron: 0 0 2 * * ?
  timezone: UTC
  start: "2026-01-01 00:00:00"
  end: "2026-12-31 23:59:59"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(UserInputError, match="does not mutate schedule blocks"):
        workflow_service.edit_workflow_result(
            "daily-sync",
            file=workflow_path,
            project="etl-prod",
            dry_run=True,
        )


def test_edit_workflow_result_requires_exactly_one_input_file(
    tmp_path: Path,
) -> None:
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text("patch:\n  workflow:\n    set:\n      timeout: 10\n")
    workflow_path = tmp_path / "workflow.yaml"
    workflow_path.write_text(
        "\n".join(
            [
                "workflow:",
                "  name: daily-sync",
                "tasks:",
                "  - name: extract",
                "    type: SHELL",
                "    command: echo extract",
                "",
            ]
        )
    )

    with pytest.raises(UserInputError, match="exactly one"):
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            file=workflow_path,
            project="etl-prod",
        )


def test_edit_workflow_result_dry_run_warns_on_risky_time_parameter_format(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          command: echo $[YYYYMMdd]
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )

    assert result.warnings == [
        "dry run: no request was sent",
        "tasks[1].command contains $[YYYYMMdd]: uppercase year tokens such as "
        "YYYY use week-based year semantics in DS Java-style time patterns, not "
        "calendar year semantics.",
    ]
    assert [detail["code"] for detail in result.warning_details] == [
        "dry_run_no_request_sent",
        "parameter_time_format_week_year_token",
    ]


def test_edit_workflow_result_dry_run_compiles_extended_task_execution_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    codes = iter([8201, 8202])
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          flag: NO
          environment_code: 42
          task_group_id: 21
          task_group_priority: 7
          timeout: 15
          timeout_notify_strategy: FAILED
          cpu_quota: 50
          memory_max: 1024
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
        dry_run=True,
    )
    data = _mapping(result.data)
    diff = _mapping(data["diff"])
    form = _mapping(_mapping(data["request"])["form"])
    task_definitions = json.loads(str(form["taskDefinitionJson"]))

    assert diff["updated_tasks"] == ["load"]
    assert task_definitions[0]["code"] == 201
    assert task_definitions[0]["version"] == 1
    assert task_definitions[1]["code"] == 202
    assert task_definitions[1]["version"] == 1
    assert task_definitions[1]["flag"] == "NO"
    assert task_definitions[1]["environmentCode"] == 42
    assert task_definitions[1]["taskGroupId"] == 21
    assert task_definitions[1]["taskGroupPriority"] == 7
    assert task_definitions[1]["timeout"] == 15
    assert task_definitions[1]["timeoutFlag"] == "OPEN"
    assert task_definitions[1]["timeoutNotifyStrategy"] == "FAILED"
    assert task_definitions[1]["cpuQuota"] == 50
    assert task_definitions[1]["memoryMax"] == 1024


def test_edit_workflow_result_applies_ds_like_task_version_bumps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    offline_daily = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    fake_workflow_adapter.workflows[0] = offline_daily
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_daily,
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          command: echo load v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    updated_dag = fake_workflow_adapter.dags[101]
    tasks_by_name = {
        str(task.name): task for task in updated_dag.taskDefinitionList or []
    }
    relations = updated_dag.workflowTaskRelationList or []

    assert _mapping(result.data)["version"] == 2
    assert tasks_by_name["extract"].code == 201
    assert tasks_by_name["extract"].version == 1
    assert tasks_by_name["load"].code == 202
    assert tasks_by_name["load"].version == 2
    assert len(relations) == 2
    assert relations[0].preTaskCode == 0
    assert relations[0].preTaskVersion == 0
    assert relations[0].postTaskCode == 201
    assert relations[0].postTaskVersion == 1
    assert relations[1].preTaskCode == 201
    assert relations[1].preTaskVersion == 1
    assert relations[1].postTaskCode == 202
    assert relations[1].postTaskVersion == 2


def test_edit_workflow_result_treats_semantic_default_task_patch_as_no_op(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    workflow = FakeWorkflow(
        code=101,
        name="daily-sync",
        project_code_value=7,
        description="Daily ETL workflow",
        user_id_value=11,
        user_name_value="alice",
        project_name_value="etl-prod",
        timeout=30,
        release_state_value=FakeEnumValue("ONLINE"),
        schedule_release_state_value=FakeEnumValue("ONLINE"),
        execution_type_value=FakeEnumValue("PARALLEL"),
    )
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
        timeout=15,
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[workflow],
        dags={
            101: FakeDag(
                workflow_definition_value=workflow,
                task_definition_list_value=[extract, load],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                    )
                ],
            )
        },
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: load
        set:
          worker_group: default
          timeout_notify_strategy: WARN
          cpu_quota: -1
          memory_max: -1
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )

    assert result.warnings[0] == (
        "patch produced no persistent workflow change; no update request was sent"
    )
    assert workflow_adapter.update_calls == []


def test_edit_workflow_result_reports_invalid_task_patch_as_user_input_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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

    with pytest.raises(UserInputError, match="requires timeout > 0") as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=True,
        )

    assert exc_info.value.suggestion == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_edit_workflow_result_reports_patch_operation_conflict_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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

    with pytest.raises(
        UserInputError,
        match="Patch renames task 'extract' more than once",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
            dry_run=True,
        )

    assert exc_info.value.suggestion == (
        "Fix the workflow patch, then retry `dsctl workflow edit --dry-run` to "
        "inspect the compiled diff before applying it."
    )


def test_edit_workflow_result_requires_offline_before_apply(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
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

    with pytest.raises(InvalidStateError, match="must be offline") as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl workflow offline WORKFLOW --project PROJECT` first, then "
        "retry `dsctl workflow edit`. Review `schedule_impact_detail` before "
        "taking an attached schedule offline."
    )
    assert exc_info.value.details["current_release_state"] == "ONLINE"
    assert "schedule_impact" in exc_info.value.details
    assert exc_info.value.details["constraint_detail"] == {
        "code": "workflow_must_be_offline",
        "message": (
            "workflow is currently online; DolphinScheduler only allows "
            "whole-definition edits while offline"
        ),
        "blocking": True,
        "current_release_state": "ONLINE",
        "required_release_state": "OFFLINE",
        "current_schedule_release_state": "ONLINE",
    }
    assert exc_info.value.details["schedule_impact_detail"] == {
        "code": "offline_also_offlines_attached_schedule",
        "message": (
            "taking this workflow offline before apply will also take the "
            "attached schedule offline"
        ),
        "blocking": False,
        "current_release_state": "ONLINE",
        "required_release_state": "OFFLINE",
        "current_schedule_release_state": "ONLINE",
    }


def test_edit_workflow_result_rejects_invalid_patch_yaml_with_dry_run_suggestion(
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

    with pytest.raises(
        UserInputError,
        match="Workflow patch YAML root must be a mapping",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Fix the patch YAML, then retry the same command with `--dry-run` to "
        "inspect the compiled diff before apply."
    )


def test_edit_workflow_result_suggests_dry_run_for_remote_validation_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    offline_workflow = replace(
        fake_workflow_adapter.workflows[0],
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
    )
    offline_workflow_adapter = replace(
        fake_workflow_adapter,
        workflows=[offline_workflow],
        dags={
            **fake_workflow_adapter.dags,
            101: replace(
                fake_workflow_adapter.dags[101],
                workflow_definition_value=offline_workflow,
            ),
        },
        update_errors_by_code={
            101: ApiResultError(
                result_code=workflow_service.CHECK_WORKFLOW_TASK_RELATION_ERROR,
                result_message="workflow task relation invalid",
            )
        },
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=offline_workflow_adapter,
        task_adapter=fake_task_adapter,
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

    with pytest.raises(
        UserInputError,
        match="workflow task relation invalid",
    ) as exc_info:
        workflow_service.edit_workflow_result(
            "daily-sync",
            patch=patch_path,
            project="etl-prod",
        )

    assert exc_info.value.suggestion == (
        "Retry with `dsctl workflow edit --dry-run` to inspect the compiled diff "
        "and DS-native payload before sending it again."
    )


def test_edit_workflow_result_warns_when_patch_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
    )
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

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["description"] == "Daily ETL workflow"
    assert result.warnings == [
        "patch produced no persistent workflow change; no update request was sent",
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately",
    ]
    assert result.warning_details == [
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
    assert fake_workflow_adapter.update_calls == []


def test_edit_workflow_result_emits_schedule_impact_warning_details_after_apply(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    daily = fake_workflow_adapter.workflows[0]
    schedule = daily.schedule
    assert schedule is not None
    offline_workflow = replace(
        daily,
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
        schedule_value=replace(
            schedule,
            release_state_value=FakeEnumValue("OFFLINE"),
        ),
    )
    fake_workflow_adapter.workflows[0] = offline_workflow
    fake_workflow_adapter.dags[101] = replace(
        fake_workflow_adapter.dags[101],
        workflow_definition_value=offline_workflow,
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
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

    result = workflow_service.edit_workflow_result(
        "daily-sync",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)

    assert data["description"] == "Daily ETL workflow v2"
    assert result.warnings == [
        "workflow edit does not modify the attached schedule; use "
        "`schedule update|online|offline` separately"
    ]
    assert result.warning_details == [
        {
            "code": "attached_schedule_not_modified",
            "message": (
                "workflow edit does not modify the attached schedule; use "
                "`schedule update|online|offline` separately"
            ),
            "desired_workflow_release_state": None,
            "current_schedule_release_state": "OFFLINE",
        }
    ]
    assert fake_workflow_adapter.update_calls[-1]["workflow_code"] == 101


def test_edit_workflow_result_rewrites_switch_refs_after_task_rename(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    routed = FakeWorkflow(
        code=301,
        name="route-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
        schedule_release_state_value=FakeEnumValue("OFFLINE"),
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
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - from: task-a
        to: task-a-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "route-workflow",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)
    yaml_result = workflow_service.export_workflow_yaml_result(
        "301",
        project="etl-prod",
    )
    yaml_data = _mapping(yaml_result.data)
    document = yaml.safe_load(str(yaml_data["yaml"]))
    switch_params = document["tasks"][0]["task_params"]

    assert data["name"] == "route-workflow"
    assert workflow_adapter.update_calls[-1]["workflow_code"] == 301
    assert switch_params["switchResult"]["dependTaskList"][0]["nextNode"] == "task-a-v2"
    assert switch_params["switchResult"]["nextNode"] == "task-default"
    assert switch_params["nextBranch"] == "task-a-v2"


def test_edit_workflow_result_preserves_generic_task_params_shape_after_apply(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    generic_workflow = FakeWorkflow(
        code=302,
        name="spark-workflow",
        project_code_value=7,
        project_name_value="etl-prod",
        release_state_value=FakeEnumValue("OFFLINE"),
    )
    spark_task = FakeTaskDefinition(
        code=410,
        name="spark-job",
        project_code_value=7,
        task_type_value="SPARK",
        task_params_value=json.dumps(
            {
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
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=0,
                        post_task_code_value=410,
                    )
                ],
            )
        },
    )
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=fake_task_adapter,
    )
    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    update:
      - match:
          name: spark-job
        set:
          task_params:
            mainClass: com.example.jobs.SparkJobV2
            mainJar:
              id: 10
            deployMode: client
""".strip(),
        encoding="utf-8",
    )

    result = workflow_service.edit_workflow_result(
        "spark-workflow",
        patch=patch_path,
        project="etl-prod",
    )
    data = _mapping(result.data)
    yaml_result = workflow_service.export_workflow_yaml_result(
        "spark-workflow",
        project="etl-prod",
    )
    yaml_data = _mapping(yaml_result.data)
    document = yaml.safe_load(str(yaml_data["yaml"]))
    spec = WorkflowSpec.model_validate(document)

    assert data["name"] == "spark-workflow"
    assert workflow_adapter.update_calls[-1]["workflow_code"] == 302
    assert spec.tasks[0].type == "SPARK"
    assert spec.tasks[0].task_params == {
        "mainClass": "com.example.jobs.SparkJobV2",
        "mainJar": {"id": 10},
        "deployMode": "client",
    }


def test_workflow_create_get_edit_roundtrip_preserves_switch_and_generic_tasks(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    codes = iter(
        [
            9101,
            9102,
            9103,
            9104,
            9201,
            9202,
            9203,
            9204,
            9301,
            9302,
            9303,
            9304,
        ]
    )
    monkeypatch.setattr(workflow_compile_service, "gen_code", lambda: next(codes))
    workflow_adapter = FakeWorkflowAdapter(workflows=[], dags={})
    _install_workflow_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=FakeTaskAdapter(workflow_tasks={}),
    )
    spec_path = tmp_path / "workflow.yaml"
    spec_path.write_text(
        """
workflow:
  name: roundtrip-flow
  project: etl-prod
  description: Roundtrip workflow
tasks:
  - name: extract
    type: SHELL
    command: echo extract
  - name: route
    type: SWITCH
    depends_on: [extract]
    task_params:
      switchResult:
        dependTaskList:
          - condition: ${route} == "spark"
            nextNode: spark-job
        nextNode: fallback
  - name: spark-job
    type: SPARK
    task_params:
      mainClass: com.example.jobs.SparkJob
      mainJar:
        id: 9
      deployMode: cluster
  - name: fallback
    type: SHELL
    command: echo fallback
""".strip(),
        encoding="utf-8",
    )

    created = workflow_service.create_workflow_result(file=spec_path)
    created_data = _mapping(created.data)
    assert created_data["name"] == "roundtrip-flow"

    exported = workflow_service.export_workflow_yaml_result(
        "roundtrip-flow",
        project="etl-prod",
    )
    exported_data = _mapping(exported.data)
    exported_document = yaml.safe_load(str(exported_data["yaml"]))
    exported_spec = WorkflowSpec.model_validate(exported_document)

    assert exported_spec.tasks[1].type == "SWITCH"
    assert exported_spec.tasks[1].task_params == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": "spark-job",
                }
            ],
            "nextNode": "fallback",
        }
    }
    assert exported_spec.tasks[2].type == "SPARK"
    assert exported_spec.tasks[2].task_params == {
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }

    exported_path = tmp_path / "exported.workflow.yaml"
    exported_path.write_text(str(exported_data["yaml"]), encoding="utf-8")
    dry_run = workflow_service.create_workflow_result(file=exported_path, dry_run=True)
    dry_run_form = _mapping(_mapping(_mapping(dry_run.data)["request"])["form"])
    dry_run_definitions = json.loads(str(dry_run_form["taskDefinitionJson"]))

    assert json.loads(dry_run_definitions[1]["taskParams"]) == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": 9203,
                }
            ],
            "nextNode": 9204,
        }
    }
    assert json.loads(dry_run_definitions[2]["taskParams"]) == {
        "mainClass": "com.example.jobs.SparkJob",
        "mainJar": {"id": 9},
        "deployMode": "cluster",
    }

    patch_path = tmp_path / "workflow.patch.yaml"
    patch_path.write_text(
        """
patch:
  tasks:
    rename:
      - from: spark-job
        to: spark-job-v2
    update:
      - match:
          name: spark-job
        set:
          task_params:
            mainClass: com.example.jobs.SparkJobV2
            mainJar:
              id: 10
            deployMode: client
""".strip(),
        encoding="utf-8",
    )

    updated = workflow_service.edit_workflow_result(
        "roundtrip-flow",
        patch=patch_path,
        project="etl-prod",
    )
    updated_data = _mapping(updated.data)
    assert updated_data["name"] == "roundtrip-flow"

    updated_export = workflow_service.export_workflow_yaml_result(
        "roundtrip-flow",
        project="etl-prod",
    )
    updated_document = yaml.safe_load(str(_mapping(updated_export.data)["yaml"]))
    updated_spec = WorkflowSpec.model_validate(updated_document)

    assert updated_spec.tasks[1].task_params == {
        "switchResult": {
            "dependTaskList": [
                {
                    "condition": '${route} == "spark"',
                    "nextNode": "spark-job-v2",
                }
            ],
            "nextNode": "fallback",
        }
    }
    assert updated_spec.tasks[2].name == "spark-job-v2"
    assert updated_spec.tasks[2].task_params == {
        "mainClass": "com.example.jobs.SparkJobV2",
        "mainJar": {"id": 10},
        "deployMode": "client",
    }
