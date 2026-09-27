import json
from contextlib import nullcontext
from typing import cast

import pytest
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
    fake_task_definition_service_runtime,
)
from tests.request_assertions import first_dry_run_request
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence
from tests.workflow_domain_fakes import (
    _FakeWorkflowOperations,
    install_workflow_domain_runtime,
)

from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    UserInputError,
)
from dsctl.output import result_payload
from dsctl.output_formats import RenderOptions, render_command
from dsctl.services import runtime as runtime_service
from dsctl.services import task as task_service
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services.selection import ResourceDefaults
from dsctl.services.task_authoring_catalog import get_task_authoring_catalog
from dsctl.upstream.legacy_task_definitions import (
    LegacyTaskDefinitions,
    LegacyWorkflowOperations,
)


def _install_task_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    context: ResourceDefaults | None = None,
) -> None:
    profile = make_profile()
    monkeypatch.setattr(
        runtime_service,
        "open_task_definition_service_runtime",
        lambda env_file=None: fake_task_definition_service_runtime(
            project_adapter,
            profile=profile,
            context=context,
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
        ),
    )


def _install_legacy_task_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    context: ResourceDefaults,
) -> _FakeWorkflowOperations:
    profile = make_profile(ds_version="1.3.9")
    operations = install_workflow_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=context,
        profile=profile,
    )
    task_runtime = runtime_service.TaskDefinitionServiceRuntime(
        profile=profile,
        context=context,
        definitions=LegacyTaskDefinitions(
            profile_version="1.3.9",
            operations=cast("LegacyWorkflowOperations", operations),
            catalog=get_task_authoring_catalog("1.3.9"),
            compile_update=prepare_legacy_workflow_mutation_plan,
        ),
    )
    monkeypatch.setattr(
        runtime_service,
        "open_task_definition_service_runtime",
        lambda env_file=None: nullcontext(task_runtime),
    )
    return operations


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_workflow_adapter() -> FakeWorkflowAdapter:
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
        flag_value=FakeEnumValue("YES"),
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
        flag_value=FakeEnumValue("YES"),
    )
    return FakeWorkflowAdapter(
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
                task_definition_list_value=[extract, load],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                        pre_task_version_value=1,
                        post_task_version_value=1,
                    )
                ],
            )
        },
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
            ]
        }
    )


def test_list_tasks_result_uses_project_default_explicit_workflow_and_filters(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.list_tasks_result(workflow="daily-sync", search="extract")
    items = _sequence(result.data)

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert _mapping(result.resolved["workflow"])["source"] == "flag"
    assert list(items) == [
        {
            "code": 201,
            "name": "extract",
            "version": 1,
        }
    ]


@pytest.mark.parametrize("result_code", [30002, 1400001])
def test_list_tasks_result_translates_workflow_read_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    result_code: int,
) -> None:
    fake_workflow_adapter.get_errors_by_call[1] = ApiResultError(
        result_code=result_code,
        result_message="upstream permission failure",
    )
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(PermissionDeniedError, match="Task list requires read") as exc:
        task_service.list_tasks_result(
            workflow="daily-sync",
        )

    assert exc.value.details["result_code"] == result_code


def test_list_tasks_result_translates_disappeared_workflow_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_workflow_adapter.get_errors_by_call[1] = ApiResultError(
        result_code=50003,
        result_message="upstream workflow missing",
    )
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(NotFoundError, match="Workflow 'daily-sync' was not found"):
        task_service.list_tasks_result(
            workflow="daily-sync",
        )


def test_get_task_result_returns_one_task_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.get_task_result("extract", workflow="daily-sync")
    data = _mapping(result.data)

    assert result.resolved["task"] == {
        "code": 201,
        "name": "extract",
        "version": 1,
    }
    assert data["code"] == 201
    assert data["taskType"] == "SHELL"


def test_139_task_list_and_get_use_name_selector_and_id_native_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, id=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
                user_id_value=11,
            )
        ],
        dags={},
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={})
    context = ResourceDefaults(project="etl-prod")
    operations = _install_legacy_task_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=context,
    )
    process_definition_json = json.dumps(
        {
            "globalParams": [],
            "tasks": [
                {
                    "id": "extract-native-id",
                    "name": "extract",
                    "type": "SHELL",
                    "description": "extract source",
                    "params": {
                        "rawScript": "echo extract",
                        "resourceList": [],
                        "localParams": [],
                        "futureParam": "keep",
                    },
                    "preTasks": [],
                    "runFlag": "NORMAL",
                    "maxRetryTimes": 0,
                    "retryInterval": 1,
                    "taskInstancePriority": "MEDIUM",
                    "workerGroup": "default",
                }
            ],
            "timeout": 0,
            "tenantId": 4,
        },
        separators=(",", ":"),
    )
    locations = json.dumps(
        {
            "extract-native-id": {
                "name": "extract",
                "targetarr": "",
                "nodenumber": 0,
                "x": 10,
                "y": 20,
            }
        },
        separators=(",", ":"),
    )
    operations.legacy_definitions[101] = (process_definition_json, locations, "[]")

    listed = task_service.list_tasks_result(workflow="daily-sync", search="tract")
    read = task_service.get_task_result("extract", workflow="daily-sync")

    assert listed.data == [{"id": "extract-native-id", "name": "extract"}]
    assert listed.resolved["project"] == {
        "id": 7,
        "name": "etl-prod",
        "description": None,
        "source": "context",
    }
    assert listed.resolved["workflow"] == {
        "id": 101,
        "name": "daily-sync",
        "version": 1,
        "source": "flag",
    }
    assert read.resolved["task"] == {
        "id": "extract-native-id",
        "name": "extract",
    }
    assert _mapping(read.data)["taskParams"] == {
        "rawScript": "echo extract",
        "resourceList": [],
        "localParams": [],
        "futureParam": "keep",
    }
    assert "code" not in _mapping(read.data)
    assert "version" not in _mapping(read.data)


def test_139_task_update_dry_run_and_apply_use_whole_workflow_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, id=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                id=101,
                name="daily-sync",
                project_code_value=7,
                project_name_value="etl-prod",
                user_id_value=11,
            )
        ],
        dags={},
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={})
    context = ResourceDefaults(project="etl-prod")
    operations = _install_legacy_task_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=context,
    )
    process_definition_json = json.dumps(
        {
            "globalParams": [],
            "tasks": [
                {
                    "id": "extract-native-id",
                    "name": "extract",
                    "type": "SHELL",
                    "description": "",
                    "params": {
                        "rawScript": "echo extract",
                        "resourceList": [],
                        "localParams": [],
                        "futureParam": "keep",
                    },
                    "preTasks": [],
                    "runFlag": "NORMAL",
                    "maxRetryTimes": 0,
                    "retryInterval": 1,
                    "taskInstancePriority": "MEDIUM",
                    "workerGroup": "default",
                    "futureTaskField": "keep",
                }
            ],
            "timeout": 0,
            "tenantId": 4,
        },
        separators=(",", ":"),
    )
    locations = json.dumps(
        {
            "extract-native-id": {
                "name": "extract",
                "targetarr": "",
                "nodenumber": 0,
                "x": 10,
                "y": 20,
            }
        },
        separators=(",", ":"),
    )
    operations.legacy_definitions[101] = (process_definition_json, locations, "[]")

    dry_run = task_service.update_task_result(
        "extract",
        workflow="daily-sync",
        set_values=["command=echo changed", "retry.times=2"],
        dry_run=True,
    )
    request = _mapping(first_dry_run_request(_mapping(dry_run.data)))
    form = _mapping(request["form"])
    dry_process = json.loads(str(form["processDefinitionJson"]))

    assert request["method"] == "POST"
    assert request["path"] == "/projects/etl-prod/process/update"
    assert dry_run.resolved["task"] == {
        "id": "extract-native-id",
        "name": "extract",
    }
    assert _mapping(dry_run.data)["changes"] == [
        {
            "field": "command",
            "before": "echo extract",
            "after": "echo changed",
        },
        {
            "field": "retry.times",
            "before": 0,
            "after": 2,
        },
    ]
    assert dry_process["tasks"][0]["params"] == {
        "rawScript": "echo changed",
        "resourceList": [],
        "localParams": [],
        "futureParam": "keep",
    }
    assert dry_process["tasks"][0]["futureTaskField"] == "keep"
    assert task_adapter.generate_code_calls == []

    applied = task_service.update_task_result(
        "extract",
        workflow="daily-sync",
        set_values=["command=echo changed", "retry.times=2"],
    )

    assert _mapping(applied.data)["taskParams"] == {
        "rawScript": "echo changed",
        "resourceList": [],
        "localParams": [],
        "futureParam": "keep",
    }
    assert _mapping(applied.data)["failRetryTimes"] == 2
    assert task_adapter.generate_code_calls == []


@pytest.mark.parametrize("result_code", [30002, 1400001])
def test_get_task_result_translates_task_read_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    result_code: int,
) -> None:
    fake_task_adapter.get_errors_by_code = {
        201: ApiResultError(
            result_code=result_code,
            result_message="upstream permission failure",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(PermissionDeniedError, match="requires read permission") as exc:
        task_service.get_task_result("extract", workflow="daily-sync")

    assert exc.value.details["result_code"] == result_code


def test_get_task_result_translates_task_detail_not_found_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.get_errors_by_code = {
        201: ApiResultError(
            result_code=50030,
            result_message="upstream task missing",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(NotFoundError, match="Task 'extract' was not found") as exc:
        task_service.get_task_result("extract", workflow="daily-sync")

    assert exc.value.details["result_code"] == 50030


def test_list_tasks_result_requires_workflow_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Workflow is required") as exc_info:
        task_service.list_tasks_result()
    assert exc_info.value.suggestion == "Pass --workflow NAME."


def test_list_tasks_explicit_project_still_requires_workflow(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Workflow is required"):
        task_service.list_tasks_result(project="etl-prod")


def test_get_task_result_reports_missing_tasks(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(NotFoundError, match="was not found"):
        task_service.get_task_result("missing", workflow="daily-sync")


@pytest.mark.parametrize("result_code", [30002, 1400001])
def test_update_task_result_translates_prepare_read_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    result_code: int,
) -> None:
    fake_task_adapter.get_errors_by_code = {
        202: ApiResultError(
            result_code=result_code,
            result_message="upstream permission failure",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(PermissionDeniedError, match="requires read permission") as exc:
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["command=echo changed"],
        )

    assert exc.value.details["result_code"] == result_code
    assert fake_task_adapter.update_calls == []


def test_update_task_result_dry_run_emits_native_update_request(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=[
            "command=echo load v2",
            "retry.times=3",
            "priority=HIGH",
        ],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))
    task_params = _mapping(json.loads(str(payload["taskParams"])))

    assert data["dry_run"] is True
    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/task-definition/202/with-upstream"
    assert form["upstreamCodes"] == "201"
    assert data["changes"] == [
        {
            "field": "command",
            "before": "echo load",
            "after": "echo load v2",
        },
        {
            "field": "retry.times",
            "before": 0,
            "after": 3,
        },
        {
            "field": "priority",
            "before": None,
            "after": "HIGH",
        },
    ]
    assert "updated_fields" not in data
    assert data["no_change"] is False
    dry_run_warning = (
        "dry run: no mutation was sent; lookup and verification reads may occur"
    )
    assert result.warnings == [dry_run_warning]
    assert result.warning_details == [
        {
            "code": "dry_run_no_mutation_sent",
            "message": dry_run_warning,
            "mutation_sent": False,
        }
    ]
    assert payload["taskPriority"] == "HIGH"
    assert payload["failRetryTimes"] == 3
    assert task_params["rawScript"] == "echo load v2"


def test_update_task_default_preview_shows_validated_changes_without_native_body(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )
    command = '  printf "updated task"  \n'
    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=[
            f"command={command}",
            "timeout=10",
            "timeout_notify_strategy=FAILED",
            "retry.times=3",
            "worker_group=  batch  ",
            "description=",
        ],
        dry_run=True,
    )
    rendered = render_command(
        result_payload("task.update", result),
        action="task.update",
        options=RenderOptions(),
    )
    payload = _mapping(json.loads(rendered.stdout))
    data = _mapping(payload["data"])

    assert data["changes"] == [
        {"field": "command", "before": "echo load", "after": command},
        {"field": "timeout", "before": 0, "after": 10},
        {
            "field": "timeout_notify_strategy",
            "before": None,
            "after": "FAILED",
        },
        {"field": "retry.times", "before": 0, "after": 3},
        {"field": "worker_group", "before": None, "after": "batch"},
    ]
    assert "requests" not in data
    assert data["execution_order"] == [
        {"method": "PUT", "path": "/projects/7/task-definition/202/with-upstream"}
    ]
    assert fake_task_adapter.update_calls == []


def test_update_task_result_preserves_exact_command_text(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )
    command = '  printf "true"  \n'

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=[f"command={command}"],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))
    task_params = _mapping(json.loads(str(payload["taskParams"])))

    assert task_params["rawScript"] == command


def test_update_remote_shell_command_preserves_existing_connection_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    remote = FakeTaskDefinition(
        code=203,
        name="remote",
        project_code_value=7,
        task_type_value="REMOTESHELL",
        task_params_value=('{"rawScript":"echo old","type":"SSH","datasource":17}'),
        project_name_value="etl-prod",
        flag_value=FakeEnumValue("YES"),
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
                task_definition_list_value=[remote],
                workflow_task_relation_list_value=[],
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: [remote]})
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "remote",
        workflow="daily-sync",
        set_values=["command=echo new"],
        dry_run=True,
    )
    no_change = task_service.update_task_result(
        "remote",
        workflow="daily-sync",
        set_values=["command=echo old"],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))
    task_params = _mapping(json.loads(str(payload["taskParams"])))

    no_change_data = _mapping(no_change.data)
    assert no_change_data["no_change"] is True
    assert no_change_data["requests"] == []
    assert task_params == {
        "rawScript": "echo new",
        "type": "SSH",
        "datasource": 17,
    }


def test_update_task_result_applies_native_task_update(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=["command=echo load v2"],
    )
    data = _mapping(result.data)
    task_params = _mapping(json.loads(str(data["taskParams"])))

    assert data["code"] == 202
    assert data["version"] == 2
    assert task_params["rawScript"] == "echo load v2"
    assert len(fake_task_adapter.update_calls) == 1
    assert fake_task_adapter.update_calls[0]["project_code"] == 7
    assert fake_task_adapter.update_calls[0]["code"] == 202
    assert fake_task_adapter.update_calls[0]["upstream_codes"] == [201]


def test_update_task_result_reports_schema_suggestion_for_unsupported_set_key(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="Unsupported task update field",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            set_values=["unknown=1"],
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --command task.update` and inspect "
        "set.supported_keys. For structural definition changes, use `dsctl "
        "workflow edit --patch|--file`; for finished instance repair, use "
        "`dsctl workflow-instance edit --patch|--file`."
    )


def test_update_task_result_suggests_schema_for_invalid_timeout_notify_strategy(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="timeout_notify_strategy requires timeout > 0",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["timeout_notify_strategy=FAILED"],
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --command task.update` and inspect "
        "set.supported_keys. For structural definition changes, use `dsctl "
        "workflow edit --patch|--file`; for finished instance repair, use "
        "`dsctl workflow-instance edit --patch|--file`."
    )


def test_update_task_result_dry_run_supports_extended_execution_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=[
            "flag=NO",
            "environment_code=42",
            "task_group_id=12",
            "task_group_priority=3",
            "cpu_quota=50",
            "memory_max=1024",
            "timeout=10",
            "timeout_notify_strategy=FAILED",
        ],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))

    assert [
        _mapping(item)["field"] for item in _sequence(_mapping(result.data)["changes"])
    ] == [
        "flag",
        "environment_code",
        "task_group_id",
        "task_group_priority",
        "cpu_quota",
        "memory_max",
        "timeout",
        "timeout_notify_strategy",
    ]
    assert _mapping(_sequence(_mapping(result.data)["changes"])[0]) == {
        "field": "flag",
        "before": "YES",
        "after": "NO",
    }
    assert payload["flag"] == "NO"
    assert payload["environmentCode"] == 42
    assert payload["taskGroupId"] == 12
    assert payload["taskGroupPriority"] == 3
    assert payload["cpuQuota"] == 50
    assert payload["memoryMax"] == 1024
    assert payload["timeout"] == 10
    assert payload["timeoutFlag"] == "OPEN"
    assert payload["timeoutNotifyStrategy"] == "FAILED"


def test_update_task_result_can_clear_execution_fields_to_ds_defaults(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
        flag_value=FakeEnumValue("YES"),
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
        worker_group_value="analytics",
        environment_code_value=42,
        task_group_id_value=12,
        task_group_priority_value=3,
        cpu_quota_value=50,
        memory_max_value=1024,
        flag_value=FakeEnumValue("YES"),
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
                task_definition_list_value=[extract, load],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                        pre_task_version_value=1,
                        post_task_version_value=1,
                    )
                ],
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: [extract, load]})
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=[
            "worker_group=",
            "environment_code=",
            "task_group_id=",
            "cpu_quota=",
            "memory_max=",
        ],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))

    assert payload["workerGroup"] == "default"
    assert payload["environmentCode"] == -1
    assert payload["taskGroupId"] == 0
    assert payload["taskGroupPriority"] == 0
    assert payload["cpuQuota"] == -1
    assert payload["memoryMax"] == -1


def test_update_task_result_opens_timeout_with_default_notify_strategy(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=["timeout=10"],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(first_dry_run_request(data))
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))

    assert payload["timeout"] == 10
    assert payload["timeoutFlag"] == "OPEN"
    assert payload["timeoutNotifyStrategy"] == "WARN"


def test_update_task_result_treats_warn_timeout_strategy_as_semantic_no_op(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    extract = FakeTaskDefinition(
        code=201,
        name="extract",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo extract"}',
        project_name_value="etl-prod",
        flag_value=FakeEnumValue("YES"),
    )
    load = FakeTaskDefinition(
        code=202,
        name="load",
        project_code_value=7,
        task_type_value="SHELL",
        task_params_value='{"rawScript":"echo load"}',
        project_name_value="etl-prod",
        timeout=15,
        flag_value=FakeEnumValue("YES"),
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
                task_definition_list_value=[extract, load],
                workflow_task_relation_list_value=[
                    FakeWorkflowTaskRelation(
                        pre_task_code_value=201,
                        post_task_code_value=202,
                        pre_task_version_value=1,
                        post_task_version_value=1,
                    )
                ],
            )
        },
    )
    task_adapter = FakeTaskAdapter(workflow_tasks={101: [extract, load]})
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=workflow_adapter,
        task_adapter=task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=["timeout_notify_strategy=WARN"],
        dry_run=True,
    )
    data = _mapping(result.data)

    assert data["changes"] == []
    assert data["no_change"] is True


def test_update_task_result_rejects_timeout_notify_strategy_without_timeout(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="timeout > 0"):
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["timeout_notify_strategy=FAILED"],
            dry_run=True,
        )


def test_update_task_result_warns_when_update_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    result = task_service.update_task_result(
        "load",
        workflow="daily-sync",
        set_values=["command=echo load"],
    )
    data = _mapping(result.data)

    assert data["code"] == 202
    assert result.warnings == ["task update: no persistent changes detected"]
    assert result.warning_details == [
        {
            "code": "task_update_no_persistent_change",
            "message": "task update: no persistent changes detected",
            "no_change": True,
            "request_sent": False,
        }
    ]
    assert fake_task_adapter.update_calls == []


def test_update_task_result_maps_invalid_state_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.update_errors_by_code = {
        202: ApiResultError(
            result_code=50056,
            result_message="task state does not support modification",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        InvalidStateError,
        match="does not support modification",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["command=echo load v2"],
        )
    assert exc_info.value.suggestion == (
        "Inspect the containing workflow definition state; if the workflow is "
        "online, bring it offline before retrying `task update`."
    )


def test_update_task_result_maps_project_write_permission_errors(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.update_errors_by_code = {
        202: ApiResultError(
            result_code=30003,
            result_message="project write permission denied",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        PermissionDeniedError,
        match="write permission for the selected project",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["command=echo load v2"],
        )
    assert exc_info.value.details["result_code"] == 30003


def test_update_task_result_reports_schema_suggestion_for_remote_no_change_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
) -> None:
    fake_task_adapter.update_errors_by_code = {
        202: ApiResultError(
            result_code=50057,
            result_message="no persisted change",
        )
    }
    _install_task_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        context=ResourceDefaults(project="etl-prod"),
    )

    with pytest.raises(
        UserInputError,
        match="Task update did not change any persisted fields",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            workflow="daily-sync",
            set_values=["command=echo load v2"],
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schema --command task.update` and inspect "
        "set.supported_keys. For structural definition changes, use `dsctl "
        "workflow edit --patch|--file`; for finished instance repair, use "
        "`dsctl workflow-instance edit --patch|--file`."
    )
