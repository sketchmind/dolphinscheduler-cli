import json
from collections.abc import Mapping, Sequence

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
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
)
from dsctl.services import runtime as runtime_service
from dsctl.services import task as task_service


def _install_task_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    task_adapter: FakeTaskAdapter,
    context: SessionContext | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=context,
            workflow_adapter=workflow_adapter,
            task_adapter=task_adapter,
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


def test_list_tasks_result_uses_context_and_filters(
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.list_tasks_result(search="extract")
    items = _sequence(result.data)

    assert _mapping(result.resolved["project"])["source"] == "context"
    assert _mapping(result.resolved["workflow"])["source"] == "context"
    assert list(items) == [
        {
            "code": 201,
            "name": "extract",
            "version": 1,
        }
    ]


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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.get_task_result("extract")
    data = _mapping(result.data)

    assert result.resolved["task"] == {
        "code": 201,
        "name": "extract",
        "version": 1,
    }
    assert data["code"] == 201
    assert data["taskType"] == "SHELL"


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
        context=SessionContext(project="etl-prod"),
    )

    with pytest.raises(UserInputError, match="Workflow is required") as exc_info:
        task_service.list_tasks_result()
    assert exc_info.value.suggestion == (
        "Pass --workflow NAME or run `dsctl use workflow NAME`."
    )


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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(NotFoundError, match="was not found"):
        task_service.get_task_result("missing")


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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
        set_values=[
            "command=echo load v2",
            "retry.times=3",
            "priority=HIGH",
        ],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(data["request"])
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))
    task_params = _mapping(json.loads(str(payload["taskParams"])))

    assert data["dry_run"] is True
    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/task-definition/202/with-upstream"
    assert form["upstreamCodes"] == "201"
    assert data["updated_fields"] == ["command", "retry.times", "priority"]
    assert data["no_change"] is False
    assert result.warnings == ["dry run: no request was sent"]
    assert result.warning_details == [
        {
            "code": "dry_run_no_request_sent",
            "message": "dry run: no request was sent",
            "request_sent": False,
        }
    ]
    assert payload["taskPriority"] == "HIGH"
    assert payload["failRetryTimes"] == 3
    assert task_params["rawScript"] == "echo load v2"


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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
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
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(
        UserInputError,
        match="timeout_notify_strategy requires timeout > 0",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            set_values=["timeout_notify_strategy=FAILED"],
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
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
    request = _mapping(data["request"])
    form = _mapping(request["form"])
    payload = _mapping(json.loads(str(form["taskDefinitionJsonObj"])))

    assert _sequence(_mapping(result.data)["updated_fields"]) == [
        "flag",
        "environment_code",
        "task_group_id",
        "task_group_priority",
        "cpu_quota",
        "memory_max",
        "timeout",
        "timeout_notify_strategy",
    ]
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
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
    request = _mapping(data["request"])
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
        set_values=["timeout=10"],
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(data["request"])
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
        set_values=["timeout_notify_strategy=WARN"],
        dry_run=True,
    )
    data = _mapping(result.data)

    assert data["updated_fields"] == []
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(UserInputError, match="timeout > 0"):
        task_service.update_task_result(
            "load",
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    result = task_service.update_task_result(
        "load",
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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(
        InvalidStateError,
        match="does not support modification",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            set_values=["command=echo load v2"],
        )
    assert exc_info.value.suggestion == (
        "Inspect the containing workflow definition state; if the workflow is "
        "online, bring it offline before retrying `task update`."
    )


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
        context=SessionContext(project="etl-prod", workflow="daily-sync"),
    )

    with pytest.raises(
        UserInputError,
        match="Task update did not change any persisted fields",
    ) as exc_info:
        task_service.update_task_result(
            "load",
            set_values=["command=echo load v2"],
        )
    assert exc_info.value.suggestion == (
        "Run `dsctl schema` and inspect task.update option set.supported_keys. "
        "For structural task changes such as rename, type changes, or add/remove, "
        "use `dsctl workflow edit --patch ...`."
    )
