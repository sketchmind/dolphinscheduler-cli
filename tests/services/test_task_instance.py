from collections.abc import Callable

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
)
from tests.runtime_instance_domain_fakes import install_runtime_instance_domain_runtime
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    PermissionDeniedError,
    TaskNotDispatchedError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.services import task_instance as task_instance_service
from dsctl.services.selection import ResourceDefaults

_PROJECT_CONTEXT = ResourceDefaults(project="etl-prod")


def _install_task_instance_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    task_instance_adapter: FakeTaskInstanceAdapter,
    context: ResourceDefaults = _PROJECT_CONTEXT,
) -> None:
    install_runtime_instance_domain_runtime(
        monkeypatch,
        project_adapter=project_adapter,
        profile=make_profile(),
        workflow_instance_adapter=workflow_instance_adapter,
        task_instance_adapter=task_instance_adapter,
        context=context,
    )


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=[FakeProject(code=7, name="etl-prod")])


@pytest.fixture
def fake_workflow_instance_adapter() -> FakeWorkflowInstanceAdapter:
    return FakeWorkflowInstanceAdapter(
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


@pytest.fixture
def fake_task_instance_adapter() -> FakeTaskInstanceAdapter:
    return FakeTaskInstanceAdapter(
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
            FakeTaskInstance(
                id=4001,
                name="stream-orders",
                task_type_value="FLINK_STREAM",
                workflow_instance_id_value=0,
                workflow_instance_name_value="stream-orders",
                project_code_value=7,
                task_code_value=401,
                task_definition_version_value=1,
                process_definition_name_value="stream-orders",
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
                start_time_value="2026-04-11 10:15:00",
                host="worker-2",
                executor_name_value="carol",
                task_execute_type_value=FakeEnumValue("STREAM"),
            ),
        ],
        log_messages_by_task_instance_id={
            3001: ["line-1", "line-2", "line-3", "line-4"],
        },
    )


def test_list_task_instances_result_returns_page_inside_workflow_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.list_task_instances_result(
        workflow_instance=901,
        search="extract",
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved["workflow_instance"] == 901
    assert data["total"] == 1
    assert _mapping(items[0])["id"] == 3001


def test_list_task_instances_result_can_auto_exhaust_pages(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.list_task_instances_result(
        workflow_instance=902,
        page_size=1,
        all_pages=True,
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved["all"] is True
    assert data["total"] == 2
    assert len(items) == 2


def test_list_task_instances_result_supports_project_scoped_filters(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.list_task_instances_result(
        project="etl-prod",
        host="worker-1",
        executor="bob",
        start="2026-04-11 10:00:00",
        end="2026-04-11 10:10:00",
        execute_type="BATCH",
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert _mapping(result.resolved["project"])["code"] == 7
    assert result.resolved["host"] == "worker-1"
    assert result.resolved["executor"] == "bob"
    assert data["total"] == 1
    assert _mapping(items[0])["id"] == 3002


@pytest.mark.parametrize("workflow_instance", [None, 901])
def test_list_task_instances_result_requires_project_selection_before_domain_io(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
    workflow_instance: int | None,
) -> None:
    workflow_detail_calls = 0
    task_page_calls = 0

    def unexpected_workflow_get(*, workflow_instance_id: int) -> object:
        nonlocal workflow_detail_calls
        del workflow_instance_id
        workflow_detail_calls += 1
        message = "workflow detail I/O must not run"
        raise AssertionError(message)

    def unexpected_task_list(**kwargs: object) -> object:
        nonlocal task_page_calls
        del kwargs
        task_page_calls += 1
        message = "task-instance page I/O must not run"
        raise AssertionError(message)

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "get",
        unexpected_workflow_get,
    )
    monkeypatch.setattr(fake_task_instance_adapter, "list", unexpected_task_list)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
        context=ResourceDefaults(),
    )

    with pytest.raises(UserInputError, match="Project is required") as exc_info:
        task_instance_service.list_task_instances_result(
            workflow_instance=workflow_instance
        )

    assert exc_info.value.suggestion == (
        "Pass --project NAME, or configure a project in the selected context."
    )
    assert workflow_detail_calls == 0
    assert task_page_calls == 0


def test_get_task_instance_result_requires_project_before_detail_io(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    workflow_detail_calls = 0
    task_detail_calls = 0

    def unexpected_workflow_get(*, workflow_instance_id: int) -> object:
        nonlocal workflow_detail_calls
        del workflow_instance_id
        workflow_detail_calls += 1
        message = "workflow detail I/O must not run"
        raise AssertionError(message)

    def unexpected_task_get(*, project_code: int, task_instance_id: int) -> object:
        nonlocal task_detail_calls
        del project_code, task_instance_id
        task_detail_calls += 1
        message = "task detail I/O must not run"
        raise AssertionError(message)

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "get",
        unexpected_workflow_get,
    )
    monkeypatch.setattr(fake_task_instance_adapter, "get", unexpected_task_get)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
        context=ResourceDefaults(),
    )

    with pytest.raises(UserInputError, match="Project is required"):
        task_instance_service.get_task_instance_result(
            3001,
            workflow_instance=901,
        )

    assert workflow_detail_calls == 0
    assert task_detail_calls == 0


def test_list_task_instances_result_rejects_workflow_definition_filter() -> None:
    with pytest.raises(UserInputError, match="cannot reliably filter") as exc_info:
        task_instance_service.list_task_instances_result(
            project="etl-prod",
            workflow="daily-sync",
        )

    assert exc_info.value.details["upstream_filter"] == "workflowDefinitionName"
    assert "workflow-instance list" in (exc_info.value.suggestion or "")


def test_list_task_instances_result_rejects_redundant_workflow_with_instance() -> None:
    with pytest.raises(UserInputError, match="does not accept --workflow") as exc_info:
        task_instance_service.list_task_instances_result(
            workflow_instance=901,
            workflow="daily-sync",
        )

    assert exc_info.value.details["workflow_instance_id"] == 901
    assert "--workflow-instance already scopes" in (exc_info.value.suggestion or "")


def test_get_task_instance_result_returns_one_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.get_task_instance_result(
        3001,
        workflow_instance=901,
    )
    data = _mapping(result.data)

    assert _mapping(result.resolved["workflowInstance"])["id"] == 901
    assert _mapping(result.resolved["taskInstance"])["id"] == 3001
    assert data["taskCode"] == 201


@pytest.mark.parametrize(
    ("task_instance_id", "expected_type"),
    [(3001, "BATCH"), (4001, "STREAM")],
)
def test_get_task_instance_result_can_resolve_project_only(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
    task_instance_id: int,
    expected_type: str,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.get_task_instance_result(task_instance_id)

    assert _mapping(result.data)["taskExecuteType"] == expected_type
    assert result.resolved["taskInstance"] == {"id": task_instance_id}
    assert "workflowInstance" not in result.resolved


def test_project_only_task_instance_read_does_not_cross_projects(
    monkeypatch: pytest.MonkeyPatch,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[
            FakeProject(code=7, name="etl-prod"),
            FakeProject(code=8, name="other"),
        ]
    )
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(NotFoundError) as exc_info:
        task_instance_service.get_task_instance_result(4001, project="other")

    assert exc_info.value.details == {"resource": "task-instance", "id": 4001}
    assert "project 'other'" in str(exc_info.value)
    assert "--execute-type STREAM" in (exc_info.value.suggestion or "")


def test_get_sub_workflow_instance_result_returns_child_relation(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.get_sub_workflow_instance_result(
        3003,
        workflow_instance=902,
    )

    assert result.data == {"subWorkflowInstanceId": 903}
    assert result.resolved["workflowInstance"] == {"id": 902}
    assert result.resolved["taskInstance"] == {"id": 3003}
    assert _mapping(result.resolved["project"])["source"] == "context"


def test_get_sub_workflow_instance_result_rejects_non_sub_workflow_task(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    def not_sub_workflow(
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        del project_code, task_instance_id
        raise ApiResultError(
            result_code=10021,
            result_message="task instance is not sub workflow instance",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "sub_workflow_instance_by_task",
        not_sub_workflow,
    )

    with pytest.raises(InvalidStateError, match="SUB_WORKFLOW") as exc_info:
        task_instance_service.get_sub_workflow_instance_result(
            3001,
            workflow_instance=901,
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl task-instance get 3001 --project etl-prod "
        "--workflow-instance 901` to inspect the task type. Only SUB_WORKFLOW "
        "task instances have a child workflow instance."
    )


def test_get_task_instance_log_result_translates_not_dispatched(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    def not_dispatched_log(
        *,
        task_instance_id: int,
    ) -> None:
        del task_instance_id
        raise ApiResultError(
            result_code=10103,
            result_message=(
                "TaskInstanceLogPath is empty, maybe the taskInstance doesn't "
                "be dispatched"
            ),
        )

    monkeypatch.setattr(
        fake_task_instance_adapter, "task_log_lines", not_dispatched_log
    )

    with pytest.raises(TaskNotDispatchedError) as exc_info:
        task_instance_service.get_task_instance_log_result(3001)

    assert exc_info.value.details == {"resource": "task-instance", "id": 3001}
    source = _mapping(exc_info.value.to_payload()["source"])
    assert source["result_code"] == 10103
    assert "dsctl workflow-instance list" in (exc_info.value.suggestion or "")


def test_get_task_instance_log_result_preserves_generic_log_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=10103,
        result_message="view task instance log error: connection refused",
    )

    def failed_log(
        *,
        task_instance_id: int,
    ) -> None:
        del task_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_task_instance_adapter, "task_log_lines", failed_log)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        task_instance_service.get_task_instance_log_result(3001)

    assert exc_info.value is upstream_error


def test_get_task_instance_result_reports_missing_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(NotFoundError, match="was not found"):
        task_instance_service.get_task_instance_result(
            9999,
            workflow_instance=901,
        )


@pytest.mark.parametrize(
    "operation",
    [
        task_instance_service.get_task_instance_result,
        task_instance_service.watch_task_instance_result,
    ],
)
def test_task_instance_reads_translate_v2_empty_success_body(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
    operation: Callable[..., object],
) -> None:
    def missing_get(
        *,
        project_code: int,
        task_instance_id: int,
    ) -> None:
        del project_code, task_instance_id

    monkeypatch.setattr(fake_task_instance_adapter, "get", missing_get)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(NotFoundError) as exc_info:
        operation(9999, workflow_instance=901)

    error = exc_info.value
    assert error.details == {
        "resource": "task-instance",
        "id": 9999,
        "workflow_instance_id": 901,
    }
    assert error.suggestion == (
        "Run `dsctl task-instance list --project etl-prod --workflow-instance "
        "901` to inspect available task instance ids."
    )
    assert "retryable" not in error.details


@pytest.mark.parametrize(
    "operation",
    [
        task_instance_service.get_task_instance_result,
        task_instance_service.watch_task_instance_result,
    ],
)
def test_task_instance_reads_preserve_generic_query_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
    operation: Callable[..., object],
) -> None:
    upstream_error = ApiResultError(
        result_code=10205,
        result_message="query task instance error:database unavailable",
    )

    def fail_get(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_task_instance_adapter, "get", fail_get)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(ApiResultError) as exc_info:
        operation(9999, workflow_instance=901)

    assert exc_info.value is upstream_error


@pytest.mark.parametrize(
    "operation",
    [
        task_instance_service.get_task_instance_result,
        task_instance_service.watch_task_instance_result,
    ],
)
def test_task_instance_reads_translate_project_permission_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
    operation: Callable[..., object],
) -> None:
    upstream_error = ApiResultError(
        result_code=30002,
        result_message="user has no project operation privilege",
    )

    def fail_get(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_task_instance_adapter, "get", fail_get)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        operation(9999, workflow_instance=901)

    error = exc_info.value
    assert error.details == {
        "resource": "task-instance",
        "id": 9999,
        "workflow_instance_id": 901,
    }
    assert error.to_payload()["source"] == upstream_error.source


def test_project_only_task_read_preserves_permission_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=30002,
        result_message="user has no project operation privilege",
    )

    def fail_get(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_task_instance_adapter, "get", fail_get)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(PermissionDeniedError) as exc_info:
        task_instance_service.get_task_instance_result(4001)

    assert exc_info.value.details == {"resource": "task-instance", "id": 4001}
    assert exc_info.value.to_payload()["source"] == upstream_error.source


def test_task_instance_log_translates_missing_result_code(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    upstream_error = ApiResultError(
        result_code=10008,
        result_message="task instance not found",
    )

    def missing_log(
        *,
        task_instance_id: int,
    ) -> None:
        del task_instance_id
        raise upstream_error

    monkeypatch.setattr(fake_task_instance_adapter, "task_log_lines", missing_log)
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(NotFoundError) as exc_info:
        task_instance_service.get_task_instance_log_result(9999)

    error = exc_info.value
    assert error.details == {"resource": "task-instance", "id": 9999}
    assert error.suggestion == (
        "Use `dsctl workflow-instance list` in the relevant project to find the "
        "owning workflow instance, then inspect it with `dsctl task-instance "
        "list --workflow-instance`."
    )
    assert error.to_payload()["source"] == upstream_error.source
    assert "retryable" not in error.details


def test_watch_task_instance_result_waits_for_finished_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    watch_adapter = FakeTaskInstanceAdapter(
        task_instances=[
            FakeTaskInstance(
                id=3001,
                name="extract",
                workflow_instance_id_value=901,
                project_code_value=7,
                state_value=FakeEnumValue("RUNNING_EXECUTION"),
            )
        ],
        task_instance_sequences_by_id={
            3001: [
                FakeTaskInstance(
                    id=3001,
                    name="extract",
                    workflow_instance_id_value=901,
                    project_code_value=7,
                    state_value=FakeEnumValue("RUNNING_EXECUTION"),
                ),
                FakeTaskInstance(
                    id=3001,
                    name="extract",
                    workflow_instance_id_value=901,
                    project_code_value=7,
                    state_value=FakeEnumValue("SUCCESS"),
                ),
            ]
        },
    )
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=watch_adapter,
    )
    monkeypatch.setattr("dsctl.services.task_instance.time.sleep", lambda _: None)

    result = task_instance_service.watch_task_instance_result(
        3001,
        workflow_instance=901,
        interval_seconds=1,
        timeout_seconds=5,
    )

    assert _mapping(result.data)["state"] == "SUCCESS"
    assert _mapping(result.resolved)["taskInstance"] == {"id": 3001}


def test_watch_stream_task_instance_uses_project_only_scope(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    stream_adapter = FakeTaskInstanceAdapter(
        task_instances=[],
        task_instance_sequences_by_id={
            4001: [
                FakeTaskInstance(
                    id=4001,
                    workflow_instance_id_value=0,
                    project_code_value=7,
                    state_value=FakeEnumValue("RUNNING_EXECUTION"),
                    task_execute_type_value=FakeEnumValue("STREAM"),
                ),
                FakeTaskInstance(
                    id=4001,
                    workflow_instance_id_value=0,
                    project_code_value=7,
                    state_value=FakeEnumValue("SUCCESS"),
                    task_execute_type_value=FakeEnumValue("STREAM"),
                ),
            ]
        },
    )
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=stream_adapter,
    )
    monkeypatch.setattr("dsctl.services.task_instance.time.sleep", lambda _: None)

    result = task_instance_service.watch_task_instance_result(
        4001,
        interval_seconds=1,
        timeout_seconds=5,
    )

    assert _mapping(result.data)["state"] == "SUCCESS"
    assert "workflowInstance" not in result.resolved


def test_watch_task_instance_result_times_out(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )
    monotonic_values = iter((0.0, 5.1))
    monkeypatch.setattr(
        "dsctl.services.task_instance.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("dsctl.services.task_instance.time.sleep", lambda _: None)

    with pytest.raises(WaitTimeoutError, match="Timed out waiting") as exc_info:
        task_instance_service.watch_task_instance_result(
            3001,
            workflow_instance=901,
            interval_seconds=1,
            timeout_seconds=5,
        )

    assert exc_info.value.details["last_state"] == "RUNNING_EXECUTION"
    assert exc_info.value.suggestion == (
        "Retry with a larger --timeout-seconds value or inspect the current "
        "state with `dsctl task-instance get 3001 --project etl-prod "
        "--workflow-instance 901`."
    )


def test_force_success_task_instance_result_returns_forced_result_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.force_success_task_instance_result(
        3002,
        workflow_instance=902,
    )
    data = _mapping(result.data)

    assert data["id"] == 3002
    assert data["state"] == "FORCED_SUCCESS"
    assert fake_task_instance_adapter.force_success_ids == [3002]


def test_force_success_task_instance_result_requires_finished_workflow_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="owning workflow instance") as exc_info:
        task_instance_service.force_success_task_instance_result(
            3001,
            workflow_instance=901,
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl workflow-instance get 901 --project etl-prod` to inspect the "
        "owning workflow instance. Wait for it to reach a final state, then "
        "retry `task-instance force-success`."
    )


def test_force_success_task_instance_result_reports_task_state_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="FAILURE") as exc_info:
        task_instance_service.force_success_task_instance_result(
            3003,
            workflow_instance=902,
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl task-instance get 3003 --project etl-prod "
        "--workflow-instance 902` to inspect the current task state. "
        "`task-instance force-success` only applies to FAILURE, "
        "NEED_FAULT_TOLERANCE, or KILL."
    )


def test_savepoint_task_instance_result_returns_requested_wrapper(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.savepoint_task_instance_result(
        3001,
        workflow_instance=901,
    )
    data = _mapping(result.data)

    assert data["requested"] is True
    assert _mapping(data["taskInstance"])["id"] == 3001
    assert fake_task_instance_adapter.savepoint_ids == [3001]


def test_stream_task_savepoint_resolves_directly_from_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.savepoint_task_instance_result(4001)

    assert _mapping(result.data)["requested"] is True
    assert result.resolved["taskInstance"] == {"id": 4001}
    assert "workflowInstance" not in result.resolved
    assert fake_task_instance_adapter.savepoint_ids == [4001]


def test_savepoint_task_instance_result_reports_running_state_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="still be running") as exc_info:
        task_instance_service.savepoint_task_instance_result(
            3002,
            workflow_instance=902,
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl task-instance get 3002 --project etl-prod "
        "--workflow-instance 902` to inspect the current task state. "
        "`task-instance savepoint` only applies while the task instance is "
        "still running."
    )


def test_savepoint_task_instance_result_preserves_generic_upstream_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    def broken_savepoint(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise ApiResultError(
            result_code=10196,
            result_message="task savepoint error",
        )

    monkeypatch.setattr(fake_task_instance_adapter, "savepoint", broken_savepoint)

    with pytest.raises(ApiResultError, match="task savepoint error") as exc_info:
        task_instance_service.savepoint_task_instance_result(
            3001,
            workflow_instance=901,
        )

    assert exc_info.value.result_code == 10196
    assert exc_info.value.details == {
        "result_code": 10196,
        "resource": "task-instance",
        "id": 3001,
        "workflow_instance_id": 901,
        "action": "savepoint",
    }


def test_stop_task_instance_result_returns_requested_wrapper(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.stop_task_instance_result(
        3001,
        workflow_instance=901,
    )
    data = _mapping(result.data)

    assert data["requested"] is True
    assert _mapping(data["taskInstance"])["id"] == 3001
    assert fake_task_instance_adapter.stopped_ids == [3001]


def test_stream_task_stop_resolves_directly_from_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    result = task_instance_service.stop_task_instance_result(4001)

    assert _mapping(result.data)["requested"] is True
    assert result.resolved["taskInstance"] == {"id": 4001}
    assert "workflowInstance" not in result.resolved
    assert fake_task_instance_adapter.stopped_ids == [4001]


def test_stop_task_instance_result_preserves_generic_upstream_failure(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    def broken_stop(*, project_code: int, task_instance_id: int) -> None:
        del project_code, task_instance_id
        raise ApiResultError(
            result_code=10197,
            result_message="task stop error",
        )

    monkeypatch.setattr(fake_task_instance_adapter, "stop", broken_stop)

    with pytest.raises(ApiResultError, match="task stop error") as exc_info:
        task_instance_service.stop_task_instance_result(
            3001,
            workflow_instance=901,
        )

    assert exc_info.value.result_code == 10197
    assert exc_info.value.details == {
        "result_code": 10197,
        "resource": "task-instance",
        "id": 3001,
        "workflow_instance_id": 901,
        "action": "stop",
    }


def test_list_task_instances_result_reports_supported_state_names(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Task instance state must be one of the DS execution status names",
    ) as exc_info:
        task_instance_service.list_task_instances_result(
            workflow_instance=901,
            state="not-a-real-state",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl enum list task-execution-status` to inspect the supported DS "
        "task-instance states."
    )


def test_list_task_instances_result_reports_supported_execute_types(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Task execute type must be one of the DS task execute-type names",
    ) as exc_info:
        task_instance_service.list_task_instances_result(
            workflow_instance=901,
            execute_type="not-real",
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl enum list task-execute-type` to inspect the supported DS "
        "task execute-type names."
    )


def test_stop_task_instance_result_reports_running_state_suggestion(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="still be running") as exc_info:
        task_instance_service.stop_task_instance_result(
            3002,
            workflow_instance=902,
        )

    assert exc_info.value.suggestion == (
        "Run `dsctl task-instance get 3002 --project etl-prod "
        "--workflow-instance 902` to inspect the current task state. "
        "`task-instance stop` only applies while the task instance is still "
        "running."
    )


def test_task_instance_dynamic_suggestion_shell_quotes_selected_project(
    monkeypatch: pytest.MonkeyPatch,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    fake_task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    _install_task_instance_service_fakes(
        monkeypatch,
        project_adapter=FakeProjectAdapter(
            projects=[FakeProject(code=7, name="etl prod")]
        ),
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_instance_adapter=fake_task_instance_adapter,
        context=ResourceDefaults(project="etl prod"),
    )

    with pytest.raises(InvalidStateError, match="still be running") as exc_info:
        task_instance_service.stop_task_instance_result(
            3002,
            workflow_instance=902,
        )

    suggestion = exc_info.value.suggestion
    assert suggestion is not None
    assert "--project 'etl prod'" in suggestion
    assert "--project PROJECT" not in suggestion
