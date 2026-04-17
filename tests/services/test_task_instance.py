from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskInstance,
    FakeTaskInstanceAdapter,
    FakeWorkflowInstance,
    FakeWorkflowInstanceAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    TaskNotDispatchedError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.services import runtime as runtime_service
from dsctl.services import task_instance as task_instance_service


def _install_task_instance_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    task_instance_adapter: FakeTaskInstanceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_instance_adapter=task_instance_adapter,
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


def test_list_task_instances_result_requires_project_without_workflow_instance(
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

    with pytest.raises(UserInputError, match="Project is required"):
        task_instance_service.list_task_instances_result()


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
    assert result.resolved == {
        "workflowInstance": {"id": 902},
        "taskInstance": {"id": 3003},
    }


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
        "Run `dsctl task-instance get 3001 --workflow-instance 901` to inspect "
        "the task type. Only SUB_WORKFLOW task instances have a child workflow "
        "instance."
    )


def test_get_task_instance_log_result_returns_tail_lines(
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

    result = task_instance_service.get_task_instance_log_result(3001, tail=2)
    data = _mapping(result.data)

    assert data["lineCount"] == 2
    assert data["text"] == "line-3\nline-4"


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
        skip_line_num: int,
        limit: int,
    ) -> None:
        del task_instance_id, skip_line_num, limit
        raise ApiResultError(
            result_code=10103,
            result_message=(
                "TaskInstanceLogPath is empty, maybe the taskInstance doesn't "
                "be dispatched"
            ),
        )

    monkeypatch.setattr(fake_task_instance_adapter, "log_chunk", not_dispatched_log)

    with pytest.raises(TaskNotDispatchedError) as exc_info:
        task_instance_service.get_task_instance_log_result(3001)

    assert exc_info.value.details["result_code"] == 10103
    assert "workflow-instance digest" in (exc_info.value.suggestion or "")


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
        "state with `task-instance get 3001 --workflow-instance 901`."
    )


def test_force_success_task_instance_result_returns_forced_success_payload(
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
        "Run `dsctl workflow-instance get 901` to inspect the owning workflow "
        "instance. Wait for it to reach a final state, then retry "
        "`task-instance force-success`."
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
        "Run `dsctl task-instance get 3003 --workflow-instance 902` to inspect "
        "the current task state. `task-instance force-success` only applies to "
        "FAILURE, NEED_FAULT_TOLERANCE, or KILL."
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
        "Run `dsctl task-instance get 3002 --workflow-instance 902` to inspect "
        "the current task state. `task-instance savepoint` only applies while "
        "the task instance is still running."
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
        "Run `dsctl enum list task_execution_status` to inspect the supported DS "
        "task-instance states."
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
        "Run `dsctl task-instance get 3002 --workflow-instance 902` to inspect "
        "the current task state. `task-instance stop` only applies while the "
        "task instance is still running."
    )
