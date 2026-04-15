import json
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest
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

from dsctl.errors import (
    ApiResultError,
    InvalidStateError,
    NotFoundError,
    UserInputError,
    WaitTimeoutError,
)
from dsctl.services import runtime as runtime_service
from dsctl.services import workflow_instance as workflow_instance_service


def _install_workflow_instance_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_instance_adapter: FakeWorkflowInstanceAdapter,
    task_adapter: FakeTaskAdapter | None = None,
    task_instance_adapter: FakeTaskInstanceAdapter | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            workflow_instance_adapter=workflow_instance_adapter,
            task_adapter=task_adapter,
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
    return FakeWorkflowInstanceAdapter(
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
                command_type_value=FakeEnumValue("START_PROCESS"),
                executor_id_value=11,
                executor_name_value="alice",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
            FakeWorkflowInstance(
                id=902,
                workflow_definition_code_value=101,
                workflow_definition_version_value=1,
                project_code_value=7,
                dag_data_value=workflow_dag,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="daily-sync-902",
                host="master-1",
                command_type_value=FakeEnumValue("START_PROCESS"),
                executor_id_value=11,
                executor_name_value="alice",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
            FakeWorkflowInstance(
                id=903,
                workflow_definition_code_value=201,
                workflow_definition_version_value=1,
                project_code_value=7,
                state_value=FakeEnumValue("SUCCESS"),
                run_times_value=1,
                name="child-workflow-903",
                host="master-2",
                command_type_value=FakeEnumValue("COMPLEMENT_DATA"),
                executor_id_value=12,
                executor_name_value="bob",
                workflow_instance_priority_value=FakeEnumValue("MEDIUM"),
                worker_group_value="default",
            ),
        ],
        parent_workflow_instance_ids_by_sub_id={903: 902},
    )


def test_list_workflow_instances_result_returns_ds_page(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.list_workflow_instances_result(
        state="running_execution"
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 1
    assert _mapping(items[0])["id"] == 901
    assert result.resolved["state"] == "RUNNING_EXECUTION"


def test_list_workflow_instances_result_reports_supported_state_names(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Workflow instance state must be one of the DS execution status names",
    ) as exc_info:
        workflow_instance_service.list_workflow_instances_result(state="running")
    assert exc_info.value.suggestion == (
        "Run `dsctl enum list workflow_execution_status` to inspect the "
        "supported state names."
    )


def test_list_workflow_instances_result_can_auto_exhaust_pages(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.list_workflow_instances_result(
        page_size=1,
        all_pages=True,
    )
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved["all"] is True
    assert data["total"] == 3
    assert len(items) == 3


def test_get_workflow_instance_result_returns_one_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.get_workflow_instance_result(901)
    data = _mapping(result.data)

    assert result.resolved == {"workflowInstance": {"id": 901}}
    assert data["state"] == "RUNNING_EXECUTION"
    assert data["projectCode"] == 7


def test_get_parent_workflow_instance_result_returns_parent_relation(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.get_parent_workflow_instance_result(903)

    assert result.data == {"parentWorkflowInstance": 902}
    assert result.resolved == {"subWorkflowInstance": {"id": 903}}


def test_get_parent_workflow_instance_result_rejects_non_sub_workflow_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    def not_sub_workflow(
        *,
        project_code: int,
        sub_workflow_instance_id: int,
    ) -> None:
        del project_code, sub_workflow_instance_id
        raise ApiResultError(
            result_code=50010,
            result_message="workflow instance is not sub workflow instance",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "parent_instance_by_sub_workflow",
        not_sub_workflow,
    )

    with pytest.raises(InvalidStateError, match="sub-workflow instance") as exc_info:
        workflow_instance_service.get_parent_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get ID` for regular workflow instances; "
        "`parent` only applies to sub-workflow instances."
    )


def test_digest_workflow_instance_result_returns_runtime_summary(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
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
                    retry_times_value=0,
                    host="worker-1",
                    start_time_value="2026-04-11 10:00:00",
                    duration_value="15s",
                ),
                FakeTaskInstance(
                    id=3002,
                    name="load",
                    task_type_value="SQL",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=202,
                    state_value=FakeEnumValue("SUBMITTED_SUCCESS"),
                    retry_times_value=0,
                ),
                FakeTaskInstance(
                    id=3003,
                    name="validate",
                    task_type_value="SHELL",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=203,
                    state_value=FakeEnumValue("FAILURE"),
                    retry_times_value=2,
                    host="worker-2",
                    end_time_value="2026-04-11 10:01:00",
                    duration_value="8s",
                ),
                FakeTaskInstance(
                    id=3004,
                    name="notify",
                    task_type_value="HTTP",
                    workflow_instance_id_value=901,
                    workflow_instance_name_value="daily-sync-901",
                    project_code_value=7,
                    task_code_value=204,
                    state_value=FakeEnumValue("SUCCESS"),
                    retry_times_value=1,
                    end_time_value="2026-04-11 10:02:00",
                    duration_value="3s",
                ),
            ]
        ),
    )

    result = workflow_instance_service.digest_workflow_instance_result(901)
    data = _mapping(result.data)

    assert result.resolved == {"workflowInstance": {"id": 901}}
    assert _mapping(data["workflowInstance"])["state"] == "RUNNING_EXECUTION"
    assert data["taskCount"] == 4
    assert data["taskStateCounts"] == {
        "FAILURE": 1,
        "RUNNING_EXECUTION": 1,
        "SUBMITTED_SUCCESS": 1,
        "SUCCESS": 1,
    }
    assert data["taskTypeCounts"] == {
        "HTTP": 1,
        "SHELL": 2,
        "SQL": 1,
    }
    assert data["progress"] == {
        "running": 1,
        "queued": 1,
        "paused": 0,
        "failed": 1,
        "success": 1,
        "other": 0,
        "finished": 2,
        "active": 2,
    }
    assert data["runningTasks"] == [
        {
            "id": 3001,
            "taskCode": 201,
            "name": "extract",
            "taskType": "SHELL",
            "state": "RUNNING_EXECUTION",
            "retryTimes": 0,
            "host": "worker-1",
            "startTime": "2026-04-11 10:00:00",
            "endTime": None,
            "duration": "15s",
        }
    ]
    assert data["queuedTasks"] == [
        {
            "id": 3002,
            "taskCode": 202,
            "name": "load",
            "taskType": "SQL",
            "state": "SUBMITTED_SUCCESS",
            "retryTimes": 0,
            "host": None,
            "startTime": None,
            "endTime": None,
            "duration": None,
        }
    ]
    assert data["failedTasks"] == [
        {
            "id": 3003,
            "taskCode": 203,
            "name": "validate",
            "taskType": "SHELL",
            "state": "FAILURE",
            "retryTimes": 2,
            "host": "worker-2",
            "startTime": None,
            "endTime": "2026-04-11 10:01:00",
            "duration": "8s",
        }
    ]
    assert data["retriedTasks"] == [
        {
            "id": 3003,
            "taskCode": 203,
            "name": "validate",
            "taskType": "SHELL",
            "state": "FAILURE",
            "retryTimes": 2,
            "host": "worker-2",
            "startTime": None,
            "endTime": "2026-04-11 10:01:00",
            "duration": "8s",
        },
        {
            "id": 3004,
            "taskCode": 204,
            "name": "notify",
            "taskType": "HTTP",
            "state": "SUCCESS",
            "retryTimes": 1,
            "host": None,
            "startTime": None,
            "endTime": "2026-04-11 10:02:00",
            "duration": "3s",
        },
    ]


def test_get_workflow_instance_result_reports_missing_instance(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(NotFoundError, match="was not found"):
        workflow_instance_service.get_workflow_instance_result(999)


def test_stop_workflow_instance_result_requests_stop_and_returns_refresh(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.stop_workflow_instance_result(901)
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.stopped_ids == [901]
    assert data["state"] == "READY_STOP"
    assert result.warnings == [
        "stop requested; current workflow instance state is READY_STOP"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_instance_action_state_after_request",
            "action": "stop",
            "message": "stop requested; current workflow instance state is READY_STOP",
            "current_state": "READY_STOP",
            "expect_non_final": False,
            "target_state": "STOP",
        }
    ]


def test_stop_workflow_instance_result_rejects_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    fake_workflow_instance_adapter.workflow_instances = [
        fake_workflow_instance_adapter.workflow_instances[1]
    ]
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="cannot be stopped") as exc_info:
        workflow_instance_service.stop_workflow_instance_result(902)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get ID` or "
        "`dsctl workflow-instance watch ID` to inspect the current state before "
        "retrying stop."
    )


def test_rerun_workflow_instance_result_reports_runtime_control_conflict(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    def busy_rerun(*, workflow_instance_id: int) -> None:
        raise ApiResultError(
            result_code=workflow_instance_service.WORKFLOW_INSTANCE_EXECUTING_COMMAND,
            result_message=f"workflow instance id {workflow_instance_id} is busy",
        )

    monkeypatch.setattr(fake_workflow_instance_adapter, "rerun", busy_rerun)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="already executing another runtime control command",
    ) as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(902)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get ID` or "
        "`dsctl workflow-instance watch ID` to inspect the current state, wait "
        "for the active runtime control command to finish, then retry "
        "`dsctl workflow-instance rerun ID`."
    )


def test_watch_workflow_instance_result_waits_for_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    watch_adapter = FakeWorkflowInstanceAdapter(
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=watch_adapter,
    )
    monkeypatch.setattr("dsctl.services.workflow_instance.time.sleep", lambda _: None)

    result = workflow_instance_service.watch_workflow_instance_result(
        901,
        interval_seconds=1,
        timeout_seconds=5,
    )

    assert _mapping(result.data)["state"] == "SUCCESS"


def test_watch_workflow_instance_result_times_out(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
    watch_adapter = FakeWorkflowInstanceAdapter(
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=watch_adapter,
    )
    monotonic_values = iter((0.0, 5.1))
    monkeypatch.setattr(
        "dsctl.services.workflow_instance.time.monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr("dsctl.services.workflow_instance.time.sleep", lambda _: None)

    with pytest.raises(WaitTimeoutError, match="Timed out waiting") as exc_info:
        workflow_instance_service.watch_workflow_instance_result(
            901,
            interval_seconds=1,
            timeout_seconds=5,
        )
    assert exc_info.value.suggestion == (
        "Retry with a larger --timeout-seconds value or inspect the current "
        "state with `workflow-instance get 901`."
    )


def test_rerun_workflow_instance_result_requests_rerun(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    result = workflow_instance_service.rerun_workflow_instance_result(902)
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.rerun_ids == [902]
    assert data["state"] == "RUNNING_EXECUTION"
    assert result.warnings == []


def test_rerun_workflow_instance_result_warns_if_state_remains_final(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
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

    def no_op_rerun(*, workflow_instance_id: int) -> None:
        workflow_instance_adapter.rerun_ids.append(workflow_instance_id)

    monkeypatch.setattr(workflow_instance_adapter, "rerun", no_op_rerun)
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    result = workflow_instance_service.rerun_workflow_instance_result(902)

    assert result.warnings == [
        "rerun requested; current workflow instance state is SUCCESS"
    ]
    assert result.warning_details == [
        {
            "code": "workflow_instance_action_state_after_request",
            "action": "rerun",
            "message": "rerun requested; current workflow instance state is SUCCESS",
            "current_state": "SUCCESS",
            "expect_non_final": True,
            "target_state": None,
        }
    ]


def test_rerun_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(InvalidStateError, match="final state") as exc_info:
        workflow_instance_service.rerun_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance rerun ID`."
    )


def test_execute_task_in_workflow_instance_result_requires_online_definition(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
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

    def offline_definition_execute_task(
        *,
        project_code: int,
        workflow_instance_id: int,
        task_code: int,
        scope: str,
    ) -> None:
        del project_code, workflow_instance_id, task_code, scope
        raise ApiResultError(
            result_code=workflow_instance_service.WORKFLOW_DEFINITION_NOT_RELEASE,
            result_message="workflow definition not online",
        )

    monkeypatch.setattr(
        fake_workflow_instance_adapter,
        "execute_task",
        offline_definition_execute_task,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="workflow definition must be online",
    ) as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="self",
        )
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get ID` to inspect the referenced "
        "workflow definition, bring that workflow online with "
        "`dsctl workflow online`, then retry the runtime action."
    )


def test_recover_failed_workflow_instance_result_requests_recovery(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    result = workflow_instance_service.recover_failed_workflow_instance_result(903)
    data = _mapping(result.data)

    assert workflow_instance_adapter.recovered_failed_ids == [903]
    assert data["state"] == "RUNNING_EXECUTION"


def test_recover_failed_workflow_instance_result_rejects_non_failure_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="FAILURE state before recover-failed",
    ) as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(901)
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow-instance get ID` or "
        "`dsctl workflow-instance watch ID` to confirm the instance is in "
        "FAILURE before retrying `recover-failed`."
    )


def test_recover_failed_workflow_instance_result_waits_for_final_state_on_race(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
) -> None:
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

    def not_finished_recover_failed(*, workflow_instance_id: int) -> None:
        raise ApiResultError(
            result_code=workflow_instance_service.WORKFLOW_INSTANCE_NOT_FINISHED,
            result_message=f"workflow instance id {workflow_instance_id} not finished",
        )

    monkeypatch.setattr(
        workflow_instance_adapter,
        "recover_failed",
        not_finished_recover_failed,
    )
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=workflow_instance_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="must be in a final state before this action can proceed",
    ) as exc_info:
        workflow_instance_service.recover_failed_workflow_instance_result(903)
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance recover-failed ID`."
    )


def test_execute_task_in_workflow_instance_result_requests_task_execution(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    result = workflow_instance_service.execute_task_in_workflow_instance_result(
        902,
        task="extract",
        scope="pre",
    )
    data = _mapping(result.data)

    assert fake_workflow_instance_adapter.executed_tasks == [(902, 201, "pre")]
    assert data["state"] == "RUNNING_EXECUTION"
    assert _mapping(result.resolved["task"])["code"] == 201
    assert result.resolved["scope"] == "pre"


def test_execute_task_in_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(InvalidStateError, match="final state") as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            901,
            task="extract",
        )
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance execute-task ID --task TASK`."
    )


def test_execute_task_in_workflow_instance_result_reports_scope_choices(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
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
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
        task_adapter=task_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Task execution scope must be one of: self, pre, post",
    ) as exc_info:
        workflow_instance_service.execute_task_in_workflow_instance_result(
            902,
            task="extract",
            scope="before",
        )

    assert exc_info.value.suggestion == (
        "Pass `--scope self`, `--scope pre`, or `--scope post`."
    )


def test_update_workflow_instance_result_dry_run_compiles_patch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
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

    result = workflow_instance_service.update_workflow_instance_result(
        902,
        patch=patch_file,
        dry_run=True,
    )
    data = _mapping(result.data)
    request = _mapping(data["request"])
    form = _mapping(request["form"])

    assert request["method"] == "PUT"
    assert request["path"] == "/projects/7/workflow-instances/902"
    assert form["syncDefine"] is False
    assert form["timeout"] == 45
    assert data["no_change"] is False
    assert _mapping(result.resolved["project"])["name"] == "etl-prod"
    assert _mapping(result.resolved["workflow"])["code"] == 101


def test_update_workflow_instance_result_updates_finished_instance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
      global_params:
        env: prod
        region: cn
  tasks:
    rename:
      - from: extract
        to: extract-v2
""".strip(),
        encoding="utf-8",
    )

    result = workflow_instance_service.update_workflow_instance_result(
        902,
        patch=patch_file,
        sync_definition=True,
    )
    data = _mapping(result.data)
    update_call = fake_workflow_instance_adapter.update_calls[0]
    relation_payload = json.loads(str(update_call["task_relation_json"]))
    definition_payload = json.loads(str(update_call["task_definition_json"]))
    global_params_payload = json.loads(str(update_call["global_params"]))
    locations_payload = json.loads(str(update_call["locations"]))

    assert update_call["project_code"] == 7
    assert update_call["workflow_instance_id"] == 902
    assert update_call["sync_define"] is True
    assert update_call["timeout"] == 45
    assert update_call["schedule_time"] is None
    assert [item["postTaskCode"] for item in relation_payload] == [201, 202]
    assert [item["name"] for item in definition_payload] == ["extract-v2", "load"]
    assert definition_payload[0]["version"] == 1
    assert definition_payload[0]["taskParams"] == '{"rawScript":"echo extract"}'
    assert global_params_payload == [
        {
            "prop": "env",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "prod",
        },
        {
            "prop": "region",
            "direct": "IN",
            "type": "VARCHAR",
            "value": "cn",
        },
    ]
    assert [item["taskCode"] for item in locations_payload] == [201, 202]
    assert data["workflowDefinitionVersion"] == 2
    assert data["timeout"] == 45
    assert result.resolved["syncDefine"] is True
    assert _mapping(result.resolved["workflow"])["version"] == 2


def test_update_workflow_instance_result_rejects_non_final_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
    patch_file = tmp_path / "workflow-instance.patch.yaml"
    patch_file.write_text(
        """
patch:
  workflow:
    set:
      timeout: 45
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        InvalidStateError,
        match="final state before update",
    ) as exc_info:
        workflow_instance_service.update_workflow_instance_result(
            901,
            patch=patch_file,
        )
    assert exc_info.value.suggestion == (
        "Wait for the workflow instance to reach a final state, then retry "
        "`dsctl workflow-instance update ID --patch PATCH`."
    )


def test_update_workflow_instance_result_rejects_unsupported_workflow_fields(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_instance_adapter: FakeWorkflowInstanceAdapter,
) -> None:
    _install_workflow_instance_service_fakes(
        monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_instance_adapter=fake_workflow_instance_adapter,
    )
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

    with pytest.raises(UserInputError, match="only supports") as exc_info:
        workflow_instance_service.update_workflow_instance_result(
            902,
            patch=patch_file,
        )
    assert exc_info.value.suggestion == (
        "Use `dsctl workflow edit --patch ...` for definition-level fields "
        "such as name, description, or release_state."
    )
