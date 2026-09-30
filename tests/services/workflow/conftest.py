"""Fresh adapters shared by workflow service tests."""

import json

import pytest
from tests.fakes import (
    FakeDag,
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowTaskRelation,
)
from tests.services.workflow.harness import (
    _WorkflowServiceHarness,
)


@pytest.fixture
def stale_legacy_workflow_graph() -> tuple[str, str, str]:
    """Independent native graph with consistent edges and stale UI counters."""
    return (
        json.dumps(
            {
                "globalParams": [],
                "timeout": 30,
                "tenantId": -1,
                "tasks": [
                    {
                        "id": "tasks-extract",
                        "name": "extract",
                        "type": "SHELL",
                        "params": {"rawScript": "echo extract"},
                        "preTasks": [],
                        "runFlag": "NORMAL",
                    },
                    {
                        "id": "tasks-transform",
                        "name": "transform",
                        "type": "SHELL",
                        "params": {"rawScript": "echo transform"},
                        "preTasks": ["extract"],
                        "runFlag": "NORMAL",
                    },
                    {
                        "id": "tasks-load",
                        "name": "load",
                        "type": "SHELL",
                        "params": {"rawScript": "echo load"},
                        "preTasks": ["extract", "transform"],
                        "runFlag": "NORMAL",
                    },
                ],
            }
        ),
        json.dumps(
            {
                "tasks-extract": {
                    "name": "extract",
                    "targetarr": "",
                    "nodenumber": "6",
                    "x": 50,
                    "y": 80,
                },
                "tasks-transform": {
                    "name": "transform",
                    "targetarr": "tasks-extract",
                    "nodenumber": 0,
                    "x": 300,
                    "y": 80,
                },
                "tasks-load": {
                    "name": "load",
                    "targetarr": "tasks-extract,tasks-transform",
                    "nodenumber": 0,
                    "x": 550,
                    "y": 80,
                },
            }
        ),
        json.dumps(
            [
                {
                    "endPointSourceId": "tasks-extract",
                    "endPointTargetId": "tasks-transform",
                },
                {
                    "endPointSourceId": "tasks-extract",
                    "endPointTargetId": "tasks-load",
                },
                {
                    "endPointSourceId": "tasks-transform",
                    "endPointTargetId": "tasks-load",
                },
            ]
        ),
    )


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
            id=23,
            start_time_value="2026-01-01 00:00:00",
            end_time_value="2026-12-31 23:59:59",
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
    adhoc_dag = FakeDag(
        workflow_definition_value=adhoc,
        task_definition_list_value=[],
        workflow_task_relation_list_value=[],
    )
    return FakeWorkflowAdapter(
        workflows=[daily, adhoc],
        dags={101: dag, 102: adhoc_dag},
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


@pytest.fixture
def workflow_harness(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_workflow_adapter: FakeWorkflowAdapter,
    fake_task_adapter: FakeTaskAdapter,
    fake_schedule_adapter: FakeScheduleAdapter,
) -> _WorkflowServiceHarness:
    return _WorkflowServiceHarness(
        monkeypatch=monkeypatch,
        project_adapter=fake_project_adapter,
        workflow_adapter=fake_workflow_adapter,
        task_adapter=fake_task_adapter,
        schedule_adapter=fake_schedule_adapter,
    )
