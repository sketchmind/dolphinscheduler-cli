from collections.abc import Mapping, Sequence
from typing import cast

import pytest
from tests.fakes import (
    FakeDependentLineageTask,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskAdapter,
    FakeTaskDefinition,
    FakeWorkflow,
    FakeWorkflowAdapter,
    FakeWorkflowLineage,
    FakeWorkflowLineageAdapter,
    FakeWorkflowLineageDetail,
    FakeWorkflowLineageRelation,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import ApiResultError, InvalidStateError
from dsctl.services import runtime as runtime_service
from dsctl.services import workflow_lineage as workflow_lineage_service


@pytest.fixture(autouse=True)
def patch_workflow_lineage_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )
    workflow_adapter = FakeWorkflowAdapter(
        workflows=[
            FakeWorkflow(
                code=101,
                name="daily-sync",
                project_code_value=7,
            )
        ],
        dags={},
    )
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
                    )
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
                    )
                ],
            )
        },
        dependent_tasks_by_target={
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
            ]
        },
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod", workflow="daily-sync"),
            workflow_adapter=workflow_adapter,
            workflow_lineage_adapter=workflow_lineage_adapter,
            task_adapter=task_adapter,
        ),
    )


def test_list_workflow_lineage_result_returns_project_graph() -> None:
    result = workflow_lineage_service.list_workflow_lineage_result()
    resolved = _mapping(result.resolved)

    assert _mapping(resolved["project"])["source"] == "context"
    assert result.data == {
        "workFlowRelationList": [
            {"sourceWorkFlowCode": 101, "targetWorkFlowCode": 102}
        ],
        "workFlowRelationDetailList": [
            {
                "workFlowCode": 101,
                "workFlowName": "daily-sync",
                "workFlowPublishStatus": "ONLINE",
                "scheduleStartTime": None,
                "scheduleEndTime": None,
                "crontab": None,
                "schedulePublishStatus": 0,
                "sourceWorkFlowCode": None,
            }
        ],
    }


def test_get_workflow_lineage_result_uses_workflow_context() -> None:
    result = workflow_lineage_service.get_workflow_lineage_result(None)
    resolved = _mapping(result.resolved)
    data = _mapping(result.data)

    assert _mapping(resolved["workflow"])["source"] == "context"
    assert data["workFlowRelationList"] == [
        {"sourceWorkFlowCode": 101, "targetWorkFlowCode": 102}
    ]


def test_list_workflow_dependent_tasks_result_resolves_task_selector() -> None:
    result = workflow_lineage_service.list_workflow_dependent_tasks_result(
        None,
        task="extract",
    )
    resolved = _mapping(result.resolved)

    assert _mapping(resolved["task"])["name"] == "extract"
    assert result.data == [
        {
            "projectCode": 7,
            "workflowDefinitionCode": 102,
            "workflowDefinitionName": "quality-check",
            "taskDefinitionCode": 302,
            "taskDefinitionName": "depends-on-extract",
        }
    ]


def test_list_workflow_lineage_result_translates_upstream_query_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=7, name="etl-prod")]
    )

    class ErrorWorkflowLineageAdapter:
        def list(self, *, project_code: int) -> None:
            del project_code
            raise ApiResultError(
                result_code=10161,
                result_message="query workflow lineage error",
            )

        def get(self, *, project_code: int, workflow_code: int) -> None:
            del project_code, workflow_code
            message = "unexpected call"
            raise AssertionError(message)

        def query_dependent_tasks(
            self,
            *,
            project_code: int,
            workflow_code: int,
            task_code: int | None = None,
        ) -> Sequence[object]:
            del project_code, workflow_code, task_code
            message = "unexpected call"
            raise AssertionError(message)

    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            profile=make_profile(),
            context=SessionContext(project="etl-prod"),
            workflow_lineage_adapter=cast(
                "FakeWorkflowLineageAdapter",
                ErrorWorkflowLineageAdapter(),
            ),
        ),
    )

    with pytest.raises(InvalidStateError) as exc_info:
        workflow_lineage_service.list_workflow_lineage_result()

    error = exc_info.value
    assert error.details["result_code"] == 10161
    assert error.source is not None
    assert error.source["type"] == "api_result_error"


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value
