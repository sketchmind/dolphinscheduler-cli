import pytest
from tests.fakes import FakeTaskAdapter

from dsctl.errors import ApiResultError, ApiTransportError
from dsctl.services._task_code_allocation import allocate_server_task_codes


def test_server_task_code_allocation_translates_result_error() -> None:
    adapter = FakeTaskAdapter(
        workflow_tasks={},
        generate_codes_error=ApiResultError(
            result_code=12345,
            result_message="allocation failed",
        ),
    )

    with pytest.raises(ApiTransportError) as exc_info:
        allocate_server_task_codes(
            2,
            adapter=adapter,
            project_code=7,
            action="workflow.create",
        )

    error = exc_info.value
    assert type(error) is ApiTransportError
    assert error.details == {
        "resource": "project",
        "project_code": 7,
        "action": "workflow.create",
        "task_code_count": 2,
        "result_code": 12345,
        "result_message": "allocation failed",
    }
    assert error.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 12345,
        "result_message": "allocation failed",
    }
    assert error.suggestion == (
        "Retry the workflow mutation after checking server health."
    )
    assert adapter.generate_code_calls == [{"project_code": 7, "count": 2}]


def test_server_task_code_allocation_enriches_transport_error() -> None:
    adapter = FakeTaskAdapter(
        workflow_tasks={},
        generate_codes_error=ApiTransportError(
            "Malformed task-code response",
            details={"endpoint": "/projects/7/task-definition/gen-task-codes"},
            source={
                "kind": "remote",
                "system": "dolphinscheduler",
                "layer": "response",
            },
            suggestion="Verify the server response contract, then retry.",
        ),
    )

    with pytest.raises(ApiTransportError) as exc_info:
        allocate_server_task_codes(
            2,
            adapter=adapter,
            project_code=7,
            action="workflow.create",
        )

    error = exc_info.value
    assert error.details == {
        "endpoint": "/projects/7/task-definition/gen-task-codes",
        "resource": "project",
        "project_code": 7,
        "action": "workflow.create",
        "task_code_count": 2,
    }
    assert error.source == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "response",
    }
    assert error.suggestion == "Verify the server response contract, then retry."
    assert isinstance(error.__cause__, ApiTransportError)
