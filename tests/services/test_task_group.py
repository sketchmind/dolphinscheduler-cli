from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeEnumValue,
    FakeProject,
    FakeProjectAdapter,
    FakeTaskGroup,
    FakeTaskGroupAdapter,
    FakeTaskGroupQueue,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.context import SessionContext
from dsctl.errors import InvalidStateError, UserInputError
from dsctl.services import runtime as runtime_service
from dsctl.services import task_group as task_group_service


def _install_task_group_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    task_group_adapter: FakeTaskGroupAdapter,
    context: SessionContext | None = None,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            task_group_adapter=task_group_adapter,
            context=context,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


def test_list_task_groups_result_returns_first_page_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("YES"),
            ),
            FakeTaskGroup(
                id=9,
                name="ops",
                project_code_value=12,
                group_size_value=2,
                status_value=FakeEnumValue("NO"),
            ),
        ]
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    result = task_group_service.list_task_groups_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert {
        "total": data["total"],
        "totalPage": data["totalPage"],
        "pageSize": data["pageSize"],
        "currentPage": data["currentPage"],
        "pageNo": data["pageNo"],
    } == {
        "total": 2,
        "totalPage": 2,
        "pageSize": 1,
        "currentPage": 1,
        "pageNo": 1,
    }
    assert list(items) == [
        {
            "id": 7,
            "name": "etl",
            "projectCode": 11,
            "description": None,
            "groupSize": 4,
            "useSize": 0,
            "userId": 1,
            "status": "YES",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_list_task_groups_result_with_project_filter_resolves_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=11, name="etl-prod", description="prod")]
    )
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("YES"),
            ),
            FakeTaskGroup(
                id=9,
                name="ops",
                project_code_value=12,
                group_size_value=2,
                status_value=FakeEnumValue("NO"),
            ),
        ]
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    result = task_group_service.list_task_groups_result(project="etl-prod")
    data = _mapping(result.data)

    assert result.resolved == {
        "project": {
            "code": 11,
            "name": "etl-prod",
            "description": "prod",
            "source": "flag",
        },
        "search": None,
        "status": None,
        "page_no": 1,
        "page_size": 100,
        "all": False,
    }
    assert data["total"] == 1


def test_create_task_group_result_uses_selected_project_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(
        projects=[FakeProject(code=11, name="etl-prod", description="prod")]
    )
    task_group_adapter = FakeTaskGroupAdapter(task_groups=[])
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
        context=SessionContext(project="etl-prod"),
    )

    result = task_group_service.create_task_group_result(
        name="etl",
        group_size=4,
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "project": {
            "code": 11,
            "name": "etl-prod",
            "description": "prod",
            "source": "context",
        },
        "taskGroup": {
            "id": 1,
            "name": "etl",
            "projectCode": 11,
        },
    }
    assert data["name"] == "etl"
    assert data["projectCode"] == 11
    assert data["status"] == "YES"


def test_update_task_group_result_requires_one_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("YES"),
            )
        ]
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    with pytest.raises(UserInputError, match="at least one field change") as exc_info:
        task_group_service.update_task_group_result("etl")

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --name, --group-size, or --description."
    )


def test_list_task_groups_result_rejects_invalid_status_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(task_groups=[])
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    with pytest.raises(
        UserInputError,
        match="Task-group status filter must be one of",
    ) as exc_info:
        task_group_service.list_task_groups_result(status="paused")

    assert exc_info.value.suggestion == "Pass `open`/`closed` or `1`/`0`."


def test_close_task_group_result_returns_updated_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("YES"),
            )
        ]
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    result = task_group_service.close_task_group_result("etl")
    data = _mapping(result.data)

    assert result.resolved == {
        "taskGroup": {
            "id": 7,
            "name": "etl",
            "projectCode": 11,
        }
    }
    assert data["status"] == "NO"


def test_close_task_group_result_reports_reopen_suggestion_when_already_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("NO"),
            )
        ]
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    with pytest.raises(InvalidStateError, match="already closed") as exc_info:
        task_group_service.close_task_group_result("etl")

    assert exc_info.value.suggestion == "Run `dsctl task-group start etl` to reopen it."


def test_list_task_group_queues_result_returns_queue_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[
            FakeTaskGroup(
                id=7,
                name="etl",
                project_code_value=11,
                group_size_value=4,
                status_value=FakeEnumValue("YES"),
            )
        ],
        task_group_queues=[
            FakeTaskGroupQueue(
                id=31,
                task_id_value=101,
                task_name_value="extract",
                project_name_value="etl-prod",
                project_code_value="11",
                workflow_instance_name_value="daily-etl-1",
                group_id_value=7,
                workflow_instance_id_value=501,
                priority=2,
                force_start_value=0,
                in_queue_value=1,
                status_value=FakeEnumValue("WAIT_QUEUE"),
            )
        ],
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    result = task_group_service.list_task_group_queues_result("etl")
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert result.resolved == {
        "taskGroup": {
            "id": 7,
            "name": "etl",
            "projectCode": 11,
        },
        "taskInstance": None,
        "workflowInstance": None,
        "status": None,
        "page_no": 1,
        "page_size": 100,
        "all": False,
    }
    assert list(items) == [
        {
            "id": 31,
            "taskId": 101,
            "taskName": "extract",
            "projectName": "etl-prod",
            "projectCode": "11",
            "workflowInstanceName": "daily-etl-1",
            "groupId": 7,
            "workflowInstanceId": 501,
            "priority": 2,
            "forceStart": 0,
            "inQueue": 1,
            "status": "WAIT_QUEUE",
            "createTime": None,
            "updateTime": None,
        }
    ]


def test_force_start_task_group_queue_result_returns_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[],
        task_group_queues=[
            FakeTaskGroupQueue(
                id=31,
                task_id_value=101,
                task_name_value="extract",
                project_name_value="etl-prod",
                project_code_value="11",
                workflow_instance_name_value="daily-etl-1",
                group_id_value=7,
            )
        ],
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    result = task_group_service.force_start_task_group_queue_result(31)

    assert result.data == {
        "queueId": 31,
        "forceStarted": True,
    }


def test_force_start_task_group_queue_result_reports_already_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_adapter = FakeProjectAdapter(projects=[])
    task_group_adapter = FakeTaskGroupAdapter(
        task_groups=[],
        task_group_queues=[
            FakeTaskGroupQueue(
                id=31,
                task_id_value=101,
                task_name_value="extract",
                project_name_value="etl-prod",
                project_code_value="11",
                workflow_instance_name_value="daily-etl-1",
                group_id_value=7,
                force_start_value=1,
                status_value=FakeEnumValue("ACQUIRE_SUCCESS"),
            )
        ],
    )
    _install_task_group_service_fakes(
        monkeypatch,
        project_adapter=project_adapter,
        task_group_adapter=task_group_adapter,
    )

    with pytest.raises(
        InvalidStateError,
        match="already acquired task-group resources",
    ) as exc_info:
        task_group_service.force_start_task_group_queue_result(31)

    assert exc_info.value.suggestion == (
        "No need to force-start it again; the queue item has already acquired "
        "task-group resources."
    )
