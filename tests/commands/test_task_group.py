import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import SessionContext
from dsctl.services import runtime as runtime_service
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

runner = CliRunner()


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(
        projects=[FakeProject(code=11, name="etl-prod", description="prod")]
    )


@pytest.fixture
def fake_task_group_adapter() -> FakeTaskGroupAdapter:
    return FakeTaskGroupAdapter(
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


@pytest.fixture(autouse=True)
def patch_task_group_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_task_group_adapter: FakeTaskGroupAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            task_group_adapter=fake_task_group_adapter,
            context=SessionContext(project="etl-prod"),
            profile=make_profile(),
        ),
    )


def test_task_group_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["task-group", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["name"] == "etl"


def test_task_group_list_command_reports_status_choices() -> None:
    result = runner.invoke(app, ["task-group", "list", "--status", "paused"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Pass `open`/`closed` or `1`/`0`."


def test_task_group_create_command_uses_project_selection() -> None:
    result = runner.invoke(
        app,
        [
            "task-group",
            "create",
            "--name",
            "ops",
            "--group-size",
            "2",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.create"
    assert payload["resolved"]["project"]["source"] == "context"
    assert payload["data"]["name"] == "ops"
    assert payload["data"]["projectCode"] == 11


def test_task_group_queue_list_command_returns_queue_payload() -> None:
    result = runner.invoke(app, ["task-group", "queue", "list", "etl"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.queue.list"
    assert payload["resolved"]["taskGroup"]["id"] == 7
    assert payload["data"]["totalList"][0]["status"] == "WAIT_QUEUE"


def test_task_group_queue_force_start_command_returns_confirmation() -> None:
    result = runner.invoke(app, ["task-group", "queue", "force-start", "31"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.queue.force-start"
    assert payload["data"] == {"queueId": 31, "forceStarted": True}


def test_task_group_queue_force_start_command_reports_already_started(
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
                force_start_value=1,
                in_queue_value=1,
                status_value=FakeEnumValue("ACQUIRE_SUCCESS"),
            )
        ],
    )
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            project_adapter,
            task_group_adapter=task_group_adapter,
            context=SessionContext(project="etl-prod"),
            profile=make_profile(),
        ),
    )

    result = runner.invoke(app, ["task-group", "queue", "force-start", "31"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-group.queue.force-start"
    assert payload["error"]["type"] == "invalid_state"
    assert payload["error"]["suggestion"] == (
        "No need to force-start it again; the queue item has already acquired "
        "task-group resources."
    )
