import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeQueue,
    FakeQueueAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_queue_adapter() -> FakeQueueAdapter:
    return FakeQueueAdapter(
        queues=[
            FakeQueue(id=7, queue_name_value="default", queue="root.default"),
            FakeQueue(id=9, queue_name_value="analytics", queue="root.analytics"),
        ]
    )


@pytest.fixture(autouse=True)
def patch_queue_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_queue_adapter: FakeQueueAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            queue_adapter=fake_queue_adapter,
            profile=make_profile(),
        ),
    )


def test_queue_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["queue", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "queue.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["queueName"] == "default"


def test_queue_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["queue", "get", "default"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.get"
    assert payload["resolved"]["queue"]["id"] == 7
    assert payload["data"]["queue"] == "root.default"


def test_queue_create_command_returns_created_queue() -> None:
    result = runner.invoke(
        app,
        [
            "queue",
            "create",
            "--queue-name",
            "ops",
            "--queue",
            "root.ops",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.create"
    assert payload["data"]["queueName"] == "ops"
    assert payload["data"]["queue"] == "root.ops"


def test_queue_update_command_returns_updated_queue() -> None:
    result = runner.invoke(
        app,
        [
            "queue",
            "update",
            "default",
            "--queue",
            "root.changed",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.update"
    assert payload["data"]["queueName"] == "default"
    assert payload["data"]["queue"] == "root.changed"


def test_queue_update_command_requires_one_change() -> None:
    result = runner.invoke(app, ["queue", "update", "default"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --queue-name or --queue."
    )


def test_queue_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["queue", "delete", "default"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_queue_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["queue", "delete", "default", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "queue.delete"
    assert payload["data"]["deleted"] is True
