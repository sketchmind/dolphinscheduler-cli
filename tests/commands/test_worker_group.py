import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeWorkerGroup,
    FakeWorkerGroupAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_worker_group_adapter() -> FakeWorkerGroupAdapter:
    return FakeWorkerGroupAdapter(
        worker_groups=[
            FakeWorkerGroup(id=7, name="default", addr_list_value="worker-a:1234"),
            FakeWorkerGroup(id=9, name="analytics", addr_list_value="worker-b:1234"),
        ]
    )


@pytest.fixture(autouse=True)
def patch_worker_group_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_worker_group_adapter: FakeWorkerGroupAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            worker_group_adapter=fake_worker_group_adapter,
            profile=make_profile(),
        ),
    )


def test_worker_group_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["worker-group", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "worker-group.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["name"] == "default"


def test_worker_group_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["worker-group", "get", "default"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.get"
    assert payload["resolved"]["workerGroup"]["id"] == 7
    assert payload["data"]["addrList"] == "worker-a:1234"


def test_worker_group_selector_help_points_to_list_discovery() -> None:
    result = runner.invoke(app, ["worker-group", "get", "--help"])

    assert result.exit_code == 0
    assert "worker-group" in result.stdout
    assert "list" in result.stdout


def test_worker_group_create_command_returns_created_worker_group() -> None:
    result = runner.invoke(
        app,
        [
            "worker-group",
            "create",
            "--name",
            "ops",
            "--addr",
            "worker-a:1234",
            "--addr",
            "worker-b:1234",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.create"
    assert payload["data"]["name"] == "ops"
    assert payload["data"]["addrList"] == "worker-a:1234,worker-b:1234"


def test_worker_group_create_help_points_to_worker_server_discovery() -> None:
    result = runner.invoke(app, ["worker-group", "create", "--help"])

    assert result.exit_code == 0
    assert "monitor server" in result.stdout
    assert "worker" in result.stdout


def test_worker_group_update_command_returns_updated_worker_group() -> None:
    result = runner.invoke(
        app,
        [
            "worker-group",
            "update",
            "default",
            "--addr",
            "worker-c:1234",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.update"
    assert payload["data"]["name"] == "default"
    assert payload["data"]["addrList"] == "worker-c:1234"


def test_worker_group_update_command_requires_one_change() -> None:
    result = runner.invoke(app, ["worker-group", "update", "default"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --name, --addr, or --description."
    )


def test_worker_group_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["worker-group", "delete", "default"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_worker_group_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["worker-group", "delete", "default", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "worker-group.delete"
    assert payload["data"]["deleted"] is True
