import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeQueue,
    FakeQueueAdapter,
    FakeTenant,
    FakeTenantAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


def _queues() -> list[FakeQueue]:
    return [
        FakeQueue(id=11, queue_name_value="default", queue="root.default"),
        FakeQueue(id=12, queue_name_value="analytics", queue="root.analytics"),
    ]


@pytest.fixture
def fake_queue_adapter() -> FakeQueueAdapter:
    return FakeQueueAdapter(queues=_queues())


@pytest.fixture
def fake_tenant_adapter() -> FakeTenantAdapter:
    return FakeTenantAdapter(
        tenants=[
            FakeTenant(
                id=7,
                tenant_code_value="tenant-prod",
                queue_id_value=11,
                queue_name_value="default",
                queue_value="root.default",
            ),
            FakeTenant(
                id=9,
                tenant_code_value="tenant-analytics",
                queue_id_value=12,
                queue_name_value="analytics",
                queue_value="root.analytics",
            ),
        ],
        queues=_queues(),
    )


@pytest.fixture(autouse=True)
def patch_tenant_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_tenant_adapter: FakeTenantAdapter,
    fake_queue_adapter: FakeQueueAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            tenant_adapter=fake_tenant_adapter,
            queue_adapter=fake_queue_adapter,
            profile=make_profile(),
        ),
    )


def test_tenant_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["tenant", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "tenant.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["tenantCode"] == "tenant-prod"


def test_tenant_get_command_resolves_code() -> None:
    result = runner.invoke(app, ["tenant", "get", "tenant-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.get"
    assert payload["resolved"]["tenant"]["id"] == 7
    assert payload["data"]["queueName"] == "default"


def test_tenant_create_command_returns_created_tenant() -> None:
    result = runner.invoke(
        app,
        [
            "tenant",
            "create",
            "--tenant-code",
            "tenant-ops",
            "--queue",
            "default",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.create"
    assert payload["data"]["tenantCode"] == "tenant-ops"
    assert payload["data"]["queueId"] == 11


def test_tenant_update_command_returns_updated_tenant() -> None:
    result = runner.invoke(
        app,
        [
            "tenant",
            "update",
            "tenant-prod",
            "--queue",
            "analytics",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.update"
    assert payload["data"]["tenantCode"] == "tenant-prod"
    assert payload["data"]["queueId"] == 12


def test_tenant_update_command_requires_one_change() -> None:
    result = runner.invoke(app, ["tenant", "update", "tenant-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --tenant-code, --queue, or "
        "--description."
    )


def test_tenant_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["tenant", "delete", "tenant-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_tenant_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["tenant", "delete", "tenant-prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "tenant.delete"
    assert payload["data"]["deleted"] is True
