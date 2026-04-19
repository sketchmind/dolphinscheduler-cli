import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeNamespace,
    FakeNamespaceAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


def _namespaces() -> list[FakeNamespace]:
    return [
        FakeNamespace(
            id=21,
            code=7001,
            namespace_value="etl-prod",
            cluster_code_value=9001,
            cluster_name_value="prod-cluster",
            user_id_value=1,
            user_name_value="admin",
        ),
        FakeNamespace(
            id=22,
            code=7002,
            namespace_value="etl-staging",
            cluster_code_value=9002,
            cluster_name_value="staging-cluster",
            user_id_value=1,
            user_name_value="admin",
        ),
    ]


@pytest.fixture
def fake_namespace_adapter() -> FakeNamespaceAdapter:
    return FakeNamespaceAdapter(namespaces=_namespaces())


@pytest.fixture(autouse=True)
def patch_namespace_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_namespace_adapter: FakeNamespaceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            namespace_adapter=fake_namespace_adapter,
            profile=make_profile(),
        ),
    )


def test_namespace_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["namespace", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "namespace.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["namespace"] == "etl-prod"


def test_namespace_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["namespace", "get", "etl-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.get"
    assert payload["resolved"]["namespace"]["id"] == 21
    assert payload["data"]["clusterCode"] == 9001


def test_namespace_selector_help_points_to_list_discovery() -> None:
    result = runner.invoke(app, ["namespace", "get", "--help"])

    assert result.exit_code == 0
    assert "namespace" in result.stdout
    assert "list" in result.stdout


def test_namespace_available_command_returns_current_user_visible_set() -> None:
    result = runner.invoke(app, ["namespace", "available"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.available"
    assert payload["resolved"] == {"scope": "current_user"}
    assert payload["data"][0]["namespace"] == "etl-prod"


def test_namespace_create_command_returns_created_namespace() -> None:
    result = runner.invoke(
        app,
        [
            "namespace",
            "create",
            "--namespace",
            "etl-ops",
            "--cluster-code",
            "9003",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.create"
    assert payload["data"]["namespace"] == "etl-ops"
    assert payload["resolved"]["namespace"]["id"] == 23


def test_namespace_create_help_points_to_cluster_discovery() -> None:
    result = runner.invoke(app, ["namespace", "create", "--help"])

    assert result.exit_code == 0
    assert "dsctl cluster list" in result.stdout


def test_namespace_create_command_reports_upstream_input_suggestion(
    fake_namespace_adapter: FakeNamespaceAdapter,
) -> None:
    fake_namespace_adapter.create_error = ApiResultError(
        result_code=10001,
        result_message="request params invalid",
    )

    result = runner.invoke(
        app,
        [
            "namespace",
            "create",
            "--namespace",
            "etl-ops",
            "--cluster-code",
            "9003",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Verify --namespace and --cluster-code, then retry."
    )


def test_namespace_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["namespace", "delete", "etl-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_namespace_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["namespace", "delete", "etl-prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "namespace.delete"
    assert payload["data"]["deleted"] is True
