import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeCluster,
    FakeClusterAdapter,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_cluster_adapter() -> FakeClusterAdapter:
    return FakeClusterAdapter(
        clusters=[
            FakeCluster(id=7, code=7, name="k8s-prod", config="kube-prod"),
            FakeCluster(id=9, code=9, name="k8s-dev", config="kube-dev"),
        ]
    )


@pytest.fixture(autouse=True)
def patch_cluster_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_cluster_adapter: FakeClusterAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            cluster_adapter=fake_cluster_adapter,
            profile=make_profile(),
        ),
    )


def test_cluster_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["cluster", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "cluster.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["name"] == "k8s-prod"


def test_cluster_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["cluster", "get", "k8s-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.get"
    assert payload["resolved"]["cluster"]["code"] == 7
    assert payload["data"]["config"] == "kube-prod"


def test_cluster_create_command_returns_created_cluster() -> None:
    result = runner.invoke(
        app,
        [
            "cluster",
            "create",
            "--name",
            "ops",
            "--config",
            "kube-ops",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.create"
    assert payload["data"]["name"] == "ops"
    assert payload["data"]["config"] == "kube-ops"


def test_cluster_update_command_returns_updated_cluster() -> None:
    result = runner.invoke(
        app,
        [
            "cluster",
            "update",
            "k8s-prod",
            "--config",
            "kube-changed",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.update"
    assert payload["data"]["name"] == "k8s-prod"
    assert payload["data"]["config"] == "kube-changed"


def test_cluster_update_command_rejects_conflicting_description_flags() -> None:
    result = runner.invoke(
        app,
        [
            "cluster",
            "update",
            "k8s-prod",
            "--description",
            "desc",
            "--clear-description",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.update"
    assert payload["error"]["type"] == "user_input_error"


def test_cluster_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["cluster", "delete", "k8s-prod"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_cluster_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["cluster", "delete", "k8s-prod", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "cluster.delete"
    assert payload["data"]["deleted"] is True
