import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeNamespace,
    FakeNamespaceAdapter,
    FakeProject,
    FakeProjectAdapter,
    FakeTenant,
    FakeTenantAdapter,
    FakeUser,
    FakeUserAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


def _tenants() -> list[FakeTenant]:
    return [
        FakeTenant(
            id=11,
            tenant_code_value="tenant-prod",
            queue_id_value=101,
            queue_name_value="default",
        ),
        FakeTenant(
            id=12,
            tenant_code_value="tenant-analytics",
            queue_id_value=102,
            queue_name_value="analytics",
        ),
    ]


def _projects() -> list[FakeProject]:
    return [
        FakeProject(
            code=701,
            name="etl-prod",
            description="Production ETL project",
            id=17,
        )
    ]


def _datasources() -> list[FakeDataSource]:
    return [
        FakeDataSource(
            id=7,
            name="warehouse",
            note="main warehouse",
            type_value=FakeEnumValue("MYSQL"),
        ),
        FakeDataSource(
            id=9,
            name="analytics",
            note="analytics postgres",
            type_value=FakeEnumValue("POSTGRESQL"),
        ),
    ]


def _namespaces() -> list[FakeNamespace]:
    return [
        FakeNamespace(
            id=21,
            namespace_value="etl-prod",
            cluster_code_value=9001,
            cluster_name_value="prod-cluster",
        ),
        FakeNamespace(
            id=22,
            namespace_value="etl-staging",
            cluster_code_value=9002,
            cluster_name_value="staging-cluster",
        ),
    ]


@pytest.fixture
def fake_tenant_adapter() -> FakeTenantAdapter:
    return FakeTenantAdapter(tenants=_tenants())


@pytest.fixture
def fake_project_adapter() -> FakeProjectAdapter:
    return FakeProjectAdapter(projects=_projects())


@pytest.fixture
def fake_datasource_adapter() -> FakeDataSourceAdapter:
    return FakeDataSourceAdapter(
        datasources=_datasources(),
        authorized_by_user_id={7: {7}},
    )


@pytest.fixture
def fake_namespace_adapter() -> FakeNamespaceAdapter:
    return FakeNamespaceAdapter(
        namespaces=_namespaces(),
        authorized_by_user_id={7: {21}},
    )


@pytest.fixture
def fake_user_adapter() -> FakeUserAdapter:
    return FakeUserAdapter(
        users=[
            FakeUser(
                id=7,
                user_name_value="alice",
                email="alice@example.com",
                phone="13800138000",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                queue_name_value="default",
                queue_value="default",
                state=1,
                time_zone_value="Asia/Shanghai",
                stored_queue_value="",
            ),
            FakeUser(
                id=9,
                user_name_value="bob",
                email="bob@example.com",
                user_type_value=FakeEnumValue("GENERAL_USER"),
                tenant_id_value=11,
                tenant_code_value="tenant-prod",
                queue_name_value="default",
                queue_value="analytics",
                state=0,
                stored_queue_value="analytics",
            ),
        ],
        tenants=_tenants(),
    )


@pytest.fixture(autouse=True)
def patch_user_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_project_adapter: FakeProjectAdapter,
    fake_datasource_adapter: FakeDataSourceAdapter,
    fake_namespace_adapter: FakeNamespaceAdapter,
    fake_user_adapter: FakeUserAdapter,
    fake_tenant_adapter: FakeTenantAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            fake_project_adapter,
            datasource_adapter=fake_datasource_adapter,
            namespace_adapter=fake_namespace_adapter,
            user_adapter=fake_user_adapter,
            tenant_adapter=fake_tenant_adapter,
            profile=make_profile(),
        ),
    )


def test_user_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["user", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "user.list"
    assert payload["data"]["total"] == 2
    assert payload["data"]["totalList"][0]["userName"] == "alice"


def test_user_list_command_reports_permission_denied(
    fake_user_adapter: FakeUserAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny_list(*, page_no: int, page_size: int, search: str | None = None) -> object:
        del page_no, page_size, search
        raise ApiResultError(
            result_code=30001,
            result_message="user has no operation privilege",
        )

    monkeypatch.setattr(fake_user_adapter, "list", deny_list)

    result = runner.invoke(app, ["user", "list"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.list"
    assert payload["error"]["type"] == "permission_denied"


def test_user_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["user", "get", "alice"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.get"
    assert payload["resolved"]["user"]["id"] == 7
    assert payload["data"]["timeZone"] == "Asia/Shanghai"


def test_user_get_help_points_to_list_for_selector() -> None:
    result = runner.invoke(app, ["user", "get", "--help"])

    assert result.exit_code == 0
    assert "user" in result.stdout
    assert "list" in result.stdout


def test_user_create_command_returns_created_user() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "create",
            "--user-name",
            "carol",
            "--password",
            "supersecret",
            "--email",
            "carol@example.com",
            "--tenant",
            "tenant-prod",
            "--state",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.create"
    assert payload["data"]["userName"] == "carol"
    assert payload["data"]["tenantId"] == 11


def test_user_create_help_points_to_tenant_and_queue_lists() -> None:
    result = runner.invoke(app, ["user", "create", "--help"])

    assert result.exit_code == 0
    assert "dsctl tenant list" in result.stdout
    assert "dsctl queue list" in result.stdout


def test_user_create_command_reports_upstream_input_suggestion(
    fake_user_adapter: FakeUserAdapter,
) -> None:
    fake_user_adapter.create_errors_by_name = {
        "carol": ApiResultError(
            result_code=10001,
            result_message="request params invalid",
        )
    }

    result = runner.invoke(
        app,
        [
            "user",
            "create",
            "--user-name",
            "carol",
            "--password",
            "supersecret",
            "--email",
            "carol@example.com",
            "--tenant",
            "tenant-prod",
            "--state",
            "1",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Verify --user-name, --password, --email, --tenant, --state, "
        "and optional --phone/--queue values, then retry."
    )


def test_user_update_command_supports_clear_queue() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "update",
            "bob",
            "--clear-queue",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.update"
    assert payload["data"]["userName"] == "bob"
    assert payload["data"]["queue"] == "default"
    assert payload["data"]["queueName"] == "default"


def test_user_update_command_requires_one_change_suggestion() -> None:
    result = runner.invoke(app, ["user", "update", "alice"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --user-name, --password, --email, "
        "--tenant, --state, --phone, --clear-phone, --queue, --clear-queue, or "
        "--time-zone."
    )


def test_user_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["user", "delete", "alice"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_user_delete_command_returns_deleted_confirmation() -> None:
    result = runner.invoke(app, ["user", "delete", "alice", "--force"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.delete"
    assert payload["data"]["deleted"] is True


def test_user_grant_project_command_returns_confirmation() -> None:
    result = runner.invoke(app, ["user", "grant", "project", "alice", "etl-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.grant.project"
    assert payload["data"]["granted"] is True
    assert payload["data"]["permission"] == "write"
    assert payload["resolved"]["project"]["code"] == 701


def test_user_grant_project_help_points_to_user_and_project_lists() -> None:
    result = runner.invoke(app, ["user", "grant", "project", "--help"])

    assert result.exit_code == 0
    assert "user" in result.stdout
    assert "list" in result.stdout
    assert "project" in result.stdout


def test_user_revoke_project_command_returns_confirmation() -> None:
    result = runner.invoke(app, ["user", "revoke", "project", "alice", "etl-prod"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.revoke.project"
    assert payload["data"]["revoked"] is True
    assert payload["resolved"]["user"]["id"] == 7


def test_user_grant_datasource_command_returns_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "grant",
            "datasource",
            "alice",
            "--datasource",
            "warehouse",
            "--datasource",
            "analytics",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.grant.datasource"
    assert payload["data"]["granted"] is True
    assert [item["id"] for item in payload["data"]["datasources"]] == [7, 9]


def test_user_grant_datasource_help_points_to_datasource_list() -> None:
    result = runner.invoke(app, ["user", "grant", "datasource", "--help"])

    assert result.exit_code == 0
    assert "datasource" in result.stdout
    assert "list" in result.stdout


def test_user_revoke_datasource_command_returns_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "revoke",
            "datasource",
            "alice",
            "--datasource",
            "warehouse",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.revoke.datasource"
    assert payload["data"]["revoked"] is True
    assert payload["data"]["datasources"] == []


def test_user_grant_namespace_command_returns_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "grant",
            "namespace",
            "alice",
            "--namespace",
            "etl-prod",
            "--namespace",
            "etl-staging",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.grant.namespace"
    assert payload["data"]["granted"] is True
    assert [item["id"] for item in payload["data"]["namespaces"]] == [21, 22]


def test_user_grant_namespace_help_points_to_namespace_list() -> None:
    result = runner.invoke(app, ["user", "grant", "namespace", "--help"])

    assert result.exit_code == 0
    assert "dsctl namespace list" in result.stdout


def test_user_revoke_namespace_command_returns_confirmation() -> None:
    result = runner.invoke(
        app,
        [
            "user",
            "revoke",
            "namespace",
            "alice",
            "--namespace",
            "etl-prod",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "user.revoke.namespace"
    assert payload["data"]["revoked"] is True
    assert payload["data"]["namespaces"] == []
