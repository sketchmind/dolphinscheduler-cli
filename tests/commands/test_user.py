import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.output import CommandResult
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
)
from tests.security_fakes import install_user_service_fakes
from tests.support import normalize_cli_help

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
    install_user_service_fakes(
        monkeypatch,
        fake_user_adapter,
        fake_tenant_adapter,
        project_adapter=fake_project_adapter,
        datasource_adapter=fake_datasource_adapter,
        namespace_adapter=fake_namespace_adapter,
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
    payload = json.loads(result.stderr)
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


@pytest.mark.parametrize("from_stdin", [False, True])
def test_user_create_reads_password_without_exposing_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    from_stdin: bool,
) -> None:
    secret_file = tmp_path / "password.txt"
    secret_file.write_text("secret-value\n", encoding="utf-8")
    received: list[object] = []

    def create(**kwargs: object) -> CommandResult:
        received.append(kwargs["password"])
        return CommandResult(data={"userName": "carol"})

    monkeypatch.setattr("dsctl.commands.user.create_user_result", create)
    result = runner.invoke(
        app,
        [
            "user",
            "create",
            "--user-name",
            "carol",
            "--email",
            "carol@example.com",
            "--tenant",
            "tenant-prod",
            "--state",
            "1",
            "--password-file",
            "-" if from_stdin else str(secret_file),
        ],
        input="secret-value\n" if from_stdin else None,
    )
    assert result.exit_code == 0
    assert received == ["secret-value"]
    assert "secret-value" not in result.output


def test_user_password_sources_conflict_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected(**kwargs: object) -> CommandResult:
        pytest.fail("invalid password sources must not call the mutation service")

    monkeypatch.setattr("dsctl.commands.user.update_user_result", unexpected)
    result = runner.invoke(
        app,
        [
            "user",
            "update",
            "alice",
            "--password",
            "secret-value",
            "--password-file",
            "-",
        ],
        input="another-secret\n",
    )
    assert result.exit_code == 1
    assert json.loads(result.stderr)["error"]["type"] == "user_input_error"
    assert "secret-value" not in result.output
    assert "another-secret" not in result.output


def test_user_create_help_points_to_tenant_and_queue_lists() -> None:
    result = runner.invoke(app, ["user", "create", "--help"])
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "dsctl tenant list" in help_text
    assert "dsctl queue list" in help_text


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
    payload = json.loads(result.stderr)
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
    payload = json.loads(result.stderr)
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
    payload = json.loads(result.stderr)
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
    assert payload["data"]["verification"] == "membership_only"
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
    help_text = normalize_cli_help(result.stdout)

    assert result.exit_code == 0
    assert "dsctl namespace list" in help_text


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
