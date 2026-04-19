import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.errors import ApiResultError
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeAccessToken,
    FakeAccessTokenAdapter,
    FakeProjectAdapter,
    FakeUser,
    FakeUserAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


@pytest.fixture
def fake_user_adapter() -> FakeUserAdapter:
    return FakeUserAdapter(
        users=[
            FakeUser(
                id=1,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=10,
                tenant_code_value="analytics",
            )
        ]
    )


@pytest.fixture
def fake_access_token_adapter(
    fake_user_adapter: FakeUserAdapter,
) -> FakeAccessTokenAdapter:
    return FakeAccessTokenAdapter(
        access_tokens=[
            FakeAccessToken(
                id=11,
                user_id_value=1,
                token="token-11",
                expire_time_value="2026-12-31 00:00:00",
                user_name_value="alice",
            )
        ],
        users=fake_user_adapter.users,
    )


@pytest.fixture(autouse=True)
def patch_access_token_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_user_adapter: FakeUserAdapter,
    fake_access_token_adapter: FakeAccessTokenAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            access_token_adapter=fake_access_token_adapter,
            user_adapter=fake_user_adapter,
            profile=make_profile(),
        ),
    )


def test_access_token_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["access-token", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["id"] == 11


def test_access_token_list_command_reports_permission_denied(
    fake_access_token_adapter: FakeAccessTokenAdapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny_list(*, page_no: int, page_size: int, search: str | None = None) -> object:
        del page_no, page_size, search
        raise ApiResultError(
            result_code=30001,
            result_message="user has no operation privilege",
        )

    monkeypatch.setattr(fake_access_token_adapter, "list", deny_list)

    result = runner.invoke(app, ["access-token", "list"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.list"
    assert payload["error"]["type"] == "permission_denied"
    assert payload["error"]["source"] == {
        "kind": "remote",
        "system": "dolphinscheduler",
        "layer": "result",
        "result_code": 30001,
        "result_message": "user has no operation privilege",
    }


def test_access_token_get_command_reports_not_found_suggestion() -> None:
    result = runner.invoke(app, ["access-token", "get", "99"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.get"
    assert payload["error"]["type"] == "not_found"
    assert payload["error"]["suggestion"] == (
        "Retry with `access-token list` to inspect available values and verify "
        "the selected id."
    )


def test_access_token_get_help_points_to_list_for_selector() -> None:
    result = runner.invoke(app, ["access-token", "get", "--help"])

    assert result.exit_code == 0
    assert "access-token" in result.stdout
    assert "list" in result.stdout


def test_access_token_create_command_returns_created_token() -> None:
    result = runner.invoke(
        app,
        [
            "access-token",
            "create",
            "--user",
            "alice",
            "--expire-time",
            "2027-01-01 00:00:00",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.create"
    assert payload["data"]["id"] == 12
    assert payload["data"]["userId"] == 1


def test_access_token_create_help_points_to_user_list_and_time_format() -> None:
    result = runner.invoke(app, ["access-token", "create", "--help"])

    assert result.exit_code == 0
    assert "dsctl user list" in result.stdout
    assert "2027-01-01" in result.stdout
    assert "00:00:00" in result.stdout


def test_access_token_update_command_can_regenerate_token() -> None:
    result = runner.invoke(
        app,
        [
            "access-token",
            "update",
            "11",
            "--regenerate-token",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.update"
    assert payload["data"]["token"] == "regenerated-token-11"


def test_access_token_update_command_requires_change() -> None:
    result = runner.invoke(app, ["access-token", "update", "11"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass at least one update flag such as --user, --expire-time, --token, "
        "or --regenerate-token."
    )


def test_access_token_update_command_rejects_conflicting_token_flags() -> None:
    result = runner.invoke(
        app,
        [
            "access-token",
            "update",
            "11",
            "--token",
            "manual-token",
            "--regenerate-token",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Choose exactly one token source: pass --token explicitly, or use "
        "--regenerate-token to mint a new token."
    )


def test_access_token_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["access-token", "delete", "11"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_access_token_generate_command_returns_token_string() -> None:
    result = runner.invoke(
        app,
        [
            "access-token",
            "generate",
            "--user",
            "alice",
            "--expire-time",
            "2027-01-01 00:00:00",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "access-token.generate"
    assert payload["data"] == {
        "token": "generated-token-1",
        "userId": 1,
        "expireTime": "2027-01-01 00:00:00",
    }
