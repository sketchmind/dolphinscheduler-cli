import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeDataSource,
    FakeDataSourceAdapter,
    FakeEnumValue,
    FakeProjectAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

runner = CliRunner()


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.fixture
def fake_datasource_adapter() -> FakeDataSourceAdapter:
    return FakeDataSourceAdapter(
        datasources=[
            FakeDataSource(
                id=7,
                name="warehouse",
                note="main warehouse",
                type_value=FakeEnumValue("MYSQL"),
                detail_payload_value={
                    "host": "db.example",
                    "port": 3306,
                    "password": "******",
                },
            )
        ],
        connection_test_results={7: True},
    )


@pytest.fixture(autouse=True)
def patch_datasource_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_datasource_adapter: FakeDataSourceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            datasource_adapter=fake_datasource_adapter,
            profile=make_profile(),
        ),
    )


def test_datasource_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["datasource", "list", "--page-size", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "datasource.list"
    assert payload["data"]["total"] == 1
    assert payload["data"]["totalList"][0]["name"] == "warehouse"


def test_datasource_get_command_resolves_name() -> None:
    result = runner.invoke(app, ["datasource", "get", "warehouse"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.get"
    assert payload["resolved"]["datasource"]["id"] == 7
    assert payload["data"]["host"] == "db.example"


def test_datasource_create_command_returns_created_payload(tmp_path: Path) -> None:
    file = _write_json(
        tmp_path / "create.json",
        {
            "name": "analytics",
            "type": "POSTGRESQL",
            "password": "secret",
        },
    )

    result = runner.invoke(app, ["datasource", "create", "--file", str(file)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.create"
    assert payload["data"]["name"] == "analytics"
    assert payload["resolved"]["datasource"]["id"] == 8


def test_datasource_create_command_rejects_unknown_type(tmp_path: Path) -> None:
    file = _write_json(
        tmp_path / "unknown.json",
        {
            "name": "analytics",
            "type": "UNKNOWN",
            "password": "secret",
        },
    )

    result = runner.invoke(app, ["datasource", "create", "--file", str(file)])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl template datasource` to choose a supported datasource type, "
        "then `dsctl template datasource --type TYPE`."
    )


def test_datasource_create_command_rejects_payload_with_id(tmp_path: Path) -> None:
    file = _write_json(
        tmp_path / "with-id.json",
        {
            "id": 7,
            "name": "analytics",
            "type": "POSTGRESQL",
            "password": "secret",
        },
    )

    result = runner.invoke(app, ["datasource", "create", "--file", str(file)])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Remove `id` from the create payload; DS assigns it."
    )


def test_datasource_update_command_emits_password_preservation_warning(
    tmp_path: Path,
) -> None:
    file = _write_json(
        tmp_path / "update.json",
        {
            "id": 7,
            "name": "warehouse",
            "type": "MYSQL",
            "password": "******",
        },
    )

    result = runner.invoke(
        app,
        ["datasource", "update", "warehouse", "--file", str(file)],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.update"
    assert payload["warnings"] == [
        "datasource update: masked password placeholder detected; "
        "preserving the existing password"
    ]
    assert payload["warning_details"] == [
        {
            "code": "datasource_update_preserved_existing_password",
            "message": (
                "datasource update: masked password placeholder detected; "
                "preserving the existing password"
            ),
            "field": "password",
            "reason": "masked_placeholder",
            "preserved_existing": True,
        }
    ]


def test_datasource_update_command_rejects_mismatched_payload_id(
    tmp_path: Path,
) -> None:
    file = _write_json(
        tmp_path / "mismatch.json",
        {
            "id": 9,
            "name": "warehouse",
            "type": "MYSQL",
        },
    )

    result = runner.invoke(
        app,
        ["datasource", "update", "warehouse", "--file", str(file)],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.update"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Update the payload `id` to match the selected datasource, or remove "
        "`id` and let the CLI target control the update."
    )


def test_datasource_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["datasource", "delete", "warehouse"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."


def test_datasource_test_command_returns_connection_result() -> None:
    result = runner.invoke(app, ["datasource", "test", "warehouse"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "datasource.test"
    assert payload["data"]["connected"] is True
