import json
from pathlib import Path

from typer.testing import CliRunner

from dsctl.app import app

runner = CliRunner()


def test_enum_names_command_returns_supported_enum_names() -> None:
    result = runner.invoke(app, ["enum", "names"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "enum.names"
    assert "view" not in payload["resolved"]["enum"]
    assert {"name": "priority", "list_command": "dsctl enum list priority"} in (
        payload["data"]
    )


def test_enum_names_command_can_render_table_rows() -> None:
    result = runner.invoke(app, ["--format", "table", "enum", "names"])

    assert result.exit_code == 0
    assert "name" in result.stdout
    assert "list_command" in result.stdout
    assert "dsctl enum list priority" in result.stdout


def test_enum_list_command_returns_enum_members() -> None:
    result = runner.invoke(app, ["enum", "list", "priority"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "enum.list"
    assert payload["resolved"]["enum"]["name"] == "priority"
    assert payload["data"]["module"] == "common.enums.priority"
    assert payload["data"]["members"][0]["name"] == "HIGHEST"


def test_enum_list_command_accepts_class_name_alias() -> None:
    result = runner.invoke(app, ["enum", "list", "ReleaseState"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["resolved"]["enum"]["name"] == "release-state"
    assert payload["data"]["member_count"] == 2


def test_enum_discovery_uses_the_exact_342_runtime_slice(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )

    names_result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "enum", "names"],
    )
    list_result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "enum", "list", "priority"],
    )

    assert names_result.exit_code == 0
    assert list_result.exit_code == 0
    names_payload = json.loads(names_result.stdout)
    list_payload = json.loads(list_result.stdout)
    assert {item["name"] for item in names_payload["data"]} >= {"priority"}
    assert list_payload["resolved"]["enum"]["name"] == "priority"


def test_cli_preflight_remains_fail_closed_for_an_unsupported_legacy_action(
    isolated_cwd: Path,
) -> None:
    (isolated_cwd / "cluster.env").write_text(
        "DS_VERSION=3.0.6\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        ["--env-file", "cluster.env", "task-type", "list"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "unsupported_feature"
    assert payload["error"]["details"]["selected_version"] == "3.0.6"


def test_enum_list_command_rejects_unknown_enum() -> None:
    result = runner.invoke(app, ["enum", "list", "missing-enum"])

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["action"] == "enum.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `dsctl enum names` to choose a supported enum name."
    )
