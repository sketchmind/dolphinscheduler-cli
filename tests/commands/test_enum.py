import json

from typer.testing import CliRunner

from dsctl.app import app

runner = CliRunner()


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


def test_enum_list_command_rejects_unknown_enum() -> None:
    result = runner.invoke(app, ["enum", "list", "missing-enum"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "enum.list"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Run `capabilities` and inspect `data.enums.names` to choose a "
        "supported enum name."
    )
