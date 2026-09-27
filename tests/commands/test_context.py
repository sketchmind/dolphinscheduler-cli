from __future__ import annotations

import json
from typing import TYPE_CHECKING

import httpx
import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.context import registry_path
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_user_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.delenv("DSCTL_CONTEXT", raising=False)
    monkeypatch.delenv("DSCTL_ENV_FILE", raising=False)
    monkeypatch.chdir(tmp_path)

    def reject_remote(*args: object, **kwargs: object) -> None:
        pytest.fail("Local context management must not make HTTP requests")

    monkeypatch.setattr(httpx.Client, "send", reject_remote)


@pytest.fixture
def connection_file(tmp_path: Path) -> Path:
    path = tmp_path / "production.env"
    path.write_text(
        "DS_API_URL=https://production.example/dolphinscheduler\n"
        "DS_API_TOKEN=private-connection-secret\nDS_VERSION=3.4.1\n"
    )
    return path


def invoke(*args: str) -> Mapping[str, object]:
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return _mapping(json.loads(result.stdout))


def test_context_management_references_file_and_never_copies_credentials(
    connection_file: Path,
) -> None:
    created = invoke(
        "context",
        "create",
        "production",
        "--file",
        str(connection_file),
        "--project",
        "etl",
    )
    assert created["data"] == {
        "name": "production",
        "env_file": str(connection_file.resolve()),
        "api_url": "https://production.example/dolphinscheduler",
        "project": "etl",
    }
    assert _mapping(created["resolved"])["remote_validation"] == "not_performed"
    assert "private-connection-secret" not in registry_path().read_text()
    connection_file.unlink()
    assert _mapping(invoke("context", "get", "production")["data"])["project"] == "etl"
    assert (
        _mapping(_sequence(invoke("context", "list")["data"])[0])["name"]
        == "production"
    )
    assert (
        _mapping(invoke("context", "update", "production", "--clear-project")["data"])[
            "project"
        ]
        is None
    )
    assert (
        _mapping(invoke("context", "delete", "production")["data"])["deleted"] is True
    )
    assert invoke("context", "list")["data"] == []


def test_context_create_rejects_incomplete_file_without_using_process_credentials(
    monkeypatch: pytest.MonkeyPatch,
    connection_file: Path,
) -> None:
    monkeypatch.setenv("DS_API_TOKEN", "process-token")
    connection_file.write_text("DS_API_URL=https://production.example\n")
    result = runner.invoke(
        app, ["context", "create", "production", "--file", str(connection_file)]
    )
    assert result.exit_code != 0
    assert not registry_path().exists()
    assert "process-token" not in result.output


def test_context_file_replacement_clears_project_and_rebinds_url(
    connection_file: Path, tmp_path: Path
) -> None:
    invoke(
        "context",
        "create",
        "production",
        "--file",
        str(connection_file),
        "--project",
        "etl",
    )
    replacement = tmp_path / "replacement.env"
    replacement.write_text(
        "DS_API_URL=https://replacement.example\nDS_API_TOKEN=replacement-token\n"
    )
    updated = invoke("context", "update", "production", "--file", str(replacement))
    assert _mapping(updated["data"])["project"] is None
    assert _mapping(updated["data"])["api_url"] == "https://replacement.example"


def test_context_update_rejects_empty_and_conflicting_changes(
    connection_file: Path,
) -> None:
    invoke("context", "create", "production", "--file", str(connection_file))
    for options in ([], ["--project", "etl", "--clear-project"]):
        result = runner.invoke(app, ["context", "update", "production", *options])
        assert result.exit_code != 0
        assert json.loads(result.stderr)["error"]["type"] == "user_input_error"


def test_config_default_and_effective_selection_are_distinct(
    monkeypatch: pytest.MonkeyPatch, connection_file: Path
) -> None:
    invoke(
        "context",
        "create",
        "production",
        "--file",
        str(connection_file),
        "--project",
        "etl",
    )
    monkeypatch.setenv("DS_API_URL", "https://override.example")
    monkeypatch.setenv("DS_API_TOKEN", "override-token")
    written = invoke("config", "set", "default-context", "production")
    assert written["data"] == {"key": "default-context", "value": "production"}
    assert _mapping(written["resolved"])["saved"] is True
    assert _mapping(_mapping(written["resolved"])["effective"])["context"] is None
    assert (
        _mapping(invoke("config", "get", "default-context")["data"])["value"]
        == "production"
    )
    current = invoke("context")
    assert _mapping(current["data"])["api_url"] == "https://override.example"
    assert _mapping(current["data"])["project"] is None
    monkeypatch.delenv("DS_API_URL")
    monkeypatch.delenv("DS_API_TOKEN")
    assert _mapping(invoke("context")["data"])["context"] == "production"
    assert (
        _mapping(invoke("config", "unset", "default-context")["data"])["value"] is None
    )


def test_config_write_survives_invalid_higher_priority_selector(
    monkeypatch: pytest.MonkeyPatch, connection_file: Path
) -> None:
    invoke("context", "create", "production", "--file", str(connection_file))
    monkeypatch.setenv("DSCTL_CONTEXT", "missing-context")
    written = invoke("config", "set", "default-context", "production")
    assert _mapping(written["data"])["value"] == "production"
    assert _mapping(written["resolved"])["effective"] is None
    assert written["warnings"]
    assert (
        _mapping(invoke("config", "get", "default-context")["data"])["value"]
        == "production"
    )


def test_config_only_accepts_default_context_key() -> None:
    for args in (["get", "project"], ["set", "project", "etl"], ["unset", "project"]):
        result = runner.invoke(app, ["config", *args])
        assert result.exit_code != 0
        assert json.loads(result.stderr)["error"]["type"] == "user_input_error"


def test_use_command_is_removed() -> None:
    result = runner.invoke(app, ["use", "project", "etl"])
    assert result.exit_code != 0
    assert "No such command" in result.output
