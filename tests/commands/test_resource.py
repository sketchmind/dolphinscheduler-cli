import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeResourceAdapter,
    FakeResourceItem,
    fake_service_runtime,
)
from tests.support import make_profile, normalize_cli_help

runner = CliRunner()


def _resource_adapter() -> FakeResourceAdapter:
    return FakeResourceAdapter(
        resources=[
            FakeResourceItem(
                alias="demo.sql",
                file_name_value="demo.sql",
                full_name_value="/tenant/resources/demo.sql",
                is_directory_value=False,
                size=20,
            ),
            FakeResourceItem(
                alias="scripts",
                file_name_value="scripts",
                full_name_value="/tenant/resources/scripts",
                is_directory_value=True,
            ),
        ],
        contents_by_full_name={
            "/tenant/resources/demo.sql": b"select 1;\nselect 2;\n",
        },
        base_dir_value="/tenant/resources",
    )


@pytest.fixture
def fake_resource_adapter() -> FakeResourceAdapter:
    return _resource_adapter()


@pytest.fixture(autouse=True)
def patch_resource_service(
    monkeypatch: pytest.MonkeyPatch,
    fake_resource_adapter: FakeResourceAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            resource_adapter=fake_resource_adapter,
            profile=make_profile(),
        ),
    )


def test_resource_list_command_returns_paginated_payload() -> None:
    result = runner.invoke(app, ["resource", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["action"] == "resource.list"
    assert payload["resolved"]["directory"] == "/tenant/resources"
    assert payload["data"]["totalList"][0]["fullName"] == "/tenant/resources/demo.sql"


def test_resource_view_command_returns_text_content() -> None:
    result = runner.invoke(
        app,
        ["resource", "view", "/tenant/resources/demo.sql", "--limit", "1"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "resource.view"
    assert payload["data"]["content"] == "select 1;"


def test_resource_path_help_points_to_list_discovery() -> None:
    result = runner.invoke(app, ["resource", "view", "--help"])

    assert result.exit_code == 0
    help_text = normalize_cli_help(result.stdout)
    assert "dsctl resource list" in help_text
    assert "--dir DIR" in help_text


def test_resource_upload_and_mkdir_commands_mutate_resources(tmp_path: Path) -> None:
    upload_path = tmp_path / "upload.sql"
    upload_path.write_text("select 3;\n", encoding="utf-8")

    upload_result = runner.invoke(
        app,
        ["resource", "upload", "--file", str(upload_path)],
    )
    mkdir_result = runner.invoke(app, ["resource", "mkdir", "archive"])

    assert upload_result.exit_code == 0
    assert mkdir_result.exit_code == 0
    upload_payload = json.loads(upload_result.stdout)
    mkdir_payload = json.loads(mkdir_result.stdout)
    assert upload_payload["data"]["fullName"] == "/tenant/resources/upload.sql"
    assert mkdir_payload["data"]["isDirectory"] is True


def test_resource_create_command_rejects_empty_content() -> None:
    result = runner.invoke(
        app,
        ["resource", "create", "--name", "notes.txt", "--content", ""],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "resource.create"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Pass non-empty inline content, or use `resource upload` if the content "
        "already lives in a local file."
    )


def test_resource_create_help_points_to_upload_for_local_files() -> None:
    result = runner.invoke(app, ["resource", "create", "--help"])

    assert result.exit_code == 0
    assert "resource upload --file PATH" in normalize_cli_help(result.stdout)


def test_resource_download_command_writes_output_file(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "resource",
            "download",
            "/tenant/resources/demo.sql",
            "--output",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    saved_path = tmp_path / "demo.sql"
    assert saved_path.read_bytes() == b"select 1;\nselect 2;\n"
    assert payload["action"] == "resource.download"
    assert payload["data"]["saved_to"] == str(saved_path.resolve())


def test_resource_download_command_requires_overwrite_for_existing_output(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "demo.sql"
    output_path.write_text("existing", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "resource",
            "download",
            "/tenant/resources/demo.sql",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "resource.download"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == (
        "Retry with --overwrite, or choose a different --output path."
    )


def test_resource_delete_command_requires_force() -> None:
    result = runner.invoke(app, ["resource", "delete", "/tenant/resources/demo.sql"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["action"] == "resource.delete"
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["suggestion"] == "Retry the same command with --force."
