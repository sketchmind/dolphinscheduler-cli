import json

import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.services import runtime as runtime_service
from tests.fakes import (
    FakeProjectAdapter,
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_service_runtime,
)
from tests.support import make_profile, strip_cli_ansi

runner = CliRunner()


@pytest.fixture(autouse=True)
def patch_task_type_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            profile=make_profile(),
            task_type_adapter=FakeTaskTypeAdapter(
                task_types=[
                    FakeTaskType(
                        task_type_value="SHELL",
                        is_collection_value=True,
                        task_category_value="Universal",
                    ),
                    FakeTaskType(
                        task_type_value="CUSTOM_PLUGIN",
                        is_collection_value=False,
                        task_category_value="Universal",
                    ),
                ]
            ),
        ),
    )


def test_task_type_list_command_returns_remote_discovery_payload() -> None:
    result = runner.invoke(app, ["task-type", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.list"
    assert payload["resolved"] == {"source": "favourite/taskTypes"}
    assert payload["data"]["count"] == 2
    assert payload["data"]["taskTypes"][0] == {
        "taskType": "SHELL",
        "isCollection": True,
        "taskCategory": "Universal",
    }
    assert payload["data"]["taskTypesByCategory"] == {
        "Universal": ["SHELL", "CUSTOM_PLUGIN"]
    }
    assert "SPARK" in payload["data"]["cliCoverage"]["genericTaskTemplateTypes"]
    assert payload["data"]["cliCoverage"]["untemplatedTaskTypes"] == ["CUSTOM_PLUGIN"]


def test_task_type_help_distinguishes_live_catalog_from_template_catalog() -> None:
    group_result = runner.invoke(app, ["task-type", "--help"])
    list_result = runner.invoke(app, ["task-type", "list", "--help"])

    assert group_result.exit_code == 0
    assert list_result.exit_code == 0
    assert "local task authoring contracts" in group_result.stdout
    assert "schema" in group_result.stdout
    assert "CLI authoring" in list_result.stdout
    assert "coverage" in list_result.stdout


def test_task_type_get_command_returns_local_authoring_summary() -> None:
    result = runner.invoke(app, ["task-type", "get", "sql"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.get"
    assert payload["resolved"] == {"task_type": "SQL"}
    assert payload["data"]["task_type"] == "SQL"
    assert payload["data"]["schema_command"] == "dsctl task-type schema SQL"
    assert payload["data"]["raw_template_command"] == "dsctl template task SQL --raw"
    assert "task_params.sql" in payload["data"]["required_paths"]
    assert payload["data"]["required_paths_by_payload_mode"] == {}


def test_task_type_schema_command_returns_field_contract() -> None:
    result = runner.invoke(app, ["task-type", "schema", "SQL"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.schema"
    assert payload["resolved"] == {"task_type": "SQL", "view": "fields"}
    field_paths = [field["path"] for field in payload["data"]["fields"]]
    assert "task_params.sqlType" in field_paths
    assert payload["data"]["state_rules"][1]["when"] == "task_params.sqlType == 1"
    assert "schema" not in payload["data"]
    assert "choice_sources" not in payload["data"]
    assert "compile_mappings" not in payload["data"]


def test_task_type_schema_command_returns_choice_sources_for_fields() -> None:
    result = runner.invoke(app, ["task-type", "schema", "SHELL"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    resource_field = next(
        field
        for field in payload["data"]["fields"]
        if field["path"] == "task_params.resourceList[].resourceName"
    )
    assert resource_field["choice_source"] == "dsctl resource list --dir DIR"
    assert resource_field["choice_value"] == "fullName"
    assert resource_field["related_commands"] == [
        "dsctl resource list",
        "dsctl resource upload --file FILE",
        "dsctl resource view RESOURCE",
    ]


def test_task_type_schema_table_uses_canonical_fields() -> None:
    result = runner.invoke(
        app,
        ["--output-format", "table", "task-type", "schema", "SHELL"],
    )

    assert result.exit_code == 0
    assert result.stdout.splitlines()[0].startswith("path")
    assert "retry.times" in result.stdout
    assert "depends_on[]" in result.stdout


def test_task_type_schema_json_columns_project_canonical_fields() -> None:
    result = runner.invoke(
        app,
        [
            "--compact",
            "--columns",
            "path,type",
            "task-type",
            "schema",
            "SHELL",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert "rows" not in payload["data"]
    assert payload["data"]["fields"][0] == {"path": "name", "type": "string"}


def test_task_type_schema_command_supports_direct_progressive_views() -> None:
    field_result = runner.invoke(
        app,
        [
            "task-type",
            "schema",
            "SHELL",
            "--field",
            "task_params.resourceList[].resourceName",
        ],
    )
    json_schema_result = runner.invoke(
        app,
        ["task-type", "schema", "SHELL", "--json-schema"],
    )
    compile_result = runner.invoke(
        app,
        ["task-type", "schema", "SHELL", "--compile-mappings"],
    )
    full_result = runner.invoke(
        app,
        ["task-type", "schema", "SHELL", "--full"],
    )

    assert field_result.exit_code == 0
    field_payload = json.loads(field_result.stdout)
    assert field_payload["resolved"]["view"] == "field"
    assert [item["path"] for item in field_payload["data"]["fields"]] == [
        "task_params.resourceList[].resourceName"
    ]

    assert json_schema_result.exit_code == 0
    json_schema_payload = json.loads(json_schema_result.stdout)
    assert json_schema_payload["resolved"]["view"] == "json_schema"
    assert "schema" in json_schema_payload["data"]
    assert "fields" not in json_schema_payload["data"]

    assert compile_result.exit_code == 0
    compile_payload = json.loads(compile_result.stdout)
    assert compile_payload["resolved"]["view"] == "compile_mappings"
    assert "compile_mappings" in compile_payload["data"]
    assert "fields" not in compile_payload["data"]

    assert full_result.exit_code == 0
    full_payload = json.loads(full_result.stdout)
    assert full_payload["resolved"]["view"] == "full"
    assert "fields" in full_payload["data"]
    assert "schema" in full_payload["data"]


def test_task_type_schema_compile_table_uses_mapping_rows() -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "task-type",
            "schema",
            "SHELL",
            "--compile-mappings",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout.splitlines()[0].startswith("authoring_path")
    assert "taskDefinitionJson[].taskParams.rawScript" in result.stdout


def test_task_type_schema_field_table_is_one_canonical_row() -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "task-type",
            "schema",
            "SHELL",
            "--field",
            "task_params.resourceList[].resourceName",
        ],
    )

    assert result.exit_code == 0
    lines = result.stdout.splitlines()
    assert len(lines) == 3
    assert "task_params.resourceList[].resourceName" in lines[2]


def test_task_type_compile_columns_project_mapping_rows() -> None:
    result = runner.invoke(
        app,
        [
            "--compact",
            "--columns",
            "authoring_path,ds_payload_path",
            "task-type",
            "schema",
            "SHELL",
            "--compile-mappings",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["compile_mappings"][0] == {
        "authoring_path": "name",
        "ds_payload_path": "taskDefinitionJson[].name",
    }


@pytest.mark.parametrize("output_format", ["table", "tsv"])
def test_task_type_json_schema_rejects_lossy_row_formats(
    output_format: str,
) -> None:
    result = runner.invoke(
        app,
        [
            "--output-format",
            output_format,
            "task-type",
            "schema",
            "SHELL",
            "--json-schema",
        ],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "user_input_error" in result.stderr
    assert "JSON-only" in result.stderr


def test_task_type_json_schema_rejects_column_projection() -> None:
    result = runner.invoke(
        app,
        [
            "--columns",
            "title,type",
            "task-type",
            "schema",
            "SHELL",
            "--json-schema",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["details"]["view"] == "json_schema"
    assert "JSON-only" in payload["error"]["message"]


def test_task_type_json_schema_supports_compact_complete_json() -> None:
    result = runner.invoke(
        app,
        ["--compact", "task-type", "schema", "SHELL", "--json-schema"],
    )

    assert result.exit_code == 0
    assert "\n" not in result.stdout.rstrip("\n")
    payload = json.loads(result.stdout)
    assert "properties" in payload["data"]["schema"]
    assert "$defs" in payload["data"]["schema"]


def test_task_type_compile_column_error_points_to_the_selected_view() -> None:
    result = runner.invoke(
        app,
        [
            "--columns",
            "path",
            "task-type",
            "schema",
            "SHELL",
            "--compile-mappings",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["details"]["view"] == "compile_mappings"
    assert "data_shapes_by_view.compile_mappings" in payload["error"]["suggestion"]


def test_task_type_schema_tsv_and_full_table_use_the_selected_row_shape() -> None:
    tsv_result = runner.invoke(
        app,
        [
            "--output-format",
            "tsv",
            "task-type",
            "schema",
            "SHELL",
            "--compile-mappings",
        ],
    )
    full_result = runner.invoke(
        app,
        [
            "--output-format",
            "table",
            "task-type",
            "schema",
            "SHELL",
            "--full",
        ],
    )

    assert tsv_result.exit_code == 0
    assert tsv_result.stdout.splitlines()[0] == "authoring_path\tds_payload_path"
    assert full_result.exit_code == 0
    assert full_result.stdout.splitlines()[0].startswith("path")


def test_task_type_schema_command_rejects_multiple_view_selectors() -> None:
    result = runner.invoke(
        app,
        [
            "task-type",
            "schema",
            "SHELL",
            "--json-schema",
            "--compile-mappings",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "user_input_error"
    assert payload["error"]["details"] == {
        "constraint": "at_most_one_of",
        "selected": ["--json-schema", "--compile-mappings"],
    }


def test_task_type_schema_help_exposes_direct_view_flags() -> None:
    result = runner.invoke(app, ["task-type", "schema", "--help"])

    assert result.exit_code == 0
    help_text = strip_cli_ansi(result.stdout)
    assert "--field" in help_text
    assert "--json-schema" in help_text
    assert "--compile-mappings" in help_text
    assert "--full" in help_text
