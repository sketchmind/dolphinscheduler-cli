import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from dsctl.app import app
from dsctl.client import DolphinSchedulerClient
from dsctl.services import runtime as runtime_service
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.task_type_inventory import TASK_TYPE_DOMAIN, TaskTypeDomain
from tests.fakes import (
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_bound_domain_service_runtime,
)
from tests.support import make_profile, strip_cli_ansi

runner = CliRunner()

_LEGACY_CATALOG_VERSIONS = tuple(f"3.1.{patch}" for patch in range(10))
_CATEGORY_CATALOG_VERSIONS = (
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)


def _install_native_task_type_transport(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
    rows: list[dict[str, str | bool | None]],
) -> list[httpx.Request]:
    profile = make_profile(ds_version=ds_version)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 0, "msg": "success", "data": rows})

    @contextmanager
    def open_runtime(
        domain: BoundDomain[TaskTypeDomain],
        *,
        env_file: str | None = None,
    ) -> Iterator[BoundDomainServiceRuntime[TaskTypeDomain]]:
        del env_file
        assert domain is TASK_TYPE_DOMAIN
        with DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client:
            yield BoundDomainServiceRuntime(
                profile=profile,
                context=ResourceDefaults(),
                http_client=client,
                domain=TASK_TYPE_DOMAIN.bind(profile, http_client=client),
            )

    monkeypatch.setattr(
        runtime_service, "open_bound_domain_service_runtime", open_runtime
    )
    return requests


@pytest.mark.parametrize(
    "ds_version", _LEGACY_CATALOG_VERSIONS + _CATEGORY_CATALOG_VERSIONS
)
def test_task_type_list_projects_native_catalog_through_codec_service_and_cli(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
) -> None:
    name_field, category_field = (
        ("taskName", "taskType")
        if ds_version in _LEGACY_CATALOG_VERSIONS
        else ("taskType", "taskCategory")
    )
    # TaskTypeConfiguration constructs (task name, collection flag, category).
    requests = _install_native_task_type_transport(
        monkeypatch,
        ds_version,
        [
            {name_field: "SHELL", category_field: "Universal", "collection": True},
            {name_field: "DEPENDENT", category_field: "Logic", "collection": False},
        ],
    )

    result = runner.invoke(app, ["task-type", "list"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.list"
    assert payload["resolved"]["source"] == "favourite/taskTypes"
    assert payload["data"]["taskTypes"] == [
        {"taskType": "SHELL", "taskCategory": "Universal", "isCollection": True},
        {"taskType": "DEPENDENT", "taskCategory": "Logic", "isCollection": False},
    ]
    assert payload["data"]["taskTypesByCategory"] == {
        "Universal": ["SHELL"],
        "Logic": ["DEPENDENT"],
    }
    assert payload["data"]["count"] == 2
    coverage = payload["data"]["cliCoverage"]
    assert "SHELL" in coverage["taskTemplateTypes"]
    assert coverage["untemplatedTaskTypes"] == []
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/dolphinscheduler/favourite/taskTypes"),
    ]


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.9", "3.2.0", "3.4.3"])
@pytest.mark.parametrize("field", ["name", "category"])
@pytest.mark.parametrize("missing", [False, True])
def test_task_type_list_still_rejects_missing_native_name_or_category(
    monkeypatch: pytest.MonkeyPatch,
    ds_version: str,
    field: str,
    *,
    missing: bool,
) -> None:
    name_field, category_field = (
        ("taskName", "taskType")
        if ds_version in _LEGACY_CATALOG_VERSIONS
        else ("taskType", "taskCategory")
    )
    native: dict[str, str | bool | None] = {
        name_field: "SHELL",
        category_field: "Universal",
        "collection": True,
    }
    selected = name_field if field == "name" else category_field
    if missing:
        native.pop(selected)
    else:
        native[selected] = None
    requests = _install_native_task_type_transport(monkeypatch, ds_version, [native])

    result = runner.invoke(app, ["task-type", "list"])

    assert result.exit_code != 0
    payload = json.loads(result.stderr)
    assert payload["error"]["type"] == "api_transport_error"
    expected = "taskType" if field == "name" else "taskCategory"
    assert f"missing required field '{expected}'" in payload["error"]["message"]
    assert len(requests) == 1


@pytest.fixture(autouse=True)
def patch_task_type_service(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = FakeTaskTypeAdapter(
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
    )

    @contextmanager
    def open_fake_runtime(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: Path | None = None,
    ) -> Iterator[BoundDomainServiceRuntime[object]]:
        del env_file, cwd
        assert domain is TASK_TYPE_DOMAIN
        with fake_bound_domain_service_runtime(
            TaskTypeDomain(task_types=adapter),
            profile=make_profile(),
        ) as runtime:
            yield runtime

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        open_fake_runtime,
    )


def test_task_type_list_command_returns_remote_discovery_payload() -> None:
    result = runner.invoke(app, ["task-type", "list"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["action"] == "task-type.list"
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "source": "favourite/taskTypes",
    }
    assert payload["data"]["count"] == 2
    assert payload["data"]["taskTypes"][0] == {
        "taskType": "SHELL",
        "isCollection": True,
        "taskCategory": "Universal",
    }
    assert payload["data"]["taskTypesByCategory"] == {
        "Universal": ["SHELL", "CUSTOM_PLUGIN"]
    }
    assert "DATAX" in payload["data"]["cliCoverage"]["typedTaskSpecs"]
    assert "DATAX" not in payload["data"]["cliCoverage"]["genericTaskTemplateTypes"]
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
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "task_type": "SQL",
    }
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
    assert payload["resolved"] == {
        "selection": {
            "source": "unconfigured",
            "context": None,
            "env_file": None,
            "api_url": None,
        },
        "task_type": "SQL",
        "view": "fields",
    }
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
    assert resource_field["choice_source"] == "dsctl resource list"
    assert (
        resource_field["choice_value"]
        == "fullName relative to the FILE root, retaining one leading slash"
    )
    assert resource_field["related_commands"] == [
        "dsctl resource list",
        "dsctl resource upload --file FILE",
        "dsctl resource view RESOURCE",
    ]


def test_task_type_schema_table_uses_canonical_fields() -> None:
    result = runner.invoke(
        app,
        ["--format", "table", "task-type", "schema", "SHELL"],
    )

    assert result.exit_code == 0
    assert result.stdout.splitlines()[0].startswith("path")
    assert "retry.times" in result.stdout
    assert "depends_on[]" in result.stdout


def test_task_type_schema_json_columns_project_canonical_fields() -> None:
    result = runner.invoke(
        app,
        [
            "--format",
            "json-compact",
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
            "--format",
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
            "--format",
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
            "--format",
            "json-compact",
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
            "--format",
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
        ["--format", "json-compact", "task-type", "schema", "SHELL", "--json-schema"],
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
    suggestion = payload["error"]["suggestion"]
    assert "Do not repeat the command" in suggestion
    assert "data_shapes_by_view.compile_mappings" in suggestion


def test_task_type_schema_tsv_and_full_table_use_the_selected_row_shape() -> None:
    tsv_result = runner.invoke(
        app,
        [
            "--format",
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
            "--format",
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
