import json
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from pathlib import Path
from shlex import split
from typing import cast

import pytest
from tests.fakes import (
    FakeTaskType,
    FakeTaskTypeAdapter,
    fake_bound_domain_service_runtime,
)
from tests.support import make_profile
from tests.value_shape_assertions import assert_mapping as _mapping
from tests.value_shape_assertions import assert_sequence as _sequence

from dsctl.context import create_context
from dsctl.errors import ApiTransportError, UserInputError
from dsctl.generated.task_profiles import TARGET_DS_VERSIONS
from dsctl.output import error_payload, result_payload
from dsctl.services import runtime as runtime_service
from dsctl.services import task_type as task_type_service
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.task_authoring_catalog import (
    TaskTypedAuthoringReview,
    get_task_authoring_catalog,
)
from dsctl.services.template import supported_task_template_types
from dsctl.services.version_resolution import invocation_scope
from dsctl.upstream.task_type_inventory import TASK_TYPE_DOMAIN, TaskTypeDomain


def _install_task_type_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    adapter: FakeTaskTypeAdapter,
    *,
    ds_version: str = "3.4.1",
) -> None:
    @contextmanager
    def open_fake_runtime(
        domain: object,
        *,
        env_file: str | None = None,
        cwd: Path | None = None,
    ) -> Iterator[BoundDomainServiceRuntime[TaskTypeDomain]]:
        del env_file, cwd
        assert domain is TASK_TYPE_DOMAIN
        with fake_bound_domain_service_runtime(
            TaskTypeDomain(task_types=adapter),
            profile=make_profile(ds_version=ds_version),
        ) as runtime:
            yield cast("BoundDomainServiceRuntime[TaskTypeDomain]", runtime)

    monkeypatch.setattr(
        runtime_service,
        "open_bound_domain_service_runtime",
        open_fake_runtime,
    )


def _local_json_refs(value: object) -> list[str]:
    refs: list[str] = []
    if isinstance(value, Mapping):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/"):
            refs.append(ref)
        for nested in value.values():
            refs.extend(_local_json_refs(nested))
    elif isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for nested in value:
            refs.extend(_local_json_refs(nested))
    return refs


def _resolve_local_json_ref(schema: Mapping[str, object], ref: str) -> object:
    current: object = schema
    for raw_token in ref.removeprefix("#/").split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        assert isinstance(current, Mapping), ref
        assert token in current, ref
        current = current[token]
    return current


def test_list_task_types_result_returns_remote_payload_and_cli_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value="SHELL",
                is_collection_value=True,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="REMOTE_SHELL",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="CUSTOM_PLUGIN",
                is_collection_value=False,
                task_category_value="Universal",
            ),
            FakeTaskType(
                task_type_value="SUB_WORKFLOW",
                is_collection_value=True,
                task_category_value="Logic",
            ),
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    result = task_type_service.list_task_types_result()
    data = _mapping(result.data)
    task_types = _sequence(data["taskTypes"])
    coverage = _mapping(data["cliCoverage"])

    assert result.resolved == {"source": "favourite/taskTypes"}
    assert data["count"] == 4
    assert list(task_types) == [
        {
            "taskType": "SHELL",
            "isCollection": True,
            "taskCategory": "Universal",
        },
        {
            "taskType": "REMOTE_SHELL",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "CUSTOM_PLUGIN",
            "isCollection": False,
            "taskCategory": "Universal",
        },
        {
            "taskType": "SUB_WORKFLOW",
            "isCollection": True,
            "taskCategory": "Logic",
        },
    ]
    assert data["taskTypesByCategory"] == {
        "Universal": ["SHELL", "REMOTE_SHELL", "CUSTOM_PLUGIN"],
        "Logic": ["SUB_WORKFLOW"],
    }
    assert "REMOTESHELL" in _sequence(coverage["taskTemplateTypes"])
    assert "DATAX" in _sequence(coverage["typedTaskSpecs"])
    assert "DATAX" not in _sequence(coverage["genericTaskTemplateTypes"])
    assert "CUSTOM_PLUGIN" in _sequence(coverage["untemplatedTaskTypes"])
    assert "REMOTE_SHELL" not in _sequence(coverage["untemplatedTaskTypes"])


def test_list_task_types_result_rejects_missing_required_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value=None,
                task_category_value="Universal",
            )
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter)

    with pytest.raises(ApiTransportError, match="missing required field 'taskType'"):
        task_type_service.list_task_types_result()


def test_legacy_task_type_list_matches_native_sub_process_to_cli_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = FakeTaskTypeAdapter(
        task_types=[
            FakeTaskType(
                task_type_value="SUB_PROCESS",
                is_collection_value=True,
                task_category_value="Logic",
            )
        ]
    )
    _install_task_type_service_fakes(monkeypatch, adapter, ds_version="2.0.0")

    data = _mapping(task_type_service.list_task_types_result().data)
    task_types = _sequence(data["taskTypes"])
    coverage = _mapping(data["cliCoverage"])

    assert _mapping(task_types[0])["taskType"] == "SUB_PROCESS"
    assert "SUB_WORKFLOW" in _sequence(coverage["taskTemplateTypes"])
    assert "SUB_PROCESS" not in _sequence(coverage["taskTemplateTypes"])
    assert "SUB_PROCESS" not in _sequence(coverage["untemplatedTaskTypes"])


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_task_type_coverage_uses_exact_profile_inventory(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = task_type_service._task_type_list_data([], catalog=catalog)
    coverage = data["cliCoverage"]

    assert set(coverage["taskTemplateTypes"]) == set(catalog.authoring_task_types)
    assert set(coverage["typedTaskSpecs"]) == set(catalog.reviewed_typed_task_types)
    assert set(coverage["genericTaskTemplateTypes"]) == (
        set(catalog.authoring_task_types) - set(catalog.reviewed_typed_task_types)
    )


@pytest.mark.parametrize(
    ("ds_version", "task_type", "expected_kind", "has_warnings"),
    [
        ("1.3.9", "SUB_WORKFLOW", "typed", False),
        ("3.4.2", "EMR_SERVERLESS", "typed", False),
    ],
)
def test_local_task_type_discovery_honors_env_file_profile(
    tmp_path: Path,
    ds_version: str,
    task_type: str,
    expected_kind: str,
    *,
    has_warnings: bool,
) -> None:
    env_file = tmp_path / f"ds-{ds_version}.env"
    env_file.write_text(f"DS_VERSION={ds_version}\n", encoding="utf-8")

    result = task_type_service.task_type_summary_result(
        task_type,
        env_file=str(env_file),
    )
    data = _mapping(result.data)

    assert data["task_type"] == task_type
    assert data["kind"] == expected_kind
    assert bool(result.warnings) is has_warnings


def test_task_type_summary_result_describes_local_authoring_contract() -> None:
    result = task_type_service.task_type_summary_result("sql")
    data = _mapping(result.data)

    assert result.resolved == {"task_type": "SQL"}
    assert data["task_type"] == "SQL"
    assert data["template_command"] == "dsctl template task SQL"
    assert data["raw_template_command"] == "dsctl template task SQL --raw"
    assert "task_params.sql" in _sequence(data["required_paths"])
    rows = [_mapping(item) for item in _sequence(data["rows"])]
    commands = {row["name"]: row["command"] for row in rows}
    assert commands["schema"] == "dsctl task-type schema SQL"
    assert commands["json-schema"] == "dsctl task-type schema SQL --json-schema"
    assert commands["compile-mappings"] == (
        "dsctl task-type schema SQL --compile-mappings"
    )
    assert commands["full-schema"] == "dsctl task-type schema SQL --full"


@pytest.mark.parametrize("ds_version", ["3.4.1", "3.4.2"])
def test_sql_discovery_projects_the_selected_exact_profile_catalog(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    summary = _mapping(
        task_type_service.task_type_summary_result(
            "SQL",
            catalog=catalog,
        ).data
    )
    schema = _mapping(
        task_type_service.task_type_schema_result(
            "SQL",
            catalog=catalog,
        ).data
    )
    fields = [_mapping(item) for item in _sequence(schema["fields"])]
    paths = [str(field["path"]) for field in fields]
    state_rules = [_mapping(item) for item in _sequence(schema["state_rules"])]

    assert summary["variants"] == ["output"]
    assert summary["required_paths"] == [
        "name",
        "type",
        "task_params",
        "task_params.type",
        "task_params.datasource",
        "task_params.sql",
        "task_params.sqlType",
    ]
    assert "task_params.sqlSource" not in paths
    assert "task_params.sqlResource" not in paths
    assert [rule["when"] for rule in state_rules] == [
        "task_params.sqlType == 0",
        "task_params.sqlType == 1",
    ]


def test_sql_json_schema_keeps_integer_choices_type_compatible() -> None:
    data = _mapping(
        task_type_service.task_type_schema_result("SQL", json_schema=True).data
    )
    schema = _mapping(data["schema"])
    definitions = _mapping(schema["$defs"])
    task_params = _mapping(definitions["task_params"])
    properties = _mapping(task_params["properties"])
    sql_type = _mapping(properties["sqlType"])

    assert sql_type["type"] == "integer"
    assert sql_type["enum"] == [0, 1]


def test_reviewed_legacy_task_schema_uses_exact_parameter_data_types() -> None:
    catalog = get_task_authoring_catalog("1.3.9")
    shell_fact = catalog.task_type_facts["SHELL"]
    reviewed_shell = replace(
        shell_fact,
        typed_authoring_review=TaskTypedAuthoringReview(
            source_task_type="SHELL",
            cli_task_type="SHELL",
            review="test-reviewed-exact-profile",
            cli_model="ScriptTaskParamsSpec",
            semantic_fingerprint=shell_fact.semantic_fingerprint,
        ),
    )
    reviewed_catalog = replace(
        catalog,
        task_type_facts={**catalog.task_type_facts, "SHELL": reviewed_shell},
    )

    data = _mapping(
        task_type_service.task_type_schema_result(
            "SHELL",
            catalog=reviewed_catalog,
        ).data
    )
    fields = {
        str(field["path"]): field
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    assert fields["task_params.localParams[].type"]["choices"] == [
        "VARCHAR",
        "INTEGER",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BOOLEAN",
    ]


def test_139_shell_schema_projects_only_legacy_task_node_fields() -> None:
    legacy_catalog = get_task_authoring_catalog("1.3.9")

    legacy_data = _mapping(
        task_type_service.task_type_schema_result(
            "SHELL",
            catalog=legacy_catalog,
        ).data
    )
    legacy_fields = {
        str(field["path"]): field
        for item in _sequence(legacy_data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    assert not {
        "environment_code",
        "task_group_id",
        "task_group_priority",
        "delay",
        "cpu_quota",
        "memory_max",
        "task_params.varPool[]",
    }.intersection(legacy_fields)
    assert legacy_fields["task_params.rawScript"]["compile_path"] == (
        "processDefinitionJson.tasks[].params.rawScript"
    )
    assert legacy_fields["task_params.localParams[]"]["compile_path"] == (
        "processDefinitionJson.tasks[].params.localParams"
    )
    assert legacy_fields["task_params.resourceList[]"]["compile_path"] == (
        "processDefinitionJson.tasks[].params.resourceList"
    )
    assert legacy_fields["task_params.localParams[].direct"]["choices"] == ["IN"]

    modern_data = _mapping(
        task_type_service.task_type_schema_result(
            "SHELL",
            catalog=get_task_authoring_catalog("3.4.1"),
        ).data
    )
    modern_fields = {
        str(field["path"]): field
        for item in _sequence(modern_data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }
    assert "task_params.varPool[]" in modern_fields
    assert modern_fields["task_params.localParams[].direct"]["choices"] == [
        "IN",
        "OUT",
    ]
    assert modern_fields["task_params.rawScript"]["compile_path"] == (
        "taskDefinitionJson[].taskParams.rawScript"
    )


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.0.0", "3.1.0"])
def test_reviewed_legacy_json_schema_uses_exact_parameter_data_types(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result(
            "SHELL",
            catalog=catalog,
            json_schema=True,
        ).data
    )
    schema = _mapping(data["schema"])
    task_params = _mapping(_mapping(schema["$defs"])["task_params"])
    data_type = _mapping(_mapping(task_params["$defs"])["DataType"])

    assert frozenset(_sequence(data_type["enum"])) == catalog.parameter_data_types


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.4.2"])
def test_reviewed_task_schema_exposes_selected_exact_parameter_data_types(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result(
            "SHELL",
            catalog=catalog,
        ).data
    )
    fields = {
        str(field["path"]): field
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }
    choices = fields["task_params.localParams[].type"]["choices"]

    assert choices == [
        "VARCHAR",
        "INTEGER",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME",
        "TIMESTAMP",
        "BOOLEAN",
        "LIST",
        "FILE",
    ]
    assert frozenset(_sequence(choices)) == catalog.parameter_data_types


@pytest.mark.parametrize(
    ("ds_version", "has_http_body"),
    [
        ("2.0.0", False),
        ("3.2.0", False),
        ("3.2.1", True),
        ("3.3.1", True),
        ("3.4.0", True),
    ],
)
def test_http_schema_projects_the_exact_authoring_surface(
    ds_version: str,
    *,
    has_http_body: bool,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result("HTTP", catalog=catalog).data
    )
    paths = {
        str(field["path"])
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    assert ("task_params.httpBody" in paths) is has_http_body
    assert "task_params.socketTimeout" not in paths
    json_data = _mapping(
        task_type_service.task_type_schema_result(
            "HTTP",
            catalog=catalog,
            json_schema=True,
        ).data
    )
    task_params_schema = _mapping(_mapping(json_data["schema"])["$defs"])["task_params"]
    properties = _mapping(_mapping(task_params_schema)["properties"])
    assert ("httpBody" in properties) is has_http_body
    assert "socketTimeout" not in properties


@pytest.mark.parametrize(
    ("ds_version", "has_failure_control", "has_parameter_passing"),
    [
        ("2.0.0", False, False),
        ("3.2.0", True, False),
        ("3.2.1", True, True),
        ("3.3.1", True, True),
        ("3.4.0", True, True),
    ],
)
def test_dependent_schema_projects_only_authored_exact_fields(
    ds_version: str,
    *,
    has_failure_control: bool,
    has_parameter_passing: bool,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result("DEPENDENT", catalog=catalog).data
    )
    paths = {
        str(field["path"])
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    for path in (
        "task_params.dependence.checkInterval",
        "task_params.dependence.failurePolicy",
        "task_params.dependence.failureWaitingTime",
    ):
        assert (path in paths) is has_failure_control
    assert (
        "task_params.dependence.dependTaskList[].dependItemList[].parameterPassing"
        in paths
    ) is has_parameter_passing
    assert not any(path.endswith("dependResult") for path in paths)
    assert not any("resourceList" in path for path in paths)
    json_data = _mapping(
        task_type_service.task_type_schema_result(
            "DEPENDENT",
            catalog=catalog,
            json_schema=True,
        ).data
    )
    task_params_schema = _mapping(_mapping(json_data["schema"])["$defs"])["task_params"]
    definitions = _mapping(_mapping(task_params_schema)["$defs"])
    root_properties = _mapping(_mapping(task_params_schema)["properties"])
    dependence_properties = _mapping(
        _mapping(definitions["DependenceSpec"])["properties"]
    )
    item_properties = _mapping(_mapping(definitions["DependentItemSpec"])["properties"])
    assert "resourceList" not in root_properties
    assert ("checkInterval" in dependence_properties) is has_failure_control
    assert ("failurePolicy" in dependence_properties) is has_failure_control
    assert ("parameterPassing" in item_properties) is has_parameter_passing
    assert "dependResult" not in item_properties


@pytest.mark.parametrize(
    ("ds_version", "native_code_field"),
    [
        ("2.0.0", "processDefinitionCode"),
        ("3.2.0", "processDefinitionCode"),
        ("3.2.1", "processDefinitionCode"),
        ("3.3.1", "workflowDefinitionCode"),
        ("3.4.0", "workflowDefinitionCode"),
    ],
)
def test_sub_workflow_schema_keeps_canonical_input_and_shows_native_compile_path(
    ds_version: str,
    native_code_field: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result("SUB_WORKFLOW", catalog=catalog).data
    )
    fields = {
        str(field["path"]): field
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    code_field = fields["task_params.workflowDefinitionCode"]
    assert code_field["compile_path"] == (
        f"taskDefinitionJson[].taskParams.{native_code_field}"
    )


def test_139_sub_workflow_schema_uses_only_same_project_name_identity() -> None:
    catalog = get_task_authoring_catalog("1.3.9")

    data = _mapping(
        task_type_service.task_type_schema_result(
            "SUB_WORKFLOW",
            catalog=catalog,
        ).data
    )
    fields = {
        str(field["path"]): field
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    assert fields["task_params.childWorkflowName"] == {
        "path": "task_params.childWorkflowName",
        "type": "string",
        "required": True,
        "description": (
            "Same-project child workflow name resolved by the service to the exact "
            "DolphinScheduler 1.3.9 processDefinitionId."
        ),
        "choice_source": "dsctl workflow list --project PROJECT",
        "choice_value": "name",
        "related_commands": [
            "dsctl workflow list --project PROJECT",
            "dsctl workflow get WORKFLOW --project PROJECT",
        ],
        "compile_path": "processDefinitionJson.tasks[].params.processDefinitionId",
    }
    assert not {
        "task_params.workflowDefinitionCode",
        "task_params.localParams[]",
        "task_params.resourceList[]",
        "task_params.varPool[]",
    }.intersection(fields)

    json_data = _mapping(
        task_type_service.task_type_schema_result(
            "SUB_WORKFLOW",
            catalog=catalog,
            json_schema=True,
        ).data
    )
    task_params_schema = _mapping(
        _mapping(_mapping(json_data["schema"])["$defs"])["task_params"]
    )
    properties = _mapping(task_params_schema["properties"])
    assert set(properties) == {"childWorkflowName"}
    assert task_params_schema["additionalProperties"] is False
    assert task_params_schema["required"] == ["childWorkflowName"]
    assert _mapping(properties["childWorkflowName"])["type"] == "string"
    assert "anyOf" not in _mapping(properties["childWorkflowName"])


def test_139_sub_workflow_summary_explains_identity_and_parameter_boundary() -> None:
    data = _mapping(
        task_type_service.task_type_summary_result(
            "SUB_WORKFLOW",
            catalog=get_task_authoring_catalog("1.3.9"),
        ).data
    )
    choice_sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(data["choice_sources"])
    }

    assert data["required_paths"] == [
        "name",
        "type",
        "task_params",
        "task_params.childWorkflowName",
    ]
    assert choice_sources["task_params.childWorkflowName"]["value"] == "name"
    guidance = str(_mapping(data["workflow_usage"])["child_parameters"])
    for expected in (
        "same project",
        "SUB_PROCESS",
        "processDefinitionId",
        "parent values do not override",
        "localParams",
    ):
        assert expected in guidance


@pytest.mark.parametrize(
    ("ds_version", "task_type"),
    [("2.0.9", "SPARK")],
)
def test_opaque_task_schema_does_not_claim_typed_parameter_fields(
    ds_version: str,
    task_type: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    data = _mapping(
        task_type_service.task_type_schema_result(
            task_type,
            catalog=catalog,
        ).data
    )
    paths = {
        str(field["path"])
        for item in _sequence(data["fields"])
        if isinstance((field := _mapping(item)).get("path"), str)
    }

    assert "task_params.localParams[].type" not in paths


def test_341_and_342_inline_sql_discovery_are_semantically_identical() -> None:
    ds_341 = task_type_service.task_type_schema_result(
        "SQL",
        catalog=get_task_authoring_catalog("3.4.1"),
        full=True,
    )
    ds_342 = task_type_service.task_type_schema_result(
        "SQL",
        catalog=get_task_authoring_catalog("3.4.2"),
        full=True,
    )

    assert ds_341.data == ds_342.data


def test_sub_workflow_summary_explains_child_parameter_inheritance() -> None:
    result = task_type_service.task_type_summary_result("SUB_WORKFLOW")
    data = _mapping(result.data)
    workflow_usage = _mapping(data["workflow_usage"])

    guidance = str(workflow_usage["child_parameters"])
    for expected in (
        "parent workflow globals",
        "startup parameters",
        "workflow-instance varPool",
        "child global defaults are overridden",
        "localParams do not become child inputs",
        "DolphinScheduler 3.4.1",
        "standalone defaults",
    ):
        assert expected in guidance


@pytest.mark.parametrize(
    ("ds_version", "expected_fragment"),
    [
        ("2.0.0", "only when that name is selected in task localParams"),
        ("3.2.0", "workflow-instance varPool"),
        ("3.2.1", "workflow-instance varPool"),
        ("3.3.1", "only parent startup parameters"),
        ("3.4.0", "parent workflow globals, startup parameters"),
    ],
)
def test_sub_workflow_summary_reuses_exact_nested_parameter_semantics(
    ds_version: str,
    expected_fragment: str,
) -> None:
    data = _mapping(
        task_type_service.task_type_summary_result(
            "SUB_WORKFLOW",
            catalog=get_task_authoring_catalog(ds_version),
        ).data
    )
    guidance = str(_mapping(data["workflow_usage"])["child_parameters"])

    assert expected_fragment in guidance
    if ds_version != "3.4.1":
        assert "DS 3.4.1" not in guidance


def test_sub_workflow_local_params_schema_rejects_child_input_interpretation() -> None:
    result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.localParams[]",
    )
    fields = _sequence(_mapping(result.data)["fields"])

    assert len(fields) == 1
    field = _mapping(fields[0])
    assert field["description"] == (
        "DS-native task-local properties. In DS 3.4.1 they do not become child "
        "workflow inputs. Supply parent-specific values via the parent "
        "workflow.global_params or startup parameters."
    )
    assert "dsctl template params --topic context" in _sequence(
        field["related_commands"]
    )


def test_sub_workflow_schema_marks_unused_ds_fields_as_round_trip_only() -> None:
    local_value_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.localParams[].value",
    )
    var_pool_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.varPool[]",
    )
    resource_result = task_type_service.task_type_schema_result(
        "SUB_WORKFLOW",
        field="task_params.resourceList[].resourceName",
    )

    local_value = _mapping(_sequence(_mapping(local_value_result.data)["fields"])[0])
    var_pool = _mapping(_sequence(_mapping(var_pool_result.data)["fields"])[0])
    resource = _mapping(_sequence(_mapping(resource_result.data)["fields"])[0])
    assert "do not become child workflow inputs" in str(local_value["description"])
    assert "not the parent workflow-instance varPool" in str(var_pool["description"])
    assert "round-trip-only" in str(resource["description"])
    assert "choice_source" not in resource
    assert "related_commands" not in resource

    summary = _mapping(task_type_service.task_type_summary_result("SUB_WORKFLOW").data)
    choice_sources = _sequence(summary["choice_sources"])
    choice_paths = [_mapping(item)["path"] for item in choice_sources]
    assert "task_params.workflowDefinitionCode" in choice_paths
    assert not any("resourceList" in str(path) for path in choice_paths)


@pytest.mark.parametrize(
    (
        "task_type",
        "expected_payload_modes",
        "expected_required_paths",
        "expected_required_paths_by_payload_mode",
    ),
    [
        (
            "SHELL",
            ["command", "task_params"],
            ["name", "type"],
            {
                "command": ["command"],
                "task_params": ["task_params.rawScript"],
            },
        ),
        (
            "PYTHON",
            ["command", "task_params"],
            ["name", "type"],
            {
                "command": ["command"],
                "task_params": ["task_params.rawScript"],
            },
        ),
        (
            "REMOTESHELL",
            ["task_params"],
            ["name", "type"],
            {
                "task_params": [
                    "task_params.rawScript",
                    "task_params.datasource",
                ]
            },
        ),
        (
            "SQL",
            ["task_params"],
            [
                "name",
                "type",
                "task_params",
                "task_params.type",
                "task_params.datasource",
                "task_params.sql",
                "task_params.sqlType",
            ],
            {},
        ),
    ],
)
def test_task_type_summary_separates_payload_mode_requirements(
    task_type: str,
    expected_payload_modes: list[str],
    expected_required_paths: list[str],
    expected_required_paths_by_payload_mode: dict[str, list[str]],
) -> None:
    data = _mapping(task_type_service.task_type_summary_result(task_type).data)

    assert data["payload_modes"] == expected_payload_modes
    assert data["required_paths"] == expected_required_paths
    assert (
        data["required_paths_by_payload_mode"]
        == expected_required_paths_by_payload_mode
    )


def test_task_type_summary_keeps_required_fields_with_choice_conditions() -> None:
    data = _mapping(task_type_service.task_type_summary_result("DEPENDENT").data)

    assert (
        "task_params.dependence.dependTaskList[].dependItemList[].dateValue"
        in _sequence(data["required_paths"])
    )


def test_remote_shell_schema_exposes_ssh_as_the_only_connection_type() -> None:
    data = _mapping(task_type_service.task_type_schema_result("REMOTESHELL").data)
    fields = [_mapping(item) for item in _sequence(data["fields"])]
    remote_type = next(field for field in fields if field["path"] == "task_params.type")

    assert remote_type["type"] == "enum"
    assert remote_type["choices"] == ["SSH"]

    json_schema_data = _mapping(
        task_type_service.task_type_schema_result(
            "REMOTESHELL",
            json_schema=True,
        ).data
    )
    schema = _mapping(json_schema_data["schema"])
    definitions = _mapping(schema["$defs"])
    task_params = _mapping(definitions["task_params"])
    properties = _mapping(task_params["properties"])
    assert _mapping(properties["type"])["const"] == "SSH"


def test_task_type_schema_result_describes_fields_and_state_rules() -> None:
    result = task_type_service.task_type_schema_result("SQL")
    data = _mapping(result.data)
    fields = _sequence(data["fields"])
    state_rules = _sequence(data["state_rules"])

    assert result.resolved == {"task_type": "SQL", "view": "fields"}
    assert data["schema_version"] == 2
    assert "rows" not in data
    assert "schema" not in data
    assert "choice_sources" not in data
    assert "compile_mappings" not in data
    assert any(_mapping(field)["path"] == "task_params.sqlType" for field in fields)
    assert _mapping(state_rules[1])["when"] == "task_params.sqlType == 1"
    links = _mapping(data["links"])
    assert links["field"] == "dsctl task-type schema SQL --field FIELD_PATH"
    assert links["json_schema"] == "dsctl task-type schema SQL --json-schema"
    assert links["full"] == "dsctl task-type schema SQL --full"


@pytest.mark.parametrize("selector", ["context", "env-file"])
@pytest.mark.parametrize(
    "view", ["summary", "fields", "field", "json-schema", "compile-mappings", "full"]
)
def test_task_authoring_discovery_preserves_selected_target(
    tmp_path: Path,
    selector: str,
    view: str,
) -> None:
    env_file = tmp_path / "production settings.env"
    env_file.write_text(
        "DS_API_URL=https://production.example\n"
        "DS_API_TOKEN=private-token\n"
        "DS_VERSION=3.4.2\n",
        encoding="utf-8",
    )
    target = "production team" if selector == "context" else str(env_file)
    if selector == "context":
        create_context(target, env_file=env_file, api_url="https://production.example")
    scope = (
        invocation_scope(context_name=target)
        if selector == "context"
        else nullcontext()
    )
    selected_env_file = str(env_file) if selector == "env-file" else None
    with scope:
        result = (
            task_type_service.task_type_summary_result(
                "SQL", env_file=selected_env_file
            )
            if view == "summary"
            else task_type_service.task_type_schema_result(
                "SQL",
                field="task_params.datasource" if view == "field" else None,
                json_schema=view == "json-schema",
                compile_mappings=view == "compile-mappings",
                full=view == "full",
                env_file=selected_env_file,
            )
        )
    data = _mapping(result.data)
    commands: list[str] = []
    if view == "summary":
        commands.extend(
            str(data[key])
            for key in (
                "template_command",
                "raw_template_command",
                "schema_command",
                "template_index_command",
                "parameter_command",
            )
        )
        commands.extend(
            str(_mapping(row)["command"]) for row in _sequence(data["rows"])
        )
    elif view == "full":
        commands.extend(
            str(data[key]) for key in ("template_command", "raw_template_command")
        )
    else:
        commands.extend(str(command) for command in _mapping(data["links"]).values())
    for field in _sequence(data.get("fields", [])):
        source = _mapping(field).get("choice_source")
        if isinstance(source, str) and (
            "datasource list" in source or "enum list" in source
        ):
            commands.append(source)
    if view in {"json-schema", "full"}:
        metadata = _mapping(_mapping(data["schema"])["x-dsctl"])
        commands.extend(
            str(metadata[key]) for key in ("template_command", "raw_template_command")
        )
        assert metadata["lint_command_pattern"] == "dsctl lint workflow FILE"
    assert commands
    for command in commands:
        tokens = split(command)
        assert tokens[:3] == ["dsctl", f"--{selector}", target]
        assert tokens.count(f"--{selector}") == 1
    assert "private-token" not in json.dumps(data)


def test_task_field_typo_candidates_preserve_explicit_environment(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "production settings.env"
    env_file.write_text("DS_VERSION=3.4.2\n", encoding="utf-8")
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SQL", field="task_params.datasorce", env_file=str(env_file)
        )
    details = exc_info.value.details
    commands: list[object] = [details["discovery_command"]]
    commands.extend(
        _mapping(candidate)["command"] for candidate in _sequence(details["candidates"])
    )
    assert len(commands) > 1
    for command in commands:
        tokens = split(str(command))
        assert tokens[:3] == ["dsctl", "--env-file", str(env_file)]


def test_task_type_json_schema_preserves_nested_and_array_authoring_fields() -> None:
    result = task_type_service.task_type_schema_result("SHELL", json_schema=True)
    data = _mapping(result.data)
    schema = _mapping(data["schema"])
    properties = _mapping(schema["properties"])

    assert result.resolved == {"task_type": "SHELL", "view": "json_schema"}
    assert "fields" not in data
    assert "state_rules" not in data
    assert "choice_sources" not in data
    assert "compile_mappings" not in data
    schema_metadata = _mapping(schema["x-dsctl"])
    assert schema_metadata["lint_command_pattern"] == "dsctl lint workflow FILE"
    assert schema_metadata["lint_command"] == "dsctl lint workflow FILE"
    assert "state_rules" not in schema_metadata
    assert "choice_sources" not in schema_metadata
    assert "compile_mappings" not in schema_metadata

    retry = _mapping(properties["retry"])
    assert retry["type"] == "object"
    retry_properties = _mapping(retry["properties"])
    assert _mapping(retry_properties["times"])["type"] == "integer"
    assert _mapping(retry_properties["times"])["default"] == 0
    assert _mapping(retry_properties["interval"])["type"] == "integer"
    assert _mapping(retry_properties["interval"])["default"] == 0

    depends_on = _mapping(properties["depends_on"])
    assert depends_on["type"] == "array"
    assert depends_on["default"] == []
    assert _mapping(depends_on["items"])["type"] == "string"


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_task_type_json_schema_local_refs_resolve(task_type: str) -> None:
    result = task_type_service.task_type_schema_result(task_type, json_schema=True)
    schema = _mapping(_mapping(result.data)["schema"])

    for ref in _local_json_refs(schema):
        _resolve_local_json_ref(schema, ref)


def test_task_type_schema_result_exposes_field_discovery_commands() -> None:
    sql_result = task_type_service.task_type_schema_result("SQL")
    shell_result = task_type_service.task_type_schema_result("SHELL")
    conditions_result = task_type_service.task_type_schema_result("CONDITIONS")

    sql_data = _mapping(sql_result.data)
    shell_data = _mapping(shell_result.data)
    conditions_data = _mapping(conditions_result.data)

    sql_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(sql_data["fields"])
    }
    shell_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(shell_data["fields"])
    }
    conditions_fields = {
        _mapping(field)["path"]: _mapping(field)
        for field in _sequence(conditions_data["fields"])
    }

    assert sql_fields["task_params.groupId"]["choice_source"] == (
        "dsctl alert-group list"
    )
    assert "dsctl alert-group create --name NAME --instance-id ID" in _sequence(
        sql_fields["task_params.groupId"]["related_commands"]
    )

    resource_field = shell_fields["task_params.resourceList[].resourceName"]
    assert resource_field["choice_source"] == "dsctl resource list"
    assert (
        resource_field["choice_value"]
        == "fullName relative to the FILE root, retaining one leading slash"
    )
    assert "dsctl resource upload --file FILE" in _sequence(
        resource_field["related_commands"]
    )

    predicate_task = conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].task"
    ]
    assert predicate_task["choice_source"] == "other tasks in the same workflow YAML"
    assert conditions_fields[
        "task_params.dependence.dependTaskList[].dependItemList[].status"
    ]["choices"] == ["SUCCESS", "FAILURE"]
    assert not {
        "task_params.dependence.dependTaskList[].dependItemList[].dependentType",
        "task_params.dependence.dependTaskList[].dependItemList[].projectCode",
        "task_params.dependence.dependTaskList[].dependItemList[].definitionCode",
        "task_params.dependence.dependTaskList[].dependItemList[].depTaskCode",
        "task_params.dependence.dependTaskList[].dependItemList[].cycle",
        "task_params.dependence.dependTaskList[].dependItemList[].dateValue",
    }.intersection(conditions_fields)

    conditions_schema_result = task_type_service.task_type_schema_result(
        "CONDITIONS",
        json_schema=True,
    )
    conditions_schema = _mapping(_mapping(conditions_schema_result.data)["schema"])
    outer_defs = _mapping(conditions_schema["$defs"])
    params_schema = _mapping(outer_defs["task_params"])
    params_defs = _mapping(params_schema["$defs"])
    condition_result_schema = _mapping(params_defs["ConditionResultSpec"])
    condition_result_properties = _mapping(condition_result_schema["properties"])
    assert "conditionSuccess" not in condition_result_properties

    shell_full_result = task_type_service.task_type_schema_result("SHELL", full=True)
    shell_full_data = _mapping(shell_full_result.data)
    shell_choice_sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(shell_full_data["choice_sources"])
    }
    assert shell_choice_sources["task_params.resourceList[].resourceName"] == {
        "path": "task_params.resourceList[].resourceName",
        "command": "dsctl resource list",
        "value": "fullName relative to the FILE root, retaining one leading slash",
        "description": (
            "Run `dsctl resource list` without --dir for the FILE root in "
            "`resolved.directory`, then navigate directories and select a file "
            "`fullName`. Remove that FILE-root prefix and retain one leading "
            "slash for task_params.resourceList[].resourceName; when the root is /, "
            "fullName is already relative. Upload the file first when it is missing."
        ),
        "related_commands": [
            "dsctl resource list",
            "dsctl resource upload --file FILE",
            "dsctl resource view RESOURCE",
        ],
    }


def test_task_type_schema_result_supports_compile_and_full_views() -> None:
    compile_result = task_type_service.task_type_schema_result(
        "SQL",
        compile_mappings=True,
    )
    compile_data = _mapping(compile_result.data)

    assert compile_result.resolved == {
        "task_type": "SQL",
        "view": "compile_mappings",
    }
    assert "fields" not in compile_data
    mappings = _sequence(compile_data["compile_mappings"])
    assert compile_data["compile_mapping_policy"] == (
        "Compiled by workflow create/edit before sending DS REST form fields."
    )
    assert all(
        set(_mapping(mapping)) == {"authoring_path", "ds_payload_path"}
        for mapping in mappings
    )
    assert any(
        _mapping(mapping)["authoring_path"] == "task_params.sql" for mapping in mappings
    )

    full_result = task_type_service.task_type_schema_result("SQL", full=True)
    full_data = _mapping(full_result.data)
    assert full_result.resolved == {"task_type": "SQL", "view": "full"}
    assert "schema_version" not in full_data
    assert set(full_data) == {
        "task_type",
        "category",
        "kind",
        "schema",
        "fields",
        "state_rules",
        "choice_sources",
        "compile_mappings",
        "template_command",
        "raw_template_command",
    }
    assert "compatibility_variants" not in full_data
    full_fields = _sequence(full_data["fields"])
    assert all("choice_value" not in _mapping(field) for field in full_fields)
    full_schema_metadata = _mapping(_mapping(full_data["schema"])["x-dsctl"])
    assert full_schema_metadata["state_rules"] == full_data["state_rules"]
    assert full_schema_metadata["choice_sources"] == full_data["choice_sources"]
    assert full_schema_metadata["compile_mappings"] == full_data["compile_mappings"]


def test_task_type_schema_result_filters_one_field_and_related_rules() -> None:
    result = task_type_service.task_type_schema_result(
        "SQL",
        field="task_params.sqlType",
    )
    data = _mapping(result.data)

    assert result.resolved == {
        "task_type": "SQL",
        "view": "field",
        "field": "task_params.sqlType",
    }
    fields = _sequence(data["fields"])
    assert [_mapping(item)["path"] for item in fields] == ["task_params.sqlType"]
    state_rules = _sequence(data["state_rules"])
    assert [_mapping(item)["when"] for item in state_rules] == [
        "task_params.sqlType == 0",
        "task_params.sqlType == 1",
    ]


@pytest.mark.parametrize(
    ("task_type", "field", "expected_when"),
    [
        ("SQL", "task_params.sql", []),
        ("SQL", "task_params.preStatements[]", ["task_params.sqlType == 1"]),
        (
            "SQL",
            "task_params.sendEmail",
            ["task_params.sqlType == 0", "task_params.sqlType == 1"],
        ),
        (
            "DEPENDENT",
            "task_params.dependence.dependTaskList[].dependItemList[].cycle",
            [
                "dependItem.cycle == hour",
                "dependItem.cycle == day",
                "dependItem.cycle == week",
                "dependItem.cycle == month",
            ],
        ),
        (
            "CONDITIONS",
            "task_params.dependence.dependTaskList[].dependItemList[].task",
            [],
        ),
        (
            "SHELL",
            "task_params.localParams[]",
            ["command is set", "task_params is set"],
        ),
        ("SQL", "name", []),
    ],
)
def test_task_type_field_view_uses_explicit_state_rule_paths(
    task_type: str,
    field: str,
    expected_when: list[str],
) -> None:
    result = task_type_service.task_type_schema_result(task_type, field=field)
    rules = _sequence(_mapping(result.data)["state_rules"])

    assert [_mapping(rule)["when"] for rule in rules] == expected_when


def test_task_type_choice_value_is_only_emitted_for_selectable_values() -> None:
    result = task_type_service.task_type_schema_result("SHELL")
    fields = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(_mapping(result.data)["fields"])
    }

    assert fields["task_params.resourceList[].resourceName"]["choice_value"] == (
        "fullName relative to the FILE root, retaining one leading slash"
    )
    assert "choice_value" not in fields["task_params.localParams[]"]

    full_result = task_type_service.task_type_schema_result("SHELL", full=True)
    sources = {
        _mapping(item)["path"]: _mapping(item)
        for item in _sequence(_mapping(full_result.data)["choice_sources"])
    }
    assert "task_params.localParams[]" not in sources


@pytest.mark.parametrize(
    ("first_selector", "second_selector"),
    [
        ("--field", "--json-schema"),
        ("--field", "--compile-mappings"),
        ("--field", "--full"),
        ("--json-schema", "--compile-mappings"),
        ("--json-schema", "--full"),
        ("--compile-mappings", "--full"),
    ],
)
def test_task_type_schema_result_rejects_ambiguous_selectors(
    first_selector: str,
    second_selector: str,
) -> None:
    selected = {first_selector, second_selector}
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="command" if "--field" in selected else None,
            json_schema="--json-schema" in selected,
            compile_mappings="--compile-mappings" in selected,
            full="--full" in selected,
        )

    assert exc_info.value.details == {
        "constraint": "at_most_one_of",
        "selected": [first_selector, second_selector],
    }
    assert exc_info.value.suggestion is not None
    assert "only one" in exc_info.value.suggestion


def test_task_type_schema_result_returns_bounded_field_candidates() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="task_params.resource.resourceName",
        )

    details = exc_info.value.details
    assert details["task_type"] == "SHELL"
    assert details["field"] == "task_params.resource.resourceName"
    assert isinstance(details["available_count"], int)
    candidates = _sequence(details["candidates"])
    assert len(candidates) <= 3
    candidate = _mapping(candidates[0])
    assert set(candidate) == {"path", "command"}
    assert candidate["path"] == "task_params.resourceList[].resourceName"
    assert candidate["command"] == (
        "dsctl task-type schema SHELL --field 'task_params.resourceList[].resourceName'"
    )
    assert "available_fields" not in details
    candidate_command = candidate["command"]
    assert isinstance(candidate_command, str)
    assert candidate_command in (exc_info.value.suggestion or "")


def test_typed_sqoop_typos_recover_known_common_fields() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result("SQOOP", field="naem")

    candidates = _sequence(exc_info.value.details["candidates"])
    assert _mapping(candidates[0])["path"] == "name"
    assert "open_task_params" not in exc_info.value.details


def test_typed_sqoop_field_suggests_the_closed_task_params_contract() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SQOOP",
            field="task_params.pluginSpecificField",
        )

    candidates = _sequence(exc_info.value.details["candidates"])
    assert {_mapping(candidate)["path"] for candidate in candidates} == {
        "task_params.subcommand",
        "task_params.args",
        "task_params.args[]",
    }
    assert "open_task_params" not in exc_info.value.details


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_task_type_default_schema_has_bounded_compact_envelope(task_type: str) -> None:
    payload = result_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type),
    )
    compact = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()

    assert len(compact) < 12 * 1024


@pytest.mark.parametrize("task_type", supported_task_template_types())
def test_task_type_detailed_views_have_bounded_compact_envelopes(
    task_type: str,
) -> None:
    json_schema_payload = result_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type, json_schema=True),
    )
    compile_payload = result_payload(
        "task-type.schema",
        task_type_service.task_type_schema_result(task_type, compile_mappings=True),
    )
    default_data = _mapping(task_type_service.task_type_schema_result(task_type).data)
    fields = _sequence(default_data["fields"])

    def compact_size(payload: object) -> int:
        return len(
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )

    assert compact_size(json_schema_payload) < 10 * 1024
    assert compact_size(compile_payload) < 5 * 1024
    for field in fields:
        field_path = _mapping(field)["path"]
        assert isinstance(field_path, str)
        field_payload = result_payload(
            "task-type.schema",
            task_type_service.task_type_schema_result(task_type, field=field_path),
        )
        # Dependency date fields intentionally retain four cycle-specific rules;
        # correctness is worth the increase over a metadata-only field row.
        assert compact_size(field_payload) < 3 * 1024


def test_unknown_task_type_field_error_has_bounded_compact_envelope() -> None:
    with pytest.raises(UserInputError) as exc_info:
        task_type_service.task_type_schema_result(
            "SHELL",
            field="task_params.resource.resourceName",
        )

    payload = error_payload("task-type.schema", exc_info.value)
    compact = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert len(compact) < 2 * 1024
