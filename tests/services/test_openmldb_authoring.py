from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, Protocol, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.task_authoring import (
    task_type_schema_result,
    task_type_summary_data,
)
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject


_OPENMLDB_FACET = "OPENMLDB/literal_single_statement"
_OPENMLDB_VERSIONS = (
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_OPENMLDB_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_OPENMLDB_FINGERPRINTS = {
    "3.1.0": "sha256:641a2e59b50f9f68d8104d93cd3e57473c3cb64f220ff15176a51d2eec368b00",
    "3.1.9": "sha256:75f81707134adb55c79e1f614eee7a893fcb8d06abc199bdae3af73c7f6af421",
    "3.2.0": "sha256:4a67716f871e27f751196d8993228c295b72809ccf9b939bb41238185ef9c554",
    "3.2.1": "sha256:4a67716f871e27f751196d8993228c295b72809ccf9b939bb41238185ef9c554",
    "3.2.2": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
    "3.3.1": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
    "3.3.2": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
    "3.4.0": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
    "3.4.1": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
    "3.4.2": "sha256:d82d54aecc8febe7aaea4a7ddeb35191dc667edf59561c5ed5b3cf96a94bc6aa",
}
_REQUIRED_TYPED_FIELDS = {"zk", "zkPath", "executeMode", "sql"}
_EMPTY_REFS = TaskRefIndex.from_code_by_name({})


class _OpenmldbRuntimeSurface(Protocol):
    available: bool
    python_launcher: str | None
    task_params_logged: bool
    sql_logged: bool
    rendered_script_logged: bool
    generated_script_logged: bool
    line_endings_normalized: bool
    result_output_supported: bool
    failover_supported: bool
    retry_reexecutes: bool


class _TaskAuthoringSurfaceWithOpenmldb(Protocol):
    openmldb: _OpenmldbRuntimeSurface


def _canonical_params(
    *,
    zk: str = "localhost:2181",
    zk_path: str = "/openmldb",
    execute_mode: str = "offline",
    sql: str = "SELECT 1",
) -> YamlObject:
    return {
        "zk": zk,
        "zkPath": zk_path,
        "executeMode": execute_mode,
        "sql": sql,
    }


def _openmldb_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(ds_version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"openmldb-{ds_version}"},
            "tasks": [
                {
                    "name": "execute-openmldb-sql",
                    "type": "OPENMLDB",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(ds_version)
    prepared = prepare_workflow_create_compilation(
        _openmldb_spec(task_params, ds_version=ds_version),
        catalog=catalog,
    )
    payload = prepared.materialize([34_100])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "OPENMLDB"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=ds_version,
        task_type="OPENMLDB",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = decode_task_parameters(
        version=ds_version,
        task_type="OPENMLDB",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _template_yaml(ds_version: str) -> str:
    return authoring_prep.template_yaml("OPENMLDB", ds_version, variant="minimal")


def _fake_openmldb_dag(
    ds_version: str,
    task_params: YamlObject,
    *,
    workflow_name: str,
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name="execute-openmldb-sql",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="OPENMLDB",
        task_params_value=json.dumps(task_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )


def _openmldb_edit_plan(
    ds_version: str,
    task_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_openmldb_dag(
        ds_version,
        _canonical_params(),
        workflow_name="openmldb-edit",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "openmldb-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "execute-openmldb-sql",
                    "type": "OPENMLDB",
                    "task_params": task_params,
                }
            ],
        },
        input_mode=input_mode,
    )


def _openmldb_metadata_edit_plan(
    ds_version: str,
    native_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_openmldb_dag(
        ds_version,
        native_params,
        workflow_name="openmldb-metadata-edit",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="execute-openmldb-sql",
        input_mode=input_mode,
    )


def _compiled_params_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    task_params = json.loads(definition["taskParams"])
    assert isinstance(task_params, dict)
    return cast("YamlObject", task_params)


def _resolved_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    return authoring_prep.resolved_field_schema(task_params_schema, field_name)


def _task_params_json_schema(ds_version: str) -> dict[object, object]:
    return authoring_prep.task_params_schema("OPENMLDB", ds_version)


def _schema_guidance(ds_version: str) -> str:
    return authoring_prep.schema_guidance("OPENMLDB", ds_version)


def _runtime_accepts(field_name: str, value: YamlValue) -> bool:
    params = _canonical_params()
    params[field_name] = value
    try:
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "OPENMLDB",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    except ValueError:
        return False
    return True


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
def test_openmldb_catalog_exposes_exact_literal_single_statement_facet(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("OPENMLDB")
    membership = catalog.require_facet("OPENMLDB", _OPENMLDB_FACET)
    fact = catalog.task_type_facts["OPENMLDB"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("OPENMLDB") is True
    assert catalog.supports_opaque_authoring("OPENMLDB") is False
    assert profile.category == "MachineLearning"
    assert profile.default_facet == _OPENMLDB_FACET
    assert set(profile.facets) == {_OPENMLDB_FACET}
    assert fact.semantic_fingerprint == _OPENMLDB_FINGERPRINTS[ds_version]
    assert source_review is not None
    assert (
        source_review.review == "openmldb-literal-single-statement-python-safe-subset"
    )
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.contract.family == "openmldb-literal-single-statement-v1"
    assert membership.contract.params_model is not None
    assert (
        membership.contract.params_model.__name__
        == "OpenmldbLiteralSingleStatementTaskParamsSpec"
    )
    assert membership.contract.opaque_authoring_selector is None
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _OPENMLDB_ABSENT_VERSIONS)
def test_openmldb_is_absent_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert "OPENMLDB" not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring("OPENMLDB") is False
    assert catalog.supports_opaque_authoring("OPENMLDB") is False
    assert "OPENMLDB" not in catalog.authoring_task_types


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
def test_openmldb_schema_exposes_only_the_four_literal_native_fields(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "OPENMLDB",
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_param_fields = {
        path.removeprefix("task_params."): field
        for path, field in fields.items()
        if path.startswith("task_params.")
    }

    assert result.data["task_type"] == "OPENMLDB"
    assert result.data["category"] == "MachineLearning"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == _REQUIRED_TYPED_FIELDS
    assert result.data["state_rules"] == []
    for field_name in _REQUIRED_TYPED_FIELDS:
        assert task_param_fields[field_name]["required"] is True
        assert task_param_fields[field_name]["compile_path"].endswith(
            f"taskParams.{field_name}"
        )
    for excluded in ("localParams", "varPool", "resourceList", "futureField"):
        assert excluded not in task_param_fields

    descriptions = " ".join(
        str(field.get("description", "")) for field in task_param_fields.values()
    ).lower()
    for term in (
        "openmldb",
        "zookeeper",
        "python3",
        "worker",
        "logged",
        "result",
        "failover",
        "retry",
    ):
        assert term in descriptions


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
def test_openmldb_json_schema_is_closed_and_requires_every_native_field(
    ds_version: str,
) -> None:
    task_params = _task_params_json_schema(ds_version)
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(required) == _REQUIRED_TYPED_FIELDS
    assert set(properties) == _REQUIRED_TYPED_FIELDS

    for field_name in ("zk", "zkPath", "sql"):
        field_schema = _resolved_property_schema(task_params, field_name)
        assert field_schema["type"] == "string"
        assert field_schema["minLength"] == 1
        assert isinstance(field_schema["pattern"], str)

    execute_mode = _resolved_property_schema(task_params, "executeMode")
    execute_mode_values = execute_mode["enum"]
    assert isinstance(execute_mode_values, list)
    assert execute_mode["type"] == "string"
    assert set(execute_mode_values) == {"offline", "online"}


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.2", "3.4.2"])
def test_openmldb_summary_and_compile_mappings_cover_exactly_four_fields(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("OPENMLDB", catalog=catalog)
    result = task_type_schema_result(
        "OPENMLDB",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["task_type"] == "OPENMLDB"
    assert summary["category"] == "MachineLearning"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        *{f"task_params.{field_name}" for field_name in _REQUIRED_TYPED_FIELDS},
    }
    assert set(mappings) == {
        f"task_params.{field_name}" for field_name in _REQUIRED_TYPED_FIELDS
    }
    for authoring_path, payload_path in mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
def test_openmldb_minimal_template_prepares_exact_native_identity(
    ds_version: str,
) -> None:
    yaml_text = _template_yaml(ds_version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "OPENMLDB"
    assert set(params) == _REQUIRED_TYPED_FIELDS
    normalized = get_task_authoring_catalog(ds_version).normalize_task_params(
        "OPENMLDB",
        cast("YamlObject", params),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == params
    assert _compiled_task_params(ds_version, normalized) == params

    guidance = yaml_text.lower().replace("-", "_")
    for term in (
        "python3",
        "openmldb",
        "worker",
        "logged",
        "result",
        "retry",
        "failover",
    ):
        assert term in guidance
    expected_launcher = "python_home" if ds_version < "3.2.0" else "python_launcher"
    assert expected_launcher in guidance


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_openmldb_model_projector_and_prepared_create_preserve_native_identity(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(
        zk="zk-1.example.com:2181,zk-2.example.com:2182",
        zk_path="/production/features.v2",
        execute_mode="online",
        sql="\tSELECT '中文' AS label\nFROM features\t",
    )
    catalog = get_task_authoring_catalog(ds_version)

    normalized = catalog.normalize_task_params("OPENMLDB", params, intent=intent)

    assert normalized == params
    assert normalized is not params
    assert _encoded_task_params(ds_version, normalized) == params
    assert _decoded_task_params(ds_version, params) == params
    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_openmldb_prepared_edit_projects_the_exact_replacement_params(
    ds_version: str,
    input_mode: str,
) -> None:
    params = _canonical_params(
        zk="zk-a.internal:2181,zk-b.internal:2181",
        zk_path="/prod/openmldb",
        execute_mode="online",
        sql="SELECT feature_id\nFROM feature_table",
    )

    plan = _openmldb_edit_plan(ds_version, params, input_mode=input_mode)

    assert plan.merged_spec.tasks[0].task_params == params
    assert _compiled_params_from_plan(plan) == params


@pytest.mark.parametrize(
    ("field_name", "valid_values", "invalid_values"),
    [
        (
            "zk",
            (
                "localhost:1",
                "localhost:2181",
                "127.0.0.1:2181",
                "1.2.3.4:65535",
                "zk-1.example.com:2181,zk-2.example.com:65535",
                f"{'a' * 63}:2181",
            ),
            (
                "",
                " ",
                "localhost",
                ":2181",
                "localhost:",
                "localhost:0",
                "localhost:02181",
                "localhost:65536",
                "localhost:2181,",
                ",localhost:2181",
                "localhost:2181, zk-2:2181",
                "999.999.999.999:2181",
                "256.0.0.1:2181",
                "-zk.example.com:2181",
                "zk-.example.com:2181",
                f"{'a' * 64}:2181",
                "http://localhost:2181",
                "alice:secret@localhost:2181",
                "${ZK}:2181",
                "$[zk]:2181",
                "localhost:2181'\nimport os",
                "localhost:2181\\next",
            ),
        ),
        (
            "zkPath",
            (
                "/",
                "/openmldb",
                "/production/openmldb-1",
                "/tenant_7/features.v2",
                "/A0/_cluster",
                "/features.",
                "/features..v2",
            ),
            (
                "",
                " ",
                "openmldb",
                "/openmldb/",
                "//openmldb",
                "/openmldb//features",
                "/.",
                "/..",
                "/prod/../openmldb",
                "/.hidden",
                "/prod/.hidden",
                "/prod openmldb",
                "/${cluster}",
                "/$[cluster]",
                "/openmldb'\nimport os",
                "/openmldb\\next",
                "/openmldb?token=value",
            ),
        ),
        (
            "sql",
            (
                "SELECT 1",
                " SELECT 1 ",
                "\tSELECT feature_id\nFROM features\t",
                "WITH x AS (SELECT 1)\nSELECT * FROM x",
                "SELECT '中文' AS label",
                "SELECT '$value' AS literal",
            ),
            (
                "",
                " ",
                "\t\n",
                "\u00a0",
                "\ufeff",
                "SELECT 1;",
                "SELECT ';' AS punctuation",
                'SELECT "unsafe"',
                "SELECT \\path",
                "SELECT ${secret}",
                "SELECT $[yyyyMMdd]",
                "SELECT\r1",
                "SELECT\r\n1",
                "SELECT \x00",
                "SELECT \x01",
                "SELECT \x08",
                "SELECT \x0b",
                "SELECT \x0c",
                "SELECT \x1f",
                "SELECT \x7f",
                "SELECT \x80",
                "SELECT \x85",
                "SELECT \x9f",
                *(
                    f"SELECT {chr(codepoint)}"
                    for codepoint in range(0x20)
                    if codepoint not in {0x09, 0x0A}
                ),
                *(f"SELECT {chr(codepoint)}" for codepoint in range(0x7F, 0xA0)),
            ),
        ),
    ],
)
def test_openmldb_json_schema_patterns_match_runtime_validation(
    field_name: str,
    valid_values: tuple[str, ...],
    invalid_values: tuple[str, ...],
) -> None:
    field_schema = _resolved_property_schema(
        _task_params_json_schema("3.4.2"),
        field_name,
    )
    pattern = field_schema["pattern"]
    assert isinstance(pattern, str)

    for value in valid_values:
        assert re.fullmatch(pattern, value), (field_name, value)
        assert _runtime_accepts(field_name, value) is True
    for value in invalid_values:
        assert re.fullmatch(pattern, value) is None, (field_name, value)
        assert _runtime_accepts(field_name, value) is False


@pytest.mark.parametrize("execute_mode", ["offline", "online"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_openmldb_execute_mode_accepts_only_exact_lowercase_literals(
    execute_mode: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params(execute_mode=execute_mode)
    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "OPENMLDB",
        params,
        intent=intent,
    )

    assert normalized == params
    assert normalized["executeMode"] == execute_mode


@pytest.mark.parametrize(
    "execute_mode",
    ["OFFLINE", "ONLINE", "Offline", "online ", " online", "batch", "", 1, None],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_openmldb_execute_mode_rejects_aliases_and_coercion(
    execute_mode: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["executeMode"] = execute_mode

    with pytest.raises(ValueError, match="executeMode"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "OPENMLDB",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("field_name", sorted(_REQUIRED_TYPED_FIELDS))
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_openmldb_typed_create_and_edit_require_all_four_fields(
    field_name: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params.pop(field_name)

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "OPENMLDB",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("field_name", sorted(_REQUIRED_TYPED_FIELDS))
@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_openmldb_string_fields_reject_nonstring_values(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "OPENMLDB",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("futureField", {"enabled": True}),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_openmldb_typed_authoring_rejects_inherited_and_future_fields(
    field_name: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "OPENMLDB",
            params,
            intent=intent,
        )


def test_openmldb_typed_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = _canonical_params()
    canonical["futureField"] = {"enabled": True}
    native = _canonical_params()
    native["localParams"] = []

    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encoded_task_params("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="localParams"):
        _decoded_task_params("3.4.2", native)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("zk", "${ZK}:2181"),
        ("zkPath", "/${cluster}"),
        ("executeMode", "OFFLINE"),
        ("sql", "SELECT ${secret}"),
        ("sql", 'SELECT "unsafe"'),
        ("sql", "SELECT 1; SELECT 2"),
    ],
)
def test_openmldb_typed_decode_rejects_unsafe_native_values(
    field_name: str,
    value: YamlValue,
) -> None:
    native = _canonical_params()
    native[field_name] = value

    with pytest.raises(TaskParameterProjectionError, match=field_name):
        _decoded_task_params("3.4.2", native)


@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_openmldb_typed_projector_wraps_nonstring_execute_mode_errors(
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params["executeMode"] = value

    with pytest.raises(TaskParameterProjectionError, match="executeMode"):
        _encoded_task_params("3.4.2", params)
    with pytest.raises(TaskParameterProjectionError, match="executeMode"):
        _decoded_task_params("3.4.2", params)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("zk", "localhost"),
        ("zkPath", "relative/path"),
        ("executeMode", "OFFLINE"),
        ("sql", "SELECT ${secret}"),
        ("sql", "SELECT 1; SELECT 2"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_openmldb_invalid_canonical_create_never_downgrades_to_opaque(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        _compiled_task_params("3.4.2", params)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("zk", "localhost"),
        ("zkPath", "relative/path"),
        ("executeMode", "OFFLINE"),
        ("sql", "SELECT ${secret}"),
        ("sql", "SELECT 1; SELECT 2"),
        ("localParams", []),
        ("futureField", {"native": True}),
    ],
)
def test_openmldb_invalid_canonical_edit_never_downgrades_to_opaque(
    input_mode: str,
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field_name] = value

    with pytest.raises(UserInputError, match=field_name):
        _openmldb_edit_plan("3.4.2", params, input_mode=input_mode)


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_CREATE),
        (TaskAuthoringIntent.TYPED_EDIT, TaskAuthoringIntent.TYPED_EDIT),
    ],
)
@pytest.mark.parametrize(
    "params",
    [
        {**_canonical_params(), "sql": "SELECT ${secret}"},
        {**_canonical_params(), "executeMode": "OFFLINE"},
        {**_canonical_params(), "localParams": []},
        {**_canonical_params(), "futureField": True},
    ],
)
def test_openmldb_invalid_params_never_select_an_opaque_public_intent(
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
    params: YamlObject,
) -> None:
    selected = get_task_authoring_catalog("3.4.2").effective_authoring_intent(
        "OPENMLDB",
        requested=requested,
        task_params=params,
    )

    assert selected is expected


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_openmldb_has_no_public_raw_opaque_create_or_edit_selector(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    native = _canonical_params()
    native["localParams"] = []

    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog(ds_version).normalize_task_params(
            "OPENMLDB",
            native,
            intent=intent,
        )

    assert str(captured.value) == (
        f"OPENMLDB opaque authoring is unsupported for DolphinScheduler {ds_version}."
    )
    assert captured.value.details == {
        "selected_version": ds_version,
        "task_type": "OPENMLDB",
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {ds_version} policy permits only opaque "
            "preservation for OPENMLDB."
        ),
    }


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
def test_openmldb_safe_native_export_is_typed_and_reencodes_exactly(
    ds_version: str,
) -> None:
    native_params = _canonical_params(
        zk="zk-1.internal:2181,zk-2.internal:2181",
        zk_path="/production/openmldb",
        execute_mode="online",
        sql="SELECT feature_id\nFROM features",
    )
    dag = _fake_openmldb_dag(
        ds_version,
        native_params,
        workflow_name="openmldb-safe-export",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    exported = document["tasks"][0]["task_params"]
    assert exported == native_params
    assert isinstance(exported, dict)

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    assert (
        baseline.projection_sources["execute-openmldb-sql"]
        is ProjectionSource.TYPED_AUTHORING
    )
    normalized = catalog.normalize_task_params(
        "OPENMLDB",
        cast("YamlObject", exported),
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    assert _decoded_task_params(ds_version, native_params) == exported
    assert _encoded_task_params(ds_version, normalized) == native_params
    assert _compiled_task_params(ds_version, normalized) == native_params


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_openmldb_safe_native_metadata_edit_keeps_typed_provenance(
    ds_version: str,
    input_mode: str,
) -> None:
    native_params = _canonical_params(
        zk="zk-a.internal:2181,zk-b.internal:2181",
        zk_path="/prod/features.v2",
        execute_mode="online",
        sql="SELECT 7 AS provenance",
    )

    plan = _openmldb_metadata_edit_plan(
        ds_version,
        native_params,
        input_mode=input_mode,
    )

    assert plan.merged_spec.tasks[0].task_params == native_params
    assert _compiled_params_from_plan(plan) == native_params


def _unsafe_or_extra_native_params(case: str) -> YamlObject:
    native = _canonical_params()
    if case == "unsafe-zk":
        native["zk"] = "localhost:2181'\nimport os"
    elif case == "unsafe-sql":
        native["sql"] = "SELECT ${secret}"
    elif case == "extra-field":
        native["localParams"] = [
            {
                "prop": "runtime_secret",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "${project_secret}",
            }
        ]
        native["futureField"] = {"nested": ["native", {"preserve": True}]}
    else:
        raise AssertionError(case)
    return native


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize("case", ["unsafe-zk", "unsafe-sql", "extra-field"])
def test_openmldb_unsafe_or_extra_native_metadata_edit_is_lossless(
    ds_version: str,
    input_mode: str,
    case: str,
) -> None:
    native_params = _unsafe_or_extra_native_params(case)
    dag = _fake_openmldb_dag(
        ds_version,
        native_params,
        workflow_name="openmldb-opaque-preserve",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(ds_version)

    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )
    assert document["tasks"][0]["task_params"] == native_params

    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    assert (
        baseline.projection_sources["execute-openmldb-sql"]
        is ProjectionSource.OPAQUE_PRESERVE
    )
    plan = _openmldb_metadata_edit_plan(
        ds_version,
        native_params,
        input_mode=input_mode,
    )

    assert _compiled_params_from_plan(plan) == native_params


@pytest.mark.parametrize("ds_version", _OPENMLDB_VERSIONS)
def test_openmldb_opaque_preserve_deep_copies_inherited_and_future_state(
    ds_version: str,
) -> None:
    native = _unsafe_or_extra_native_params("extra-field")
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "OPENMLDB",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == expected
    assert preserved is not native
    local_params = native["localParams"]
    assert isinstance(local_params, list)
    local_param = local_params[0]
    assert isinstance(local_param, dict)
    local_param["value"] = "mutated"
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    assert preserved == expected


@pytest.mark.parametrize("ds_version", _OPENMLDB_ABSENT_VERSIONS)
def test_openmldb_runtime_surface_is_absent_before_3_1(ds_version: str) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithOpenmldb",
        get_task_authoring_surface(ds_version),
    ).openmldb

    assert surface.available is False
    assert surface.python_launcher is None
    assert surface.task_params_logged is False
    assert surface.sql_logged is False
    assert surface.rendered_script_logged is False
    assert surface.generated_script_logged is False
    assert surface.line_endings_normalized is False
    assert surface.result_output_supported is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is False


@pytest.mark.parametrize(
    ("ds_version", "python_launcher"),
    [
        ("3.1.0", "PYTHON_HOME"),
        ("3.1.9", "PYTHON_HOME"),
        ("3.2.0", "PYTHON_LAUNCHER"),
        ("3.2.1", "PYTHON_LAUNCHER"),
        ("3.2.2", "PYTHON_LAUNCHER"),
        ("3.3.1", "PYTHON_LAUNCHER"),
        ("3.3.2", "PYTHON_LAUNCHER"),
        ("3.4.0", "PYTHON_LAUNCHER"),
        ("3.4.1", "PYTHON_LAUNCHER"),
        ("3.4.2", "PYTHON_LAUNCHER"),
    ],
)
def test_openmldb_runtime_surface_locks_launcher_logging_and_retry_semantics(
    ds_version: str,
    python_launcher: str,
) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithOpenmldb",
        get_task_authoring_surface(ds_version),
    ).openmldb

    assert surface.available is True
    assert surface.python_launcher == python_launcher
    assert surface.task_params_logged is True
    assert surface.sql_logged is True
    assert surface.rendered_script_logged is True
    assert surface.generated_script_logged is True
    assert surface.line_endings_normalized is True
    assert surface.result_output_supported is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.9"])
def test_openmldb_3_1_guidance_names_the_exact_legacy_launcher(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "python_home" in guidance
    assert "python_launcher" not in guidance
    assert "python3" in guidance


@pytest.mark.parametrize(
    "ds_version",
    ["3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.4.2"],
)
def test_openmldb_3_2_plus_guidance_names_the_exact_modern_launcher(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "python_launcher" in guidance
    assert "python3" in guidance


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_openmldb_guidance_discloses_worker_prerequisites_and_full_logging(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    for prerequisite in (
        "python3",
        "openmldb",
        "sqlalchemy",
        "sdk",
        "worker",
        "zookeeper",
        "permission",
    ):
        assert prerequisite in guidance
    assert "task params" in guidance
    assert "raw sql" in guidance
    assert "rendered python" in guidance
    assert "final generated python file" in guidance
    assert "info" in guidance
    assert "secret" in guidance
    assert "does not redact" in guidance


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_openmldb_guidance_discloses_result_failover_and_retry_limits(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "synchronous" in guidance
    assert "local python3 process" in guidance
    assert "result" in guidance
    assert "discard" in guidance
    assert "failover" in guidance
    assert "resume" in guidance
    assert "retry" in guidance
    assert "reexecute" in guidance
    assert "side effect" in guidance


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_openmldb_guidance_explains_safe_sql_and_line_ending_boundary(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "single statement" in guidance
    assert "semicolon" in guidance
    assert "double quote" in guidance
    assert "backslash" in guidance
    assert "placeholder" in guidance
    assert "canonical and wire spelling" in guidance
    assert "rejects cr" in guidance
    assert "normalizes crlf" in guidance


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_openmldb_guidance_explains_exact_execute_mode_runtime_behavior(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "offline" in guidance
    assert "online" in guidance
    assert "sync_job" in guidance
    assert "job_timeout" in guidance
    assert "1800000" in guidance
