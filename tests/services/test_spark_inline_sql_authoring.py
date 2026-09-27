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

from dsctl.errors import UserInputError
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.mutation import (
    WorkflowMutationPlan,
    prepare_workflow_mutation_plan,
)
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
    from dsctl.support.json_types import JsonObject


_SPARK_FACET = "SPARK/inline_local_sql"
_SPARK_VERSIONS = (
    "3.0.0",
    "3.0.6",
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
_SPARK_EARLY_VERSIONS = ("1.3.9", "2.0.0", "2.0.9")
_EMPTY_REFS = TaskRefIndex.from_code_by_name({})


class _SparkInlineSqlRuntimeSurface(Protocol):
    available: bool
    wire_epoch: str | None
    parameter_substitution: bool
    spark_home: str | None
    sql_logged: bool
    line_endings_normalized: bool
    failover_supported: bool
    retry_reexecutes: bool


class _TaskAuthoringSurfaceWithSparkInlineSql(Protocol):
    spark_inline_sql: _SparkInlineSqlRuntimeSurface


def _canonical_params(raw_script: str = "SELECT 1 AS answer") -> YamlObject:
    return {"rawScript": raw_script}


def _expected_native(ds_version: str, raw_script: str) -> YamlObject:
    if ds_version <= "3.1.9":
        return {
            "programType": "SQL",
            "sparkVersion": "SPARK2",
            "rawScript": raw_script,
            "deployMode": "local",
        }
    native: YamlObject = {
        "programType": "SQL",
        "rawScript": raw_script,
        "deployMode": "local",
        "sqlExecutionType": "SCRIPT",
    }
    if ds_version >= "3.2.2":
        native["master"] = "local"
    return native


def _spark_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(ds_version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"spark-inline-sql-{ds_version}"},
            "tasks": [
                {
                    "name": "run-inline-local-sql",
                    "type": "SPARK",
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
        _spark_spec(task_params, ds_version=ds_version),
        catalog=catalog,
    )
    payload = prepared.materialize([30_000])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "SPARK"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=ds_version,
        task_type="SPARK",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decoded_task_params(ds_version: str, task_params: YamlObject) -> YamlObject:
    projected = decode_task_parameters(
        version=ds_version,
        task_type="SPARK",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _template_yaml(ds_version: str) -> str:
    return authoring_prep.template_yaml("SPARK", ds_version, variant="minimal")


def _opaque_native_params() -> YamlObject:
    return {
        "programType": "JAVA",
        "deployMode": "cluster",
        "sparkVersion": "SPARK3",
        "mainClass": "com.example.AnalyticsJob",
        "mainJar": {"id": 91, "res": "analytics.jar"},
        "localParams": [],
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _fake_spark_dag(
    ds_version: str,
    task_params: YamlObject,
    *,
    workflow_name: str,
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name="run-inline-local-sql",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="SPARK",
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


def _spark_edit_plan(
    ds_version: str,
    task_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_spark_dag(
        ds_version,
        _expected_native(ds_version, "SELECT 1 AS answer"),
        workflow_name="spark-inline-edit",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "spark-inline-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "run-inline-local-sql",
                    "type": "SPARK",
                    "task_params": task_params,
                }
            ],
        },
        input_mode=input_mode,
    )


def _spark_metadata_edit_plan(
    ds_version: str,
    native_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_spark_dag(
        ds_version,
        native_params,
        workflow_name="spark-metadata-edit",
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="run-inline-local-sql",
        input_mode=input_mode,
    )


def _resolved_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    return authoring_prep.resolved_field_schema(task_params_schema, field_name)


def _schema_guidance(ds_version: str) -> str:
    return authoring_prep.schema_guidance("SPARK", ds_version)


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_catalog_exposes_one_exact_inline_local_sql_facet(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("SPARK")
    membership = catalog.require_facet("SPARK", _SPARK_FACET)
    fact = catalog.task_type_facts["SPARK"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("SPARK") is True
    assert catalog.supports_opaque_authoring("SPARK") is True
    assert profile.category == "Universal"
    assert profile.default_facet == _SPARK_FACET
    assert set(profile.facets) == {_SPARK_FACET}
    assert source_review is not None
    assert source_review.review == "spark-inline-local-sql-literal-subset"
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.contract.family == "spark-inline-local-sql-v1"
    assert membership.contract.params_model is not None
    assert (
        membership.contract.params_model.__name__ == "SparkInlineLocalSqlTaskParamsSpec"
    )
    assert membership.contract.opaque_authoring_selector is not None
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _SPARK_EARLY_VERSIONS)
def test_spark_inline_sql_facet_is_absent_but_plugin_remains_opaque(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert "SPARK" in catalog.upstream_task_types
    assert catalog.supports_typed_authoring("SPARK") is False
    assert catalog.supports_opaque_authoring("SPARK") is True
    opaque = _opaque_native_params()
    normalized = catalog.normalize_task_params(
        "SPARK", opaque, intent=TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert normalized == opaque
    assert normalized is not opaque


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_schema_exposes_only_literal_raw_script(ds_version: str) -> None:
    result = task_type_schema_result(
        "SPARK",
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

    assert result.data["task_type"] == "SPARK"
    assert result.data["category"] == "Universal"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == {"rawScript"}
    assert task_param_fields["rawScript"]["required"] is True
    assert task_param_fields["rawScript"]["compile_path"].endswith(
        "taskParams.rawScript"
    )
    assert result.data["state_rules"] == []

    guidance = str(task_param_fields["rawScript"].get("description", "")).lower()
    for term in ("spark", "sql", "local", "worker", "logged", "failover", "retry"):
        assert term in guidance


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.2.1", "3.4.2"])
def test_spark_json_schema_matches_closed_literal_projection(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "SPARK",
        json_schema=True,
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert isinstance(result.data, dict)
    schema = result.data["schema"]
    assert isinstance(schema, dict)
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    task_params = definitions["task_params"]
    assert isinstance(task_params, dict)
    properties = task_params["properties"]
    assert isinstance(properties, dict)

    assert task_params["additionalProperties"] is False
    assert set(task_params["required"]) == {"rawScript"}
    assert set(properties) == {"rawScript"}
    raw_script = _resolved_property_schema(task_params, "rawScript")
    assert raw_script["type"] == "string"
    assert raw_script["minLength"] == 1
    pattern = raw_script["pattern"]
    assert isinstance(pattern, str)
    for valid in (
        "SELECT 1",
        "\tSELECT\n  *\r\nFROM orders\t",
        "WITH x AS (SELECT 1)\nSELECT * FROM x",
    ):
        assert re.fullmatch(pattern, valid)
    for invalid in (
        "",
        "   \t\r\n",
        "\u0085",
        "\ufeff",
        "SELECT ${secret}",
        "SELECT $[yyyyMMdd]",
        "SELECT \x00",
        "SELECT \x01",
        "SELECT \x7f",
    ):
        assert re.fullmatch(pattern, invalid) is None


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.2.0", "3.4.2"])
def test_spark_summary_and_compile_mapping_lock_the_single_field_surface(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("SPARK", catalog=catalog)
    result = task_type_schema_result("SPARK", compile_mappings=True, catalog=catalog)
    assert isinstance(result.data, dict)
    mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["task_type"] == "SPARK"
    assert summary["category"] == "Universal"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.rawScript",
    }
    assert mappings == {
        "task_params.rawScript": "taskDefinitionJson[].taskParams.rawScript"
    }


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_minimal_template_compiles_to_exact_wire_epoch(ds_version: str) -> None:
    yaml_text = _template_yaml(ds_version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "SPARK"
    assert params == _canonical_params()
    assert _compiled_task_params(ds_version, params) == _expected_native(
        ds_version, "SELECT 1 AS answer"
    )

    guidance = yaml_text.lower().replace("-", " ")
    for term in ("spark", "sql", "worker", "logged", "retry", "failover"):
        assert term in guidance


@pytest.mark.parametrize(
    ("ds_version", "resource_file_guidance"),
    [
        ("3.0.0", "resource-file sql is absent"),
        ("3.1.9", "resource-file sql is absent"),
        ("3.2.0", "resource-file sql is also an explicit native opaque mode"),
        ("3.4.2", "resource-file sql is also an explicit native opaque mode"),
    ],
)
def test_spark_template_discloses_exact_native_opaque_modes(
    ds_version: str,
    resource_file_guidance: str,
) -> None:
    guidance = _template_yaml(ds_version).lower()

    assert "application programs remain explicit native opaque modes" in guidance
    assert resource_file_guidance in guidance


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_spark_model_preserves_sql_text_and_projector_selects_exact_epoch(
    ds_version: str,
    intent: TaskAuthoringIntent,
) -> None:
    raw_script = "\tSELECT col\r\nFROM orders\nWHERE id = 7\t"
    params = _canonical_params(raw_script)
    catalog = get_task_authoring_catalog(ds_version)

    normalized = catalog.normalize_task_params("SPARK", params, intent=intent)

    assert normalized == params
    assert normalized is not params
    expected = _expected_native(ds_version, raw_script)
    assert _encoded_task_params(ds_version, normalized) == expected
    assert _decoded_task_params(ds_version, expected) == params
    assert _compiled_task_params(ds_version, params) == expected


@pytest.mark.parametrize(
    "raw_script",
    [
        "SELECT 1",
        " SELECT 1 ",
        "\tSELECT 1\n",
        "SELECT ';' AS punctuation;",
        "WITH t AS (SELECT 1)\r\nSELECT * FROM t",
        "SELECT '中文' AS label",
    ],
)
def test_spark_raw_script_accepts_literal_sql_without_trimming_or_parsing(
    raw_script: str,
) -> None:
    params = _canonical_params(raw_script)
    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "SPARK", params, intent=TaskAuthoringIntent.TYPED_CREATE
    )
    assert normalized == params
    assert normalized["rawScript"] == raw_script


@pytest.mark.parametrize(
    "raw_script",
    [
        "",
        " ",
        " \t\r\n ",
        "\u0085",
        "\ufeff",
        "SELECT ${secret}",
        "SELECT $[yyyyMMdd]",
        "SELECT \x00",
        "SELECT \x01",
        "SELECT \x08",
        "SELECT \x0b",
        "SELECT \x0c",
        "SELECT \x1f",
        "SELECT \x7f",
    ],
)
@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_spark_raw_script_rejects_blank_placeholders_and_non_sql_controls(
    raw_script: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(ValueError, match="rawScript"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "SPARK", _canonical_params(raw_script), intent=intent
        )


@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_spark_raw_script_rejects_nonstrings(value: YamlValue) -> None:
    with pytest.raises(ValueError, match="rawScript"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "SPARK",
            {"rawScript": value},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("programType", "SQL"),
        ("deployMode", "local"),
        ("sparkVersion", "SPARK2"),
        ("sqlExecutionType", "SCRIPT"),
        ("master", "local"),
        ("localParams", []),
        ("futureField", {"enabled": True}),
    ],
)
@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_spark_typed_authoring_rejects_projector_owned_and_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value
    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "SPARK", params, intent=intent
        )


def test_spark_typed_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = _canonical_params()
    canonical["futureField"] = {"enabled": True}
    native = _expected_native("3.4.2", "SELECT 1")
    native["futureField"] = True

    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encoded_task_params("3.4.2", canonical)
    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _decoded_task_params("3.4.2", native)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("rawScript", "SELECT ${secret}"),
        ("rawScript", "SELECT $[yyyyMMdd]"),
        ("rawScript", " \t\r\n"),
        ("programType", "SQL"),
        ("sqlExecutionType", "SCRIPT"),
        ("futureField", {"native": True}),
    ],
)
def test_spark_invalid_sql_script_edits_never_downgrade_to_opaque(
    input_mode: str,
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(UserInputError, match=field):
        _spark_edit_plan("3.4.2", params, input_mode=input_mode)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("rawScript", "SELECT ${secret}"),
        ("rawScript", "SELECT $[yyyyMMdd]"),
        ("rawScript", ""),
        ("programType", "SQL"),
        ("sqlExecutionType", "SCRIPT"),
        ("futureField", {"native": True}),
    ],
)
def test_spark_invalid_sql_script_create_never_downgrades_to_opaque(
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value
    with pytest.raises(ValueError, match=field):
        _compiled_task_params("3.4.2", params)


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
        {"rawScript": "SELECT ${secret}"},
        {"rawScript": "SELECT 1", "programType": "SQL"},
        {
            "rawScript": "SELECT 1",
            "programType": "SQL",
            "sqlExecutionType": "SCRIPT",
        },
    ],
)
def test_spark_invalid_inline_sql_never_selects_an_opaque_public_intent(
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
    params: YamlObject,
) -> None:
    selected = get_task_authoring_catalog("3.4.2").effective_authoring_intent(
        "SPARK", requested=requested, task_params=params
    )
    assert selected is expected


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_safe_native_export_canonicalizes_and_reencodes_exactly(
    ds_version: str,
) -> None:
    raw_script = "SELECT order_id\nFROM orders"
    native_params = _expected_native(ds_version, raw_script)
    dag = _fake_spark_dag(ds_version, native_params, workflow_name="spark-safe-export")
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
    assert exported == _canonical_params(raw_script)
    assert isinstance(exported, dict)

    normalized = catalog.normalize_task_params(
        "SPARK",
        cast("YamlObject", exported),
        intent=TaskAuthoringIntent.TYPED_EDIT,
    )
    assert _decoded_task_params(ds_version, native_params) == exported
    assert _encoded_task_params(ds_version, normalized) == native_params
    assert _compiled_task_params(ds_version, normalized) == native_params


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    "raw_script",
    ["SELECT 1 FROM legacy_state", "SELECT ${secret} FROM legacy_state"],
)
def test_spark_unrecognized_raw_script_only_native_metadata_edit_is_lossless(
    ds_version: str,
    input_mode: str,
    raw_script: str,
) -> None:
    native_params: YamlObject = {"rawScript": raw_script}
    dag = _fake_spark_dag(
        ds_version,
        native_params,
        workflow_name="spark-unrecognized-native",
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
        baseline.projection_sources["run-inline-local-sql"]
        is ProjectionSource.OPAQUE_PRESERVE
    )

    plan = _spark_metadata_edit_plan(
        ds_version,
        native_params,
        input_mode=input_mode,
    )
    compiled = json.loads(
        json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]["taskParams"]
    )

    assert compiled == native_params


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_spark_exact_safe_native_metadata_edit_reprojects_the_exact_wire(
    ds_version: str,
    input_mode: str,
) -> None:
    native_params: YamlObject = _expected_native(
        ds_version,
        "SELECT 7 AS provenance",
    )
    dag = _fake_spark_dag(
        ds_version,
        native_params,
        workflow_name="spark-safe-provenance",
    )
    baseline = workflow_live_baseline(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        catalog=get_task_authoring_catalog(ds_version),
    )
    assert (
        baseline.projection_sources["run-inline-local-sql"]
        is ProjectionSource.TYPED_AUTHORING
    )

    plan = _spark_metadata_edit_plan(
        ds_version,
        native_params,
        input_mode=input_mode,
    )
    compiled = json.loads(
        json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]["taskParams"]
    )

    assert plan.merged_spec.tasks[0].task_params == {
        "rawScript": "SELECT 7 AS provenance"
    }
    assert compiled == native_params


@pytest.mark.parametrize(
    ("ds_version", "native_params"),
    [
        ("3.0.0", {"programType": "JAVA", "mainClass": "example.Job"}),
        (
            "3.2.0",
            {
                "programType": "SQL",
                "sqlExecutionType": "FILE",
                "resourceList": [{"id": 81, "res": "query.sql"}],
            },
        ),
        (
            "3.4.2",
            {
                "programType": "SQL",
                "sqlExecutionType": "FILE",
                "resourceList": [{"id": 82, "res": "query.sql"}],
                "futureField": {"preserve": True},
            },
        ),
    ],
)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_spark_java_and_file_metadata_edits_preserve_the_whole_opaque_package(
    ds_version: str,
    native_params: YamlObject,
    input_mode: str,
) -> None:
    plan = _spark_metadata_edit_plan(
        ds_version,
        native_params,
        input_mode=input_mode,
    )
    compiled = json.loads(
        json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]["taskParams"]
    )

    assert compiled == native_params


def test_spark_safe_native_rename_keeps_projection_provenance() -> None:
    ds_version = "3.4.2"
    native_params = _expected_native(ds_version, "SELECT 8 AS renamed")
    dag = _fake_spark_dag(
        ds_version,
        native_params,
        workflow_name="spark-rename-provenance",
    )
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "rename": [
                        {
                            "from": "run-inline-local-sql",
                            "to": "renamed-inline-local-sql",
                        }
                    ]
                }
            }
        }
    ).patch

    plan = prepare_workflow_mutation_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog(ds_version),
    )
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]

    assert definition["name"] == "renamed-inline-local-sql"
    assert json.loads(definition["taskParams"]) == native_params


def test_spark_deleted_native_task_drops_its_projection_provenance() -> None:
    ds_version = "3.4.2"
    deleted_native = _expected_native(ds_version, "SELECT 9 AS deleted")
    kept_native = _expected_native(ds_version, "SELECT 10 AS kept")
    tasks = [
        FakeTaskDefinition(
            code=101,
            name="run-inline-local-sql",
            project_code_value=7,
            project_name_value="analytics",
            task_type_value="SPARK",
            task_params_value=json.dumps(deleted_native),
            worker_group_value="default",
        ),
        FakeTaskDefinition(
            code=102,
            name="keep-inline-local-sql",
            project_code_value=7,
            project_name_value="analytics",
            task_type_value="SPARK",
            task_params_value=json.dumps(kept_native),
            worker_group_value="default",
        ),
    ]
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="spark-delete-provenance",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=tasks,
        workflow_task_relation_list_value=[],
    )
    patch = WorkflowPatchDocument.model_validate(
        {"patch": {"tasks": {"delete": ["run-inline-local-sql"]}}}
    ).patch

    plan = prepare_workflow_mutation_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog(ds_version),
    )
    definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])

    assert [definition["name"] for definition in definitions] == [
        "keep-inline-local-sql"
    ]
    assert json.loads(definitions[0]["taskParams"]) == kept_native


@pytest.mark.parametrize(
    ("ds_version", "native"),
    [
        (
            "3.0.0",
            {"programType": "JAVA", "mainClass": "com.example.Job"},
        ),
        ("3.1.9", {"programType": "SCALA", "mainClass": "example.Job"}),
        ("3.4.2", {"programType": "PYTHON", "mainJar": {"id": 12}}),
        (
            "3.2.0",
            {
                "programType": "SQL",
                "sqlExecutionType": "FILE",
                "resourceList": [{"id": 88}],
            },
        ),
        (
            "3.4.2",
            {
                "programType": "SQL",
                "sqlExecutionType": "FILE",
                "resourceList": [{"id": 89}],
            },
        ),
    ],
)
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.OPAQUE_CREATE),
        (TaskAuthoringIntent.TYPED_EDIT, TaskAuthoringIntent.OPAQUE_EDIT),
    ],
)
def test_spark_public_opaque_selector_accepts_only_excluded_program_modes(
    ds_version: str,
    native: YamlObject,
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    selected = catalog.effective_authoring_intent(
        "SPARK", requested=requested, task_params=native
    )
    assert selected is expected

    if requested is TaskAuthoringIntent.TYPED_CREATE:
        assert _compiled_task_params(ds_version, native) == native
        return

    validated = validate_workflow_document(
        {
            "workflow": {"name": f"opaque-spark-{ds_version}"},
            "tasks": [
                {
                    "name": "native-spark",
                    "type": "SPARK",
                    "task_params": native,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )
    assert validated.tasks[0].task_params == native


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.1.9"])
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_CREATE),
        (TaskAuthoringIntent.TYPED_EDIT, TaskAuthoringIntent.TYPED_EDIT),
    ],
)
def test_spark_file_sql_is_not_a_public_mode_before_3_2(
    ds_version: str,
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
) -> None:
    native: YamlObject = {
        "programType": "SQL",
        "sqlExecutionType": "FILE",
        "resourceList": [{"id": 88}],
    }
    catalog = get_task_authoring_catalog(ds_version)
    selected = catalog.effective_authoring_intent(
        "SPARK", requested=requested, task_params=native
    )
    assert selected is expected

    if requested is TaskAuthoringIntent.TYPED_CREATE:
        with pytest.raises(ValueError) as create_error:
            _compiled_task_params(ds_version, native)
        assert any(
            field in str(create_error.value)
            for field in ("programType", "resourceList", "sqlExecutionType")
        )
    else:
        with pytest.raises(UserInputError) as edit_error:
            _spark_edit_plan(ds_version, native, input_mode="file")
        assert any(
            field in str(edit_error.value)
            for field in ("programType", "resourceList", "sqlExecutionType")
        )


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.2.0", "3.4.2"])
@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_CREATE),
        (TaskAuthoringIntent.TYPED_EDIT, TaskAuthoringIntent.TYPED_EDIT),
    ],
)
@pytest.mark.parametrize(
    "native",
    [
        {"rawScript": "SELECT ${secret}"},
        {
            "programType": "SQL",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
            "rawScript": "SELECT ${secret}",
        },
        {
            "programType": "SQL",
            "deployMode": "local",
            "sqlExecutionType": "SCRIPT",
            "rawScript": "SELECT 1",
            "futureField": True,
        },
        {"programType": "R", "rawScript": "SELECT 1"},
    ],
)
def test_spark_public_opaque_selector_never_accepts_inline_sql_or_unknown_modes(
    ds_version: str,
    requested: TaskAuthoringIntent,
    expected: TaskAuthoringIntent,
    native: YamlObject,
) -> None:
    selected = get_task_authoring_catalog(ds_version).effective_authoring_intent(
        "SPARK", requested=requested, task_params=native
    )
    assert selected is expected

    if requested is TaskAuthoringIntent.TYPED_CREATE:
        with pytest.raises(ValueError):
            _compiled_task_params(ds_version, native)
    else:
        with pytest.raises(UserInputError):
            _spark_edit_plan(ds_version, native, input_mode="file")


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_opaque_preserve_deep_copies_excluded_and_future_fields(
    ds_version: str,
) -> None:
    native = _opaque_native_params()
    expected = deepcopy(native)
    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "SPARK", native, intent=TaskAuthoringIntent.OPAQUE_PRESERVE
    )

    assert preserved == expected
    assert preserved is not native
    main_jar = native["mainJar"]
    assert isinstance(main_jar, dict)
    main_jar["id"] = 999
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    assert preserved == expected


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_opaque_export_and_unchanged_edit_round_trip_losslessly(
    ds_version: str,
) -> None:
    native_params = _opaque_native_params()
    dag = _fake_spark_dag(
        ds_version, native_params, workflow_name="spark-opaque-roundtrip"
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
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        catalog=catalog,
    ).materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])
    assert compiled == native_params


@pytest.mark.parametrize("ds_version", _SPARK_EARLY_VERSIONS)
def test_spark_inline_runtime_surface_is_absent_before_3_0(ds_version: str) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithSparkInlineSql",
        get_task_authoring_surface(ds_version),
    ).spark_inline_sql

    assert surface.available is False
    assert surface.wire_epoch is None
    assert surface.parameter_substitution is False
    assert surface.spark_home is None
    assert surface.sql_logged is False
    assert surface.line_endings_normalized is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is False


@pytest.mark.parametrize(
    ("ds_version", "wire_epoch", "parameter_substitution", "spark_home"),
    [
        ("3.0.0", "legacy-spark2", False, "SPARK_HOME2"),
        ("3.0.6", "legacy-spark2", False, "SPARK_HOME2"),
        ("3.1.0", "legacy-spark2", True, "SPARK_HOME2"),
        ("3.1.9", "legacy-spark2", True, "SPARK_HOME2"),
        ("3.2.0", "script", True, "SPARK_HOME"),
        ("3.2.1", "script", True, "SPARK_HOME"),
        ("3.2.2", "script-master", True, "SPARK_HOME"),
        ("3.3.1", "script-master", True, "SPARK_HOME"),
        ("3.3.2", "script-master", True, "SPARK_HOME"),
        ("3.4.0", "script-master", True, "SPARK_HOME"),
        ("3.4.1", "script-master", True, "SPARK_HOME"),
        ("3.4.2", "script-master", True, "SPARK_HOME"),
    ],
)
def test_spark_runtime_surface_locks_wire_and_execution_epochs(
    ds_version: str,
    wire_epoch: str,
    *,
    parameter_substitution: bool,
    spark_home: str,
) -> None:
    surface = cast(
        "_TaskAuthoringSurfaceWithSparkInlineSql",
        get_task_authoring_surface(ds_version),
    ).spark_inline_sql

    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.parameter_substitution is parameter_substitution
    assert surface.spark_home == spark_home
    assert surface.sql_logged is True
    assert surface.line_endings_normalized is True
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.0.6"])
def test_spark_3_0_guidance_discloses_no_sql_parameter_substitution(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "before" in guidance
    assert "parameter substitution" in guidance
    assert "not replaced" in guidance
    assert "spark_home2" in guidance
    assert "logged" in guidance


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.1.9"])
def test_spark_3_1_guidance_discloses_prepared_map_expansion(ds_version: str) -> None:
    guidance = _schema_guidance(ds_version)

    assert "prepared" in guidance
    assert "before" in guidance
    assert "spark_home2" in guidance
    assert "logged" in guidance


@pytest.mark.parametrize("ds_version", _SPARK_VERSIONS)
def test_spark_guidance_distinguishes_wire_spelling_from_runtime_newlines(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "canonical and wire spelling is preserved" in guidance
    assert "normalizes crlf" in guidance
    assert "worker sql file" in guidance


@pytest.mark.parametrize("ds_version", ["3.2.0", "3.2.1", "3.4.2"])
def test_spark_3_2_plus_guidance_discloses_spark_home_and_expansion(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    assert "prepared" in guidance
    assert "spark_home" in guidance
    assert "logged" in guidance


@pytest.mark.parametrize("ds_version", ["3.0.0", "3.1.0", "3.2.0", "3.4.2"])
def test_spark_guidance_discloses_runtime_prerequisites_and_retry_risk(
    ds_version: str,
) -> None:
    guidance = _schema_guidance(ds_version)

    for term in ("spark", "java", "hadoop", "hive", "catalog", "permission"):
        assert term in guidance
    assert "synchronous" in guidance
    assert "local process" in guidance
    assert "failover" in guidance
    assert "retry" in guidance
    assert "side effect" in guidance
    assert "live" in guidance
    assert "confidential" in guidance
