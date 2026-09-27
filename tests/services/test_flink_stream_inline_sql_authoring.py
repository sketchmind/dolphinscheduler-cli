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
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
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
from dsctl.services.template import task_template_result
from dsctl.upstream.resolver import ResolvedProject
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject


_FACET = "FLINK_STREAM/inline_local_sql"
_TYPED_VERSIONS = (
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
)
_UNSUPPORTED_EXECUTOR_VERSIONS = (
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_FINGERPRINTS = {
    "3.1.5": "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e",
    "3.1.6": "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e",
    "3.1.7": "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e",
    "3.1.8": "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e",
    "3.1.9": "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e",
    "3.2.0": "sha256:e24ff00bc70305ee4f8d13356dbe9265546977dc8a250736ae1f495d84b0a3c3",
    "3.2.1": "sha256:e24ff00bc70305ee4f8d13356dbe9265546977dc8a250736ae1f495d84b0a3c3",
    "3.2.2": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
    "3.3.1": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
    "3.3.2": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
    "3.4.0": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
    "3.4.1": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
    "3.4.2": "sha256:887352c0ac165c821d2db81362fe16ba60c12e96660547112e524aea494c9dd9",
}
_REFS = TaskRefIndex.from_code_by_name({})


class _FlinkStreamSurface(Protocol):
    available: bool
    wire_epoch: str | None
    script_encoding: str | None
    sql_command: str | None
    parameter_substitution: bool
    task_params_logged: bool
    script_logged: bool
    command_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_reexecutes: bool
    local_sql_application_id_expected: bool
    cancel_requires_application_id: bool
    savepoint_requires_application_id: bool
    stop_sequence: str
    reliable_stop_supported: bool
    exclusion_reason: str | None


class _Surface(Protocol):
    flink_stream_inline_sql: _FlinkStreamSurface


def _canonical(raw_script: str = "SELECT 1") -> YamlObject:
    return {"rawScript": raw_script}


def _native(raw_script: str = "SELECT 1") -> YamlObject:
    return {
        "programType": "SQL",
        "deployMode": "local",
        "initScript": "",
        "rawScript": raw_script,
    }


def _opaque_native() -> YamlObject:
    return {
        "programType": "JAVA",
        "deployMode": "cluster",
        "mainClass": "com.example.StreamJob",
        "mainJar": {"id": 91, "res": "flink-stream-job.jar"},
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _spec(
    version: str,
    params: YamlObject,
    *,
    intent: TaskAuthoringIntent = TaskAuthoringIntent.TYPED_CREATE,
) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"flink-stream-inline-sql-{version}"},
            "tasks": [
                {
                    "name": "run-flink-stream-inline-local-sql",
                    "type": "FLINK_STREAM",
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=intent,
        ),
    )


def _compiled(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([39_000])["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert definition["taskType"] == "FLINK_STREAM"
    assert definition["taskExecuteType"] == "STREAM"
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type="FLINK_STREAM",
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decode(
    version: str,
    params: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="FLINK_STREAM",
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _fake_dag(
    version: str,
    params: YamlObject,
    *,
    workflow_name: str,
    task_name: str = "run-flink-stream-inline-local-sql",
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name=task_name,
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="FLINK_STREAM",
        task_params_value=json.dumps(params),
        worker_group_value="default",
        task_execute_type_value=FakeEnumValue("BATCH"),
    )
    if version in {"3.2.0", "3.2.1", "3.2.2"}:
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


def _edit_plan(
    version: str,
    params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dag(version, _native(), workflow_name="flink-stream-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "flink-stream-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "run-flink-stream-inline-local-sql",
                    "type": "FLINK_STREAM",
                    "task_params": params,
                }
            ],
        },
        input_mode=input_mode,
    )


def _metadata_plan(
    version: str,
    native: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dag(version, native, workflow_name="flink-stream-metadata")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="run-flink-stream-inline-local-sql",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    assert definition["taskExecuteType"] == "STREAM"
    params = json.loads(definition["taskParams"])
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _template_yaml(version: str) -> str:
    return authoring_prep.template_yaml("FLINK_STREAM", version, variant="minimal")


def _guidance(version: str) -> str:
    return authoring_prep.schema_guidance("FLINK_STREAM", version)


def _raw_script_schema(version: str) -> dict[object, object]:
    result = task_type_schema_result(
        "FLINK_STREAM",
        json_schema=True,
        catalog=get_task_authoring_catalog(version),
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
    raw_script = properties["rawScript"]
    assert isinstance(raw_script, dict)
    reference = raw_script.get("$ref")
    if isinstance(reference, str):
        nested = task_params["$defs"]
        assert isinstance(nested, dict)
        raw_script = nested[reference.rsplit("/", maxsplit=1)[-1]]
        assert isinstance(raw_script, dict)
    return raw_script


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_flink_stream_catalog_exposes_one_exact_inline_local_sql_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("FLINK_STREAM")
    membership = catalog.require_facet("FLINK_STREAM", _FACET)
    fact = catalog.task_type_facts["FLINK_STREAM"]
    review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("FLINK_STREAM") is True
    assert catalog.supports_opaque_authoring("FLINK_STREAM") is True
    assert profile.category == "Universal"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.plugin.task.flink.FlinkStreamParameters"
    )
    assert fact.semantic_fingerprint == _FINGERPRINTS[version]
    assert review is not None
    assert review.review == "flink-stream-inline-local-sql-literal-subset"
    assert review.cli_model == "FlinkInlineLocalSqlTaskParamsSpec"
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == review.review
    assert membership.contract.family == "flink-stream-inline-local-sql-v1"
    assert membership.contract.params_model is not None
    assert (
        membership.contract.params_model.__name__ == "FlinkInlineLocalSqlTaskParamsSpec"
    )
    assert membership.contract.opaque_authoring_selector is not None
    assert membership.profile_version == version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_flink_stream_is_upstream_absent_before_3_1(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "FLINK_STREAM" not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring("FLINK_STREAM") is False
    assert catalog.supports_opaque_authoring("FLINK_STREAM") is False
    assert "FLINK_STREAM" not in catalog.authoring_task_types


def test_flink_stream_3_1_0_is_an_exact_broken_executor_hole() -> None:
    version = "3.1.0"
    catalog = get_task_authoring_catalog(version)
    fact = catalog.task_type_facts["FLINK_STREAM"]
    surface = cast(
        "_Surface", get_task_authoring_surface(version)
    ).flink_stream_inline_sql

    assert "FLINK_STREAM" in catalog.upstream_task_types
    assert fact.semantic_fingerprint == (
        "sha256:15ba339d55fa5220c21193bde98d94bd5936884231a8c83cf8e9177cee66672e"
    )
    assert fact.typed_authoring_review is None
    assert catalog.supports_typed_authoring("FLINK_STREAM") is False
    assert catalog.supports_opaque_authoring("FLINK_STREAM") is True
    with pytest.raises(UnsupportedFeatureError):
        catalog.require_facet("FLINK_STREAM", _FACET)
    assert surface.available is False
    assert surface.wire_epoch == "broken-unconditional-main-jar-no-review"
    assert surface.script_encoding == "utf-8"
    assert surface.sql_command == "PATH"
    assert surface.parameter_substitution is False
    assert surface.stop_sequence == "plugin-cancel-then-pid-tree"
    assert surface.exclusion_reason == "broken-unconditional-main-jar-no-review"


@pytest.mark.parametrize("version", _UNSUPPORTED_EXECUTOR_VERSIONS)
def test_flink_stream_new_executor_service_is_an_exact_typed_hole(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    fact = catalog.task_type_facts["FLINK_STREAM"]
    surface = cast(
        "_Surface", get_task_authoring_surface(version)
    ).flink_stream_inline_sql
    native = _opaque_native()

    assert "FLINK_STREAM" in catalog.upstream_task_types
    assert fact.typed_authoring_review is None
    assert catalog.supports_typed_authoring("FLINK_STREAM") is False
    assert catalog.supports_opaque_authoring("FLINK_STREAM") is True
    with pytest.raises(UnsupportedFeatureError):
        catalog.require_facet("FLINK_STREAM", _FACET)
    assert surface.available is False
    assert surface.stop_sequence == "plugin-cancel-only"
    assert surface.exclusion_reason == "stream-executor-service-not-supported"
    assert _compiled(version, native) == native
    decoded, source = _decode(version, _native())
    assert decoded == _native()
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError) as captured:
        _encode(version, _canonical())
    assert captured.value.details["reason"] == "stream-executor-service-not-supported"


def test_flink_stream_3_1_0_retains_only_generic_opaque_authoring() -> None:
    version = "3.1.0"
    catalog = get_task_authoring_catalog(version)
    native = _opaque_native()
    template = task_template_result("FLINK_STREAM", catalog=catalog)

    assert template.resolved["template_kind"] == "generic"
    assert (
        catalog.effective_authoring_intent(
            "FLINK_STREAM",
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert _compiled(version, native) == native
    for input_mode in ("patch", "file"):
        assert (
            _compiled_from_plan(_edit_plan(version, native, input_mode=input_mode))
            == native
        )


def test_flink_stream_exact_model_does_not_leak_into_stable_generic_loading() -> None:
    document: YamlObject = {
        "workflow": {"name": "generic-flink-stream"},
        "tasks": [
            {
                "name": "generic-stream-task",
                "type": "FLINK_STREAM",
                "task_params": {},
            }
        ],
    }

    generic = validate_workflow_document(document)
    stable = _spec("3.4.1", {})

    assert generic.tasks[0].task_params == {}
    assert stable.tasks[0].task_params == {}
    with pytest.raises(ValueError, match="rawScript"):
        _spec("3.2.2", {})


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_flink_stream_membership_is_exact_version_selected(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert ("FLINK_STREAM" in catalog.reviewed_typed_task_types) is (
        version in _TYPED_VERSIONS
    )


@pytest.mark.parametrize("version", ["3.1.9", "3.2.1", "3.2.2"])
def test_flink_stream_schema_template_and_mapping_publish_only_raw_script(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data("FLINK_STREAM", catalog=catalog)
    schema = task_type_schema_result(
        "FLINK_STREAM",
        catalog=catalog,
    )
    mapping_result = task_type_schema_result(
        "FLINK_STREAM",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(schema.data, dict)
    assert isinstance(mapping_result.data, dict)
    fields = {
        row["path"]
        for row in schema.data["fields"]
        if isinstance(row, dict)
        and isinstance(row.get("path"), str)
        and row["path"].startswith("task_params.")
    }
    mappings = {
        row["authoring_path"]: row["ds_payload_path"]
        for row in mapping_result.data["compile_mappings"]
        if isinstance(row, dict)
        and isinstance(row.get("authoring_path"), str)
        and row["authoring_path"].startswith("task_params.")
    }
    task = yaml.safe_load(_template_yaml(version))

    assert summary["task_type"] == "FLINK_STREAM"
    assert summary["category"] == "Universal"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert fields == {"task_params.rawScript"}
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.rawScript",
    }
    assert mappings == {
        "task_params.rawScript": "taskDefinitionJson[].taskParams.rawScript"
    }
    assert task["type"] == "FLINK_STREAM"
    assert task["task_params"] == {"rawScript": "SELECT 1 AS answer"}
    assert _compiled(version, task["task_params"]) == _native("SELECT 1 AS answer")


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_flink_stream_projector_emits_and_decodes_exactly_four_wire_fields(
    version: str,
) -> None:
    raw_script = "\tSELECT 'literal; \\\\ $() 中文' AS `value`\nFROM orders;"

    assert _encode(version, _canonical(raw_script)) == _native(raw_script)
    decoded, source = _decode(version, _native(raw_script))
    assert decoded == _canonical(raw_script)
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    ("raw_script", "accepted"),
    [
        ("SELECT 1", True),
        ("\tSELECT '中文; \\\\ $()' AS `value`\nFROM orders;", True),
        ("", False),
        (" \t\n", False),
        ("\u00a0\u2003\ufeff", False),
        ("SELECT ${x}", False),
        ("SELECT $[x]", False),
        ("SELECT\r1", False),
        ("SELECT \x00", False),
        ("SELECT \x1f", False),
        ("SELECT \x7f", False),
        ("SELECT \x80", False),
        ("SELECT \x9f", False),
        (f"SELECT '{chr(0xD800)}'", False),
        (f"SELECT '{chr(0xDFFF)}'", False),
    ],
)
def test_flink_stream_schema_runtime_and_projector_share_character_rules(
    version: str,
    raw_script: str,
    *,
    accepted: bool,
) -> None:
    catalog = get_task_authoring_catalog(version)
    pattern = _raw_script_schema(version)["pattern"]
    assert isinstance(pattern, str)
    schema_accepts = re.fullmatch(pattern, raw_script) is not None

    try:
        normalized = catalog.normalize_task_params(
            "FLINK_STREAM",
            _canonical(raw_script),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    except (UnsupportedFeatureError, ValueError):
        runtime_accepts = False
    else:
        runtime_accepts = True
        assert normalized == _canonical(raw_script)

    assert schema_accepts is accepted
    assert runtime_accepts is accepted
    if accepted:
        assert _encode(version, _canonical(raw_script)) == _native(raw_script)
    else:
        with pytest.raises(TaskParameterProjectionError, match="rawScript"):
            _encode(version, _canonical(raw_script))


@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_flink_stream_raw_script_rejects_nonstrings(value: YamlValue) -> None:
    with pytest.raises(ValueError, match="rawScript"):
        get_task_authoring_catalog("3.2.2").normalize_task_params(
            "FLINK_STREAM",
            {"rawScript": value},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "native",
    [
        {**_native(), "futureField": True},
        {**_native(), "initScript": "SET execution.runtime-mode = streaming;"},
        {**_native(), "localParams": []},
        {"programType": "SQL", "deployMode": "local", "rawScript": "SELECT 1"},
    ],
)
def test_flink_stream_near_local_sql_wire_is_preserve_only(native: YamlObject) -> None:
    decoded, source = _decode("3.2.2", native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError):
        _decode("3.2.2", native, source=ProjectionSource.TYPED_AUTHORING)


@pytest.mark.parametrize(
    "params",
    [
        {"rawScript": "SELECT ${x}"},
        {"rawScript": "SELECT $[x]"},
        {"rawScript": "SELECT 1", "programType": "SQL"},
        _native("SELECT ${x}"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_flink_stream_invalid_local_sql_never_falls_open_to_opaque(
    params: YamlObject,
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.2.2")

    assert (
        catalog.effective_authoring_intent(
            "FLINK_STREAM",
            requested=intent,
            task_params=params,
        )
        is intent
    )
    with pytest.raises(ValueError):
        catalog.normalize_task_params("FLINK_STREAM", params, intent=intent)
    if intent is TaskAuthoringIntent.TYPED_CREATE:
        with pytest.raises(ValueError):
            _compiled("3.2.2", params)
        return
    for input_mode in ("patch", "file"):
        with pytest.raises(UserInputError):
            _edit_plan("3.2.2", params, input_mode=input_mode)


@pytest.mark.parametrize(
    ("version", "native"),
    [
        ("3.1.9", {"programType": "JAVA", "mainClass": "example.Job"}),
        ("3.1.9", {"programType": "SCALA", "mainClass": "example.Job"}),
        ("3.2.2", {"programType": "PYTHON", "mainJar": {"id": 12}}),
        (
            "3.1.9",
            {
                "programType": "SQL",
                "deployMode": "cluster",
                "rawScript": "SELECT 1",
            },
        ),
        (
            "3.1.9",
            {
                "programType": "SQL",
                "deployMode": "application",
                "rawScript": "SELECT 1",
            },
        ),
        (
            "3.2.0",
            {
                "programType": "SQL",
                "deployMode": "standalone",
                "rawScript": "SELECT 1",
            },
        ),
    ],
)
def test_flink_stream_reviewed_native_modes_are_explicit_opaque_create_and_edit(
    version: str,
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.effective_authoring_intent(
            "FLINK_STREAM",
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert (
        catalog.effective_authoring_intent(
            "FLINK_STREAM",
            requested=TaskAuthoringIntent.TYPED_EDIT,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_EDIT
    )
    assert _compiled(version, native) == native
    for input_mode in ("patch", "file"):
        assert (
            _compiled_from_plan(_edit_plan(version, native, input_mode=input_mode))
            == native
        )


@pytest.mark.parametrize(
    ("version", "native"),
    [
        (
            "3.1.9",
            {
                "programType": "SQL",
                "deployMode": "standalone",
                "rawScript": "SELECT 1",
            },
        ),
        ("3.2.2", {"programType": "FUTURE", "futureField": True}),
    ],
)
def test_flink_stream_opaque_selector_is_exact_and_never_falls_open(
    version: str,
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(version)
    for intent in (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT):
        assert (
            catalog.effective_authoring_intent(
                "FLINK_STREAM",
                requested=intent,
                task_params=native,
            )
            is intent
        )
    with pytest.raises(ValueError):
        _compiled(version, native)
    for input_mode in ("patch", "file"):
        with pytest.raises(UserInputError):
            _edit_plan(version, native, input_mode=input_mode)


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (
            _native("SELECT 7 AS safe_provenance"),
            _canonical("SELECT 7 AS safe_provenance"),
            ProjectionSource.TYPED_AUTHORING,
        ),
        (
            {**_native("SELECT 8 AS future_state"), "futureField": {"keep": True}},
            {**_native("SELECT 8 AS future_state"), "futureField": {"keep": True}},
            ProjectionSource.OPAQUE_PRESERVE,
        ),
        (
            _opaque_native(),
            _opaque_native(),
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_flink_stream_export_distinguishes_safe_and_opaque_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.2.2"
    dag = _fake_dag(version, native, workflow_name="flink-stream-export")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    baseline = workflow_live_baseline(dag, project=project, catalog=catalog)
    document = yaml.safe_load(
        workflow_yaml_document(
            dag,
            project=project,
            attached_schedule=None,
            catalog=catalog,
        )
    )

    assert baseline.projection_sources["run-flink-stream-inline-local-sql"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("native", "merged"),
    [
        (
            _native("SELECT 9 AS safe_metadata"),
            _canonical("SELECT 9 AS safe_metadata"),
        ),
        (
            {
                **_native("SELECT 10 AS opaque_metadata"),
                "futureField": {"preserve": True},
            },
            {
                **_native("SELECT 10 AS opaque_metadata"),
                "futureField": {"preserve": True},
            },
        ),
    ],
)
def test_flink_stream_metadata_edit_preserves_projection_provenance(
    native: YamlObject,
    merged: YamlObject,
    input_mode: str,
) -> None:
    plan = _metadata_plan("3.2.2", native, input_mode=input_mode)

    assert plan.merged_spec.tasks[0].task_params == merged
    assert _compiled_from_plan(plan) == native


@pytest.mark.parametrize("native", [_native(), _opaque_native()])
def test_flink_stream_unchanged_update_preserves_wire_identity(
    native: YamlObject,
) -> None:
    version = "3.2.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    baseline = workflow_live_baseline(
        _fake_dag(version, native, workflow_name="flink-stream-unchanged"),
        project=project,
        catalog=catalog,
    )
    payload = prepare_preserved_workflow_update_compilation(
        baseline.spec,
        release_state="OFFLINE",
        active_task_identities=baseline.task_identities,
        unavailable_task_identities=(),
        preserved_projection_sources=baseline.projection_sources,
        catalog=catalog,
    ).materialize([])
    definition = json.loads(payload["taskDefinitionJson"])[0]
    compiled = json.loads(definition["taskParams"])

    assert definition["taskExecuteType"] == "STREAM"
    assert compiled == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_flink_stream_opaque_preserve_is_a_deep_identity_copy(version: str) -> None:
    native = _opaque_native()
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(version).normalize_task_params(
        "FLINK_STREAM",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )

    assert preserved == expected
    assert preserved is not native
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    assert preserved == expected


@pytest.mark.parametrize(
    ("version", "wire_epoch", "command", "substitution", "stop_sequence"),
    [
        (
            "3.1.9",
            "utf8-path-command",
            "PATH",
            False,
            "plugin-cancel-then-pid-tree",
        ),
        (
            "3.2.0",
            "utf8-path-command",
            "PATH",
            False,
            "pid-tree-then-plugin-cancel",
        ),
        (
            "3.2.1",
            "utf8-path-command",
            "PATH",
            False,
            "pid-tree-then-plugin-cancel",
        ),
        (
            "3.2.2",
            "utf8-path-command",
            "PATH",
            False,
            "pid-tree-then-plugin-cancel",
        ),
    ],
)
def test_flink_stream_runtime_surface_locks_every_executor_and_stop_epoch(
    version: str,
    wire_epoch: str,
    command: str,
    *,
    substitution: bool,
    stop_sequence: str,
) -> None:
    surface = cast(
        "_Surface", get_task_authoring_surface(version)
    ).flink_stream_inline_sql

    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.script_encoding == "utf-8"
    assert surface.sql_command == command
    assert surface.parameter_substitution is substitution
    assert surface.task_params_logged is True
    assert surface.script_logged is True
    assert surface.command_logged is True
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True
    assert surface.local_sql_application_id_expected is False
    assert surface.cancel_requires_application_id is True
    assert surface.savepoint_requires_application_id is True
    assert surface.stop_sequence == stop_sequence
    assert surface.reliable_stop_supported is False
    assert surface.exclusion_reason is None


@pytest.mark.parametrize(
    ("version", "stop_sequence_guidance"),
    [
        (
            "3.1.9",
            "plugin cancel runs before the worker pid tree is killed",
        ),
        (
            "3.2.2",
            "the worker pid tree is killed before plugin cancel runs",
        ),
    ],
)
def test_flink_stream_guidance_discloses_runtime_and_unreliable_stop(
    version: str,
    stop_sequence_guidance: str,
) -> None:
    guidance = _guidance(version)
    template = _template_yaml(version).lower()

    for term in (
        "flink sql client",
        "java",
        "connector",
        "catalog",
        "permissions",
        "logs task parameters",
        "raw sql",
        "at info",
        "result output",
        "durable application id",
        "failover",
        "retry",
        "side effects",
        "secret storage",
        "live evidence",
        "cancel",
        "savepoint",
        "application id",
        "unbounded",
        "may continue",
    ):
        assert term in guidance
        assert term in template
    assert "utf-8" in guidance
    assert "utf-8" in template
    assert "flink_home/bin/sql-client.sh" not in guidance
    assert "prepared-map parameter substitution" not in guidance
    assert stop_sequence_guidance in guidance
    assert stop_sequence_guidance in template
