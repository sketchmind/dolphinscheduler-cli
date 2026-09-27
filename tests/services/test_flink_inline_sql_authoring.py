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
from dsctl.models import task_params_model_for_type
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
    from dsctl.support.json_types import JsonObject


_FACET = "FLINK/inline_local_sql"
_TYPED = (
    "3.0.0",
    "3.0.6",
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
_ASCII = {"3.0.0", "3.0.6"}
_NO_TYPED = ("1.3.9", "2.0.0", "2.0.9", "3.1.0")
_REFS = TaskRefIndex.from_code_by_name({})


class _FlinkSurface(Protocol):
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
    exclusion_reason: str | None


class _Surface(Protocol):
    flink_inline_sql: _FlinkSurface


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
        "mainClass": "com.example.FlinkJob",
        "mainJar": {"id": 91, "res": "flink-job.jar"},
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _flink_spec(
    version: str,
    params: YamlObject,
    *,
    intent: TaskAuthoringIntent = TaskAuthoringIntent.TYPED_CREATE,
) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"flink-inline-sql-{version}"},
            "tasks": [
                {
                    "name": "run-inline-local-sql",
                    "type": "FLINK",
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=intent,
        ),
    )


def _compiled_task_params(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _flink_spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([35_000])["taskDefinitionJson"])[0]
    native_params = json.loads(definition["taskParams"])
    assert definition["taskType"] == "FLINK"
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _fake_dag(
    version: str,
    params: YamlObject,
    *,
    workflow_name: str,
    task_name: str = "run-inline-local-sql",
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name=task_name,
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="FLINK",
        task_params_value=json.dumps(params),
        worker_group_value="default",
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
    dag = _fake_dag(version, _native(), workflow_name="flink-explicit-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "flink-explicit-edit", "project": "analytics"},
            "tasks": [
                {"name": "run-inline-local-sql", "type": "FLINK", "task_params": params}
            ],
        },
        input_mode=input_mode,
    )


def _metadata_edit_plan(
    version: str,
    native_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dag(version, native_params, workflow_name="flink-metadata-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="run-inline-local-sql",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    params = json.loads(definition["taskParams"])
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _template_yaml(version: str) -> str:
    return authoring_prep.template_yaml("FLINK", version, variant="minimal")


def _schema_guidance(version: str) -> str:
    return authoring_prep.schema_guidance("FLINK", version)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type="FLINK",
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
        task_type="FLINK",
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _raw_script_schema(version: str) -> dict[object, object]:
    result = task_type_schema_result(
        "FLINK", json_schema=True, catalog=get_task_authoring_catalog(version)
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


@pytest.mark.parametrize("version", _TYPED)
def test_flink_exact_profiles_expose_one_reviewed_inline_local_sql_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("FLINK")
    membership = catalog.require_facet("FLINK", _FACET)

    assert profile.category == "Universal"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert membership.profile_version == version
    assert membership.contract.review == "flink-inline-local-sql-literal-subset"
    assert membership.typed_create is membership.typed_edit is True


@pytest.mark.parametrize("version", _NO_TYPED)
def test_flink_early_and_broken_profiles_have_no_typed_facet(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert catalog.supports_typed_authoring("FLINK") is False
    assert catalog.supports_opaque_authoring("FLINK") is True
    with pytest.raises(UnsupportedFeatureError):
        catalog.require_facet("FLINK", _FACET)


def test_flink_3_1_0_hole_records_the_unconditional_main_jar_defect() -> None:
    surface = cast("_Surface", get_task_authoring_surface("3.1.0")).flink_inline_sql

    assert surface.available is False
    assert surface.wire_epoch == "broken-unconditional-main-jar-no-review"
    assert surface.exclusion_reason == "broken-unconditional-main-jar-no-review"
    assert surface.script_encoding == "utf-8"
    assert surface.sql_command == "PATH"
    assert surface.parameter_substitution is False


def test_flink_3_1_0_keeps_generic_opaque_create_and_edit_available() -> None:
    version = "3.1.0"
    catalog = get_task_authoring_catalog(version)
    native = _opaque_native()
    template = task_template_result("FLINK", catalog=catalog)

    assert catalog.supports_typed_authoring("FLINK") is False
    assert catalog.supports_opaque_authoring("FLINK") is True
    assert template.resolved["template_kind"] == "generic"
    assert (
        catalog.effective_authoring_intent(
            "FLINK",
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert _compiled_task_params(version, native) == native
    for input_mode in ("patch", "file"):
        assert (
            _compiled_from_plan(_edit_plan(version, native, input_mode=input_mode))
            == native
        )


@pytest.mark.parametrize("version", _TYPED)
def test_flink_exact_contract_uses_ascii_subclass_only_on_3_0(version: str) -> None:
    base = task_params_model_for_type("FLINK")
    exact = (
        get_task_authoring_catalog(version)
        .require_facet("FLINK", _FACET)
        .contract.params_model
    )

    assert base is not None
    assert base.__name__ == "FlinkInlineLocalSqlTaskParamsSpec"
    assert exact is not None
    if version in _ASCII:
        assert exact.__name__ == "FlinkInlineLocalSqlAsciiTaskParamsSpec"
        assert issubclass(exact, base)
    else:
        assert exact is base


@pytest.mark.parametrize("version", ["3.0.0", "3.1.9", "3.4.2"])
def test_flink_json_schema_is_closed_to_one_required_raw_script(version: str) -> None:
    raw_script = _raw_script_schema(version)

    assert raw_script["type"] == "string"
    assert raw_script["minLength"] == 1
    assert isinstance(raw_script["pattern"], str)


@pytest.mark.parametrize("version", ["3.0.0", "3.1.9", "3.4.2"])
def test_flink_template_summary_and_mapping_publish_the_single_field_surface(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data("FLINK", catalog=catalog)
    schema = task_type_schema_result("FLINK", compile_mappings=True, catalog=catalog)
    assert isinstance(schema.data, dict)
    mappings = {
        row["authoring_path"]: row["ds_payload_path"]
        for row in schema.data["compile_mappings"]
        if isinstance(row, dict)
        and isinstance(row.get("authoring_path"), str)
        and row["authoring_path"].startswith("task_params.")
    }
    yaml_text = _template_yaml(version)
    task = yaml.safe_load(yaml_text)

    assert summary["task_type"] == "FLINK"
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
    assert task["type"] == "FLINK"
    assert task["task_params"] == {"rawScript": "SELECT 1 AS answer"}
    assert _compiled_task_params(version, task["task_params"]) == _native(
        "SELECT 1 AS answer"
    )


@pytest.mark.parametrize(
    ("version", "wire_epoch", "encoding", "command", "substitution"),
    [
        ("3.0.0", "legacy-default-charset", "platform-default", "PATH", False),
        ("3.0.6", "legacy-default-charset", "platform-default", "PATH", False),
        ("3.1.9", "utf8-path-command", "utf-8", "PATH", False),
        ("3.2.0", "utf8-path-command", "utf-8", "PATH", False),
        ("3.2.1", "utf8-path-command", "utf-8", "PATH", False),
        ("3.2.2", "utf8-path-command", "utf-8", "PATH", False),
        ("3.3.1", "utf8-flink-home-command", "utf-8", "FLINK_HOME", False),
        ("3.3.2", "utf8-flink-home-command", "utf-8", "FLINK_HOME", False),
        ("3.4.0", "utf8-flink-home-command", "utf-8", "FLINK_HOME", False),
        ("3.4.1", "utf8-flink-home-command", "utf-8", "FLINK_HOME", False),
        ("3.4.2", "utf8-parameterized", "utf-8", "FLINK_HOME", True),
    ],
)
def test_flink_runtime_surface_locks_every_executor_epoch(
    version: str,
    wire_epoch: str,
    encoding: str,
    command: str,
    *,
    substitution: bool,
) -> None:
    surface = cast("_Surface", get_task_authoring_surface(version)).flink_inline_sql

    assert surface.available is True
    assert surface.wire_epoch == wire_epoch
    assert surface.script_encoding == encoding
    assert surface.sql_command == command
    assert surface.parameter_substitution is substitution
    assert surface.task_params_logged is True
    assert surface.script_logged is True
    assert surface.command_logged is True
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True
    assert surface.exclusion_reason is None


@pytest.mark.parametrize("version", ["3.0.0", "3.1.9", "3.3.1", "3.4.2"])
def test_flink_runtime_guidance_discloses_prerequisites_and_replay_risk(
    version: str,
) -> None:
    guidance = _schema_guidance(version)
    template = _template_yaml(version).lower()

    for term in (
        "flink sql client",
        "java",
        "connector",
        "catalog",
        "permissions",
        "result output",
        "durable application id",
        "failover",
        "retry",
        "side effects",
        "live evidence",
        "secret storage",
    ):
        assert term in guidance
        assert term in template
    assert "logs task parameters" in guidance
    assert "at info" in guidance
    assert "logs task parameters" in template
    assert "at info" in template
    assert ("platform-default" in guidance) is (version == "3.0.0")
    assert ("utf-8" in guidance) is (version != "3.0.0")
    assert ("flink_home/bin/sql-client.sh" in guidance) is (version >= "3.3.1")
    assert ("prepared-map parameter substitution" in guidance) is (version == "3.4.2")
    assert "java, scala, python" in template
    assert "non-local sql" in template


@pytest.mark.parametrize("version", _TYPED)
def test_flink_projector_emits_and_decodes_the_exact_four_field_wire(
    version: str,
) -> None:
    raw_script = "\tSELECT 'literal; \\\\ $()' AS `value`\nFROM orders;"

    assert _encode(version, _canonical(raw_script)) == _native(raw_script)
    decoded, source = _decode(version, _native(raw_script))
    assert decoded == _canonical(raw_script)
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    ("version", "raw_script", "accepted"),
    [
        ("3.0.0", "SELECT 1", True),
        ("3.0.0", "SELECT '中文'", False),
        ("3.1.9", "SELECT '中文'", True),
    ],
)
def test_flink_character_epoch_is_ascii_then_utf8(
    version: str, raw_script: str, *, accepted: bool
) -> None:
    catalog = get_task_authoring_catalog(version)
    if not accepted:
        with pytest.raises(ValueError, match="rawScript"):
            catalog.normalize_task_params(
                "FLINK", _canonical(raw_script), intent=TaskAuthoringIntent.TYPED_CREATE
            )
        return
    assert catalog.normalize_task_params(
        "FLINK", _canonical(raw_script), intent=TaskAuthoringIntent.TYPED_CREATE
    ) == _canonical(raw_script)


@pytest.mark.parametrize("version", ["3.0.0", "3.1.9", "3.4.2"])
@pytest.mark.parametrize(
    "raw_script",
    [
        "",
        " \t\n",
        "\u00a0\u2003\ufeff",
        "SELECT ${x}",
        "SELECT $[x]",
        "SELECT\r1",
        "SELECT \x00",
        "SELECT \x1f",
        "SELECT \x7f",
        "SELECT \x80",
        "SELECT \x9f",
    ],
)
def test_flink_rejects_blank_placeholders_cr_and_unsafe_controls(
    version: str, raw_script: str
) -> None:
    pattern = _raw_script_schema(version)["pattern"]
    assert isinstance(pattern, str)
    assert re.fullmatch(pattern, raw_script) is None
    for intent in (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT):
        with pytest.raises(ValueError, match="rawScript"):
            get_task_authoring_catalog(version).normalize_task_params(
                "FLINK", _canonical(raw_script), intent=intent
            )


@pytest.mark.parametrize("version", ["3.1.9", "3.4.2"])
@pytest.mark.parametrize("codepoint", [0xD800, 0xDFFF])
def test_flink_utf8_schema_runtime_and_projector_reject_lone_surrogates(
    version: str,
    codepoint: int,
) -> None:
    raw_script = f"SELECT '{chr(codepoint)}'"
    pattern = _raw_script_schema(version)["pattern"]
    assert isinstance(pattern, str)
    assert re.fullmatch(pattern, raw_script) is None

    with pytest.raises(ValueError, match="rawScript"):
        get_task_authoring_catalog(version).normalize_task_params(
            "FLINK",
            _canonical(raw_script),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    with pytest.raises(TaskParameterProjectionError, match="rawScript"):
        _encode(version, _canonical(raw_script))


@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_flink_raw_script_rejects_nonstrings(value: YamlValue) -> None:
    with pytest.raises(ValueError, match="rawScript"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "FLINK", {"rawScript": value}, intent=TaskAuthoringIntent.TYPED_CREATE
        )


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
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_flink_invalid_local_sql_never_downgrades_to_opaque(
    params: YamlObject, intent: TaskAuthoringIntent
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    assert (
        catalog.effective_authoring_intent(
            "FLINK", requested=intent, task_params=params
        )
        is intent
    )
    with pytest.raises(ValueError):
        catalog.normalize_task_params("FLINK", params, intent=intent)
    if intent is TaskAuthoringIntent.TYPED_CREATE:
        with pytest.raises(ValueError):
            _compiled_task_params("3.4.2", params)
        return
    for input_mode in ("patch", "file"):
        with pytest.raises(UserInputError):
            _edit_plan("3.4.2", params, input_mode=input_mode)


def test_flink_projector_rejects_unowned_fields_in_both_directions() -> None:
    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encode("3.4.2", {**_canonical(), "futureField": True})
    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _decode(
            "3.4.2",
            {**_native(), "futureField": True},
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize(
    ("version", "native"),
    [
        ("3.0.0", {"programType": "JAVA", "mainClass": "example.Job"}),
        ("3.1.9", {"programType": "SCALA", "mainClass": "example.Job"}),
        ("3.4.2", {"programType": "PYTHON", "mainJar": {"id": 12}}),
        (
            "3.0.0",
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
def test_flink_explicit_native_modes_are_identity_projected_by_create_and_edit(
    version: str,
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.effective_authoring_intent(
            "FLINK",
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert (
        catalog.effective_authoring_intent(
            "FLINK",
            requested=TaskAuthoringIntent.TYPED_EDIT,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_EDIT
    )
    assert _compiled_task_params(version, native) == native
    for input_mode in ("patch", "file"):
        assert (
            _compiled_from_plan(_edit_plan(version, native, input_mode=input_mode))
            == native
        )


@pytest.mark.parametrize(
    ("version", "native"),
    [
        (
            "3.0.0",
            {
                "programType": "SQL",
                "deployMode": "application",
                "rawScript": "SELECT 1",
            },
        ),
        (
            "3.1.9",
            {
                "programType": "SQL",
                "deployMode": "standalone",
                "rawScript": "SELECT 1",
            },
        ),
    ],
)
def test_flink_nonlocal_sql_selector_is_exact_and_never_falls_open(
    version: str,
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog(version)
    for intent in (TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT):
        assert (
            catalog.effective_authoring_intent(
                "FLINK", requested=intent, task_params=native
            )
            is intent
        )
    with pytest.raises(ValueError):
        _compiled_task_params(version, native)
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
def test_flink_baseline_and_export_distinguish_safe_from_opaque_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.4.2"
    dag = _fake_dag(version, native, workflow_name="flink-provenance-export")
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

    assert baseline.projection_sources["run-inline-local-sql"] is source
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
def test_flink_metadata_patch_and_file_preserve_projection_provenance(
    native: YamlObject,
    merged: YamlObject,
    input_mode: str,
) -> None:
    plan = _metadata_edit_plan("3.4.2", native, input_mode=input_mode)

    assert plan.merged_spec.tasks[0].task_params == merged
    assert _compiled_from_plan(plan) == native


@pytest.mark.parametrize(
    "native",
    [
        _native("SELECT 11 AS safe_rename"),
        {
            **_native("SELECT 12 AS opaque_rename"),
            "futureField": {"preserve": True},
        },
    ],
)
def test_flink_rename_preserves_safe_and_opaque_wire_identity(
    native: YamlObject,
) -> None:
    version = "3.4.2"
    dag = _fake_dag(version, native, workflow_name="flink-rename")
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
        catalog=get_task_authoring_catalog(version),
    )
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]

    assert definition["name"] == "renamed-inline-local-sql"
    assert json.loads(definition["taskParams"]) == native


def test_flink_delete_drops_deleted_provenance_and_keeps_remaining_wire() -> None:
    version = "3.4.2"
    kept_native = {
        **_native("SELECT 13 AS opaque_kept"),
        "futureField": {"preserve": True},
    }
    tasks = [
        FakeTaskDefinition(
            code=101,
            name="delete-inline-local-sql",
            project_code_value=7,
            project_name_value="analytics",
            task_type_value="FLINK",
            task_params_value=json.dumps(_native("SELECT 14 AS deleted")),
            worker_group_value="default",
        ),
        FakeTaskDefinition(
            code=102,
            name="keep-inline-local-sql",
            project_code_value=7,
            project_name_value="analytics",
            task_type_value="FLINK",
            task_params_value=json.dumps(kept_native),
            worker_group_value="default",
        ),
    ]
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="flink-delete",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=tasks,
        workflow_task_relation_list_value=[],
    )
    patch = WorkflowPatchDocument.model_validate(
        {"patch": {"tasks": {"delete": ["delete-inline-local-sql"]}}}
    ).patch
    plan = prepare_workflow_mutation_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        patch=patch,
        release_state="OFFLINE",
        catalog=get_task_authoring_catalog(version),
    )
    definitions = json.loads(plan.compilation.preview()["taskDefinitionJson"])

    assert [definition["name"] for definition in definitions] == [
        "keep-inline-local-sql"
    ]
    assert json.loads(definitions[0]["taskParams"]) == kept_native


@pytest.mark.parametrize(
    "native",
    [
        _native("SELECT 15 AS safe_unchanged"),
        {
            **_native("SELECT 16 AS opaque_unchanged"),
            "futureField": {"preserve": True},
        },
    ],
)
def test_flink_unchanged_update_preserves_safe_and_opaque_wire(
    native: YamlObject,
) -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    baseline = workflow_live_baseline(
        _fake_dag(version, native, workflow_name="flink-unchanged"),
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
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native
