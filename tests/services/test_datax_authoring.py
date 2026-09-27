from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

from dsctl.errors import UnsupportedFeatureError, UserInputError
from dsctl.models.common import ModelValidationError
from dsctl.models.task_spec import literal_json
from dsctl.models.workflow_patch import WorkflowPatchDocument
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._legacy_workflow_mutation import (
    prepare_legacy_workflow_mutation_plan,
)
from dsctl.services._workflow.authoring import (
    workflow_authoring_catalog_for_version,
    workflow_authoring_context,
)
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services._workflow.mutation import prepare_workflow_mutation_plan
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.legacy_workflow_graph import (
    DecodedLegacyWorkflowGraph,
    decode_legacy_workflow_graph,
    prepare_legacy_workflow_graph,
)
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


_TASK_TYPE = "DATAX"
_FACET = "DATAX/literal_custom_json_job"
_TYPED_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
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
    "3.4.3",
)
_REFS = TaskRefIndex.from_code_by_name({})
_JOB_JSON = """{
  "job": {
    "content": [],
    "setting": {"speed": {"channel": 1}}
  }
}"""
_UPDATED_JOB_JSON = '{"job":{"content":[],"setting":{"speed":{"channel":2}}}}'


def _canonical(json_text: str = _JOB_JSON) -> YamlObject:
    return {"json": json_text}


def _native(version: str, json_text: str = _JOB_JSON) -> YamlObject:
    native: YamlObject = {
        "customConfig": 1,
        "json": json_text,
    }
    if version != "1.3.9":
        native.update({"xms": 1, "xmx": 1})
    return native


def _ui_native(version: str, json_text: str = _JOB_JSON) -> YamlObject:
    native = _native(version, json_text)
    native["localParams"] = []
    if version not in {"1.3.9", "2.0.0", "2.0.9"}:
        native["resourceList"] = []
    return native


def _spec(version: str, params: YamlObject) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"datax-{version}"},
            "tasks": [
                {
                    "name": "run-datax-job",
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )


def _compiled(version: str, params: YamlObject) -> YamlObject:
    spec = _spec(version, params)
    if version == "1.3.9":
        payload = prepare_legacy_workflow_graph(
            spec,
            task_id_factory=lambda _task_name: "tasks-datax",
        ).materialize()
        process_definition = json.loads(payload["processDefinitionJson"])
        native = process_definition["tasks"][0]["params"]
    else:
        catalog = get_task_authoring_catalog(version)
        prepared = prepare_workflow_create_compilation(spec, catalog=catalog)
        definition = json.loads(prepared.materialize([41_000])["taskDefinitionJson"])[0]
        assert definition["taskType"] == _TASK_TYPE
        native = json.loads(definition["taskParams"])
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _decode(
    version: str,
    native: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.OPAQUE_PRESERVE,
) -> tuple[YamlObject, ProjectionSource]:
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(native)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _encode(
    version: str,
    params: YamlObject,
    *,
    source: ProjectionSource = ProjectionSource.TYPED_AUTHORING,
) -> YamlObject:
    encoded = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", encoded.task_params)


def _modern_dag(
    native: YamlObject, *, workflow_name: str, global_params: bool = False
) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
            global_params_value=(
                '[{"prop":"unsafe","value":"value"}]' if global_params else None
            ),
            global_param_map_value={"unsafe": "value"} if global_params else None,
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name="run-datax-job",
                project_code_value=7,
                project_name_value="analytics",
                task_type_value=_TASK_TYPE,
                task_params_value=json.dumps(native),
                worker_group_value="default",
            )
        ],
        workflow_task_relation_list_value=[],
    )


def _modern_edit_plan(
    params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    dag = _modern_dag(_native(version), workflow_name="datax-edit")
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "datax-edit", "project": "analytics"},
            "tasks": [
                {"name": "run-datax-job", "type": _TASK_TYPE, "task_params": params}
            ],
        },
        input_mode=input_mode,
    )


def _modern_metadata_plan(
    native: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    dag = _modern_dag(native, workflow_name="datax-metadata")
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="run-datax-job",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _legacy_graph(native: YamlObject) -> DecodedLegacyWorkflowGraph:
    return decode_legacy_workflow_graph(
        process_definition_json=json.dumps(
            {
                "globalParams": [],
                "tasks": [
                    {
                        "id": "tasks-datax",
                        "name": "run-datax-job",
                        "type": _TASK_TYPE,
                        "description": "Original DataX task",
                        "params": native,
                        "preTasks": [],
                    }
                ],
                "timeout": 0,
                "tenantId": -1,
            }
        ),
        locations=json.dumps(
            {
                "tasks-datax": {
                    "name": "run-datax-job",
                    "targetarr": "",
                    "nodenumber": 0,
                    "x": 0,
                    "y": 0,
                }
            }
        ),
        connects="[]",
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datax_catalog_exposes_one_closed_typed_custom_json_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert profile.category == "DataIntegration"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert membership.contract.params_model is not None
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datax_catalog_publishes_the_exact_json_compile_path(version: str) -> None:
    membership = get_task_authoring_catalog(version).require_facet(
        _TASK_TYPE,
        _FACET,
    )

    assert len(membership.contract.fields) == 1
    expected = (
        "processDefinitionJson.tasks[].params.json"
        if version == "1.3.9"
        else "taskDefinitionJson[].taskParams.json"
    )
    assert membership.contract.fields[0].compile_path == expected


def test_datax_schema_and_template_publish_only_literal_json() -> None:
    catalog = get_task_authoring_catalog("3.4.2")
    result = task_type_schema_result(
        _TASK_TYPE,
        json_schema=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["json"]
    assert set(task_params["properties"]) == {"json"}
    assert task_params["properties"]["json"]["type"] == "string"
    assert task_params["properties"]["json"]["minLength"] == 1

    template = task_template_result(
        _TASK_TYPE,
        catalog=catalog,
    )
    assert isinstance(template.data, dict)
    task = yaml.safe_load(template.data["yaml"])
    assert task["type"] == _TASK_TYPE
    assert set(task["task_params"]) == {"json"}
    assert isinstance(json.loads(task["task_params"]["json"]), dict)
    assert _compiled("3.4.2", task["task_params"]) == _native(
        "3.4.2",
        task["task_params"]["json"],
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datax_canonical_json_compiles_exact_version_wire(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    authored = _canonical()

    normalized = catalog.normalize_task_params(
        _TASK_TYPE,
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == authored
    assert normalized is not authored
    assert _encode(version, normalized) == _native(version)
    assert _compiled(version, authored) == _native(version)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datax_typed_native_decode_is_a_projection_fixed_point(version: str) -> None:
    native = _native(version)

    canonical, source = _decode(version, native)

    assert canonical == _canonical()
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _encode(version, canonical, source=source) == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_datax_decode_strips_only_exact_ui_empty_defaults(version: str) -> None:
    native = _ui_native(version)

    canonical, source = _decode(version, native)

    assert canonical == _canonical()
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _encode(version, canonical, source=source) == _native(version)


@pytest.mark.parametrize("version", ["1.3.9", "2.0.0", "2.0.9"])
def test_datax_early_resource_list_even_when_empty_is_opaque(version: str) -> None:
    native = {**_ui_native(version), "resourceList": []}

    decoded, source = _decode(version, native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize(
    "json_text",
    [
        "",
        "   ",
        "not-json",
        "[]",
        '"scalar"',
        "null",
        '{"job":"${secret}"}',
        '{"job":"$[yyyyMMdd]"}',
        json.dumps({"${dynamicKey}": 1}),
        json.dumps({"job": "nul\x00byte"}),
        json.dumps({"job": "line\nbreak"}),
        json.dumps({"job": "delete\x7fbyte"}),
        json.dumps({"job": "control\x85byte"}),
        json.dumps({"job": "surrogate\ud800"}),
    ],
)
def test_datax_json_must_be_one_literal_control_free_json_object(
    json_text: str,
) -> None:
    with pytest.raises(ValueError, match="json"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            {"json": json_text},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("json_text", ["{}", "{ }", "{\n  \n}"])
@pytest.mark.parametrize("version", ["3.4.2", "3.4.3"])
def test_datax_empty_object_follows_exact_native_resource_fallback_boundary(
    json_text: str, version: str
) -> None:
    catalog = get_task_authoring_catalog(version)
    params = _canonical(json_text)
    if version == "3.4.2":
        assert (
            catalog.normalize_task_params(
                _TASK_TYPE, params, intent=TaskAuthoringIntent.TYPED_CREATE
            )
            == params
        )
        assert (
            encode_task_parameters(
                version=version,
                task_type=_TASK_TYPE,
                task_params={"json": json_text},
                refs=_REFS,
                source=ProjectionSource.TYPED_AUTHORING,
            ).task_params["json"]
            == json_text
        )
        return
    with pytest.raises(ValueError, match="nonempty JSON object"):
        catalog.normalize_task_params(
            _TASK_TYPE, params, intent=TaskAuthoringIntent.TYPED_CREATE
        )
    with pytest.raises(TaskParameterProjectionError) as caught:
        encode_task_parameters(
            version=version,
            task_type=_TASK_TYPE,
            task_params={"json": json_text},
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    assert caught.value.details["reason"] == "inline-json-selects-resource-fallback"
    with pytest.raises(ValueError, match="nonempty JSON object"):
        validate_workflow_document(
            {
                "workflow": {"name": "mixed-sql-datax"},
                "tasks": [
                    {
                        "name": "query",
                        "type": "SQL",
                        "task_params": {
                            "datasource": "analytics",
                            "sql": "SELECT 1",
                        },
                    },
                    {"name": "transfer", "type": _TASK_TYPE, "task_params": params},
                ],
            },
            authoring_context=workflow_authoring_context(
                catalog=catalog, intent=TaskAuthoringIntent.TYPED_CREATE
            ),
        )
    native: JsonObject = {"customConfig": 1, "json": json_text, "xms": 1, "xmx": 1}
    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type=_TASK_TYPE,
        task_params=native,
        refs=_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.task.task_params == native
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE


@pytest.mark.parametrize("json_text", ['{"job":{}}', '{"value":null}', _JOB_JSON])
def test_datax_343_nonempty_literal_jobs_preserve_exact_spelling(
    json_text: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.3")
    assert catalog.normalize_task_params(
        _TASK_TYPE, _canonical(json_text), intent=TaskAuthoringIntent.TYPED_CREATE
    ) == {"json": json_text}
    assert (
        encode_task_parameters(
            version="3.4.3",
            task_type=_TASK_TYPE,
            task_params={"json": json_text},
            refs=_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        ).task_params["json"]
        == json_text
    )


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_datax_json_rejects_non_finite_numbers(constant: str) -> None:
    with pytest.raises(ValueError, match="json"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            {"json": f'{{"job":{{"value":{constant}}}}}'},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_datax_json_accepts_escaped_surrogate_pair_without_rewriting() -> None:
    json_text = r'{"job":{"emoji":"\ud83d\ude00"}}'

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        {"json": json_text},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == {"json": json_text}


@pytest.mark.parametrize(
    "number_token",
    [
        "1e9999",
        "1e99999999999999999999",
        "1" + ("0" * 5_000),
    ],
)
def test_datax_json_accepts_numbers_beyond_python_numeric_ranges(
    number_token: str,
) -> None:
    json_text = f'{{"job":{{"value":{number_token}}}}}'

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        {"json": json_text},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == {"json": json_text}


def test_datax_nested_json_preserves_original_spelling() -> None:
    json_text = '{"nested":' * 100 + '"literal"' + "}" * 100

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        _TASK_TYPE,
        {"json": json_text},
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == {"json": json_text}


@pytest.mark.parametrize(
    ("leaf", "message"),
    [
        ("literal", None),
        ("${secret}", "DolphinScheduler placeholders"),
        ("\x00", "controls or surrogates"),
        ({"${secret}": "literal"}, "DolphinScheduler placeholders"),
    ],
    ids=["valid", "placeholder-value", "control-value", "placeholder-key"],
)
def test_datax_deep_decoded_json_string_validation_is_iterative(
    leaf: YamlValue,
    message: str | None,
) -> None:
    # Exercise the decoded-string walk independently of the JSON decoder's
    # interpreter-specific recursion limit, retaining all 2,000 nesting levels.
    decoded = leaf
    for depth in range(2_000):
        decoded = {"nested": decoded} if depth % 2 else [decoded]

    if message is None:
        literal_json._validate_literal_json_decoded_strings(decoded)
    else:
        with pytest.raises(ValueError, match=message):
            literal_json._validate_literal_json_decoded_strings(decoded)


@pytest.mark.parametrize("task_type", ["DATAX", "CHUNJUN"])
def test_literal_json_parser_recursion_failure_is_a_validation_error(
    task_type: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")

    def decoder_depth_limit(*_args: object, **_kwargs: object) -> None:
        message = "decoder recursion limit"
        raise RecursionError(message)

    monkeypatch.setattr(json, "loads", decoder_depth_limit)

    with pytest.raises(
        ModelValidationError,
        match=(
            r"task_params\.json: json must be one syntactically valid "
            "JSON object string"
        ),
    ):
        catalog.normalize_task_params(
            task_type,
            {"json": _JOB_JSON},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "json_text",
    [
        '{"job":{},"job":{"content":[]}}',
        '{"job":{"setting":{},"setting":{"speed":1}}}',
        r'{"job":{},"\u006aob":{"content":[]}}',
    ],
)
def test_datax_json_rejects_duplicate_keys_at_every_depth(json_text: str) -> None:
    with pytest.raises(ValueError, match="json"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            {"json": json_text},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("customConfig", 1),
        ("localParams", []),
        ("resourceList", []),
        ("xms", 1),
        ("xmx", 1),
        ("dataSource", 7),
        ("futureField", {"native": True}),
    ],
)
def test_datax_typed_authoring_rejects_every_unowned_native_field(
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


def test_datax_internal_model_field_name_is_not_an_authoring_alias() -> None:
    with pytest.raises(ValueError, match="json_text"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            {"json_text": _JOB_JSON},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    "params",
    [
        {"json": "[]"},
        {"json": '{"job":"${secret}"}'},
        {"json": _JOB_JSON, "xms": 1},
    ],
)
@pytest.mark.parametrize(
    "requested",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_datax_invalid_typed_input_never_downgrades_to_opaque(
    params: YamlObject,
    requested: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=requested,
            task_params=params,
        )
        is requested
    )
    with pytest.raises(ValueError):
        catalog.normalize_task_params(_TASK_TYPE, params, intent=requested)
    with pytest.raises(UnsupportedFeatureError, match="DATAX"):
        catalog.normalize_task_params(
            _TASK_TYPE,
            params,
            intent=(
                TaskAuthoringIntent.OPAQUE_CREATE
                if requested is TaskAuthoringIntent.TYPED_CREATE
                else TaskAuthoringIntent.OPAQUE_EDIT
            ),
        )


@pytest.mark.parametrize(
    "native",
    [
        {
            **_native("3.4.2"),
            "xms": 2,
            "futureField": cast(
                "YamlValue",
                {"nested": ["preserve", True]},
            ),
        },
        {
            **_native("3.4.2"),
            "localParams": [
                {
                    "prop": "opaque",
                    "direct": "IN",
                    "type": "VARCHAR",
                    "value": "native",
                }
            ],
        },
        {
            **_native("3.4.2"),
            "resourceList": [{"resourceName": "/jobs/datax.json"}],
        },
        {
            "customConfig": 0,
            "json": "",
            "localParams": [],
            "xms": 1,
            "xmx": 1,
            "resourceList": [],
            "dataSource": 7,
            "targetDataSource": 8,
            "sql": "select * from source_table",
            "targetTable": "target_table",
        },
    ],
)
def test_datax_richer_and_builtin_native_modes_are_opaque_preserve_only(
    native: YamlObject,
) -> None:
    decoded, source = _decode("3.4.2", native)

    assert decoded == native
    assert decoded is not native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    assert _encode("3.4.2", decoded, source=source) == native
    with pytest.raises(TaskParameterProjectionError):
        _decode("3.4.2", native, source=ProjectionSource.TYPED_AUTHORING)


def test_datax_310_is_an_explicit_runtime_hole_with_generic_opaque_paths() -> None:
    version = "3.1.0"
    catalog = get_task_authoring_catalog(version)
    surface = get_task_authoring_surface(version).datax
    native = _native(version)

    assert _TASK_TYPE in catalog.upstream_task_types
    assert _TASK_TYPE not in catalog.reviewed_typed_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is False
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is True
    assert surface.registered is True
    assert surface.typed_custom_json_available is False
    assert surface.exclusion_reason == (
        "null-empty-prepare-params-map-breaks-custom-command"
    )
    with pytest.raises(
        UnsupportedFeatureError,
        match=r"DATAX typed authoring.*3\.1\.0",
    ):
        catalog.normalize_task_params(
            _TASK_TYPE,
            _canonical(),
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    opaque = catalog.normalize_task_params(
        _TASK_TYPE,
        native,
        intent=TaskAuthoringIntent.OPAQUE_CREATE,
    )
    preserved = catalog.normalize_task_params(
        _TASK_TYPE,
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    assert opaque == native
    assert opaque is not native
    assert preserved == native
    assert preserved is not native
    assert _encode(version, native, source=ProjectionSource.OPAQUE_PRESERVE) == native
    with pytest.raises(TaskParameterProjectionError):
        _encode(version, _canonical())


def test_datax_310_public_workflow_authoring_never_downgrades_canonical_input() -> None:
    with pytest.raises(UnsupportedFeatureError, match=r"DATAX.*3\.1\.0"):
        _spec("3.1.0", _canonical())


def test_datax_310_workflow_requires_explicit_native_discriminator() -> None:
    native = _native("3.1.0")

    assert _compiled("3.1.0", native) == native


def test_datax_310_schema_and_default_template_publish_a_safe_opaque_scaffold() -> None:
    catalog = get_task_authoring_catalog("3.1.0")

    schema = task_type_schema_result(_TASK_TYPE, json_schema=True, catalog=catalog)
    template = task_template_result(_TASK_TYPE, catalog=catalog)

    assert isinstance(schema.data, dict)
    assert schema.data["kind"] == "generic"
    assert isinstance(template.data, dict)
    assert "variant" not in template.data["template"]
    task = yaml.safe_load(template.data["yaml"])
    assert task["type"] == _TASK_TYPE
    assert task["task_params"]["customConfig"] == 0
    assert "json" not in task["task_params"]
    assert _compiled("3.1.0", task["task_params"]) == task["task_params"]


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datax_modern_workflow_edit_roundtrips_canonical_intent(
    input_mode: str,
) -> None:
    plan = _modern_edit_plan(_canonical(_UPDATED_JOB_JSON), input_mode=input_mode)

    assert _compiled_from_plan(plan) == _native("3.4.2", _UPDATED_JOB_JSON)


@pytest.mark.parametrize("version", ["3.1.1", "3.1.4", "3.1.8", "3.4.2"])
def test_datax_create_rejects_visible_workflow_globals(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = _spec(version, _canonical())
    spec.workflow.global_params = {"unsafe": "value"}

    with pytest.raises(UserInputError, match="parameter-free") as captured:
        prepare_workflow_create_compilation(spec, catalog=catalog)

    assert captured.value.details["reason"] == (
        "datax-workflow-parameters-outside-typed-facet"
    )
    assert "worker-prepared" in (captured.value.suggestion or "")


def test_datax_before_prepared_argument_forwarding_allows_globals() -> None:
    catalog = get_task_authoring_catalog("3.0.5")
    spec = _spec("3.0.5", _canonical())
    spec.workflow.global_params = {"safe": "not-forwarded"}

    prepared = prepare_workflow_create_compilation(spec, catalog=catalog)

    assert json.loads(prepared.preview()["globalParams"])[0]["prop"] == "safe"


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datax_parameter_edit_rechecks_existing_globals(input_mode: str) -> None:
    version = "3.1.1"
    dag = _modern_dag(_native(version), workflow_name="datax-edit", global_params=True)
    with pytest.raises(UserInputError, match="parameter-free"):
        authoring_prep.single_task_params_edit_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            catalog=get_task_authoring_catalog(version),
            document={
                "workflow": {
                    "name": "datax-edit",
                    "project": "analytics",
                    "global_params": {"unsafe": "value"},
                },
                "tasks": [
                    {
                        "name": "run-datax-job",
                        "type": _TASK_TYPE,
                        "task_params": _canonical(_UPDATED_JOB_JSON),
                    }
                ],
            },
            input_mode=input_mode,
        )


def test_datax_global_edit_rechecks_unchanged_task() -> None:
    version = "3.1.8"
    dag = _modern_dag(_native(version), workflow_name="datax-globals")
    patch = WorkflowPatchDocument.model_validate(
        {"patch": {"workflow": {"set": {"global_params": {"unsafe": "value"}}}}}
    ).patch

    with pytest.raises(UserInputError, match="parameter-free"):
        prepare_workflow_mutation_plan(
            dag,
            project=ResolvedProject(code=7, name="analytics", description=None),
            catalog=get_task_authoring_catalog(version),
            patch=patch,
            release_state="OFFLINE",
        )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datax_metadata_edit_preserves_existing_globals(input_mode: str) -> None:
    native = {**_native("3.1.1"), "futureField": "preserve"}
    dag = _modern_dag(native, workflow_name="datax-metadata", global_params=True)

    plan = authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=ResolvedProject(code=7, name="analytics", description=None),
        catalog=get_task_authoring_catalog("3.1.1"),
        task_name="run-datax-job",
        input_mode=input_mode,
    )

    assert _compiled_from_plan(plan) == native
    assert json.loads(plan.compilation.preview()["globalParams"])[0]["prop"] == "unsafe"


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datax_modern_metadata_only_edit_preserves_richer_native_state(
    input_mode: str,
) -> None:
    native: YamlObject = {
        **_native("3.4.2"),
        "xms": 2,
        "localParams": [
            {
                "prop": "runtime_secret",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "opaque",
            }
        ],
        "futureField": cast(
            "YamlValue",
            {"nested": ["native", {"preserve": True}]},
        ),
    }

    plan = _modern_metadata_plan(native, input_mode=input_mode)

    assert _compiled_from_plan(plan) == native


def test_datax_139_legacy_metadata_edit_preserves_richer_native_state() -> None:
    native: YamlObject = {
        **_native("1.3.9"),
        "localParams": [
            {
                "prop": "opaque",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "native",
            }
        ],
        "futureField": {"preserve": True},
    }
    graph = _legacy_graph(native)
    patch = WorkflowPatchDocument.model_validate(
        {
            "patch": {
                "tasks": {
                    "update": [
                        {
                            "match": {"name": "run-datax-job"},
                            "set": {"description": "metadata only"},
                        }
                    ]
                }
            }
        }
    ).patch

    plan = prepare_legacy_workflow_mutation_plan(
        graph,
        workflow_name="datax-legacy-metadata",
        project_name="analytics",
        description=None,
        release_state="OFFLINE",
        mutation=patch,
        catalog=get_task_authoring_catalog("1.3.9"),
    )
    process_definition = json.loads(plan.compilation.preview()["processDefinitionJson"])

    assert process_definition["tasks"][0]["params"] == native


@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_datax_invalid_modern_workflow_create_and_edit_fail_closed(
    input_mode: str,
) -> None:
    invalid: YamlObject = {"json": "[]"}

    with pytest.raises(ValueError):
        _compiled("3.4.2", invalid)
    with pytest.raises(UserInputError):
        _modern_edit_plan(invalid, input_mode=input_mode)


@pytest.mark.parametrize(
    (
        "version",
        "wire_epoch",
        "python_launcher",
        "datax_launcher",
        "line_separator",
        "cancel_mode",
        "prepared_params_forwarded",
        "resource_files_supported",
    ),
    [
        (
            "1.3.9",
            "legacy-custom-json",
            "PYTHON_HOME-or-python2.7",
            "DATAX_HOME/bin/datax.py",
            "lf",
            "wrapper-kill",
            False,
            False,
        ),
        (
            "2.0.0",
            "custom-json-jvm-memory",
            "PYTHON_HOME-or-python2.7",
            "DATAX_HOME/bin/datax.py",
            "lf",
            "wrapper-kill",
            False,
            False,
        ),
        (
            "3.0.6",
            "custom-json-jvm-memory",
            "PYTHON_HOME-or-python2.7",
            "DATAX_HOME/bin/datax.py",
            "lf",
            "wrapper-kill",
            False,
            False,
        ),
        (
            "3.1.0",
            "custom-json-jvm-memory",
            "PYTHON_HOME-or-python2.7",
            "DATAX_HOME/bin/datax.py",
            "lf",
            "wrapper-kill",
            True,
            True,
        ),
        (
            "3.1.9",
            "custom-json-jvm-memory",
            "PYTHON_HOME-or-python2.7",
            "DATAX_HOME/bin/datax.py",
            "lf",
            "wrapper-kill",
            True,
            True,
        ),
        (
            "3.2.0",
            "custom-json-jvm-memory",
            "PYTHON_LAUNCHER",
            "DATAX_LAUNCHER",
            "system",
            "direct-process-destroy",
            True,
            True,
        ),
        (
            "3.2.2",
            "custom-json-jvm-memory",
            "PYTHON_LAUNCHER",
            "DATAX_LAUNCHER",
            "system",
            "direct-process-destroy",
            True,
            True,
        ),
        (
            "3.3.1",
            "custom-json-jvm-memory",
            "PYTHON_LAUNCHER",
            "DATAX_LAUNCHER",
            "system",
            "process-tree-and-application",
            True,
            True,
        ),
        (
            "3.4.2",
            "custom-json-jvm-memory",
            "PYTHON_LAUNCHER",
            "DATAX_LAUNCHER",
            "system",
            "process-tree-and-application",
            True,
            True,
        ),
    ],
)
def test_datax_runtime_surface_locks_exact_execution_epochs(
    version: str,
    wire_epoch: str,
    python_launcher: str,
    datax_launcher: str,
    line_separator: str,
    cancel_mode: str,
    prepared_params_forwarded: object,
    resource_files_supported: object,
) -> None:
    surface = get_task_authoring_surface(version).datax

    assert surface.registered is True
    assert surface.typed_custom_json_available is (version != "3.1.0")
    assert surface.wire_epoch == wire_epoch
    assert surface.python_launcher == python_launcher
    assert surface.datax_launcher == datax_launcher
    assert surface.line_separator == line_separator
    assert surface.parameter_substitution is True
    assert surface.prepared_params_forwarded is prepared_params_forwarded
    assert surface.resource_files_supported is resource_files_supported
    assert surface.task_params_logged is True
    assert surface.command_logged is True
    assert surface.secret_storage_supported is False
    assert surface.result_output_supported is False
    assert surface.cancel_mode == cancel_mode
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True
    assert surface.exclusion_reason == (
        "null-empty-prepare-params-map-breaks-custom-command"
        if version == "3.1.0"
        else None
    )
