from __future__ import annotations

import json
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
from dsctl.services._workflow.compile import (
    prepare_preserved_workflow_update_compilation,
    prepare_workflow_create_compilation,
)
from dsctl.services._workflow.render import (
    workflow_live_baseline,
    workflow_yaml_document,
)
from dsctl.services.lint import lint_workflow_result
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
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.services.task_authoring_catalog import TaskAuthoringField
    from dsctl.support.json_types import JsonObject


_FACET = "SAGEMAKER/start_pipeline_execution"
_FAMILY = "sagemaker-start-pipeline-execution-v1"
_REVIEW = "sagemaker-start-pipeline-execution-exact-public-template"
_TYPED_VERSIONS = (
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
_ABSENT_VERSIONS = ("1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6")
_DATASOURCE_VERSIONS = (
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
)
_NO_DATASOURCE_VERSIONS = ("3.1.0", "3.1.9", "3.2.0")
_FINGERPRINTS = {
    "3.1.0": "sha256:5e47446e27ffd70477c024b106462853ef6fc4825af88fe26f13e390207ae28d",
    "3.1.9": "sha256:5e47446e27ffd70477c024b106462853ef6fc4825af88fe26f13e390207ae28d",
    "3.2.0": "sha256:2a503b5a5e5d5770f573b14a07581f8c309edcc8b92337f4fc607a20b8817fce",
    "3.2.1": "sha256:5d664221a3cd26d55400c4e230178c66c5de213abf3d6b68267ef2b9ed39bf96",
    "3.2.2": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
    "3.3.1": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
    "3.3.2": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
    "3.4.0": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
    "3.4.1": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
    "3.4.2": "sha256:e912856b4bf4b1fe96783af7b8fe99ec4ac8b7ee794b8f585334885674030582",
}
_LEGACY_PARAMETER_TYPES = (
    "BOOLEAN",
    "DATE",
    "DOUBLE",
    "FLOAT",
    "INTEGER",
    "LIST",
    "LONG",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
)
_MODERN_PARAMETER_TYPES = (
    "BOOLEAN",
    "DATE",
    "DOUBLE",
    "FILE",
    "FLOAT",
    "INTEGER",
    "LIST",
    "LONG",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
)
_RAW_REQUEST = (
    "{\n"
    '  "PipelineName": "nightly-training",\n'
    '  "PipelineExecutionDescription": "nightly execution",\n'
    '  "ClientRequestToken": "stable-client-token",\n'
    '  "PipelineParameters": [\n'
    '    {"Name": "training_job_name", "Value": "nightly-training"}\n'
    "  ],\n"
    '  "ParallelismConfiguration": {"MaxParallelExecutionSteps": 2},\n'
    '  "SelectiveExecutionConfig": {"SelectedSteps": ["Preprocess"]}\n'
    "}"
)
_PLACEHOLDER_REQUEST = (
    "{\n"
    '  "PipelineName": "${pipeline_name}",\n'
    '  "ParallelismConfiguration": {\n'
    '    "MaxParallelExecutionSteps": ${max_parallel_steps}\n'
    "  }\n"
    "}"
)
_REFS = TaskRefIndex.from_code_by_name({})


class _SagemakerSurface(Protocol):
    available: bool
    execution_epoch: str | None
    credential_source: str | None
    credential_keys: tuple[str, ...]
    credential_provider_types: tuple[str, ...]
    datasource_required: bool
    datasource_type_required: bool
    datasource_initialized: bool
    datasource_credentials_used: bool
    raw_request_model: str | None
    raw_request_property_naming: str | None
    raw_request_unknown_fields_ignored: bool
    raw_request_mapper_unknown_enum_values_become_null: bool
    whole_text_parameter_substitution: bool
    parameter_values_json_escaped: bool
    local_param_forwarding: str | None
    resource_files_supported: bool
    task_params_logged: bool
    resolved_request_logged: bool
    pipeline_identifiers_logged: bool
    pipeline_steps_logged: bool
    credentials_logged: bool
    secret_storage_supported: bool
    poll_interval_ms: int | None
    polling_statuses: tuple[str, ...]
    success_statuses: tuple[str, ...]
    result_output_supported: bool
    application_id_fields: tuple[str, ...]
    application_id_persistence: str | None
    failover_supported: bool
    cancel_supported: bool
    retry_may_resubmit: bool
    callback_persistence_gap: bool
    client_closed: bool
    explicit_internal_timeout: bool


def _local_param(
    *,
    prop: str = "pipeline_name",
    direct: str = "IN",
    data_type: str = "VARCHAR",
    value: str = "nightly-training",
) -> YamlObject:
    return {
        "prop": prop,
        "direct": direct,
        "type": data_type,
        "value": value,
    }


def _canonical(
    version: str,
    *,
    request: str = _RAW_REQUEST,
    local_params: list[YamlObject] | None = None,
    datasource: int = 7,
) -> YamlObject:
    params: YamlObject = {
        "sagemakerRequestJson": request,
        "localParams": cast("YamlValue", [] if local_params is None else local_params),
    }
    if version in _DATASOURCE_VERSIONS:
        params["datasource"] = datasource
    return params


def _native(
    version: str,
    *,
    request: str = _RAW_REQUEST,
    local_params: list[YamlObject] | None = None,
    ui_residue: str = "absent",
) -> YamlObject:
    params: YamlObject = {
        **_canonical(version, request=request, local_params=local_params),
        "resourceList": [],
    }
    if version in _DATASOURCE_VERSIONS:
        params["type"] = "SAGEMAKER"
        if ui_residue != "absent":
            residue: YamlValue = "" if ui_residue == "empty" else None
            params.update(
                {
                    "username": residue,
                    "password": residue,
                    "awsRegion": residue,
                }
            )
    return params


def _opaque_native(version: str) -> YamlObject:
    return {
        **_native(version),
        "varPool": [
            {
                "prop": "runtime_value",
                "direct": "OUT",
                "type": "VARCHAR",
                "value": "preserve-me",
            }
        ],
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
            "workflow": {"name": f"sagemaker-{version}"},
            "tasks": [
                {
                    "name": "start-sagemaker-pipeline",
                    "type": "SAGEMAKER",
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
    definition = json.loads(prepared.materialize([31_000])["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])
    assert definition["taskType"] == "SAGEMAKER"
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type="SAGEMAKER",
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
        task_type="SAGEMAKER",
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
) -> FakeDag:
    task = FakeTaskDefinition(
        code=101,
        name="start-sagemaker-pipeline",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="SAGEMAKER",
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
    dag = _fake_dag(version, _native(version), workflow_name="sagemaker-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {"name": "sagemaker-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "start-sagemaker-pipeline",
                    "type": "SAGEMAKER",
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
    dag = _fake_dag(version, native, workflow_name="sagemaker-metadata-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="start-sagemaker-pipeline",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    params = json.loads(definition["taskParams"])
    assert isinstance(params, dict)
    return cast("YamlObject", params)


def _field_map(version: str) -> dict[str, TaskAuthoringField]:
    profile = get_task_authoring_catalog(version).require_task_type("SAGEMAKER")
    membership = profile.facets[_FACET]
    return {field.path: field for field in membership.contract.fields}


def _task_params_schema(version: str) -> dict[object, object]:
    return authoring_prep.task_params_schema("SAGEMAKER", version)


def _template_yaml(version: str, variant: str) -> str:
    if variant == "params":
        return authoring_prep.parameter_example_yaml("SAGEMAKER", version)
    return authoring_prep.template_yaml("SAGEMAKER", version, variant=variant)


def _guidance(version: str) -> str:
    return authoring_prep.schema_guidance("SAGEMAKER", version)


def test_sagemaker_310_typed_create_projects_raw_request_and_local_params() -> None:
    raw_request = (
        "{\n"
        '  "PipelineName": "nightly-training",\n'
        '  "PipelineParameters": [\n'
        '    {"Name": "training_job_name", "Value": "${training_job_name}"}\n'
        "  ]\n"
        "}"
    )
    local_params = [_local_param(prop="training_job_name")]
    catalog = get_task_authoring_catalog("3.1.0")
    assert catalog.supports_typed_authoring("SAGEMAKER") is True
    spec = validate_workflow_document(
        {
            "workflow": {"name": "sagemaker-3-1-0"},
            "tasks": [
                {
                    "name": "submit-training-job",
                    "type": "SAGEMAKER",
                    "task_params": {
                        "sagemakerRequestJson": raw_request,
                        "localParams": cast("YamlValue", local_params),
                    },
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([31_000])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert definition["taskType"] == "SAGEMAKER"
    assert json.loads(definition["taskParams"]) == {
        "sagemakerRequestJson": raw_request,
        "localParams": local_params,
        "resourceList": [],
    }


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_catalog_locks_all_ten_exact_memberships(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("SAGEMAKER")
    membership = catalog.require_facet("SAGEMAKER", _FACET)
    fact = catalog.task_type_facts["SAGEMAKER"]
    review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("SAGEMAKER") is True
    assert catalog.supports_opaque_authoring("SAGEMAKER") is False
    assert profile.category == "MachineLearning"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert fact.semantic_fingerprint == _FINGERPRINTS[version]
    assert review is not None
    assert review.review == _REVIEW
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == _REVIEW
    assert membership.contract.family == _FAMILY
    assert membership.contract.params_model is not None
    assert (
        membership.contract.params_model.__name__
        == "SagemakerStartPipelineExecutionTaskParamsSpec"
    )
    assert membership.contract.opaque_authoring_selector is None
    assert membership.profile_version == version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is False
    assert membership.opaque_edit is False
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_sagemaker_is_absent_from_all_five_earlier_profiles(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "SAGEMAKER" not in catalog.task_type_facts
    with pytest.raises(UnsupportedFeatureError, match="SAGEMAKER"):
        catalog.require_task_type("SAGEMAKER")


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_compile_projects_each_exact_public_wire(version: str) -> None:
    local_params = [_local_param()]
    expected: YamlObject = {
        "sagemakerRequestJson": _RAW_REQUEST,
        "localParams": cast("YamlValue", local_params),
        "resourceList": [],
    }
    if version in _DATASOURCE_VERSIONS:
        expected.update({"datasource": 7, "type": "SAGEMAKER"})

    assert (
        _compiled(version, _canonical(version, local_params=local_params)) == expected
    )


def test_sagemaker_datasource_and_type_join_only_after_320() -> None:
    assert _encode("3.2.0", _canonical("3.2.0")) == {
        "sagemakerRequestJson": _RAW_REQUEST,
        "localParams": [],
        "resourceList": [],
    }
    assert _encode("3.2.1", _canonical("3.2.1")) == {
        "sagemakerRequestJson": _RAW_REQUEST,
        "localParams": [],
        "datasource": 7,
        "resourceList": [],
        "type": "SAGEMAKER",
    }


@pytest.mark.parametrize("version", _NO_DATASOURCE_VERSIONS)
def test_sagemaker_rejects_datasource_before_321(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError, match="does not exist"):
        _encode(version, {**_canonical(version), "datasource": 7})


@pytest.mark.parametrize("version", _DATASOURCE_VERSIONS)
def test_sagemaker_requires_one_datasource_reference_and_owns_type(
    version: str,
) -> None:
    with pytest.raises(TaskParameterProjectionError, match="requires one positive"):
        _encode(
            version,
            {
                "sagemakerRequestJson": _RAW_REQUEST,
                "localParams": [],
            },
        )
    for value in (0, -1, True, 7.0, "", "  "):
        with pytest.raises(
            TaskParameterProjectionError,
            match=r"SAGEMAKER typed task_params\.datasource is invalid",
        ) as exc_info:
            _encode(version, {**_canonical(version), "datasource": value})
        assert exc_info.value.details["field"] == "task_params.datasource"
    with pytest.raises(
        TaskParameterProjectionError,
        match=r"SAGEMAKER typed task_params\.type is invalid",
    ) as exc_info:
        _encode(version, {**_canonical(version), "type": "SAGEMAKER"})
    assert exc_info.value.details["field"] == "task_params.type"


@pytest.mark.parametrize("version", _DATASOURCE_VERSIONS)
@pytest.mark.parametrize("include_null", [False, True], ids=("missing", "null"))
@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_sagemaker_active_datasource_is_required_before_reference_resolution(
    version: str, intent: TaskAuthoringIntent, *, include_null: bool
) -> None:
    params = _canonical(version)
    params.pop("datasource")
    if include_null:
        params["datasource"] = None

    with pytest.raises(
        ValueError,
        match=r"SAGEMAKER task_params\.datasource requires one positive integer id "
        r"or nonblank exact name",
    ):
        get_task_authoring_catalog(version).normalize_task_params(
            "SAGEMAKER", params, intent=intent
        )


@pytest.mark.parametrize("version", _DATASOURCE_VERSIONS)
@pytest.mark.parametrize("datasource", [7, "training-credentials", "123"])
@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT]
)
def test_sagemaker_active_datasource_keeps_positive_ids_and_exact_names(
    version: str, datasource: int | str, intent: TaskAuthoringIntent
) -> None:
    params: YamlObject = {**_canonical(version), "datasource": datasource}

    normalized = get_task_authoring_catalog(version).normalize_task_params(
        "SAGEMAKER", params, intent=intent
    )

    assert normalized["datasource"] == datasource
    assert type(normalized["datasource"]) is type(datasource)


@pytest.mark.parametrize("version", [*_DATASOURCE_VERSIONS, "3.1.0", "3.2.0"])
@pytest.mark.parametrize("include_null", [False, True], ids=("missing", "null"))
@pytest.mark.parametrize(
    "sql_datasource", [17, "warehouse"], ids=("sql-id", "sql-name")
)
def test_sagemaker_datasource_requiredness_is_independent_of_sql_name_deferral(
    tmp_path: Path, version: str, sql_datasource: int | str, *, include_null: bool
) -> None:
    params = _canonical(version)
    params.pop("datasource", None)
    if include_null:
        params["datasource"] = None
    document: YamlObject = {
        "workflow": {"name": "mixed-sql-sagemaker"},
        "tasks": [
            {
                "name": "query",
                "type": "SQL",
                "task_params": {
                    "type": "MYSQL",
                    "datasource": sql_datasource,
                    "sql": "select 1",
                    "sqlType": 0,
                },
            },
            {
                "name": "train",
                "type": "SAGEMAKER",
                "task_params": params,
            },
        ],
    }
    source = tmp_path / "mixed-sql-sagemaker.yaml"
    source.write_text(yaml.safe_dump(document), encoding="utf-8")

    result = lint_workflow_result(
        file=source, catalog=get_task_authoring_catalog(version)
    )

    assert isinstance(result.data, dict)
    if version in _DATASOURCE_VERSIONS:
        assert result.data["valid"] is False
        assert isinstance(result.failure, UserInputError)
        assert "SAGEMAKER task_params.datasource requires" in result.failure.message
        diagnostics = result.data["diagnostics"]
        assert isinstance(diagnostics, list)
        assert any(
            isinstance(diagnostic, dict)
            and diagnostic["severity"] == "error"
            and diagnostic["path"] == "tasks[1].task_params"
            and isinstance(diagnostic["message"], str)
            and "SAGEMAKER task_params.datasource" in diagnostic["message"]
            for diagnostic in diagnostics
        )
    else:
        assert result.data["valid"] is True
        assert result.failure is None


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_catalog_exposes_exact_local_param_epoch(version: str) -> None:
    fields = _field_map(version)
    expected_paths = {
        "task_params.sagemakerRequestJson",
        "task_params.localParams",
        "task_params.localParams[]",
        "task_params.localParams[].prop",
        "task_params.localParams[].direct",
        "task_params.localParams[].type",
        "task_params.localParams[].value",
    }
    if version in _DATASOURCE_VERSIONS:
        expected_paths.add("task_params.datasource")

    assert set(fields) == expected_paths
    assert fields["task_params.localParams[].direct"].choices == ("IN", "OUT")
    assert fields["task_params.localParams[].type"].choices == (
        _LEGACY_PARAMETER_TYPES
        if version in {"3.1.0", "3.1.9"}
        else _MODERN_PARAMETER_TYPES
    )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("direct", ["IN", "OUT"])
def test_sagemaker_accepts_both_public_parameter_directions(
    version: str,
    direct: str,
) -> None:
    parameter_types = (
        _LEGACY_PARAMETER_TYPES
        if version in {"3.1.0", "3.1.9"}
        else _MODERN_PARAMETER_TYPES
    )
    for data_type in parameter_types:
        local = [_local_param(direct=direct, data_type=data_type)]
        assert (
            _encode(version, _canonical(version, local_params=local))["localParams"]
            == local
        )


@pytest.mark.parametrize("version", ["3.1.0", "3.1.9"])
def test_sagemaker_file_parameter_type_joins_only_in_320(version: str) -> None:
    params = _canonical(
        version,
        local_params=[_local_param(data_type="FILE")],
    )
    with pytest.raises(UnsupportedFeatureError) as exc_info:
        _spec(version, params)

    assert exc_info.value.details["field"] == "tasks[].task_params.localParams[].type"
    modern = _canonical(
        "3.2.0",
        local_params=[_local_param(data_type="FILE")],
    )
    assert _encode("3.2.0", modern)["localParams"] == modern["localParams"]


def test_sagemaker_local_param_names_are_unique_after_normalization() -> None:
    params = _canonical(
        "3.4.2",
        local_params=[
            _local_param(prop="pipeline_name"),
            _local_param(prop=" pipeline_name "),
        ],
    )
    with pytest.raises(ValueError, match="must be unique"):
        _spec("3.4.2", params)
    with pytest.raises(TaskParameterProjectionError, match="must be unique"):
        _encode("3.4.2", params)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_placeholder_template_preserves_exact_authored_text(
    version: str,
) -> None:
    local_params = [
        _local_param(prop="pipeline_name"),
        _local_param(
            prop="max_parallel_steps",
            data_type="INTEGER",
            value="2",
        ),
    ]
    authored = _canonical(
        version,
        request=_PLACEHOLDER_REQUEST,
        local_params=local_params,
    )

    normalized = get_task_authoring_catalog(version).normalize_task_params(
        "SAGEMAKER",
        authored,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    encoded = _encode(version, authored)
    compiled = _compiled(version, authored)
    decoded, source = _decode(version, encoded)

    assert normalized["sagemakerRequestJson"] == _PLACEHOLDER_REQUEST
    assert encoded["sagemakerRequestJson"] == _PLACEHOLDER_REQUEST
    assert compiled["sagemakerRequestJson"] == _PLACEHOLDER_REQUEST
    assert decoded == authored
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    "request_text",
    ["[]", '"string"', "null", "true", "42", "{not-json}"],
)
def test_sagemaker_placeholder_free_request_must_be_one_json_object(
    request_text: str,
) -> None:
    with pytest.raises(ValueError, match="sagemakerRequestJson"):
        _spec("3.4.2", _canonical("3.4.2", request=request_text))
    with pytest.raises(TaskParameterProjectionError, match="sagemakerRequestJson"):
        _encode("3.4.2", _canonical("3.4.2", request=request_text))


def test_sagemaker_valid_json_request_preserves_unknown_aws_spelling_as_text() -> None:
    request = (
        '{"PipelineName":"nightly-training",'
        '"FutureAwsRequestMember":{"Preserve":[1,true,null]}}'
    )
    encoded = _encode("3.4.2", _canonical("3.4.2", request=request))

    assert encoded["sagemakerRequestJson"] == request


@pytest.mark.parametrize("version", ["3.1.0", "3.2.0", "3.2.1", "3.4.2"])
def test_sagemaker_json_schema_locks_exact_public_fields(version: str) -> None:
    schema = _task_params_schema(version)
    properties = schema["properties"]
    assert isinstance(properties, dict)
    expected = {"sagemakerRequestJson", "localParams"}
    if version in _DATASOURCE_VERSIONS:
        expected.add("datasource")

    assert set(properties) == expected
    assert schema["additionalProperties"] is False
    required = schema["required"]
    assert isinstance(required, list)
    assert "sagemakerRequestJson" in required
    if version in _DATASOURCE_VERSIONS:
        assert "datasource" in required
        datasource_schema = properties["datasource"]
        assert isinstance(datasource_schema, dict)
        assert datasource_schema["anyOf"] == [
            {"minimum": 1, "type": "integer"},
            {"pattern": r"\S", "type": "string"},
        ]
        assert "default" not in datasource_schema
    else:
        assert "datasource" not in required
        assert "datasource" not in properties
    local_params = properties["localParams"]
    assert isinstance(local_params, dict)
    items = local_params["items"]
    assert isinstance(items, dict)
    assert items["$ref"] == "#/$defs/task_params/$defs/GlobalParamSpec"
    definitions = schema["$defs"]
    assert isinstance(definitions, dict)
    item_schema = definitions["GlobalParamSpec"]
    direct_schema = definitions["Direct"]
    type_schema = definitions["DataType"]
    assert set(item_schema["properties"]) == {"prop", "direct", "type", "value"}
    assert item_schema["required"] == ["prop"]
    assert direct_schema["enum"] == ["IN", "OUT"]
    assert tuple(type_schema["enum"]) == (
        _LEGACY_PARAMETER_TYPES
        if version in {"3.1.0", "3.1.9"}
        else _MODERN_PARAMETER_TYPES
    )


@pytest.mark.parametrize("version", ["3.1.0", "3.2.0", "3.2.1", "3.4.2"])
def test_sagemaker_summary_and_compile_mappings_lock_exact_contract(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data("SAGEMAKER", catalog=catalog)
    result = task_type_schema_result(
        "SAGEMAKER",
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
    fields = _field_map(version)
    expected_paths = set(fields)

    assert summary["task_type"] == "SAGEMAKER"
    assert summary["category"] == "MachineLearning"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(mappings) == expected_paths
    assert mappings["task_params.localParams[]"] == (
        "taskDefinitionJson[].taskParams.localParams"
    )
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        "task_params.sagemakerRequestJson",
        "task_params.localParams[].prop",
        *(("task_params.datasource",) if version in _DATASOURCE_VERSIONS else ()),
    }


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("variant", ["minimal", "params"])
def test_sagemaker_templates_compile_on_every_exact_epoch(
    version: str,
    variant: str,
) -> None:
    task = yaml.safe_load(_template_yaml(version, variant))
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "SAGEMAKER"
    assert isinstance(params["sagemakerRequestJson"], str)
    assert ("datasource" in params) is (version in _DATASOURCE_VERSIONS)
    if version in _DATASOURCE_VERSIONS:
        assert params["datasource"] == 1
    if variant == "params":
        assert "${training_job_name}" in params["sagemakerRequestJson"]
        assert params["localParams"] == [
            {
                "prop": "training_job_name",
                "direct": "IN",
                "type": "VARCHAR",
                "value": "nightly-training",
            }
        ]
    _spec(version, cast("YamlObject", params))
    assert _compiled(version, cast("YamlObject", params))["resourceList"] == []


@pytest.mark.parametrize(
    ("version", "required_fragments"),
    [
        (
            "3.1.0",
            (
                "resource.aws.access.key.id",
                "legacy synchronous worker",
                "no durable application id",
            ),
        ),
        (
            "3.2.1",
            (
                "positive sagemaker datasource",
                "username, password, and awsregion",
                "callback",
            ),
        ),
        (
            "3.4.2",
            (
                "aws.sagemaker.credentials.provider.type",
                "ignored by the aws client",
                "optional endpoint",
            ),
        ),
    ],
)
def test_sagemaker_guidance_exposes_request_and_runtime_hazards(
    version: str,
    required_fragments: tuple[str, ...],
) -> None:
    guidance = _guidance(version)

    for fragment in (
        "startpipelineexecutionrequest",
        "uppercamelcase",
        "unknown or incorrectly cased fields are ignored",
        "across the whole json text",
        "performs no json escaping",
        "5000 ms",
        "executing",
        "succeeded",
        "no internal deadline",
        "not closed",
        "structured output are unsupported",
        "retry",
        "info-logs",
        *required_fragments,
    ):
        assert fragment in guidance


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_canonical_native_wire_decodes_typed(version: str) -> None:
    native = _native(version)
    decoded, source = _decode(version, native)

    assert decoded == _canonical(version)
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("version", _DATASOURCE_VERSIONS)
@pytest.mark.parametrize("ui_residue", ["empty", "null"])
def test_sagemaker_empty_ui_credential_residue_remains_typed(
    version: str,
    ui_residue: str,
) -> None:
    native = _native(version, ui_residue=ui_residue)
    decoded, source = _decode(version, native)

    assert decoded == _canonical(version)
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("username", "AKIA-NOT-A-USER"),
        ("password", "not-empty"),
        ("awsRegion", "us-east-1"),
        ("resourceList", [{"id": 11}]),
        ("varPool", []),
        ("futureField", {"native": True}),
    ],
)
def test_sagemaker_richer_native_state_is_opaque_preserve_only(
    field: str,
    value: YamlValue,
) -> None:
    native = _native("3.4.2")
    native[field] = value
    decoded, source = _decode("3.4.2", native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError, match=field):
        _decode("3.4.2", native, source=ProjectionSource.TYPED_AUTHORING)


@pytest.mark.parametrize(
    ("version", "field", "value"),
    [
        ("3.2.0", "type", "SAGEMAKER"),
        ("3.2.0", "datasource", 7),
        ("3.2.0", "username", ""),
        ("3.2.1", "type", "OTHER"),
        ("3.2.1", "type", None),
    ],
)
def test_sagemaker_wrong_epoch_or_discriminator_is_not_typed(
    version: str,
    field: str,
    value: YamlValue,
) -> None:
    native = _native(version)
    native[field] = value
    decoded, source = _decode(version, native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError, match=field):
        _decode(version, native, source=ProjectionSource.TYPED_AUTHORING)


@pytest.mark.parametrize("version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT],
)
def test_sagemaker_has_no_generic_opaque_create_or_edit(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError) as captured:
        get_task_authoring_catalog(version).normalize_task_params(
            "SAGEMAKER",
            _opaque_native(version),
            intent=intent,
        )

    assert str(captured.value) == (
        f"SAGEMAKER opaque authoring is unsupported for DolphinScheduler {version}."
    )
    assert captured.value.details == {
        "selected_version": version,
        "task_type": "SAGEMAKER",
        "intent": intent.value,
        "constraint": (
            f"Exact DolphinScheduler {version} policy permits only opaque "
            "preservation for SAGEMAKER."
        ),
    }


def test_sagemaker_opaque_preserve_is_a_deep_identity_copy() -> None:
    native = _opaque_native("3.4.2")
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "SAGEMAKER",
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


@pytest.mark.parametrize("version", ["3.1.0", "3.2.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_sagemaker_high_level_edit_projects_exact_public_wire(
    version: str,
    input_mode: str,
) -> None:
    request = '{"PipelineName":"edited-pipeline"}'
    params = _canonical(
        version,
        request=request,
        local_params=[_local_param(prop="edited_name", value="edited-pipeline")],
    )
    compiled = _compiled_from_plan(_edit_plan(version, params, input_mode=input_mode))

    assert compiled["sagemakerRequestJson"] == request
    assert compiled["localParams"] == params["localParams"]
    assert compiled["resourceList"] == []
    if version in _DATASOURCE_VERSIONS:
        assert compiled["datasource"] == 7
        assert compiled["type"] == "SAGEMAKER"
    else:
        assert "datasource" not in compiled
        assert "type" not in compiled


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (
            _native("3.4.2", ui_residue="empty"),
            _canonical("3.4.2"),
            ProjectionSource.TYPED_AUTHORING,
        ),
        (
            _native("3.4.2", ui_residue="null"),
            _canonical("3.4.2"),
            ProjectionSource.TYPED_AUTHORING,
        ),
        (
            _opaque_native("3.4.2"),
            _opaque_native("3.4.2"),
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_sagemaker_export_distinguishes_typed_and_opaque_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.4.2"
    dag = _fake_dag(version, native, workflow_name="sagemaker-export")
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

    assert baseline.projection_sources["start-sagemaker-pipeline"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("native", "expected"),
    [
        (_native("3.4.2", ui_residue="empty"), _native("3.4.2")),
        (_opaque_native("3.4.2"), _opaque_native("3.4.2")),
    ],
)
def test_sagemaker_metadata_edit_applies_typed_or_opaque_fixed_point(
    native: YamlObject,
    expected: YamlObject,
    input_mode: str,
) -> None:
    plan = _metadata_plan("3.4.2", native, input_mode=input_mode)

    assert _compiled_from_plan(plan) == expected


@pytest.mark.parametrize(
    ("native", "expected"),
    [
        (_native("3.4.2", ui_residue="empty"), _native("3.4.2")),
        (_opaque_native("3.4.2"), _opaque_native("3.4.2")),
    ],
)
def test_sagemaker_unchanged_update_applies_typed_or_opaque_fixed_point(
    native: YamlObject,
    expected: YamlObject,
) -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    baseline = workflow_live_baseline(
        _fake_dag(version, native, workflow_name="sagemaker-unchanged"),
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

    assert compiled == expected


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_sagemaker_runtime_surface_is_absent_before_310(version: str) -> None:
    surface = cast("_SagemakerSurface", get_task_authoring_surface(version).sagemaker)

    assert surface.available is False
    assert surface.execution_epoch is None
    assert surface.credential_source is None
    assert surface.credential_keys == ()
    assert surface.credential_provider_types == ()
    assert surface.datasource_required is False
    assert surface.datasource_type_required is False
    assert surface.datasource_initialized is False
    assert surface.datasource_credentials_used is False
    assert surface.raw_request_model is None
    assert surface.raw_request_property_naming is None
    assert surface.raw_request_unknown_fields_ignored is False
    assert surface.raw_request_mapper_unknown_enum_values_become_null is False
    assert surface.whole_text_parameter_substitution is False
    assert surface.local_param_forwarding is None
    assert surface.parameter_values_json_escaped is False
    assert surface.resource_files_supported is False
    assert surface.task_params_logged is False
    assert surface.resolved_request_logged is False
    assert surface.pipeline_identifiers_logged is False
    assert surface.pipeline_steps_logged is False
    assert surface.credentials_logged is False
    assert surface.secret_storage_supported is False
    assert surface.poll_interval_ms is None
    assert surface.polling_statuses == ()
    assert surface.success_statuses == ()
    assert surface.result_output_supported is False
    assert surface.application_id_fields == ()
    assert surface.application_id_persistence is None
    assert surface.failover_supported is False
    assert surface.cancel_supported is False
    assert surface.retry_may_resubmit is False
    assert surface.callback_persistence_gap is False
    assert surface.client_closed is False
    assert surface.explicit_internal_timeout is False


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_sagemaker_runtime_surface_locks_common_public_contract(version: str) -> None:
    surface = cast("_SagemakerSurface", get_task_authoring_surface(version).sagemaker)

    assert surface.available is True
    assert surface.raw_request_model == "StartPipelineExecutionRequest"
    assert surface.raw_request_property_naming == "UpperCamelCase"
    assert surface.raw_request_unknown_fields_ignored is True
    assert surface.raw_request_mapper_unknown_enum_values_become_null is True
    assert surface.whole_text_parameter_substitution is True
    assert surface.local_param_forwarding == (
        "all-local-params" if version in {"3.4.1", "3.4.2"} else "input-only"
    )
    assert surface.parameter_values_json_escaped is False
    assert surface.resource_files_supported is False
    assert surface.task_params_logged is True
    assert surface.resolved_request_logged is True
    assert surface.pipeline_identifiers_logged is True
    assert surface.pipeline_steps_logged is True
    assert surface.secret_storage_supported is False
    assert surface.poll_interval_ms == 5000
    assert surface.polling_statuses == ("Executing",)
    assert surface.success_statuses == ("Succeeded",)
    assert surface.result_output_supported is False
    assert surface.cancel_supported is True
    assert surface.retry_may_resubmit is True
    assert surface.client_closed is False
    assert surface.explicit_internal_timeout is False


@pytest.mark.parametrize(
    (
        "version",
        "execution_epoch",
        "credential_source",
        "credential_keys",
        "provider_types",
        "datasource_required",
        "datasource_credentials_used",
        "credentials_logged",
        "application_id_fields",
        "application_id_persistence",
        "failover_supported",
        "callback_gap",
    ),
    [
        (
            "3.1.0",
            "legacy-synchronous-worker",
            "worker-properties-static-basic",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
            ("AWSStaticCredentialsProvider",),
            False,
            False,
            False,
            (),
            "legacy-pre-submit-null",
            False,
            False,
        ),
        (
            "3.1.9",
            "abstract-remote-task",
            "worker-properties-static-basic",
            (
                "resource.aws.access.key.id",
                "resource.aws.secret.access.key",
                "resource.aws.region",
            ),
            ("AWSStaticCredentialsProvider",),
            False,
            False,
            False,
            ("pipelineExecutionArn", "clientRequestToken"),
            "appIds-callback",
            True,
            True,
        ),
        (
            "3.2.1",
            "abstract-remote-task",
            "datasource-static-basic",
            ("datasource.userName", "datasource.password", "datasource.awsRegion"),
            ("AWSStaticCredentialsProvider",),
            True,
            True,
            True,
            ("pipelineExecutionArn", "clientRequestToken"),
            "appIds-callback",
            True,
            True,
        ),
        (
            "3.4.2",
            "abstract-remote-task",
            "aws-authentication",
            (
                "aws.sagemaker.credentials.provider.type",
                "aws.sagemaker.access.key.id",
                "aws.sagemaker.access.key.secret",
                "aws.sagemaker.region",
                "aws.sagemaker.endpoint",
            ),
            (
                "AWSStaticCredentialsProvider",
                "InstanceProfileCredentialsProvider",
            ),
            True,
            False,
            True,
            ("pipelineExecutionArn", "clientRequestToken"),
            "appIds-callback",
            True,
            True,
        ),
    ],
)
def test_sagemaker_runtime_surface_locks_auth_and_recovery_epochs(
    version: str,
    execution_epoch: str,
    credential_source: str,
    credential_keys: tuple[str, ...],
    provider_types: tuple[str, ...],
    *,
    datasource_required: bool,
    datasource_credentials_used: bool,
    credentials_logged: bool,
    application_id_fields: tuple[str, ...],
    application_id_persistence: str,
    failover_supported: bool,
    callback_gap: bool,
) -> None:
    surface = cast("_SagemakerSurface", get_task_authoring_surface(version).sagemaker)

    assert surface.execution_epoch == execution_epoch
    assert surface.credential_source == credential_source
    assert surface.credential_keys == credential_keys
    assert surface.credential_provider_types == provider_types
    assert surface.datasource_required is datasource_required
    assert surface.datasource_type_required is datasource_required
    assert surface.datasource_initialized is datasource_required
    assert surface.datasource_credentials_used is datasource_credentials_used
    assert surface.credentials_logged is credentials_logged
    assert surface.application_id_fields == application_id_fields
    assert surface.application_id_persistence == application_id_persistence
    assert surface.failover_supported is failover_supported
    assert surface.callback_persistence_gap is callback_gap
