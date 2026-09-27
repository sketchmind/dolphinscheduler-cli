from __future__ import annotations

import json
import re
from copy import deepcopy
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
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.services._workflow.mutation import WorkflowMutationPlan
    from dsctl.support.json_types import JsonObject


_TASK_TYPE = "ALIYUN_SERVERLESS_SPARK"
_FACET = "ALIYUN_SERVERLESS_SPARK/literal_jar_submit"
_TYPED_VERSIONS = (
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
    "3.1.0",
    "3.1.9",
    "3.2.0",
    "3.2.1",
    "3.2.2",
)
_FINGERPRINT = "sha256:2144b73670e6e1c6ef6061e55ba8d1eb3ee6830b63c60a2ec634ef5ee0609440"
_CANONICAL_FIELDS = {
    "datasource",
    "workspaceId",
    "resourceQueueId",
    "jobName",
    "entryPoint",
    "entryPointArguments",
    "sparkSubmitParameters",
    "isProduction",
}
_REQUIRED_FIELDS = _CANONICAL_FIELDS - {"isProduction"}
_WIRE_FIELDS = _CANONICAL_FIELDS | {"type", "codeType"}
_LITERAL_FIELDS = {
    "workspaceId",
    "resourceQueueId",
    "jobName",
    "sparkSubmitParameters",
}
_EXCLUDED_FIELDS = {
    "engineReleaseVersion",
    "templateId",
    "localParams",
    "varPool",
    "resourceList",
    "futureField",
}
_REFS = TaskRefIndex.from_code_by_name({})


class _AliyunServerlessSparkSurface(Protocol):
    available: bool
    runtime_epoch: str | None
    template_fetch_always: bool
    template_may_mutate_submit_parameters: bool
    template_may_derive_release_and_fusion: bool
    retry_attempts: int
    retry_interval_ms: int
    template_retry: bool
    start_retry: bool
    status_retry: bool
    cancel_retry: bool
    client_token_within_attempt: bool
    failed_exit: str | None
    start_exception_cause_preserved: bool
    cancel_failure_propagated: bool
    cancel_exception_cause_preserved: bool
    task_params_logged: bool
    job_id_logged: bool
    state_logged: bool
    credentials_logged: bool
    result_output_supported: bool
    durable_application_id: bool
    failover_supported: bool
    retry_may_duplicate: bool
    cancel_requires_in_memory_job_id: bool
    poll_interval_seconds: int
    poll_deadline: bool
    credential_source: str | None
    credential_keys: tuple[str, ...]
    custom_endpoint_supported: bool
    default_endpoint_template: str | None
    connectivity_check_verified: bool


class _Surface(Protocol):
    aliyun_serverless_spark: _AliyunServerlessSparkSurface


def _canonical(
    *,
    datasource: int = 17,
    workspace_id: str = "w-analytics-prod",
    resource_queue_id: str = "root.analytics",
    job_name: str = "daily-orders",
    entry_point: str = "oss://analytics-jobs/jars/orders.jar",
    entry_point_arguments: list[str] | None = None,
    spark_submit_parameters: str = (
        "--class com.example.Orders --conf spark.sql.shuffle.partitions=64"
    ),
    is_production: bool = False,
) -> YamlObject:
    return {
        "datasource": datasource,
        "workspaceId": workspace_id,
        "resourceQueueId": resource_queue_id,
        "jobName": job_name,
        "entryPoint": entry_point,
        "entryPointArguments": cast(
            "YamlValue",
            (
                ["--date", "2026-08-20"]
                if entry_point_arguments is None
                else entry_point_arguments
            ),
        ),
        "sparkSubmitParameters": spark_submit_parameters,
        "isProduction": is_production,
    }


def _native(**overrides: YamlValue) -> YamlObject:
    canonical = _canonical()
    canonical.update(overrides)
    arguments = canonical["entryPointArguments"]
    assert isinstance(arguments, list)
    return {
        "datasource": canonical["datasource"],
        "workspaceId": canonical["workspaceId"],
        "resourceQueueId": canonical["resourceQueueId"],
        "jobName": canonical["jobName"],
        "entryPoint": canonical["entryPoint"],
        "entryPointArguments": "#".join(cast("list[str]", arguments)),
        "sparkSubmitParameters": canonical["sparkSubmitParameters"],
        "isProduction": canonical["isProduction"],
        "type": "ALIYUN_SERVERLESS_SPARK",
        "codeType": "JAR",
    }


def _opaque_python_native() -> YamlObject:
    return {
        "datasource": 17,
        "workspaceId": "w-analytics-prod",
        "resourceQueueId": "root.analytics",
        "jobName": "python-orders",
        "entryPoint": "oss://analytics-jobs/python/orders.py",
        "entryPointArguments": "--date#2026-08-20",
        "sparkSubmitParameters": "--conf spark.executor.instances=2",
        "isProduction": False,
        "type": "ALIYUN_SERVERLESS_SPARK",
        "codeType": "PYTHON",
        "futureField": {"nested": ["native", {"preserve": True}]},
    }


def _jar_extra_native() -> YamlObject:
    return {
        **_native(),
        "engineReleaseVersion": "esr-2.1-native",
        "templateId": "template-managed-by-server",
        "localParams": [],
        "varPool": [{"prop": "runtime", "value": "native"}],
        "resourceList": [],
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
            "workflow": {"name": f"aliyun-serverless-spark-{version}"},
            "tasks": [
                {
                    "name": "submit-aliyun-serverless-spark-jar",
                    "type": _TASK_TYPE,
                    "task_params": params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=intent,
        ),
    )


def _compiled_definition(version: str, params: YamlObject) -> YamlObject:
    catalog = get_task_authoring_catalog(version)
    prepared = prepare_workflow_create_compilation(
        _spec(version, params),
        catalog=catalog,
    )
    definition = json.loads(prepared.materialize([43_000])["taskDefinitionJson"])[0]
    assert isinstance(definition, dict)
    return cast("YamlObject", definition)


def _compiled(version: str, params: YamlObject) -> YamlObject:
    definition = _compiled_definition(version, params)
    native = json.loads(cast("str", definition["taskParams"]))

    assert definition["taskType"] == _TASK_TYPE
    assert definition["taskExecuteType"] == "BATCH"
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _encode(version: str, params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version=version,
        task_type=_TASK_TYPE,
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
        task_type=_TASK_TYPE,
        task_params=cast("JsonObject", deepcopy(params)),
        refs=_REFS,
        source=source,
    )
    return cast("YamlObject", decoded.task.task_params), decoded.reencode_source


def _fake_dag(
    params: YamlObject,
    *,
    workflow_name: str,
    task_name: str = "submit-aliyun-serverless-spark-jar",
) -> FakeDag:
    return FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name=workflow_name,
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[
            FakeTaskDefinition(
                code=101,
                name=task_name,
                project_code_value=7,
                project_name_value="analytics",
                task_type_value=_TASK_TYPE,
                task_params_value=json.dumps(params),
                worker_group_value="default",
                task_execute_type_value=FakeEnumValue("BATCH"),
            )
        ],
        workflow_task_relation_list_value=[],
    )


def _edit_plan(
    version: str,
    params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    dag = _fake_dag(_native(), workflow_name="aliyun-serverless-spark-edit")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_params_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        document={
            "workflow": {
                "name": "aliyun-serverless-spark-edit",
                "project": "analytics",
            },
            "tasks": [
                {
                    "name": "submit-aliyun-serverless-spark-jar",
                    "type": _TASK_TYPE,
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
    dag = _fake_dag(native, workflow_name="aliyun-serverless-spark-metadata")
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(version)
    return authoring_prep.single_task_metadata_edit_plan(
        dag,
        project=project,
        catalog=catalog,
        task_name="submit-aliyun-serverless-spark-jar",
        input_mode=input_mode,
    )


def _compiled_from_plan(plan: WorkflowMutationPlan) -> YamlObject:
    definition = json.loads(plan.compilation.preview()["taskDefinitionJson"])[0]
    native = json.loads(definition["taskParams"])

    assert definition["taskType"] == _TASK_TYPE
    assert definition["taskExecuteType"] == "BATCH"
    assert isinstance(native, dict)
    return cast("YamlObject", native)


def _template_yaml(version: str) -> str:
    return authoring_prep.template_yaml(_TASK_TYPE, version, variant="minimal")


def _guidance(version: str) -> str:
    return authoring_prep.schema_guidance(_TASK_TYPE, version)


def _task_params_schema(version: str) -> dict[object, object]:
    return authoring_prep.task_params_schema(_TASK_TYPE, version)


def _resolved_field_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    return authoring_prep.resolved_field_schema(task_params_schema, field_name)


def _schema_pattern_accepts(field_name: str, value: str) -> bool:
    task_params = _task_params_schema("3.4.2")
    field_schema = _resolved_field_schema(task_params, field_name)
    pattern = field_schema["pattern"]
    assert isinstance(pattern, str)
    return re.fullmatch(pattern, value) is not None


def _argument_schema_pattern_accepts(value: str) -> bool:
    task_params = _task_params_schema("3.4.2")
    arguments = _resolved_field_schema(task_params, "entryPointArguments")
    item = arguments["items"]
    assert isinstance(item, dict)
    reference = item.get("$ref")
    if isinstance(reference, str):
        definitions = task_params["$defs"]
        assert isinstance(definitions, dict)
        item = definitions[reference.rsplit("/", maxsplit=1)[-1]]
        assert isinstance(item, dict)
    pattern = item["pattern"]
    assert isinstance(pattern, str)
    return re.fullmatch(pattern, value) is not None


def _runtime_accepts(field_name: str, value: YamlValue) -> bool:
    params = _canonical()
    params[field_name] = value
    try:
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
    except ValueError:
        return False
    return True


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_catalog_exposes_one_exact_literal_jar_facet(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type(_TASK_TYPE)
    membership = catalog.require_facet(_TASK_TYPE, _FACET)
    fact = catalog.task_type_facts[_TASK_TYPE]
    review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring(_TASK_TYPE) is True
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is True
    assert profile.category == "Cloud"
    assert profile.default_facet == _FACET
    assert set(profile.facets) == {_FACET}
    assert fact.parameter_model_import == (
        "org.apache.dolphinscheduler.plugin.task.aliyunserverlessspark."
        "AliyunServerlessSparkParameters"
    )
    assert fact.semantic_fingerprint == _FINGERPRINT
    assert review is not None
    assert review.review == "aliyun-serverless-spark-literal-jar-exact-subset"
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == review.review
    assert membership.contract.family == "aliyun-serverless-spark-literal-jar-v1"
    assert membership.contract.params_model is not None
    assert membership.contract.opaque_authoring_selector is not None
    assert membership.profile_version == version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_aliyun_serverless_spark_is_upstream_absent_before_3_3_1(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)

    assert _TASK_TYPE not in catalog.upstream_task_types
    assert catalog.supports_typed_authoring(_TASK_TYPE) is False
    assert catalog.supports_opaque_authoring(_TASK_TYPE) is False
    assert _TASK_TYPE not in catalog.authoring_task_types


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_aliyun_serverless_spark_projector_rejects_absent_versions(
    version: str,
) -> None:
    with pytest.raises(TaskParameterProjectionError, match=_TASK_TYPE):
        _encode(version, _canonical())


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_schema_exposes_only_the_owned_canonical_fields(
    version: str,
) -> None:
    result = task_type_schema_result(
        _TASK_TYPE,
        catalog=get_task_authoring_catalog(version),
    )
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }
    task_fields = {
        path.removeprefix("task_params."): field
        for path, field in fields.items()
        if path.startswith("task_params.")
    }

    assert result.data["task_type"] == _TASK_TYPE
    assert result.data["category"] == "Cloud"
    assert result.data["kind"] == "typed"
    assert set(task_fields) == _CANONICAL_FIELDS | {"entryPointArguments[]"}
    for field_name in _REQUIRED_FIELDS:
        assert task_fields[field_name]["required"] is True
    assert task_fields["isProduction"]["required"] is False
    assert task_fields["isProduction"]["default"] is False
    for excluded in _EXCLUDED_FIELDS | {"type", "codeType"}:
        assert excluded not in task_fields


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.2"])
def test_aliyun_serverless_spark_json_schema_is_closed_and_strict(
    version: str,
) -> None:
    task_params = _task_params_schema(version)
    properties = task_params["properties"]
    required = task_params["required"]
    assert isinstance(properties, dict)
    assert isinstance(required, list)

    assert task_params["additionalProperties"] is False
    assert set(properties) == _CANONICAL_FIELDS
    assert set(required) == _REQUIRED_FIELDS
    for field_name in _CANONICAL_FIELDS:
        field_schema = _resolved_field_schema(task_params, field_name)
        description = field_schema.get("description")
        assert isinstance(description, str)
        assert "not secret storage" in description.lower()
        assert "does not detect or redact" in description.lower()
    datasource = _resolved_field_schema(task_params, "datasource")
    datasource_variants = datasource["anyOf"]
    assert isinstance(datasource_variants, list)
    datasource_schemas = {
        variant["type"]: variant
        for variant in datasource_variants
        if isinstance(variant, dict) and isinstance(variant.get("type"), str)
    }
    assert set(datasource_schemas) == {"integer", "string"}
    assert datasource_schemas["integer"]["minimum"] == 1
    assert datasource_schemas["string"]["pattern"] == r"\S"
    for field_name in _LITERAL_FIELDS | {"entryPoint"}:
        field_schema = _resolved_field_schema(task_params, field_name)
        assert field_schema["type"] == "string"
        assert field_schema["minLength"] == 1
    arguments = _resolved_field_schema(task_params, "entryPointArguments")
    assert arguments["type"] == "array"
    assert arguments["minItems"] == 1
    argument_item = arguments["items"]
    assert isinstance(argument_item, dict)
    assert argument_item["type"] == "string"
    assert argument_item["minLength"] == 1
    production = _resolved_field_schema(task_params, "isProduction")
    assert production["type"] == "boolean"
    assert production["default"] is False


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.2"])
def test_aliyun_serverless_spark_summary_and_mappings_publish_the_narrow_model(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    summary = task_type_summary_data(_TASK_TYPE, catalog=catalog)
    result = task_type_schema_result(
        _TASK_TYPE,
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

    assert summary["task_type"] == _TASK_TYPE
    assert summary["category"] == "Cloud"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert set(summary["required_paths"]) == {
        "name",
        "type",
        "task_params",
        *{f"task_params.{field_name}" for field_name in _REQUIRED_FIELDS},
    }
    assert set(mappings) == {
        *(f"task_params.{field_name}" for field_name in _CANONICAL_FIELDS),
        "task_params.entryPointArguments[]",
    }
    for field_name in _CANONICAL_FIELDS:
        assert mappings[f"task_params.{field_name}"] == (
            f"taskDefinitionJson[].taskParams.{field_name}"
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_minimal_template_is_valid_and_compiles_exactly(
    version: str,
) -> None:
    yaml_text = _template_yaml(version)
    task = yaml.safe_load(yaml_text)
    assert isinstance(task, dict)
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == _TASK_TYPE
    assert set(params) == _CANONICAL_FIELDS
    assert params["isProduction"] is False
    assert isinstance(params["entryPointArguments"], list)
    assert params["entryPointArguments"]
    entry_point = params["entryPoint"]
    assert isinstance(entry_point, str)
    assert entry_point.startswith("oss://")
    normalized = get_task_authoring_catalog(version).normalize_task_params(
        _TASK_TYPE,
        cast("YamlObject", params),
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == params
    assert set(_compiled(version, normalized)) == _WIRE_FIELDS

    guidance = yaml_text.lower()
    for term in (
        "aliyun",
        "oss://",
        "datasource",
        "template",
        "retry",
        "duplicate",
        "failover",
        "cancel",
        "task params",
    ):
        assert term in guidance


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_aliyun_serverless_spark_normalize_encode_decode_and_create_are_exact(
    version: str,
    intent: TaskAuthoringIntent,
) -> None:
    canonical = _canonical(
        datasource=23,
        workspace_id="w-生产-analytics",
        resource_queue_id="root.analytics.v2",
        job_name="orders-v2 中文",
        entry_point="oss://analytics-jobs/releases/orders%20v2.jar",
        entry_point_arguments=["--date=2026-08-20", "--label=中文", "$()"],
        spark_submit_parameters=(
            "--class com.example.OrdersV2 --conf spark.executor.instances=4"
        ),
        is_production=True,
    )
    expected_native: YamlObject = {
        **canonical,
        "entryPointArguments": "--date=2026-08-20#--label=中文#$()",
        "type": _TASK_TYPE,
        "codeType": "JAR",
    }
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params(_TASK_TYPE, canonical, intent=intent)
    decoded, source = _decode(version, expected_native)

    assert normalized == canonical
    assert normalized is not canonical
    assert _encode(version, normalized) == expected_native
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING
    assert _compiled(version, canonical) == expected_native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_aliyun_serverless_spark_typed_patch_and_file_edit_emit_exact_batch_wire(
    version: str,
    input_mode: str,
) -> None:
    canonical = _canonical(
        datasource=29,
        job_name="edited-orders",
        entry_point_arguments=["--date", "2026-08-21", "--mode=replace"],
        is_production=True,
    )
    expected: YamlObject = {
        **canonical,
        "entryPointArguments": "--date#2026-08-21#--mode=replace",
        "type": _TASK_TYPE,
        "codeType": "JAR",
    }

    plan = _edit_plan(version, canonical, input_mode=input_mode)

    assert plan.merged_spec.tasks[0].task_params == canonical
    assert _compiled_from_plan(plan) == expected


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_defaults_production_false_and_always_emits_it(
    version: str,
) -> None:
    canonical = _canonical()
    canonical.pop("isProduction")

    normalized = get_task_authoring_catalog(version).normalize_task_params(
        _TASK_TYPE,
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == {**canonical, "isProduction": False}
    assert _encode(version, canonical)["isProduction"] is False
    assert _compiled(version, canonical)["isProduction"] is False


@pytest.mark.parametrize(
    "arguments",
    [
        ["--date", "2026-08-20"],
        ["--conf=spark.sql.shuffle.partitions=64"],
        ["中文 value", "literal $() and \\ spelling"],
    ],
)
def test_aliyun_serverless_spark_argument_lists_round_trip_without_rewriting(
    arguments: list[str],
) -> None:
    canonical = _canonical(entry_point_arguments=arguments)
    native = _encode("3.4.2", canonical)
    decoded, source = _decode("3.4.2", native)

    assert native["entryPointArguments"] == "#".join(arguments)
    assert decoded == canonical
    assert source is ProjectionSource.TYPED_AUTHORING


@pytest.mark.parametrize("field_name", sorted(_REQUIRED_FIELDS))
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_aliyun_serverless_spark_requires_every_owned_identity_field(
    field_name: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical()
    params.pop(field_name)

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=intent,
        )


@pytest.mark.parametrize("value", [0, -1, True, False, 1.0, "", "  ", None, [], {}])
def test_aliyun_serverless_spark_datasource_is_positive_id_or_nonblank_name(
    value: YamlValue,
) -> None:
    params = _canonical()
    params["datasource"] = value

    with pytest.raises(ValueError, match="datasource"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("value", [0, 1, "false", "true", None, [], {}])
def test_aliyun_serverless_spark_production_flag_is_one_strict_boolean(
    value: YamlValue,
) -> None:
    params = _canonical()
    params["isProduction"] = value

    with pytest.raises(ValueError, match="isProduction"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("field_name", sorted(_LITERAL_FIELDS))
@pytest.mark.parametrize("value", [None, 1, True, [], {}])
def test_aliyun_serverless_spark_literal_fields_reject_nonstrings(
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


@pytest.mark.parametrize("field_name", sorted(_LITERAL_FIELDS))
@pytest.mark.parametrize(
    ("value", "accepted"),
    [
        ("literal-v2", True),
        ("中文 value", True),
        (r"literal $() and \\ spelling", True),
        ("", False),
        (" ", False),
        (" leading", False),
        ("trailing ", False),
        ("${runtime}", False),
        ("$[yyyyMMdd]", False),
        ("line\nbreak", False),
        ("nul\x00byte", False),
        ("delete\x7fbyte", False),
        ("control\x85byte", False),
        ("surrogate\ud800", False),
    ],
)
def test_aliyun_serverless_spark_literal_json_schema_matches_runtime_validation(
    field_name: str,
    value: str,
    *,
    accepted: bool,
) -> None:
    assert _schema_pattern_accepts(field_name, value) is accepted
    assert _runtime_accepts(field_name, value) is accepted


@pytest.mark.parametrize(
    ("entry_point", "accepted"),
    [
        ("oss://analytics-jobs/jars/orders.jar", True),
        ("oss://analytics-jobs/releases/orders-artifact", True),
        ("oss://analytics-jobs/releases/orders%20v2.jar", True),
        ("oss://analytics-jobs/目录/orders.jar", True),
        ("oss://analytics-jobs/literal-$()-orders.jar", True),
        ("", False),
        ("oss://", False),
        ("oss://analytics-jobs", False),
        ("/local/jobs/orders.jar", False),
        ("https://analytics-jobs/orders.jar", False),
        ("oss://user@analytics-jobs/orders.jar", False),
        ("oss://analytics-jobs/orders.jar?version=2", False),
        ("oss://analytics-jobs/orders.jar#fragment", False),
        (" oss://analytics-jobs/orders.jar", False),
        ("oss://analytics-jobs/orders.jar ", False),
        ("oss://${bucket}/orders.jar", False),
        ("oss://analytics-jobs/$[yyyyMMdd]/orders.jar", False),
        ("oss://analytics-jobs/line\nbreak.jar", False),
        ("oss://analytics-jobs/control\x85.jar", False),
        ("oss://analytics-jobs/no\xa0break.jar", False),
        ("oss://analytics-jobs/zero\ufeffwidth.jar", False),
        ("oss://analytics-jobs/surrogate\ud800.jar", False),
    ],
)
def test_aliyun_serverless_spark_entry_point_schema_matches_safe_oss_runtime(
    entry_point: str,
    *,
    accepted: bool,
) -> None:
    assert _schema_pattern_accepts("entryPoint", entry_point) is accepted
    assert _runtime_accepts("entryPoint", entry_point) is accepted


@pytest.mark.parametrize("value", [None, "--date", 1, True, {}, ()])
def test_aliyun_serverless_spark_arguments_require_one_strict_list(
    value: YamlValue | tuple[()] | tuple[str, ...],
) -> None:
    params = _canonical()
    params["entryPointArguments"] = cast("YamlValue", value)

    with pytest.raises(ValueError, match="entryPointArguments"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("argument", "accepted"),
    [
        ("--safe", True),
        ("中文 value", True),
        (r"literal $() and \\ spelling", True),
        ("", False),
        (" ", False),
        (" leading", False),
        ("trailing ", False),
        ("contains#delimiter", False),
        ("${runtime}", False),
        ("$[yyyyMMdd]", False),
        ("line\nbreak", False),
        ("nul\x00byte", False),
        ("delete\x7fbyte", False),
        ("control\x85byte", False),
        ("surrogate\ud800", False),
    ],
)
def test_aliyun_serverless_spark_argument_item_schema_matches_runtime_validation(
    argument: str,
    *,
    accepted: bool,
) -> None:
    assert _argument_schema_pattern_accepts(argument) is accepted
    assert _runtime_accepts("entryPointArguments", [argument]) is accepted


@pytest.mark.parametrize("argument", [1, True, None, [], {}])
def test_aliyun_serverless_spark_arguments_reject_nonstring_items(
    argument: YamlValue,
) -> None:
    params = _canonical()
    params["entryPointArguments"] = ["--safe", argument]

    with pytest.raises(ValueError, match="entryPointArguments"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("engineReleaseVersion", "esr-2.1-native"),
        ("templateId", "template-managed-by-server"),
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("futureField", {"enabled": True}),
        ("type", _TASK_TYPE),
        ("codeType", "JAR"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_aliyun_serverless_spark_typed_authoring_rejects_unowned_fields(
    field_name: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical()
    params[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            params,
            intent=intent,
        )


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("datasource", True),
        ("workspaceId", "${workspace}"),
        ("entryPoint", "https://example.invalid/orders.jar"),
        ("entryPointArguments", ["--safe", "has#delimiter"]),
        ("isProduction", 1),
        ("engineReleaseVersion", "esr-2.1-native"),
        ("type", _TASK_TYPE),
        ("codeType", "JAR"),
    ],
)
def test_aliyun_serverless_spark_invalid_canonical_never_downgrades_to_opaque(
    input_mode: str,
    field_name: str,
    value: YamlValue,
) -> None:
    params = _canonical()
    params[field_name] = value
    catalog = get_task_authoring_catalog("3.4.2")

    for requested in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        assert (
            catalog.effective_authoring_intent(
                _TASK_TYPE,
                requested=requested,
                task_params=params,
            )
            is requested
        )
    with pytest.raises(ValueError, match=field_name):
        _compiled("3.4.2", params)
    with pytest.raises(UserInputError, match=field_name):
        _edit_plan("3.4.2", params, input_mode=input_mode)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
@pytest.mark.parametrize("code_type", ["PYTHON", "SQL"])
def test_aliyun_serverless_spark_recognized_non_jar_modes_are_explicit_opaque(
    version: str,
    code_type: str,
) -> None:
    native = _opaque_python_native()
    native["codeType"] = code_type
    catalog = get_task_authoring_catalog(version)

    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
            requested=TaskAuthoringIntent.TYPED_CREATE,
            task_params=native,
        )
        is TaskAuthoringIntent.OPAQUE_CREATE
    )
    assert (
        catalog.effective_authoring_intent(
            _TASK_TYPE,
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
    "native",
    [
        {**_opaque_python_native(), "type": "SPARK"},
        {**_opaque_python_native(), "type": "FUTURE"},
        {**_opaque_python_native(), "codeType": "FUTURE"},
        {**_opaque_python_native(), "codeType": "jar"},
        {**_native(), "engineReleaseVersion": "esr-2.1-native"},
    ],
)
def test_aliyun_serverless_spark_unknown_modes_and_jar_extras_never_fall_open(
    native: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog("3.4.2")

    for requested in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        assert (
            catalog.effective_authoring_intent(
                _TASK_TYPE,
                requested=requested,
                task_params=native,
            )
            is requested
        )
    with pytest.raises(ValueError):
        _compiled("3.4.2", native)
    for input_mode in ("patch", "file"):
        with pytest.raises(UserInputError):
            _edit_plan("3.4.2", native, input_mode=input_mode)


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_safe_and_opaque_native_have_exact_provenance(
    version: str,
) -> None:
    canonical = _canonical()
    safe, safe_source = _decode(version, _native())
    jar_extra = _jar_extra_native()
    decoded_extra, extra_source = _decode(version, jar_extra)
    opaque = _opaque_python_native()
    decoded_opaque, opaque_source = _decode(version, opaque)

    assert safe == canonical
    assert safe_source is ProjectionSource.TYPED_AUTHORING
    assert decoded_extra == jar_extra
    assert extra_source is ProjectionSource.OPAQUE_PRESERVE
    assert decoded_opaque == opaque
    assert opaque_source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError):
        _decode(version, jar_extra, source=ProjectionSource.TYPED_AUTHORING)


@pytest.mark.parametrize(
    "native",
    [
        {**_native(), "entryPointArguments": ""},
        {**_native(), "entryPointArguments": "--date##2026-08-20"},
        {**_native(), "datasource": True},
        {**_native(), "type": "SPARK"},
        {**_native(), "codeType": "jar"},
        {**_native(), "entryPoint": "https://example.invalid/orders.jar"},
        {**_native(), "workspaceId": "${workspace}"},
        {key: value for key, value in _native().items() if key != "isProduction"},
    ],
)
def test_aliyun_serverless_spark_near_safe_jar_wire_is_preserve_only(
    native: YamlObject,
) -> None:
    decoded, source = _decode("3.4.2", native)

    assert decoded == native
    assert source is ProjectionSource.OPAQUE_PRESERVE
    with pytest.raises(TaskParameterProjectionError):
        _decode("3.4.2", native, source=ProjectionSource.TYPED_AUTHORING)


@pytest.mark.parametrize(
    "intent", [TaskAuthoringIntent.OPAQUE_CREATE, TaskAuthoringIntent.OPAQUE_EDIT]
)
def test_aliyun_serverless_spark_jar_extras_have_no_raw_opaque_write_selector(
    intent: TaskAuthoringIntent,
) -> None:
    with pytest.raises(UnsupportedFeatureError, match="literal_jar_submit"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            _TASK_TYPE,
            _jar_extra_native(),
            intent=intent,
        )


@pytest.mark.parametrize(
    ("native", "exported", "source"),
    [
        (_native(), _canonical(), ProjectionSource.TYPED_AUTHORING),
        (
            _jar_extra_native(),
            _jar_extra_native(),
            ProjectionSource.OPAQUE_PRESERVE,
        ),
        (
            _opaque_python_native(),
            _opaque_python_native(),
            ProjectionSource.OPAQUE_PRESERVE,
        ),
    ],
)
def test_aliyun_serverless_spark_export_distinguishes_projection_provenance(
    native: YamlObject,
    exported: YamlObject,
    source: ProjectionSource,
) -> None:
    version = "3.4.2"
    dag = _fake_dag(native, workflow_name="aliyun-serverless-spark-export")
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

    assert baseline.projection_sources["submit-aliyun-serverless-spark-jar"] is source
    assert baseline.spec.tasks[0].task_params == exported
    assert document["tasks"][0]["task_params"] == exported


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    "native", [_native(), _jar_extra_native(), _opaque_python_native()]
)
def test_aliyun_serverless_spark_metadata_edits_preserve_exact_native_wire(
    native: YamlObject,
    input_mode: str,
) -> None:
    plan = _metadata_plan("3.4.2", native, input_mode=input_mode)

    assert _compiled_from_plan(plan) == native


@pytest.mark.parametrize(
    "native", [_native(), _jar_extra_native(), _opaque_python_native()]
)
def test_aliyun_serverless_spark_unchanged_update_preserves_exact_native_wire(
    native: YamlObject,
) -> None:
    version = "3.4.2"
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = workflow_authoring_catalog_for_version(version)
    baseline = workflow_live_baseline(
        _fake_dag(native, workflow_name="aliyun-serverless-spark-unchanged"),
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

    assert definition["taskExecuteType"] == "BATCH"
    assert json.loads(definition["taskParams"]) == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_aliyun_serverless_spark_opaque_preserve_is_a_deep_identity_copy(
    version: str,
) -> None:
    native = _jar_extra_native()
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(version).normalize_task_params(
        _TASK_TYPE,
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
    (
        "version",
        "runtime_epoch",
        "template_retry",
        "start_retry",
        "cancel_retry",
        "client_token",
        "failed_exit",
        "cancel_failure_propagated",
        "start_cause",
        "cancel_cause",
    ),
    [
        (
            "3.3.1",
            "legacy-single-submit",
            False,
            False,
            False,
            False,
            "KILL",
            False,
            False,
            False,
        ),
        (
            "3.3.2",
            "retry-client-token",
            True,
            True,
            True,
            True,
            "FAILURE",
            True,
            False,
            False,
        ),
        (
            "3.4.0",
            "retry-client-token",
            True,
            True,
            True,
            True,
            "FAILURE",
            True,
            False,
            False,
        ),
        (
            "3.4.1",
            "retry-client-token",
            True,
            True,
            True,
            True,
            "FAILURE",
            True,
            False,
            False,
        ),
        (
            "3.4.2",
            "retry-client-token-cause",
            True,
            True,
            True,
            True,
            "FAILURE",
            True,
            True,
            True,
        ),
    ],
)
def test_aliyun_serverless_spark_runtime_surface_locks_three_exact_epochs(
    version: str,
    runtime_epoch: str,
    *,
    template_retry: bool,
    start_retry: bool,
    cancel_retry: bool,
    client_token: bool,
    failed_exit: str,
    cancel_failure_propagated: bool,
    start_cause: bool,
    cancel_cause: bool,
) -> None:
    surface = cast(
        "_Surface", get_task_authoring_surface(version)
    ).aliyun_serverless_spark

    assert surface.available is True
    assert surface.runtime_epoch == runtime_epoch
    assert surface.template_fetch_always is True
    assert surface.template_may_mutate_submit_parameters is True
    assert surface.template_may_derive_release_and_fusion is True
    assert surface.retry_attempts == 11
    assert surface.retry_interval_ms == 1000
    assert surface.template_retry is template_retry
    assert surface.start_retry is start_retry
    assert surface.status_retry is True
    assert surface.cancel_retry is cancel_retry
    assert surface.client_token_within_attempt is client_token
    assert surface.failed_exit == failed_exit
    assert surface.start_exception_cause_preserved is start_cause
    assert surface.cancel_failure_propagated is cancel_failure_propagated
    assert surface.cancel_exception_cause_preserved is cancel_cause
    assert surface.task_params_logged is True
    assert surface.job_id_logged is True
    assert surface.state_logged is True
    assert surface.credentials_logged is False
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_may_duplicate is True
    assert surface.cancel_requires_in_memory_job_id is True
    assert surface.poll_interval_seconds == 10
    assert surface.poll_deadline is False
    assert surface.credential_source == "datasource"
    assert surface.credential_keys == (
        "accessKeyId",
        "accessKeySecret",
        "regionId",
    )
    assert surface.custom_endpoint_supported is True
    assert surface.default_endpoint_template == ("emr-serverless-spark.%s.aliyuncs.com")
    assert surface.connectivity_check_verified is False


@pytest.mark.parametrize("version", ["3.3.1", "3.3.2", "3.4.2"])
def test_aliyun_serverless_spark_guidance_discloses_remote_mutation_and_trust(
    version: str,
) -> None:
    guidance = _guidance(version)
    template = _template_yaml(version).lower()

    for term in (
        "aliyun",
        "oss://",
        "datasource",
        "accesskeyid",
        "accesskeysecret",
        "regionid",
        "endpoint",
        "emr-serverless-spark.%s.aliyuncs.com",
        "credential",
        "connectivity",
        "not verified",
        "template",
        "always",
        "mutate",
        "spark submit parameters",
        "display release",
        "fusion",
        "task params",
        "not secret storage",
        "does not detect or redact",
        "jobrunid",
        "state",
        "info",
        "result output",
        "durable",
        "callback",
        "failover",
        "retry",
        "duplicate",
        "cancel",
        "in-memory",
        "10 seconds",
        "deadline",
        "live evidence",
        "profile promotion",
    ):
        assert term in guidance
        assert term in template


def test_aliyun_serverless_spark_3_3_1_guidance_names_legacy_failure_behavior() -> None:
    guidance = _guidance("3.3.1")

    assert "poll" in guidance
    assert "11 attempts" in guidance
    assert "template fetch" in guidance
    assert "start" in guidance
    assert "not retried" in guidance
    assert "cancel failure" in guidance
    assert "logged" in guidance
    assert "swallowed" in guidance
    assert "failed" in guidance
    assert "kill" in guidance
    assert "client token" in guidance
    assert "does not" in guidance


@pytest.mark.parametrize("version", ["3.3.2", "3.4.0", "3.4.1"])
def test_aliyun_serverless_spark_middle_epoch_guidance_bounds_client_token(
    version: str,
) -> None:
    guidance = _guidance(version)

    assert "template" in guidance
    assert "start" in guidance
    assert "status" in guidance
    assert "cancel" in guidance
    assert "11 attempts" in guidance
    assert "1000" in guidance
    assert "client token" in guidance
    assert "one ds attempt" in guidance
    assert "new token" in guidance
    assert "ds retry" in guidance
    assert "cause" in guidance
    assert "not preserved" in guidance


def test_aliyun_serverless_spark_3_4_2_guidance_names_preserved_remote_causes() -> None:
    guidance = _guidance("3.4.2")

    assert "start" in guidance
    assert "cancel" in guidance
    assert "exception cause" in guidance
    assert "preserved" in guidance
