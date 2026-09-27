from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, Literal, cast

import pytest
import yaml
from tests.fakes import FakeDag, FakeEnumValue, FakeTaskDefinition, FakeWorkflow
from tests.services import _task_authoring_prep as authoring_prep

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

if TYPE_CHECKING:
    from dsctl.models import WorkflowSpec
    from dsctl.models.common import YamlObject, YamlValue


MlflowOpaqueMode = Literal[
    "basic_algorithm",
    "automl",
    "custom_project",
    "model_serve",
    "model_docker",
]

_MLFLOW_FACET = "MLFLOW/model_serve"
_MLFLOW_VERSIONS = (
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
_MLFLOW_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_MLFLOW_FINGERPRINTS = {
    "3.1.0": "sha256:42780524b4806652ac5bbdc3d15a13c8199a33cba4c57c95ab57af1cc6f08e65",
    "3.1.9": "sha256:42780524b4806652ac5bbdc3d15a13c8199a33cba4c57c95ab57af1cc6f08e65",
    "3.2.0": "sha256:4a05003e43ed091b457896b223a7f8fc463a54ac1eed39886a8837249b7dc1da",
    "3.2.1": "sha256:4a05003e43ed091b457896b223a7f8fc463a54ac1eed39886a8837249b7dc1da",
    "3.2.2": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
    "3.3.1": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
    "3.3.2": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
    "3.4.0": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
    "3.4.1": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
    "3.4.2": "sha256:986846a767b76d2291b371f3522161fde2a381daee2e28fe37fdcca77068e88f",
}
_OPAQUE_MODES: tuple[MlflowOpaqueMode, ...] = (
    "basic_algorithm",
    "automl",
    "custom_project",
    "model_serve",
    "model_docker",
)
_ALL_TYPED_FIELDS = {
    "mlflowTaskType",
    "deployType",
    "mlflowTrackingUri",
    "deployModelKey",
    "deployPort",
}


def _canonical_params() -> YamlObject:
    return {
        "mlflowTaskType": "MLflow Models",
        "deployType": "MLFLOW",
        "mlflowTrackingUri": "https://mlflow.example.com/api",
        "deployModelKey": "models:/fraud-detector/1",
        "deployPort": "7000",
    }


def _mlflow_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(ds_version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"mlflow-{ds_version}"},
            "tasks": [
                {
                    "name": "serve-registered-model",
                    "type": "MLFLOW",
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
        _mlflow_spec(task_params, ds_version=ds_version),
        catalog=catalog,
    )
    payload = prepared.materialize([31_900])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "MLFLOW"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _template_yaml() -> str:
    return authoring_prep.template_yaml("MLFLOW", "3.4.2", variant="minimal")


def _template_task() -> YamlObject:
    document = yaml.safe_load(_template_yaml())
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _opaque_native_params(mode: MlflowOpaqueMode) -> YamlObject:
    common: YamlObject = {
        "mlflowTrackingUri": "https://alice:secret@mlflow.example.com?token=native",
        "localParams": [
            {
                "prop": "runtime",
                "direct": "OUT",
                "type": "INTEGER",
                "value": "7",
            }
        ],
        "varPool": [{"prop": "runtime", "value": {"state": "ready"}}],
        "resourceList": [
            {
                "id": 91,
                "resourceName": "/native/mlflow.json",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "futureField": {"nested": ["native"]},
    }
    if mode == "basic_algorithm":
        return {
            **common,
            "mlflowTaskType": "MLflow Projects",
            "mlflowJobType": "BasicAlgorithm",
            "dataPath": "/data/iris data.csv",
            "algorithm": "xgboost",
            "params": "n_estimators=$(native)",
            "searchParams": "max_depth=[5, 10]",
            "experimentName": "native experiment",
            "modelName": "native-model",
            "registerModel": True,
        }
    if mode == "automl":
        return {
            **common,
            "mlflowTaskType": "MLflow Projects",
            "mlflowJobType": "AutoML",
            "dataPath": "/data/iris data.csv",
            "automlTool": "flaml",
            "params": "time_budget=$[yyyyMMdd]",
            "experimentName": "native automl",
            "modelName": "native-automl",
            "registerModel": True,
        }
    if mode == "custom_project":
        return {
            **common,
            "mlflowTaskType": "MLflow Projects",
            "mlflowJobType": "CustomProject",
            "mlflowProjectRepository": (
                "https://alice:secret@example.com/ml/project.git#examples/model"
            ),
            "mlflowProjectVersion": "${native_version}",
            "params": "-P learning_rate=0.2; touch /tmp/native",
            "experimentName": "native custom",
        }
    deploy_type = "MLFLOW" if mode == "model_serve" else "DOCKER"
    return {
        **common,
        "mlflowTaskType": "MLflow Models",
        "deployType": deploy_type,
        "deployModelKey": "models:/native/1;touch/tmp/native",
        "deployPort": "7000;touch/tmp/native",
    }


@pytest.mark.parametrize("ds_version", _MLFLOW_VERSIONS)
def test_mlflow_catalog_exposes_one_exact_model_serve_facet(ds_version: str) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("MLFLOW")
    membership = catalog.require_facet("MLFLOW", _MLFLOW_FACET)
    fact = catalog.task_type_facts["MLFLOW"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("MLFLOW") is True
    assert catalog.supports_opaque_authoring("MLFLOW") is True
    assert profile.category == "MachineLearning"
    assert profile.default_facet == _MLFLOW_FACET
    assert set(profile.facets) == {_MLFLOW_FACET}
    assert fact.semantic_fingerprint == _MLFLOW_FINGERPRINTS[ds_version]
    assert source_review is not None
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _MLFLOW_VERSIONS)
def test_mlflow_schema_exposes_only_the_model_serve_safe_surface(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "MLFLOW",
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

    assert result.data["task_type"] == "MLFLOW"
    assert result.data["category"] == "MachineLearning"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == _ALL_TYPED_FIELDS
    assert all(field["required"] is True for field in task_param_fields.values())
    assert task_param_fields["mlflowTaskType"]["choices"] == ["MLflow Models"]
    assert task_param_fields["deployType"]["choices"] == ["MLFLOW"]
    assert result.data["state_rules"] == []

    descriptions = " ".join(
        str(field.get("description", "")) for field in task_param_fields.values()
    ).lower()
    assert "mlflow" in descriptions
    assert "shell" in descriptions
    assert "worker" in descriptions
    assert "credential" in descriptions
    assert "foreground" in descriptions
    assert "failover" in descriptions
    model_key_description = task_param_fields["deployModelKey"]["description"]
    assert "exactly" in model_key_description
    assert "additional artifact subpaths" in model_key_description


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_mlflow_json_schema_is_closed_to_five_required_wire_fields(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "MLFLOW",
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
    assert set(task_params["required"]) == _ALL_TYPED_FIELDS
    assert set(properties) == _ALL_TYPED_FIELDS
    for field_name in (
        "localParams",
        "varPool",
        "resourceList",
        "registerModel",
        "mlflowJobType",
    ):
        assert field_name not in properties


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_mlflow_summary_and_compile_mappings_are_model_serve_only(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("MLFLOW", catalog=catalog)
    result = task_type_schema_result(
        "MLFLOW",
        compile_mappings=True,
        catalog=catalog,
    )
    assert isinstance(result.data, dict)
    task_param_mappings = {
        mapping["authoring_path"]: mapping["ds_payload_path"]
        for mapping in result.data["compile_mappings"]
        if isinstance(mapping, dict)
        and isinstance(mapping.get("authoring_path"), str)
        and mapping["authoring_path"].startswith("task_params.")
    }

    assert summary["task_type"] == "MLFLOW"
    assert summary["category"] == "MachineLearning"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    assert {f"task_params.{field}" for field in _ALL_TYPED_FIELDS}.issubset(
        summary["required_paths"]
    )
    assert set(task_param_mappings) == {
        f"task_params.{field_name}" for field_name in _ALL_TYPED_FIELDS
    }
    for authoring_path, payload_path in task_param_mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


def test_mlflow_minimal_template_is_safe_and_compiles_exactly() -> None:
    yaml_text = _template_yaml()
    task = _template_task()
    params = task["task_params"]
    assert isinstance(params, dict)

    assert task["type"] == "MLFLOW"
    assert params == _canonical_params()
    assert _compiled_task_params("3.4.2", params) == params

    guidance = yaml_text.lower()
    assert "posix" in guidance
    assert "mlflow" in guidance
    assert "foreground" in guidance
    assert "all interfaces" in guidance
    assert "credential" in guidance
    assert "failover" in guidance


@pytest.mark.parametrize("ds_version", _MLFLOW_VERSIONS)
def test_mlflow_typed_normalization_and_prepared_compile_are_exact_identity(
    ds_version: str,
) -> None:
    params = _canonical_params()
    catalog = get_task_authoring_catalog(ds_version)

    assert (
        catalog.normalize_task_params(
            "MLFLOW",
            params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )
        == params
    )
    assert (
        catalog.normalize_task_params(
            "MLFLOW",
            params,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        )
        == params
    )
    assert _compiled_task_params(ds_version, params) == params


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
@pytest.mark.parametrize("field", sorted(_ALL_TYPED_FIELDS))
def test_mlflow_typed_create_and_edit_require_all_five_wire_fields(
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params.pop(field)

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mlflowTaskType", ""),
        ("mlflowTaskType", "   "),
        ("mlflowTaskType", None),
        ("deployType", ""),
        ("deployType", "   "),
        ("deployType", None),
        ("mlflowTrackingUri", ""),
        ("mlflowTrackingUri", "   "),
        ("mlflowTrackingUri", None),
        ("deployModelKey", ""),
        ("deployModelKey", "   "),
        ("deployModelKey", None),
        ("deployPort", ""),
        ("deployPort", "   "),
        ("deployPort", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_typed_create_and_edit_reject_blank_or_null_required_values(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mlflowTaskType", "MLflow Projects"),
        ("mlflowTaskType", "mlflow models"),
        ("mlflowTaskType", "MLFLOW MODELS"),
        ("deployType", "DOCKER"),
        ("deployType", "mlflow"),
        ("deployType", "Mlflow"),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_typed_facet_rejects_projects_docker_and_inexact_literals(
    field: str,
    value: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "tracking_uri",
    [
        "http://127.0.0.1:5000",
        "https://mlflow.example.com",
        "https://mlflow.example.com:8443/api/v1",
        "https://mlflow.example.com/tenant/acme",
    ],
)
def test_mlflow_tracking_uri_accepts_safe_absolute_http_endpoints(
    tracking_uri: str,
) -> None:
    params = _canonical_params()
    params["mlflowTrackingUri"] = tracking_uri

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "MLFLOW",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "tracking_uri",
    [
        "mlflow.example.com:5000",
        "ftp://mlflow.example.com",
        "https:///missing-host",
        "https://alice:secret@mlflow.example.com",
        "https://mlflow.example.com?token=secret",
        "https://mlflow.example.com/#token",
        "https://mlflow.example.com/path with space",
        "https://mlflow.example.com;touch/tmp/owned",
        "https://mlflow.example.com/$(id)",
        "https://mlflow.example.com/${tenant}",
        "https://mlflow.example.com/$[yyyyMMdd]",
        "https://mlflow.example.com/`id`",
        "https://mlflow.example.com/\\value",
        'https://mlflow.example.com/"value',
        "https://mlflow.example.com/*",
        "https://mlflow.example.com/next\ncommand",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_tracking_uri_rejects_credentials_placeholders_and_shell_text(
    tracking_uri: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["mlflowTrackingUri"] = tracking_uri

    with pytest.raises(ValueError, match="mlflowTrackingUri"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "model_key",
    [
        "models:/fraud-detector/1",
        "models:/fraud_detector/Production",
        "models:/fraud.detector/17",
        "runs:/0123456789abcdef/model",
        "runs:/0123456789abcdef/artifacts/fraud-model",
    ],
)
def test_mlflow_model_key_accepts_portable_models_and_runs_paths(
    model_key: str,
) -> None:
    params = _canonical_params()
    params["deployModelKey"] = model_key

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "MLFLOW",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "model_key",
    [
        "model:/fraud/1",
        "s3://models/fraud/1",
        "models:/fraud",
        "runs:/0123456789abcdef",
        "models://fraud/1",
        "models:/fraud//1",
        "models:/../1",
        "models:/fraud/..",
        "models:/fraud/1/unexpected",
        "models:/fraud model/1",
        "models:/fraud/1;touch/tmp/owned",
        "models:/fraud/${version}",
        "runs:/$(id)/model",
        "runs:/run/*",
        "runs:/run/`id`",
        "runs:/run/model\nnext",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_model_key_rejects_bad_schemes_paths_and_shell_text(
    model_key: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["deployModelKey"] = model_key

    with pytest.raises(ValueError, match="deployModelKey"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("port", ["1", "7000", "65535"])
def test_mlflow_port_accepts_exact_decimal_wire_strings(port: str) -> None:
    params = _canonical_params()
    params["deployPort"] = port

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "MLFLOW",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "port",
    [
        0,
        7000,
        65536,
        "0",
        "65536",
        "-1",
        "+7000",
        " 7000",
        "7000 ",
        "7.0",
        "http",
        "$PORT",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_port_rejects_non_wire_types_and_out_of_range_values(
    port: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["deployPort"] = port

    with pytest.raises(ValueError, match="deployPort"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("localParams", []),
        ("varPool", []),
        ("resourceList", []),
        ("registerModel", True),
        ("mlflowJobType", "CustomProject"),
        ("mlflowProjectRepository", "https://example.com/project.git"),
        ("mlflowProjectVersion", "main"),
        ("automlTool", "flaml"),
        ("algorithm", "xgboost"),
        ("searchParams", "max_depth=[5, 10]"),
        ("dataPath", "/data/iris.csv"),
        ("params", "n_estimators=100"),
        ("experimentName", "experiment"),
        ("modelName", "model"),
        ("futureField", {"enabled": True}),
        ("mlflowJobType", None),
        ("registerModel", None),
        ("futureField", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_mlflow_typed_create_and_edit_reject_unowned_or_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "MLFLOW",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("ds_version", _MLFLOW_VERSIONS)
@pytest.mark.parametrize("mode", _OPAQUE_MODES)
@pytest.mark.parametrize(
    "intent",
    [
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
        TaskAuthoringIntent.OPAQUE_PRESERVE,
    ],
)
def test_mlflow_opaque_authoring_preserves_all_five_native_modes_exactly(
    ds_version: str,
    mode: MlflowOpaqueMode,
    intent: TaskAuthoringIntent,
) -> None:
    native = _opaque_native_params(mode)

    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "MLFLOW",
        native,
        intent=intent,
    )

    assert preserved == native
    assert preserved is not native


@pytest.mark.parametrize("mode", _OPAQUE_MODES)
def test_mlflow_opaque_preservation_is_a_deep_copy(mode: MlflowOpaqueMode) -> None:
    native = _opaque_native_params(mode)
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "MLFLOW",
        native,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")
    resources = native["resourceList"]
    assert isinstance(resources, list)
    resource = resources[0]
    assert isinstance(resource, dict)
    metadata = resource["futureMetadata"]
    assert isinstance(metadata, dict)
    metadata["checksum"] = "mutated"
    var_pool = native["varPool"]
    assert isinstance(var_pool, list)
    runtime = var_pool[0]
    assert isinstance(runtime, dict)
    runtime["value"] = {"state": "mutated"}

    assert preserved == expected


@pytest.mark.parametrize(
    "mode",
    ["basic_algorithm", "automl", "custom_project", "model_docker"],
)
def test_mlflow_public_create_and_edit_select_exact_reviewed_opaque_modes(
    mode: MlflowOpaqueMode,
) -> None:
    native = _opaque_native_params(mode)
    catalog = get_task_authoring_catalog("3.4.2")

    assert _compiled_task_params("3.4.2", native) == native

    document: YamlObject = {
        "workflow": {"name": f"opaque-{mode}"},
        "tasks": [
            {
                "name": "native-mlflow",
                "type": "MLFLOW",
                "task_params": native,
            }
        ],
    }
    validated = validate_workflow_document(
        document,
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_EDIT,
        ),
    )

    assert validated.tasks[0].task_params == native


def test_mlflow_public_create_does_not_downgrade_invalid_model_serve_to_opaque() -> (
    None
):
    native = _canonical_params()
    native["deployModelKey"] = "models:/unsafe/1;touch/tmp/native"

    with pytest.raises(ValueError, match="deployModelKey"):
        _compiled_task_params("3.4.2", native)


@pytest.mark.parametrize("ds_version", _MLFLOW_VERSIONS)
@pytest.mark.parametrize("mode", _OPAQUE_MODES)
def test_mlflow_opaque_export_and_unchanged_patch_round_trip_losslessly(
    ds_version: str,
    mode: MlflowOpaqueMode,
) -> None:
    native_params = _opaque_native_params(mode)
    task = FakeTaskDefinition(
        code=101,
        name=f"native-mlflow-{mode}",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="MLFLOW",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="mlflow-roundtrip",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
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


@pytest.mark.parametrize("ds_version", _MLFLOW_ABSENT_VERSIONS)
def test_mlflow_is_unavailable_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("MLFLOW") is False
    assert catalog.supports_opaque_authoring("MLFLOW") is False
    assert "MLFLOW" not in catalog.authoring_task_types
