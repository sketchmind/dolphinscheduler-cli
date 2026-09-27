from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import replace
from typing import TYPE_CHECKING, cast

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
    prepare_workflow_file_mutation_plan,
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


_JUPYTER_FACET = "JUPYTER/preinstalled_notebook"
_JUPYTER_VERSIONS = (
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
_JUPYTER_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
)
_JUPYTER_FINGERPRINTS = {
    "3.1.0": "sha256:0040b428155c875d74efe4139e66b6273ec0a624077dd8ab6f8614e821ba8282",
    "3.1.9": "sha256:0040b428155c875d74efe4139e66b6273ec0a624077dd8ab6f8614e821ba8282",
    "3.2.0": "sha256:788ad1420b1b0aa7743dc078ed02d5e21ecc77eb36f1f415d0c33d337f70cd40",
    "3.2.1": "sha256:788ad1420b1b0aa7743dc078ed02d5e21ecc77eb36f1f415d0c33d337f70cd40",
    "3.2.2": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
    "3.3.1": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
    "3.3.2": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
    "3.4.0": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
    "3.4.1": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
    "3.4.2": "sha256:d1e3250955a30fd3e535025da7034715a68858d648a69b18b53f4f2f665eea1d",
}
_REQUIRED_TYPED_FIELDS = {
    "condaEnvName",
    "inputNotePath",
    "outputNotePath",
}
_OPTIONAL_TYPED_FIELDS = {
    "kernel",
    "engine",
    "executionTimeout",
    "startTimeout",
}
_ALL_TYPED_FIELDS = _REQUIRED_TYPED_FIELDS | _OPTIONAL_TYPED_FIELDS | {"parameters"}
_SECRET_PARAMETER_NAMES = {
    "access_key",
    "api_key",
    "credential",
    "password",
    "passwd",
    "private_key",
    "secret",
    "token",
}
_EMPTY_REFS = TaskRefIndex.from_code_by_name({})


def _canonical_params(
    *,
    parameters: YamlObject | None = None,
    include_options: bool = False,
) -> YamlObject:
    task_params: YamlObject = {
        "condaEnvName": "analytics-py310",
        "inputNotePath": "/opt/notebooks/input.ipynb",
        "outputNotePath": "/opt/notebooks/output.ipynb",
        "parameters": {} if parameters is None else parameters,
    }
    if include_options:
        task_params.update(
            {
                "kernel": "python3",
                "engine": "nbclient",
                "executionTimeout": 900,
                "startTimeout": 60,
            }
        )
    return task_params


def _expected_native_params(canonical: YamlObject) -> YamlObject:
    native: YamlObject = {
        "condaEnvName": canonical["condaEnvName"],
        "inputNotePath": canonical["inputNotePath"],
        "outputNotePath": canonical["outputNotePath"],
    }
    parameters = canonical.get("parameters")
    if isinstance(parameters, dict) and parameters:
        native["parameters"] = json.dumps(
            parameters,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    for field_name in ("kernel", "engine"):
        value = canonical.get(field_name)
        if isinstance(value, str):
            native[field_name] = value
    for field_name in ("executionTimeout", "startTimeout"):
        value = canonical.get(field_name)
        if isinstance(value, int) and not isinstance(value, bool):
            native[field_name] = str(value)
    return native


def _jupyter_spec(task_params: YamlObject, *, ds_version: str) -> WorkflowSpec:
    catalog = get_task_authoring_catalog(ds_version)
    return validate_workflow_document(
        {
            "workflow": {"name": f"jupyter-{ds_version}"},
            "tasks": [
                {
                    "name": "execute-preinstalled-notebook",
                    "type": "JUPYTER",
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
        _jupyter_spec(task_params, ds_version=ds_version),
        catalog=catalog,
    )
    payload = prepared.materialize([31_700])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert prepared.required_task_code_count == 1
    assert definition["taskType"] == "JUPYTER"
    native_params = json.loads(definition["taskParams"])
    assert isinstance(native_params, dict)
    return cast("YamlObject", native_params)


def _encoded_task_params(task_params: YamlObject) -> YamlObject:
    projected = encode_task_parameters(
        version="3.4.2",
        task_type="JUPYTER",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _decoded_task_params(task_params: YamlObject) -> YamlObject:
    projected = decode_task_parameters(
        version="3.4.2",
        task_type="JUPYTER",
        task_params=cast("JsonObject", deepcopy(task_params)),
        refs=_EMPTY_REFS,
        source=ProjectionSource.TYPED_AUTHORING,
    )
    return cast("YamlObject", projected.task_params)


def _template_yaml(ds_version: str) -> str:
    return authoring_prep.template_yaml("JUPYTER", ds_version, variant="minimal")


def _template_task(ds_version: str) -> YamlObject:
    document = yaml.safe_load(_template_yaml(ds_version))
    assert isinstance(document, dict)
    return cast("YamlObject", document)


def _opaque_native_params(
    conda_env_name: str = "requirements.txt",
) -> YamlObject:
    return {
        "condaEnvName": conda_env_name,
        "inputNotePath": "/native/input notebook.ipynb",
        "outputNotePath": "/native/output notebook.ipynb",
        "parameters": '{"run date":"${native_day}"}',
        "kernel": "python 3",
        "engine": "nb client",
        "executionTimeout": "0",
        "startTimeout": "$NATIVE_TIMEOUT",
        "others": "--cwd /tmp; touch /tmp/native",
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
                "resourceName": f"/native/{conda_env_name}",
                "futureMetadata": {"checksum": "native"},
            }
        ],
        "futureField": {"nested": ["native"]},
    }


def _jupyter_edit_plan(
    ds_version: str,
    task_params: YamlObject,
    *,
    input_mode: str,
) -> WorkflowMutationPlan:
    baseline_params = _opaque_native_params()
    task = FakeTaskDefinition(
        code=101,
        name="native-jupyter",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="JUPYTER",
        task_params_value=json.dumps(baseline_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="jupyter-edit",
            project_code_value=7,
            project_name_value="analytics",
        ),
        task_definition_list_value=[task],
        workflow_task_relation_list_value=[],
    )
    project = ResolvedProject(code=7, name="analytics", description=None)
    catalog = get_task_authoring_catalog(ds_version)
    if input_mode == "patch":
        patch = WorkflowPatchDocument.model_validate(
            {
                "patch": {
                    "tasks": {
                        "update": [
                            {
                                "match": {"name": "native-jupyter"},
                                "set": {"task_params": task_params},
                            }
                        ]
                    }
                }
            }
        ).patch
        return prepare_workflow_mutation_plan(
            dag,
            project=project,
            patch=patch,
            release_state="OFFLINE",
            catalog=catalog,
        )
    desired = validate_workflow_document(
        {
            "workflow": {"name": "jupyter-edit", "project": "analytics"},
            "tasks": [
                {
                    "name": "native-jupyter",
                    "type": "JUPYTER",
                    "task_params": task_params,
                }
            ],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
        ),
    )
    return prepare_workflow_file_mutation_plan(
        dag,
        project=project,
        desired=desired,
        release_state="OFFLINE",
        catalog=catalog,
    )


def _resolved_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    properties = task_params_schema["properties"]
    assert isinstance(properties, dict)
    field_schema = properties[field_name]
    assert isinstance(field_schema, dict)
    return _resolve_schema_reference(task_params_schema, field_schema)


def _resolve_schema_reference(
    task_params_schema: dict[object, object],
    field_schema: dict[object, object],
) -> dict[object, object]:
    reference = field_schema.get("$ref")
    if not isinstance(reference, str):
        return field_schema
    definitions = task_params_schema["$defs"]
    assert isinstance(definitions, dict)
    resolved = definitions[reference.rsplit("/", maxsplit=1)[-1]]
    assert isinstance(resolved, dict)
    return resolved


def _non_null_property_schema(
    task_params_schema: dict[object, object],
    field_name: str,
) -> dict[object, object]:
    field_schema = _resolved_property_schema(task_params_schema, field_name)
    alternatives = field_schema.get("anyOf")
    if not isinstance(alternatives, list):
        return field_schema
    non_null = [
        alternative
        for alternative in alternatives
        if isinstance(alternative, dict) and alternative.get("type") != "null"
    ]
    assert len(non_null) == 1
    return _resolve_schema_reference(task_params_schema, non_null[0])


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
def test_jupyter_catalog_exposes_one_exact_preinstalled_notebook_facet(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    profile = catalog.require_task_type("JUPYTER")
    membership = catalog.require_facet("JUPYTER", _JUPYTER_FACET)
    fact = catalog.task_type_facts["JUPYTER"]
    source_review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("JUPYTER") is True
    assert catalog.supports_opaque_authoring("JUPYTER") is True
    assert profile.category == "MachineLearning"
    assert profile.default_facet == _JUPYTER_FACET
    assert set(profile.facets) == {_JUPYTER_FACET}
    assert fact.semantic_fingerprint == _JUPYTER_FINGERPRINTS[ds_version]
    assert source_review is not None
    assert source_review.review == ("jupyter-preinstalled-notebook-shell-safe-subset")
    assert source_review.semantic_fingerprint == fact.semantic_fingerprint
    assert membership.contract.review == source_review.review
    assert membership.profile_version == ds_version
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
def test_jupyter_schema_exposes_only_the_shell_safe_preinstalled_subset(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "JUPYTER",
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

    assert result.data["task_type"] == "JUPYTER"
    assert result.data["category"] == "MachineLearning"
    assert result.data["kind"] == "typed"
    assert set(task_param_fields) == _ALL_TYPED_FIELDS
    for field_name in _REQUIRED_TYPED_FIELDS:
        assert task_param_fields[field_name]["required"] is True
    assert task_param_fields["parameters"]["required"] is False
    assert task_param_fields["parameters"]["default"] == {}
    for field_name in _OPTIONAL_TYPED_FIELDS:
        assert task_param_fields[field_name]["required"] is False
    for field_name in _ALL_TYPED_FIELDS:
        assert task_param_fields[field_name]["compile_path"].endswith(
            f"taskParams.{field_name}"
        )
    assert result.data["state_rules"] == []

    descriptions = " ".join(
        str(field.get("description", "")) for field in task_param_fields.values()
    ).lower()
    assert "preinstalled" in descriptions
    assert "papermill" in descriptions
    assert "shell" in descriptions
    assert ".ipynb" in descriptions
    assert "opaque" in descriptions
    assert "logged" in descriptions
    assert "does not redact" in descriptions
    assert "failover" in descriptions


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.0", "3.4.2"])
def test_jupyter_json_schema_matches_the_closed_safe_projection(
    ds_version: str,
) -> None:
    result = task_type_schema_result(
        "JUPYTER",
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
    assert set(task_params["required"]) == _REQUIRED_TYPED_FIELDS
    assert set(properties) == _ALL_TYPED_FIELDS

    for field_name in ("condaEnvName", "inputNotePath", "outputNotePath"):
        field_schema = _resolved_property_schema(task_params, field_name)
        assert field_schema["type"] == "string"
        assert field_schema["minLength"] == 1
        pattern = field_schema["pattern"]
        assert isinstance(pattern, str)
        assert re.search(pattern, "$(id)") is None
        assert re.search(pattern, "unsafe value") is None
    conda_pattern = _resolved_property_schema(task_params, "condaEnvName")["pattern"]
    assert isinstance(conda_pattern, str)
    assert re.search(conda_pattern, "analytics-py310") is not None
    assert re.search(conda_pattern, "requirements.txt") is None
    assert re.search(conda_pattern, "analytics.tar.gz") is None

    for field_name in ("inputNotePath", "outputNotePath"):
        notebook_pattern = _resolved_property_schema(task_params, field_name)["pattern"]
        assert isinstance(notebook_pattern, str)
        assert re.search(notebook_pattern, "/notebooks/input.ipynb") is not None
        assert re.search(notebook_pattern, "notebooks/input.ipynb") is None
        assert re.search(notebook_pattern, "notebooks/input.py") is None

    parameters_schema = _resolved_property_schema(task_params, "parameters")
    assert parameters_schema["type"] == "object"
    assert parameters_schema["default"] == {}
    key_schema = parameters_schema["propertyNames"]
    assert isinstance(key_schema, dict)
    key_pattern = key_schema["pattern"]
    assert isinstance(key_pattern, str)
    assert re.search(key_pattern, "business_date") is not None
    assert re.search(key_pattern, "business date") is None
    secret_exclusion = key_schema["not"]
    assert isinstance(secret_exclusion, dict)
    assert set(secret_exclusion["enum"]) == _SECRET_PARAMETER_NAMES
    for exact_secret_name in _SECRET_PARAMETER_NAMES:
        assert re.search(key_pattern, exact_secret_name) is None
        assert re.search(key_pattern, exact_secret_name.upper()) is None
    for ordinary_name in ("tokenizer", "password_policy", "secretariat"):
        assert re.search(key_pattern, ordinary_name) is not None
        assert ordinary_name not in secret_exclusion["enum"]
    value_schema = parameters_schema["additionalProperties"]
    assert isinstance(value_schema, dict)
    value_pattern = value_schema["pattern"]
    assert isinstance(value_pattern, str)
    assert re.search(value_pattern, "2026-08-20") is not None
    assert re.search(value_pattern, "$(id)") is None
    assert re.search(value_pattern, "unsafe value") is None
    assert re.search(value_pattern, "https://alice:secret@example.com/path") is None

    for field_name in ("kernel", "engine"):
        option_schema = _non_null_property_schema(task_params, field_name)
        assert option_schema["type"] == "string"
        option_pattern = option_schema["pattern"]
        assert isinstance(option_pattern, str)
        assert re.search(option_pattern, "python3") is not None
        assert re.search(option_pattern, "python 3") is None
    for field_name in ("executionTimeout", "startTimeout"):
        timeout_schema = _non_null_property_schema(task_params, field_name)
        assert timeout_schema["type"] == "integer"
        assert timeout_schema["exclusiveMinimum"] == 0

    for excluded_field in (
        "others",
        "resourceList",
        "localParams",
        "varPool",
        "futureField",
    ):
        assert excluded_field not in properties


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.2", "3.4.2"])
def test_jupyter_summary_and_compile_mappings_cover_the_exact_typed_surface(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)
    summary = task_type_summary_data("JUPYTER", catalog=catalog)
    result = task_type_schema_result(
        "JUPYTER",
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

    assert summary["task_type"] == "JUPYTER"
    assert summary["category"] == "MachineLearning"
    assert summary["kind"] == "typed"
    assert "default_variant" not in summary
    assert summary["variants"] == []
    expected_required_paths = {
        "name",
        "type",
        "task_params",
        *(f"task_params.{field_name}" for field_name in _REQUIRED_TYPED_FIELDS),
    }
    assert set(summary["required_paths"]) == expected_required_paths
    assert set(task_param_mappings) == {
        f"task_params.{field_name}" for field_name in _ALL_TYPED_FIELDS
    }
    for authoring_path, payload_path in task_param_mappings.items():
        field_name = authoring_path.removeprefix("task_params.")
        assert payload_path == f"taskDefinitionJson[].taskParams.{field_name}"


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
def test_jupyter_minimal_template_is_canonical_and_compiles_to_minimal_wire(
    ds_version: str,
) -> None:
    yaml_text = _template_yaml(ds_version)
    task = _template_task(ds_version)
    params = task["task_params"]
    assert isinstance(params, dict)
    expected = _canonical_params()

    assert task["type"] == "JUPYTER"
    assert params == expected
    assert _compiled_task_params(ds_version, params) == _expected_native_params(
        expected
    )

    guidance = yaml_text.lower().replace("-", " ")
    assert "preinstalled" in guidance
    assert "papermill" in guidance
    assert "shell" in guidance
    assert ".ipynb" in guidance
    assert "opaque" in guidance


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
def test_jupyter_normalization_projector_and_compile_select_minimal_wire(
    ds_version: str,
) -> None:
    params_without_default = _canonical_params()
    params_without_default.pop("parameters")
    catalog = get_task_authoring_catalog(ds_version)

    for intent in (
        TaskAuthoringIntent.TYPED_CREATE,
        TaskAuthoringIntent.TYPED_EDIT,
    ):
        normalized = catalog.normalize_task_params(
            "JUPYTER",
            params_without_default,
            intent=intent,
        )
        assert normalized == _canonical_params()
        assert normalized is not params_without_default

    canonical = _canonical_params()
    expected_native = _expected_native_params(canonical)
    assert _encoded_task_params(canonical) == expected_native
    assert _decoded_task_params(expected_native) == canonical
    assert _compiled_task_params(ds_version, canonical) == expected_native
    assert "parameters" not in expected_native


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
def test_jupyter_options_and_literal_parameters_project_exactly(
    ds_version: str,
) -> None:
    parameters: YamlObject = {
        "zeta": "last",
        "business_date": "2026-08-20",
        "source_uri": "s3://analytics-bucket/daily",
    }
    canonical = _canonical_params(parameters=parameters, include_options=True)
    expected_native = _expected_native_params(canonical)
    catalog = get_task_authoring_catalog(ds_version)

    normalized = catalog.normalize_task_params(
        "JUPYTER",
        canonical,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )
    assert normalized == canonical
    assert _encoded_task_params(canonical) == expected_native
    assert _decoded_task_params(expected_native) == canonical
    assert _compiled_task_params(ds_version, canonical) == expected_native

    raw_parameters = expected_native["parameters"]
    assert raw_parameters == (
        '{"business_date":"2026-08-20",'
        '"source_uri":"s3://analytics-bucket/daily","zeta":"last"}'
    )
    assert expected_native["executionTimeout"] == "900"
    assert expected_native["startTimeout"] == "60"


@pytest.mark.parametrize("field", sorted(_REQUIRED_TYPED_FIELDS))
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_typed_create_and_edit_require_the_three_native_fields(
    field: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params.pop(field)

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize("field", sorted(_REQUIRED_TYPED_FIELDS))
@pytest.mark.parametrize("value", ["", "   ", None])
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_required_fields_reject_blank_or_null_values(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "conda_env_name",
    ["analytics-py310", "analytics_py310", "py3.11", "env.cuda-12"],
)
def test_jupyter_conda_environment_accepts_preinstalled_safe_names(
    conda_env_name: str,
) -> None:
    params = _canonical_params()
    params["condaEnvName"] = conda_env_name

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "conda_env_name",
    [
        "requirements.txt",
        "Requirements.TXT",
        "analytics.tar.gz",
        "PACKED.TAR.GZ",
        "/opt/envs/analytics",
        "../analytics",
        "-analytics",
        "analytics env",
        "analytics;touch/tmp/owned",
        "analytics&&id",
        "$(id)",
        "${CONDA_ENV}",
        "$[yyyyMMdd]",
        "`id`",
        'analytics"env',
        "analytics\\env",
        "analytics*",
        "analytics\nnext",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_conda_environment_rejects_creation_modes_and_shell_text(
    conda_env_name: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["condaEnvName"] = conda_env_name

    with pytest.raises(ValueError, match="condaEnvName"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("input_path", "output_path"),
    [
        ("/input.ipynb", "/output.ipynb"),
        ("/notebooks/input.ipynb", "/notebooks/output.ipynb"),
        ("/var/lib/notebooks/input.ipynb", "/srv/notebooks/output.ipynb"),
        ("/opt/notebooks/input.ipynb", "/opt/notebooks/output.ipynb"),
    ],
)
def test_jupyter_notebook_paths_accept_distinct_shell_safe_ipynb_files(
    input_path: str,
    output_path: str,
) -> None:
    params = _canonical_params()
    params["inputNotePath"] = input_path
    params["outputNotePath"] = output_path

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize("field", ["inputNotePath", "outputNotePath"])
@pytest.mark.parametrize(
    "notebook_path",
    [
        "notebooks/input.py",
        "notebooks/input.ipynb",
        "../notebooks/input.ipynb",
        "/opt/../notebooks/input.ipynb",
        "/opt/./notebooks/input.ipynb",
        "notebooks/input.IPYNB",
        "notebooks/input.ipynb.bak",
        "-input.ipynb",
        "notebooks/input notebook.ipynb",
        "notebooks/input.ipynb;touch/tmp/owned",
        "notebooks/$(id).ipynb",
        "notebooks/${name}.ipynb",
        "notebooks/$[yyyyMMdd].ipynb",
        "notebooks/`id`.ipynb",
        'notebooks/input".ipynb',
        "notebooks/input\\name.ipynb",
        "notebooks/*.ipynb",
        "notebooks/input.ipynb\nnext",
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_notebook_paths_reject_non_notebooks_and_shell_text(
    field: str,
    notebook_path: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = notebook_path

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "notebook_path",
    ["/same.ipynb", "/notebooks/same.ipynb", "/opt/notebooks/same.ipynb"],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_input_and_output_notebook_paths_must_be_distinct(
    notebook_path: str,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["inputNotePath"] = notebook_path
    params["outputNotePath"] = notebook_path

    with pytest.raises(ValueError, match="outputNotePath"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("kernel", "python3"),
        ("kernel", "python-3.11"),
        ("engine", "nbclient"),
        ("engine", "nbconvert_v7"),
    ],
)
def test_jupyter_kernel_and_engine_accept_one_safe_shell_token(
    field: str,
    value: str,
) -> None:
    params = _canonical_params()
    params[field] = value

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize("field", ["kernel", "engine"])
@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "-help",
        "python 3",
        "python;id",
        "python&&id",
        "$(id)",
        "${KERNEL}",
        "$[yyyyMMdd]",
        "`id`",
        'python"3',
        "python\\3",
        "python*",
        "python\nnext",
        3,
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_kernel_and_engine_reject_blank_non_string_or_shell_text(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


def test_jupyter_explicit_null_options_normalize_to_omission() -> None:
    params = _canonical_params()
    params.update(
        {
            "kernel": None,
            "engine": None,
            "executionTimeout": None,
            "startTimeout": None,
        }
    )

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == _canonical_params()


@pytest.mark.parametrize(
    "parameters",
    [
        {},
        {"business_date": "2026-08-20"},
        {"source_uri": "s3://analytics-bucket/daily", "mode": "approved"},
    ],
)
def test_jupyter_parameters_accept_literal_shell_safe_string_maps(
    parameters: YamlObject,
) -> None:
    params = _canonical_params(parameters=parameters)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize(
    "parameters",
    [
        [],
        {"limit": 7},
        {7: "value"},
        {"business date": "2026-08-20"},
        {"-name": "value"},
        {"password": "visible"},
        {"PASSWD": "visible"},
        {"token": "visible"},
        {"api_key": "visible"},
        {"access_key": "visible"},
        {"private_key": "visible"},
        {"credential": "visible"},
        {"secret": "visible"},
        {"name": ""},
        {"name": "unsafe value"},
        {"name": "-value"},
        {"name": "value;id"},
        {"name": "value&&id"},
        {"name": "$(id)"},
        {"name": "${value}"},
        {"name": "$[yyyyMMdd]"},
        {"name": "`id`"},
        {"name": 'value"next'},
        {"name": "value\\next"},
        {"name": "value*"},
        {"name": "value\nnext"},
        {"name": "https://alice:secret@example.com/path"},
        {"name": "s3://access:secret@bucket/path"},
        None,
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_parameters_reject_non_strings_options_and_shell_text(
    parameters: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params["parameters"] = parameters

    with pytest.raises(ValueError, match="parameters"):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    "parameter_name",
    [
        "tokenizer",
        "password_policy",
        "secretariat",
        "credentials_enabled",
        "api_key_version",
        "access_key_name",
    ],
)
def test_jupyter_secret_name_denylist_is_exact_not_substring_based(
    parameter_name: str,
) -> None:
    parameters: YamlObject = {parameter_name: "nonsecret-value"}
    params = _canonical_params(parameters=parameters)

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params


@pytest.mark.parametrize("field", ["executionTimeout", "startTimeout"])
@pytest.mark.parametrize("timeout", [1, 60, 86_400])
def test_jupyter_timeouts_accept_strict_positive_integers(
    field: str,
    timeout: int,
) -> None:
    params = _canonical_params()
    params[field] = timeout

    normalized = get_task_authoring_catalog("3.4.2").normalize_task_params(
        "JUPYTER",
        params,
        intent=TaskAuthoringIntent.TYPED_CREATE,
    )

    assert normalized == params
    assert _encoded_task_params(params)[field] == str(timeout)


@pytest.mark.parametrize("field", ["executionTimeout", "startTimeout"])
@pytest.mark.parametrize(
    "timeout",
    [0, -1, True, False, 1.0, "1", "+1", " 1", "1 ", "$TIMEOUT"],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_timeouts_reject_non_positive_or_non_integer_values(
    field: str,
    timeout: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = timeout

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("others", "--cwd /tmp"),
        ("resourceList", []),
        ("localParams", []),
        ("varPool", []),
        ("futureField", {"enabled": True}),
        ("others", None),
        ("resourceList", None),
        ("localParams", None),
        ("varPool", None),
        ("futureField", None),
    ],
)
@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_jupyter_typed_authoring_rejects_unowned_native_and_future_fields(
    field: str,
    value: YamlValue,
    intent: TaskAuthoringIntent,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        get_task_authoring_catalog("3.4.2").normalize_task_params(
            "JUPYTER",
            params,
            intent=intent,
        )


def test_jupyter_typed_projector_rejects_unowned_fields_in_both_directions() -> None:
    canonical = _canonical_params()
    canonical["futureField"] = {"enabled": True}
    native = _expected_native_params(_canonical_params())
    native["futureField"] = {"enabled": True}

    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _encoded_task_params(canonical)
    with pytest.raises(TaskParameterProjectionError, match="futureField"):
        _decoded_task_params(native)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("parameters", "not-json"),
        ("parameters", "[]"),
        ("parameters", '{"limit":7}'),
        ("executionTimeout", "0"),
        ("startTimeout", "$TIMEOUT"),
    ],
)
def test_jupyter_typed_decode_rejects_malformed_or_unsafe_native_values(
    field: str,
    value: YamlValue,
) -> None:
    native = _expected_native_params(_canonical_params())
    native[field] = value

    with pytest.raises(TaskParameterProjectionError, match=field):
        _decoded_task_params(native)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("others", "--cwd /srv/notebooks"),
        ("futureField", {"native": True}),
    ],
)
def test_jupyter_invalid_canonical_create_does_not_downgrade_to_opaque(
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(ValueError, match=field):
        _compiled_task_params("3.4.2", params)


@pytest.mark.parametrize("input_mode", ["patch", "file"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("others", "--cwd /srv/notebooks"),
        ("futureField", {"native": True}),
    ],
)
def test_jupyter_invalid_canonical_edits_do_not_downgrade_to_opaque(
    input_mode: str,
    field: str,
    value: YamlValue,
) -> None:
    params = _canonical_params()
    params[field] = value

    with pytest.raises(UserInputError, match=field):
        _jupyter_edit_plan("3.4.2", params, input_mode=input_mode)


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize("conda_env_name", ["requirements.txt", "packed.tar.gz"])
def test_jupyter_public_create_selects_only_exact_native_bootstrap_modes(
    ds_version: str,
    conda_env_name: str,
) -> None:
    native = _opaque_native_params(conda_env_name)

    assert _compiled_task_params(ds_version, native) == native


@pytest.mark.parametrize("ds_version", ["3.1.0", "3.2.1", "3.4.2"])
@pytest.mark.parametrize("conda_env_name", ["requirements.txt", "packed.tar.gz"])
@pytest.mark.parametrize("input_mode", ["patch", "file"])
def test_jupyter_public_edits_select_only_exact_native_bootstrap_modes(
    ds_version: str,
    conda_env_name: str,
    input_mode: str,
) -> None:
    native = _opaque_native_params(conda_env_name)
    native["futureField"] = {"edited": ["still-native"]}

    plan = _jupyter_edit_plan(ds_version, native, input_mode=input_mode)
    payload = plan.compilation.materialize([])
    compiled = json.loads(json.loads(payload["taskDefinitionJson"])[0]["taskParams"])

    assert compiled == native


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
@pytest.mark.parametrize(
    "conda_env_name",
    ["requirements.txt", "packed.tar.gz"],
)
@pytest.mark.parametrize(
    "intent",
    [
        TaskAuthoringIntent.OPAQUE_CREATE,
        TaskAuthoringIntent.OPAQUE_EDIT,
        TaskAuthoringIntent.OPAQUE_PRESERVE,
    ],
)
def test_jupyter_opaque_native_runtime_and_future_shapes_are_deep_copied(
    ds_version: str,
    conda_env_name: str,
    intent: TaskAuthoringIntent,
) -> None:
    native = _opaque_native_params(conda_env_name)
    expected = deepcopy(native)

    preserved = get_task_authoring_catalog(ds_version).normalize_task_params(
        "JUPYTER",
        native,
        intent=intent,
    )

    assert preserved == expected
    assert preserved is not native
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
    future = native["futureField"]
    assert isinstance(future, dict)
    nested = future["nested"]
    assert isinstance(nested, list)
    nested.append("mutated")

    assert preserved == expected


@pytest.mark.parametrize("ds_version", _JUPYTER_VERSIONS)
@pytest.mark.parametrize("conda_env_name", ["requirements.txt", "packed.tar.gz"])
def test_jupyter_opaque_export_and_unchanged_patch_round_trip_losslessly(
    ds_version: str,
    conda_env_name: str,
) -> None:
    native_params = _opaque_native_params(conda_env_name)
    task = FakeTaskDefinition(
        code=101,
        name="native-jupyter",
        project_code_value=7,
        project_name_value="analytics",
        task_type_value="JUPYTER",
        task_params_value=json.dumps(native_params),
        worker_group_value="default",
    )
    if ds_version in {"3.2.0", "3.2.1", "3.2.2"}:
        task = replace(task, is_cache_value=FakeEnumValue("NO"))
    dag = FakeDag(
        workflow_definition_value=FakeWorkflow(
            code=11,
            name="jupyter-roundtrip",
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


@pytest.mark.parametrize("ds_version", _JUPYTER_ABSENT_VERSIONS)
def test_jupyter_is_unavailable_before_its_exact_upstream_release(
    ds_version: str,
) -> None:
    catalog = get_task_authoring_catalog(ds_version)

    assert catalog.supports_typed_authoring("JUPYTER") is False
    assert catalog.supports_opaque_authoring("JUPYTER") is False
    assert "JUPYTER" not in catalog.authoring_task_types
