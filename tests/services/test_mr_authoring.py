from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject, YamlValue
    from dsctl.support.json_types import JsonObject

_NO_TASK_REFS = TaskRefIndex.from_code_by_name({})
_CANONICAL_MR_PARAMS: JsonObject = {
    "mainJar": "/jobs/wordcount.jar",
    "mainClass": "com.example.WordCount",
    "mainArgs": ["hdfs:///input", "hdfs:///output"],
}
_MODERN_RESOURCE_REFS = TaskResourceRefIndex.from_resolved_files(
    ["/jobs/wordcount.jar"],
    id_by_full_name={},
    wire_full_name_by_full_name={
        "/jobs/wordcount.jar": "/tenant/resources/jobs/wordcount.jar"
    },
)
_MODERN_MR_NATIVE_PARAMS: JsonObject = {
    "localParams": [],
    "mainJar": {"resourceName": "/tenant/resources/jobs/wordcount.jar"},
    "mainClass": "com.example.WordCount",
    "mainArgs": "hdfs:///input hdfs:///output",
    "others": "",
    "appName": "",
    "yarnQueue": "",
    "resourceList": [],
    "programType": "JAVA",
}
_LEGACY_MR_NATIVE_PARAMS: JsonObject = {
    "localParams": [],
    "mainJar": {"id": 731},
    "mainClass": "com.example.WordCount",
    "mainArgs": "hdfs:///input hdfs:///output",
    "others": "",
    "appName": "",
    "resourceList": [],
    "programType": "JAVA",
}
_MODERN_MR_VERSIONS = (
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


def test_mr_schema_exposes_one_closed_java_jar_intent() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    result = task_type_schema_result("MR", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert catalog.supports_typed_authoring("MR") is True
    assert {path for path in fields if path.startswith("task_params.")} == {
        "task_params.mainJar",
        "task_params.mainClass",
        "task_params.mainArgs",
        "task_params.mainArgs[]",
    }
    assert fields["task_params.mainJar"]["choice_source"] == "dsctl resource list"
    assert (
        fields["task_params.mainJar"]["choice_value"]
        == "fullName relative to the FILE root, retaining one leading slash"
    )
    assert fields["task_params.mainArgs"]["default"] == []


def test_mr_json_schema_is_closed_and_matches_the_literal_runtime_subset() -> None:
    result = task_type_schema_result(
        "MR",
        json_schema=True,
        catalog=get_task_authoring_catalog("3.4.1"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["mainJar", "mainClass"]
    assert set(task_params["properties"]) == {
        "mainJar",
        "mainClass",
        "mainArgs",
    }
    assert task_params["properties"]["mainArgs"]["default"] == []


@pytest.mark.parametrize(
    "task_params",
    [
        {"mainJar": "/jobs/wordcount.jar"},
        {"mainJar": "jobs/wordcount.jar", "mainClass": "example.WordCount"},
        {"mainJar": "/jobs/../wordcount.jar", "mainClass": "example.WordCount"},
        {"mainJar": "/jobs/${jar}.jar", "mainClass": "example.WordCount"},
        {"mainJar": "/jobs/wordcount.zip", "mainClass": "example.WordCount"},
        {"mainJar": "/jobs/wordcount.jar", "mainClass": "example..WordCount"},
        {"mainJar": "/jobs/wordcount.jar", "mainClass": "example.Job$Inner"},
        {
            "mainJar": "/jobs/wordcount.jar",
            "mainClass": "example.WordCount",
            "mainArgs": "hdfs:///input",
        },
        {
            "mainJar": "/jobs/wordcount.jar",
            "mainClass": "example.WordCount",
            "mainArgs": ["two words"],
        },
        {
            "mainJar": "/jobs/wordcount.jar",
            "mainClass": "example.WordCount",
            "mainArgs": ["$(id)"],
        },
        {
            "mainJar": "/jobs/wordcount.jar",
            "mainClass": "example.WordCount",
            "others": "-Dmapreduce.job.reduces=4",
        },
    ],
)
def test_mr_typed_model_rejects_unowned_or_shell_unsafe_input(
    task_params: YamlObject,
) -> None:
    with pytest.raises(ValueError):
        get_task_authoring_catalog("3.4.1").normalize_task_params(
            "MR",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", TARGET_DS_VERSIONS)
def test_mr_minimal_template_validates_on_every_reviewed_profile(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result("MR", catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(result.data["yaml"])

    spec = validate_workflow_document(
        {
            "workflow": {"name": f"mr-template-{version}"},
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    assert spec.tasks[0].task_params == _CANONICAL_MR_PARAMS


@pytest.mark.parametrize("version", _MODERN_MR_VERSIONS)
def test_mr_modern_workflow_compiles_the_exact_java_resource_name_wire(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": "mr-modern"},
            "tasks": [
                {
                    "name": "wordcount",
                    "type": "MR",
                    "task_params": cast("YamlValue", _CANONICAL_MR_PARAMS),
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
    ).materialize([51_001], resource_refs=_MODERN_RESOURCE_REFS)
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert definition["taskType"] == "MR"
    assert json.loads(definition["taskParams"]) == _MODERN_MR_NATIVE_PARAMS


@pytest.mark.parametrize("version", _MODERN_MR_VERSIONS)
def test_mr_modern_exact_wire_round_trips_as_typed_authoring(version: str) -> None:
    exported = decode_task_parameters_with_provenance(
        version=version,
        task_type="MR",
        task_params=_MODERN_MR_NATIVE_PARAMS,
        refs=_NO_TASK_REFS,
        resource_refs=_MODERN_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported.task.task_params == _CANONICAL_MR_PARAMS
    assert (
        encode_task_parameters(
            version=version,
            task_type="MR",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            resource_refs=_MODERN_RESOURCE_REFS,
            source=exported.reencode_source,
        ).task_params
        == _MODERN_MR_NATIVE_PARAMS
    )


def test_mr_modern_richer_native_state_remains_opaque_and_lossless() -> None:
    native = {**_MODERN_MR_NATIVE_PARAMS, "others": "-Dmapreduce.job.reduces=4"}

    exported = decode_task_parameters_with_provenance(
        version="3.4.1",
        task_type="MR",
        task_params=native,
        refs=_NO_TASK_REFS,
        resource_refs=_MODERN_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert exported.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert exported.task.task_params == native
    assert (
        encode_task_parameters(
            version="3.4.1",
            task_type="MR",
            task_params=exported.task.task_params,
            refs=_NO_TASK_REFS,
            resource_refs=_MODERN_RESOURCE_REFS,
            source=exported.reencode_source,
        ).task_params
        == native
    )


def test_mr_explicit_opaque_authoring_accepts_executable_scala_mode() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    native: YamlObject = {
        "programType": "SCALA",
        "mainJar": {"resourceName": "/jobs/wordcount.jar"},
    }

    assert (
        catalog.normalize_task_params(
            "MR",
            native,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )
        == native
    )


@pytest.mark.parametrize("program_type", ["PYTHON", "JAVA", "UNKNOWN"])
def test_mr_explicit_opaque_authoring_rejects_broken_or_owned_modes(
    program_type: str,
) -> None:
    catalog = get_task_authoring_catalog("3.4.1")

    with pytest.raises(
        UnsupportedFeatureError,
        match="MR/literal_java_jar_job is unsupported",
    ):
        catalog.normalize_task_params(
            "MR",
            {
                "programType": program_type,
                "mainJar": {"resourceName": "/jobs/wordcount.jar"},
            },
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )


@pytest.mark.parametrize(
    "version",
    ["1.3.9", "2.0.0", "2.0.9", "3.0.0", "3.0.6", "3.1.0", "3.1.9"],
)
def test_mr_legacy_wire_binds_one_positive_resource_id_and_round_trips(
    version: str,
) -> None:
    resource_refs = TaskResourceRefIndex.from_id_by_full_name(
        {"/jobs/wordcount.jar": 731}
    )

    native = encode_task_parameters(
        version=version,
        task_type="MR",
        task_params=_CANONICAL_MR_PARAMS,
        refs=_NO_TASK_REFS,
        resource_refs=resource_refs,
        source=ProjectionSource.TYPED_AUTHORING,
    ).task_params
    exported = decode_task_parameters_with_provenance(
        version=version,
        task_type="MR",
        task_params=native,
        refs=_NO_TASK_REFS,
        resource_refs=resource_refs,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert native == _LEGACY_MR_NATIVE_PARAMS
    assert exported.reencode_source is ProjectionSource.TYPED_AUTHORING
    assert exported.task.task_params == _CANONICAL_MR_PARAMS
