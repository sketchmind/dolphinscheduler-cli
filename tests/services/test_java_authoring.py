from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest
import yaml

from dsctl.errors import UnsupportedFeatureError
from dsctl.models.workflow_spec import validate_workflow_document
from dsctl.services._workflow.authoring import workflow_authoring_context
from dsctl.services._workflow.compile import prepare_workflow_create_compilation
from dsctl.services.task_authoring import task_type_schema_result
from dsctl.services.task_authoring_catalog import (
    JAVA_LITERAL_FAT_JAR_FACET,
    TaskAuthoringIntent,
    get_task_authoring_catalog,
)
from dsctl.services.template import task_template_result
from dsctl.upstream.task_authoring_surface import get_task_authoring_surface
from dsctl.upstream.task_parameter_projection import (
    ProjectionSource,
    TaskParameterProjectionError,
    TaskRefIndex,
    TaskResourceRefIndex,
    decode_task_parameters_with_provenance,
    encode_task_parameters,
)

if TYPE_CHECKING:
    from dsctl.models.common import YamlObject
    from dsctl.support.json_types import JsonObject

_MAIN_JAR = "/jobs/daily-orders.jar"
_NATIVE_JAR = "/tenant/resources/jobs/daily-orders.jar"
_RESOURCE_REFS = TaskResourceRefIndex.from_resolved_files(
    [_MAIN_JAR],
    id_by_full_name={},
    wire_full_name_by_full_name={_MAIN_JAR: _NATIVE_JAR},
)
_TYPED_VERSIONS = (
    "3.2.0",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)
_ABSENT_VERSIONS = (
    "1.3.9",
    "2.0.0",
    "2.0.9",
    "3.0.0",
    "3.0.6",
    "3.1.0",
    "3.1.9",
)
_REFS = TaskRefIndex.from_code_by_name({})
# Native fingerprints migrated with reviewed normalized-model equivalence;
# receipt: build/ds-3.4.3-admission/task-fingerprint-migration-receipts.json.
_FINGERPRINTS = {
    "3.2.0": "sha256:87c600d429ba758c678f7982e1e17c06edb89d26e65bdcd7f03c35e6846afc41",
    "3.2.2": "sha256:0e335ea530a914ffe41113f154f8b9c7b5c68439845e87b4d4e1cd1c9fed47d2",
    "3.3.1": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
    "3.3.2": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
    "3.4.0": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
    "3.4.1": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
    "3.4.2": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
    "3.4.3": "sha256:39efa16f2e062d216afa59d2c04e698a296e963ee1ff8590d1c7d18c2bf68c0a",
}


def _native_java_params(version: str) -> JsonObject:
    resource: JsonObject = {"resourceName": _NATIVE_JAR}
    legacy_wire = version in {"3.2.0", "3.2.1", "3.2.2"}
    native: JsonObject = {
        "localParams": [],
        "mainJar": resource,
        "runType": "JAR" if legacy_wire else "FAT_JAR",
        "mainArgs": "--date 2026-08-30",
        "jvmArgs": "",
        "isModulePath": False,
        "resourceList": [resource],
    }
    if legacy_wire:
        native["rawScript"] = ""
    else:
        native["mainClass"] = ""
    return native


@pytest.mark.parametrize(
    ("version", "run_type", "epoch_field"),
    [
        ("3.2.0", "JAR", "rawScript"),
        ("3.2.2", "JAR", "rawScript"),
        ("3.3.1", "FAT_JAR", "mainClass"),
        ("3.4.1", "FAT_JAR", "mainClass"),
    ],
)
def test_java_literal_fat_jar_compiles_one_safe_canonical_intent_across_epochs(
    version: str,
    run_type: str,
    epoch_field: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    spec = validate_workflow_document(
        {
            "workflow": {"name": f"java-fat-jar-{version}"},
            "tasks": [
                {
                    "name": "run-daily-orders",
                    "type": "JAVA",
                    "task_params": {
                        "mainJar": _MAIN_JAR,
                        "mainArgs": ["--date", "2026-08-30"],
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
    ).materialize([31_000], resource_refs=_RESOURCE_REFS)
    definition = json.loads(payload["taskDefinitionJson"])[0]
    task_params = json.loads(definition["taskParams"])
    resource = {"resourceName": _NATIVE_JAR}
    expected = {
        "localParams": [],
        "mainJar": resource,
        "runType": run_type,
        "mainArgs": "--date 2026-08-30",
        "jvmArgs": "",
        "isModulePath": False,
        "resourceList": [resource],
    }
    expected[epoch_field] = ""

    assert definition["taskType"] == "JAVA"
    assert task_params == expected


def test_java_schema_exposes_one_closed_resource_and_argument_intent() -> None:
    catalog = get_task_authoring_catalog("3.4.1")
    result = task_type_schema_result("JAVA", catalog=catalog)
    assert isinstance(result.data, dict)
    fields = {
        field["path"]: field
        for field in result.data["fields"]
        if isinstance(field, dict) and isinstance(field.get("path"), str)
    }

    assert {path for path in fields if path.startswith("task_params.")} == {
        "task_params.mainJar",
        "task_params.mainArgs",
        "task_params.mainArgs[]",
    }
    assert fields["task_params.mainJar"]["choice_source"] == "dsctl resource list"
    assert (
        fields["task_params.mainJar"]["choice_value"]
        == "fullName relative to the FILE root, retaining one leading slash"
    )
    assert fields["task_params.mainArgs"]["default"] == []


def test_java_json_schema_is_closed_and_matches_the_safe_runtime_subset() -> None:
    result = task_type_schema_result(
        "JAVA",
        json_schema=True,
        catalog=get_task_authoring_catalog("3.4.1"),
    )
    assert isinstance(result.data, dict)
    task_params = result.data["schema"]["$defs"]["task_params"]

    assert task_params["additionalProperties"] is False
    assert task_params["required"] == ["mainJar"]
    assert set(task_params["properties"]) == {"mainJar", "mainArgs"}
    assert task_params["properties"]["mainJar"]["pattern"] == (
        r"^/(?!\.{1,2}(?:/|$))(?!.*//)(?!.*(?:/\.{1,2})(?:/|$))"
        r"[A-Za-z0-9_./:@%+=,\-]+\.jar$"
    )
    assert task_params["properties"]["mainArgs"]["default"] == []
    assert task_params["properties"]["mainArgs"]["items"]["pattern"] == (
        r"^[A-Za-z0-9_./:@%+=,\-]+$"
    )


@pytest.mark.parametrize(
    "task_params",
    [
        {"mainJar": "jobs/app.jar"},
        {"mainJar": "/jobs/../app.jar"},
        {"mainJar": "/jobs/my app.jar"},
        {"mainJar": "/jobs/${jar}.jar"},
        {"mainJar": _MAIN_JAR, "mainArgs": "--date"},
        {"mainJar": _MAIN_JAR, "mainArgs": ["two words"]},
        {"mainJar": _MAIN_JAR, "mainArgs": ["$(id)"]},
        {"mainJar": _MAIN_JAR, "jvmArgs": "-Xmx1g"},
    ],
)
def test_java_typed_model_rejects_unowned_or_shell_unsafe_input(
    task_params: YamlObject,
) -> None:
    catalog = get_task_authoring_catalog("3.4.1")

    with pytest.raises(ValueError):
        catalog.normalize_task_params(
            "JAVA",
            task_params,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_java_minimal_template_validates_on_every_reviewed_profile(
    version: str,
) -> None:
    catalog = get_task_authoring_catalog(version)
    result = task_template_result("JAVA", catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(result.data["yaml"])

    spec = validate_workflow_document(
        {
            "workflow": {"name": f"java-template-{version}"},
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )
    assert spec.tasks[0].task_params == {
        "mainJar": _MAIN_JAR,
        "mainArgs": ["--date", "2026-08-30"],
    }


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_java_is_source_absent_before_320(version: str) -> None:
    catalog = get_task_authoring_catalog(version)

    assert "JAVA" not in catalog.upstream_task_types
    assert get_task_authoring_surface(version).java.available is False
    with pytest.raises(UnsupportedFeatureError, match="JAVA"):
        catalog.require_task_type("JAVA")


def test_java_321_allows_only_source_opaque_and_preserves_broken_jar() -> None:
    catalog = get_task_authoring_catalog("3.2.1")
    surface = get_task_authoring_surface("3.2.1").java
    broken_jar = cast("YamlObject", _native_java_params("3.2.1"))
    native: YamlObject = {
        "runType": "JAVA",
        "rawScript": "public class Main {}",
    }

    assert "JAVA" in catalog.upstream_task_types
    assert "JAVA" not in catalog.reviewed_typed_task_types
    assert catalog.supports_typed_authoring("JAVA") is False
    assert catalog.supports_opaque_authoring("JAVA") is True
    assert surface.available is True
    assert surface.typed_fat_jar_supported is False
    assert surface.runtime_epoch == "broken-double-prefixed-resource-context"
    assert surface.exclusion_reason == "main-jar-absolute-path-is-double-prefixed"

    with pytest.raises(UnsupportedFeatureError, match=r"JAVA typed authoring.*3\.2\.1"):
        catalog.normalize_task_params(
            "JAVA",
            {"mainJar": _MAIN_JAR, "mainArgs": []},
            intent=TaskAuthoringIntent.TYPED_CREATE,
        )

    normalized = catalog.normalize_task_params(
        "JAVA",
        native,
        intent=TaskAuthoringIntent.OPAQUE_CREATE,
    )
    assert normalized == native
    assert normalized is not native

    with pytest.raises(
        UnsupportedFeatureError,
        match="JAVA/legacy_source_opaque is unsupported",
    ):
        catalog.normalize_task_params(
            "JAVA",
            broken_jar,
            intent=TaskAuthoringIntent.OPAQUE_CREATE,
        )

    preserved = catalog.normalize_task_params(
        "JAVA",
        broken_jar,
        intent=TaskAuthoringIntent.OPAQUE_PRESERVE,
    )
    assert preserved == broken_jar


@pytest.mark.parametrize(
    "intent",
    [TaskAuthoringIntent.TYPED_CREATE, TaskAuthoringIntent.TYPED_EDIT],
)
def test_java_321_workflow_path_never_downgrades_canonical_fat_jar_to_opaque(
    intent: TaskAuthoringIntent,
) -> None:
    catalog = get_task_authoring_catalog("3.2.1")

    with pytest.raises(
        UnsupportedFeatureError,
        match="JAVA/legacy_source_opaque is unsupported",
    ):
        validate_workflow_document(
            {
                "workflow": {"name": "java-321-runtime-hole"},
                "tasks": [
                    {
                        "name": "broken-java-fat-jar",
                        "type": "JAVA",
                        "task_params": {
                            "mainJar": _MAIN_JAR,
                            "mainArgs": [],
                        },
                    }
                ],
            },
            authoring_context=workflow_authoring_context(
                catalog=catalog,
                intent=intent,
            ),
        )


def test_java_321_opaque_source_template_validates_and_compiles_identity() -> None:
    catalog = get_task_authoring_catalog("3.2.1")
    result = task_template_result("JAVA", catalog=catalog)
    assert isinstance(result.data, dict)
    task = yaml.safe_load(result.data["yaml"])
    spec = validate_workflow_document(
        {
            "workflow": {"name": "java-321-source-opaque"},
            "tasks": [task],
        },
        authoring_context=workflow_authoring_context(
            catalog=catalog,
            intent=TaskAuthoringIntent.TYPED_CREATE,
        ),
    )

    payload = prepare_workflow_create_compilation(
        spec,
        catalog=catalog,
    ).materialize([31_021])
    definition = json.loads(payload["taskDefinitionJson"])[0]

    assert result.resolved["template_kind"] == "generic"
    assert definition["taskType"] == "JAVA"
    assert json.loads(definition["taskParams"]) == task["task_params"]


def test_java_321_projection_rejects_typed_but_preserves_opaque_wire() -> None:
    canonical: JsonObject = {"mainJar": _MAIN_JAR, "mainArgs": []}
    native = _native_java_params("3.2.1")

    with pytest.raises(TaskParameterProjectionError) as encode_error:
        encode_task_parameters(
            version="3.2.1",
            task_type="JAVA",
            task_params=canonical,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    assert encode_error.value.details["reason"] == (
        "main-jar-absolute-path-is-double-prefixed"
    )

    with pytest.raises(TaskParameterProjectionError) as decode_error:
        decode_task_parameters_with_provenance(
            version="3.2.1",
            task_type="JAVA",
            task_params=native,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    assert decode_error.value.details["reason"] == (
        "main-jar-absolute-path-is-double-prefixed"
    )

    decoded = decode_task_parameters_with_provenance(
        version="3.2.1",
        task_type="JAVA",
        task_params=native,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.task.task_params == native
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert (
        encode_task_parameters(
            version="3.2.1",
            task_type="JAVA",
            task_params=decoded.task.task_params,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )


@pytest.mark.parametrize("version", _ABSENT_VERSIONS)
def test_java_projection_fails_closed_when_task_type_is_absent(version: str) -> None:
    with pytest.raises(TaskParameterProjectionError) as captured:
        encode_task_parameters(
            version=version,
            task_type="JAVA",
            task_params={"mainJar": _MAIN_JAR, "mainArgs": []},
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
    assert captured.value.details["reason"] == "task-type-absent-in-version"


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_java_exact_native_wire_round_trips_through_typed_provenance(
    version: str,
) -> None:
    native = _native_java_params(version)

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="JAVA",
        task_params=native,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task.task_type == "JAVA"
    assert decoded.task.task_params == {
        "mainJar": _MAIN_JAR,
        "mainArgs": ["--date", "2026-08-30"],
    }
    assert decoded.reencode_source is ProjectionSource.TYPED_AUTHORING

    reencoded = encode_task_parameters(
        version=version,
        task_type=decoded.task.task_type,
        task_params=decoded.task.task_params,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=decoded.reencode_source,
    )
    assert reencoded.task_params == native


@pytest.mark.parametrize("version", _TYPED_VERSIONS)
def test_java_catalog_locks_reviewed_exact_memberships(version: str) -> None:
    catalog = get_task_authoring_catalog(version)
    profile = catalog.require_task_type("JAVA")
    membership = catalog.require_facet("JAVA", JAVA_LITERAL_FAT_JAR_FACET)
    fact = catalog.task_type_facts["JAVA"]
    review = fact.typed_authoring_review

    assert catalog.supports_typed_authoring("JAVA") is True
    assert catalog.supports_opaque_authoring("JAVA") is True
    assert profile.category == "Universal"
    assert profile.default_facet == JAVA_LITERAL_FAT_JAR_FACET
    assert set(profile.facets) == {JAVA_LITERAL_FAT_JAR_FACET}
    assert fact.semantic_fingerprint == _FINGERPRINTS[version]
    assert review is not None
    assert review.review == (
        "exact-3.4.3-java-source-closure"
        if version == "3.4.3"
        else "java-literal-fat-jar-shell-safe-subset"
    )
    assert review.semantic_fingerprint == fact.semantic_fingerprint
    assert review.cli_model == "JavaLiteralFatJarTaskParamsSpec"
    assert membership.contract.family == "java-literal-fat-jar-v1"
    assert membership.contract.opaque_authoring_selector is not None
    assert membership.contract.restrict_opaque_authoring_to_selector is True
    assert membership.typed_create is True
    assert membership.typed_edit is True
    assert membership.opaque_create is True
    assert membership.opaque_edit is True
    assert membership.opaque_preserve is True


@pytest.mark.parametrize(
    (
        "version",
        "wire_epoch",
        "runtime_epoch",
        "run_type",
        "source_mode",
        "normal_jar",
        "jvm_args_before_target",
        "parameter_substitution",
    ),
    [
        (
            "3.2.0",
            "source-and-jar",
            "legacy-resource-map",
            "JAR",
            True,
            False,
            False,
            False,
        ),
        (
            "3.2.2",
            "source-and-jar",
            "fixed-resource-context",
            "JAR",
            True,
            False,
            False,
            False,
        ),
        (
            "3.3.1",
            "fat-and-normal-jar",
            "fat-normal-legacy-argument-order",
            "FAT_JAR",
            False,
            True,
            False,
            False,
        ),
        (
            "3.4.0",
            "fat-and-normal-jar",
            "fat-normal-legacy-argument-order",
            "FAT_JAR",
            False,
            True,
            False,
            False,
        ),
        (
            "3.4.1",
            "fat-and-normal-jar",
            "fat-normal-parameterized",
            "FAT_JAR",
            False,
            True,
            True,
            True,
        ),
        (
            "3.4.2",
            "fat-and-normal-jar",
            "fat-normal-parameterized",
            "FAT_JAR",
            False,
            True,
            True,
            True,
        ),
    ],
)
def test_java_surface_records_each_wire_and_runtime_epoch(
    version: str,
    wire_epoch: str,
    runtime_epoch: str,
    run_type: str,
    *,
    source_mode: bool,
    normal_jar: bool,
    jvm_args_before_target: bool,
    parameter_substitution: bool,
) -> None:
    surface = get_task_authoring_surface(version).java

    assert surface.wire_epoch == wire_epoch
    assert surface.runtime_epoch == runtime_epoch
    assert surface.fat_jar_run_type == run_type
    assert surface.source_mode_supported is source_mode
    assert surface.normal_jar_supported is normal_jar
    assert surface.jvm_args_before_target is jvm_args_before_target
    assert surface.parameter_substitution is parameter_substitution
    assert surface.task_params_logged is True
    assert surface.command_logged is True
    assert surface.result_output_supported is False
    assert surface.durable_application_id is False
    assert surface.failover_supported is False
    assert surface.retry_reexecutes is True


@pytest.mark.parametrize(
    ("version", "cancel_mode"),
    [
        ("3.2.0", "direct-process"),
        ("3.2.1", "direct-process"),
        ("3.2.2", "direct-process"),
        ("3.3.1", "process-tree-and-generic-app"),
        ("3.4.2", "process-tree-and-generic-app"),
    ],
)
def test_java_surface_records_exact_cancel_epoch(
    version: str,
    cancel_mode: str,
) -> None:
    assert get_task_authoring_surface(version).java.cancel_mode == cancel_mode


@pytest.mark.parametrize(
    ("version", "alternate_native", "typed_run_type"),
    [
        ("3.2.0", {"runType": "JAVA", "rawScript": "public class Main {}"}, "JAR"),
        (
            "3.4.2",
            {"runType": "NORMAL_JAR", "mainClass": "com.example.Main"},
            "FAT_JAR",
        ),
    ],
)
def test_java_explicit_opaque_authoring_accepts_only_the_alternate_native_mode(
    version: str,
    alternate_native: YamlObject,
    typed_run_type: str,
) -> None:
    catalog = get_task_authoring_catalog(version)

    normalized = catalog.normalize_task_params(
        "JAVA",
        alternate_native,
        intent=TaskAuthoringIntent.OPAQUE_CREATE,
    )
    assert normalized == alternate_native
    assert normalized is not alternate_native

    rejected_params: tuple[YamlObject, ...] = (
        {"runType": typed_run_type},
        {"runType": "UNKNOWN"},
    )
    for rejected in rejected_params:
        with pytest.raises(
            UnsupportedFeatureError,
            match="JAVA/literal_fat_jar is unsupported",
        ):
            catalog.normalize_task_params(
                "JAVA",
                rejected,
                intent=TaskAuthoringIntent.OPAQUE_CREATE,
            )


@pytest.mark.parametrize("version", ["3.2.0", "3.4.2"])
def test_java_richer_native_state_stays_lossless_opaque(version: str) -> None:
    native = _native_java_params(version)
    native["jvmArgs"] = "-Xmx1g"
    native["futureField"] = {"preserve": [True, 7]}

    decoded = decode_task_parameters_with_provenance(
        version=version,
        task_type="JAVA",
        task_params=native,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )

    assert decoded.task.task_params == native
    assert decoded.task.task_params is not native
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE
    assert (
        encode_task_parameters(
            version=version,
            task_type="JAVA",
            task_params=decoded.task.task_params,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=decoded.reencode_source,
        ).task_params
        == native
    )

    with pytest.raises(
        TaskParameterProjectionError,
        match="does not own fields: futureField",
    ):
        decode_task_parameters_with_provenance(
            version=version,
            task_type="JAVA",
            task_params=native,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )


@pytest.mark.parametrize("numeric_flag", [0, 1])
def test_java_decode_never_coerces_numeric_module_path_to_boolean(
    numeric_flag: int,
) -> None:
    native = _native_java_params("3.4.1")
    native["isModulePath"] = numeric_flag

    decoded = decode_task_parameters_with_provenance(
        version="3.4.1",
        task_type="JAVA",
        task_params=native,
        refs=_REFS,
        resource_refs=_RESOURCE_REFS,
        source=ProjectionSource.OPAQUE_PRESERVE,
    )
    assert decoded.task.task_params == native
    assert decoded.reencode_source is ProjectionSource.OPAQUE_PRESERVE

    with pytest.raises(TaskParameterProjectionError, match="isModulePath"):
        decode_task_parameters_with_provenance(
            version="3.4.1",
            task_type="JAVA",
            task_params=native,
            refs=_REFS,
            resource_refs=_RESOURCE_REFS,
            source=ProjectionSource.TYPED_AUTHORING,
        )
