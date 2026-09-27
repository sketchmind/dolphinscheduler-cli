"""Source-backed resource ownership and explicit non-JSON transport contracts."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_domains import _select_request_epoch
from ds_codegen.compiled_resources import RESOURCE_COMPILED_DOMAIN
from ds_codegen.runtime_contract import (
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
)

if TYPE_CHECKING:
    from tests.codegen.exact_contract_corpus import ExactContractCorpus

pytestmark = pytest.mark.source_contract
_RECIPES = {
    "1.3.9": "legacy_139",
    "2.0.0": "id_rest",
    "2.0.1": "id_rest",
    "2.0.9": "id_rest",
    "2.0.2": "id_rest",
    "2.0.3": "id_rest",
    "2.0.4": "id_rest",
    "2.0.5": "id_rest",
    "2.0.6": "id_rest",
    "2.0.7": "id_rest",
    "2.0.8": "id_rest",
    "3.0.0": "id_rest",
    "3.0.1": "id_rest",
    "3.0.6": "id_rest",
    "3.0.2": "id_rest",
    "3.0.3": "id_rest",
    "3.0.4": "id_rest",
    "3.0.5": "id_rest",
    "3.1.0": "id_rest",
    "3.1.1": "id_rest",
    "3.1.2": "id_rest",
    "3.1.9": "id_rest",
    "3.1.3": "id_rest",
    "3.1.4": "id_rest",
    "3.1.5": "id_rest",
    "3.1.6": "id_rest",
    "3.1.7": "id_rest",
    "3.1.8": "id_rest",
    "3.2.0": "storage_path",
    "3.2.1": "storage_path",
    "3.2.2": "storage_path_322",
    "3.3.1": "absolute_path",
    "3.3.2": "absolute_path",
    "3.4.0": "absolute_path",
    "3.4.1": "absolute_path",
    "3.4.2": "absolute_path",
    "3.4.3": "absolute_path_native_view",
}


def test_all_resource_source_coordinates_keep_complete_exact_ownership(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    count = 0
    for version in exact_contract_corpus.versions:
        snapshot = exact_contract_corpus.snapshot(version)
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        owned = {
            source
            for name, binding in bindings.items()
            if name in domain.semantic_operations
            for source in binding.source_operations
        }
        assert len(owned) == 8
        assert not any(
            owned.intersection(binding.source_operations)
            for name, binding in bindings.items()
            if name not in domain.semantic_operations
        )
        operations = {
            domain.classify_operation(operation): operation
            for operation in snapshot.operations
            if operation.operation_id in owned
        }
        assert None not in operations
        codecs = {}
        for primitive in domain.primitives:
            if version in primitive.absent_versions:
                assert primitive.name not in operations
                continue
            operation = operations[primitive.name]
            epoch = _select_request_epoch(domain, primitive, operation, snapshot)
            assert epoch.versions == frozenset({version})
            policy = domain.response_policy(snapshot, operation, primitive.name)
            codecs[primitive.name] = policy.codec
            if primitive.name == "upload":
                assert epoch.channel == "multipart"
                assert epoch.file_fields == ("file",)
                assert "file" in epoch.request_fields
                assert policy.schema is None
                assert policy.response_transport == "json"
                assert primitive.result_envelope == "required"
            elif primitive.name == "create" and _RECIPES[version] in {
                "legacy_139",
                "id_rest",
            }:
                assert operation.logical_return_type == "Map<String, Object>"
                assert policy.schema is None
                assert policy.capture is None
                assert primitive.result_envelope == "required"
            elif primitive.name == "download":
                assert policy.response_transport == "binary"
                assert policy.schema is None
                assert primitive.result_envelope == "optional"
            else:
                assert epoch.file_fields == ()
                assert policy.response_transport == "json"
            count += 1
        assert domain.recipe_policy(codecs) == _RECIPES[version]
        with pytest.raises(ValueError, match="exact reviewed recipe"):
            domain.recipe_policy({**codecs, "page": "candidate_page"})
    assert count == 296


def test_id_backed_create_void_data_does_not_widen_directory_mutations(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    expected = {
        ("1.3.9", "create"): None,
        ("1.3.9", "mkdir"): "mkdir",
        ("2.0.0", "create"): None,
        ("2.0.0", "mkdir"): "mkdir",
        ("3.1.9", "create"): None,
        ("3.1.9", "mkdir"): "mkdir",
        ("3.2.0", "create"): None,
        ("3.2.0", "mkdir"): None,
    }
    for (version, primitive), schema in expected.items():
        snapshot = exact_contract_corpus.snapshot(version)
        operation = next(
            item
            for item in snapshot.operations
            if domain.classify_operation(item) == primitive
        )
        assert operation.logical_return_type == (
            "Void" if version == "3.2.0" else "Map<String, Object>"
        )
        policy = domain.response_policy(snapshot, operation, primitive)
        assert policy.schema == schema
        assert policy.capture == (None if schema is None else {})


def test_id_backed_create_void_data_matches_exact_result_replacement_flow(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    id_backed_versions = tuple(
        version
        for version, recipe in _RECIPES.items()
        if recipe in {"legacy_139", "id_rest"}
    )
    for version in id_backed_versions:
        relative = (
            "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
            "service/ResourcesService.java"
            if version == "1.3.9"
            else "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/"
            "api/service/impl/ResourcesServiceImpl.java"
        )
        source = exact_contract_corpus.source_file(version, relative).read_text(
            encoding="utf-8"
        )
        method = source.index("onlineCreateResource(")
        bean_map = source.index("result.setData(resultMap);", method)
        replacement = source.index("result = uploadContentTo", bean_map)
        returned = source.index("return result;", replacement)
        assert method < bean_map < replacement < returned

        helper_name = (
            "uploadContentToHdfs"
            if version.startswith(("1.", "2."))
            else "uploadContentToStorage"
        )
        helper_match = re.search(
            rf"private\s+Result(?:<Object>)?\s+{helper_name}\s*\(",
            source,
        )
        assert helper_match is not None
        helper = helper_match.start()
        helper_body = source.index("{", helper_match.end())
        fresh_result = source.index("new Result", helper_body)
        assert helper < helper_body < fresh_result


@pytest.mark.parametrize(
    "version", ["1.3.9", "2.0.0", "2.0.1", "3.2.0", "3.2.2", "3.4.2"]
)
@pytest.mark.parametrize(
    "change", ["type", "binding", "required", "default", "missing"]
)
def test_upload_source_file_declaration_is_checked_before_schema_projection(
    exact_contract_corpus: ExactContractCorpus, version: str, change: str
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item
        for item in snapshot.operations
        if domain.classify_operation(item) == "upload"
    )
    parameter = next(item for item in operation.parameters if item.wire_name == "file")
    changed = parameter
    if change == "type":
        changed = replace(parameter, java_type="String")
    elif change == "binding":
        changed = replace(parameter, binding="request_body")
    elif change == "required":
        changed = replace(parameter, required=False)
    elif change == "default":
        changed = replace(parameter, default_value="candidate")
    operation = replace(
        operation,
        parameters=[
            changed if item == parameter else item
            for item in operation.parameters
            if change != "missing" or item != parameter
        ],
    )
    with pytest.raises(ValueError, match="request fields changed"):
        domain.response_policy(snapshot, operation, "upload")


@pytest.mark.parametrize(
    ("version", "source_type"),
    [
        ("1.3.9", "org.springframework.core.io.Resource"),
        ("3.2.2", "org.springframework.core.io.Resource"),
        ("3.4.2", "void"),
    ],
)
def test_binary_policy_keeps_original_source_return_evidence_without_json_schema(
    exact_contract_corpus: ExactContractCorpus, version: str, source_type: str
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ResourcesController.downloadResource"
    )
    assert operation.logical_return_type == source_type
    policy = domain.response_policy(snapshot, operation, "download")
    assert policy.response_transport == "binary"
    assert policy.schema is None
    with pytest.raises(ValueError, match="native response role changed"):
        domain.response_policy(
            snapshot, replace(operation, logical_return_type="String"), "download"
        )


@pytest.mark.parametrize("version", ["1.3.9", "3.2.0", "3.4.2"])
def test_resource_type_enum_members_do_not_become_unreviewed_authority(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    snapshot = exact_contract_corpus.snapshot(version)
    operation = next(
        item
        for item in snapshot.operations
        if domain.classify_operation(item) == "page"
    )
    enum = next(item for item in snapshot.enums if item.name == "ResourceType")
    snapshot = replace(
        snapshot,
        enums=[
            replace(item, values=item.values[1:]) if item == enum else item
            for item in snapshot.enums
        ],
    )
    with pytest.raises(ValueError, match="native type enum changed"):
        domain.response_policy(snapshot, operation, "page")


@pytest.mark.parametrize(
    ("version", "model_name", "field"),
    [
        ("2.0.0", "Resource", "id"),
        ("3.2.0", "StorageEntity", "size"),
        ("3.4.2", "ResourceItemVO", "isDirectory"),
    ],
)
def test_resource_consumed_identity_and_projection_fields_fail_closed(
    exact_contract_corpus: ExactContractCorpus,
    version: str,
    model_name: str,
    field: str,
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    snapshot = exact_contract_corpus.snapshot(version)
    primitive = "lookup" if field == "id" else "page"
    operation = next(
        item
        for item in snapshot.operations
        if domain.classify_operation(item) == primitive
    )
    model = next(item for item in snapshot.models if item.name == model_name)
    snapshot = replace(
        snapshot,
        models=[
            replace(
                item, fields=[value for value in item.fields if value.name != field]
            )
            if item == model
            else item
            for item in snapshot.models
        ],
    )
    with pytest.raises(ValueError, match="consumed response fields changed"):
        domain.response_policy(snapshot, operation, primitive)


def test_modern_view_bug_evidence_is_owned_without_replacing_the_binary_recipe(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    domain = RESOURCE_COMPILED_DOMAIN
    snapshot = exact_contract_corpus.snapshot("3.4.2")
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "ResourcesController.viewResource"
    )
    policy = domain.response_policy(snapshot, operation, "view_native")
    assert policy.schema == "view_native"
    assert policy.response_transport == "json"
    assert (
        "ResourcesController.viewResource"
        in runtime_operation_bindings("3.4.2")["resource.view"].source_operations
    )
    assert (
        "ResourcesController.downloadResource"
        in runtime_operation_bindings("3.4.2")["resource.view"].source_operations
    )
