from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from ds_codegen.discovery_contracts import (
    DocumentSource,
    compile_document_sources,
    compile_public_operations,
)
from ds_codegen.ir import ContractSnapshot, OperationSpec, ParameterSpec
from ds_codegen.version_discovery import (
    compile_discovery_profile,
    write_version_discovery,
)

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    from tests.codegen.exact_contract_corpus import ExactContractCorpus

    from ds_codegen.discovery_contracts import ApiOperationContract


INTERMEDIATE_VERSIONS = (
    "2.0.1",
    "2.0.2",
    "2.0.3",
    "2.0.4",
    "2.0.5",
    "2.0.6",
    "2.0.7",
    "2.0.8",
    "3.0.1",
    "3.0.2",
    "3.0.3",
    "3.0.4",
    "3.0.5",
    "3.1.1",
    "3.1.2",
    "3.1.3",
    "3.1.4",
    "3.1.5",
    "3.1.6",
    "3.1.7",
    "3.1.8",
)

_GROUPED_CONFIGURATION = (
    "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/"
    "configuration/OpenAPIConfiguration.java"
)

# These routes come from the native AccessTokenV2Controller and
# ProjectV2Controller declarations, which still live in the v1 source namespace.
_GROUPED_V2_ROUTES = {
    ("POST", "v2/access-tokens"),
    ("GET", "v2/projects"),
    ("POST", "v2/projects"),
    ("GET", "v2/projects/authed-project"),
    ("GET", "v2/projects/authed-user"),
    ("GET", "v2/projects/created-and-authed"),
    ("GET", "v2/projects/list"),
    ("GET", "v2/projects/list-dependent"),
    ("GET", "v2/projects/unauth-project"),
    ("DELETE", "v2/projects/{code}"),
    ("GET", "v2/projects/{code}"),
    ("PUT", "v2/projects/{code}"),
}


@pytest.mark.source_contract
def test_discovery_memberships_come_from_all_exact_sources(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    profiles = [
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    ]
    assert len(profiles) == 37
    assert [profile.version for profile in profiles if profile.product_path] == [
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ]
    assert [profile.version for profile in profiles if profile.openapi] == [
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    ]
    assert {
        p.version: p.route_miss_code for p in profiles if p.route_miss_code is not None
    } == {
        "2.0.0": 110003,
        **dict.fromkeys(INTERMEDIATE_VERSIONS, 110003),
        "2.0.9": 110003,
        "3.0.0": 110003,
        "3.0.6": 110003,
        "3.1.0": 110003,
        "3.1.9": 110003,
        "3.2.0": 110003,
        "3.2.1": 110003,
        "3.2.2": 110003,
    }
    assert {
        p.version: p.database_product_version
        for p in profiles
        if p.database_product_version not in {None, p.version}
    } == {"2.0.8": "2.0.7", "3.0.3": "3.0.2", "3.2.2": "3.3.0"}
    for profile in profiles:
        if profile.product_path:
            assert profile.product_path == "ui-plugins/query-product-info"
            assert profile.product_version_field == "version"
            assert profile.evidence


@pytest.mark.source_contract
@pytest.mark.parametrize("change", ["method", "return", "field"])
def test_product_probe_rejects_changed_source_contract(
    exact_contract_corpus: ExactContractCorpus, change: str
) -> None:
    snapshot = deepcopy(exact_contract_corpus.snapshot("3.4.2"))
    if change == "field":
        model = next(
            model for model in snapshot.models if model.name == "ProductInfoDto"
        )
        model.fields[0] = replace(model.fields[0], java_type="Integer")
    else:
        index = next(
            i
            for i, operation in enumerate(snapshot.operations)
            if operation.operation_id == "UiPluginController.queryProductInfo"
        )
        operation = snapshot.operations[index]
        snapshot.operations[index] = (
            replace(operation, http_method="POST")
            if change == "method"
            else replace(operation, return_type="String")
        )
    with pytest.raises(ValueError, match="product-info"):
        compile_discovery_profile(snapshot, exact_contract_corpus.source_root("3.4.2"))


@pytest.mark.source_contract
def test_openapi_api_label_is_not_a_product_version_fact(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    path = (
        tmp_path
        / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api"
        / "configuration/SwaggerConfiguration.java"
    )
    path.parent.mkdir(parents=True)
    path.write_text('new OpenAPI().info(new Info().version("V1"));')
    with pytest.raises(ValueError, match="source seam changed"):
        compile_discovery_profile(exact_contract_corpus.snapshot("3.2.0"), tmp_path)


@pytest.mark.source_contract
def test_intermediate_sources_remain_explicit_version_only_after_admission(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    profiles = tuple(
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    )
    intermediate = tuple(
        profile for profile in profiles if profile.version in INTERMEDIATE_VERSIONS
    )
    assert len(intermediate) == 21
    for profile in intermediate:
        assert profile.product_path is None
        assert profile.product_version_field is None
        assert not profile.openapi
        assert profile.route_miss_code == 110003
        assert profile.evidence

    write_version_discovery(tmp_path, profiles)
    generated = _load_generated_discovery(tmp_path)
    assert set(INTERMEDIATE_VERSIONS) <= generated["SOURCE_EVIDENCE"].keys()
    for probe in generated["PROBES"]:
        assert set(probe.exact_versions).isdisjoint(INTERMEDIATE_VERSIONS)

    # Matching database version text without a trusted endpoint is insufficient.
    write_version_discovery(tmp_path, intermediate)
    isolated = _load_generated_discovery(tmp_path)
    assert isolated["PROBES"] == ()
    assert set(isolated["SOURCE_EVIDENCE"]) == set(INTERMEDIATE_VERSIONS)


@pytest.mark.source_contract
def test_public_api_documents_retain_all_exact_memberships(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    profiles = tuple(
        compile_discovery_profile(
            exact_contract_corpus.snapshot(version),
            exact_contract_corpus.source_root(version),
        )
        for version in exact_contract_corpus.versions
    )
    for profile in profiles:
        assert profile.documents
        assert profile.public_operations
        if profile.version.startswith("3.1."):
            assert [(doc.path, doc.document_group) for doc in profile.documents] == [
                ("v3/api-docs?group=v1(current)", "v1"),
                ("v3/api-docs?group=v2", "v2"),
            ]
        elif profile.openapi:
            assert [doc.path for doc in profile.documents] == ["v3/api-docs"]
        else:
            assert [doc.path for doc in profile.documents] == ["v2/api-docs"]
    legacy = profiles[0]
    assert legacy.version == "1.3.9"
    assert len(legacy.public_operations) == 133
    operations = {
        operation.operation_id: operation for operation in legacy.public_operations
    }
    assert "AccessTokenController.createToken" not in operations
    assert "AccessTokenController.queryAccessTokenList" in operations
    write_version_discovery(tmp_path, profiles)
    generated = _load_generated_discovery(tmp_path)
    assert tuple(generated["CONTRACT_PROFILES"]) == exact_contract_corpus.versions
    assert len(generated["DISCOVERY_CONTRACT_DIGEST"]) == 64
    assert generated["DOCUMENT_PROBES"][0].source == "swagger2"
    assert generated["DOCUMENT_PROBES"][0].exact_versions[0] == "1.3.9"
    assert len(generated["OPERATION_CONTRACTS"]) < sum(
        len(profile.public_operations) for profile in profiles
    )
    before = generated["DISCOVERY_CONTRACT_DIGEST"]
    write_version_discovery(tmp_path, profiles[:-1])
    changed = _load_generated_discovery(tmp_path)
    assert changed["DISCOVERY_CONTRACT_DIGEST"] != before


@pytest.mark.source_contract
@pytest.mark.parametrize(
    ("version", "v1_count"),
    [("3.1.0", 242), *((f"3.1.{patch}", 243) for patch in range(1, 10))],
)
def test_grouped_documents_follow_source_path_selectors(
    exact_contract_corpus: ExactContractCorpus, version: str, v1_count: int
) -> None:
    source = exact_contract_corpus.source_file(
        version, _GROUPED_CONFIGURATION
    ).read_text(encoding="utf-8")
    assert '.groupName("v1(current)")' in source
    assert (
        '.paths(PathSelectors.any().and(PathSelectors.ant("/v2/**").negate()))'
        in source
    )
    assert '.groupName("v2")' in source
    assert '.paths(PathSelectors.any().and(PathSelectors.ant("/v2/**")))' in source

    snapshot = exact_contract_corpus.snapshot(version)
    original_namespaces = {
        operation.operation_id: operation.api_group for operation in snapshot.operations
    }
    assert set(original_namespaces.values()) == {"v1"}
    assert {
        (operation.http_method, operation.path)
        for operation in snapshot.operations
        if operation.controller in {"AccessTokenV2Controller", "ProjectV2Controller"}
    } == _GROUPED_V2_ROUTES

    profile = compile_discovery_profile(
        snapshot, exact_contract_corpus.source_root(version)
    )
    all_routes = {
        (operation.method, operation.path) for operation in profile.public_operations
    }
    document_routes = {
        document.path: {
            (operation.method, operation.path)
            for operation in profile.public_operations
            if operation.document_group == document.document_group
        }
        for document in profile.documents
    }
    assert document_routes == {
        "v3/api-docs?group=v1(current)": all_routes - _GROUPED_V2_ROUTES,
        "v3/api-docs?group=v2": _GROUPED_V2_ROUTES,
    }
    assert {path: len(routes) for path, routes in document_routes.items()} == {
        "v3/api-docs?group=v1(current)": v1_count,
        "v3/api-docs?group=v2": 12,
    }
    assert {
        operation.operation_id: operation.api_group for operation in snapshot.operations
    } == original_namespaces


@pytest.mark.source_contract
@pytest.mark.parametrize(
    ("before", "after"),
    [
        (
            '.paths(PathSelectors.any().and(PathSelectors.ant("/v2/**").negate()))',
            '.paths(PathSelectors.any().and(PathSelectors.ant("/v3/**").negate()))',
        ),
        (
            '.paths(PathSelectors.any().and(PathSelectors.ant("/v2/**")))',
            '.paths(PathSelectors.any().and(PathSelectors.ant("/v3/**")))',
        ),
        (
            'RequestHandlerSelectors.basePackage("org.apache.dolphinscheduler.api.controller")',
            'RequestHandlerSelectors.basePackage("org.apache.dolphinscheduler.api.controller.v2")',
        ),
    ],
    ids=["v1-selector", "v2-selector", "controller-package"],
)
def test_grouped_document_configuration_rejects_changed_selectors(
    exact_contract_corpus: ExactContractCorpus,
    tmp_path: Path,
    before: str,
    after: str,
) -> None:
    source = exact_contract_corpus.source_file(
        "3.1.9", _GROUPED_CONFIGURATION
    ).read_text(encoding="utf-8")
    assert before in source
    path = tmp_path / _GROUPED_CONFIGURATION
    path.parent.mkdir(parents=True)
    path.write_text(source.replace(before, after), encoding="utf-8")
    with pytest.raises(ValueError, match="source seam changed"):
        compile_document_sources(tmp_path)


@pytest.mark.source_contract
def test_grouped_document_configuration_rejects_swapped_group_selectors(
    exact_contract_corpus: ExactContractCorpus, tmp_path: Path
) -> None:
    source = exact_contract_corpus.source_file(
        "3.1.9", _GROUPED_CONFIGURATION
    ).read_text(encoding="utf-8")
    changed = source.replace('.groupName("v1(current)")', '.groupName("temporary")')
    changed = changed.replace('.groupName("v2")', '.groupName("v1(current)")')
    changed = changed.replace('.groupName("temporary")', '.groupName("v2")')
    path = tmp_path / _GROUPED_CONFIGURATION
    path.parent.mkdir(parents=True)
    path.write_text(changed, encoding="utf-8")
    with pytest.raises(ValueError, match="source seam changed"):
        compile_document_sources(tmp_path)


@pytest.mark.source_contract
@pytest.mark.parametrize(
    "version",
    ["3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"],
)
def test_springdoc_parameter_annotations_do_not_hide_token_routes(
    exact_contract_corpus: ExactContractCorpus, version: str
) -> None:
    profile = compile_discovery_profile(
        exact_contract_corpus.snapshot(version),
        exact_contract_corpus.source_root(version),
    )
    operations = {
        operation.operation_id: operation for operation in profile.public_operations
    }
    # Both source methods carry @Parameter(hidden = true), which does not hide
    # routes in springdoc 1.6.9 OperationService.isHidden(Method).
    generate = operations["AccessTokenController.generateToken"]
    assert (generate.method, generate.path) == ("POST", "access-tokens/generate")
    assert {
        (parameter.name, parameter.location) for parameter in generate.parameters
    } == {
        ("userId", "request"),
        ("expireTime", "request"),
    }
    delete = operations["AccessTokenController.delAccessTokenById"]
    assert (delete.method, delete.path) == ("DELETE", "access-tokens/{id}")
    assert {
        (parameter.name, parameter.location) for parameter in delete.parameters
    } == {
        ("id", "path"),
    }
    assert not delete.ignored_document_parameters


@pytest.mark.parametrize(
    (
        "class_annotation",
        "method_annotation",
        "parameter_annotation",
        "expected_parameters",
    ),
    [
        ("@Hidden", "", "", None),
        ("@ApiIgnore", "", "", None),
        ("", "@Hidden", "", None),
        ("", "@ApiIgnore", "", None),
        ("", "@Operation(hidden = true)", "", None),
        ("", "@ApiOperation(hidden = true)", "", None),
        ("", "@Parameter(hidden = true)", "", ("value", "visible")),
        ("", "@ApiParam(hidden = true)", "", ("value", "visible")),
        ("", "@Operation(hidden = false)", "", ("value", "visible")),
        ("", "@ApiOperation(hidden = false)", "", ("value", "visible")),
        ("", "", "@Parameter(hidden = true)", ("visible",)),
        ("", "", "@ApiParam(hidden = true)", ("visible",)),
        ("", "", "@Parameter(hidden = false)", ("value", "visible")),
        ("", "", "@ApiParam(hidden = false)", ("value", "visible")),
    ],
    ids=[
        "hidden-controller",
        "ignored-controller",
        "hidden-method",
        "ignored-method",
        "hidden-openapi-operation",
        "hidden-swagger-operation",
        "openapi-parameter-on-method",
        "swagger-parameter-on-method",
        "visible-openapi-operation",
        "visible-swagger-operation",
        "hidden-openapi-parameter",
        "hidden-swagger-parameter",
        "visible-openapi-parameter",
        "visible-swagger-parameter",
    ],
)
def test_document_visibility_respects_annotation_scope(
    tmp_path: Path,
    class_annotation: str,
    method_annotation: str,
    parameter_annotation: str,
    expected_parameters: tuple[str, ...] | None,
) -> None:
    operations = _compile_visibility_source(
        tmp_path, class_annotation, method_annotation, parameter_annotation
    )
    if expected_parameters is None:
        assert operations == ()
    else:
        assert len(operations) == 1
        assert (operations[0].method, operations[0].path) == ("GET", "visibility")
        assert (
            tuple(parameter.name for parameter in operations[0].parameters)
            == expected_parameters
        )


def _compile_visibility_source(
    source_root: Path,
    class_annotation: str,
    method_annotation: str,
    parameter_annotation: str,
) -> tuple[ApiOperationContract, ...]:
    path = (
        source_root
        / "dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api"
        / "controller/VisibilityController.java"
    )
    path.parent.mkdir(parents=True)
    path.write_text(
        f"""{class_annotation}
class VisibilityController {{
    {method_annotation}
    @GetMapping("/visibility")
    public String read({parameter_annotation} @RequestParam("value") String value,
                       @RequestParam("visible") String visible) {{ return value; }}
}}
""",
        encoding="utf-8",
    )
    operation = OperationSpec(
        operation_id="VisibilityController.read",
        controller="VisibilityController",
        method_name="read",
        api_group="v1",
        http_method="GET",
        path="visibility",
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        consumes=[],
        return_type="String",
        inferred_return_type=None,
        logical_return_type="String",
        response_projection="direct",
        parameters=[
            ParameterSpec(
                name=name,
                java_type="String",
                binding="request_param",
                wire_name=name,
                required=True,
                default_value=None,
                hidden=False,
                description=None,
                example=None,
                allowable_values=None,
                schema_type=None,
            )
            for name in ("value", "visible")
        ],
    )
    snapshot = ContractSnapshot(
        ds_version="3.4.1",
        operation_count=1,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[operation],
        enums=[],
        dtos=[],
        models=[],
    )
    operations, _ = compile_public_operations(
        snapshot, source_root, (DocumentSource("openapi3", "v3/api-docs", None),)
    )
    return operations


@pytest.mark.source_contract
def test_springfox_documentation_quirks_do_not_change_wire_contracts(
    exact_contract_corpus: ExactContractCorpus,
) -> None:
    profile = compile_discovery_profile(
        exact_contract_corpus.snapshot("1.3.9"),
        exact_contract_corpus.source_root("1.3.9"),
    )
    operations = {
        operation.operation_id: operation for operation in profile.public_operations
    }
    paging = operations["ProjectController.queryProjectListPaging"]
    assert paging.ignored_document_parameters == ("projectId",)
    assert {parameter.name for parameter in paging.parameters} == {
        "pageNo",
        "pageSize",
        "searchVal",
    }
    user_list = operations["UsersController.listAll"]
    assert not user_list.parameters
    assert {"tenantName", "userName", "userPassword", "userType"} <= set(
        user_list.ignored_document_parameters
    )
    email = next(
        parameter
        for parameter in operations["UsersController.createUser"].parameters
        if parameter.name == "email"
    )
    assert email.schema_type == "string"
    assert email.document_schema_types == ("integer", "string")
    assert email.required
    assert email.document_required == (False, True)
    enum = next(
        parameter
        for parameter in operations["AlertGroupController.createAlertgroup"].parameters
        if parameter.name == "groupType"
    )
    assert enum.schema_type == "string"
    assert enum.enum_values == ("EMAIL", "SMS")


def _load_generated_discovery(output_root: Path) -> dict[str, Any]:
    # Load scratch output as its own package so its relative data-module imports
    # cannot silently resolve the installed generated contracts.
    package_name = f"discovery_test_{uuid4().hex}"
    package = ModuleType(package_name)
    package.__path__ = [str(output_root / "generated")]
    sys.modules[package_name] = package
    name = f"{package_name}.version_discovery"
    spec = importlib.util.spec_from_file_location(
        name, output_root / "generated/version_discovery.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return vars(module)
    finally:
        for key in tuple(sys.modules):
            if key == package_name or key.startswith(package_name + "."):
                del sys.modules[key]


def test_discovery_rejects_missing_document_configuration(tmp_path: Path) -> None:
    snapshot = ContractSnapshot(
        ds_version="3.4.1",
        operation_count=0,
        enum_count=0,
        dto_count=0,
        model_count=0,
        operations=[],
        enums=[],
        dtos=[],
        models=[],
    )
    with pytest.raises(ValueError, match="API documentation configuration missing"):
        compile_discovery_profile(snapshot, tmp_path)
