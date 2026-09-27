from __future__ import annotations

import importlib
import sys
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainPlan
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_TENANT = "org.apache.dolphinscheduler.dao.entity.Tenant"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"


def _load(name: str) -> Any:
    tools_dir = Path(__file__).resolve().parents[2] / "tools"
    if str(tools_dir) not in sys.path:
        sys.path.insert(0, str(tools_dir))
    return importlib.import_module(name)


def _compile(bundles: tuple[RuntimeBundle, ...]) -> Any:
    definition = _load("ds_codegen.compiled_tenants").TENANT_COMPILED_DOMAIN
    return _load("ds_codegen.compiled_domains").compile_domains(
        bundles,
        (definition,),
        operation_dependencies={
            "tenant.create": ("queue.page",),
            "tenant.update": ("queue.page",),
            "user.create": ("tenant.page",),
            "user.update": ("tenant.page",),
        },
    )


@pytest.fixture(scope="module")
def tenant_plan(exact_runtime_bundles: tuple[RuntimeBundle, ...]) -> CompiledDomainPlan:
    plan: CompiledDomainPlan = _compile(exact_runtime_bundles).plan("tenant")
    return plan


def test_tenant_compiler_preserves_every_exact_recipe_and_response_epoch(
    tenant_plan: CompiledDomainPlan,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    expected = {
        "1.3.9": (
            "legacy_named",
            "page_legacy_named",
            "create_legacy_void",
            "update_legacy_void",
            "delete_legacy_void",
        ),
        "2.0.0": (
            "void",
            "page_entity_required_id_strict",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.1": (
            "void",
            "page_entity_required_id_strict",
            "create_void",
            "update_void",
            "delete_void",
        ),
        "2.0.2": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.3": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.4": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.5": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.6": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.7": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.8": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "2.0.9": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.0": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.1": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.2": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.3": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.4": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.5": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.0.6": (
            "create_entity",
            "page_entity_required_id_strict",
            "create_entity_required_id",
            "update_void",
            "delete_void",
        ),
        "3.1.0": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.1": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.2": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.3": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.4": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.5": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.6": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.7": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.8": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.1.9": (
            "create_entity",
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        "3.2.0": (
            "create_entity",
            "page_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
    }
    expected.update(
        dict.fromkeys(
            ("3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
            (
                "boolean",
                "page_entity_nullable_id",
                "create_entity_nullable_id",
                "update_bool",
                "delete_bool",
            ),
        )
    )
    assert tuple(profile.version for profile in tenant_plan.profiles) == tuple(expected)
    metadata = {
        bundle.spec.version: bundle.metadata for bundle in exact_runtime_bundles
    }
    for profile in tenant_plan.profiles:
        programs = dict(profile.programs)
        assert profile.status == "supported"
        assert tuple(programs) == ("page", "create", "update", "delete")
        assert (
            profile.recipe_id,
            *(program.codec for program in programs.values()),
        ) == expected[profile.version]
        assert (
            profile.source_contract_digest
            == metadata[profile.version].source_contract_digest
        )
        assert profile.record()["recipe_id"] == profile.recipe_id
        assert programs["page"].result_envelope == "optional"
        assert all(
            programs[name].result_envelope == "required"
            for name in ("create", "update", "delete")
        )
        page_method = (
            "queryTenantListPaging"
            if profile.recipe_id == "boolean"
            else "queryTenantlistPaging"
        )
        assert programs["page"].source_operation == f"TenantController.{page_method}"
        assert {program.source_operation for program in programs.values()} == {
            f"TenantController.{page_method}",
            "TenantController.createTenant",
            "TenantController.updateTenant",
            "TenantController.deleteTenantById",
        }


def test_tenant_compiler_preserves_the_closed_transport_and_field_ownership(
    tenant_plan: CompiledDomainPlan,
) -> None:
    codecs = dict(tenant_plan.codecs)
    assert {
        (record["method"], record["path"], record["channel"], record["params"])
        for record in codecs.values()
    } == {
        ("GET", "tenant/list-paging", "query", "page_legacy"),
        ("GET", "tenants", "query", "page"),
        ("POST", "tenant/create", "form", "create_legacy"),
        ("POST", "tenants", "form", "create"),
        ("POST", "tenant/update", "form", "update_legacy"),
        ("PUT", "tenants/{id}", "path_form", "update"),
        ("POST", "tenant/delete", "form", "id"),
        ("DELETE", "tenants/{id}", "path", "id"),
    }
    assert {request.schema for request in tenant_plan.requests} == {
        "page_legacy",
        "page",
        "create_legacy",
        "create",
        "update_legacy",
        "update",
        "id",
    }
    assert codecs["update_bool"]["fields"] == [
        {"name": "id", "binding": "path_variable"},
        {"name": "tenantCode", "binding": "request_param"},
        {"name": "queueId", "binding": "request_param"},
        {"name": "description", "binding": "request_param"},
    ]
    assert codecs["update_bool"]["path_fields"] == ["id"]
    assert codecs["update_bool"]["path_encoding"] == "percent-encoded-utf8-segment-v1"
    assert codecs["delete_legacy_void"]["fields"] == [
        {"name": "id", "binding": "request_param"}
    ]
    assert codecs["delete_legacy_void"]["path_fields"] == []
    assert codecs["delete_bool"]["fields"] == [
        {"name": "id", "binding": "path_variable"}
    ]
    assert (
        codecs["update_bool"]["response"]
        == codecs["delete_bool"]["response"]
        == "boolean"
    )
    assert {response.schema for response in tenant_plan.scalar_responses} == {"boolean"}


@pytest.mark.parametrize(
    ("page", "create", "update", "delete"),
    [
        ("page_legacy_named", "create_void", "update_void", "delete_void"),
        (
            "page_entity_required_id_strict",
            "create_entity_nullable_id",
            "update_void",
            "delete_void",
        ),
        (
            "page_entity_nullable_id_strict",
            "create_entity_nullable_id",
            "update_bool",
            "delete_bool",
        ),
        (
            "page_entity_nullable_id",
            "create_entity_nullable_id",
            "update_bool",
            "delete_void",
        ),
        (
            "page_entity_nullable_id",
            "create_entity_nullable_id",
            "update_void",
            "delete_bool",
        ),
    ],
)
def test_tenant_recipe_rejects_crossed_epochs(
    page: str,
    create: str,
    update: str,
    delete: str,
) -> None:
    definition = _load("ds_codegen.compiled_tenants").TENANT_COMPILED_DOMAIN
    with pytest.raises(ValueError, match="recipe is unsupported"):
        definition.recipe_policy(
            {"page": page, "create": create, "update": update, "delete": delete}
        )


@pytest.mark.parametrize(
    ("version", "model_path", "field_name", "attribute", "value", "match"),
    [
        (
            "1.3.9",
            _TENANT,
            "tenantName",
            "java_type",
            "Integer",
            "entity response epoch changed",
        ),
        ("2.0.0", _TENANT, "id", "nullable", True, "entity response epoch changed"),
        (
            "3.4.1",
            _TENANT,
            "queueId",
            "default_value",
            "1",
            "entity response epoch changed",
        ),
        (
            "3.4.1",
            _TENANT,
            "description",
            "wire_name",
            "descriptionV2",
            "entity response epoch changed",
        ),
        ("3.4.1", _PAGE, "totalList", "nullable", True, "page response epoch changed"),
        (
            "3.4.1",
            _PAGE,
            "totalList",
            "default_factory",
            None,
            "page response epoch changed",
        ),
        ("3.4.1", _PAGE, "total", "java_type", "Long", "page response epoch changed"),
    ],
)
def test_tenant_compiler_rejects_response_model_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    version: str,
    model_path: str,
    field_name: str,
    attribute: str,
    value: Any,
    match: str,
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                models=[
                    replace(
                        model,
                        fields=[
                            replace(field, **{attribute: value})
                            if field.wire_name == field_name
                            else field
                            for field in model.fields
                        ],
                    )
                    if model.import_path == model_path
                    else model
                    for model in bundle.snapshot.models
                ],
            ),
        )
        if bundle.spec.version == version
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match=match):
        _compile(bundles)


@pytest.mark.parametrize(
    ("operation_id", "changes", "match"),
    [
        ("TenantController.updateTenant", {"path": "tenants-v2/{id}"}, "request shape"),
        (
            "TenantController.deleteTenantById",
            {"http_method": "POST"},
            "delete path transport must use a reviewed integer-segment shape",
        ),
        (
            "TenantController.queryTenantListPaging",
            {"logical_return_type": f"{_PAGE}<String>"},
            "page response type changed",
        ),
        (
            "TenantController.createTenant",
            {"logical_return_type": "Boolean"},
            "create response changed",
        ),
    ],
)
def test_tenant_compiler_rejects_operation_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    operation_id: str,
    changes: dict[str, Any],
    match: str,
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    replace(operation, **changes)
                    if operation.operation_id == operation_id
                    else operation
                    for operation in bundle.snapshot.operations
                ],
            ),
        )
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match=match):
        _compile(bundles)


def test_tenant_compiler_rejects_crossed_path_and_form_ownership(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    replace(
                        operation,
                        parameters=[
                            replace(parameter, binding="request_param")
                            if parameter.wire_name == "id"
                            else parameter
                            for parameter in operation.parameters
                        ],
                    )
                    if operation.operation_id == "TenantController.updateTenant"
                    else operation
                    for operation in bundle.snapshot.operations
                ],
            ),
        )
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="implicit path arguments are unsupported"):
        _compile(bundles)


def test_tenant_compiler_preserves_exact_profiles_for_selected_bundles(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    tenant_plan: CompiledDomainPlan,
) -> None:
    selected = exact_runtime_bundles[:-1]
    plan = _compile(selected).plan("tenant")

    assert tuple(profile.version for profile in plan.profiles) == tuple(
        bundle.spec.version for bundle in selected
    )
    assert plan.profiles == tenant_plan.profiles[:-1]


@pytest.mark.parametrize(
    ("missing", "match"),
    [
        (True, "snapshot is missing operations"),
        (False, "unowned classified operations"),
    ],
)
def test_tenant_compiler_rejects_source_inventory_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    *,
    missing: bool,
    match: str,
) -> None:
    bundles = []
    for bundle in exact_runtime_bundles:
        if bundle.spec.version != "3.4.1":
            bundles.append(bundle)
            continue
        operations = list(bundle.snapshot.operations)
        create = next(
            operation
            for operation in operations
            if operation.operation_id == "TenantController.createTenant"
        )
        if missing:
            operations.remove(create)
        else:
            operations.append(
                replace(create, operation_id="TenantController.createTenantAlias")
            )
        bundles.append(
            replace(bundle, snapshot=replace(bundle.snapshot, operations=operations))
        )
    with pytest.raises(ValueError, match=match):
        _compile(tuple(bundles))


def test_tenant_compiler_rejects_request_schema_drift_within_one_epoch(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundles = tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    replace(
                        operation,
                        parameters=[
                            replace(parameter, java_type="String")
                            if parameter.wire_name == "queueId"
                            else parameter
                            for parameter in operation.parameters
                        ],
                    )
                    if operation.operation_id == "TenantController.createTenant"
                    else operation
                    for operation in bundle.snapshot.operations
                ],
            ),
        )
        if bundle.spec.version == "3.4.1"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="request schema create differs"):
        _compile(bundles)
