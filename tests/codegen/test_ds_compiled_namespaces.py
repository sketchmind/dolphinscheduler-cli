from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import Field, ValidationError
from tests.codegen.compiled_support import replace_operation, response_adapter

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_namespaces import NAMESPACE_COMPILED_DOMAIN
from ds_codegen.governance_contract import governance_contract
from ds_codegen.runtime_contract import runtime_operation_bindings
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_domains import CompiledDomainSet
    from ds_codegen.governance_contract import GovernanceVersionContract
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_ENTITY = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"


@pytest.fixture(scope="module")
def compiled_namespaces(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> CompiledDomainSet:
    return compile_domains(exact_runtime_bundles, (NAMESPACE_COMPILED_DOMAIN,))


def test_namespace_profiles_preserve_reviewed_recipes_and_permission_reads(
    compiled_namespaces: CompiledDomainSet,
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    plan = compiled_namespaces.plan("namespace")
    epochs = (
        (
            (
                "1.3.9",
                "2.0.0",
                "2.0.1",
                "2.0.2",
                "2.0.3",
                "2.0.4",
                "2.0.5",
                "2.0.6",
                "2.0.7",
                "2.0.8",
                "2.0.9",
            ),
            None,
        ),
        (
            ("3.0.0", "3.0.1", "3.0.2", "3.0.3", "3.0.4", "3.0.5", "3.0.6"),
            "k8s_quota_namespace",
        ),
        (
            (
                "3.1.0",
                "3.1.1",
                "3.1.2",
                "3.1.3",
                "3.1.4",
                "3.1.5",
                "3.1.6",
                "3.1.7",
                "3.1.8",
                "3.1.9",
            ),
            "cluster_quota_destructive",
        ),
        (("3.2.0",), "cluster_quota_registration_only"),
        (("3.2.1",), "cluster_registration_only"),
        (
            ("3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
            "cluster_entity_registration_only",
        ),
    )
    expected = {version: recipe for versions, recipe in epochs for version in versions}
    assert tuple(profile.version for profile in plan.profiles) == tuple(expected)
    assert len(plan.requests) == 6
    assert len(plan.responses) == 8
    assert len(plan.codecs) == 13
    for original, profile, legacy in zip(
        exact_runtime_bundles,
        plan.profiles,
        compiled_namespaces.legacy_bundles,
        strict=True,
    ):
        assert profile.recipe_id == expected[profile.version]
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )
        reviewed = governance_contract(profile.version).namespace
        assert profile.status == (
            "supported" if reviewed.support == "supported" else "upstream_absent"
        )
        programs = dict(profile.programs)
        if reviewed.support == "absent":
            assert not programs
            continue
        assert set(programs) == {"page", "available", "create", "delete"}
        bindings = runtime_operation_bindings(profile.version)
        owned = {
            source
            for name, binding in bindings.items()
            if name.startswith("namespace.")
            for source in binding.source_operations
        }
        assert owned == {program.source_operation for program in programs.values()}
        remaining = {operation.operation_id for operation in legacy.snapshot.operations}
        assert not owned & remaining
        assert set(bindings["user.grant.namespace"].source_operations) <= remaining
        assert set(bindings["user.revoke.namespace"].source_operations) <= remaining
        # Permission reads still consume this exact model after the four owned
        # namespace operations move out of the package-backed controller.
        assert {_ENTITY, _PAGE} <= {
            model.import_path for model in legacy.snapshot.models
        }
        assert programs["page"].source_operation == reviewed.page_operation
        assert programs["delete"].codec == (
            "delete_kubernetes"
            if reviewed.deletes_kubernetes_namespace
            else "delete_registration"
        )
        for primitive, program in programs.items():
            assert program.result_envelope == (
                "optional" if primitive in {"page", "available"} else "required"
            )
    assert sum(len(profile.programs) for profile in plan.profiles) == 104


def test_namespace_equal_delete_exchange_keeps_different_destructive_recipes(
    compiled_namespaces: CompiledDomainSet,
) -> None:
    plan = compiled_namespaces.plan("namespace")
    codecs = dict(plan.codecs)
    assert codecs["delete_kubernetes"] == codecs["delete_registration"]
    profiles = {profile.version: profile for profile in plan.profiles}
    before = dict(profiles["3.1.9"].programs)["delete"]
    after = dict(profiles["3.2.0"].programs)["delete"]
    assert before.source_operation == after.source_operation
    assert before.request_schema_digest == after.request_schema_digest
    assert before.codec != after.codec
    assert before.codec_digest != after.codec_digest
    assert profiles["3.1.9"].recipe_id != profiles["3.2.0"].recipe_id


def test_namespace_create_requests_keep_selector_quotas_and_omission(
    compiled_namespaces: CompiledDomainSet,
) -> None:
    plan = compiled_namespaces.plan("namespace")
    requests = {request.schema: request for request in plan.requests}
    assert set(requests) == {
        "page",
        "empty",
        "id",
        "create_k8s_quota",
        "create_cluster_quota",
        "create_cluster",
    }
    assert "pass" in requests["empty"].source
    for schema, selector in (
        ("create_k8s_quota", "k8s"),
        ("create_cluster_quota", "clusterCode"),
        ("create_cluster", "clusterCode"),
    ):
        request = requests[schema]
        namespace: dict[str, object] = {
            "Field": Field,
            "BaseParamsModel": BaseParamsModel,
        }
        exec(compile(request.source, f"<{schema}>", "exec"), namespace)  # noqa: S102
        model = cast("type[BaseParamsModel]", namespace[request.class_name])
        payload = {
            "namespace": "test",
            selector: "configured" if selector == "k8s" else 17,
        }
        assert (
            model.model_validate(payload).model_dump(
                exclude_none=True, exclude_unset=True
            )
            == payload
        )
        with pytest.raises(ValidationError, match=selector):
            model.model_validate({"namespace": "test"})
        if schema.endswith("quota"):
            result = model.model_validate(
                {**payload, "limitsCpu": 1.5, "limitsMemory": 2}
            )
            assert result.model_dump()["limitsCpu"] == 1.5
            assert result.model_dump()["limitsMemory"] == 2
        else:
            with pytest.raises(ValidationError, match="limitsCpu"):
                model.model_validate({**payload, "limitsCpu": 1.5})


@pytest.mark.parametrize(
    ("schema", "strict", "nullable_list"),
    [
        ("page_k8s_quota_strict", True, True),
        ("page_cluster_quota_strict", True, False),
        ("page_cluster_quota", False, False),
        ("page_cluster", False, False),
    ],
)
def test_namespace_page_response_epochs_preserve_defaults_and_strictness(
    compiled_namespaces: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    *,
    strict: bool,
    nullable_list: bool,
) -> None:
    adapter = response_adapter(
        compiled_namespaces.plan("namespace"), schema, monkeypatch
    )
    defaults = adapter.dump_python(adapter.validate_python({}))
    assert isinstance(defaults, dict)
    assert defaults["totalList"] == (None if nullable_list else [])
    assert defaults["pageSize"] == 20
    assert defaults["currentPage"] == defaults["total"] == 0
    if strict:
        with pytest.raises(ValidationError, match="total"):
            adapter.validate_python({"total": "2"})
    else:
        result = adapter.dump_python(adapter.validate_python({"total": "2"}))
        assert isinstance(result, dict)
        assert result["total"] == 2


@pytest.mark.parametrize(
    "schema", ["available_k8s_quota", "available_cluster_quota", "available_cluster"]
)
def test_namespace_available_preserves_list_root_and_element_defaults(
    compiled_namespaces: CompiledDomainSet,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
) -> None:
    adapter = response_adapter(
        compiled_namespaces.plan("namespace"), schema, monkeypatch
    )
    assert adapter.validate_python([]) == []
    invalid: object
    for invalid in (None, {}, [3]):
        with pytest.raises(ValidationError):
            adapter.validate_python(invalid)
    result = adapter.dump_python(adapter.validate_python([{}]))
    assert isinstance(result, list)
    assert result[0]["id"] is None
    assert result[0]["userId"] == 0
    if schema.endswith("quota"):
        assert result[0]["limitsCpu"] is None
        assert result[0]["podRequestCpu"] == 0.0
    else:
        assert "limitsCpu" not in result[0]


@pytest.mark.parametrize(
    "drift",
    ["selector", "quotas", "create_result", "create_side_effect", "delete_side_effect"],
)
def test_namespace_rejects_governance_recipe_and_wire_disagreement(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    contract = governance_contract("3.2.0")
    if drift == "selector":
        recipe = replace(contract.namespace, selector="k8s")
        message = "reviewed selector or quotas"
    elif drift == "quotas":
        recipe = replace(contract.namespace, quotas_supported=False)
        message = "reviewed selector or quotas"
    elif drift == "create_result":
        recipe = replace(contract.namespace, create_result="entity")
        message = "create response changed"
    elif drift == "create_side_effect":
        recipe = replace(
            contract.namespace, creates_kubernetes_namespace_if_absent=False
        )
        message = "reviewed governance recipe changed"
    else:
        recipe = replace(contract.namespace, deletes_kubernetes_namespace=True)
        message = "recipe is unsupported"

    def reviewed(version: str) -> GovernanceVersionContract:
        return (
            replace(contract, namespace=recipe)
            if version == "3.2.0"
            else governance_contract(version)
        )

    monkeypatch.setattr("ds_codegen.compiled_namespaces.governance_contract", reviewed)
    with pytest.raises(ValueError, match=message):
        compile_domains(exact_runtime_bundles, (NAMESPACE_COMPILED_DOMAIN,))


@pytest.mark.parametrize(
    "drift", ["quota_type", "quota_required", "delete_method", "available_type"]
)
def test_namespace_rejects_request_and_response_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    drift: str,
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.0.0"
    )
    method = {
        "quota_type": "createNamespace",
        "quota_required": "createNamespace",
        "delete_method": "delNamespaceById",
        "available_type": "queryAvailableNamespaceList",
    }[drift]
    operation = next(
        item
        for item in bundle.snapshot.operations
        if item.operation_id == f"K8sNamespaceController.{method}"
    )
    if drift == "quota_type":
        changed = replace(
            operation,
            parameters=[
                replace(parameter, java_type="String")
                if parameter.wire_name == "limitsCpu"
                else parameter
                for parameter in operation.parameters
            ],
        )
        message = "request type or default changed"
    elif drift == "quota_required":
        changed = replace(
            operation,
            parameters=[
                replace(parameter, required=True)
                if parameter.wire_name == "limitsCpu"
                else parameter
                for parameter in operation.parameters
            ],
        )
        message = "matching epochs"
    elif drift == "delete_method":
        changed = replace(operation, http_method="DELETE")
        message = "delete request shape has 0 matching epochs"
    else:
        changed = replace(operation, logical_return_type=f"Optional<List<{_ENTITY}>>")
        message = "available response changed"
    with pytest.raises(ValueError, match=message):
        compile_domains(
            replace_operation(exact_runtime_bundles, "3.0.0", changed),
            (NAMESPACE_COMPILED_DOMAIN,),
        )


@pytest.mark.parametrize(
    ("model_path", "field_name"), [(_ENTITY, "clusterCode"), (_PAGE, "totalList")]
)
def test_namespace_rejects_response_field_drift(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    model_path: str,
    field_name: str,
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
                            replace(field, nullable=not field.nullable)
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
        if bundle.spec.version == "3.2.0"
        else bundle
        for bundle in exact_runtime_bundles
    )
    with pytest.raises(ValueError, match="response"):
        compile_domains(bundles, (NAMESPACE_COMPILED_DOMAIN,))
