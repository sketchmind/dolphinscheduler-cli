from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, cast

import pytest

from ds_codegen import compiled_domains
from ds_codegen.compatibility_impact import REVIEWED_DS_VERSIONS, WireTypeRef
from ds_codegen.compiled_domains import (
    CompiledOperationDependency,
    _local_binding_roots,
    _require_exclusive_source_ownership,
    _validate_definitions,
    _validate_operation_dependencies,
    compile_domains,
)
from ds_codegen.compiled_queues import QUEUE_COMPILED_DOMAIN
from ds_codegen.compiled_tenants import TENANT_COMPILED_DOMAIN
from ds_codegen.runtime_contract import (
    PROJECT_PREFERENCE_READ_VERSIONS,
    runtime_auxiliary_operation_bindings,
    runtime_operation_bindings,
    slice_contract_for_bindings,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.compatibility_impact import ReviewedBinding
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract

_DEPENDENCIES = {
    "tenant.create": ("queue.page",),
    "tenant.update": ("queue.page",),
    "user.create": ("tenant.page",),
    "user.update": ("tenant.page",),
}
_TENANT = "org.apache.dolphinscheduler.dao.entity.Tenant"
_QUEUE_PAGE = "QueueController.queryQueueListPaging"
_QUEUE = "org.apache.dolphinscheduler.dao.entity.Queue"
_WORKFLOW_DEPENDENCY_CLAUSES = (
    CompiledOperationDependency(
        providers=("project.get",), versions=frozenset(REVIEWED_DS_VERSIONS)
    ),
    CompiledOperationDependency(
        providers=("project-preference.read",),
        versions=PROJECT_PREFERENCE_READ_VERSIONS,
    ),
)


def test_tenant_owns_only_its_operations_while_user_delegates_to_its_port(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    result = compile_domains(
        exact_runtime_bundles,
        (TENANT_COMPILED_DOMAIN,),
        operation_dependencies=_DEPENDENCIES,
    )

    for original, remaining in zip(
        exact_runtime_bundles, result.legacy_bundles, strict=True
    ):
        profile = result.plan("tenant").profile(original.spec.version)
        assert len(profile.programs) == 4
        assert all(
            program.source_operation.startswith("TenantController.")
            for _primitive, program in profile.programs
        )
        operations = {
            operation.operation_id for operation in remaining.snapshot.operations
        }
        assert _QUEUE_PAGE in operations
        assert not any(
            operation.startswith("TenantController.") for operation in operations
        )
        assert {"user.create", "user.update", "queue.page"} <= set(
            remaining.metadata.semantic_operations
        )
        assert _TENANT not in {model.import_path for model in remaining.snapshot.models}
        assert (
            remaining.metadata.source_contract_digest
            == original.metadata.source_contract_digest
        )
        assert (
            profile.source_contract_digest == original.metadata.source_contract_digest
        )


def test_joint_tenant_and_queue_ownership_removes_only_the_delegated_wire_closures(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    result = compile_domains(
        exact_runtime_bundles,
        (TENANT_COMPILED_DOMAIN, QUEUE_COMPILED_DOMAIN),
        operation_dependencies=_DEPENDENCIES,
    )
    for original, remaining in zip(
        exact_runtime_bundles, result.legacy_bundles, strict=True
    ):
        version = original.spec.version
        queue = result.plan("queue").profile(version)
        expected_queue_count = (
            3
            if version
            in {
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
                "3.0.0",
                "3.0.1",
                "3.0.2",
                "3.0.3",
                "3.0.4",
                "3.0.5",
                "3.0.6",
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
            }
            else 4
        )
        assert len(queue.programs) == expected_queue_count
        assert len(result.plan("tenant").profile(version).programs) == 4
        assert all(
            program.source_operation.startswith("QueueController.")
            for _primitive, program in queue.programs
        )
        assert not any(
            operation.operation_id.startswith(("TenantController.", "QueueController."))
            for operation in remaining.snapshot.operations
        )
        assert not {_TENANT, _QUEUE} & {
            model.import_path for model in remaining.snapshot.models
        }
        assert {"user.create", "user.update"} <= set(
            remaining.metadata.semantic_operations
        )
        assert queue.source_contract_digest == original.metadata.source_contract_digest
        assert (
            remaining.metadata.source_contract_digest
            == original.metadata.source_contract_digest
        )


@pytest.mark.parametrize(
    "declaration",
    [
        {"queue.unknown": frozenset({"1.3.9"})},
        {"queue.delete": frozenset({"9.9.9"})},
        {"queue.delete": frozenset()},
        {"queue.delete": {"1.3.9"}},
    ],
)
def test_semantic_absence_declarations_are_explicit_and_exact(
    declaration: dict[str, object],
) -> None:
    malformed = replace(
        QUEUE_COMPILED_DOMAIN,
        semantic_absent_versions=cast("Mapping[str, frozenset[str]]", declaration),
    )
    with pytest.raises(ValueError, match="semantic absence declaration is invalid"):
        _validate_definitions((malformed,))


def test_semantic_absence_cannot_redeclare_whole_domain_absence() -> None:
    malformed = replace(QUEUE_COMPILED_DOMAIN, absent_versions=frozenset({"1.3.9"}))
    with pytest.raises(ValueError, match="semantic absence declaration is invalid"):
        _validate_definitions((malformed,))


@pytest.mark.parametrize("incorrect", ["missing", "extra"])
def test_semantic_absence_must_match_bindings_without_primitive_inference(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    incorrect: str,
) -> None:
    declaration = {}
    if incorrect == "extra":
        declaration = {
            "queue.delete": QUEUE_COMPILED_DOMAIN.semantic_absent_versions[
                "queue.delete"
            ]
            | {"3.4.1"}
        }
    malformed = replace(QUEUE_COMPILED_DOMAIN, semantic_absent_versions=declaration)
    with pytest.raises(ValueError, match="semantic binding inventory is incomplete"):
        compile_domains(
            exact_runtime_bundles, (malformed,), operation_dependencies=_DEPENDENCIES
        )


def test_semantic_absence_cannot_hide_an_unowned_existing_source_operation(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing_delete_binding(version: str) -> dict[str, ReviewedBinding]:
        bindings = dict(runtime_operation_bindings(version))
        if version == "3.4.1":
            bindings.pop("queue.delete")
        return bindings

    monkeypatch.setattr(
        compiled_domains, "runtime_operation_bindings", missing_delete_binding
    )
    malformed = replace(
        QUEUE_COMPILED_DOMAIN,
        semantic_absent_versions={
            "queue.delete": QUEUE_COMPILED_DOMAIN.semantic_absent_versions[
                "queue.delete"
            ]
            | {"3.4.1"}
        },
    )
    with pytest.raises(ValueError, match="unowned classified operations"):
        compile_domains(
            exact_runtime_bundles, (malformed,), operation_dependencies=_DEPENDENCIES
        )


@pytest.mark.parametrize(
    ("dependencies", "message"),
    [
        ({"unknown.create": ("queue.page",)}, "consumer is invalid"),
        ({"tenant.create": ("queue.unknown",)}, "provider is missing"),
        ({"tenant.create": ("tenant.page",)}, "must cross domains"),
        ({"tenant.create": ("user.list",)}, "not covered"),
        ({"tenant.create": ("queue.page", "queue.page")}, "consumer is invalid"),
        ({"tenant.create": ("queue.page",), "queue.page": ("tenant.create",)}, "cycle"),
    ],
)
def test_dependency_projection_rejects_unreviewed_edges(
    dependencies: dict[str, tuple[str, ...]],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_operation_dependencies(
            "3.4.1", runtime_operation_bindings("3.4.1"), dependencies
        )


def test_reviewed_dependency_skips_a_truly_absent_consumer() -> None:
    dependencies = {"project-parameter.get": ("project.get",)}
    assert "project-parameter.get" not in runtime_operation_bindings("1.3.9")
    assert (
        _validate_operation_dependencies(
            "1.3.9", runtime_operation_bindings("1.3.9"), dependencies
        )
        == {}
    )
    assert (
        _validate_operation_dependencies(
            "3.4.1", runtime_operation_bindings("3.4.1"), dependencies
        )
        == dependencies
    )


def test_absent_dependency_consumer_cannot_hide_an_unknown_provider() -> None:
    with pytest.raises(ValueError, match="provider is missing"):
        _validate_operation_dependencies(
            "1.3.9",
            runtime_operation_bindings("1.3.9"),
            {"project-parameter.get": ("project.unknown",)},
        )


def test_present_dependency_consumer_requires_its_exact_provider() -> None:
    bindings = dict(runtime_operation_bindings("3.4.1"))
    bindings.pop("project.get")
    with pytest.raises(ValueError, match="provider is missing"):
        _validate_operation_dependencies(
            "3.4.1", bindings, {"project-parameter.get": ("project.get",)}
        )


def test_gated_dependency_uses_only_its_explicit_exact_versions() -> None:
    providers = ("queue.page",)
    dependencies = {
        "tenant.create": CompiledOperationDependency(
            providers=providers, versions=frozenset({"3.4.1"})
        )
    }
    early_bindings = runtime_operation_bindings("1.3.9")
    assert {"tenant.create", "queue.page"} <= early_bindings.keys()
    assert _validate_operation_dependencies("1.3.9", early_bindings, dependencies) == {}
    assert _validate_operation_dependencies(
        "3.4.1", runtime_operation_bindings("3.4.1"), dependencies
    ) == {"tenant.create": providers}


@pytest.mark.parametrize("versions", [frozenset(), frozenset({"9.9.9"}), {"3.4.1"}])
def test_gated_dependency_versions_must_be_nonempty_exact_and_immutable(
    versions: object,
) -> None:
    with pytest.raises(ValueError, match="versions are invalid"):
        _validate_operation_dependencies(
            "1.3.9",
            runtime_operation_bindings("1.3.9"),
            {
                "tenant.create": CompiledOperationDependency(
                    providers=("queue.page",), versions=cast("frozenset[str]", versions)
                )
            },
        )


@pytest.mark.parametrize(
    ("consumer", "providers", "message"),
    [
        ("unknown.create", ("queue.page",), "consumer is invalid"),
        ("tenant.create", ("queue.unknown",), "provider is missing"),
        ("tenant.create", ("tenant.page",), "must cross domains"),
        ("tenant.create", (), "consumer is invalid"),
        ("tenant.create", ("queue.page", "queue.page"), "consumer is invalid"),
    ],
)
def test_inactive_gated_dependency_still_requires_a_valid_declaration(
    consumer: str, providers: tuple[str, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_operation_dependencies(
            "1.3.9",
            runtime_operation_bindings("1.3.9"),
            {
                consumer: CompiledOperationDependency(
                    providers=providers, versions=frozenset({"3.4.1"})
                )
            },
        )


def test_inactive_gated_dependency_does_not_authorize_a_chain() -> None:
    with pytest.raises(ValueError, match="chains and cycles are unsupported"):
        _validate_operation_dependencies(
            "1.3.9",
            runtime_operation_bindings("1.3.9"),
            {
                "tenant.create": CompiledOperationDependency(
                    providers=("queue.page",), versions=frozenset({"3.4.1"})
                ),
                "queue.page": ("user.list",),
            },
        )


def test_gated_dependency_cannot_select_an_absent_consumer() -> None:
    with pytest.raises(ValueError, match="selects an absent consumer"):
        _validate_operation_dependencies(
            "1.3.9",
            runtime_operation_bindings("1.3.9"),
            {
                "project-parameter.get": CompiledOperationDependency(
                    providers=("project.get",), versions=frozenset({"1.3.9"})
                )
            },
        )


@pytest.mark.parametrize(
    "consumer", ["workflow.run", "workflow.backfill", "workflow.run-task"]
)
def test_dependency_clauses_select_independent_provider_epochs(consumer: str) -> None:
    for version in REVIEWED_DS_VERSIONS:
        bindings = {
            **runtime_operation_bindings(version),
            **runtime_auxiliary_operation_bindings(version),
        }
        expected: tuple[str, ...] = ("project.get",)
        if "project-preference.read" in bindings:
            expected += ("project-preference.read",)
        assert _validate_operation_dependencies(
            version, bindings, {consumer: _WORKFLOW_DEPENDENCY_CLAUSES}
        ) == {consumer: expected}


def test_dependency_clauses_do_not_infer_activation_from_provider_presence() -> None:
    clauses = (
        _WORKFLOW_DEPENDENCY_CLAUSES[0],
        replace(_WORKFLOW_DEPENDENCY_CLAUSES[1], versions=frozenset({"3.4.1"})),
    )
    bindings = {
        **runtime_operation_bindings("3.4.2"),
        **runtime_auxiliary_operation_bindings("3.4.2"),
    }
    assert "project-preference.read" in bindings
    assert _validate_operation_dependencies(
        "3.4.2", bindings, {"workflow.run": clauses}
    ) == {"workflow.run": ("project.get",)}


@pytest.mark.parametrize("versions", [frozenset({"1.3.9"}), frozenset({"3.4.1"})])
def test_dependency_clauses_reject_repeated_providers_even_in_disjoint_epochs(
    versions: frozenset[str],
) -> None:
    clauses = (
        replace(_WORKFLOW_DEPENDENCY_CLAUSES[0], versions=frozenset({"1.3.9"})),
        replace(_WORKFLOW_DEPENDENCY_CLAUSES[0], versions=versions),
    )
    with pytest.raises(ValueError, match="consumer is invalid"):
        _validate_operation_dependencies(
            "3.4.2", runtime_operation_bindings("3.4.2"), {"workflow.run": clauses}
        )


@pytest.mark.parametrize(
    "declaration",
    [
        (),
        ("project.get", _WORKFLOW_DEPENDENCY_CLAUSES[1]),
        list(_WORKFLOW_DEPENDENCY_CLAUSES),
    ],
)
def test_dependency_clauses_reject_empty_mixed_or_mutable_declarations(
    declaration: object,
) -> None:
    with pytest.raises(ValueError, match="consumer is invalid"):
        _validate_operation_dependencies(
            "3.4.1",
            runtime_operation_bindings("3.4.1"),
            {
                "workflow.run": cast(
                    "compiled_domains.CompiledDependencyDeclaration", declaration
                )
            },
        )


@pytest.mark.parametrize("drift", ["unknown", "chain"])
def test_dependency_clauses_validate_inactive_providers_before_projection(
    drift: str,
) -> None:
    declarations: dict[str, compiled_domains.CompiledDependencyDeclaration] = {
        "workflow.run": _WORKFLOW_DEPENDENCY_CLAUSES
    }
    if drift == "unknown":
        declarations["workflow.run"] = (
            _WORKFLOW_DEPENDENCY_CLAUSES[0],
            replace(_WORKFLOW_DEPENDENCY_CLAUSES[1], providers=("project.unknown",)),
        )
        message = "provider is missing"
    else:
        declarations["project-preference.read"] = ("project.get",)
        message = "chains and cycles are unsupported"
    with pytest.raises(ValueError, match=message):
        _validate_operation_dependencies(
            "1.3.9", runtime_operation_bindings("1.3.9"), declarations
        )


def test_dependency_clauses_require_every_active_provider() -> None:
    bindings = dict(runtime_operation_bindings("3.4.1"))
    assert "project-preference.read" not in bindings
    with pytest.raises(ValueError, match="provider is missing"):
        _validate_operation_dependencies(
            "3.4.1", bindings, {"workflow.run": _WORKFLOW_DEPENDENCY_CLAUSES}
        )


@pytest.mark.parametrize(
    ("drift", "message"),
    [("overlap", "ownership is ambiguous"), ("all_sources", "leaves no local source")],
)
def test_dependency_clauses_validate_combined_source_ownership(
    drift: str, message: str
) -> None:
    bindings = dict(runtime_operation_bindings("3.4.1"))
    project = bindings["project.get"]
    if drift == "overlap":
        bindings["environment.page"] = project
    else:
        consumer = bindings["workflow.run"]
        bindings["environment.page"] = replace(
            consumer,
            source_operations=tuple(
                source
                for source in consumer.source_operations
                if source not in project.source_operations
            ),
        )
    clauses = (
        _WORKFLOW_DEPENDENCY_CLAUSES[0],
        CompiledOperationDependency(
            providers=("environment.page",), versions=frozenset({"3.4.1"})
        ),
    )
    with pytest.raises(ValueError, match=message):
        _validate_operation_dependencies("3.4.1", bindings, {"workflow.run": clauses})


def test_multiple_providers_cannot_claim_one_delegated_source() -> None:
    bindings = dict(runtime_operation_bindings("3.4.1"))
    bindings["environment.page"] = bindings["queue.page"]
    with pytest.raises(ValueError, match="ownership is ambiguous"):
        _validate_operation_dependencies(
            "3.4.1", bindings, {"tenant.create": ("queue.page", "environment.page")}
        )


def test_dependency_cannot_remove_every_local_source() -> None:
    bindings = dict(runtime_operation_bindings("3.4.1"))
    bindings["environment.page"] = bindings["tenant.create"]
    with pytest.raises(ValueError, match="leaves no local source"):
        _validate_operation_dependencies(
            "3.4.1", bindings, {"tenant.create": ("environment.page",)}
        )


def test_new_undeclared_source_overlap_still_has_multiple_owners() -> None:
    bindings = {
        **runtime_operation_bindings("3.4.1"),
        **runtime_auxiliary_operation_bindings("3.4.1"),
    }
    tenant = {
        name: binding
        for name, binding in bindings.items()
        if name.startswith("tenant.")
    }
    legacy = {name: binding for name, binding in bindings.items() if name not in tenant}
    unexpected = bindings["tenant.page"].source_operations[0]
    legacy["user.list"] = replace(
        legacy["user.list"],
        source_operations=(*legacy["user.list"].source_operations, unexpected),
    )
    with pytest.raises(ValueError, match="multiple owners"):
        _require_exclusive_source_ownership(
            "3.4.1",
            compiled_by_domain={"tenant": tenant},
            legacy=legacy,
            all_bindings=bindings,
            operation_dependencies=_DEPENDENCIES,
        )


def test_delegated_source_requires_a_remaining_executable_owner() -> None:
    bindings = runtime_operation_bindings("3.4.1")
    tenant = {
        name: binding
        for name, binding in bindings.items()
        if name.startswith("tenant.")
    }
    legacy = {
        name: binding
        for name, binding in bindings.items()
        if name not in tenant and not name.startswith("queue.")
    }
    with pytest.raises(ValueError, match="ownership is incomplete"):
        _require_exclusive_source_ownership(
            "3.4.1",
            compiled_by_domain={"tenant": tenant},
            legacy=legacy,
            all_bindings=bindings,
            operation_dependencies=_DEPENDENCIES,
        )


@pytest.mark.parametrize("still_referenced", [False, True])
def test_dependency_only_types_are_removed_unless_local_wire_still_references_them(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    *,
    still_referenced: bool,
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    all_bindings = runtime_operation_bindings("3.4.1")
    bindings = {"user.create": all_bindings["user.create"]}
    operations, types = _local_binding_roots(
        bindings, all_bindings=all_bindings, operation_dependencies=_DEPENDENCIES
    )
    snapshot = bundle.snapshot
    if still_referenced:
        snapshot = replace(
            snapshot,
            operations=[
                replace(operation, logical_return_type=_TENANT)
                if operation.operation_id == "UsersController.createUser"
                else operation
                for operation in snapshot.operations
            ],
        )
    sliced = slice_contract_for_bindings(
        snapshot,
        bindings,
        execution_operation_ids=operations,
        execution_type_refs=types,
    )

    assert (
        _TENANT in {model.import_path for model in sliced.models}
    ) is still_referenced
    assert not any(
        operation.operation_id.startswith("TenantController.")
        for operation in sliced.operations
    )


@pytest.mark.parametrize("missing", ["operation", "type"])
def test_execution_projection_still_requires_the_complete_reviewed_dependency_evidence(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    missing: str,
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    all_bindings = runtime_operation_bindings("3.4.1")
    bindings = {"user.create": all_bindings["user.create"]}
    operations, types = _local_binding_roots(
        bindings, all_bindings=all_bindings, operation_dependencies=_DEPENDENCIES
    )
    snapshot = bundle.snapshot
    if missing == "operation":
        snapshot = replace(
            snapshot,
            operations=[
                operation
                for operation in snapshot.operations
                if not operation.operation_id.startswith("TenantController.")
            ],
        )
    else:
        snapshot = replace(
            snapshot,
            models=[model for model in snapshot.models if model.import_path != _TENANT],
        )
    message = (
        "runtime contract slice is incomplete"
        if missing == "operation"
        else "no exact target"
    )
    with pytest.raises(ValueError, match=message):
        slice_contract_for_bindings(
            snapshot,
            bindings,
            execution_operation_ids=operations,
            execution_type_refs=types,
        )


@pytest.mark.parametrize("invalid", ["unpaired", "operation", "type"])
def test_execution_projection_cannot_widen_its_reviewed_authority(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
    invalid: str,
) -> None:
    snapshot = next(
        bundle.snapshot
        for bundle in exact_runtime_bundles
        if bundle.spec.version == "3.4.1"
    )
    all_bindings = runtime_operation_bindings("3.4.1")
    bindings = {"user.create": all_bindings["user.create"]}
    operations, types = _local_binding_roots(
        bindings, all_bindings=all_bindings, operation_dependencies=_DEPENDENCIES
    )
    if invalid == "operation":
        operations.add("QueueController.queryQueueListPaging")
    elif invalid == "type":
        types.add(WireTypeRef("models", "org.example.Unreviewed"))

    with pytest.raises(ValueError, match="runtime execution projection"):
        slice_contract_for_bindings(
            snapshot,
            bindings,
            execution_operation_ids=operations,
            execution_type_refs=None if invalid == "unpaired" else types,
        )
