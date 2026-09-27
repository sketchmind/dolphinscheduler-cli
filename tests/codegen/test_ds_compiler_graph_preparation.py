"""Graph preparation is invocation-local and preserves exact source validation."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from ds_codegen.compiled_domains import compile_domains
from ds_codegen.compiled_queues import QUEUE_COMPILED_DOMAIN
from ds_codegen.compiled_tenants import TENANT_COMPILED_DOMAIN
from ds_codegen.render.package import executable_schema, planner
from ds_codegen.runtime_bundles import _COMPILED_OPERATION_DEPENDENCIES
from ds_codegen.snapshot_resolution import SnapshotResolutionError

if TYPE_CHECKING:
    from ds_codegen.ir import ContractSnapshot
    from ds_codegen.runtime_bundles import RuntimeBundle

pytestmark = pytest.mark.source_contract
_DOMAINS = (TENANT_COMPILED_DOMAIN, QUEUE_COMPILED_DOMAIN)


def test_compiler_prepares_each_source_graph_once_across_domains_and_operations(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    selected = tuple(
        bundle
        for bundle in exact_runtime_bundles
        if bundle.spec.version in {"3.4.1", "3.4.2"}
    )
    assert len(selected) == 2
    calls = dict.fromkeys((id(bundle.snapshot) for bundle in selected), 0)
    original = planner.build_package_context

    def counted(snapshot: ContractSnapshot) -> planner.PackageRenderContext:
        if id(snapshot) in calls:
            calls[id(snapshot)] += 1
        return original(snapshot)

    monkeypatch.setattr(planner, "build_package_context", counted)
    for invocation in (1, 2):
        result = compile_domains(
            selected,
            _DOMAINS,
            operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
        )
        assert tuple(profile.version for profile in result.plan("tenant").profiles) == (
            "3.4.1",
            "3.4.2",
        )
        assert set(calls.values()) == {invocation}


@pytest.mark.parametrize("kind", ["request", "response"])
def test_prepared_rendering_matches_fresh_rendering_and_rejects_foreign_snapshot(
    exact_runtime_bundles: tuple[RuntimeBundle, ...], kind: str
) -> None:
    snapshot = next(
        bundle.snapshot
        for bundle in exact_runtime_bundles
        if bundle.spec.version == "3.4.1"
    )
    operation = next(
        item
        for item in snapshot.operations
        if item.operation_id == "QueueController.queryQueueListPaging"
    )
    context = planner.build_package_context(snapshot)
    if kind == "request":
        render = executable_schema.render_operation_request_params
        expected = render(snapshot, operation, class_name="Params")
        assert (
            render(snapshot, operation, class_name="Params", source_context=context)
            == expected
        )
        with pytest.raises(ValueError, match="different snapshot"):
            render(
                replace(snapshot),
                operation,
                class_name="Params",
                source_context=context,
            )
    else:
        response_render = executable_schema.render_operation_response_module
        parts = ("wire_programs", "_schemas", "queue", "probe")
        roots = ("wire_runtime", "_models")
        expected_response = response_render(
            snapshot, operation, module_parts=parts, root_model_module_parts=roots
        )
        assert (
            response_render(
                snapshot,
                operation,
                module_parts=parts,
                root_model_module_parts=roots,
                source_context=context,
            )
            == expected_response
        )
        with pytest.raises(ValueError, match="different snapshot"):
            response_render(
                replace(snapshot),
                operation,
                module_parts=parts,
                root_model_module_parts=roots,
                source_context=context,
            )


def test_new_invocation_revalidates_mutated_snapshot_and_recovers_after_failure(
    exact_runtime_bundles: tuple[RuntimeBundle, ...],
) -> None:
    bundle = next(
        bundle for bundle in exact_runtime_bundles if bundle.spec.version == "3.4.1"
    )
    expected = compile_domains(
        (bundle,), _DOMAINS, operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES
    )
    bundle.snapshot.models.append(bundle.snapshot.models[0])
    try:
        with pytest.raises(SnapshotResolutionError, match="multiple surfaces"):
            compile_domains(
                (bundle,),
                _DOMAINS,
                operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES,
            )
    finally:
        bundle.snapshot.models.pop()
    assert (
        compile_domains(
            (bundle,), _DOMAINS, operation_dependencies=_COMPILED_OPERATION_DEPENDENCIES
        )
        == expected
    )
