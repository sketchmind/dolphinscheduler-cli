"""Preparation helpers for compiled-domain contract tests."""

from __future__ import annotations

import sys
from dataclasses import replace
from types import ModuleType
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

if TYPE_CHECKING:
    import pytest

    from ds_codegen.compiled_domains import CompiledDomainPlan
    from ds_codegen.compiled_wire_artifacts import CompiledWireModule
    from ds_codegen.ir import OperationSpec
    from ds_codegen.runtime_bundles import RuntimeBundle


def response_adapter(
    plan: CompiledDomainPlan, schema: str, monkeypatch: pytest.MonkeyPatch
) -> TypeAdapter[object]:
    response = next(item for item in plan.responses if item.schema == schema)
    load_schema_pool(response.pool_modules, monkeypatch)
    module_name = f"dsctl.generated.wire_programs._test_{response.module_name}"
    module = ModuleType(module_name)
    module.__package__ = (
        f"dsctl.generated.wire_programs.{response.module_name.rpartition('.')[0]}"
    )
    monkeypatch.setitem(sys.modules, module_name, module)
    exec(compile(response.content, f"<{module_name}>", "exec"), module.__dict__)  # noqa: S102
    return TypeAdapter(module.RESPONSE_TYPE)


def load_schema_pool(
    modules: tuple[CompiledWireModule, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Load only trusted compiler-produced implementation dependencies in tests."""
    for dependency in modules:
        name = f"dsctl.generated.wire_programs.{dependency.name}"
        module = ModuleType(name)
        module.__package__ = name.rpartition(".")[0]
        monkeypatch.setitem(sys.modules, name, module)
        exec(compile(dependency.content, f"<{name}>", "exec"), module.__dict__)  # noqa: S102


def single_version_bundles(
    bundles: tuple[RuntimeBundle, ...], version: str
) -> tuple[RuntimeBundle, ...]:
    """Select one exact input for drift checks independent of other versions."""
    return (next(bundle for bundle in bundles if bundle.spec.version == version),)


def replace_operation(
    bundles: tuple[RuntimeBundle, ...], version: str, changed: OperationSpec
) -> tuple[RuntimeBundle, ...]:
    return tuple(
        replace(
            bundle,
            snapshot=replace(
                bundle.snapshot,
                operations=[
                    changed
                    if operation.operation_id == changed.operation_id
                    else operation
                    for operation in bundle.snapshot.operations
                ],
            ),
        )
        if bundle.spec.version == version
        else bundle
        for bundle in bundles
    )
