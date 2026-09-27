from __future__ import annotations

import re
import sys
from dataclasses import dataclass, replace
from types import ModuleType
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field, TypeAdapter

from ds_codegen.compiled_schema_pool import pool_schema_enums
from ds_codegen.contract_inputs import canonical_json_digest
from ds_codegen.ir import ContractSnapshot, DtoFieldSpec, DtoSpec, EnumSpec, ModelSpec
from ds_codegen.operation_paths import executable_path_operation
from ds_codegen.render.package import planner
from ds_codegen.render.package.operations_renderer import (
    operation_response_use,
    response_adapter_annotation,
)
from ds_codegen.render.package.operations_renderer import (
    render_operation_request_params as render_request_params_source,
)
from ds_codegen.render.package.renderer_deps import (
    operation_render_deps,
    type_render_deps,
)
from ds_codegen.render.package.type_renderer import render_type_module
from ds_codegen.render.package.type_support import (
    relative_import_statement,
    render_annotation_type,
)
from ds_codegen.snapshot_resolution import (
    ResolutionScope,
    ScopedTypeUse,
)
from dsctl.generated.wire_runtime.api.operations._base import BaseParamsModel

if TYPE_CHECKING:
    from ds_codegen.compiled_wire_artifacts import CompiledWireModule
    from ds_codegen.ir import OperationSpec
    from ds_codegen.render.package.planner import PackageRenderContext
    from dsctl.upstream._generated_types import OpaqueGeneratedValue

_RenderableSpec = EnumSpec | DtoSpec | ModelSpec


@dataclass(frozen=True)
class RenderedOperationResponse:
    """One executable response closure rendered from exact operation IR."""

    adapter_annotation: str
    source: str
    import_paths: tuple[str, ...]
    executable_digest: str
    pool_modules: tuple[CompiledWireModule, ...] = ()


@dataclass(frozen=True)
class RenderedOperationRequest:
    """One executable request model rendered from exact operation IR."""

    class_name: str
    source: str
    support_source: str | None
    executable_digest: str
    pool_modules: tuple[CompiledWireModule, ...] = ()


def render_operation_request_params(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    class_name: str,
    parameter_bindings: tuple[
        Literal["path_variable", "request_body", "request_param"], ...
    ] = ("request_param",),
    module_parts: tuple[str, ...] = ("wire_programs", "_request_schema_probe"),
    root_model_module_parts: tuple[str, ...] = ("wire_runtime", "_models"),
    source_context: PackageRenderContext | None = None,
) -> RenderedOperationRequest:
    """Render one channel's request model from exact IR without doc metadata."""
    if not parameter_bindings or len(parameter_bindings) != len(
        set(parameter_bindings)
    ):
        message = f"operation {operation.operation_id} request bindings are invalid"
        raise ValueError(message)
    if "path_variable" in parameter_bindings:
        operation = executable_path_operation(operation)
    sanitized = replace(
        operation,
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        parameters=[
            replace(
                parameter,
                binding=(
                    "request_param"
                    if parameter.binding in parameter_bindings
                    else parameter.binding
                ),
                description=None,
                example=None,
                allowable_values=None,
            )
            for parameter in operation.parameters
        ],
    )
    selected_parameters = tuple(
        parameter
        for parameter in operation.parameters
        if parameter.binding in parameter_bindings
    )
    scope = ResolutionScope("operation_request", operation.operation_id)
    context = _source_context(snapshot, source_context)
    graph = context.type_resolver.resolve_type_graph(
        tuple(
            ScopedTypeUse(parameter.java_type, scope)
            for parameter in selected_parameters
        )
    )
    support_source: str | None = None
    closure_source = ""
    if graph.import_paths:
        request_snapshot = _request_only_snapshot(
            snapshot,
            sanitized,
            import_paths=graph.import_paths,
        )
        context, original_module_parts = _flattened_context(
            request_snapshot,
            module_parts=module_parts,
        )
        _require_unique_flattened_exports(context)
        closure_source = render_type_module(
            module_parts=module_parts,
            specs=[
                *request_snapshot.enums,
                *request_snapshot.dtos,
                *request_snapshot.models,
            ],
            specialized_models=list(context.specialized_by_java_type.values()),
            dto_specs_by_import_path={
                item.import_path: item for item in request_snapshot.dtos
            },
            model_specs_by_import_path={
                item.import_path: item for item in request_snapshot.models
            },
            context=context,
            deps=replace(
                type_render_deps(),
                root_model_module_parts=root_model_module_parts,
                model_role_module_parts=original_module_parts,
            ),
        )
    source = render_request_params_source(
        sanitized,
        context,
        deps=operation_render_deps(),
        class_name=class_name,
        include_docstring=False,
        honor_parameter_defaults=True,
    )
    uses_native_default_schema = "GetPydanticSchema(" in source
    if graph.import_paths or uses_native_default_schema:
        support_source = "\n".join(
            (
                closure_source.rstrip(),
                "",
                *(
                    (
                        "from typing import Annotated, Literal",
                        "from pydantic import Field, GetPydanticSchema",
                    )
                    if uses_native_default_schema
                    else ("from pydantic import Field",)
                ),
                relative_import_statement(
                    module_parts,
                    (*root_model_module_parts[:-1], "api", "operations", "_base"),
                    "BaseParamsModel",
                ),
                "",
                source,
                "",
            )
        )
    executable_source = support_source or source
    package_name = ".".join(("dsctl", "generated", *module_parts[:-1]))
    module_name = f"{package_name}._request_schema_probe"
    namespace: dict[str, OpaqueGeneratedValue] = {
        "__name__": module_name,
        "BaseParamsModel": BaseParamsModel,
        "Field": Field,
    }
    probe_module = ModuleType(module_name)
    probe_module.__package__ = package_name
    probe_module.__dict__.update(namespace)
    previous_module = sys.modules.get(module_name)
    sys.modules[module_name] = probe_module
    try:
        exec(  # noqa: S102 - execute only deterministic source emitted above.
            compile(executable_source, f"<{operation.operation_id}-request>", "exec"),
            probe_module.__dict__,
        )
    finally:
        if previous_module is None:
            del sys.modules[module_name]
        else:
            sys.modules[module_name] = previous_module
    candidate = probe_module.__dict__.get(class_name)
    if not isinstance(candidate, type) or not issubclass(candidate, BaseModel):
        message = f"operation {operation.operation_id} request model is invalid"
        raise TypeError(message)
    pooled = pool_schema_enums(executable_source, module_parts=module_parts)
    if pooled.modules:
        closure_digest = executable_schema_digest(
            kind="request", source=executable_source, root_annotation=class_name
        )
        executable_source = (
            pooled.source + f"\nSOURCE_CLOSURE_DIGEST = {closure_digest!r}\n"
        )
        support_source = executable_source
    return RenderedOperationRequest(
        class_name=class_name,
        source=source,
        support_source=support_source,
        executable_digest=executable_schema_digest(
            kind="request",
            source=executable_source,
            root_annotation=class_name,
        ),
        pool_modules=pooled.modules,
    )


def render_operation_response_module(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    module_parts: tuple[str, ...],
    root_model_module_parts: tuple[str, ...],
    source_context: PackageRenderContext | None = None,
) -> RenderedOperationResponse:
    """Render an operation's complete response type closure from exact IR."""
    source_context = _source_context(snapshot, source_context)
    response_use = operation_response_use(operation, source_context)
    scope = ResolutionScope(response_use.owner_kind, response_use.owner_ref)
    graph = source_context.type_resolver.resolve_type_graph(
        (ScopedTypeUse(response_use.java_type, scope),)
    )
    response_snapshot = _response_only_snapshot(
        snapshot,
        operation,
        response_java_type=response_use.java_type,
        import_paths=graph.import_paths,
    )
    context, original_module_parts = _flattened_context(
        response_snapshot,
        module_parts=module_parts,
    )
    _require_unique_flattened_exports(context)
    root_annotation = render_annotation_type(
        response_use.java_type,
        owner_import_path=response_use.owner_ref,
        context=context,
        owner_kind=response_use.owner_kind,
    )
    adapter_annotation = response_adapter_annotation(
        operation,
        return_type=root_annotation,
    )
    specs: list[_RenderableSpec] = [
        *response_snapshot.enums,
        *response_snapshot.dtos,
        *response_snapshot.models,
    ]
    deps = replace(
        type_render_deps(),
        root_model_module_parts=root_model_module_parts,
        model_role_module_parts=original_module_parts,
    )
    source = render_type_module(
        module_parts=module_parts,
        specs=specs,
        specialized_models=list(context.specialized_by_java_type.values()),
        dto_specs_by_import_path={
            item.import_path: item for item in response_snapshot.dtos
        },
        model_specs_by_import_path={
            item.import_path: item for item in response_snapshot.models
        },
        context=context,
        deps=deps,
    )
    # Root-only containers have no fields from which the type renderer can
    # discover imports; endpoint response overrides can also add StrictInt.
    root_names = set(re.findall(r"\b[A-Za-z_]\w*\b", adapter_annotation))
    if "StrictInt" in root_names:
        source += "\nfrom pydantic import StrictInt\n"
    root_aliases = sorted(root_names & {"JsonObject", "JsonValue"})
    if root_aliases:
        source += (
            "\n"
            + relative_import_statement(
                module_parts, root_model_module_parts, ", ".join(root_aliases)
            )
            + "\n"
        )
    package_name = ".".join(("dsctl", "generated", *module_parts[:-1]))
    module_name = f"{package_name}._response_schema_probe"
    probe_module = ModuleType(module_name)
    probe_module.__package__ = package_name
    probe_module.__dict__["_TypeAdapter"] = TypeAdapter
    previous_module = sys.modules.get(module_name)
    sys.modules[module_name] = probe_module
    try:
        exec(  # noqa: S102 - execute only deterministic source emitted above.
            compile(source, f"<{operation.operation_id}-response>", "exec"),
            probe_module.__dict__,
        )
        rebuild_source = _render_response_model_rebuilds(probe_module)
        if rebuild_source:
            source += f"\n{rebuild_source}\n"
            exec(  # noqa: S102 - deterministic module-owned model names only.
                compile(
                    rebuild_source,
                    f"<{operation.operation_id}-response-rebuild>",
                    "exec",
                ),
                probe_module.__dict__,
            )
        exec(  # noqa: S102 - validates the rendered root in its module namespace.
            compile(
                f"_COMPILED_RESPONSE_ADAPTER = _TypeAdapter({adapter_annotation})\n",
                f"<{operation.operation_id}-response-adapter>",
                "exec",
            ),
            probe_module.__dict__,
        )
    finally:
        if previous_module is None:
            del sys.modules[module_name]
        else:
            sys.modules[module_name] = previous_module
    candidate_adapter = probe_module.__dict__.get("_COMPILED_RESPONSE_ADAPTER")
    if not isinstance(candidate_adapter, TypeAdapter):
        message = f"operation {operation.operation_id} response adapter is invalid"
        raise TypeError(message)
    pooled = pool_schema_enums(source, module_parts=module_parts)
    if pooled.modules:
        closure_digest = executable_schema_digest(
            kind="response", source=source, root_annotation=adapter_annotation
        )
        source = pooled.source + f"\nSOURCE_CLOSURE_DIGEST = {closure_digest!r}\n"
    return RenderedOperationResponse(
        adapter_annotation=adapter_annotation,
        source=source,
        import_paths=tuple(sorted(graph.import_paths)),
        executable_digest=executable_schema_digest(
            kind="response",
            source=source,
            root_annotation=adapter_annotation,
        ),
        pool_modules=pooled.modules,
    )


def _render_response_model_rebuilds(module: ModuleType) -> str:
    """Rebuild every module-owned model against the complete module namespace."""
    model_names = tuple(
        name
        for name, value in module.__dict__.items()
        if isinstance(value, type)
        and issubclass(value, BaseModel)
        and value.__module__ == module.__name__
    )
    return "\n".join(
        f"{name}.model_rebuild(_types_namespace=globals())" for name in model_names
    )


def executable_schema_digest(
    *,
    kind: Literal["request", "response"],
    source: str,
    root_annotation: str,
) -> str:
    """Fingerprint compiler-owned executable schema source without runtime ABI."""
    return canonical_json_digest(
        {
            "schema_version": 1,
            "kind": kind,
            "source": source,
            "root_annotation": root_annotation,
        }
    )


def _response_only_snapshot(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    response_java_type: str,
    import_paths: frozenset[str],
) -> ContractSnapshot:
    enums, dtos, models = _closure_specs(snapshot, import_paths=import_paths)
    response_operation = replace(
        operation,
        summary=None,
        description=None,
        documentation=None,
        parameter_docs={},
        returns_doc=None,
        return_type=response_java_type,
        inferred_return_type=None,
        logical_return_type=response_java_type,
        response_projection="direct",
        parameters=[],
    )
    return ContractSnapshot(
        ds_version=snapshot.ds_version,
        operation_count=1,
        enum_count=len(enums),
        dto_count=len(dtos),
        model_count=len(models),
        operations=[response_operation],
        enums=enums,
        dtos=dtos,
        models=models,
    )


def _request_only_snapshot(
    snapshot: ContractSnapshot,
    operation: OperationSpec,
    *,
    import_paths: frozenset[str],
) -> ContractSnapshot:
    enums, dtos, models = _closure_specs(snapshot, import_paths=import_paths)
    request_operation = replace(
        operation,
        return_type="void",
        inferred_return_type=None,
        logical_return_type="void",
        response_projection="direct",
    )
    return ContractSnapshot(
        ds_version=snapshot.ds_version,
        operation_count=1,
        enum_count=len(enums),
        dto_count=len(dtos),
        model_count=len(models),
        operations=[request_operation],
        enums=enums,
        dtos=dtos,
        models=models,
    )


def _closure_specs(
    snapshot: ContractSnapshot,
    *,
    import_paths: frozenset[str],
) -> tuple[list[EnumSpec], list[DtoSpec], list[ModelSpec]]:
    enums = [
        replace(
            item,
            documentation=None,
            values=[replace(value, documentation=None) for value in item.values],
        )
        for item in snapshot.enums
        if item.import_path in import_paths
    ]
    dtos = [
        replace(
            item,
            documentation=None,
            fields=[_without_field_docs(field) for field in item.fields],
        )
        for item in snapshot.dtos
        if item.import_path in import_paths
    ]
    models = [
        replace(
            item,
            documentation=None,
            fields=[_without_field_docs(field) for field in item.fields],
        )
        for item in snapshot.models
        if item.import_path in import_paths
    ]
    return enums, dtos, models


def _source_context(
    snapshot: ContractSnapshot,
    context: PackageRenderContext | None,
) -> PackageRenderContext:
    if context is None:
        return planner.build_package_context(snapshot)
    if context.snapshot is not snapshot:
        message = "executable schema source context belongs to a different snapshot"
        raise ValueError(message)
    return context


def _flattened_context(
    snapshot: ContractSnapshot,
    *,
    module_parts: tuple[str, ...],
) -> tuple[PackageRenderContext, dict[str, tuple[str, ...]]]:
    context = planner.build_package_context(snapshot)
    original_module_parts = {
        import_path: assignment.module_parts
        for import_path, assignment in context.assignments_by_import_path.items()
    }
    return (
        replace(
            context,
            assignments_by_import_path={
                import_path: replace(assignment, module_parts=module_parts)
                for import_path, assignment in (
                    context.assignments_by_import_path.items()
                )
            },
            specialized_by_java_type={
                java_type: replace(specialized, module_parts=module_parts)
                for java_type, specialized in context.specialized_by_java_type.items()
            },
        ),
        original_module_parts,
    )


def _without_field_docs(field: DtoFieldSpec) -> DtoFieldSpec:
    return replace(
        field,
        description=None,
        example=None,
        allowable_values=None,
        documentation=None,
    )


def _require_unique_flattened_exports(context: PackageRenderContext) -> None:
    exports: dict[str, str] = {}
    for import_path, assignment in context.assignments_by_import_path.items():
        previous = exports.setdefault(assignment.class_name, import_path)
        if previous != import_path:
            message = (
                "flattened response closure has a class-name collision: "
                f"{assignment.class_name} ({previous}, {import_path})"
            )
            raise ValueError(message)
    for java_type, specialized in context.specialized_by_java_type.items():
        previous = exports.setdefault(specialized.class_name, java_type)
        if previous != java_type:
            message = (
                "flattened response closure has a class-name collision: "
                f"{specialized.class_name} ({previous}, {java_type})"
            )
            raise ValueError(message)


__all__ = [
    "RenderedOperationRequest",
    "RenderedOperationResponse",
    "executable_schema_digest",
    "render_operation_request_params",
    "render_operation_response_module",
]
