"""Controller contract extraction helpers."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

import javalang

from ds_codegen.extract.inference_support import _collect_class_field_types
from ds_codegen.extract.metadata import (
    _annotation_values,
    _extract_method_parameter_metadata,
    _extract_parameter_annotation_metadata,
    _find_annotation_values,
    _get_bool_value,
    _get_string_list_value,
    _get_string_value,
    _merge_parameter_annotation_metadata,
    _normalize_annotation_doc_text,
    _ParameterAnnotationMetadata,
    _parse_doc_comment,
)
from ds_codegen.extract.type_lookup import _render_type
from ds_codegen.ir import (
    HttpMethod,
    OperationSpec,
    ParamBinding,
    ParameterSpec,
    ResponseProjection,
)
from ds_codegen.java_source import (
    build_import_map as _build_import_map,
)
from ds_codegen.java_source import (
    parse_java_compilation_unit as _parse_java_compilation_unit,
)

if TYPE_CHECKING:
    from collections.abc import Callable


HTTP_MAPPING_ANNOTATIONS: dict[str, HttpMethod] = {
    "DeleteMapping": "DELETE",
    "GetMapping": "GET",
    "PatchMapping": "PATCH",
    "PostMapping": "POST",
    "PutMapping": "PUT",
}
PARAM_BINDING_ANNOTATIONS: dict[str, ParamBinding] = {
    "PathVariable": "path_variable",
    "RequestAttribute": "request_attribute",
    "RequestBody": "request_body",
    "RequestParam": "request_param",
}
IMPLICIT_REQUEST_PARAM_TYPES = {
    "BigDecimal",
    "BigInteger",
    "Boolean",
    "Byte",
    "Character",
    "Double",
    "Float",
    "Integer",
    "LocalDate",
    "LocalDateTime",
    "LocalTime",
    "Long",
    "Short",
    "String",
}
SERVER_INJECTED_PARAMETER_TYPES = {
    "HttpServletRequest",
    "HttpServletResponse",
    "HttpSession",
    "ServletRequest",
    "ServletResponse",
}
CONTROLLER_ROOTS = (
    Path(
        "references/dolphinscheduler/dolphinscheduler-api/src/main/java/"
        "org/apache/dolphinscheduler/api/controller"
    ),
    Path(
        "references/dolphinscheduler/dolphinscheduler-api/src/main/java/"
        "org/apache/dolphinscheduler/api/controller/v2"
    ),
)


@dataclass(frozen=True)
class ControllerExtractDeps:
    infer_operation_return_type: Callable[..., str | None]
    resolve_operation_logical_return_type: Callable[..., str]
    resolve_operation_response_projection: Callable[..., ResponseProjection]
    resolve_referenced_import_path: Callable[..., str | None]
    looks_like_request_dto_import: Callable[[str], bool]
    collect_type_reference_names: Callable[[object | None], set[str]]
    collect_type_reference_names_from_java_type: Callable[[str], set[str]]


def iter_controller_paths(repo_root: Path) -> list[Path]:
    controller_paths: list[Path] = []
    for root in CONTROLLER_ROOTS:
        search_root = repo_root / root
        if not search_root.exists():
            continue
        controller_paths.extend(sorted(search_root.glob("*.java")))
    return controller_paths


def extract_controller_contract(
    *,
    repo_root: Path,
    controller_path: Path,
    deps: ControllerExtractDeps,
) -> tuple[list[OperationSpec], set[str], set[str], set[str]]:
    source = controller_path.read_text()
    compilation_unit = _parse_java_compilation_unit(source)
    controller = compilation_unit.types[0]
    class_mapping = _extract_request_mapping_path(controller.annotations)
    api_group = "v2" if ".controller.v2" in compilation_unit.package.name else "v1"

    import_map = _build_import_map(compilation_unit)
    package_name = compilation_unit.package.name if compilation_unit.package else None
    controller_field_types = _collect_class_field_types(controller)

    operations: list[OperationSpec] = []
    enum_imports: set[str] = set()
    dto_imports: set[str] = set()
    model_imports: set[str] = set()
    for method in controller.methods:
        operation = extract_operation_spec(
            repo_root=repo_root,
            controller_path=controller_path,
            controller_name=controller.name,
            method=method,
            class_mapping=class_mapping,
            api_group=api_group,
            import_map=import_map,
            package_name=package_name,
            controller_field_types=controller_field_types,
            deps=deps,
        )
        if operation is None:
            continue
        operations.append(operation)

        referenced_parameter_types = {
            parameter.java_type for parameter in operation.parameters
        }
        for referenced_type in referenced_parameter_types:
            import_path = deps.resolve_referenced_import_path(
                repo_root,
                referenced_type,
                import_map,
                package_name,
            )
            if import_path is None:
                continue
            if deps.looks_like_request_dto_import(import_path):
                dto_imports.add(import_path)
            else:
                enum_imports.add(import_path)

        return_type_names = deps.collect_type_reference_names(method.return_type)
        if operation.inferred_return_type is not None:
            return_type_names.update(
                deps.collect_type_reference_names_from_java_type(
                    operation.inferred_return_type
                )
            )
        return_type_names.update(
            deps.collect_type_reference_names_from_java_type(
                operation.logical_return_type
            )
        )
        for referenced_type in return_type_names:
            import_path = deps.resolve_referenced_import_path(
                repo_root,
                referenced_type,
                import_map,
                package_name,
            )
            if import_path is not None:
                model_imports.add(import_path)

    return operations, enum_imports, dto_imports, model_imports


def deduplicate_operation_ids(operations: list[OperationSpec]) -> list[OperationSpec]:
    operation_id_counts = Counter(operation.operation_id for operation in operations)
    used_operation_ids: set[str] = set()
    deduplicated_operations: list[OperationSpec] = []
    for operation in operations:
        if operation_id_counts[operation.operation_id] == 1:
            used_operation_ids.add(operation.operation_id)
            deduplicated_operations.append(operation)
            continue
        base_operation_id = (
            f"{operation.operation_id}__"
            f"{_slugify_operation_id_suffix(operation.http_method, operation.path)}"
        )
        candidate_operation_id = base_operation_id
        suffix_index = 2
        while candidate_operation_id in used_operation_ids:
            candidate_operation_id = f"{base_operation_id}_{suffix_index}"
            suffix_index += 1
        used_operation_ids.add(candidate_operation_id)
        deduplicated_operations.append(
            replace(operation, operation_id=candidate_operation_id)
        )
    return deduplicated_operations


def _slugify_operation_id_suffix(http_method: str, path: str) -> str:
    raw_suffix = f"{http_method.lower()}_{path}"
    slug = re.sub(r"[^0-9A-Za-z]+", "_", raw_suffix).strip("_")
    return re.sub(r"_+", "_", slug)


def extract_operation_spec(
    *,
    repo_root: Path,
    controller_path: Path,
    controller_name: str,
    method: javalang.tree.MethodDeclaration,
    class_mapping: str,
    api_group: str,
    import_map: dict[str, str],
    package_name: str | None,
    controller_field_types: dict[str, str],
    deps: ControllerExtractDeps,
) -> OperationSpec | None:
    http_method: HttpMethod | None = None
    method_mapping = ""
    consumes: list[str] = []
    for annotation in method.annotations:
        candidate_http_method = HTTP_MAPPING_ANNOTATIONS.get(annotation.name)
        if candidate_http_method is None:
            continue
        annotation_values = _annotation_values(annotation)
        http_method = candidate_http_method
        method_mapping = _get_string_value(annotation_values, "value") or ""
        consumes = _get_string_list_value(annotation_values, "consumes")
        break

    if http_method is None:
        return None

    operation_values = _find_annotation_values(method.annotations, "Operation")
    method_doc = _parse_doc_comment(method.documentation)
    method_parameter_metadata = _extract_method_parameter_metadata(method.annotations)
    summary = _normalize_annotation_doc_text(
        _get_string_value(operation_values, "summary")
    )
    description = _normalize_annotation_doc_text(
        _get_string_value(operation_values, "description")
    )
    path = _join_paths(class_mapping, method_mapping)
    parameters = [
        extract_parameter_spec(
            parameter,
            parameter_doc=method_doc.params.get(parameter.name),
            method_parameter_metadata=method_parameter_metadata,
        )
        for parameter in method.parameters
    ]

    raw_return_type = _render_type(method.return_type)
    inferred_return_type = deps.infer_operation_return_type(
        repo_root=repo_root,
        controller_path=controller_path,
        method=method,
        controller_field_types=controller_field_types,
        import_map=import_map,
        package_name=package_name,
    )

    logical_return_type = deps.resolve_operation_logical_return_type(
        repo_root=repo_root,
        raw_return_type=raw_return_type,
        inferred_return_type=inferred_return_type,
        import_map=import_map,
        package_name=package_name,
    )

    return OperationSpec(
        operation_id=f"{controller_name}.{method.name}",
        controller=controller_name,
        method_name=method.name,
        api_group=api_group,
        http_method=http_method,
        path=path,
        summary=summary,
        description=description,
        documentation=method_doc.description,
        parameter_docs=method_doc.params,
        returns_doc=method_doc.returns,
        consumes=consumes,
        return_type=raw_return_type,
        inferred_return_type=inferred_return_type,
        logical_return_type=logical_return_type,
        response_projection=deps.resolve_operation_response_projection(
            raw_return_type=raw_return_type,
            logical_return_type=logical_return_type,
        ),
        parameters=parameters,
    )


def extract_parameter_spec(
    parameter: javalang.tree.FormalParameter,
    *,
    parameter_doc: str | None,
    method_parameter_metadata: dict[str, _ParameterAnnotationMetadata],
) -> ParameterSpec:
    hidden = False
    binding: ParamBinding | None = None
    wire_name: str | None = None
    required: bool | None = None
    default_value: str | None = None
    direct_parameter_metadata = _ParameterAnnotationMetadata(
        wire_name=None,
        required=None,
        description=None,
        example=None,
        allowable_values=None,
        schema_type=None,
    )

    for annotation in parameter.annotations:
        if annotation.name == "Parameter":
            annotation_values = _annotation_values(annotation)
            hidden = bool(annotation_values.get("hidden", False))
            direct_parameter_metadata = _merge_parameter_annotation_metadata(
                direct_parameter_metadata,
                _extract_parameter_annotation_metadata(annotation),
            )
        annotation_values = _annotation_values(annotation)
        candidate_binding = PARAM_BINDING_ANNOTATIONS.get(annotation.name)
        if candidate_binding is not None:
            binding = candidate_binding
            wire_name = (
                _get_string_value(annotation_values, "value")
                or _get_string_value(annotation_values, "name")
                or parameter.name
            )
            required = _get_bool_value(annotation_values, "required")
            default_value = _get_string_value(annotation_values, "defaultValue")

    if (
        binding is None
        and parameter.type.name.endswith("Request")
        and parameter.type.name not in SERVER_INJECTED_PARAMETER_TYPES
    ):
        binding = "model_attribute"
    if binding is None and _looks_like_implicit_request_param(parameter.type):
        binding = "request_param"
        wire_name = parameter.name

    method_parameter = (
        method_parameter_metadata.get(wire_name) if wire_name is not None else None
    )
    if method_parameter is None:
        method_parameter = method_parameter_metadata.get(parameter.name)
    description = (
        direct_parameter_metadata.description
        or (method_parameter.description if method_parameter is not None else None)
        or parameter_doc
    )
    required = (
        required
        if required is not None
        else direct_parameter_metadata.required
        if direct_parameter_metadata.required is not None
        else method_parameter.required
        if method_parameter is not None
        else None
    )
    example = direct_parameter_metadata.example or (
        method_parameter.example if method_parameter is not None else None
    )
    allowable_values = direct_parameter_metadata.allowable_values or (
        method_parameter.allowable_values if method_parameter is not None else None
    )
    schema_type = direct_parameter_metadata.schema_type or (
        method_parameter.schema_type if method_parameter is not None else None
    )

    return ParameterSpec(
        name=parameter.name,
        java_type=_render_type(parameter.type),
        binding=binding,
        wire_name=wire_name,
        required=required,
        default_value=default_value,
        hidden=hidden,
        description=description,
        example=example,
        allowable_values=allowable_values,
        schema_type=schema_type,
    )


def _extract_request_mapping_path(
    annotations: list[javalang.tree.Annotation],
) -> str:
    values = _find_annotation_values(annotations, "RequestMapping")
    return _get_string_value(values, "value") or ""


def _looks_like_implicit_request_param(type_node: object | None) -> bool:
    if isinstance(type_node, javalang.tree.BasicType):
        return True
    if not isinstance(type_node, javalang.tree.ReferenceType):
        return False
    if type_node.arguments:
        return False
    if getattr(type_node, "dimensions", None):
        return True
    return cast("str", type_node.name) in IMPLICIT_REQUEST_PARAM_TYPES


def _join_paths(class_mapping: str, method_mapping: str) -> str:
    parts = [
        segment.strip("/") for segment in (class_mapping, method_mapping) if segment
    ]
    return "/".join(part for part in parts if part)
