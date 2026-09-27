"""Project source models onto the payload shape serialized by controllers."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, NoReturn

import javalang

from ds_codegen.extract.type_lookup import _render_type
from ds_codegen.java_source import load_type_declaration
from ds_codegen.source import default_ds_source_root, read_ds_source_version

if TYPE_CHECKING:
    from pathlib import Path

    from ds_codegen.ir import DtoFieldSpec


_PAGE_INFO_IMPORT = "org.apache.dolphinscheduler.api.utils.PageInfo"
_BASE_CONTROLLER_IMPORT = "org.apache.dolphinscheduler.api.controller.BaseController"
_CONSTANTS_IMPORT = "org.apache.dolphinscheduler.common.Constants"
_LEGACY_PAGE_INTERNAL_FIELDS = (
    ("lists", "List<T>"),
    ("totalCount", "Integer"),
    ("pageSize", "Integer"),
    ("currentPage", "Integer"),
    ("pageNo", "Integer"),
)
_LEGACY_PAGE_WIRE_CONSTANTS = {
    "TOTAL_LIST": "totalList",
    "CURRENT_PAGE": "currentPage",
    "TOTAL_PAGE": "totalPage",
    "TOTAL": "total",
}


def project_model_wire_fields(
    repo_root: Path,
    import_path: str,
    fields: list[DtoFieldSpec],
) -> list[DtoFieldSpec]:
    """Return exact wire fields when a controller reshapes a source model."""

    if import_path != _PAGE_INFO_IMPORT:
        return fields
    source_root = default_ds_source_root(repo_root)
    if read_ds_source_version(source_root) != "1.3.9":
        return fields

    _require_legacy_page_internal_shape(fields)
    _validate_legacy_page_reassembly(repo_root)
    by_name = {field.wire_name: field for field in fields}
    return [
        replace(
            by_name["lists"],
            name="totalList",
            wire_name="totalList",
            description="total list",
        ),
        replace(
            by_name["totalCount"],
            name="total",
            wire_name="total",
            description="total",
        ),
        replace(
            by_name["totalCount"],
            name="totalPage",
            wire_name="totalPage",
            default_value=None,
            nullable=True,
            description="total page",
        ),
        replace(
            by_name["currentPage"],
            nullable=True,
        ),
    ]


def _require_legacy_page_internal_shape(fields: list[DtoFieldSpec]) -> None:
    actual = tuple((field.wire_name, field.java_type) for field in fields)
    if actual != _LEGACY_PAGE_INTERNAL_FIELDS:
        _fail(f"the reviewed internal PageInfo declaration changed (found {actual!r})")


def _validate_legacy_page_reassembly(repo_root: Path) -> None:
    loaded = load_type_declaration(repo_root, _BASE_CONTROLLER_IMPORT, {})
    if loaded is None:
        _fail("the reviewed BaseController cannot be loaded")
    _, declaration, _, _ = loaded
    if not isinstance(declaration, javalang.tree.ClassDeclaration):
        _fail("the reviewed BaseController is no longer a class")

    paging_method = _require_method(
        declaration,
        name="returnDataListPaging",
        parameter_types=("Map<String, Object>",),
    )
    _require_page_getter_reassembly(paging_method)

    success_method = _require_method(
        declaration,
        name="success",
        parameter_types=("Object", "Integer", "Integer", "Integer"),
    )
    _require_wire_map_reassembly(success_method)
    _require_wire_constants(repo_root)


def _require_method(
    declaration: javalang.tree.ClassDeclaration,
    *,
    name: str,
    parameter_types: tuple[str, ...],
) -> javalang.tree.MethodDeclaration:
    matches = [
        method
        for method in declaration.methods
        if method.name == name
        and tuple(_render_type(parameter.type) for parameter in method.parameters)
        == parameter_types
    ]
    if len(matches) != 1:
        _fail(
            f"expected exactly one {name}{parameter_types!r} method, "
            f"found {len(matches)}"
        )
    return matches[0]


def _require_page_getter_reassembly(
    method: javalang.tree.MethodDeclaration,
) -> None:
    expected_getters = (
        "getLists",
        "getCurrentPage",
        "getTotalCount",
        "getTotalPage",
    )
    matching_returns = []
    for _, node in method:
        if not isinstance(node, javalang.tree.ReturnStatement):
            continue
        invocation = node.expression
        if not isinstance(invocation, javalang.tree.MethodInvocation):
            continue
        if invocation.member != "success" or invocation.qualifier:
            continue
        matching_returns.append(invocation)
    if len(matching_returns) != 1:
        _fail(
            "returnDataListPaging no longer has exactly one reviewed success "
            "reassembly return"
        )
    arguments = matching_returns[0].arguments
    actual_getters = tuple(
        argument.member
        if isinstance(argument, javalang.tree.MethodInvocation)
        and argument.qualifier == "pageInfo"
        and not argument.arguments
        else None
        for argument in arguments
    )
    if actual_getters != expected_getters:
        _fail(
            f"returnDataListPaging getter reassembly changed (found {actual_getters!r})"
        )


def _require_wire_map_reassembly(method: javalang.tree.MethodDeclaration) -> None:
    expected_parameters = ("totalList", "currentPage", "total", "totalPage")
    actual_parameters = tuple(parameter.name for parameter in method.parameters)
    if actual_parameters != expected_parameters:
        _fail(
            "the reviewed paging success parameter names changed "
            f"(found {actual_parameters!r})"
        )

    actual_puts: set[tuple[str, str]] = set()
    set_data_count = 0
    for _, node in method:
        if not isinstance(node, javalang.tree.MethodInvocation):
            continue
        if (
            node.qualifier == "map"
            and node.member == "put"
            and len(node.arguments) == 2
        ):
            constant, value = node.arguments
            if isinstance(constant, javalang.tree.MemberReference) and isinstance(
                value,
                javalang.tree.MemberReference,
            ):
                actual_puts.add((constant.member, value.member))
        if (
            node.qualifier == "result"
            and node.member == "setData"
            and len(node.arguments) == 1
            and _is_unqualified_reference(node.arguments[0], "map")
        ):
            set_data_count += 1

    expected_puts = {
        ("TOTAL_LIST", "totalList"),
        ("CURRENT_PAGE", "currentPage"),
        ("TOTAL_PAGE", "totalPage"),
        ("TOTAL", "total"),
    }
    if actual_puts != expected_puts or set_data_count != 1:
        _fail(
            "the reviewed paging success wire map reassembly changed "
            f"(puts={sorted(actual_puts)!r}, setData={set_data_count})"
        )


def _require_wire_constants(repo_root: Path) -> None:
    loaded = load_type_declaration(repo_root, _CONSTANTS_IMPORT, {})
    if loaded is None:
        _fail("the reviewed Constants declaration cannot be loaded")
    _, declaration, _, _ = loaded
    if not isinstance(declaration, javalang.tree.ClassDeclaration):
        _fail("the reviewed Constants declaration is no longer a class")

    actual: dict[str, str] = {}
    for field in declaration.fields:
        for declarator in field.declarators:
            if declarator.name not in _LEGACY_PAGE_WIRE_CONSTANTS:
                continue
            initializer = declarator.initializer
            if isinstance(initializer, javalang.tree.Literal):
                actual[declarator.name] = initializer.value.strip('"')
    if actual != _LEGACY_PAGE_WIRE_CONSTANTS:
        _fail(f"the reviewed paging wire constant values changed (found {actual!r})")


def _is_unqualified_reference(node: object, name: str) -> bool:
    return (
        isinstance(node, javalang.tree.MemberReference)
        and not node.qualifier
        and node.member == name
    )


def _fail(detail: str) -> NoReturn:
    message = (
        "DolphinScheduler 1.3.9 PageInfo wire projection source evidence no "
        f"longer matches: {detail}"
    )
    raise RuntimeError(message)
