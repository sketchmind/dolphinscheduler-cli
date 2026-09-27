"""Compile native resource exchanges while preserving reviewed byte transports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.resource_contract import (
    RESOURCE_SEMANTIC_OPERATIONS,
    TARGET_RESOURCE_VERSIONS,
    resource_contract,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from dsctl.support.json_types import JsonValue

COMPILED_RESOURCE_SCHEMA_VERSION = 1
_RESOURCE_ENUM = "org.apache.dolphinscheduler.spi.enums.ResourceType"
_LEGACY_RESOURCE_ENUM = "org.apache.dolphinscheduler.common.enums.ResourceType"
_BINARY_RESOURCE = "org.springframework.core.io.Resource"
_NAMES = (
    "page",
    "lookup",
    "base_dir",
    "create",
    "mkdir",
    "delete",
    "upload",
    "download",
    "view_native",
)
_Fact = tuple[str, str, bool | None, str | None]


def _recipe_id(version: str) -> str:
    recipe = resource_contract(version).resource
    if recipe.upload_route == "legacy-create":
        return "legacy_139"
    if recipe.identity_wire == "id":
        return "id_rest"
    if recipe.parent_directory_wire == "absolute-path":
        return (
            "absolute_path_native_view"
            if recipe.view_wire == "generated-path"
            else "absolute_path"
        )
    return (
        "storage_path_322"
        if recipe.create_operation.endswith(".createResourceFile")
        else "storage_path"
    )


def _classify(operation: OperationSpec) -> str | None:
    return {
        "ResourcesController.queryResourceListPaging": "page",
        "ResourcesController.pagingResourceItemRequest": "page",
        "ResourcesController.queryResource": "lookup",
        "ResourcesController.queryResourceBaseDir": "base_dir",
        "ResourcesController.onlineCreateResource": "create",
        "ResourcesController.createResourceFile": "create",
        "ResourcesController.createFileFromContent": "create",
        "ResourcesController.createDirectory": "mkdir",
        "ResourcesController.deleteResource": "delete",
        "ResourcesController.createResource": "upload",
        "ResourcesController.createFile": "upload",
        "ResourcesController.downloadResource": "download",
        "ResourcesController.viewResource": "view_native",
    }.get(operation.operation_id)


def _facts(version: str, primitive: str) -> dict[str, _Fact] | None:
    recipe = resource_contract(version).resource
    legacy = recipe.upload_route == "legacy-create"
    id_backed = recipe.identity_wire == "id"
    absolute = recipe.parent_directory_wire == "absolute-path"
    required = None if id_backed else True
    resource_type = _LEGACY_RESOURCE_ENUM if legacy else _RESOURCE_ENUM
    typed: _Fact = (resource_type, "request_param", required, None)
    text: _Fact = ("String", "request_param", required, None)
    optional_text: _Fact = ("String", "request_param", False, None)
    identifier: _Fact = ("int", "request_param", required, None)
    fields: dict[str, _Fact] = {}
    if primitive == "lookup":
        if recipe.lookup_operation is None:
            return None
        return {
            "fullName": optional_text,
            "id": (
                "Integer",
                "request_param" if legacy else "path_variable",
                False,
                None,
            ),
            "type": typed,
        }
    if primitive == "base_dir":
        return {"type": typed} if recipe.base_directory_wire == "endpoint" else None
    if primitive == "page":
        if id_backed:
            fields.update(type=typed, id=identifier)
        else:
            fields["fullName"] = text
            if recipe.tenant_code_on_reads:
                fields["tenantCode"] = ("String", "request_param", None, None)
            fields["type"] = typed
        page: _Fact = ("Integer", "request_param", required, None)
        if absolute:
            fields["searchVal"] = optional_text
            fields.update(pageNo=page, pageSize=page)
        else:
            fields.update(pageNo=page, searchVal=optional_text, pageSize=page)
    elif primitive in {"create", "mkdir", "upload"}:
        fields["type"] = typed
        if primitive == "create":
            fields.update(fileName=text, suffix=text)
        else:
            fields["name"] = text
        if id_backed:
            fields["description"] = optional_text
        if primitive == "create":
            fields["content"] = text
        elif primitive == "upload":
            fields["file"] = ("MultipartFile", "request_param", required, None)
        if id_backed or (primitive == "mkdir" and not absolute):
            fields["pid"] = identifier
        fields["currentDir"] = text
    elif primitive in {"download", "delete"}:
        if id_backed:
            fields["id"] = (
                "int",
                "request_param" if legacy else "path_variable",
                None,
                None,
            )
        else:
            fields["fullName"] = text
            if primitive == "delete" and recipe.tenant_code_on_reads:
                fields["tenantCode"] = optional_text
    elif primitive == "view_native":
        if id_backed:
            fields["id"] = (
                "int",
                "request_param" if legacy else "path_variable",
                None,
                None,
            )
        elif absolute:
            fields["fullName"] = text
        fields.update(skipLineNum=identifier, limit=identifier)
        if recipe.tenant_code_on_reads:
            fields.update(fullName=text, tenantCode=text)
    else:
        message = f"unknown compiled resource primitive {primitive!r}"
        raise ValueError(message)
    return fields


def _request(version: str, primitive: str) -> CompiledRequestEpoch | None:
    facts = _facts(version, primitive)
    if facts is None:
        return None
    recipe = resource_contract(version).resource
    legacy = recipe.upload_route == "legacy-create"
    id_backed = recipe.identity_wire == "id"
    path = "resources"
    method: Literal["GET", "POST", "DELETE"] = "GET"
    channel: Literal["query", "form", "path", "path_query", "multipart"] = "query"
    paths = tuple(name for name, fact in facts.items() if fact[1] == "path_variable")
    if primitive == "page":
        path += "/list-paging" if legacy else ""
    elif primitive == "lookup":
        path += "/queryResource" if legacy else "/{id}"
    elif primitive == "base_dir":
        path += "/base-dir"
    elif primitive == "create":
        path += "/online-create"
        method, channel = "POST", "form"
    elif primitive == "mkdir":
        path += "/directory/create" if legacy else "/directory"
        method, channel = "POST", "form"
    elif primitive == "upload":
        path += "/create" if legacy else ""
        method, channel = "POST", "multipart"
    elif primitive == "download":
        path += "/{id}/download" if id_backed and not legacy else "/download"
    elif primitive == "delete":
        path += "/delete" if legacy else "/{id}" if id_backed else ""
        method = "GET" if legacy else "DELETE"
    elif primitive == "view_native":
        path += "/{id}/view" if id_backed and not legacy else "/view"
    if paths:
        channel = "path" if len(paths) == len(facts) else "path_query"
    return CompiledRequestEpoch(
        method=method,
        path=path,
        channel=channel,
        request_schema=primitive,
        request_model="".join(word.title() for word in primitive.split("_")) + "Params",
        request_fields=tuple(facts),
        path_fields=paths,
        file_fields=("file",) if primitive == "upload" else (),
        versions=frozenset({version}),
        content_addressed=True,
    )


def _primitive(name: str) -> CompiledPrimitive:
    return CompiledPrimitive(
        name=name,
        requests=tuple(
            request
            for version in TARGET_RESOURCE_VERSIONS
            if (request := _request(version, name)) is not None
        ),
        result_envelope="required"
        if name in {"upload", "create", "mkdir", "delete"}
        else "optional",
        response_transport="binary" if name == "download" else "json",
        absent_versions=frozenset(
            version
            for version in TARGET_RESOURCE_VERSIONS
            if _facts(version, name) is None
        ),
    )


def _response_root(version: str, primitive: str) -> str:
    recipe = resource_contract(version).resource
    if primitive == "page":
        page = "PageInfo" if _recipe_id(version) == "id_rest" else recipe.page_model
        return f"{page}<{recipe.item_model}>"
    if primitive == "lookup":
        return recipe.item_model
    if primitive == "base_dir":
        return "String"
    if primitive == "download":
        return (
            "void"
            if recipe.parent_directory_wire == "absolute-path"
            else _BINARY_RESOURCE
        )
    if primitive == "delete":
        return "Void"
    if primitive == "view_native":
        if recipe.view_model is None:
            return "Map<String, String>"
        return recipe.view_model.removeprefix("generated.view.")
    return "Map<String, Object>" if recipe.identity_wire == "id" else "Void"


def _require_resource_type(snapshot: ContractSnapshot) -> None:
    recipe = resource_contract(snapshot.ds_version).resource
    root = (
        _LEGACY_RESOURCE_ENUM
        if recipe.upload_route == "legacy-create"
        else _RESOURCE_ENUM
    )
    enums = [enum for enum in snapshot.enums if enum.import_path == root]
    expected = (
        ("FILE", "UDF")
        if recipe.identity_wire == "id"
        else ("FILE", "UDF", "ALL")
        if recipe.tenant_code_on_reads
        else ("FILE", "ALL")
    )
    if (
        len(enums) != 1
        or enums[0].json_value_field is not None
        or tuple(value.name for value in enums[0].values) != expected
    ):
        message = "compiled resource native type enum changed"
        raise ValueError(message)


def _require_response_fields(snapshot: ContractSnapshot, primitive: str) -> None:
    recipe = resource_contract(snapshot.ds_version).resource
    root: str | None = None
    roles: dict[str, set[str]] = {}
    if primitive in {"page", "lookup"}:
        root = recipe.item_model
        roles = {
            "fullName": {"String"},
            "isDirectory": {"boolean"},
            "size": {"long"},
            "type": {
                _LEGACY_RESOURCE_ENUM
                if recipe.upload_route == "legacy-create"
                else _RESOURCE_ENUM
            },
        }
        if primitive == "lookup":
            roles["id"] = {"int", "Integer"}
        else:
            roles.update({field: {"String"} for field in ("alias", "fileName")})
    elif primitive == "view_native" and recipe.view_model is not None:
        root, roles = recipe.view_model, {"content": {"String"}}
    if root is None:
        return
    model = require_model(snapshot, root, domain="resource")
    fields = {field.name: field.java_type for field in model.fields}
    if any(fields.get(name) not in types for name, types in roles.items()):
        message = f"compiled resource {primitive} consumed response fields changed"
        raise ValueError(message)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    version = snapshot.ds_version
    recipe = resource_contract(version).resource
    expected_facts = _facts(version, primitive)
    actual = {
        parameter.wire_name: (
            parameter.java_type,
            parameter.binding,
            parameter.required,
            parameter.default_value,
        )
        for parameter in operation.parameters
        if is_client_supplied_parameter(parameter)
    }
    if expected_facts is None or actual != expected_facts or operation.consumes:
        message = f"compiled resource {primitive} request fields changed"
        raise ValueError(message)
    expected = _response_root(version, primitive)
    if (
        operation.logical_return_type != expected
        or operation.response_projection != "direct"
    ):
        message = f"compiled resource {primitive} native response role changed"
        raise ValueError(message)
    if "type" in expected_facts:
        _require_resource_type(snapshot)
    _require_response_fields(snapshot, primitive)
    # Upload has always required a Result but intentionally ignored its data.
    # Every id-backed online-create assigns a Resource BeanMap, then replaces that
    # Result with the fresh Result returned by uploadContentToHdfs/Storage. The
    # successful wire data is therefore void. Directory creation keeps its original
    # Result and Map payload, so this exception is deliberately create-only.
    # Download reads bytes even where native 3.3+ controllers declare void.
    no_schema = (
        primitive in {"upload", "download"}
        or expected == "Void"
        or (recipe.identity_wire == "id" and primitive == "create")
    )
    capture: JsonValue = None if no_schema else "" if expected == "String" else {}
    return CompiledResponsePolicy(
        codec=f"{primitive}_{version.replace('.', '_')}",
        schema=None if no_schema else primitive,
        capture=capture,
        content_addressed=not no_schema,
        response_transport="binary" if primitive == "download" else "json",
    )


def _recipe(codecs: Mapping[str, str]) -> str:
    for version in TARGET_RESOURCE_VERSIONS:
        stamp = version.replace(".", "_")
        expected = {
            name: f"{name}_{stamp}"
            for name in _NAMES
            if _facts(version, name) is not None
        }
        if codecs == expected:
            return _recipe_id(version)
    message = "compiled resource codecs do not form an exact reviewed recipe"
    raise ValueError(message)


RESOURCE_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="resource",
    schema_constant="COMPILED_RESOURCE_SCHEMA_VERSION",
    schema_version=COMPILED_RESOURCE_SCHEMA_VERSION,
    semantic_operations=frozenset(RESOURCE_SEMANTIC_OPERATIONS),
    absent_versions=frozenset(),
    primitives=tuple(_primitive(name) for name in _NAMES),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)
