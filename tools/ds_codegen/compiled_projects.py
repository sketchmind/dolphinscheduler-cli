"""Compile exact project reads and lifecycle without changing readback recipes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ds_codegen.compiled_domains import (
    CompiledDomainDefinition,
    CompiledPrimitive,
    CompiledRequestEpoch,
    CompiledResponsePolicy,
    model_field_facts,
    require_model,
)
from ds_codegen.contract_visibility import is_client_supplied_parameter
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec

COMPILED_PROJECT_SCHEMA_VERSION = 1
COMPILED_PROJECT_SEMANTIC_OPERATIONS = frozenset(
    {
        "project.page",
        "project.get",
        "project.create",
        "project.update",
        "project.delete",
    }
)
_ENTITY = "org.apache.dolphinscheduler.dao.entity.Project"
_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_LEGACY = frozenset({"1.3.9"})
_CREATED_LIST = frozenset({"2.0.0", "2.0.1"})
_ZERO_IDS = (
    ("id", "int", False, "0", None),
    ("userId", "int", False, "0", None),
)
_NULLABLE_IDS = (
    ("id", "Integer", True, None, None),
    ("userId", "Integer", True, None, None),
)
_OWNER = ("userName", "String", True, None, None)
_CODE = ("code", "long", False, "0", None)
_PROJECT_FIELDS = (
    ("name", "String", True, None, None),
    ("description", "String", True, None, None),
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
    ("perm", "int", False, "0", None),
    ("defCount", "int", False, "0", None),
)
_RUNNING = ("instRunningCount", "int", False, "0", None)
_ENTITY_FIELDS = {
    "legacy": (*_ZERO_IDS, _OWNER, *_PROJECT_FIELDS, _RUNNING),
    "code_zero": (*_ZERO_IDS, _OWNER, _CODE, *_PROJECT_FIELDS, _RUNNING),
    "code_nullable": (*_NULLABLE_IDS, _OWNER, _CODE, *_PROJECT_FIELDS, _RUNNING),
    "code_compact": (*_NULLABLE_IDS, _OWNER, _CODE, *_PROJECT_FIELDS),
}
_NULLABLE_PAGE = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_PAGE_FIELDS = {
    "legacy": (*_NULLABLE_PAGE[:3], _NULLABLE_PAGE[4]),
    "code_zero": _NULLABLE_PAGE,
    "code_nullable": (
        ("totalList", "List<T>", False, None, "list"),
        *_NULLABLE_PAGE[1:],
    ),
    "code_compact": (
        ("totalList", "List<T>", False, None, "list"),
        *_NULLABLE_PAGE[1:],
    ),
}
_STRICT_PAGE_FIELDS = frozenset(
    {"total", "totalPage", "pageSize", "currentPage", "pageNo"}
)


@dataclass(frozen=True)
class _ProjectPolicy:
    entity: str
    recipe: str
    create_id: bool = False
    update_void: bool = False
    update_owner: bool = False
    strict_page: bool = False
    annotated_page_required: bool = False


_POLICIES = {
    "1.3.9": _ProjectPolicy("legacy", "legacy_id", create_id=True, update_void=True),
    "2.0.0": _ProjectPolicy(
        "code_zero",
        "id_create_void_update",
        create_id=True,
        update_void=True,
        update_owner=True,
        strict_page=True,
    ),
    "2.0.1": _ProjectPolicy(
        "code_zero",
        "id_create_void_update",
        create_id=True,
        update_void=True,
        update_owner=True,
        strict_page=True,
    ),
    **dict.fromkeys(
        (
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
        ),
        _ProjectPolicy(
            "code_zero",
            "project_create_void_update",
            update_void=True,
            update_owner=True,
            strict_page=True,
        ),
    ),
    **dict.fromkeys(
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
        _ProjectPolicy(
            "code_nullable",
            "owner_preserving_project",
            update_owner=True,
            strict_page=True,
        ),
    ),
    "3.2.0": _ProjectPolicy(
        "code_nullable",
        "owner_preserving_project",
        update_owner=True,
        annotated_page_required=True,
    ),
    **dict.fromkeys(
        ("3.2.1", "3.2.2"),
        _ProjectPolicy("code_nullable", "modern", annotated_page_required=True),
    ),
    **dict.fromkeys(
        ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"),
        _ProjectPolicy("code_compact", "modern", annotated_page_required=True),
    ),
}
_CODE_VERSIONS = frozenset(_POLICIES) - _LEGACY
_OWNER_VERSIONS = frozenset(
    version for version, policy in _POLICIES.items() if policy.update_owner
)


def _classify(operation: OperationSpec) -> str | None:
    if operation.operation_id == "ProjectController.deleteProject":
        return "delete_legacy" if operation.http_method == "GET" else "delete"
    return {
        "ProjectController.queryProjectListPaging": "page",
        "ProjectController.queryProjectById": "get",
        "ProjectController.queryProjectByCode": "get",
        "ProjectController.createProject": "create",
        "ProjectController.queryProjectCreatedAndAuthorizedByUser": (
            "created_and_authed"
        ),
        "ProjectController.updateProject": "update",
    }.get(operation.operation_id)


def _codecs(version: str, policy: _ProjectPolicy) -> dict[str, str]:
    page = f"page_{policy.entity}" + ("_strict" if policy.strict_page else "")
    create = f"create_{policy.entity}"
    if policy.create_id:
        create = "create_id_legacy" if version in _LEGACY else "create_id_code"
    update = "update_legacy"
    if version not in _LEGACY:
        update = f"update_{policy.entity}"
        if policy.update_void:
            update = "update_void_owner"
        elif policy.update_owner:
            update = "update_entity_owner"
    codecs = {
        "page": page,
        "get": f"get_{policy.entity}",
        "create": create,
        "update": update,
    }
    delete = "delete_legacy" if version in _LEGACY else "delete"
    codecs[delete] = delete
    if version in _CREATED_LIST:
        codecs["created_and_authed"] = "created_and_authed"
    return codecs


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    policy = _POLICIES[snapshot.ds_version]
    if operation.response_projection != "direct" or operation.consumes:
        message = f"compiled project {primitive} exchange projection changed"
        raise ValueError(message)
    _require_request_fields(snapshot.ds_version, operation, primitive, policy)
    codec = _codecs(snapshot.ds_version, policy)[primitive]
    logical = operation.logical_return_type
    if primitive in {"delete", "delete_legacy"} or (
        primitive == "update" and policy.update_void
    ):
        if logical != "Void":
            message = f"compiled project {primitive} void response changed"
            raise ValueError(message)
        return CompiledResponsePolicy(codec=codec, schema=None, capture=None)
    if primitive == "create" and policy.create_id:
        if logical != "int":
            message = "compiled project create id response changed"
            raise ValueError(message)
        return CompiledResponsePolicy(
            codec=codec, schema="id", capture=1, scalar_annotation="int"
        )
    expected = _ENTITY
    schema = f"entity_{policy.entity}"
    if primitive == "page":
        expected = f"{_PAGE}<{_ENTITY}>"
        schema = codec
        page = require_model(snapshot, _PAGE, domain="project")
        strict = frozenset(
            cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE, ())
        )
        if (
            page.extends is not None
            or model_field_facts(page) != _PAGE_FIELDS[policy.entity]
            or strict != (_STRICT_PAGE_FIELDS if policy.strict_page else frozenset())
        ):
            message = "compiled project page fields or integer policy changed"
            raise ValueError(message)
    elif primitive == "created_and_authed":
        expected = f"List<{_ENTITY}>"
        schema = "created_and_authed"
    if logical != expected:
        message = f"compiled project {primitive} response changed"
        raise ValueError(message)
    entity = require_model(snapshot, _ENTITY, domain="project")
    if (
        entity.extends is not None
        or model_field_facts(entity) != _ENTITY_FIELDS[policy.entity]
    ):
        message = "compiled project entity fields changed"
        raise ValueError(message)
    return CompiledResponsePolicy(
        codec=codec,
        schema=schema,
        capture=[] if primitive == "created_and_authed" else {},
    )


def _require_request_fields(
    version: str, operation: OperationSpec, primitive: str, policy: _ProjectPolicy
) -> None:
    expected: dict[str, tuple[str, bool | None]] = {}
    if primitive == "page":
        required = True if policy.annotated_page_required else None
        expected = {
            "searchVal": ("String", False),
            "pageSize": ("Integer", required),
            "pageNo": ("Integer", required),
        }
    if primitive in {"get", "delete", "delete_legacy", "update"}:
        if version in _LEGACY:
            expected["projectId"] = ("Integer", None)
        else:
            expected["code"] = ("long" if primitive == "get" else "Long", None)
    if primitive in {"create", "update"}:
        expected.update(projectName=("String", None), description=("String", False))
        if primitive == "update" and policy.update_owner:
            expected["userName"] = ("String", None)
    actual = {
        item.wire_name: (item.java_type, item.required, item.default_value)
        for item in operation.parameters
        if is_client_supplied_parameter(item)
    }
    if actual != {name: (*facts, None) for name, facts in expected.items()}:
        message = (
            f"compiled project {primitive} request type, "
            "requiredness or default changed"
        )
        raise ValueError(message)


def _recipe(codecs: Mapping[str, str]) -> str:
    for version, policy in _POLICIES.items():
        if codecs == _codecs(version, policy):
            return policy.recipe
    message = f"compiled project recipe is unsupported: {codecs!r}"
    raise ValueError(message)


PROJECT_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="project",
    schema_constant="COMPILED_PROJECT_SCHEMA_VERSION",
    schema_version=COMPILED_PROJECT_SCHEMA_VERSION,
    semantic_operations=COMPILED_PROJECT_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=(
        CompiledPrimitive(
            name="page",
            requests=tuple(
                CompiledRequestEpoch(
                    method="GET",
                    path=path,
                    channel="query",
                    request_schema="page",
                    request_model="ProjectPageParams",
                    request_fields=("searchVal", "pageSize", "pageNo"),
                    required_fields=frozenset({"pageSize", "pageNo"}),
                    versions=versions,
                )
                for path, versions in (
                    ("projects/list-paging", _LEGACY),
                    ("projects", _CODE_VERSIONS),
                )
            ),
            result_envelope="optional",
        ),
        CompiledPrimitive(
            name="get",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path="projects/query-by-id",
                    channel="query",
                    request_schema="id",
                    request_model="ProjectIdParams",
                    request_fields=("projectId",),
                    required_fields=frozenset({"projectId"}),
                    versions=_LEGACY,
                ),
                CompiledRequestEpoch(
                    method="GET",
                    path="projects/{code}",
                    channel="path",
                    request_schema="code",
                    request_model="ProjectCodeParams",
                    request_fields=("code",),
                    path_fields=("code",),
                    required_fields=frozenset({"code"}),
                    versions=_CODE_VERSIONS,
                ),
            ),
            result_envelope="optional",
        ),
        CompiledPrimitive(
            name="create",
            requests=tuple(
                CompiledRequestEpoch(
                    method="POST",
                    path=path,
                    channel="form",
                    request_schema="create",
                    request_model="ProjectCreateParams",
                    request_fields=("projectName", "description"),
                    required_fields=frozenset({"projectName"}),
                    versions=versions,
                )
                for path, versions in (
                    ("projects/create", _LEGACY),
                    ("projects", _CODE_VERSIONS),
                )
            ),
            result_envelope="required",
        ),
        CompiledPrimitive(
            name="created_and_authed",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path="projects/created-and-authed",
                    channel="query",
                    request_schema="empty",
                    request_model="ProjectEmptyParams",
                    request_fields=(),
                    required_fields=frozenset(),
                    versions=_CREATED_LIST,
                ),
            ),
            result_envelope="optional",
            absent_versions=frozenset(_POLICIES) - _CREATED_LIST,
        ),
        CompiledPrimitive(
            name="update",
            requests=(
                CompiledRequestEpoch(
                    method="POST",
                    path="projects/update",
                    channel="form",
                    request_schema="update_legacy",
                    request_model="ProjectUpdateLegacyParams",
                    request_fields=("projectId", "projectName", "description"),
                    required_fields=frozenset({"projectId", "projectName"}),
                    versions=_LEGACY,
                ),
                CompiledRequestEpoch(
                    method="PUT",
                    path="projects/{code}",
                    channel="path_form",
                    request_schema="update_owner",
                    request_model="ProjectUpdateOwnerParams",
                    request_fields=("code", "projectName", "description", "userName"),
                    path_fields=("code",),
                    required_fields=frozenset({"code", "projectName", "userName"}),
                    versions=_OWNER_VERSIONS,
                ),
                CompiledRequestEpoch(
                    method="PUT",
                    path="projects/{code}",
                    channel="path_form",
                    request_schema="update_direct",
                    request_model="ProjectUpdateDirectParams",
                    request_fields=("code", "projectName", "description"),
                    path_fields=("code",),
                    required_fields=frozenset({"code", "projectName"}),
                    versions=_CODE_VERSIONS - _OWNER_VERSIONS,
                ),
            ),
            result_envelope="required",
        ),
        CompiledPrimitive(
            name="delete_legacy",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path="projects/delete",
                    channel="query",
                    request_schema="id",
                    request_model="ProjectIdParams",
                    request_fields=("projectId",),
                    required_fields=frozenset({"projectId"}),
                    versions=_LEGACY,
                ),
            ),
            # The old mutation session dispatches this GET through its read
            # session. Preserve optional envelopes; retry policy stays runtime-owned.
            result_envelope="optional",
            absent_versions=_CODE_VERSIONS,
        ),
        CompiledPrimitive(
            name="delete",
            requests=(
                CompiledRequestEpoch(
                    method="DELETE",
                    path="projects/{code}",
                    channel="path",
                    request_schema="code",
                    request_model="ProjectCodeParams",
                    request_fields=("code",),
                    path_fields=("code",),
                    required_fields=frozenset({"code"}),
                    versions=_CODE_VERSIONS,
                ),
            ),
            result_envelope="required",
            absent_versions=_LEGACY,
        ),
    ),
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
)

__all__ = ["COMPILED_PROJECT_SCHEMA_VERSION", "PROJECT_COMPILED_DOMAIN"]
