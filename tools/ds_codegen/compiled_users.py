"""Compile user management and current identity from exact security contracts."""

from __future__ import annotations

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
from ds_codegen.governance_contract import governance_contract
from ds_codegen.security_contract import TARGET_SECURITY_VERSIONS, security_contract
from ds_codegen.task_definition_cleanup_contract import cleanup_strict_integer_fields

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ds_codegen.ir import ContractSnapshot, OperationSpec
    from ds_codegen.security_contract import UserRecipe
    from dsctl.support.json_types import JsonValue

COMPILED_USER_SCHEMA_VERSION = 1
COMPILED_USER_SEMANTIC_OPERATIONS = frozenset(
    {
        "identity.current",
        "user.identity",
        "user.list",
        "user.get",
        "user.create",
        "user.update",
        "user.delete",
        "user.grant.project",
        "user.revoke.project",
        "user.grant.datasource",
        "user.revoke.datasource",
        "user.grant.namespace",
        "user.revoke.namespace",
    }
)
_NAMESPACE_ABSENT = frozenset(
    version
    for version in TARGET_SECURITY_VERSIONS
    if security_contract(version).user.namespace_support == "absent"
)
_REVOKE_ABSENT = frozenset(
    version
    for version in TARGET_SECURITY_VERSIONS
    if security_contract(version).user.project_revoke == "absent"
)
_REVOKE_PRIMITIVE_ABSENT = frozenset(
    version
    for version in TARGET_SECURITY_VERSIONS
    if security_contract(version).user.project_revoke in {"absent", "replace-by-id"}
)

_PAGE = "org.apache.dolphinscheduler.api.utils.PageInfo"
_USER = "org.apache.dolphinscheduler.dao.entity.User"
_PROJECT = "org.apache.dolphinscheduler.dao.entity.Project"
_DATASOURCE = "org.apache.dolphinscheduler.dao.entity.DataSource"
_USER_IDENTITY = "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO"
_DATASOURCE_IDENTITY = "org.apache.dolphinscheduler.api.vo.DataSourceSimpleInfoVO"
_NAMESPACE = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_USER_TYPE = "org.apache.dolphinscheduler.common.enums.UserType"
_DB_TYPE_COMMON = "org.apache.dolphinscheduler.common.enums.DbType"
_DB_TYPE_SPI = "org.apache.dolphinscheduler.spi.enums.DbType"
_Field = tuple[str, str, bool, str | None, str | None]
_Fields = tuple[_Field, ...]
_REQUIRED_ID: _Field = ("id", "int", False, "0", None)
_NULLABLE_ID: _Field = ("id", "Integer", True, None, None)
_USER_ID: _Field = ("userId", "int", False, "0", None)
_USER_NAME: _Field = ("userName", "String", True, None, None)
_TIMES: _Fields = (
    ("createTime", "Date", True, None, None),
    ("updateTime", "Date", True, None, None),
)
_USER_PREFIX: _Fields = (
    _USER_NAME,
    ("userPassword", "String", True, None, None),
    ("email", "String", True, None, None),
    ("phone", "String", True, None, None),
    ("userType", _USER_TYPE, True, None, None),
    ("tenantId", "int", False, "0", None),
)
_USER_QUEUE: _Fields = (
    ("queueName", "String", True, None, None),
    ("alertGroup", "String", True, None, None),
    ("queue", "String", True, None, None),
)
_PROJECT_BODY: _Fields = (
    ("name", "String", True, None, None),
    ("description", "String", True, None, None),
    *_TIMES,
    ("perm", "int", False, "0", None),
    ("defCount", "int", False, "0", None),
)
_PROJECT_RUNNING: _Fields = (("instRunningCount", "int", False, "0", None),)
_NULLABLE_PAGE: _Fields = (
    ("totalList", "List<T>", True, None, None),
    ("total", "Integer", False, "0", None),
    ("totalPage", "Integer", True, None, None),
    ("pageSize", "Integer", False, "20", None),
    ("currentPage", "Integer", True, "0", None),
    ("pageNo", "Integer", True, None, None),
)
_LIST_PAGE: _Fields = (
    ("totalList", "List<T>", False, None, "list"),
    *_NULLABLE_PAGE[1:],
)
_LEGACY_PAGE: _Fields = (*_NULLABLE_PAGE[:3], _NULLABLE_PAGE[4])
_NAMESPACE_QUOTAS: _Fields = (
    ("limitsCpu", "Double", True, None, None),
    ("limitsMemory", "Integer", True, None, None),
)
_NAMESPACE_PODS: _Fields = (
    ("podRequestCpu", "Double", False, "0.0", None),
    ("podRequestMemory", "Integer", False, "0", None),
    ("podReplicas", "Integer", False, "0", None),
)
_NAMESPACE_CLUSTER: _Fields = (
    ("clusterCode", "Long", True, None, None),
    ("clusterName", "String", True, None, None),
)
_DB_NAMES = (
    "MYSQL",
    "POSTGRESQL",
    "HIVE",
    "SPARK",
    "CLICKHOUSE",
    "ORACLE",
    "SQLSERVER",
    "DB2",
    "PRESTO",
    "H2",
    "REDSHIFT",
    "ATHENA",
    "TRINO",
    "STARROCKS",
    "AZURESQL",
    "DAMENG",
    "OCEANBASE",
    "SSH",
    "KYUUBI",
    "DATABEND",
    "SNOWFLAKE",
    "VERTICA",
    "HANA",
    "DORIS",
    "ZEPPELIN",
    "SAGEMAKER",
    "K8S",
    "ALIYUN_SERVERLESS_SPARK",
    "DOLPHINDB",
)

_PRIMITIVES = (
    CompiledPrimitive(
        name="page",
        requests=tuple(
            CompiledRequestEpoch(
                method="GET",
                path="users/list-paging",
                channel="query",
                request_schema=schema,
                request_model=model,
                request_fields=fields,
                required_fields=frozenset({"pageNo", "pageSize"}),
            )
            for schema, model, fields in (
                (
                    "page_legacy",
                    "UserLegacyPageParams",
                    ("pageNo", "searchVal", "pageSize"),
                ),
                ("page", "UserPageParams", ("pageNo", "pageSize", "searchVal")),
            )
        ),
        result_envelope="optional",
    ),
    *(
        CompiledPrimitive(
            name=name,
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=path,
                    channel="query",
                    request_schema="empty",
                    request_model="UserEmptyParams",
                    request_fields=(),
                    required_fields=frozenset(),
                ),
            ),
            result_envelope="optional",
        )
        for name, path in (
            ("list", "users/list"),
            ("all", "users/list-all"),
            ("current", "users/get-user-info"),
        )
    ),
    *(
        CompiledPrimitive(
            name=action,
            requests=tuple(
                CompiledRequestEpoch(
                    method="POST",
                    path=f"users/{action}",
                    channel="form",
                    request_schema=f"{action}_{epoch}",
                    request_model=f"User{action.title()}{epoch.title()}Params",
                    request_fields=fields,
                    required_fields=frozenset(fields)
                    - {"queue", "phone", "state", "timeZone"},
                )
                for epoch, fields in epochs
            ),
            result_envelope="required",
        )
        for action, epochs in (
            (
                "create",
                (
                    (
                        "legacy",
                        (
                            "userName",
                            "userPassword",
                            "tenantId",
                            "queue",
                            "email",
                            "phone",
                        ),
                    ),
                    (
                        "state",
                        (
                            "userName",
                            "userPassword",
                            "tenantId",
                            "queue",
                            "email",
                            "phone",
                            "state",
                        ),
                    ),
                ),
            ),
            (
                "update",
                (
                    (
                        "legacy",
                        (
                            "id",
                            "userName",
                            "userPassword",
                            "queue",
                            "email",
                            "tenantId",
                            "phone",
                        ),
                    ),
                    (
                        "state",
                        (
                            "id",
                            "userName",
                            "userPassword",
                            "queue",
                            "email",
                            "tenantId",
                            "phone",
                            "state",
                        ),
                    ),
                    (
                        "timezone",
                        (
                            "id",
                            "userName",
                            "userPassword",
                            "queue",
                            "email",
                            "tenantId",
                            "phone",
                            "state",
                            "timeZone",
                        ),
                    ),
                ),
            ),
        )
    ),
    CompiledPrimitive(
        name="delete",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="users/delete",
                channel="form",
                request_schema="id",
                request_model="UserIdParams",
                request_fields=("id",),
                required_fields=frozenset({"id"}),
            ),
        ),
        result_envelope="required",
    ),
    *(
        CompiledPrimitive(
            name=f"{resource}_{access}",
            requests=(
                CompiledRequestEpoch(
                    method="GET",
                    path=f"{prefix}/{route}-{resource}",
                    channel="query",
                    request_schema="permission_user",
                    request_model="UserPermissionParams",
                    request_fields=("userId",),
                    required_fields=frozenset({"userId"}),
                ),
            ),
            result_envelope="optional",
            absent_versions=_NAMESPACE_ABSENT
            if resource == "namespace"
            else frozenset(),
        )
        for resource, prefix in (
            ("project", "projects"),
            ("datasource", "datasources"),
            ("namespace", "k8s-namespace"),
        )
        for access, route in (("authorized", "authed"), ("unauthorized", "unauth"))
    ),
    *(
        CompiledPrimitive(
            name=f"{resource}_grant",
            requests=(
                CompiledRequestEpoch(
                    method="POST",
                    path=f"users/grant-{resource}",
                    channel="form",
                    request_schema=f"{resource}_grant",
                    request_model=f"User{resource.title()}GrantParams",
                    request_fields=("userId", f"{resource}Ids"),
                    required_fields=frozenset({"userId", f"{resource}Ids"}),
                ),
            ),
            result_envelope="required",
            absent_versions=_NAMESPACE_ABSENT
            if resource == "namespace"
            else frozenset(),
        )
        for resource in ("project", "datasource", "namespace")
    ),
    CompiledPrimitive(
        name="project_revoke",
        requests=(
            CompiledRequestEpoch(
                method="POST",
                path="users/revoke-project",
                channel="form",
                request_schema="project_revoke",
                request_model="UserProjectRevokeParams",
                request_fields=("userId", "projectCode"),
                required_fields=frozenset({"userId", "projectCode"}),
            ),
            CompiledRequestEpoch(
                method="POST",
                path="users/revoke-project-by-id",
                channel="form",
                request_schema="project_grant",
                request_model="UserProjectGrantParams",
                request_fields=("userId", "projectIds"),
                required_fields=frozenset({"userId", "projectIds"}),
            ),
        ),
        result_envelope="required",
        absent_versions=_REVOKE_PRIMITIVE_ABSENT,
    ),
)

_OPERATIONS = {
    "UsersController.queryUserList": "page",
    "UsersController.listUser": "list",
    "UsersController.listAll": "all",
    "UsersController.getUserInfo": "current",
    "UsersController.createUser": "create",
    "UsersController.updateUser": "update",
    "UsersController.delUserById": "delete",
    "UsersController.grantProject": "project_grant",
    "UsersController.revokeProject": "project_revoke",
    "UsersController.revokeProjectById": "project_revoke",
    "UsersController.grantDataSource": "datasource_grant",
    "UsersController.grantNamespace": "namespace_grant",
    "ProjectController.queryAuthorizedProject": "project_authorized",
    "ProjectController.queryUnauthorizedProject": "project_unauthorized",
    "K8sNamespaceController.queryAuthorizedNamespace": "namespace_authorized",
    "K8sNamespaceController.queryUnauthorizedNamespace": "namespace_unauthorized",
    **{
        getattr(
            security_contract(version).user, f"datasource_{access}_operation"
        ): f"datasource_{access}"
        for version in TARGET_SECURITY_VERSIONS
        for access in ("authorized", "unauthorized")
    },
}


def _classify(operation: OperationSpec) -> str | None:
    return _OPERATIONS.get(operation.operation_id)


def _response(
    snapshot: ContractSnapshot, operation: OperationSpec, primitive: str
) -> CompiledResponsePolicy:
    recipe = security_contract(snapshot.ds_version).user
    _require_request_fields(operation, primitive, recipe)
    if operation.response_projection != "direct" or operation.consumes:
        msg = f"compiled user {primitive} exchange projection changed"
        raise ValueError(msg)
    logical = operation.logical_return_type
    schema: str | None
    capture: JsonValue
    identity_model = (
        _USER_IDENTITY
        if primitive == "all" and recipe.simple_user_list
        else _DATASOURCE_IDENTITY
        if primitive in {"datasource_authorized", "datasource_unauthorized"}
        and recipe.simple_user_list
        else None
    )
    if identity_model is not None:
        expected_fields = (
            _NULLABLE_ID,
            _USER_NAME if primitive == "all" else ("name", "String", True, None, None),
        )
        if (
            model_field_facts(require_model(snapshot, identity_model, domain="user"))
            != expected_fields
        ):
            msg = f"compiled user identity fields changed: {identity_model}"
            raise ValueError(msg)
        if logical != f"List<{identity_model}>":
            msg = f"compiled user {primitive} identity response changed: {logical}"
            raise ValueError(msg)
        schema = "user_identity" if primitive == "all" else "datasource_identity"
        return CompiledResponsePolicy(
            codec=f"{primitive}_{schema}", schema=schema, capture=[]
        )
    if primitive in {"page", "list", "all", "current", "create", "update"}:
        epoch = _user_epoch(snapshot, recipe)
        if primitive == "page":
            expected = f"{_PAGE}<{_USER}>"
            schema, capture = f"page_{epoch}_{_page_epoch(snapshot)}", {}
        elif primitive in {"list", "all"}:
            expected = f"List<{_USER}>"
            schema, capture = f"list_{epoch}", []
        elif primitive == "current":
            expected, schema, capture = _USER, f"entity_{epoch}", {}
        else:
            result = (
                recipe.create_result if primitive == "create" else recipe.update_result
            )
            expected = {
                "none": "Void",
                "entity": _USER,
                "optional": f"Optional<{_USER}>",
            }[result]
            if logical != expected:
                msg = f"compiled user {primitive} result contradicts reviewed recipe"
                raise ValueError(msg)
            schema = None if result == "none" else f"entity_{epoch}"
            if result == "optional":
                schema = f"optional_entity_{epoch}"
            return CompiledResponsePolicy(
                codec=f"{primitive}_{epoch}_{result}",
                schema=schema,
                capture=None if schema is None else {},
            )
        if logical == expected:
            return CompiledResponsePolicy(
                codec=f"{primitive}_{schema}", schema=schema, capture=capture
            )
    elif primitive.endswith(("_authorized", "_unauthorized")):
        resource = primitive.split("_", 1)[0]
        model = {
            "project": _PROJECT,
            "datasource": _DATASOURCE,
            "namespace": _NAMESPACE,
        }[resource]
        epoch = {
            "project": _project_epoch,
            "datasource": _datasource_epoch,
            "namespace": _namespace_epoch,
        }[resource](snapshot)
        if logical == f"List<{model}>":
            schema = f"{resource}_{epoch}"
            return CompiledResponsePolicy(
                codec=f"{primitive}_{epoch}", schema=schema, capture=[]
            )
    elif logical == "Void":
        suffix = (
            recipe.project_revoke.replace("-", "_")
            if primitive == "project_revoke"
            else "void"
        )
        return CompiledResponsePolicy(
            codec=f"{primitive}_{suffix}", schema=None, capture=None
        )
    msg = f"compiled user {primitive} response changed: {logical}"
    raise ValueError(msg)


def _require_request_fields(
    operation: OperationSpec, primitive: str, recipe: UserRecipe
) -> None:
    if primitive.startswith("namespace_") and recipe.namespace_support != "supported":
        message = "compiled user namespace permission contradicts reviewed support"
        raise ValueError(message)
    types = {
        "id": "int",
        "userId": "int",
        "tenantId": "int",
        "state": "int",
        "pageNo": "Integer",
        "pageSize": "Integer",
        "projectCode": "long",
        "userName": "String",
        "userPassword": "String",
        "queue": "String",
        "email": "String",
        "phone": "String",
        "timeZone": "String",
        "searchVal": "String",
        "projectIds": "String",
        "datasourceIds": "String",
        "namespaceIds": "String",
    }
    if primitive.endswith(("_authorized", "_unauthorized")):
        types["userId"] = "Integer"
    parameters = tuple(
        item for item in operation.parameters if is_client_supplied_parameter(item)
    )
    if any(
        item.java_type != types.get(item.wire_name or "")
        or item.default_value != ("" if item.wire_name == "queue" else None)
        for item in parameters
    ):
        msg = "compiled user request type or default changed"
        raise ValueError(msg)
    fields = {item.wire_name for item in parameters}
    if primitive in {"create", "update"} and (
        ("state" in fields) != recipe.has_state
        or ("timeZone" in fields) != (primitive == "update" and recipe.has_time_zone)
    ):
        msg = "compiled user mutation request contradicts reviewed fields"
        raise ValueError(msg)
    if primitive in {
        "datasource_authorized",
        "datasource_unauthorized",
    } and operation.operation_id != getattr(recipe, f"{primitive}_operation"):
        msg = "compiled user datasource operation contradicts reviewed recipe"
        raise ValueError(msg)
    if primitive == "project_revoke":
        expected = {
            "by-code": "UsersController.revokeProject",
            "by-id": "UsersController.revokeProjectById",
        }.get(recipe.project_revoke)
        if operation.operation_id != expected:
            msg = "compiled user project revoke contradicts reviewed recipe"
            raise ValueError(msg)


def _user_epoch(snapshot: ContractSnapshot, recipe: UserRecipe) -> str:
    model = require_model(snapshot, _USER, domain="user")
    fields = model_field_facts(model)
    identifier = fields[:1]
    if identifier not in {(_REQUIRED_ID,), (_NULLABLE_ID,)}:
        msg = "compiled user identifier epoch changed"
        raise ValueError(msg)
    expected: _Fields = (*identifier, *_USER_PREFIX)
    epoch = "legacy"
    if recipe.has_state:
        expected += (("state", "int", False, "0", None),)
        epoch = "state"
    expected += (("tenantCode", "String", True, None, None),)
    if not recipe.has_state:
        expected += (("tenantName", "String", True, None, None),)
    expected += _USER_QUEUE
    if recipe.has_time_zone:
        expected += (("timeZone", "String", True, None, None),)
        epoch = "timezone"
    if identifier == (_NULLABLE_ID,):
        if not recipe.has_time_zone:
            msg = "compiled user nullable identifier requires timezone epoch"
            raise ValueError(msg)
        epoch += "_nullable"
    if fields != (*expected, *_TIMES):
        msg = "compiled user entity contradicts reviewed fields"
        raise ValueError(msg)
    _require_enum(
        snapshot,
        _USER_TYPE,
        (("code", "int"), ("descp", "String")),
        (("ADMIN_USER", ("0", "admin user")), ("GENERAL_USER", ("1", "general user"))),
    )
    return epoch


def _page_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(require_model(snapshot, _PAGE, domain="user"))
    strict = bool(cleanup_strict_integer_fields(snapshot.ds_version).get(_PAGE))
    if fields == _LEGACY_PAGE and not strict:
        return "legacy"
    if fields == _NULLABLE_PAGE and strict:
        return "nullable_strict"
    if fields == _LIST_PAGE:
        return "list_strict" if strict else "list"
    msg = "compiled user page epoch changed"
    raise ValueError(msg)


def _project_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(require_model(snapshot, _PROJECT, domain="user"))
    legacy = (_REQUIRED_ID, _USER_ID, _USER_NAME)
    code: _Fields = (("code", "long", False, "0", None),)
    nullable: _Fields = (
        _NULLABLE_ID,
        ("userId", "Integer", True, None, None),
        _USER_NAME,
    )
    epochs = {
        "id": (*legacy, *_PROJECT_BODY, *_PROJECT_RUNNING),
        "code": (*legacy, *code, *_PROJECT_BODY, *_PROJECT_RUNNING),
        "code_nullable": (*nullable, *code, *_PROJECT_BODY, *_PROJECT_RUNNING),
        "code_compact": (*nullable, *code, *_PROJECT_BODY),
    }
    for epoch, expected in epochs.items():
        if fields == expected and (epoch == "id") == (
            security_contract(snapshot.ds_version).user.project_identity == "id"
        ):
            return epoch
    msg = "compiled user permission project epoch changed"
    raise ValueError(msg)


def _datasource_epoch(snapshot: ContractSnapshot) -> str:
    fields = model_field_facts(require_model(snapshot, _DATASOURCE, domain="user"))
    identifier = fields[:1]
    type_field = next((item for item in fields if item[0] == "type"), None)
    if identifier not in {(_REQUIRED_ID,), (_NULLABLE_ID,)} or type_field not in {
        ("type", _DB_TYPE_COMMON, True, None, None),
        ("type", _DB_TYPE_SPI, True, None, None),
    }:
        msg = "compiled user permission datasource identity or enum changed"
        raise ValueError(msg)
    expected = (
        *identifier,
        _USER_ID,
        _USER_NAME,
        ("name", "String", True, None, None),
        ("note", "String", True, None, None),
        type_field,
        ("connectionParams", "String", True, None, None),
        *_TIMES,
    )
    if fields != expected:
        msg = "compiled user permission datasource fields changed"
        raise ValueError(msg)
    enum_epoch = _db_type_epoch(snapshot, type_field[1])
    suffix = "nullable" if identifier == (_NULLABLE_ID,) else "required"
    return f"{enum_epoch}_{suffix}"


def _db_type_epoch(snapshot: ContractSnapshot, import_path: str) -> str:
    matches = [item for item in snapshot.enums if item.import_path == import_path]
    if len(matches) != 1:
        msg = "compiled user datasource enum is missing or ambiguous"
        raise ValueError(msg)
    enum = matches[0]
    names = tuple(item.name for item in enum.values)
    legacy_names = (*_DB_NAMES[:8], "H2")
    if import_path == _DB_TYPE_COMMON and names == legacy_names:
        epoch, columns = "legacy", 2
    elif import_path == _DB_TYPE_COMMON and names == _DB_NAMES[:10]:
        epoch, columns = "common", 1
    elif import_path == _DB_TYPE_SPI and names in {
        _DB_NAMES[:n] for n in (10, 11, 12, 24, 26, 29)
    }:
        columns = len(enum.fields)
        if (len(names), columns) not in {
            (10, 2),
            (11, 2),
            (12, 2),
            (24, 2),
            (26, 2),
            (26, 3),
            (29, 3),
        }:
            msg = "compiled user datasource enum metadata epoch changed"
            raise ValueError(msg)
        epoch = f"spi_{len(names)}_{columns}"
    else:
        msg = "compiled user datasource enum membership changed"
        raise ValueError(msg)
    enum_fields = (("code", "int"), ("descp", "String"))[:columns]
    if columns == 3:
        enum_fields = (("code", "int"), ("name", "String"), ("descp", "String"))
    values = []
    for name in names:
        arguments: tuple[str, ...] = (str(_DB_NAMES.index(name)),)
        if columns >= 2:
            arguments += (name.lower(),)
        if columns == 3:
            arguments += (
                "aliyun serverless spark"
                if name == "ALIYUN_SERVERLESS_SPARK"
                else name.lower(),
            )
        values.append((name, arguments))
    _require_enum(snapshot, import_path, enum_fields, tuple(values))
    return epoch


def _namespace_epoch(snapshot: ContractSnapshot) -> str:
    recipe = governance_contract(snapshot.ds_version).namespace
    fields = model_field_facts(require_model(snapshot, _NAMESPACE, domain="user"))
    prefix: _Fields = (_NULLABLE_ID,)
    if recipe.selector == "cluster-code":
        prefix += (("code", "Long", True, None, None),)
    expected = (*prefix, ("namespace", "String", True, None, None))
    if recipe.quotas_supported:
        expected += _NAMESPACE_QUOTAS
    expected += (_USER_ID, _USER_NAME, *_TIMES)
    if recipe.quotas_supported:
        expected += _NAMESPACE_PODS
    if recipe.selector == "k8s":
        expected += (
            ("onlineJobNum", "Integer", False, "0", None),
            ("k8s", "String", True, None, None),
        )
        epoch = "k8s_quota"
    else:
        expected += _NAMESPACE_CLUSTER
        epoch = "cluster_quota" if recipe.quotas_supported else "cluster"
    if recipe.support != "supported" or fields != expected:
        msg = "compiled user permission namespace epoch changed"
        raise ValueError(msg)
    return epoch


def _require_enum(
    snapshot: ContractSnapshot,
    import_path: str,
    fields: tuple[tuple[str, str], ...],
    values: tuple[tuple[str, tuple[str, ...]], ...],
) -> None:
    matches = [item for item in snapshot.enums if item.import_path == import_path]
    if (
        len(matches) != 1
        or matches[0].json_value_field is not None
        or tuple((item.name, item.java_type) for item in matches[0].fields) != fields
        or tuple((item.name, tuple(item.arguments)) for item in matches[0].values)
        != values
    ):
        msg = f"compiled user enum epoch changed: {import_path}"
        raise ValueError(msg)


def _recipe(codecs: Mapping[str, str]) -> str:
    coordinate = (
        codecs.get("create"),
        codecs.get("update"),
        codecs.get("project_revoke"),
        "namespace_grant" in codecs,
    )
    recipes = {
        ("create_legacy_none", "update_legacy_none", None, False): "legacy_139",
        ("create_state_none", "update_state_none", None, False): "legacy_200",
        (
            "create_state_entity",
            "update_state_none",
            "project_revoke_by_code",
            False,
        ): "state_entity",
        (
            "create_timezone_entity",
            "update_timezone_none",
            "project_revoke_by_code",
            True,
        ): "timezone_entity",
        (
            "create_timezone_nullable_entity",
            "update_timezone_nullable_none",
            "project_revoke_by_code",
            True,
        ): "timezone_entity",
        (
            "create_timezone_nullable_optional",
            "update_timezone_nullable_none",
            "project_revoke_by_id",
            True,
        ): "optional_void",
        (
            "create_timezone_nullable_optional",
            "update_timezone_nullable_entity",
            "project_revoke_by_id",
            True,
        ): "optional_entity",
    }
    if coordinate in recipes:
        recipe = recipes[coordinate]
        if codecs.get("all") == "all_user_identity":
            if recipe != "optional_entity":
                msg = "compiled user identity list contradicts mutation recipe"
                raise ValueError(msg)
            return "optional_entity_simple_identity"
        return recipe
    msg = f"compiled user recipe is unsupported: {coordinate!r}"
    raise ValueError(msg)


USER_COMPILED_DOMAIN = CompiledDomainDefinition(
    name="user",
    schema_constant="COMPILED_USER_SCHEMA_VERSION",
    schema_version=COMPILED_USER_SCHEMA_VERSION,
    semantic_operations=COMPILED_USER_SEMANTIC_OPERATIONS,
    absent_versions=frozenset(),
    primitives=_PRIMITIVES,
    classify_operation=_classify,
    response_policy=_response,
    recipe_policy=_recipe,
    semantic_absent_versions={
        "user.identity": frozenset(
            version
            for version in TARGET_SECURITY_VERSIONS
            if not security_contract(version).user.simple_user_list
        ),
        "user.revoke.project": _REVOKE_ABSENT,
        "user.grant.namespace": _NAMESPACE_ABSENT,
        "user.revoke.namespace": _NAMESPACE_ABSENT,
    },
)

__all__ = [
    "COMPILED_USER_SCHEMA_VERSION",
    "COMPILED_USER_SEMANTIC_OPERATIONS",
    "USER_COMPILED_DOMAIN",
]
