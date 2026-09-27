"""Reviewed exact-version recipes for users and access tokens.

This module is deliberately independent of the central runtime-binding tables.
It records the source facts needed to integrate the security domain into those
tables without encoding semantic-version ranges or inferring support from a
neighbouring release.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

TARGET_SECURITY_VERSIONS = (
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
    "3.2.0",
    "3.2.1",
    "3.2.2",
    "3.3.1",
    "3.3.2",
    "3.4.0",
    "3.4.1",
    "3.4.2",
    "3.4.3",
)

Support = Literal["supported", "limited", "absent"]
EntityResult = Literal["none", "entity", "optional"]
DeleteResult = Literal["none", "boolean"]
ProjectIdentity = Literal["id", "code"]
ProjectRevoke = Literal["replace-by-id", "absent", "by-code", "by-id"]


@dataclass(frozen=True)
class Evidence:
    """One exact upstream source coordinate supporting a reviewed decision."""

    source: str
    symbol: str
    conclusion: str


@dataclass(frozen=True)
class UserRecipe:
    """Wire and facet decisions for one exact user-controller contract."""

    create_result: EntityResult
    update_result: EntityResult
    has_state: bool
    has_time_zone: bool
    project_identity: ProjectIdentity
    project_revoke: ProjectRevoke
    namespace_support: Support
    datasource_authorized_operation: str
    datasource_unauthorized_operation: str
    datasource_authorized_method: str
    datasource_unauthorized_method: str
    simple_user_list: bool = False


@dataclass(frozen=True)
class AccessTokenRecipe:
    """Wire and readback decisions for one exact access-token contract."""

    create_result: EntityResult
    update_result: EntityResult
    delete_result: DeleteResult
    legacy_form_routes: bool
    token_required: bool


@dataclass(frozen=True)
class SecurityVersionContract:
    """Reviewed security-domain contract for one exact DS version."""

    version: str
    user: UserRecipe
    access_token: AccessTokenRecipe


_USER_MODEL = "org.apache.dolphinscheduler.dao.entity.User"
_TOKEN_MODEL = "org.apache.dolphinscheduler.dao.entity.AccessToken"  # noqa: S105
_PROJECT_MODEL = "org.apache.dolphinscheduler.dao.entity.Project"
_DATASOURCE_MODEL = "org.apache.dolphinscheduler.dao.entity.DataSource"
_NAMESPACE_MODEL = "org.apache.dolphinscheduler.dao.entity.K8sNamespace"
_TENANT_MODEL = "org.apache.dolphinscheduler.dao.entity.Tenant"

_USER_READ_OPERATIONS = (
    "UsersController.queryUserList",
    "UsersController.listUser",
    "UsersController.listAll",
)
_TOKEN_LIST_OPERATION = "AccessTokenController.queryAccessTokenList"  # noqa: S105
_TOKEN_GENERATE_OPERATION = "AccessTokenController.generateToken"  # noqa: S105
_PROJECT_PERMISSION_READS = (
    "ProjectController.queryAuthorizedProject",
    "ProjectController.queryUnauthorizedProject",
)
_NAMESPACE_PERMISSION_READS = (
    "K8sNamespaceController.queryAuthorizedNamespace",
    "K8sNamespaceController.queryUnauthorizedNamespace",
)


def _user_recipe(
    *,
    create_result: EntityResult,
    update_result: EntityResult,
    has_state: bool,
    has_time_zone: bool,
    project_identity: ProjectIdentity,
    project_revoke: ProjectRevoke,
    namespace_support: Support,
    datasource_authorized_operation: str,
    datasource_unauthorized_operation: str,
    datasource_authorized_method: str,
    datasource_unauthorized_method: str,
) -> UserRecipe:
    return UserRecipe(
        create_result=create_result,
        update_result=update_result,
        has_state=has_state,
        has_time_zone=has_time_zone,
        project_identity=project_identity,
        project_revoke=project_revoke,
        namespace_support=namespace_support,
        datasource_authorized_operation=datasource_authorized_operation,
        datasource_unauthorized_operation=datasource_unauthorized_operation,
        datasource_authorized_method=datasource_authorized_method,
        datasource_unauthorized_method=datasource_unauthorized_method,
    )


_DATASOURCE_LEGACY = {
    "datasource_authorized_operation": "DataSourceController.authedDatasource",
    "datasource_unauthorized_operation": "DataSourceController.unauthDatasource",
    "datasource_authorized_method": "authed_datasource",
    "datasource_unauthorized_method": "unauth_datasource",
}
_DATASOURCE_CAMEL_AUTH = {
    "datasource_authorized_operation": "DataSourceController.authedDatasource",
    "datasource_unauthorized_operation": "DataSourceController.unAuthDatasource",
    "datasource_authorized_method": "authed_datasource",
    "datasource_unauthorized_method": "un_auth_datasource",
}
_DATASOURCE_342 = {
    "datasource_authorized_operation": (
        "DataSourceController.getAuthorizedDatasourceList"
    ),
    "datasource_unauthorized_operation": (
        "DataSourceController.getUnauthorizedDatasourceList"
    ),
    "datasource_authorized_method": "get_authorized_datasource_list",
    "datasource_unauthorized_method": "get_unauthorized_datasource_list",
}

_USER_139 = _user_recipe(
    create_result="none",
    update_result="none",
    has_state=False,
    has_time_zone=False,
    project_identity="id",
    project_revoke="replace-by-id",
    namespace_support="absent",
    **_DATASOURCE_LEGACY,
)
_USER_200 = _user_recipe(
    create_result="none",
    update_result="none",
    has_state=True,
    has_time_zone=False,
    project_identity="code",
    project_revoke="absent",
    namespace_support="absent",
    **_DATASOURCE_LEGACY,
)
_USER_20_ENTITY = _user_recipe(
    create_result="entity",
    update_result="none",
    has_state=True,
    has_time_zone=False,
    project_identity="code",
    project_revoke="by-code",
    namespace_support="absent",
    **_DATASOURCE_LEGACY,
)
_USER_30_VOID = _user_recipe(
    create_result="entity",
    update_result="none",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_revoke="by-code",
    namespace_support="supported",
    **_DATASOURCE_LEGACY,
)
_USER_32_OPTIONAL_VOID = _user_recipe(
    create_result="optional",
    update_result="none",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_revoke="by-id",
    namespace_support="supported",
    **_DATASOURCE_CAMEL_AUTH,
)
_USER_32_OPTIONAL_ENTITY = _user_recipe(
    create_result="optional",
    update_result="entity",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_revoke="by-id",
    namespace_support="supported",
    **_DATASOURCE_CAMEL_AUTH,
)
_USER_342 = _user_recipe(
    create_result="optional",
    update_result="entity",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_revoke="by-id",
    namespace_support="supported",
    **_DATASOURCE_342,
)

_TOKEN_REQUIRED_VOID = AccessTokenRecipe(
    create_result="none",
    update_result="none",
    delete_result="none",
    legacy_form_routes=False,
    token_required=True,
)
_TOKEN_139 = AccessTokenRecipe(
    create_result="none",
    update_result="none",
    delete_result="none",
    legacy_form_routes=True,
    token_required=True,
)
_TOKEN_ENTITY_VOID_DELETE = AccessTokenRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="none",
    legacy_form_routes=False,
    token_required=False,
)
_TOKEN_ENTITY_BOOLEAN_DELETE = AccessTokenRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
    legacy_form_routes=False,
    token_required=False,
)


SECURITY_CONTRACTS: dict[str, SecurityVersionContract] = {
    "1.3.9": SecurityVersionContract("1.3.9", _USER_139, _TOKEN_139),
    "2.0.0": SecurityVersionContract("2.0.0", _USER_200, _TOKEN_REQUIRED_VOID),
    "2.0.1": SecurityVersionContract("2.0.1", _USER_200, _TOKEN_REQUIRED_VOID),
    "2.0.2": SecurityVersionContract(
        "2.0.2", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.3": SecurityVersionContract(
        "2.0.3", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.4": SecurityVersionContract(
        "2.0.4", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.5": SecurityVersionContract(
        "2.0.5", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.6": SecurityVersionContract(
        "2.0.6", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.7": SecurityVersionContract(
        "2.0.7", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.8": SecurityVersionContract(
        "2.0.8", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "2.0.9": SecurityVersionContract(
        "2.0.9", _USER_20_ENTITY, _TOKEN_ENTITY_VOID_DELETE
    ),
    "3.0.0": SecurityVersionContract("3.0.0", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.1": SecurityVersionContract("3.0.1", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.2": SecurityVersionContract("3.0.2", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.3": SecurityVersionContract("3.0.3", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.4": SecurityVersionContract("3.0.4", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.5": SecurityVersionContract("3.0.5", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.0.6": SecurityVersionContract("3.0.6", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.0": SecurityVersionContract("3.1.0", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.1": SecurityVersionContract("3.1.1", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.2": SecurityVersionContract("3.1.2", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.3": SecurityVersionContract("3.1.3", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.4": SecurityVersionContract("3.1.4", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.5": SecurityVersionContract("3.1.5", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.6": SecurityVersionContract("3.1.6", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.7": SecurityVersionContract("3.1.7", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.8": SecurityVersionContract("3.1.8", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.1.9": SecurityVersionContract("3.1.9", _USER_30_VOID, _TOKEN_ENTITY_VOID_DELETE),
    "3.2.0": SecurityVersionContract(
        "3.2.0", _USER_32_OPTIONAL_VOID, _TOKEN_ENTITY_VOID_DELETE
    ),
    "3.2.1": SecurityVersionContract(
        "3.2.1", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.2.2": SecurityVersionContract(
        "3.2.2", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.3.1": SecurityVersionContract(
        "3.3.1", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.3.2": SecurityVersionContract(
        "3.3.2", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.4.0": SecurityVersionContract(
        "3.4.0", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.4.1": SecurityVersionContract(
        "3.4.1", _USER_32_OPTIONAL_ENTITY, _TOKEN_ENTITY_BOOLEAN_DELETE
    ),
    "3.4.2": SecurityVersionContract("3.4.2", _USER_342, _TOKEN_ENTITY_BOOLEAN_DELETE),
    "3.4.3": SecurityVersionContract(
        "3.4.3",
        replace(_USER_342, simple_user_list=True),
        _TOKEN_ENTITY_BOOLEAN_DELETE,
    ),
}


def security_contract(version: str) -> SecurityVersionContract:
    """Return one reviewed contract, rejecting unreviewed version inference."""
    try:
        return SECURITY_CONTRACTS[version]
    except KeyError as exc:
        message = f"DS {version} has no reviewed security-domain contract"
        raise ValueError(message) from exc


def user_identity_read_operations(version: str) -> tuple[str, ...]:
    """Return the exact reads used by identity-only user consumers."""
    return (
        (
            "UsersController.getUserInfo",
            "UsersController.listAll",
            "UsersController.queryUserList",
        )
        if security_contract(version).user.simple_user_list
        else _USER_READ_OPERATIONS
    )


def semantic_operation_sources(version: str) -> dict[str, tuple[str, ...]]:
    """Return source operations needed by each stable security action."""
    contract = security_contract(version)
    recipe = contract.user
    tenant_page_operation = (
        "TenantController.queryTenantlistPaging"
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
            "3.2.0",
        }
        else "TenantController.queryTenantListPaging"
    )
    datasource_reads = (
        recipe.datasource_authorized_operation,
        recipe.datasource_unauthorized_operation,
    )
    user_reads = (
        (
            "UsersController.getUserInfo",
            "UsersController.queryUserList",
            "UsersController.listUser",
        )
        if recipe.simple_user_list
        else _USER_READ_OPERATIONS
    )
    identity_reads = user_identity_read_operations(version)
    operations: dict[str, tuple[str, ...]] = {
        "user.list": ("UsersController.queryUserList",),
        "user.get": user_reads,
        "user.create": (
            tenant_page_operation,
            *user_reads,
            "UsersController.createUser",
        ),
        "user.update": (
            *user_reads,
            tenant_page_operation,
            "UsersController.updateUser",
        ),
        "user.delete": (*identity_reads, "UsersController.delUserById"),
        "user.grant.project": (
            *identity_reads,
            *_PROJECT_PERMISSION_READS,
            "UsersController.grantProject",
        ),
        "user.grant.datasource": (
            *identity_reads,
            *datasource_reads,
            "UsersController.grantDataSource",
        ),
        "user.revoke.datasource": (
            *identity_reads,
            *datasource_reads,
            "UsersController.grantDataSource",
        ),
        "access-token.list": (_TOKEN_LIST_OPERATION,),
        "access-token.get": (_TOKEN_LIST_OPERATION,),
        "access-token.create": (
            *identity_reads,
            _TOKEN_LIST_OPERATION,
            "AccessTokenController.createToken",
            *(
                (_TOKEN_GENERATE_OPERATION,)
                if contract.access_token.token_required
                else ()
            ),
        ),
        "access-token.update": (
            *identity_reads,
            _TOKEN_LIST_OPERATION,
            "AccessTokenController.updateToken",
            *(
                (_TOKEN_GENERATE_OPERATION,)
                if contract.access_token.token_required
                else ()
            ),
        ),
        "access-token.delete": (
            _TOKEN_LIST_OPERATION,
            "AccessTokenController.delAccessTokenById",
        ),
        "access-token.generate": (
            *identity_reads,
            _TOKEN_GENERATE_OPERATION,
        ),
    }
    if recipe.project_revoke != "absent":
        revoke_operation = {
            "replace-by-id": "UsersController.grantProject",
            "by-code": "UsersController.revokeProject",
            "by-id": "UsersController.revokeProjectById",
        }[recipe.project_revoke]
        operations["user.revoke.project"] = (
            *identity_reads,
            *_PROJECT_PERMISSION_READS,
            revoke_operation,
        )
    if recipe.namespace_support != "absent":
        for action in ("user.grant.namespace", "user.revoke.namespace"):
            operations[action] = (
                *identity_reads,
                *_NAMESPACE_PERMISSION_READS,
                "UsersController.grantNamespace",
            )
    return operations


def semantic_operation_type_roots(version: str) -> dict[str, tuple[str, ...]]:
    """Return explicit model roots for the security runtime slice."""
    roots: dict[str, tuple[str, ...]] = {}
    for semantic_operation in semantic_operation_sources(version):
        if semantic_operation.startswith("access-token"):
            roots[semantic_operation] = (_TOKEN_MODEL, _USER_MODEL)
        elif semantic_operation.endswith("project"):
            roots[semantic_operation] = (_USER_MODEL, _PROJECT_MODEL)
        elif semantic_operation.endswith("datasource"):
            roots[semantic_operation] = (_USER_MODEL, _DATASOURCE_MODEL)
        elif semantic_operation.endswith("namespace"):
            roots[semantic_operation] = (_USER_MODEL, _NAMESPACE_MODEL)
        elif semantic_operation in {"user.create", "user.update"}:
            roots[semantic_operation] = (_USER_MODEL, _TENANT_MODEL)
        else:
            roots[semantic_operation] = (_USER_MODEL,)
    if security_contract(version).user.simple_user_list:
        for action, action_roots in roots.items():
            if (
                action.startswith("user.")
                and action
                not in {"user.list", "user.get", "user.create", "user.update"}
            ) or action in {
                "access-token.create",
                "access-token.update",
                "access-token.generate",
            }:
                roots[action] = (
                    *action_roots,
                    "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO",
                )
        for action in ("user.grant.datasource", "user.revoke.datasource"):
            roots[action] = (
                _USER_MODEL,
                "org.apache.dolphinscheduler.api.vo.UserSimpleInfoVO",
                "org.apache.dolphinscheduler.api.vo.DataSourceSimpleInfoVO",
            )
    return roots


def action_support(version: str) -> dict[str, Support]:
    """Return explicit supported/limited/absent decisions for stable actions."""
    recipe = security_contract(version).user
    supported: dict[str, Support] = dict.fromkeys(
        semantic_operation_sources(version),
        "supported",
    )
    if not recipe.has_state or not recipe.has_time_zone:
        supported["user.update"] = "limited"
    if not recipe.has_state:
        supported["user.create"] = "limited"
    if recipe.project_revoke == "absent":
        supported["user.revoke.project"] = "absent"
    if recipe.namespace_support == "absent":
        supported["user.grant.namespace"] = "absent"
        supported["user.revoke.namespace"] = "absent"
    return supported


SECURITY_EVIDENCE = (
    Evidence(
        source="dolphinscheduler-api/.../controller/UsersController.java",
        symbol="queryUserList/createUser/updateUser/delUserById",
        conclusion="The main user CRUD routes exist in every reviewed version.",
    ),
    Evidence(
        source="dolphinscheduler-dao/.../entity/User.java",
        symbol="state/timeZone",
        conclusion=("1.3.9 has neither field; 2.0.x has state only; 3.0.0+ has both."),
    ),
    Evidence(
        source="dolphinscheduler-api/.../service/impl/UsersServiceImpl.java",
        symbol="grantProject/revokeProject/revokeProjectById",
        conclusion=(
            "1.3.9 grantProject replaces the full set; 2.0.0-2.0.1 cannot safely "
            "revoke one project; later releases have a direct revoke route."
        ),
    ),
    Evidence(
        source="dolphinscheduler-ui/.../user-manage/components/use-authorize.ts",
        symbol="grantProject/revokeProjectById/grantDataSource/grantNamespace",
        conclusion="The retained UI uses the main permission routes, not V2.",
    ),
    Evidence(
        source="dolphinscheduler-api/.../controller/AccessTokenController.java",
        symbol="query/create/update/delete/generate",
        conclusion="All six stable token actions have a main route in all versions.",
    ),
    Evidence(
        source="dolphinscheduler-ui/.../service/modules/token/index.ts",
        symbol="access-tokens",
        conclusion=(
            "The UI keeps using the main access-token routes while V2 exists "
            "only in 3.1.0 through 3.4.1 and is removed in 3.4.2."
        ),
    ),
    Evidence(
        source="dolphinscheduler-ui/.../token/_source/createToken.vue",
        symbol="generateToken then createToken",
        conclusion=(
            "1.3.9 and 2.0.0-2.0.1 require an explicit token; the UI generates one "
            "before create/update when the caller does not provide it."
        ),
    ),
)


__all__ = [
    "SECURITY_CONTRACTS",
    "SECURITY_EVIDENCE",
    "TARGET_SECURITY_VERSIONS",
    "AccessTokenRecipe",
    "SecurityVersionContract",
    "UserRecipe",
    "action_support",
    "security_contract",
    "semantic_operation_sources",
    "semantic_operation_type_roots",
    "user_identity_read_operations",
]
