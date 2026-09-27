from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Generic, Literal, Protocol, TypedDict, TypeVar, cast

from dsctl.errors import (
    ApiResultError,
    ApiTransportError,
    NotFoundError,
    ResolutionError,
    UnsupportedFeatureError,
    UserInputError,
)
from dsctl.upstream._resolver_models import resolved_user
from dsctl.upstream.bound_domain import BoundDomain
from dsctl.upstream.compiled_domain import (
    MUTATION_ONCE_REQUIRED,
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
    CompiledProgramExpectation,
)
from dsctl.upstream.mutation_outcomes import mutation_call, verify_mutation
from dsctl.upstream.pagination import collect_pages
from dsctl.upstream.resolver import tenant as resolve_tenant
from dsctl.upstream.resolver import user as resolve_user
from dsctl.upstream.response_projection import (
    non_empty_text,
    optional_int_field,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.tenants import TenantAdapter, TenantDomain
from dsctl.upstream.wire import (
    WireContractError,
    WireExecutionMode,
    WireResultEnvelope,
)

if TYPE_CHECKING:
    import builtins
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        StringEnumValue,
        TenantOperations,
        UserListRecord,
        UserOperations,
        UserPageRecord,
        UserReadOperations,
        UserRecord,
    )
    from dsctl.upstream.resolver import ResolvedUser
    from dsctl.upstream.wire import CompiledWireProfile


_USER_RESOURCE = "user"
_PROJECT_RESOURCE = "project"
_DATASOURCE_RESOURCE = "datasource"
_NAMESPACE_RESOURCE = "namespace"
_USER_NOT_EXIST = 10010

_EntityResult = Literal["none", "entity", "optional"]
_ProjectIdentity = Literal["id", "code"]
_ProjectGrant = Literal["replace", "insert", "upsert"]
_ProjectRevoke = Literal["replace-by-id", "absent", "by-code", "by-id"]
_UserPrimitive = Literal[
    "page",
    "list",
    "all",
    "current",
    "create",
    "update",
    "delete",
    "project_authorized",
    "project_unauthorized",
    "project_grant",
    "project_revoke",
    "datasource_authorized",
    "datasource_unauthorized",
    "datasource_grant",
    "namespace_authorized",
    "namespace_unauthorized",
    "namespace_grant",
]
_NAMESPACE_ABSENT_VERSIONS = frozenset(
    {
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
    }
)
_USER_PROGRAMS = CompiledDomainPrograms[_UserPrimitive](
    name="user",
    schema_constant="COMPILED_USER_SCHEMA_VERSION",
    schema_version=1,
    expectations={
        "page": READ_RETRY_OPTIONAL,
        "list": READ_RETRY_OPTIONAL,
        "all": READ_RETRY_OPTIONAL,
        "current": READ_RETRY_OPTIONAL,
        "create": MUTATION_ONCE_REQUIRED,
        "update": MUTATION_ONCE_REQUIRED,
        "delete": MUTATION_ONCE_REQUIRED,
        "project_authorized": READ_RETRY_OPTIONAL,
        "project_unauthorized": READ_RETRY_OPTIONAL,
        "project_grant": MUTATION_ONCE_REQUIRED,
        "project_revoke": CompiledProgramExpectation(
            mode=WireExecutionMode.MUTATION_ONCE,
            envelope=WireResultEnvelope.REQUIRED,
            absent_versions=frozenset({"1.3.9", "2.0.0", "2.0.1"}),
        ),
        "datasource_authorized": READ_RETRY_OPTIONAL,
        "datasource_unauthorized": READ_RETRY_OPTIONAL,
        "datasource_grant": MUTATION_ONCE_REQUIRED,
        "namespace_authorized": CompiledProgramExpectation(
            mode=WireExecutionMode.READ_RETRY_SAFE,
            envelope=WireResultEnvelope.OPTIONAL,
            absent_versions=_NAMESPACE_ABSENT_VERSIONS,
        ),
        "namespace_unauthorized": CompiledProgramExpectation(
            mode=WireExecutionMode.READ_RETRY_SAFE,
            envelope=WireResultEnvelope.OPTIONAL,
            absent_versions=_NAMESPACE_ABSENT_VERSIONS,
        ),
        "namespace_grant": CompiledProgramExpectation(
            mode=WireExecutionMode.MUTATION_ONCE,
            envelope=WireResultEnvelope.REQUIRED,
            absent_versions=_NAMESPACE_ABSENT_VERSIONS,
        ),
    },
)


@dataclass(frozen=True)
class UserSnapshot:
    """Version-neutral user projection consumed by the stable service layer."""

    id: int
    userName: str  # noqa: N815
    email: str | None
    phone: str | None
    userType: StringEnumValue | None  # noqa: N815
    tenantId: int  # noqa: N815
    tenantCode: str | None  # noqa: N815
    queueName: str | None  # noqa: N815
    queue: str | None
    state: int
    timeZone: str | None  # noqa: N815
    storedQueue: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class PermissionProject:
    """Project identity as exposed by the exact permission API."""

    id: int
    code: int | None
    name: str
    description: str | None

    def to_data(self) -> JsonObject:
        """Emit the native numeric identity without inventing a 1.3 code."""
        identity = {"id": self.id} if self.code is None else {"code": self.code}
        return {
            **identity,
            "name": self.name,
            "description": self.description,
        }


@dataclass(frozen=True)
class PermissionDataSource:
    """Datasource identity used by grant-set recipes."""

    id: int
    name: str
    note: str | None
    type: StringEnumValue | None

    def to_data(self) -> JsonObject:
        """Return the stable CLI datasource projection."""
        return {
            "id": self.id,
            "name": self.name,
            "note": self.note,
            "type": enum_value(self.type),
        }


@dataclass(frozen=True)
class PermissionNamespace:
    """Namespace identity used by grant-set recipes."""

    id: int
    namespace: str
    clusterCode: int | None  # noqa: N815
    clusterName: str | None  # noqa: N815

    def to_data(self) -> JsonObject:
        """Return the stable CLI namespace projection."""
        return {
            "id": self.id,
            "namespace": self.namespace,
            "clusterCode": self.clusterCode,
            "clusterName": self.clusterName,
        }


class _Identified(Protocol):
    @property
    def id(self) -> int: ...


PermissionT = TypeVar("PermissionT", bound=_Identified)


@dataclass(frozen=True)
class PermissionSetChange(Generic[PermissionT]):
    """One resolved permission request and its verified final set."""

    user: ResolvedUser | UserIdentity
    requested: tuple[PermissionT, ...]
    final: tuple[PermissionT, ...]


class _UserPermissionOperations(Protocol):
    """Permission port consumed by the caller-oriented user domain."""

    def grant_project(self, *, user_id: int, selector: str) -> PermissionProject:
        """Grant one resolved project."""

    def revoke_project(self, *, user_id: int, selector: str) -> PermissionProject:
        """Revoke one resolved project."""

    def change_datasources(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionDataSource], list[PermissionDataSource]]:
        """Apply one logical datasource-set change."""

    def change_namespaces(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionNamespace], list[PermissionNamespace]]:
        """Apply one logical namespace-set change."""


@dataclass(frozen=True)
class ProjectPermissionChange:
    """One resolved single-project permission result."""

    user: ResolvedUser | UserIdentity
    project: PermissionProject


@dataclass(frozen=True)
class UserSelection:
    """Resolved user identity plus its exact refreshed payload."""

    resolved: ResolvedUser
    record: UserRecord


@dataclass(frozen=True)
class UserDeletion:
    """Verified user deletion outcome."""

    resolved: ResolvedUser | UserIdentity
    deleted: bool


@dataclass(frozen=True)
class _UserRecipe:
    create_result: _EntityResult
    update_result: _EntityResult
    has_state: bool
    has_time_zone: bool
    project_identity: _ProjectIdentity
    project_grant: _ProjectGrant
    project_revoke: _ProjectRevoke
    has_namespace_permissions: bool
    simple_user_list: bool = False


_RECIPE_139 = _UserRecipe(
    create_result="none",
    update_result="none",
    has_state=False,
    has_time_zone=False,
    project_identity="id",
    project_grant="replace",
    project_revoke="replace-by-id",
    has_namespace_permissions=False,
)
_RECIPE_200 = _UserRecipe(
    create_result="none",
    update_result="none",
    has_state=True,
    has_time_zone=False,
    project_identity="code",
    project_grant="insert",
    project_revoke="absent",
    has_namespace_permissions=False,
)
_RECIPE_20_ENTITY = _UserRecipe(
    create_result="entity",
    update_result="none",
    has_state=True,
    has_time_zone=False,
    project_identity="code",
    project_grant="insert",
    project_revoke="by-code",
    has_namespace_permissions=False,
)
_RECIPE_30 = _UserRecipe(
    create_result="entity",
    update_result="none",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_grant="replace",
    project_revoke="by-code",
    has_namespace_permissions=True,
)
_RECIPE_320 = _UserRecipe(
    create_result="optional",
    update_result="none",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_grant="upsert",
    project_revoke="by-id",
    has_namespace_permissions=True,
)
_RECIPE_321 = _UserRecipe(
    create_result="optional",
    update_result="entity",
    has_state=True,
    has_time_zone=True,
    project_identity="code",
    project_grant="upsert",
    project_revoke="by-id",
    has_namespace_permissions=True,
)
_USER_RECIPES = {
    "legacy_139": _RECIPE_139,
    "legacy_200": _RECIPE_200,
    "state_entity": _RECIPE_20_ENTITY,
    "timezone_entity": _RECIPE_30,
    "optional_void": _RECIPE_320,
    "optional_entity": _RECIPE_321,
    "optional_entity_simple_identity": replace(_RECIPE_321, simple_user_list=True),
}


@dataclass(frozen=True)
class UserDomain:
    """Caller-oriented user CRUD and permission recipes for one exact profile."""

    ds_version: str
    users: UserOperations
    tenants: TenantOperations
    permissions: _UserPermissionOperations
    recipe: _UserRecipe
    identity_users: UserIdentityLookup | None = None

    def _resolve_user(self, selector: str) -> ResolvedUser | UserIdentity:
        if self.identity_users is not None:
            return self.identity_users.resolve(selector)
        return resolve_user(selector, adapter=self.users)

    def get(self, selector: str) -> UserSelection:
        """Resolve one user selector and return its refreshed payload."""
        if self.identity_users is not None:
            current = self.identity_users.users.current()
            if _matches_user(selector, current.id, current.userName):
                return UserSelection(resolved=resolved_user(current), record=current)
        resolved = resolve_user(selector, adapter=self.users)
        return UserSelection(
            resolved=resolved,
            record=self.users.get(user_id=resolved.id),
        )

    def create(
        self,
        *,
        user_name: str,
        password: str,
        email: str,
        tenant: str,
        state: int,
        phone: str | None,
        queue: str | None,
    ) -> UserRecord:
        """Resolve the tenant and create one verified user."""
        self._guard_state(state, action="user.create")
        resolved_tenant = resolve_tenant(tenant, adapter=self.tenants)
        return self.users.create(
            user_name=user_name,
            password=password,
            email=email,
            tenant_id=resolved_tenant.id,
            phone=phone,
            queue=queue,
            state=state,
        )

    def update(
        self,
        selector: str,
        *,
        user_name: str | None,
        password: str | None,
        email: str | None,
        tenant: str | None,
        state: int | None,
        phone: str | None,
        preserve_phone: bool,
        queue: str | None,
        preserve_queue: bool,
        time_zone: str | None,
    ) -> UserSelection:
        """Preserve omitted fields and update one resolved user."""
        if state is not None:
            self._guard_state(state, action="user.update")
        if time_zone is not None and not self.recipe.has_time_zone:
            raise _unsupported_user_facet(
                self.ds_version,
                action="user.update",
                facet="time-zone",
                introduced_in="3.0.0",
            )

        selected = self.get(selector)
        current = selected.record
        next_user_name = current.userName if user_name is None else user_name
        next_email = current.email if email is None else email
        next_tenant_id = current.tenantId
        if tenant is not None:
            next_tenant_id = resolve_tenant(tenant, adapter=self.tenants).id
        next_phone = current.phone if preserve_phone else phone
        next_queue = (
            current.storedQueue if preserve_queue else "" if queue is None else queue
        )
        next_state = current.state if state is None else state
        next_time_zone = current.timeZone if time_zone is None else time_zone
        if next_user_name is None or next_email is None:
            message = "User payload was missing required fields"
            raise ApiTransportError(
                message,
                details={"resource": _USER_RESOURCE, "id": selected.resolved.id},
            )
        if (
            password is None
            and next_user_name == current.userName
            and next_email == current.email
            and next_tenant_id == current.tenantId
            and next_phone == current.phone
            and next_queue == current.storedQueue
            and next_state == current.state
            and next_time_zone == current.timeZone
        ):
            message = "User update requires at least one field change"
            raise UserInputError(
                message,
                suggestion=(
                    "Pass a different --user-name, --password, --email, --tenant, "
                    "--state, --phone, --clear-phone, --queue, --clear-queue, or "
                    "--time-zone value."
                ),
            )
        updated = self.users.update(
            user_id=selected.resolved.id,
            user_name=next_user_name,
            password="" if password is None else password,
            email=next_email,
            tenant_id=next_tenant_id,
            phone=next_phone,
            queue=next_queue or "",
            state=next_state,
            time_zone=next_time_zone,
        )
        return UserSelection(resolved=selected.resolved, record=updated)

    def delete(self, selector: str) -> UserDeletion:
        """Delete one resolved user and return the verified outcome."""
        resolved = self._resolve_user(selector)
        return UserDeletion(
            resolved=resolved,
            deleted=self.users.delete(user_id=resolved.id),
        )

    def grant_project(self, user: str, project: str) -> ProjectPermissionChange:
        """Grant one resolved project to one resolved user."""
        resolved = self._resolve_user(user)
        selected = self.permissions.grant_project(
            user_id=resolved.id,
            selector=project,
        )
        return ProjectPermissionChange(user=resolved, project=selected)

    def revoke_project(self, user: str, project: str) -> ProjectPermissionChange:
        """Revoke one resolved project when the exact profile supports it."""
        if self.recipe.project_revoke == "absent":
            message = (
                "user.revoke.project is unavailable on DolphinScheduler "
                f"{self.ds_version}."
            )
            raise UnsupportedFeatureError(
                message,
                details={
                    "action": "user.revoke.project",
                    "selected_version": self.ds_version,
                    "reason": "upstream_capability_absent",
                },
                suggestion=(
                    "Use DolphinScheduler 2.0.9 or newer, or manage the complete "
                    "project grant set in the DolphinScheduler 2.0.0 UI."
                ),
            )
        resolved = self._resolve_user(user)
        selected = self.permissions.revoke_project(
            user_id=resolved.id,
            selector=project,
        )
        return ProjectPermissionChange(user=resolved, project=selected)

    def change_datasources(
        self,
        user: str,
        selectors: Sequence[str],
        *,
        grant: bool,
    ) -> PermissionSetChange[PermissionDataSource]:
        """Grant or revoke a resolved datasource set with full readback."""
        resolved = self._resolve_user(user)
        requested, final = self.permissions.change_datasources(
            user_id=resolved.id,
            selectors=selectors,
            grant=grant,
        )
        return PermissionSetChange(
            user=resolved,
            requested=tuple(requested),
            final=tuple(final),
        )

    def change_namespaces(
        self,
        user: str,
        selectors: Sequence[str],
        *,
        grant: bool,
    ) -> PermissionSetChange[PermissionNamespace]:
        """Grant or revoke a namespace set when the profile supports it."""
        if not self.recipe.has_namespace_permissions:
            action = "user.grant.namespace" if grant else "user.revoke.namespace"
            message = f"{action} is unavailable on DolphinScheduler {self.ds_version}."
            raise UnsupportedFeatureError(
                message,
                details={
                    "action": action,
                    "selected_version": self.ds_version,
                    "reason": "upstream_capability_absent",
                    "introduced_in": "3.0.0",
                },
                suggestion=(
                    "Namespace permissions require DolphinScheduler 3.0.0 or newer."
                ),
            )
        resolved = self._resolve_user(user)
        requested, final = self.permissions.change_namespaces(
            user_id=resolved.id,
            selectors=selectors,
            grant=grant,
        )
        return PermissionSetChange(
            user=resolved,
            requested=tuple(requested),
            final=tuple(final),
        )

    def _guard_state(self, state: int, *, action: str) -> None:
        if not self.recipe.has_state and state != 1:
            raise _unsupported_user_facet(
                self.ds_version,
                action=action,
                facet="state=0",
                introduced_in="2.0.0",
            )


class UserAdapter:
    """Compiled user and permission adapter for every reviewed profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact compiled profile and its reviewed user recipe."""
        self._profile = _USER_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> UserAdapter:
        """Return the adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> UserDomain:
        """Bind user, tenant, and permission ports over one shared client."""
        programs = _USER_PROGRAMS.bind(self._profile, profile, http_client=http_client)
        tenant_domain: TenantDomain = TenantAdapter.for_version(self.ds_version).bind(
            profile, http_client=http_client
        )
        return UserDomain(
            ds_version=self.ds_version,
            users=cast(
                "UserOperations", _CompiledUserOperations(programs, self._recipe)
            ),
            tenants=tenant_domain.tenants,
            permissions=_CompiledUserPermissions(programs, self._recipe),
            recipe=self._recipe,
            identity_users=UserIdentityLookup(
                _CompiledUserLookupOperations(programs, self._recipe)
            )
            if self._recipe.simple_user_list
            else None,
        )


USER_DOMAIN = BoundDomain[UserDomain](
    name=_USER_RESOURCE,
    adapter_for_version=UserAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledUserLookupOperations:
    programs: BoundCompiledPrograms[_UserPrimitive]
    recipe: _UserRecipe

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def current(self) -> UserRecord:
        return cast(
            "UserRecord",
            _user_snapshot(
                self.programs.call("current", {}),
                recipe=self.recipe,
                ds_version=self.ds_version,
                raw=True,
            ),
        )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> UserPageRecord:
        page = self.programs.call(
            "page", {"searchVal": search, "pageNo": page_no, "pageSize": page_size}
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
        )
        projected = [
            _user_snapshot(
                item,
                recipe=self.recipe,
                ds_version=self.ds_version,
                raw=self.recipe.simple_user_list,
            )
            for item in items
        ]
        return cast(
            "UserPageRecord",
            project_page(
                page,
                projected,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_USER_RESOURCE,
            ),
        )

    def list_all(self) -> Sequence[UserRecord]:
        raw = self._raw_users()
        summaries = collect_pages(self.list, resource=_USER_RESOURCE)
        return cast(
            "Sequence[UserRecord]",
            _merge_user_collections(raw, summaries),
        )

    def get(self, *, user_id: int) -> UserRecord:
        if self.recipe.simple_user_list:
            current = self.current()
            if current.id == user_id:
                return current
        raw = self._raw_users()
        raw_match = next((item for item in raw if item.id == user_id), None)
        search = None if raw_match is None else raw_match.userName
        summaries = collect_pages(
            lambda page_no, page_size: self.list(
                page_no=page_no,
                page_size=page_size,
                search=search,
            ),
            resource=_USER_RESOURCE,
        )
        summary_match = next(
            (item for item in summaries if item.id == user_id),
            None,
        )
        if raw_match is None and summary_match is None:
            raise ApiResultError(
                result_code=_USER_NOT_EXIST,
                result_message=f"user {user_id} does not exist",
            )
        return cast(
            "UserRecord",
            _merge_user_snapshot(raw=raw_match, summary=summary_match),
        )

    def _raw_users(self) -> builtins.list[UserSnapshot]:
        payloads = [
            *cast("Sequence[object]", self.programs.call("list", {})),
            *(
                cast("Sequence[object]", self.programs.call("all", {}))
                if not self.recipe.simple_user_list
                else ()
            ),
        ]
        seen: set[int] = set()
        result: builtins.list[UserSnapshot] = []
        for payload in payloads:
            snapshot = _user_snapshot(
                payload,
                recipe=self.recipe,
                ds_version=self.ds_version,
                raw=True,
            )
            if snapshot.id in seen:
                continue
            seen.add(snapshot.id)
            result.append(snapshot)
        return result


@dataclass(frozen=True)
class _CompiledUserOperations(_CompiledUserLookupOperations):
    def create(
        self,
        *,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None = None,
        queue: str | None = None,
        state: int,
    ) -> UserRecord:
        values: JsonObject = {
            "userName": user_name,
            "userPassword": password,
            "email": email,
            "tenantId": tenant_id,
            "phone": phone,
            "queue": queue,
        }
        if self.recipe.has_state:
            values["state"] = state
        result = mutation_call(
            lambda: self.programs.call("create", values),
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="create",
        )

        def verify() -> UserRecord:
            _require_entity_result(
                result,
                expected=self.recipe.create_result,
                operation="create",
                ds_version=self.ds_version,
            )
            matches = [item for item in self.list_all() if item.userName == user_name]
            if len(matches) != 1:
                message = "User create readback did not return one exact match"
                raise ApiTransportError(
                    message,
                    details={"userName": user_name, "match_count": len(matches)},
                )
            created = matches[0]
            expected_queue = "" if queue is None else queue
            if (
                created.email != email
                or created.tenantId != tenant_id
                or created.phone != phone
                or (created.storedQueue or "") != expected_queue
                or created.state != state
            ):
                message = "User create readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={"userName": user_name},
                )
            return created

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="create",
        )

    def update(
        self,
        *,
        user_id: int,
        user_name: str,
        password: str,
        email: str,
        tenant_id: int,
        phone: str | None,
        queue: str,
        state: int,
        time_zone: str | None = None,
    ) -> UserRecord:
        values: JsonObject = {
            "id": user_id,
            "userName": user_name,
            "userPassword": password,
            "email": email,
            "tenantId": tenant_id,
            "phone": phone,
            "queue": queue,
        }
        if self.recipe.has_state:
            values["state"] = state
        if self.recipe.has_time_zone:
            values["timeZone"] = time_zone
        result = mutation_call(
            lambda: self.programs.call("update", values),
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="update",
        )

        def verify() -> UserRecord:
            _require_entity_result(
                result,
                expected=self.recipe.update_result,
                operation="update",
                ds_version=self.ds_version,
            )
            updated = self.get(user_id=user_id)
            if (
                updated.userName != user_name
                or updated.email != email
                or updated.tenantId != tenant_id
                or updated.phone != phone
                or (updated.storedQueue or "") != queue
                or updated.state != state
                or updated.timeZone != time_zone
            ):
                message = "User update readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={"id": user_id, "userName": user_name},
                )
            return updated

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="update",
        )

    def delete(self, *, user_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call("delete", {"id": user_id}),
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="delete",
        )

        def verify() -> bool:
            if result is not None:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource=_USER_RESOURCE,
                    field="deleteResult",
                    reason="void user delete returned a non-null payload",
                )
            if any(item.id == user_id for item in self.list_all()):
                message = "User delete readback still returned the deleted id"
                raise ApiTransportError(
                    message,
                    details={"id": user_id},
                )
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource=_USER_RESOURCE,
            operation="delete",
        )


@dataclass(frozen=True)
class _CompiledUserPermissions:
    programs: BoundCompiledPrograms[_UserPrimitive]
    recipe: _UserRecipe

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def grant_project(self, *, user_id: int, selector: str) -> PermissionProject:
        authorized, all_projects = self._project_sets(user_id=user_id)
        selected = _resolve_project_selector(
            selector,
            candidates=all_projects,
            identity=self.recipe.project_identity,
        )
        # Authorized-project reads expose membership, not the relation's perm.
        # Legacy replace/insert REST surfaces create only perm=7 relations.
        # Upsert versions also support read-only grants and must always write.
        if self.recipe.project_grant != "upsert" and selected.id in {
            item.id for item in authorized
        }:
            return selected
        ids = (
            sorted({item.id for item in authorized} | {selected.id})
            if self.recipe.project_grant == "replace"
            else [selected.id]
        )
        mutation_call(
            lambda: self.programs.call(
                "project_grant", {"userId": user_id, "projectIds": _ids(ids)}
            ),
            ds_version=self.ds_version,
            resource=_PROJECT_RESOURCE,
            operation="grant",
        )
        verify_mutation(
            lambda: (
                self._verify_project_set(user_id=user_id, expected_ids=set(ids))
                if self.recipe.project_grant == "replace"
                else self._verify_project_membership(
                    user_id=user_id,
                    selected=selected,
                    expected=True,
                )
            ),
            ds_version=self.ds_version,
            resource=_PROJECT_RESOURCE,
            operation="grant",
        )
        return selected

    def revoke_project(self, *, user_id: int, selector: str) -> PermissionProject:
        authorized, all_projects = self._project_sets(user_id=user_id)
        selected = _resolve_project_selector(
            selector,
            candidates=all_projects,
            identity=self.recipe.project_identity,
        )
        if selected.id not in {item.id for item in authorized}:
            return selected
        recipe = self.recipe.project_revoke
        primitive: _UserPrimitive
        values: JsonObject
        if recipe == "replace-by-id":
            values = {
                "userId": user_id,
                "projectIds": _ids(
                    item.id for item in authorized if item.id != selected.id
                ),
            }
            primitive = "project_grant"
        elif recipe == "by-code":
            if selected.code is None:
                message = "Project code revoke binding is incomplete"
                raise WireContractError(message)
            values = {"userId": user_id, "projectCode": selected.code}
            primitive = "project_revoke"
        elif recipe == "by-id":
            values = {"userId": user_id, "projectIds": str(selected.id)}
            primitive = "project_revoke"
        else:  # guarded before any request by UserDomain.revoke_project
            message = "Absent project revoke reached the wire recipe"
            raise WireContractError(message)
        mutation_call(
            lambda: self.programs.call(primitive, values),
            ds_version=self.ds_version,
            resource=_PROJECT_RESOURCE,
            operation="revoke",
        )
        verify_mutation(
            lambda: (
                self._verify_project_set(
                    user_id=user_id,
                    expected_ids={item.id for item in authorized} - {selected.id},
                )
                if recipe == "replace-by-id"
                else self._verify_project_membership(
                    user_id=user_id,
                    selected=selected,
                    expected=False,
                )
            ),
            ds_version=self.ds_version,
            resource=_PROJECT_RESOURCE,
            operation="revoke",
        )
        return selected

    def change_datasources(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionDataSource], list[PermissionDataSource]]:
        authorized = self._datasources(user_id=user_id, authorized=True)
        unauthorized = self._datasources(user_id=user_id, authorized=False)
        candidates = _dedupe_by_id([*authorized, *unauthorized])
        requested = _resolve_many(
            selectors,
            candidates=candidates,
            name=lambda item: item.name,
            resource=_DATASOURCE_RESOURCE,
        )
        current = {item.id: item for item in authorized}
        if grant:
            current.update({item.id: item for item in requested})
            operation = "grant"
        else:
            for item in requested:
                current.pop(item.id, None)
            operation = "revoke"
        final: list[PermissionDataSource] = _sorted_by_name_and_id(
            current.values(),
            name=lambda item: item.name,
        )
        if {item.id for item in authorized} != set(current):
            mutation_call(
                lambda: self.programs.call(
                    "datasource_grant",
                    {"userId": user_id, "datasourceIds": _ids(current)},
                ),
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                operation=operation,
            )
            verify_mutation(
                lambda: self._verify_datasource_set(
                    user_id=user_id,
                    expected_ids=set(current),
                ),
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
                operation=operation,
            )
        return requested, final

    def change_namespaces(
        self,
        *,
        user_id: int,
        selectors: Sequence[str],
        grant: bool,
    ) -> tuple[list[PermissionNamespace], list[PermissionNamespace]]:
        authorized = self._namespaces(user_id=user_id, authorized=True)
        unauthorized = self._namespaces(user_id=user_id, authorized=False)
        candidates = _dedupe_by_id([*authorized, *unauthorized])
        requested = _resolve_many(
            selectors,
            candidates=candidates,
            name=lambda item: item.namespace,
            resource=_NAMESPACE_RESOURCE,
        )
        current = {item.id: item for item in authorized}
        if grant:
            current.update({item.id: item for item in requested})
            operation = "grant"
        else:
            for item in requested:
                current.pop(item.id, None)
            operation = "revoke"
        final: list[PermissionNamespace] = _sorted_by_name_and_id(
            current.values(),
            name=lambda item: item.namespace,
        )
        if {item.id for item in authorized} != set(current):
            mutation_call(
                lambda: self.programs.call(
                    "namespace_grant",
                    {"userId": user_id, "namespaceIds": _ids(current)},
                ),
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                operation=operation,
            )
            verify_mutation(
                lambda: self._verify_namespace_set(
                    user_id=user_id,
                    expected_ids=set(current),
                ),
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
                operation=operation,
            )
        return requested, final

    def _project_sets(
        self,
        *,
        user_id: int,
    ) -> tuple[list[PermissionProject], list[PermissionProject]]:
        authorized = self._projects(user_id=user_id, authorized=True)
        unauthorized = self._projects(user_id=user_id, authorized=False)
        return authorized, _dedupe_by_id([*authorized, *unauthorized])

    def _projects(
        self,
        *,
        user_id: int,
        authorized: bool,
    ) -> list[PermissionProject]:
        payload = self.programs.call(
            "project_authorized" if authorized else "project_unauthorized",
            {"userId": user_id},
        )
        return [
            _permission_project(
                item,
                ds_version=self.ds_version,
                identity=self.recipe.project_identity,
            )
            for item in _object_sequence(
                payload,
                ds_version=self.ds_version,
                resource=_PROJECT_RESOURCE,
            )
        ]

    def _datasources(
        self,
        *,
        user_id: int,
        authorized: bool,
    ) -> list[PermissionDataSource]:
        payload = self.programs.call(
            "datasource_authorized" if authorized else "datasource_unauthorized",
            {"userId": user_id},
        )
        return [
            _permission_datasource(item, ds_version=self.ds_version)
            for item in _object_sequence(
                payload,
                ds_version=self.ds_version,
                resource=_DATASOURCE_RESOURCE,
            )
        ]

    def _namespaces(
        self,
        *,
        user_id: int,
        authorized: bool,
    ) -> list[PermissionNamespace]:
        payload = self.programs.call(
            "namespace_authorized" if authorized else "namespace_unauthorized",
            {"userId": user_id},
        )
        return [
            _permission_namespace(item, ds_version=self.ds_version)
            for item in _object_sequence(
                payload,
                ds_version=self.ds_version,
                resource=_NAMESPACE_RESOURCE,
            )
        ]

    def _verify_project_set(self, *, user_id: int, expected_ids: set[int]) -> bool:
        actual_ids = {
            item.id for item in self._projects(user_id=user_id, authorized=True)
        }
        if actual_ids != expected_ids:
            message = "Project permission readback did not match the requested set"
            raise ApiTransportError(
                message,
                details={
                    "ds_version": self.ds_version,
                    "user_id": user_id,
                    "expected_ids": sorted(expected_ids),
                    "actual_ids": sorted(actual_ids),
                },
            )
        return True

    def _verify_project_membership(
        self,
        *,
        user_id: int,
        selected: PermissionProject,
        expected: bool,
    ) -> bool:
        actual = selected.id in {
            item.id for item in self._projects(user_id=user_id, authorized=True)
        }
        if actual != expected:
            message = "Project permission readback did not match the requested state"
            raise ApiTransportError(
                message,
                details={
                    "user_id": user_id,
                    "project_id": selected.id,
                    "expected_authorized": expected,
                },
            )
        return True

    def _verify_datasource_set(self, *, user_id: int, expected_ids: set[int]) -> bool:
        actual_ids = {
            item.id for item in self._datasources(user_id=user_id, authorized=True)
        }
        if actual_ids != expected_ids:
            message = "Datasource permission readback did not match the requested set"
            raise ApiTransportError(
                message,
                details={
                    "user_id": user_id,
                    "expected_ids": sorted(expected_ids),
                    "actual_ids": sorted(actual_ids),
                },
            )
        return True

    def _verify_namespace_set(self, *, user_id: int, expected_ids: set[int]) -> bool:
        actual_ids = {
            item.id for item in self._namespaces(user_id=user_id, authorized=True)
        }
        if actual_ids != expected_ids:
            message = "Namespace permission readback did not match the requested set"
            raise ApiTransportError(
                message,
                details={
                    "user_id": user_id,
                    "expected_ids": sorted(expected_ids),
                    "actual_ids": sorted(actual_ids),
                },
            )
        return True


def bind_user_lookup(
    profile: ClusterProfile,
    *,
    http_client: DolphinSchedulerClient,
) -> UserReadOperations:
    """Bind exact user reads without permissions or tenant composition."""
    compiled_profile = _USER_PROGRAMS.profile(profile.ds_version)
    recipe = _recipe_for_profile(compiled_profile)
    programs = _USER_PROGRAMS.bind(compiled_profile, profile, http_client=http_client)
    return cast(
        "UserReadOperations",
        _CompiledUserLookupOperations(programs, recipe),
    )


class UserIdentityData(TypedDict):
    """Exact public identity fields supplied by a native user summary."""

    id: int
    userName: str


@dataclass(frozen=True)
class UserIdentity:
    """An identity-only response without invented user management fields."""

    id: int
    userName: str  # noqa: N815

    def to_data(self) -> UserIdentityData:
        """Return exactly the identity supplied by the selected API."""
        return {"id": self.id, "userName": self.userName}


@dataclass(frozen=True)
class UserIdentityLookup:
    """Resolve self without administrator lists, and others via the native VO."""

    users: _CompiledUserLookupOperations

    def resolve(self, selector: str) -> UserIdentity:
        """Resolve an exact name or id using identity-only native responses."""
        normalized = _normalize_selector(selector, resource="User")
        current = self.users.current()
        if _matches_user(normalized, current.id, current.userName):
            return UserIdentity(
                positive_int(
                    current.id,
                    ds_version=self.users.ds_version,
                    resource=_USER_RESOURCE,
                    field="id",
                ),
                non_empty_text(
                    current.userName,
                    ds_version=self.users.ds_version,
                    resource=_USER_RESOURCE,
                    field="userName",
                ),
            )
        candidates = [
            UserIdentity(
                positive_int(
                    response_field(
                        item,
                        "id",
                        ds_version=self.users.ds_version,
                        resource=_USER_RESOURCE,
                    ),
                    ds_version=self.users.ds_version,
                    resource=_USER_RESOURCE,
                    field="id",
                ),
                non_empty_text(
                    response_field(
                        item,
                        "userName",
                        ds_version=self.users.ds_version,
                        resource=_USER_RESOURCE,
                    ),
                    ds_version=self.users.ds_version,
                    resource=_USER_RESOURCE,
                    field="userName",
                ),
            )
            for item in cast("Sequence[object]", self.users.programs.call("all", {}))
        ]
        matches = {
            item.id: item
            for item in candidates
            if _matches_user(normalized, item.id, item.userName)
        }
        if not matches:
            # The native VO inventory includes enabled users only. Keep disabled
            # accounts addressable for administrators without paging common cases.
            matches = {}
            for item in collect_pages(
                lambda page_no, page_size: self.users.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=normalized
                    if _positive_numeric_selector(normalized) is None
                    else None,
                ),
                resource=_USER_RESOURCE,
            ):
                if not _matches_user(normalized, item.id, item.userName):
                    continue
                identity = UserIdentity(
                    positive_int(
                        item.id,
                        ds_version=self.users.ds_version,
                        resource=_USER_RESOURCE,
                        field="id",
                    ),
                    non_empty_text(
                        item.userName,
                        ds_version=self.users.ds_version,
                        resource=_USER_RESOURCE,
                        field="userName",
                    ),
                )
                matches[identity.id] = identity
        if len(matches) != 1:
            if not matches:
                message = f"User {normalized!r} was not found"
                raise NotFoundError(
                    message,
                    details={"resource": _USER_RESOURCE, "selector": normalized},
                )
            message = f"User name {normalized!r} is ambiguous"
            raise ResolutionError(
                message,
                details={
                    "resource": _USER_RESOURCE,
                    "selector": normalized,
                    "ids": sorted(matches),
                },
            )
        return next(iter(matches.values()))


def _matches_user(selector: str, user_id: int | None, user_name: str | None) -> bool:
    normalized = _normalize_selector(selector, resource="User")
    numeric = _positive_numeric_selector(normalized)
    return user_id == numeric if numeric is not None else user_name == normalized


def bind_user_identity_lookup(
    profile: ClusterProfile, *, http_client: DolphinSchedulerClient
) -> UserIdentityLookup | None:
    """Bind the source-derived simple identity recipe when the exact profile uses it."""
    compiled_profile = _USER_PROGRAMS.profile(profile.ds_version)
    recipe = _recipe_for_profile(compiled_profile)
    if not recipe.simple_user_list:
        return None
    programs = _USER_PROGRAMS.bind(compiled_profile, profile, http_client=http_client)
    return UserIdentityLookup(_CompiledUserLookupOperations(programs, recipe))


def _recipe_for_profile(profile: CompiledWireProfile) -> _UserRecipe:
    recipe = _USER_RECIPES.get(profile.recipe_id or "")
    if recipe is None:
        message = f"DS {profile.ds_version} compiled user recipe is unsupported"
        raise WireContractError(message)
    return recipe


def _user_snapshot(
    item: OpaqueGeneratedValue,
    *,
    recipe: _UserRecipe,
    ds_version: str,
    raw: bool,
) -> UserSnapshot:
    state = (
        _int_field(item, "state", ds_version=ds_version, resource=_USER_RESOURCE)
        if recipe.has_state
        else 1
    )
    raw_queue = optional_text_field(
        item,
        "queue",
        ds_version=ds_version,
        resource=_USER_RESOURCE,
    )
    queue_name = optional_text_field(
        item,
        "queueName",
        ds_version=ds_version,
        resource=_USER_RESOURCE,
    )
    effective_queue = queue_name if raw and raw_queue == "" else raw_queue
    return UserSnapshot(
        id=positive_int(
            response_field(item, "id", ds_version=ds_version, resource=_USER_RESOURCE),
            ds_version=ds_version,
            resource=_USER_RESOURCE,
            field="id",
        ),
        userName=non_empty_text(
            response_field(
                item, "userName", ds_version=ds_version, resource=_USER_RESOURCE
            ),
            ds_version=ds_version,
            resource=_USER_RESOURCE,
            field="userName",
        ),
        email=optional_text_field(
            item, "email", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        phone=optional_text_field(
            item, "phone", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        userType=_optional_enum(
            item, "userType", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        tenantId=_int_field(
            item, "tenantId", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        tenantCode=optional_text_field(
            item, "tenantCode", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        queueName=queue_name,
        queue=effective_queue,
        state=state,
        timeZone=(
            optional_text_field(
                item, "timeZone", ds_version=ds_version, resource=_USER_RESOURCE
            )
            if recipe.has_time_zone
            else None
        ),
        storedQueue=raw_queue if raw else None,
        createTime=optional_text_field(
            item, "createTime", ds_version=ds_version, resource=_USER_RESOURCE
        ),
        updateTime=optional_text_field(
            item, "updateTime", ds_version=ds_version, resource=_USER_RESOURCE
        ),
    )


def _merge_user_collections(
    raw: Sequence[UserSnapshot],
    summaries: Sequence[UserListRecord],
) -> list[UserSnapshot]:
    raw_by_id = {item.id: item for item in raw}
    summary_by_id = {item.id: item for item in summaries if item.id is not None}
    ordered_ids = list(raw_by_id)
    ordered_ids.extend(item_id for item_id in summary_by_id if item_id not in raw_by_id)
    return [
        _merge_user_snapshot(
            raw=raw_by_id.get(item_id),
            summary=summary_by_id.get(item_id),
        )
        for item_id in ordered_ids
    ]


def _merge_user_snapshot(
    *,
    raw: UserSnapshot | None,
    summary: UserListRecord | None,
) -> UserSnapshot:
    source = summary or raw
    if source is None or source.id is None or source.userName is None:
        message = "User merge payload was missing required identity fields"
        raise ApiTransportError(
            message,
            details={"resource": _USER_RESOURCE},
        )
    return UserSnapshot(
        id=source.id,
        userName=source.userName,
        email=(
            summary.email
            if summary and summary.email is not None
            else raw.email
            if raw
            else None
        ),
        phone=(
            summary.phone
            if summary and summary.phone is not None
            else raw.phone
            if raw
            else None
        ),
        userType=(
            summary.userType
            if summary and summary.userType is not None
            else raw.userType
            if raw
            else None
        ),
        tenantId=summary.tenantId if summary else raw.tenantId if raw else 0,
        tenantCode=(
            summary.tenantCode
            if summary and summary.tenantCode is not None
            else raw.tenantCode
            if raw
            else None
        ),
        queueName=(
            summary.queueName
            if summary and summary.queueName is not None
            else raw.queueName
            if raw
            else None
        ),
        queue=summary.queue if summary else raw.queue if raw else None,
        state=summary.state if summary else raw.state if raw else 1,
        timeZone=raw.timeZone if raw else None,
        storedQueue=raw.storedQueue if raw else None,
        createTime=(
            summary.createTime
            if summary and summary.createTime is not None
            else raw.createTime
            if raw
            else None
        ),
        updateTime=(
            summary.updateTime
            if summary and summary.updateTime is not None
            else raw.updateTime
            if raw
            else None
        ),
    )


def _permission_project(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    identity: _ProjectIdentity,
) -> PermissionProject:
    project_id = positive_int(
        response_field(item, "id", ds_version=ds_version, resource=_PROJECT_RESOURCE),
        ds_version=ds_version,
        resource=_PROJECT_RESOURCE,
        field="id",
    )
    code = (
        None
        if identity == "id"
        else positive_int(
            response_field(
                item, "code", ds_version=ds_version, resource=_PROJECT_RESOURCE
            ),
            ds_version=ds_version,
            resource=_PROJECT_RESOURCE,
            field="code",
        )
    )
    return PermissionProject(
        id=project_id,
        code=code,
        name=non_empty_text(
            response_field(
                item, "name", ds_version=ds_version, resource=_PROJECT_RESOURCE
            ),
            ds_version=ds_version,
            resource=_PROJECT_RESOURCE,
            field="name",
        ),
        description=optional_text_field(
            item, "description", ds_version=ds_version, resource=_PROJECT_RESOURCE
        ),
    )


def _permission_datasource(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> PermissionDataSource:
    return PermissionDataSource(
        id=positive_int(
            response_field(
                item, "id", ds_version=ds_version, resource=_DATASOURCE_RESOURCE
            ),
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="id",
        ),
        name=non_empty_text(
            response_field(
                item, "name", ds_version=ds_version, resource=_DATASOURCE_RESOURCE
            ),
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            field="name",
        ),
        note=optional_text_field(
            item,
            "note",
            ds_version=ds_version,
            resource=_DATASOURCE_RESOURCE,
            missing_is_none=True,
        ),
        type=(
            _optional_enum(
                item, "type", ds_version=ds_version, resource=_DATASOURCE_RESOURCE
            )
            if hasattr(item, "type")
            else None
        ),
    )


def _permission_namespace(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> PermissionNamespace:
    return PermissionNamespace(
        id=positive_int(
            response_field(
                item, "id", ds_version=ds_version, resource=_NAMESPACE_RESOURCE
            ),
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            field="id",
        ),
        namespace=non_empty_text(
            response_field(
                item, "namespace", ds_version=ds_version, resource=_NAMESPACE_RESOURCE
            ),
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            field="namespace",
        ),
        # 3.0.x K8sNamespace declares k8s, but no numeric cluster identity.
        # Preserve that absence in the stable permission projection.
        clusterCode=(
            optional_int_field(
                item, "clusterCode", ds_version=ds_version, resource=_NAMESPACE_RESOURCE
            )
            if hasattr(item, "clusterCode")
            else None
        ),
        clusterName=optional_text_field(
            item,
            "clusterName",
            ds_version=ds_version,
            resource=_NAMESPACE_RESOURCE,
            missing_is_none=True,
        ),
    )


def _resolve_project_selector(
    selector: str,
    *,
    candidates: Sequence[PermissionProject],
    identity: _ProjectIdentity,
) -> PermissionProject:
    normalized = _normalize_selector(selector, resource="Project")
    numeric = _positive_numeric_selector(normalized)
    if numeric is not None:
        matches = [
            item
            for item in candidates
            if (item.id if identity == "id" else item.code) == numeric
        ]
    else:
        matches = [item for item in candidates if item.name == normalized]
    numeric_label = "id" if identity == "id" else "code"

    def numeric_value(item: PermissionProject) -> int | None:
        return item.id if identity == "id" else item.code

    return _single_match(
        matches,
        resource=_PROJECT_RESOURCE,
        selector=normalized,
        numeric_label=numeric_label,
        numeric_value=numeric_value,
    )


def _resolve_many(
    selectors: Sequence[str],
    *,
    candidates: Sequence[PermissionT],
    name: Callable[[PermissionT], str],
    resource: str,
) -> list[PermissionT]:
    resolved: dict[int, PermissionT] = {}
    for selector in selectors:
        normalized = _normalize_selector(selector, resource=resource.capitalize())
        numeric = _positive_numeric_selector(normalized)
        matches = [
            item
            for item in candidates
            if (item.id == numeric if numeric is not None else name(item) == normalized)
        ]
        selected = _single_match(
            matches,
            resource=resource,
            selector=normalized,
            numeric_label="id",
            numeric_value=lambda item: item.id,
        )
        resolved[selected.id] = selected
    return _sorted_by_name_and_id(resolved.values(), name=name)


def _single_match(
    matches: Sequence[PermissionT],
    *,
    resource: str,
    selector: str,
    numeric_label: str,
    numeric_value: Callable[[PermissionT], int | None],
) -> PermissionT:
    if not matches:
        message = f"{resource.capitalize()} {selector!r} was not found"
        raise NotFoundError(
            message,
            details={"resource": resource, numeric_label: selector},
            suggestion=(
                f"Inspect the available permission choices and retry with one "
                f"exact name or numeric {numeric_label}."
            ),
        )
    if len(matches) > 1:
        message = f"{resource.capitalize()} selector {selector!r} is ambiguous"
        raise ResolutionError(
            message,
            details={
                "resource": resource,
                "selector": selector,
                f"{numeric_label}s": [numeric_value(item) for item in matches],
            },
        )
    return matches[0]


def _normalize_selector(selector: str, *, resource: str) -> str:
    normalized = selector.strip()
    if not normalized:
        message = f"{resource} selector cannot be empty"
        raise UserInputError(
            message,
            suggestion=f"Pass one exact {resource.lower()} name or numeric id.",
        )
    return normalized


def _positive_numeric_selector(selector: str) -> int | None:
    try:
        value = int(selector)
    except ValueError:
        return None
    return value if value > 0 else None


def _dedupe_by_id(items: Sequence[PermissionT]) -> list[PermissionT]:
    return list({item.id: item for item in items}.values())


def _sorted_by_name_and_id(
    items: Iterable[PermissionT],
    *,
    name: Callable[[PermissionT], str],
) -> list[PermissionT]:
    return sorted(items, key=lambda item: (name(item), item.id))


def _object_sequence(
    value: OpaqueGeneratedValue,
    *,
    ds_version: str,
    resource: str,
) -> list[OpaqueGeneratedValue]:
    if isinstance(value, list):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field="items",
        reason="permission response is not a list",
    )


def _ids(values: Iterable[int] | Mapping[int, OpaqueGeneratedValue]) -> str:
    ids = sorted(values)
    return ",".join(str(item) for item in ids)


def _optional_enum(
    item: OpaqueGeneratedValue,
    field: str,
    *,
    ds_version: str,
    resource: str,
) -> StringEnumValue | None:
    value = getattr(item, field, None)
    if value is None:
        return None
    if isinstance(getattr(value, "value", None), str):
        return cast("StringEnumValue", value)
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="enum field has no string value",
    )


def _int_field(
    item: OpaqueGeneratedValue, field: str, *, ds_version: str, resource: str
) -> int:
    value = response_field(item, field, ds_version=ds_version, resource=resource)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource=resource,
        field=field,
        reason="field is not an integer",
    )


def _require_entity_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _EntityResult,
    operation: str,
    ds_version: str,
) -> None:
    if expected == "none":
        if result is None:
            return
        reason = "void operation returned a non-null payload"
    elif expected == "entity":
        if result is not None:
            return
        reason = "entity-returning operation returned null"
    else:
        return
    raise projection_error(
        ds_version=ds_version,
        resource=_USER_RESOURCE,
        field=f"{operation}Result",
        reason=reason,
    )


def _unsupported_user_facet(
    ds_version: str,
    *,
    action: str,
    facet: str,
    introduced_in: str,
) -> UnsupportedFeatureError:
    return UnsupportedFeatureError(
        f"{facet} is unavailable for {action} on DolphinScheduler {ds_version}.",
        details={
            "action": action,
            "facet": facet,
            "selected_version": ds_version,
            "reason": "upstream_field_absent",
            "introduced_in": introduced_in,
        },
        suggestion=(
            f"Use DolphinScheduler {introduced_in} or newer for this user field."
        ),
    )


__all__ = [
    "USER_DOMAIN",
    "PermissionDataSource",
    "PermissionNamespace",
    "PermissionProject",
    "PermissionSetChange",
    "ProjectPermissionChange",
    "UserAdapter",
    "UserDeletion",
    "UserDomain",
    "UserSelection",
    "UserSnapshot",
    "bind_user_lookup",
]
