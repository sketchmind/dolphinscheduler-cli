from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiResultError, ApiTransportError
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
from dsctl.upstream.queues import (
    QueueSnapshot,
    bind_queue_lookup,
)
from dsctl.upstream.response_projection import (
    non_empty_text,
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        QueueLookupOperations,
        TenantOperations,
        TenantPageRecord,
        TenantRecord,
    )


_TENANT_NOT_EXIST = 10017

# Exact init SQL seeds (-1, 'default', queue_id=1) in t_ds_tenant.
# PostgreSQL upgrade/3.2.0_schema/postgresql/dolphinscheduler_dml.sql seeds queue_id=0.
# Evidence: dolphinscheduler-dao/src/main/resources/sql/dolphinscheduler_*.sql.
_SEEDED_DEFAULT_TENANT_VERSIONS = frozenset(
    {"3.2.0", "3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"}
)

_CreateResult = Literal["none", "tenant"]
_MutationResult = Literal["none", "boolean"]
_TenantPrimitive = Literal["page", "create", "update", "delete"]
_TENANT_EXPECTATIONS: Mapping[_TenantPrimitive, CompiledProgramExpectation] = {
    "page": READ_RETRY_OPTIONAL,
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
}
_TENANT_PROGRAMS = CompiledDomainPrograms[_TenantPrimitive](
    name="tenant",
    schema_constant="COMPILED_TENANT_SCHEMA_VERSION",
    schema_version=1,
    expectations=_TENANT_EXPECTATIONS,
)


@dataclass(frozen=True)
class TenantDomain:
    """Caller-oriented tenant surface and its required queue lookup port."""

    tenants: TenantOperations
    queues: QueueLookupOperations


@dataclass(frozen=True)
class TenantSnapshot:
    """Version-neutral tenant projection consumed by stable services."""

    id: int
    tenantCode: str  # noqa: N815
    description: str | None
    queueId: int  # noqa: N815
    queueName: str | None  # noqa: N815
    queue: str | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    legacyTenantName: str | None = None  # noqa: N815


@dataclass(frozen=True)
class _TenantRecipe:
    create_result: _CreateResult
    update_result: _MutationResult
    delete_result: _MutationResult
    legacy_tenant_name: bool = False


_LEGACY_NAMED_RECIPE = _TenantRecipe(
    create_result="none",
    update_result="none",
    delete_result="none",
    legacy_tenant_name=True,
)
_VOID_RECIPE = _TenantRecipe(
    create_result="none",
    update_result="none",
    delete_result="none",
)
_CREATE_ENTITY_RECIPE = _TenantRecipe(
    create_result="tenant",
    update_result="none",
    delete_result="none",
)
_BOOLEAN_RECIPE = _TenantRecipe(
    create_result="tenant",
    update_result="boolean",
    delete_result="boolean",
)


class TenantAdapter:
    """Compiled tenant domain adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _TENANT_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> TenantAdapter:
        """Return the tenant adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TenantDomain:
        """Bind only tenant operations and their queue lookup dependency."""
        programs = _TENANT_PROGRAMS.bind(
            self._profile,
            profile,
            http_client=http_client,
        )
        return TenantDomain(
            tenants=cast(
                "TenantOperations",
                _CompiledTenantOperations(programs, self._recipe),
            ),
            queues=bind_queue_lookup(profile, http_client=http_client),
        )


TENANT_DOMAIN = BoundDomain[TenantDomain](
    name="tenant",
    adapter_for_version=TenantAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledTenantOperations:
    programs: BoundCompiledPrograms[_TenantPrimitive]
    recipe: _TenantRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> TenantPageRecord:
        execution = self.programs.execute(
            "page",
            self.programs.prepare(
                "page", {"searchVal": search, "pageNo": page_no, "pageSize": page_size}
            ),
        )
        _require_native_tenant_integers(
            execution.raw_payload, ds_version=self.ds_version
        )
        page = execution.payload
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="tenant",
        )
        return cast(
            "TenantPageRecord",
            project_page(
                page,
                [
                    _tenant_snapshot(
                        item,
                        ds_version=self.ds_version,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource="tenant",
            ),
        )

    def list_all(self) -> Sequence[TenantRecord]:
        return collect_pages(
            self.list,
            resource="tenant",
        )

    def get(self, *, tenant_id: int) -> TenantRecord:
        for tenant in self.list_all():
            if tenant.id == tenant_id:
                return tenant
        raise ApiResultError(
            result_code=_TENANT_NOT_EXIST,
            result_message=f"tenant {tenant_id} does not exist",
        )

    def create(
        self,
        *,
        tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        values: JsonObject = {
            "tenantCode": tenant_code,
            "queueId": queue_id,
            "description": description,
        }
        if self.recipe.legacy_tenant_name:
            values["tenantName"] = tenant_code
        result = self._mutation_call(
            "create",
            lambda: self.programs.call("create", values),
        )
        _require_create_result(
            result,
            expected=self.recipe.create_result,
            ds_version=self.ds_version,
        )
        return self._verified_readback(
            operation="create",
            tenant_code=tenant_code,
            tenant_id=None,
            queue_id=queue_id,
            description=description,
        )

    def update(
        self,
        *,
        tenant_id: int,
        current_tenant_code: str,
        queue_id: int,
        description: str | None = None,
    ) -> TenantRecord:
        tenant_id = _tenant_identity(
            tenant_id, tenant_code=current_tenant_code, ds_version=self.ds_version
        )
        values: JsonObject = {
            "id": tenant_id,
            "tenantCode": current_tenant_code,
            "queueId": queue_id,
            "description": description,
        }
        if self.recipe.legacy_tenant_name:
            current = cast("TenantSnapshot", self.get(tenant_id=tenant_id))
            legacy_name = current.legacyTenantName
            if not legacy_name:
                raise projection_error(
                    ds_version=self.ds_version,
                    resource="tenant",
                    field="tenantName",
                    reason="legacy tenant update cannot preserve an empty name",
                )
            values["tenantName"] = legacy_name
        result = self._mutation_call(
            "update",
            lambda: self.programs.call("update", values),
        )
        _require_success_flag(
            result,
            expected=self.recipe.update_result,
            operation="update",
            ds_version=self.ds_version,
        )
        return self._verified_readback(
            operation="update",
            tenant_code=current_tenant_code,
            tenant_id=tenant_id,
            queue_id=queue_id,
            description=description,
        )

    def delete(self, *, tenant_id: int) -> bool:
        if tenant_id == -1:
            # Delete has no code argument: verify the native identity before writing.
            self.get(tenant_id=tenant_id)
        else:
            positive_int(
                tenant_id, ds_version=self.ds_version, resource="tenant", field="id"
            )
        result = self._mutation_call(
            "delete",
            lambda: self.programs.call("delete", {"id": tenant_id}),
        )
        return _require_success_flag(
            result,
            expected=self.recipe.delete_result,
            operation="delete",
            ds_version=self.ds_version,
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def _mutation_call(
        self,
        operation: str,
        call: Callable[[], OpaqueGeneratedValue],
    ) -> OpaqueGeneratedValue:
        return mutation_call(
            call,
            ds_version=self.ds_version,
            resource="tenant",
            operation=operation,
        )

    def _verified_readback(
        self,
        *,
        operation: str,
        tenant_code: str,
        tenant_id: int | None,
        queue_id: int,
        description: str | None,
    ) -> TenantRecord:
        def verify() -> TenantRecord:
            candidates: list[TenantRecord] = collect_pages(
                lambda page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=tenant_code,
                ),
                resource="tenant",
            )
            matches = [
                item
                for item in candidates
                if item.tenantCode == tenant_code
                and (tenant_id is None or item.id == tenant_id)
            ]
            if len(matches) != 1:
                message = "Tenant mutation readback did not return one exact match"
                raise ApiTransportError(
                    message,
                    details={
                        "tenant_code": tenant_code,
                        "tenant_id": tenant_id,
                        "match_count": len(matches),
                    },
                )
            tenant = matches[0]
            if tenant.queueId != queue_id or tenant.description != description:
                message = "Tenant mutation readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={
                        "tenant_code": tenant_code,
                        "expected_queue_id": queue_id,
                        "actual_queue_id": tenant.queueId,
                        "expected_description": description,
                        "actual_description": tenant.description,
                    },
                )
            return tenant

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="tenant",
            operation=operation,
        )


_RECIPES = {
    "legacy_named": _LEGACY_NAMED_RECIPE,
    "void": _VOID_RECIPE,
    "create_entity": _CREATE_ENTITY_RECIPE,
    "boolean": _BOOLEAN_RECIPE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _TenantRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled tenant recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled tenant recipe {profile.recipe_id!r} is unsupported"
        raise WireContractError(message) from exc


def _require_native_tenant_integers(payload: JsonValue, *, ds_version: str) -> None:
    # Generated int fields may coerce bools or synthesize defaults. Native identity
    # exceptions must be supported by explicit integer fields in the response.
    rows = payload.get("totalList") if isinstance(payload, dict) else None
    if rows is None:
        return
    if not isinstance(rows, list):
        raise projection_error(
            ds_version=ds_version,
            resource="tenant",
            field="totalList",
            reason="page items are not a list",
        )
    for row in rows:
        for field in ("id", "queueId"):
            if not isinstance(row, dict) or type(row.get(field)) is not int:
                raise projection_error(
                    ds_version=ds_version,
                    resource="tenant",
                    field=field,
                    reason="native identity must be an explicit integer",
                )


def _tenant_snapshot(item: OpaqueGeneratedValue, *, ds_version: str) -> TenantSnapshot:
    tenant_code = non_empty_text(
        response_field(item, "tenantCode", ds_version=ds_version, resource="tenant"),
        ds_version=ds_version,
        resource="tenant",
        field="tenantCode",
    )
    raw_id = response_field(item, "id", ds_version=ds_version, resource="tenant")
    tenant_id = _tenant_identity(raw_id, tenant_code=tenant_code, ds_version=ds_version)
    raw_queue_id = response_field(
        item, "queueId", ds_version=ds_version, resource="tenant"
    )
    # Only the already-validated native default identity has an unassigned queue.
    queue_id = (
        0
        if tenant_id == -1 and type(raw_queue_id) is int and raw_queue_id == 0
        else positive_int(
            raw_queue_id, ds_version=ds_version, resource="tenant", field="queueId"
        )
    )
    return TenantSnapshot(
        id=tenant_id,
        tenantCode=tenant_code,
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource="tenant",
        ),
        queueId=queue_id,
        queueName=optional_text_field(
            item,
            "queueName",
            ds_version=ds_version,
            resource="tenant",
        ),
        queue=optional_text_field(
            item,
            "queue",
            ds_version=ds_version,
            resource="tenant",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="tenant",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="tenant",
        ),
        legacyTenantName=optional_text_field(
            item,
            "tenantName",
            ds_version=ds_version,
            resource="tenant",
            missing_is_none=True,
        ),
    )


def _tenant_identity(
    raw_id: OpaqueGeneratedValue, *, tenant_code: str, ds_version: str
) -> int:
    if (
        ds_version in _SEEDED_DEFAULT_TENANT_VERSIONS
        and type(raw_id) is int
        and raw_id == -1
        and tenant_code == "default"
    ):
        return -1
    return positive_int(raw_id, ds_version=ds_version, resource="tenant", field="id")


def _require_success_flag(
    result: OpaqueGeneratedValue,
    *,
    expected: _MutationResult,
    operation: str,
    ds_version: str,
) -> bool:
    if expected == "none":
        if result is None:
            return True
        raise projection_error(
            ds_version=ds_version,
            resource="tenant",
            field=f"{operation}Result",
            reason="legacy void mutation returned a non-null payload",
        )
    if isinstance(result, bool):
        return result
    raise projection_error(
        ds_version=ds_version,
        resource="tenant",
        field=f"{operation}Result",
        reason="mutation result is not boolean",
    )


def _require_create_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _CreateResult,
    ds_version: str,
) -> None:
    if expected == "none":
        if result is None:
            return
        reason = "legacy void create returned a non-null payload"
    elif result is not None:
        return
    else:
        reason = "entity-returning create returned a null payload"
    raise projection_error(
        ds_version=ds_version,
        resource="tenant",
        field="createResult",
        reason=reason,
    )


__all__ = [
    "TENANT_DOMAIN",
    "QueueSnapshot",
    "TenantAdapter",
    "TenantDomain",
    "TenantSnapshot",
]
