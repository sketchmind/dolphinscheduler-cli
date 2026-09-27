from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiResultError, ApiTransportError, UserInputError
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
from dsctl.upstream.response_projection import (
    optional_text_field,
    positive_int,
    project_page,
    projection_error,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        AlertGroupOperations,
        AlertGroupPageRecord,
        AlertGroupRecord,
    )


_ALERT_GROUP_NOT_EXIST = 10011
_DIRECT_GET_ABSENT = frozenset({"1.3.9"})

_MutationResult = Literal["none", "entity"]
_DeleteResult = Literal["none", "boolean"]
_AlertGroupPrimitive = Literal["page", "direct_get", "create", "update", "delete"]
AlertGroupAssociation = Literal["legacy-alert-type", "plugin-instance-ids"]

_ALERT_GROUP_EXPECTATIONS: Mapping[
    _AlertGroupPrimitive,
    CompiledProgramExpectation,
] = {
    "page": READ_RETRY_OPTIONAL,
    "direct_get": CompiledProgramExpectation(
        mode=READ_RETRY_OPTIONAL.mode,
        envelope=READ_RETRY_OPTIONAL.envelope,
        absent_versions=_DIRECT_GET_ABSENT,
    ),
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
}
_ALERT_GROUP_PROGRAMS = CompiledDomainPrograms[_AlertGroupPrimitive](
    name="alert_group",
    schema_constant="COMPILED_ALERT_GROUP_SCHEMA_VERSION",
    schema_version=2,
    expectations=_ALERT_GROUP_EXPECTATIONS,
)


@dataclass(frozen=True)
class AlertGroupDomain:
    """Caller-oriented exact alert-group surface."""

    alert_groups: AlertGroupOperations


@dataclass(frozen=True)
class AlertGroupSnapshot:
    """Version-neutral projection consumed by stable alert-group services."""

    id: int
    groupName: str | None  # noqa: N815
    alertInstanceIds: str | None  # noqa: N815
    groupType: str | None  # noqa: N815
    description: str | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    createUserId: int | None  # noqa: N815


@dataclass(frozen=True)
class _AlertGroupRecipe:
    legacy_association: bool
    direct_get: bool
    create_result: _MutationResult
    update_result: _MutationResult
    delete_result: _DeleteResult


_LEGACY_RECIPE = _AlertGroupRecipe(
    legacy_association=True,
    direct_get=False,
    create_result="none",
    update_result="none",
    delete_result="none",
)
_VOID_RECIPE = _AlertGroupRecipe(
    legacy_association=False,
    direct_get=True,
    create_result="none",
    update_result="none",
    delete_result="none",
)
_CREATE_ENTITY_RECIPE = _AlertGroupRecipe(
    legacy_association=False,
    direct_get=True,
    create_result="entity",
    update_result="none",
    delete_result="none",
)
_ENTITY_RECIPE = _AlertGroupRecipe(
    legacy_association=False,
    direct_get=True,
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
)


class AlertGroupAdapter:
    """Compiled-wire alert-group adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _ALERT_GROUP_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> AlertGroupAdapter:
        """Return the adapter for one explicitly reviewed DS version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> AlertGroupDomain:
        """Bind only the alert-group operations required by stable services."""
        programs = _ALERT_GROUP_PROGRAMS.bind(
            self._profile,
            profile,
            http_client=http_client,
        )
        return AlertGroupDomain(
            alert_groups=cast(
                "AlertGroupOperations",
                _CompiledAlertGroupOperations(programs, self._recipe),
            )
        )


ALERT_GROUP_DOMAIN = BoundDomain[AlertGroupDomain](
    name="alert-group",
    adapter_for_version=AlertGroupAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledAlertGroupOperations:
    programs: BoundCompiledPrograms[_AlertGroupPrimitive]
    recipe: _AlertGroupRecipe

    @property
    def association(self) -> AlertGroupAssociation:
        """Return the selected exact-version association model."""
        return (
            "legacy-alert-type"
            if self.recipe.legacy_association
            else "plugin-instance-ids"
        )

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> AlertGroupPageRecord:
        page = self.programs.call(
            "page",
            {"searchVal": search, "pageNo": page_no, "pageSize": page_size},
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="alert-group",
        )
        return cast(
            "AlertGroupPageRecord",
            project_page(
                page,
                [
                    _alert_group_snapshot(item, ds_version=self.ds_version)
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource="alert-group",
            ),
        )

    def list_all(self) -> Sequence[AlertGroupRecord]:
        return collect_pages(self.list, resource="alert-group")

    def get(self, *, alert_group_id: int) -> AlertGroupRecord:
        if self.recipe.direct_get:
            result = self.programs.call("direct_get", {"id": alert_group_id})
            return _alert_group_snapshot(result, ds_version=self.ds_version)
        for alert_group in self.list_all():
            if alert_group.id == alert_group_id:
                return alert_group
        raise ApiResultError(
            result_code=_ALERT_GROUP_NOT_EXIST,
            result_message=f"alert group {alert_group_id} does not exist",
        )

    def create(
        self,
        *,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> AlertGroupRecord:
        args: JsonObject
        if self.recipe.legacy_association:
            _require_legacy_association(
                group_type=group_type,
                alert_instance_ids=alert_instance_ids,
            )
            args = {
                "groupName": group_name,
                "description": description,
                "groupType": group_type,
            }
        else:
            _require_modern_association(
                group_type=group_type,
                alert_instance_ids=alert_instance_ids,
            )
            args = {
                "groupName": group_name,
                "description": description,
                "alertInstanceIds": alert_instance_ids,
            }
        result = mutation_call(
            lambda: self.programs.call("create", args),
            ds_version=self.ds_version,
            resource="alert-group",
            operation="create",
        )
        return self._verified_readback(
            operation="create",
            result=result,
            expected_result=self.recipe.create_result,
            alert_group_id=None,
            group_name=group_name,
            description=description,
            alert_instance_ids=alert_instance_ids,
            group_type=group_type,
        )

    def update(
        self,
        *,
        alert_group_id: int,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> AlertGroupRecord:
        args: JsonObject
        if self.recipe.legacy_association:
            _require_legacy_association(
                group_type=group_type,
                alert_instance_ids=alert_instance_ids,
            )
            args = {
                "id": alert_group_id,
                "groupName": group_name,
                "description": description,
                "groupType": group_type,
            }
        else:
            _require_modern_association(
                group_type=group_type,
                alert_instance_ids=alert_instance_ids,
            )
            args = {
                "id": alert_group_id,
                "groupName": group_name,
                "description": description,
                "alertInstanceIds": alert_instance_ids,
            }
        result = mutation_call(
            lambda: self.programs.call("update", args),
            ds_version=self.ds_version,
            resource="alert-group",
            operation="update",
        )
        return self._verified_readback(
            operation="update",
            result=result,
            expected_result=self.recipe.update_result,
            alert_group_id=alert_group_id,
            group_name=group_name,
            description=description,
            alert_instance_ids=alert_instance_ids,
            group_type=group_type,
        )

    def delete(self, *, alert_group_id: int) -> bool:
        result = mutation_call(
            lambda: self.programs.call("delete", {"id": alert_group_id}),
            ds_version=self.ds_version,
            resource="alert-group",
            operation="delete",
        )

        def verify() -> bool:
            _require_delete_result(
                result,
                expected=self.recipe.delete_result,
                ds_version=self.ds_version,
            )
            if any(item.id == alert_group_id for item in self.list_all()):
                message = "Alert-group deletion readback still returned the deleted id"
                raise ApiTransportError(
                    message,
                    details={"alert_group_id": alert_group_id},
                )
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="alert-group",
            operation="delete",
        )

    def _verified_readback(
        self,
        *,
        operation: str,
        result: OpaqueGeneratedValue,
        expected_result: _MutationResult,
        alert_group_id: int | None,
        group_name: str,
        description: str | None,
        alert_instance_ids: str | None,
        group_type: str | None,
    ) -> AlertGroupRecord:
        def verify() -> AlertGroupRecord:
            _require_mutation_result(
                result,
                expected=expected_result,
                operation=operation,
                ds_version=self.ds_version,
            )
            resolved_id = alert_group_id
            if resolved_id is None:
                candidates = collect_pages(
                    lambda page_no, page_size: self.list(
                        page_no=page_no,
                        page_size=page_size,
                        search=group_name,
                    ),
                    resource="alert-group",
                )
                matches = [item for item in candidates if item.groupName == group_name]
                if len(matches) != 1:
                    message = (
                        "Alert-group create readback did not return one exact match"
                    )
                    raise ApiTransportError(
                        message,
                        details={
                            "group_name": group_name,
                            "match_count": len(matches),
                        },
                    )
                resolved_id = matches[0].id
            if resolved_id is None:
                message = "Alert-group readback was missing its id"
                raise ApiTransportError(
                    message,
                    details={"group_name": group_name},
                )
            refreshed = self.get(alert_group_id=resolved_id)
            association_matches = (
                getattr(refreshed, "groupType", None) == group_type
                if self.recipe.legacy_association
                else refreshed.alertInstanceIds == alert_instance_ids
            )
            if (
                refreshed.groupName != group_name
                or refreshed.description != description
                or not association_matches
            ):
                message = "Alert-group mutation readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={
                        "alert_group_id": resolved_id,
                        "group_name": group_name,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="alert-group",
            operation=operation,
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


_RECIPES = {
    "legacy_alert_type": _LEGACY_RECIPE,
    "plugin_instances_void_mutations": _VOID_RECIPE,
    "plugin_instances_entity_create": _CREATE_ENTITY_RECIPE,
    "plugin_instances_entity_mutations": _ENTITY_RECIPE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _AlertGroupRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled alert-group recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled alert-group recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _alert_group_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str
) -> AlertGroupSnapshot:
    return AlertGroupSnapshot(
        id=positive_int(
            response_field(
                item,
                "id",
                ds_version=ds_version,
                resource="alert-group",
            ),
            ds_version=ds_version,
            resource="alert-group",
            field="id",
        ),
        groupName=optional_text_field(
            item,
            "groupName",
            ds_version=ds_version,
            resource="alert-group",
        ),
        alertInstanceIds=optional_text_field(
            item,
            "alertInstanceIds",
            ds_version=ds_version,
            resource="alert-group",
            missing_is_none=True,
        ),
        groupType=optional_text_field(
            item,
            "groupType",
            ds_version=ds_version,
            resource="alert-group",
            missing_is_none=True,
        ),
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource="alert-group",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="alert-group",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="alert-group",
        ),
        createUserId=_optional_int_field(
            item,
            "createUserId",
            ds_version=ds_version,
        ),
    )


def _optional_int_field(
    item: OpaqueGeneratedValue, name: str, *, ds_version: str
) -> int | None:
    value = getattr(item, name, None)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise projection_error(
        ds_version=ds_version,
        resource="alert-group",
        field=name,
        reason="payload field is not an integer or null",
    )


def _require_mutation_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _MutationResult,
    operation: str,
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "entity" and result is not None:
        return
    raise projection_error(
        ds_version=ds_version,
        resource="alert-group",
        field=f"{operation}Result",
        reason="mutation result did not match the exact-version contract",
    )


def _require_delete_result(
    result: OpaqueGeneratedValue,
    *,
    expected: _DeleteResult,
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "boolean" and result is True:
        return
    raise projection_error(
        ds_version=ds_version,
        resource="alert-group",
        field="deleteResult",
        reason="delete result did not confirm success",
    )


def _require_legacy_association(
    *,
    group_type: str | None,
    alert_instance_ids: str | None,
) -> None:
    if group_type not in {"EMAIL", "SMS"} or alert_instance_ids is not None:
        message = "Legacy alert groups require EMAIL/SMS groupType only"
        raise UserInputError(message)


def _require_modern_association(
    *,
    group_type: str | None,
    alert_instance_ids: str | None,
) -> None:
    if group_type is not None or alert_instance_ids is None:
        message = "Modern alert groups require plugin-instance ids only"
        raise UserInputError(message)


__all__ = [
    "ALERT_GROUP_DOMAIN",
    "AlertGroupAdapter",
    "AlertGroupDomain",
    "AlertGroupSnapshot",
]
