from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
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
    non_empty_text,
    optional_int_field,
    optional_text_field,
    optional_text_sequence_field,
    positive_int,
    project_page,
    projection_error,
    require_none,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import (
    CompiledWireProfile,
    WireContractError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        EnvironmentOperations,
        EnvironmentPageRecord,
        EnvironmentPayloadRecord,
        EnvironmentRecord,
    )


_ENVIRONMENT_RESOURCE = "environment"
_ENVIRONMENT_INTRODUCED_IN = "2.0.0"
_UpdateResult = Literal["none", "environment"]
_EnvironmentPrimitive = Literal["page", "get", "create", "update", "delete"]
_ENVIRONMENT_EXPECTATIONS: Mapping[
    _EnvironmentPrimitive,
    CompiledProgramExpectation,
] = {
    "page": READ_RETRY_OPTIONAL,
    "get": READ_RETRY_OPTIONAL,
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
}
_ENVIRONMENT_PROGRAMS = CompiledDomainPrograms[_EnvironmentPrimitive](
    name="environment",
    schema_constant="COMPILED_ENVIRONMENT_SCHEMA_VERSION",
    schema_version=3,
    expectations=_ENVIRONMENT_EXPECTATIONS,
)


@dataclass(frozen=True)
class EnvironmentDomain:
    """Caller-oriented environment resource surface."""

    environments: EnvironmentOperations


@dataclass(frozen=True)
class EnvironmentSnapshot:
    """Version-neutral environment projection consumed by stable services."""

    id: int | None
    code: int
    name: str
    config: str | None
    description: str | None
    workerGroups: tuple[str, ...] | None  # noqa: N815
    operator: int | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _EnvironmentRecipe:
    update_result: _UpdateResult


_VOID_UPDATE = _EnvironmentRecipe(update_result="none")
_ENTITY_UPDATE = _EnvironmentRecipe(update_result="environment")


class EnvironmentAdapter:
    """Compiled environment adapter for every reviewed supporting profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _ENVIRONMENT_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> EnvironmentAdapter:
        """Return one generated adapter for a source-proven supporting version."""
        return cls(ds_version)

    @classmethod
    def _for_profile(
        cls,
        profile: CompiledWireProfile,
    ) -> EnvironmentAdapter:
        adapter = cls.__new__(cls)
        adapter._profile = profile
        adapter._recipe = _recipe_for_profile(profile)
        adapter.ds_version = profile.ds_version
        return adapter

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> EnvironmentDomain:
        """Bind the caller-oriented environment operations."""
        programs = _ENVIRONMENT_PROGRAMS.bind(
            self._profile,
            profile,
            http_client=http_client,
        )
        return EnvironmentDomain(
            environments=cast(
                "EnvironmentOperations",
                _CompiledEnvironmentOperations(
                    programs=programs,
                    recipe=self._recipe,
                ),
            )
        )


@dataclass(frozen=True)
class _AbsentEnvironmentAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> EnvironmentDomain:
        del profile, http_client
        message = (
            "Environment management does not exist in DolphinScheduler "
            f"{self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _ENVIRONMENT_RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _ENVIRONMENT_INTRODUCED_IN,
            },
            suggestion=(
                "Use DolphinScheduler 2.0.0 or newer for environment management."
            ),
        )


def _adapter_for_version(
    ds_version: str,
) -> BoundDomainAdapter[EnvironmentDomain]:
    profile = _ENVIRONMENT_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentEnvironmentAdapter(ds_version)
    if profile.status == "supported":
        return EnvironmentAdapter._for_profile(profile)
    message = f"DS {ds_version} has no reviewed environment capability decision"
    raise WireContractError(message)


ENVIRONMENT_DOMAIN = BoundDomain[EnvironmentDomain](
    name=_ENVIRONMENT_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _CompiledEnvironmentOperations:
    programs: BoundCompiledPrograms[_EnvironmentPrimitive]
    recipe: _EnvironmentRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> EnvironmentPageRecord:
        page = self.programs.call(
            "page",
            {
                "searchVal": search,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        )
        snapshots = [
            _environment_snapshot(item, ds_version=self.ds_version) for item in items
        ]
        return cast(
            "EnvironmentPageRecord",
            project_page(
                page,
                snapshots,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_ENVIRONMENT_RESOURCE,
            ),
        )

    def list_all(self) -> Sequence[EnvironmentRecord]:
        return cast(
            "Sequence[EnvironmentRecord]",
            collect_pages(self.list, resource=_ENVIRONMENT_RESOURCE),
        )

    def get(self, *, code: int) -> EnvironmentPayloadRecord:
        payload = self.programs.call("get", {"environmentCode": code})
        return cast(
            "EnvironmentPayloadRecord",
            _environment_snapshot(
                payload,
                ds_version=self.ds_version,
                expected_code=code,
            ),
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str] | None = None,
    ) -> EnvironmentPayloadRecord:
        payload = mutation_call(
            lambda: self.programs.call(
                "create",
                {
                    "name": name,
                    "config": config,
                    "description": description,
                    "workerGroups": _worker_groups_json(worker_groups),
                },
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="create",
        )
        code = verify_mutation(
            lambda: positive_int(
                payload,
                ds_version=self.ds_version,
                resource=_ENVIRONMENT_RESOURCE,
                field="createResult",
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="create",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._get_verified(
                code=code,
                name=name,
                config=config,
                description=description,
                worker_groups=worker_groups,
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="create",
        )

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
        worker_groups: Sequence[str],
    ) -> EnvironmentPayloadRecord:
        payload = mutation_call(
            lambda: self.programs.call(
                "update",
                {
                    "code": code,
                    "name": name,
                    "config": config,
                    "description": description,
                    "workerGroups": _worker_groups_json(worker_groups),
                },
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="update",
        )
        verify_mutation(
            lambda: _validate_update_result(
                payload,
                recipe=self.recipe,
                ds_version=self.ds_version,
                code=code,
                name=name,
                config=config,
                description=description,
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="update",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._get_verified(
                code=code,
                name=name,
                config=config,
                description=description,
                worker_groups=worker_groups,
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="update",
        )

    def delete(self, *, code: int) -> bool:
        payload = mutation_call(
            lambda: self.programs.call("delete", {"environmentCode": code}),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="delete",
        )
        verify_mutation(
            lambda: require_none(
                payload,
                ds_version=self.ds_version,
                resource=_ENVIRONMENT_RESOURCE,
                field="deleteResult",
            ),
            ds_version=self.ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            operation="delete",
            phase="mutation_response",
        )
        return True

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version

    def _get_verified(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None,
        worker_groups: Sequence[str] | None,
    ) -> EnvironmentPayloadRecord:
        environment = cast("EnvironmentSnapshot", self.get(code=code))
        if (
            environment.name != name
            or environment.config != config
            or environment.description != description
            or (
                worker_groups is not None
                and environment.workerGroups != tuple(worker_groups)
            )
        ):
            raise projection_error(
                ds_version=self.ds_version,
                resource=_ENVIRONMENT_RESOURCE,
                field="mutationReadback",
                reason="readback fields do not match the requested mutation",
            )
        return cast("EnvironmentPayloadRecord", environment)


_RECIPES = {
    "void_update": _VOID_UPDATE,
    "entity_update": _ENTITY_UPDATE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _EnvironmentRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled environment recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled environment recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _environment_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    expected_code: int | None = None,
) -> EnvironmentSnapshot:
    code = positive_int(
        response_field(
            item,
            "code",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_ENVIRONMENT_RESOURCE,
        field="code",
    )
    if expected_code is not None and code != expected_code:
        raise projection_error(
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            field="code",
            reason="response identity does not match the requested environment",
        )
    return EnvironmentSnapshot(
        id=optional_int_field(
            item,
            "id",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        code=code,
        name=non_empty_text(
            response_field(
                item,
                "name",
                ds_version=ds_version,
                resource=_ENVIRONMENT_RESOURCE,
            ),
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            field="name",
        ),
        config=optional_text_field(
            item,
            "config",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        workerGroups=optional_text_sequence_field(
            item,
            "workerGroups",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        operator=optional_int_field(
            item,
            "operator",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
    )


def _validate_update_result(
    payload: OpaqueGeneratedValue,
    *,
    recipe: _EnvironmentRecipe,
    ds_version: str,
    code: int,
    name: str,
    config: str,
    description: str | None,
) -> None:
    if recipe.update_result == "none":
        require_none(
            payload,
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            field="updateResult",
        )
        return
    actual_code = positive_int(
        response_field(
            payload,
            "code",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_ENVIRONMENT_RESOURCE,
        field="code",
    )
    actual_name = non_empty_text(
        response_field(
            payload,
            "name",
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_ENVIRONMENT_RESOURCE,
        field="name",
    )
    actual_config = optional_text_field(
        payload,
        "config",
        ds_version=ds_version,
        resource=_ENVIRONMENT_RESOURCE,
    )
    actual_description = optional_text_field(
        payload,
        "description",
        ds_version=ds_version,
        resource=_ENVIRONMENT_RESOURCE,
    )
    if (
        actual_code != code
        or actual_name != name
        or actual_config != config
        or actual_description != description
    ):
        raise projection_error(
            ds_version=ds_version,
            resource=_ENVIRONMENT_RESOURCE,
            field="updateResult",
            reason="mutation response fields do not match the requested update",
        )


def _worker_groups_json(worker_groups: Sequence[str] | None) -> str | None:
    if worker_groups is None:
        return None
    return json.dumps(list(worker_groups), ensure_ascii=False, separators=(",", ":"))


__all__ = [
    "ENVIRONMENT_DOMAIN",
    "EnvironmentAdapter",
    "EnvironmentDomain",
    "EnvironmentSnapshot",
]
