from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import NotFoundError, ResolutionError, UnsupportedFeatureError
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
    require_boolean,
    response_field,
    sequence_field,
)
from dsctl.upstream.wire import (
    CompiledWireProfile,
    WireContractError,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        ClusterOperations,
        ClusterPageRecord,
        ClusterPayloadRecord,
    )


_CLUSTER_RESOURCE = "cluster"
_CLUSTER_INTRODUCED_IN = "3.1.0"
_UpdateResult = Literal["none", "cluster"]
_DeleteResult = Literal["none", "boolean"]
_ClusterPrimitive = Literal["page", "get", "create", "update", "delete"]
_CLUSTER_EXPECTATIONS: Mapping[
    _ClusterPrimitive,
    CompiledProgramExpectation,
] = {
    "page": READ_RETRY_OPTIONAL,
    "get": replace(READ_RETRY_OPTIONAL, absent_versions=frozenset({"3.4.3"})),
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
}
_CLUSTER_PROGRAMS = CompiledDomainPrograms[_ClusterPrimitive](
    name="cluster",
    schema_constant="COMPILED_CLUSTER_SCHEMA_VERSION",
    schema_version=4,
    expectations=_CLUSTER_EXPECTATIONS,
)


@dataclass(frozen=True)
class ClusterDomain:
    """Caller-oriented cluster resource surface."""

    clusters: ClusterOperations


@dataclass(frozen=True)
class ClusterSnapshot:
    """Version-neutral cluster projection consumed by stable services."""

    id: int
    code: int
    name: str
    config: str | None
    description: str | None
    workflowDefinitions: tuple[str, ...] | None  # noqa: N815
    operator: int | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _ClusterRecipe:
    update_result: _UpdateResult
    delete_result: _DeleteResult
    definitions_field: str
    paged_detail: bool = False


_LEGACY_RECIPE = _ClusterRecipe(
    update_result="none",
    delete_result="none",
    definitions_field="processDefinitions",
)
_ENTITY_PROCESS_RECIPE = _ClusterRecipe(
    update_result="cluster",
    delete_result="boolean",
    definitions_field="processDefinitions",
)
_ENTITY_WORKFLOW_RECIPE = _ClusterRecipe(
    update_result="cluster",
    delete_result="boolean",
    definitions_field="workflowDefinitions",
)


class ClusterAdapter:
    """Compiled-wire cluster adapter for every reviewed supporting profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _CLUSTER_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> ClusterAdapter:
        """Return one generated adapter for a source-proven supporting version."""
        return cls(ds_version)

    @classmethod
    def _for_profile(
        cls,
        profile: CompiledWireProfile,
    ) -> ClusterAdapter:
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
    ) -> ClusterDomain:
        """Bind the caller-oriented cluster operations."""
        programs = _CLUSTER_PROGRAMS.bind(
            self._profile,
            profile,
            http_client=http_client,
        )
        return ClusterDomain(
            clusters=cast(
                "ClusterOperations",
                _CompiledClusterOperations(
                    programs=programs,
                    recipe=self._recipe,
                ),
            )
        )


@dataclass(frozen=True)
class _AbsentClusterAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ClusterDomain:
        del profile, http_client
        message = (
            f"Cluster management does not exist in DolphinScheduler {self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _CLUSTER_RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _CLUSTER_INTRODUCED_IN,
            },
            suggestion="Use DolphinScheduler 3.1.0 or newer for cluster management.",
        )


def _adapter_for_version(ds_version: str) -> BoundDomainAdapter[ClusterDomain]:
    profile = _CLUSTER_PROGRAMS.profile(ds_version)
    if profile.status == "upstream_absent":
        return _AbsentClusterAdapter(ds_version)
    if profile.status == "supported":
        return ClusterAdapter._for_profile(profile)
    message = f"DS {ds_version} has no reviewed cluster capability decision"
    raise WireContractError(message)


CLUSTER_DOMAIN = BoundDomain[ClusterDomain](
    name=_CLUSTER_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _CompiledClusterOperations:
    programs: BoundCompiledPrograms[_ClusterPrimitive]
    recipe: _ClusterRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> ClusterPageRecord:
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
            resource=_CLUSTER_RESOURCE,
        )
        snapshots = [
            _cluster_snapshot(
                item,
                ds_version=self.ds_version,
                definitions_field=self.recipe.definitions_field,
            )
            for item in items
        ]
        return cast(
            "ClusterPageRecord",
            project_page(
                page,
                snapshots,
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource=_CLUSTER_RESOURCE,
            ),
        )

    def get(self, *, code: int) -> ClusterPayloadRecord:
        if self.recipe.paged_detail:
            matches = [
                item
                for item in collect_pages(self.list, resource=_CLUSTER_RESOURCE)
                if item.code == code
            ]
            if not matches:
                message = f"Cluster code {code} was not found"
                raise NotFoundError(
                    message,
                    details={"resource": _CLUSTER_RESOURCE, "code": code},
                )
            if len(matches) > 1:
                message = f"Cluster code {code} is ambiguous"
                raise ResolutionError(
                    message,
                    details={"resource": _CLUSTER_RESOURCE, "code": code},
                )
            return matches[0]
        payload = self.programs.call("get", {"clusterCode": code})
        return cast(
            "ClusterPayloadRecord",
            _cluster_snapshot(
                payload,
                ds_version=self.ds_version,
                definitions_field=self.recipe.definitions_field,
                expected_code=code,
            ),
        )

    def create(
        self,
        *,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterPayloadRecord:
        payload = mutation_call(
            lambda: self.programs.call(
                "create",
                {"name": name, "config": config, "description": description},
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="create",
        )
        code = verify_mutation(
            lambda: positive_int(
                payload,
                ds_version=self.ds_version,
                resource=_CLUSTER_RESOURCE,
                field="createResult",
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="create",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._get_verified(
                code=code,
                name=name,
                config=config,
                description=description,
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="create",
        )

    def update(
        self,
        *,
        code: int,
        name: str,
        config: str,
        description: str | None = None,
    ) -> ClusterPayloadRecord:
        payload = mutation_call(
            lambda: self.programs.call(
                "update",
                {
                    "code": code,
                    "name": name,
                    "config": config,
                    "description": description,
                },
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
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
            resource=_CLUSTER_RESOURCE,
            operation="update",
            phase="mutation_response",
        )
        return verify_mutation(
            lambda: self._get_verified(
                code=code,
                name=name,
                config=config,
                description=description,
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="update",
        )

    def delete(self, *, code: int) -> bool:
        payload = mutation_call(
            lambda: self.programs.call("delete", {"clusterCode": code}),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="delete",
        )
        return verify_mutation(
            lambda: _validate_delete_result(
                payload,
                recipe=self.recipe,
                ds_version=self.ds_version,
                resource=_CLUSTER_RESOURCE,
            ),
            ds_version=self.ds_version,
            resource=_CLUSTER_RESOURCE,
            operation="delete",
            phase="mutation_response",
        )

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
    ) -> ClusterPayloadRecord:
        cluster = cast("ClusterSnapshot", self.get(code=code))
        if (
            cluster.name != name
            or cluster.config != config
            or cluster.description != description
        ):
            raise projection_error(
                ds_version=self.ds_version,
                resource=_CLUSTER_RESOURCE,
                field="mutationReadback",
                reason="readback fields do not match the requested mutation",
            )
        return cast("ClusterPayloadRecord", cluster)


_RECIPES = {
    "process_definitions_void_mutations": _LEGACY_RECIPE,
    "process_definitions_entity_mutations": _ENTITY_PROCESS_RECIPE,
    "workflow_definitions_entity_mutations": _ENTITY_WORKFLOW_RECIPE,
    "workflow_definitions_paged_readback": replace(
        _ENTITY_WORKFLOW_RECIPE, paged_detail=True
    ),
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _ClusterRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled cluster recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled cluster recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _cluster_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    definitions_field: str,
    expected_code: int | None = None,
) -> ClusterSnapshot:
    code = positive_int(
        response_field(
            item,
            "code",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_CLUSTER_RESOURCE,
        field="code",
    )
    if expected_code is not None and code != expected_code:
        raise projection_error(
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
            field="code",
            reason="response identity does not match the requested cluster",
        )
    return ClusterSnapshot(
        id=positive_int(
            response_field(
                item,
                "id",
                ds_version=ds_version,
                resource=_CLUSTER_RESOURCE,
            ),
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
            field="id",
        ),
        code=code,
        name=non_empty_text(
            response_field(
                item,
                "name",
                ds_version=ds_version,
                resource=_CLUSTER_RESOURCE,
            ),
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
            field="name",
        ),
        config=optional_text_field(
            item,
            "config",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        workflowDefinitions=optional_text_sequence_field(
            item,
            definitions_field,
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        operator=optional_int_field(
            item,
            "operator",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
    )


def _validate_update_result(
    payload: OpaqueGeneratedValue,
    *,
    recipe: _ClusterRecipe,
    ds_version: str,
    code: int,
    name: str,
    config: str,
    description: str | None,
) -> None:
    if recipe.update_result == "none":
        # Exact legacy endpoints declare Void and discard every successful
        # result-envelope data value before the adapter verifies by readback.
        return
    actual_code = positive_int(
        response_field(
            payload,
            "code",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_CLUSTER_RESOURCE,
        field="code",
    )
    actual_name = non_empty_text(
        response_field(
            payload,
            "name",
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
        ),
        ds_version=ds_version,
        resource=_CLUSTER_RESOURCE,
        field="name",
    )
    actual_config = optional_text_field(
        payload,
        "config",
        ds_version=ds_version,
        resource=_CLUSTER_RESOURCE,
    )
    actual_description = optional_text_field(
        payload,
        "description",
        ds_version=ds_version,
        resource=_CLUSTER_RESOURCE,
    )
    if (
        actual_code != code
        or actual_name != name
        or actual_config != config
        or actual_description != description
    ):
        raise projection_error(
            ds_version=ds_version,
            resource=_CLUSTER_RESOURCE,
            field="updateResult",
            reason="mutation response fields do not match the requested update",
        )


def _validate_delete_result(
    payload: OpaqueGeneratedValue,
    *,
    recipe: _ClusterRecipe,
    ds_version: str,
    resource: str,
) -> bool:
    if recipe.delete_result == "none":
        # Exact legacy endpoints declare Void and ignore successful data.
        return True
    return require_boolean(
        payload,
        ds_version=ds_version,
        resource=resource,
        field="deleteResult",
    )


__all__ = [
    "CLUSTER_DOMAIN",
    "ClusterAdapter",
    "ClusterDomain",
    "ClusterSnapshot",
]
