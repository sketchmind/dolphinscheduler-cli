from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import ApiResultError, ApiTransportError, UnsupportedFeatureError
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
    non_empty_text,
    optional_bool_field,
    optional_text_field,
    positive_int,
    project_page,
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
        WorkerGroupOperations,
        WorkerGroupPageRecord,
        WorkerGroupRecord,
    )


_WORKER_GROUP_NOT_EXIST = 1402001
_LEGACY_WORKER_GROUP_NOT_EXIST = 10174

# Exact 3.1.0/3.1.1 saveWorkerGroup checks whether a same-name row has the
# *same* id, rather than a different id. Updating a row without renaming it
# therefore returns NAME_EXIST before any requested field can be persisted.
# Source: apache/dolphinscheduler tags 3.1.0 and 3.1.1,
# dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/service/
# impl/WorkerGroupServiceImpl.java, checkWorkerGroupNameExists. Tag 3.1.2
# corrects the predicate by excluding the row's own id.
_SAME_NAME_UPDATE_BROKEN_VERSIONS = frozenset({"3.1.0", "3.1.1"})
_SAME_NAME_UPDATE_REASON = "upstream_same_name_update_self_collision"


def worker_group_same_name_update_limitation(ds_version: str) -> str | None:
    """Describe the exact upstream limitation on updates without a rename."""
    if ds_version in _SAME_NAME_UPDATE_BROKEN_VERSIONS:
        return _SAME_NAME_UPDATE_REASON
    return None


_WorkerGroupPrimitive = Literal["page", "save", "delete"]
_WORKER_GROUP_EXPECTATIONS: Mapping[
    _WorkerGroupPrimitive,
    CompiledProgramExpectation,
] = {
    "page": READ_RETRY_OPTIONAL,
    "save": MUTATION_ONCE_REQUIRED,
    "delete": MUTATION_ONCE_REQUIRED,
}
_WORKER_GROUP_PROGRAMS = CompiledDomainPrograms[_WorkerGroupPrimitive](
    name="worker_group",
    schema_constant="COMPILED_WORKER_GROUP_SCHEMA_VERSION",
    schema_version=2,
    expectations=_WORKER_GROUP_EXPECTATIONS,
)


@dataclass(frozen=True)
class WorkerGroupDomain:
    """Caller-oriented exact worker-group surface."""

    worker_groups: WorkerGroupOperations


@dataclass(frozen=True)
class WorkerGroupSnapshot:
    """Version-neutral worker-group projection consumed by stable services."""

    id: int | None
    name: str
    addrList: str | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    description: str | None
    systemDefault: bool  # noqa: N815


@dataclass(frozen=True)
class _WorkerGroupRecipe:
    supports_description: bool
    omitted_description: str | None = None
    not_found_code: int = _LEGACY_WORKER_GROUP_NOT_EXIST


_BASIC_RECIPE = _WorkerGroupRecipe(
    supports_description=False,
)
_DESCRIBED_LEGACY_NOT_FOUND_RECIPE = _WorkerGroupRecipe(
    supports_description=True,
    omitted_description="",
)
_DESCRIBED_MODERN_NOT_FOUND_RECIPE = _WorkerGroupRecipe(
    supports_description=True,
    omitted_description="",
    not_found_code=_WORKER_GROUP_NOT_EXIST,
)


class WorkerGroupAdapter:
    """Compiled-wire worker-group adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _WORKER_GROUP_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> WorkerGroupAdapter:
        """Return the worker-group adapter for one reviewed version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> WorkerGroupDomain:
        """Bind only worker-group operations needed by stable services."""
        programs = _WORKER_GROUP_PROGRAMS.bind(
            self._profile,
            profile,
            http_client=http_client,
        )
        return WorkerGroupDomain(
            worker_groups=cast(
                "WorkerGroupOperations",
                _CompiledWorkerGroupOperations(programs, self._recipe),
            )
        )


WORKER_GROUP_DOMAIN = BoundDomain[WorkerGroupDomain](
    name="worker-group",
    adapter_for_version=WorkerGroupAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledWorkerGroupOperations:
    programs: BoundCompiledPrograms[_WorkerGroupPrimitive]
    recipe: _WorkerGroupRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> WorkerGroupPageRecord:
        page = self.programs.call(
            "page",
            {
                "pageNo": page_no,
                "pageSize": page_size,
                "searchVal": search,
            },
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.ds_version,
            resource="worker-group",
        )
        return cast(
            "WorkerGroupPageRecord",
            project_page(
                page,
                [
                    _worker_group_snapshot(
                        item,
                        ds_version=self.ds_version,
                        supports_description=self.recipe.supports_description,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.ds_version,
                resource="worker-group",
            ),
        )

    def list_all(self) -> Sequence[WorkerGroupRecord]:
        return collect_pages(self.list, resource="worker-group")

    def get(self, *, worker_group_id: int) -> WorkerGroupRecord:
        for worker_group in self.list_all():
            if worker_group.id == worker_group_id:
                return worker_group
        raise ApiResultError(
            result_code=self.recipe.not_found_code,
            result_message=f"worker group {worker_group_id} does not exist",
        )

    def create(
        self,
        *,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroupRecord:
        self._require_description_support(description, operation="create")
        mutation_call(
            lambda: self.programs.call(
                "save",
                self._save_values(
                    worker_group_id=0,
                    name=name,
                    addr_list=addr_list,
                    description=description,
                ),
            ),
            ds_version=self.ds_version,
            resource="worker-group",
            operation="create",
        )
        return self._verified_readback(
            operation="create",
            worker_group_id=None,
            name=name,
            addr_list=addr_list,
            description=self._expected_description(description),
        )

    def update(
        self,
        *,
        worker_group_id: int,
        name: str,
        addr_list: str,
        description: str | None = None,
    ) -> WorkerGroupRecord:
        self._require_description_support(description, operation="update")
        mutation_call(
            lambda: self.programs.call(
                "save",
                self._save_values(
                    worker_group_id=worker_group_id,
                    name=name,
                    addr_list=addr_list,
                    description=description,
                ),
            ),
            ds_version=self.ds_version,
            resource="worker-group",
            operation="update",
        )
        return self._verified_readback(
            operation="update",
            worker_group_id=worker_group_id,
            name=name,
            addr_list=addr_list,
            description=self._expected_description(description),
        )

    def delete(self, *, worker_group_id: int) -> bool:
        mutation_call(
            lambda: self.programs.call("delete", {"id": worker_group_id}),
            ds_version=self.ds_version,
            resource="worker-group",
            operation="delete",
        )

        def verify() -> bool:
            # The exact endpoint declares Void and discards arbitrary successful
            # result-envelope data before deletion is verified by readback.
            if any(
                worker_group.id == worker_group_id for worker_group in self.list_all()
            ):
                message = "Worker-group deletion readback still returned the deleted id"
                raise ApiTransportError(
                    message,
                    details={"worker_group_id": worker_group_id},
                )
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="worker-group",
            operation="delete",
        )

    def _save_values(
        self,
        *,
        worker_group_id: int,
        name: str,
        addr_list: str,
        description: str | None,
    ) -> JsonObject:
        values: JsonObject = {
            "id": worker_group_id,
            "name": name,
            "addrList": addr_list,
        }
        recipe = self.recipe
        if recipe.supports_description:
            values["description"] = description
        return values

    def _require_description_support(
        self,
        description: str | None,
        *,
        operation: str,
    ) -> None:
        if not description or self.recipe.supports_description:
            return
        version = self.ds_version
        message = (
            f"worker-group.{operation} description is unavailable on "
            f"DolphinScheduler {version}."
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "action": f"worker-group.{operation}",
                "selected_version": version,
                "facet": "description",
                "reason": "upstream_capability_absent",
            },
            suggestion=(
                "Retry without --description, or use DolphinScheduler 3.1.0 or newer."
            ),
        )

    def _expected_description(self, description: str | None) -> str | None:
        if not self.recipe.supports_description:
            return None
        if description is None:
            return self.recipe.omitted_description
        return description

    def _verified_readback(
        self,
        *,
        operation: str,
        worker_group_id: int | None,
        name: str,
        addr_list: str,
        description: str | None,
    ) -> WorkerGroupRecord:
        def verify() -> WorkerGroupRecord:
            candidates = collect_pages(
                lambda page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=name,
                ),
                resource="worker-group",
            )
            matches = [
                item
                for item in candidates
                if item.name == name
                and (worker_group_id is None or item.id == worker_group_id)
            ]
            if len(matches) != 1:
                message = (
                    "Worker-group mutation readback did not return one exact match"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "worker_group_id": worker_group_id,
                        "name": name,
                        "match_count": len(matches),
                    },
                )
            refreshed = matches[0]
            if refreshed.addrList != addr_list or refreshed.description != description:
                message = (
                    "Worker-group mutation readback did not match requested fields"
                )
                raise ApiTransportError(
                    message,
                    details={
                        "worker_group_id": worker_group_id,
                        "name": name,
                        "expected_addr_list": addr_list,
                        "actual_addr_list": refreshed.addrList,
                        "expected_description": description,
                        "actual_description": refreshed.description,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="worker-group",
            operation=operation,
        )

    @property
    def ds_version(self) -> str:
        return self.programs.ds_version


_RECIPES = {
    "basic": _BASIC_RECIPE,
    "described_legacy_not_found": _DESCRIBED_LEGACY_NOT_FOUND_RECIPE,
    "described_modern_not_found": _DESCRIBED_MODERN_NOT_FOUND_RECIPE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _WorkerGroupRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled worker-group recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled worker-group recipe is unsupported: {profile.recipe_id!r}"
        raise WireContractError(message) from exc


def _worker_group_snapshot(
    item: OpaqueGeneratedValue,
    *,
    ds_version: str,
    supports_description: bool,
) -> WorkerGroupSnapshot:
    raw_id = response_field(
        item,
        "id",
        ds_version=ds_version,
        resource="worker-group",
    )
    worker_group_id = (
        None
        if raw_id in (None, 0)
        else positive_int(
            raw_id,
            ds_version=ds_version,
            resource="worker-group",
            field="id",
        )
    )
    return WorkerGroupSnapshot(
        id=worker_group_id,
        name=non_empty_text(
            response_field(
                item,
                "name",
                ds_version=ds_version,
                resource="worker-group",
            ),
            ds_version=ds_version,
            resource="worker-group",
            field="name",
        ),
        addrList=optional_text_field(
            item,
            "addrList",
            ds_version=ds_version,
            resource="worker-group",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="worker-group",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="worker-group",
        ),
        description=optional_text_field(
            item,
            "description",
            ds_version=ds_version,
            resource="worker-group",
            missing_is_none=not supports_description,
        ),
        systemDefault=optional_bool_field(
            item,
            "systemDefault",
            ds_version=ds_version,
            resource="worker-group",
        ),
    )


__all__ = [
    "WORKER_GROUP_DOMAIN",
    "WorkerGroupAdapter",
    "WorkerGroupDomain",
    "WorkerGroupSnapshot",
]
