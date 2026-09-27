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
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import (
        QueueLookupOperations,
        QueueOperations,
        QueuePageRecord,
        QueueRecord,
    )


_QUEUE_NOT_EXIST = 10128

_EntityResult = Literal["none", "entity"]
_DeleteResult = Literal["absent", "none", "boolean"]
_QueuePrimitive = Literal["page", "create", "update", "delete"]
_DELETE_ABSENT_VERSIONS = frozenset(
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
    }
)
_QUEUE_EXPECTATIONS: Mapping[_QueuePrimitive, CompiledProgramExpectation] = {
    "page": READ_RETRY_OPTIONAL,
    "create": MUTATION_ONCE_REQUIRED,
    "update": MUTATION_ONCE_REQUIRED,
    "delete": CompiledProgramExpectation(
        mode=MUTATION_ONCE_REQUIRED.mode,
        envelope=MUTATION_ONCE_REQUIRED.envelope,
        absent_versions=_DELETE_ABSENT_VERSIONS,
    ),
}
_QUEUE_PROGRAMS = CompiledDomainPrograms[_QueuePrimitive](
    name="queue",
    schema_constant="COMPILED_QUEUE_SCHEMA_VERSION",
    schema_version=1,
    expectations=_QUEUE_EXPECTATIONS,
)


@dataclass(frozen=True)
class QueueDomain:
    """Caller-oriented exact queue surface."""

    queues: QueueOperations


@dataclass(frozen=True)
class QueueSnapshot:
    """Version-neutral queue projection consumed by stable services."""

    id: int
    queueName: str | None  # noqa: N815
    queue: str | None
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815


@dataclass(frozen=True)
class _QueueRecipe:
    create_result: _EntityResult
    update_result: _EntityResult
    delete_result: _DeleteResult


_VOID_RECIPE = _QueueRecipe(
    create_result="none",
    update_result="none",
    delete_result="absent",
)
_CREATE_ENTITY_RECIPE = _QueueRecipe(
    create_result="entity",
    update_result="none",
    delete_result="absent",
)
_ENTITY_RECIPE = _QueueRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="absent",
)
_VOID_DELETE_RECIPE = _QueueRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="none",
)
_BOOLEAN_RECIPE = _QueueRecipe(
    create_result="entity",
    update_result="entity",
    delete_result="boolean",
)


class QueueAdapter:
    """Compiled queue domain adapter for every reviewed DS profile."""

    def __init__(self, ds_version: str) -> None:
        """Load one exact-version compiled wire-program profile."""
        self._profile = _QUEUE_PROGRAMS.profile(ds_version)
        self._recipe = _recipe_for_profile(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> QueueAdapter:
        """Return the queue adapter for one explicitly reviewed version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> QueueDomain:
        """Bind only the exact queue operations required by queue services."""
        programs = _QUEUE_PROGRAMS.bind(self._profile, profile, http_client=http_client)
        lookup = _CompiledQueueLookup(programs)
        return QueueDomain(
            queues=cast(
                "QueueOperations",
                _CompiledQueueOperations(lookup, self._recipe),
            )
        )


QUEUE_DOMAIN = BoundDomain[QueueDomain](
    name="queue",
    adapter_for_version=QueueAdapter.for_version,
)


@dataclass(frozen=True)
class _CompiledQueueLookup:
    programs: BoundCompiledPrograms[_QueuePrimitive]

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> QueuePageRecord:
        page = self.programs.call(
            "page",
            {"pageNo": page_no, "searchVal": search, "pageSize": page_size},
        )
        items = sequence_field(
            page,
            "totalList",
            ds_version=self.programs.ds_version,
            resource="queue",
        )
        return cast(
            "QueuePageRecord",
            project_page(
                page,
                [
                    _queue_snapshot(
                        item,
                        ds_version=self.programs.ds_version,
                    )
                    for item in items
                ],
                requested_page_no=page_no,
                requested_page_size=page_size,
                ds_version=self.programs.ds_version,
                resource="queue",
            ),
        )

    def list_all(self) -> Sequence[QueueRecord]:
        return collect_pages(self.list, resource="queue")

    def get(self, *, queue_id: int) -> QueueRecord:
        for queue in self.list_all():
            if queue.id == queue_id:
                return queue
        raise ApiResultError(
            result_code=_QUEUE_NOT_EXIST,
            result_message=f"queue {queue_id} does not exist",
        )


@dataclass(frozen=True)
class _CompiledQueueOperations:
    lookup: _CompiledQueueLookup
    recipe: _QueueRecipe

    def list(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None = None,
    ) -> QueuePageRecord:
        return self.lookup.list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )

    def list_all(self) -> Sequence[QueueRecord]:
        return self.lookup.list_all()

    def get(self, *, queue_id: int) -> QueueRecord:
        return self.lookup.get(queue_id=queue_id)

    def create(self, *, queue: str, queue_name: str) -> QueueRecord:
        result = mutation_call(
            lambda: self.lookup.programs.call(
                "create", {"queue": queue, "queueName": queue_name}
            ),
            ds_version=self.ds_version,
            resource="queue",
            operation="create",
        )
        return self._verified_readback(
            operation="create",
            result=result,
            expected_result=self.recipe.create_result,
            queue_id=None,
            queue_name=queue_name,
            queue=queue,
        )

    def update(
        self,
        *,
        queue_id: int,
        queue: str,
        queue_name: str,
    ) -> QueueRecord:
        result = mutation_call(
            lambda: self.lookup.programs.call(
                "update", {"id": queue_id, "queue": queue, "queueName": queue_name}
            ),
            ds_version=self.ds_version,
            resource="queue",
            operation="update",
        )
        return self._verified_readback(
            operation="update",
            result=result,
            expected_result=self.recipe.update_result,
            queue_id=queue_id,
            queue_name=queue_name,
            queue=queue,
        )

    def delete(self, *, queue_id: int) -> bool:
        recipe = self.recipe
        if recipe.delete_result == "absent":
            version = self.ds_version
            message = f"queue.delete is unavailable on DolphinScheduler {version}."
            raise UnsupportedFeatureError(
                message,
                details={
                    "action": "queue.delete",
                    "selected_version": version,
                    "reason": "upstream_capability_absent",
                },
                suggestion=("Queue deletion requires DolphinScheduler 3.2.0 or newer."),
            )
        result = mutation_call(
            lambda: self.lookup.programs.call("delete", {"id": queue_id}),
            ds_version=self.ds_version,
            resource="queue",
            operation="delete",
        )

        def verify() -> bool:
            _require_delete_result(
                result,
                expected=cast(
                    "Literal['none', 'boolean']",
                    recipe.delete_result,
                ),
                ds_version=self.ds_version,
            )
            if any(queue.id == queue_id for queue in self.list_all()):
                message = "Queue deletion readback still returned the deleted id"
                raise ApiTransportError(
                    message,
                    details={"queue_id": queue_id},
                )
            return True

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="queue",
            operation="delete",
        )

    def _verified_readback(
        self,
        *,
        operation: str,
        result: OpaqueGeneratedValue,
        expected_result: _EntityResult,
        queue_id: int | None,
        queue_name: str,
        queue: str,
    ) -> QueueRecord:
        def verify() -> QueueRecord:
            _require_entity_result(
                result,
                expected=expected_result,
                operation=operation,
                ds_version=self.ds_version,
            )
            candidates = collect_pages(
                lambda page_no, page_size: self.list(
                    page_no=page_no,
                    page_size=page_size,
                    search=queue_name,
                ),
                resource="queue",
            )
            matches = [
                item
                for item in candidates
                if item.queueName == queue_name
                and (queue_id is None or item.id == queue_id)
            ]
            if len(matches) != 1:
                message = "Queue mutation readback did not return one exact match"
                raise ApiTransportError(
                    message,
                    details={
                        "queue_id": queue_id,
                        "queue_name": queue_name,
                        "match_count": len(matches),
                    },
                )
            refreshed = matches[0]
            if refreshed.queue != queue:
                message = "Queue mutation readback did not match requested fields"
                raise ApiTransportError(
                    message,
                    details={
                        "queue_id": queue_id,
                        "queue_name": queue_name,
                        "expected_queue": queue,
                        "actual_queue": refreshed.queue,
                    },
                )
            return refreshed

        return verify_mutation(
            verify,
            ds_version=self.ds_version,
            resource="queue",
            operation=operation,
        )

    @property
    def ds_version(self) -> str:
        return self.lookup.programs.ds_version


def bind_queue_lookup(
    profile: ClusterProfile,
    *,
    http_client: DolphinSchedulerClient,
) -> QueueLookupOperations:
    """Bind the shared exact queue paging recipe to a lookup-only port."""
    programs = _QUEUE_PROGRAMS.bind(
        _QUEUE_PROGRAMS.profile(profile.ds_version),
        profile,
        http_client=http_client,
    )
    return cast("QueueLookupOperations", _CompiledQueueLookup(programs))


_RECIPES = {
    "legacy": _VOID_RECIPE,
    "void": _VOID_RECIPE,
    "create_entity": _CREATE_ENTITY_RECIPE,
    "entity": _ENTITY_RECIPE,
    "void_delete": _VOID_DELETE_RECIPE,
    "boolean": _BOOLEAN_RECIPE,
}


def _recipe_for_profile(profile: CompiledWireProfile) -> _QueueRecipe:
    if profile.status != "supported" or profile.recipe_id is None:
        message = f"DS {profile.ds_version} has no compiled queue recipe"
        raise WireContractError(message)
    try:
        return _RECIPES[profile.recipe_id]
    except KeyError as exc:
        message = f"Compiled queue recipe {profile.recipe_id!r} is unsupported"
        raise WireContractError(message) from exc


def _queue_snapshot(item: OpaqueGeneratedValue, *, ds_version: str) -> QueueSnapshot:
    return QueueSnapshot(
        id=positive_int(
            response_field(item, "id", ds_version=ds_version, resource="queue"),
            ds_version=ds_version,
            resource="queue",
            field="id",
        ),
        queueName=optional_text_field(
            item,
            "queueName",
            ds_version=ds_version,
            resource="queue",
        ),
        queue=optional_text_field(
            item,
            "queue",
            ds_version=ds_version,
            resource="queue",
        ),
        createTime=optional_text_field(
            item,
            "createTime",
            ds_version=ds_version,
            resource="queue",
        ),
        updateTime=optional_text_field(
            item,
            "updateTime",
            ds_version=ds_version,
            resource="queue",
        ),
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
        reason = "legacy void mutation returned a non-null payload"
    elif result is not None:
        return
    else:
        reason = "entity-returning mutation returned a null payload"
    raise projection_error(
        ds_version=ds_version,
        resource="queue",
        field=f"{operation}Result",
        reason=reason,
    )


def _require_delete_result(
    result: OpaqueGeneratedValue,
    *,
    expected: Literal["none", "boolean"],
    ds_version: str,
) -> None:
    if expected == "none" and result is None:
        return
    if expected == "boolean" and result is True:
        return
    raise projection_error(
        ds_version=ds_version,
        resource="queue",
        field="deleteResult",
        reason="delete result did not confirm success",
    )


__all__ = [
    "QUEUE_DOMAIN",
    "QueueAdapter",
    "QueueDomain",
    "QueueSnapshot",
    "bind_queue_lookup",
]
