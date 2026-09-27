from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from dsctl.errors import UnsupportedFeatureError
from dsctl.upstream.bound_domain import BoundDomain, BoundDomainAdapter
from dsctl.upstream.compiled_domain import (
    READ_RETRY_OPTIONAL,
    BoundCompiledPrograms,
    CompiledDomainPrograms,
)
from dsctl.upstream.response_projection import (
    optional_text_field,
    projection_error,
    response_field,
)
from dsctl.upstream.wire import CompiledWireProfile, WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.protocol import TaskTypeOperations, TaskTypeRecord


_TASK_TYPE_RESOURCE = "task-type"
_TASK_TYPE_INTRODUCED_IN = "3.1.0"
_TaskTypePrimitive = Literal["list"]
_TASK_TYPE_PROGRAMS = CompiledDomainPrograms[_TaskTypePrimitive](
    name="task_type",
    schema_constant="COMPILED_TASK_TYPE_SCHEMA_VERSION",
    schema_version=1,
    expectations={"list": READ_RETRY_OPTIONAL},
)


@dataclass(frozen=True)
class TaskTypeDomain:
    """Caller-oriented task-type catalog surface."""

    task_types: TaskTypeOperations


@dataclass(frozen=True)
class TaskTypeSnapshot:
    """Version-neutral projection of one exact upstream FavTaskDto."""

    taskType: str | None  # noqa: N815
    isCollection: bool  # noqa: N815
    taskCategory: str | None  # noqa: N815


class TaskTypeAdapter:
    """Compiled favourite-task adapter for every supporting exact profile."""

    def __init__(self, ds_version: str) -> None:
        """Select the exact compiled program and reviewed category projection."""
        self._profile = _TASK_TYPE_PROGRAMS.profile(ds_version)
        if self._profile.status != "supported":
            message = f"DS {ds_version} has no reviewed generated task-type recipe"
            raise WireContractError(message)
        self._category_available = _category_available(self._profile)
        self.ds_version = ds_version

    @classmethod
    def for_version(cls, ds_version: str) -> TaskTypeAdapter:
        """Return the exact generated adapter for a supporting version."""
        return cls(ds_version)

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskTypeDomain:
        """Bind the stable task-type discovery operations."""
        return TaskTypeDomain(
            task_types=cast(
                "TaskTypeOperations",
                _CompiledTaskTypeOperations(
                    _TASK_TYPE_PROGRAMS.bind(
                        self._profile, profile, http_client=http_client
                    ),
                    self._category_available,
                ),
            )
        )


@dataclass(frozen=True)
class _AbsentTaskTypeAdapter:
    ds_version: str

    def bind(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> TaskTypeDomain:
        del profile, http_client
        message = (
            "Favourite task-type discovery does not exist in DolphinScheduler "
            f"{self.ds_version}"
        )
        raise UnsupportedFeatureError(
            message,
            details={
                "resource": _TASK_TYPE_RESOURCE,
                "ds_version": self.ds_version,
                "reason": "upstream_capability_absent",
                "introduced_in": _TASK_TYPE_INTRODUCED_IN,
            },
            suggestion=(
                "Use `dsctl template task` for the source-derived local task "
                "catalog, or DolphinScheduler 3.1.0+ for live favourite flags."
            ),
        )


def _adapter_for_version(ds_version: str) -> BoundDomainAdapter[TaskTypeDomain]:
    if _TASK_TYPE_PROGRAMS.profile(ds_version).status == "upstream_absent":
        return _AbsentTaskTypeAdapter(ds_version)
    return TaskTypeAdapter.for_version(ds_version)


TASK_TYPE_DOMAIN = BoundDomain[TaskTypeDomain](
    name=_TASK_TYPE_RESOURCE,
    adapter_for_version=_adapter_for_version,
)


@dataclass(frozen=True)
class _CompiledTaskTypeOperations:
    programs: BoundCompiledPrograms[_TaskTypePrimitive]
    category_available: bool

    def list(self) -> Sequence[TaskTypeRecord]:
        """Return the exact server catalog projected into stable snapshots."""
        payload = self.programs.call("list", {})
        if not isinstance(payload, list):
            raise projection_error(
                ds_version=self.programs.ds_version,
                resource=_TASK_TYPE_RESOURCE,
                field="taskTypes",
                reason="response is not a list",
            )
        return cast(
            "Sequence[TaskTypeRecord]",
            [
                _task_type_snapshot(
                    item,
                    ds_version=self.programs.ds_version,
                    category_available=self.category_available,
                )
                for item in payload
            ],
        )


def _task_type_snapshot(
    item: OpaqueGeneratedValue, *, ds_version: str, category_available: bool
) -> TaskTypeSnapshot:
    collection = response_field(
        item,
        "isCollection",
        ds_version=ds_version,
        resource=_TASK_TYPE_RESOURCE,
    )
    if not isinstance(collection, bool):
        raise projection_error(
            ds_version=ds_version,
            resource=_TASK_TYPE_RESOURCE,
            field="isCollection",
            reason="field is not a boolean",
        )
    # In the legacy DTO, TaskTypeConfiguration puts the task name in taskName
    # and the category constant in taskType; 3.2.0 renames both fields.
    return TaskTypeSnapshot(
        taskType=optional_text_field(
            item,
            "taskType" if category_available else "taskName",
            ds_version=ds_version,
            resource=_TASK_TYPE_RESOURCE,
        ),
        isCollection=collection,
        taskCategory=optional_text_field(
            item,
            "taskCategory" if category_available else "taskType",
            ds_version=ds_version,
            resource=_TASK_TYPE_RESOURCE,
        ),
    )


def _category_available(profile: CompiledWireProfile) -> bool:
    if profile.recipe_id == "legacy":
        return False
    if profile.recipe_id == "category":
        return True
    message = f"Compiled task-type recipe {profile.recipe_id!r} is unsupported"
    raise WireContractError(message)


__all__ = [
    "TASK_TYPE_DOMAIN",
    "TaskTypeAdapter",
    "TaskTypeDomain",
    "TaskTypeSnapshot",
]
