from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from dsctl.upstream.protocols.base import StringEnumValue
    from dsctl.upstream.protocols.design import ScheduleRecord


RecordT_co = TypeVar("RecordT_co", covariant=True)


@dataclass(frozen=True)
class ReadPage(Generic[RecordT_co]):
    """Canonical paging metadata produced by version-specific read dialects."""

    totalList: Sequence[RecordT_co] | None  # noqa: N815
    total: int | None
    totalPage: int | None  # noqa: N815
    pageSize: int | None  # noqa: N815
    currentPage: int | None  # noqa: N815
    pageNo: int | None  # noqa: N815


@dataclass(frozen=True)
class WorkflowReference:
    """Canonical identity returned by a permission-checked workflow lookup."""

    code: int | None
    name: str | None
    version: int | None = None


@dataclass(frozen=True)
class CanonicalSchedule:
    """Version-neutral schedule fields consumed by stable read services."""

    id: int | None
    workflowDefinitionCode: int  # noqa: N815
    workflowDefinitionName: str | None  # noqa: N815
    projectName: str | None  # noqa: N815
    definitionDescription: str | None  # noqa: N815
    startTime: str | None  # noqa: N815
    endTime: str | None  # noqa: N815
    timezoneId: str | None  # noqa: N815
    crontab: str | None
    failureStrategy: StringEnumValue | None  # noqa: N815
    warningType: StringEnumValue | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    userId: int  # noqa: N815
    userName: str | None  # noqa: N815
    releaseState: StringEnumValue | None  # noqa: N815
    warningGroupId: int  # noqa: N815
    workflowInstancePriority: StringEnumValue | None  # noqa: N815
    workerGroup: str | None  # noqa: N815
    tenantCode: str | None  # noqa: N815
    environmentCode: int | None  # noqa: N815
    environmentName: str | None  # noqa: N815


@dataclass(frozen=True)
class WorkflowScalarMetadata:
    """Authoritative instance scalars overlaid on a definition-backed DAG."""

    global_params: str
    global_param_map: dict[str, str | None]
    timeout: int


@dataclass(frozen=True)
class CanonicalWorkflow:
    """Version-neutral workflow fields consumed by stable read services."""

    id: int | None
    code: int
    name: str | None
    version: int | None
    projectCode: int  # noqa: N815
    description: str | None
    globalParams: str | None  # noqa: N815
    globalParamMap: Mapping[str, str | None] | None  # noqa: N815
    createTime: str | None  # noqa: N815
    updateTime: str | None  # noqa: N815
    userId: int  # noqa: N815
    userName: str | None  # noqa: N815
    projectName: str | None  # noqa: N815
    timeout: int
    releaseState: StringEnumValue | None  # noqa: N815
    scheduleReleaseState: StringEnumValue | None  # noqa: N815
    schedule: ScheduleRecord | None
    executionType: StringEnumValue | None  # noqa: N815
    tenantId: int | None = None  # noqa: N815
    tenantCode: str | None = None  # noqa: N815


__all__ = [
    "CanonicalSchedule",
    "CanonicalWorkflow",
    "ReadPage",
    "WorkflowReference",
    "WorkflowScalarMetadata",
]
