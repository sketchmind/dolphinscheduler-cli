from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
    runtime_checkable,
)

from dsctl.support.json_types import require_json_object
from dsctl.upstream.protocols.design import ScheduleMissedFireRecord

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.pagination import PageCoverage
    from dsctl.upstream.protocols.design import ScheduleRecord


@dataclass(frozen=True)
class NativeId:
    """A DolphinScheduler identity whose native route key is an integer id."""

    value: int


@dataclass(frozen=True)
class NativeCode:
    """A DolphinScheduler identity whose native route key is an integer code."""

    value: int


NativeIdentity: TypeAlias = NativeId | NativeCode


@dataclass(frozen=True)
class ProjectRef:
    """Caller-facing project identity without an invented id/code alias."""

    native: NativeIdentity
    name: str | None
    description: str | None

    def to_data(self) -> JsonObject:
        """Render only the identity kind native to this DS profile."""
        return {
            _identity_field(self.native): self.native.value,
            "name": self.name,
            "description": self.description,
        }


@dataclass(frozen=True)
class WorkflowRef:
    """Caller-facing workflow identity scoped by one resolved project."""

    native: NativeIdentity
    name: str | None
    version: int | None

    def to_data(self) -> JsonObject:
        """Render only the identity kind native to this DS profile."""
        return {
            _identity_field(self.native): self.native.value,
            "name": self.name,
            "version": self.version,
        }


@dataclass(frozen=True)
class ProjectView:
    """Stable project read projection independent of generated models."""

    ref: ProjectRef
    id: int | None
    user_id: int | None
    user_name: str | None
    create_time: str | None
    update_time: str | None
    perm: int
    definition_count: int

    def to_data(self) -> JsonObject:
        """Render a project while preserving its native identity vocabulary."""
        common: JsonObject = {
            "id": self.id,
            "userId": self.user_id,
            "userName": self.user_name,
        }
        if isinstance(self.ref.native, NativeCode):
            common["code"] = self.ref.native.value
        common.update(
            {
                "name": self.ref.name,
                "description": self.ref.description,
                "createTime": self.create_time,
                "updateTime": self.update_time,
                "perm": self.perm,
                "defCount": self.definition_count,
            }
        )
        return common


@runtime_checkable
class _ProjectedSchedulePolicy(Protocol):
    @property
    def has_missed_fire_policy(self) -> bool:
        """Whether the exact projection includes this native field."""


def schedule_has_missed_fire_policy(schedule: ScheduleRecord) -> bool:
    """Respect a projected exact capability before native structural discovery."""
    if isinstance(schedule, _ProjectedSchedulePolicy):
        return schedule.has_missed_fire_policy
    return isinstance(schedule, ScheduleMissedFireRecord)


@dataclass(frozen=True)
class ScheduleView:
    """Attached-schedule projection used below the CLI output boundary."""

    id: int | None
    workflow_native: NativeIdentity
    workflow_name: str | None
    project_name: str | None
    start_time: str | None
    end_time: str | None
    timezone_id: str | None
    crontab: str | None
    failure_strategy: str | None
    workflow_instance_priority: str | None
    release_state: str | None
    missed_fire_policy: str | None = None
    has_missed_fire_policy: bool = False

    def to_data(self) -> JsonObject:
        """Render the stable attached-schedule subset."""
        data: JsonObject = {
            "id": self.id,
            "startTime": self.start_time,
            "endTime": self.end_time,
            "timezoneId": self.timezone_id,
            "crontab": self.crontab,
            "failureStrategy": self.failure_strategy,
            "workflowInstancePriority": self.workflow_instance_priority,
            "releaseState": self.release_state,
        }
        if self.has_missed_fire_policy:
            data["missedFirePolicy"] = self.missed_fire_policy
        return data


@dataclass(frozen=True)
class WorkflowListView:
    """Stable workflow list row for either id- or code-native profiles."""

    ref: WorkflowRef
    release_state: str | None
    schedule_release_state: str | None
    schedule_id: int | None

    def to_data(self) -> JsonObject:
        """Render the list row without fabricating a cross-version code."""
        return {
            _identity_field(self.ref.native): self.ref.native.value,
            "name": self.ref.name,
            "version": self.ref.version,
            "releaseState": self.release_state,
            "scheduleReleaseState": self.schedule_release_state,
            "scheduleId": self.schedule_id,
        }


@dataclass(frozen=True)
class WorkflowView:
    """Stable workflow detail projected from one exact version dialect."""

    ref: WorkflowRef
    project_native: NativeIdentity
    id: int | None
    description: str | None
    global_params: str | None
    global_param_map: Mapping[str, str | None] | None
    create_time: str | None
    update_time: str | None
    user_id: int
    user_name: str | None
    project_name: str | None
    timeout: int
    release_state: str | None
    execution_type: str | None
    include_execution_type: bool
    receivers: str | None = None
    receivers_cc: str | None = None
    tenant_id: int | None = None
    tenant_code: str | None = None

    def to_data(self, *, attached_schedule: ScheduleView | None) -> JsonObject:
        """Render detail and authoritative attached-schedule state."""
        data: JsonObject = {"id": self.id}
        if isinstance(self.ref.native, NativeCode):
            data["code"] = self.ref.native.value
        data.update(
            {
                "name": self.ref.name,
                "version": self.ref.version,
                _project_identity_field(self.project_native): (
                    self.project_native.value
                ),
                "description": self.description,
                "globalParams": self.global_params,
                "globalParamMap": None
                if self.global_param_map is None
                else dict(self.global_param_map),
                "createTime": self.create_time,
                "updateTime": self.update_time,
                "userId": self.user_id,
                "userName": self.user_name,
                "projectName": self.project_name,
                "timeout": self.timeout,
                "releaseState": self.release_state,
                "scheduleReleaseState": (
                    None
                    if attached_schedule is None
                    else attached_schedule.release_state
                ),
            }
        )
        if self.include_execution_type:
            data["executionType"] = self.execution_type
        data["schedule"] = (
            None if attached_schedule is None else attached_schedule.to_data()
        )
        return data


ItemT = TypeVar("ItemT")


@dataclass(frozen=True)
class DefinitionPage(Generic[ItemT]):
    """Canonical page with caller-request metadata restored when DS omits it."""

    totalList: tuple[ItemT, ...]  # noqa: N815
    total: int
    totalPage: int  # noqa: N815
    pageSize: int  # noqa: N815
    currentPage: int  # noqa: N815
    pageNo: int  # noqa: N815
    coverage: PageCoverage | None = None

    def to_data(self, serialize_item: Callable[[ItemT], JsonObject]) -> JsonObject:
        """Render a DS-style page at the output boundary."""
        data: JsonObject = {
            "totalList": [serialize_item(item) for item in self.totalList],
            "total": self.total,
            "totalPage": self.totalPage,
            "pageSize": self.pageSize,
            "currentPage": self.currentPage,
            "pageNo": self.pageNo,
        }
        if self.coverage is not None:
            data["coverage"] = require_json_object(
                self.coverage,
                label="definition page coverage",
            )
        return data


@dataclass(frozen=True)
class ProjectRead:
    """One resolved project reference and authoritative detail view."""

    project: ProjectRef
    view: ProjectView


@dataclass(frozen=True)
class WorkflowListing:
    """One resolved project and its requested workflow page."""

    project: ProjectRef
    page: DefinitionPage[WorkflowListView]


@dataclass(frozen=True)
class WorkflowRead:
    """One fully resolved workflow detail including schedule state."""

    project: ProjectRef
    workflow: WorkflowRef
    view: WorkflowView
    attached_schedule: ScheduleView | None


@dataclass(frozen=True)
class WorkflowScope:
    """One project-scoped workflow detail without an attached-schedule read."""

    project: ProjectRef
    workflow: WorkflowRef
    view: WorkflowView


def same_native_identity(left: NativeIdentity, right: NativeIdentity) -> bool:
    """Return true only for equal values of the same native identity kind."""
    return type(left) is type(right) and left.value == right.value


def _identity_field(native: NativeIdentity) -> str:
    return "id" if isinstance(native, NativeId) else "code"


def _project_identity_field(native: NativeIdentity) -> str:
    return "projectId" if isinstance(native, NativeId) else "projectCode"


__all__ = [
    "DefinitionPage",
    "NativeCode",
    "NativeId",
    "NativeIdentity",
    "ProjectRead",
    "ProjectRef",
    "ProjectView",
    "ScheduleView",
    "WorkflowListView",
    "WorkflowListing",
    "WorkflowRead",
    "WorkflowRef",
    "WorkflowScope",
    "WorkflowView",
    "same_native_identity",
]
