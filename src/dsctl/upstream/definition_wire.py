from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar

from dsctl.cli_surface import PROJECT_RESOURCE, SCHEDULE_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeIdentity,
    ProjectRef,
    ProjectView,
    ScheduleView,
    WorkflowListView,
    WorkflowRef,
    WorkflowView,
    schedule_has_missed_fire_policy,
)
from dsctl.upstream.definition_reads import WirePage
from dsctl.upstream.protocols.design import ScheduleMissedFireRecord
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.upstream.protocol import (
        ProjectPageRecord,
        ProjectPayloadRecord,
        ProjectReadOperations,
        SchedulePageRecord,
        SchedulePayloadRecord,
        ScheduleReadOperations,
        WorkflowListRecord,
        WorkflowPageRecord,
        WorkflowPayloadRecord,
        WorkflowReadOperations,
        WorkflowRecord,
    )


@dataclass(frozen=True)
class CodeDefinitionWire:
    """Adapt existing code-native operations to the deep read recipe."""

    projects: ProjectReadOperations
    workflows: WorkflowReadOperations
    schedules: ScheduleReadOperations

    @property
    def identity_kind(self) -> Literal["code"]:
        """Return the native identity kind for DS 2.x and newer."""
        return "code"

    def project_page(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[ProjectView]:
        """Project one code-native project page."""
        page = self.projects.list(
            page_no=page_no,
            page_size=page_size,
            search=search,
        )
        return _page(
            page,
            tuple(_project_view(item) for item in page.totalList or ()),
            requested_page_no=page_no,
            requested_page_size=page_size,
        )

    def project_detail(self, native: NativeIdentity) -> ProjectView:
        """Load one code-native project detail."""
        code = _require_native_code(native, resource=PROJECT_RESOURCE)
        return _project_view(self.projects.get(code=code))

    def workflow_refs(self, project: ProjectRef) -> Sequence[WorkflowRef]:
        """Project code-native simple-list rows."""
        project_code = _require_native_code(
            project.native,
            resource=PROJECT_RESOURCE,
        )
        return tuple(
            _workflow_ref(item)
            for item in self.workflows.list_refs(project_code=project_code)
        )

    def workflow_page(
        self,
        project: ProjectRef,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[WorkflowListView]:
        """Project one code-native workflow page."""
        project_code = _require_native_code(
            project.native,
            resource=PROJECT_RESOURCE,
        )
        page = self.workflows.list_page(
            project_code=project_code,
            page_no=page_no,
            page_size=page_size,
            search=search,
        )
        return _page(
            page,
            tuple(_workflow_list_view(item) for item in page.totalList or ()),
            requested_page_no=page_no,
            requested_page_size=page_size,
        )

    def workflow_detail(
        self,
        project: ProjectRef,
        workflow: NativeIdentity,
    ) -> WorkflowView:
        """Load and project one code-native workflow detail."""
        project_code = _require_native_code(
            project.native,
            resource=PROJECT_RESOURCE,
        )
        workflow_code = _require_native_code(workflow, resource=WORKFLOW_RESOURCE)
        return _workflow_view(
            self.workflows.get(
                project_code=project_code,
                code=workflow_code,
            )
        )

    def schedule_page(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
        *,
        page_no: int,
        page_size: int,
    ) -> WirePage[ScheduleView]:
        """Project schedules filtered by one workflow code."""
        project_code = _require_native_code(
            project.native,
            resource=PROJECT_RESOURCE,
        )
        workflow_code = _require_native_code(
            workflow.native,
            resource=WORKFLOW_RESOURCE,
        )
        page = self.schedules.list(
            project_code=project_code,
            workflow_code=workflow_code,
            search=None,
            page_no=page_no,
            page_size=page_size,
        )
        return _page(
            page,
            tuple(_schedule_view(item) for item in page.totalList or ()),
            requested_page_no=page_no,
            requested_page_size=page_size,
        )


PageItemT = TypeVar("PageItemT")


def _page(
    page: ProjectPageRecord | WorkflowPageRecord | SchedulePageRecord,
    items: tuple[PageItemT, ...],
    *,
    requested_page_no: int,
    requested_page_size: int,
) -> WirePage[PageItemT]:
    return WirePage(
        totalList=items,
        total=page.total,
        totalPage=page.totalPage,
        pageSize=(page.pageSize if page.pageSize is not None else requested_page_size),
        currentPage=(
            page.currentPage if page.currentPage is not None else requested_page_no
        ),
        pageNo=page.pageNo if page.pageNo is not None else requested_page_no,
    )


def _project_view(project: ProjectPayloadRecord) -> ProjectView:
    code = _positive_identity(
        project.code,
        resource=PROJECT_RESOURCE,
        field="code",
    )
    return ProjectView(
        ref=ProjectRef(
            native=NativeCode(code),
            name=project.name,
            description=project.description,
        ),
        id=project.id,
        user_id=project.userId,
        user_name=project.userName,
        create_time=project.createTime,
        update_time=project.updateTime,
        perm=project.perm,
        definition_count=project.defCount,
    )


def _workflow_ref(workflow: WorkflowRecord) -> WorkflowRef:
    return WorkflowRef(
        native=NativeCode(
            _positive_identity(
                workflow.code,
                resource=WORKFLOW_RESOURCE,
                field="code",
            )
        ),
        name=workflow.name,
        version=workflow.version,
    )


def _workflow_list_view(workflow: WorkflowListRecord) -> WorkflowListView:
    schedule = workflow.schedule
    return WorkflowListView(
        ref=_workflow_ref(workflow),
        release_state=enum_value(workflow.releaseState),
        schedule_release_state=enum_value(workflow.scheduleReleaseState),
        schedule_id=None if schedule is None else schedule.id,
    )


def _workflow_view(workflow: WorkflowPayloadRecord) -> WorkflowView:
    return WorkflowView(
        ref=_workflow_ref(workflow),
        project_native=NativeCode(
            _positive_identity(
                workflow.projectCode,
                resource=WORKFLOW_RESOURCE,
                field="projectCode",
            )
        ),
        id=workflow.id,
        description=workflow.description,
        global_params=workflow.globalParams,
        global_param_map=workflow.globalParamMap,
        create_time=workflow.createTime,
        update_time=workflow.updateTime,
        user_id=workflow.userId,
        user_name=workflow.userName,
        project_name=workflow.projectName,
        timeout=workflow.timeout,
        release_state=enum_value(workflow.releaseState),
        execution_type=enum_value(workflow.executionType),
        include_execution_type=True,
        tenant_id=_workflow_tenant_id(workflow),
        tenant_code=_workflow_tenant_code(workflow),
    )


def _workflow_tenant_id(workflow: WorkflowPayloadRecord) -> int | None:
    value = getattr(workflow, "tenantId", None)
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _workflow_tenant_code(workflow: WorkflowPayloadRecord) -> str | None:
    value = getattr(workflow, "tenantCode", None)
    if isinstance(value, str) and value.strip():
        return value
    return None


def _schedule_view(schedule: SchedulePayloadRecord) -> ScheduleView:
    return ScheduleView(
        id=schedule.id,
        workflow_native=NativeCode(
            _positive_identity(
                schedule.workflowDefinitionCode,
                resource=SCHEDULE_RESOURCE,
                field="workflowDefinitionCode",
            )
        ),
        workflow_name=schedule.workflowDefinitionName,
        project_name=schedule.projectName,
        start_time=schedule.startTime,
        end_time=schedule.endTime,
        timezone_id=schedule.timezoneId,
        crontab=schedule.crontab,
        failure_strategy=enum_value(schedule.failureStrategy),
        workflow_instance_priority=enum_value(schedule.workflowInstancePriority),
        release_state=enum_value(schedule.releaseState),
        has_missed_fire_policy=schedule_has_missed_fire_policy(schedule),
        missed_fire_policy=(
            enum_value(schedule.missedFirePolicy)
            if isinstance(schedule, ScheduleMissedFireRecord)
            else None
        ),
    )


def _require_native_code(native: NativeIdentity, *, resource: str) -> int:
    if isinstance(native, NativeCode):
        return native.value
    message = f"Code-native {resource} wire received an id identity"
    raise WireContractError(message)


def _positive_identity(value: int | None, *, resource: str, field: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    message = f"DolphinScheduler {resource} payload contained an invalid {field}"
    raise ApiTransportError(
        message,
        details={"resource": resource, "field": field},
    )


__all__ = ["CodeDefinitionWire"]
