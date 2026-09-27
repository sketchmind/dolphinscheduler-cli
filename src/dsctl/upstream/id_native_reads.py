from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar, cast

from dsctl.cli_surface import PROJECT_RESOURCE, WORKFLOW_RESOURCE
from dsctl.errors import ApiTransportError
from dsctl.upstream._compiled_project import PROJECT_PROGRAMS, ProjectPrimitive
from dsctl.upstream._compiled_workflow_runtime import (
    WORKFLOW_PROGRAMS,
    WorkflowPrimitive,
)
from dsctl.upstream.definition_models import (
    NativeId,
    NativeIdentity,
    ProjectRef,
    ProjectView,
    ScheduleView,
    WorkflowListView,
    WorkflowRef,
    WorkflowView,
)
from dsctl.upstream.definition_reads import DefinitionReads, WirePage
from dsctl.upstream.serialization import enum_value
from dsctl.upstream.wire import WireContractError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from dsctl.client import DolphinSchedulerClient
    from dsctl.config import ClusterProfile
    from dsctl.upstream._generated_types import OpaqueGeneratedValue
    from dsctl.upstream.compiled_domain import BoundCompiledPrograms
    from dsctl.upstream.protocol import ReadUpstreamSession, StringEnumValue


class IdNativeReadAdapter:
    """Exact generated adapter for the DS 1.3.9 id-native read dialect."""

    ds_version = "1.3.9"
    version_slug = "ds_1_3_9"

    def __init__(self) -> None:
        """Load the exact id-native compiled profile without remote I/O."""
        self._compiled = WORKFLOW_PROGRAMS.profile(self.ds_version)

    def bind_read(
        self,
        profile: ClusterProfile,
        *,
        http_client: DolphinSchedulerClient,
    ) -> ReadUpstreamSession:
        """Bind the id-native wire below the deep read module."""
        return cast(
            "ReadUpstreamSession",
            _IdNativeReadSession(
                definitions=DefinitionReads(
                    _IdNativeDefinitionWire(
                        WORKFLOW_PROGRAMS.bind(
                            self._compiled, profile, http_client=http_client
                        ),
                        PROJECT_PROGRAMS.bind(
                            PROJECT_PROGRAMS.profile(self.ds_version),
                            profile,
                            http_client=http_client,
                        ),
                    )
                )
            ),
        )


@dataclass(frozen=True)
class _IdNativeReadSession:
    definitions: DefinitionReads


@dataclass(frozen=True)
class _IdNativeDefinitionWire:
    programs: BoundCompiledPrograms[WorkflowPrimitive]
    projects: BoundCompiledPrograms[ProjectPrimitive]

    @property
    def identity_kind(self) -> Literal["id"]:
        """Return the native identity kind for DS 1.3.9."""
        return "id"

    def project_page(
        self,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[ProjectView]:
        """Return the authenticated user's visible project page."""
        page = self.projects.call(
            "page", {"searchVal": search, "pageSize": page_size, "pageNo": page_no}
        )
        return _page(
            page,
            project=_project_view,
            requested_page_no=page_no,
            requested_page_size=page_size,
            resource="project.page",
        )

    def project_detail(self, native: NativeIdentity) -> ProjectView:
        """Load project detail only after the recipe's visibility check."""
        project_id = _require_native_id(native, resource=PROJECT_RESOURCE)
        return _project_view(self.projects.call("get", {"projectId": project_id}))

    def workflow_refs(self, project: ProjectRef) -> Sequence[WorkflowRef]:
        """Return project-scoped references; the id recipe uses paging for safety."""
        project_name = _require_project_name(project)
        payload = self.programs.call("definition_refs", {"projectName": project_name})
        if not isinstance(payload, list):
            raise _projection_error(
                resource="workflow.ref",
                field="data",
                reason="generated reference payload is not a list",
            )
        return tuple(_workflow_ref(item) for item in payload)

    def workflow_page(
        self,
        project: ProjectRef,
        *,
        page_no: int,
        page_size: int,
        search: str | None,
    ) -> WirePage[WorkflowListView]:
        """Return workflows from the permission-checked project route."""
        project_name = _require_project_name(project)
        page = self.programs.call(
            "definition_page",
            {
                "projectName": project_name,
                "pageNo": page_no,
                "searchVal": search,
                "userId": None,
                "pageSize": page_size,
            },
        )
        return _page(
            page,
            project=_workflow_list_view,
            requested_page_no=page_no,
            requested_page_size=page_size,
            resource="workflow.page",
        )

    def workflow_detail(
        self,
        project: ProjectRef,
        workflow: NativeIdentity,
    ) -> WorkflowView:
        """Load id-native detail after scoped discovery."""
        workflow_id = _require_native_id(workflow, resource=WORKFLOW_RESOURCE)
        project_name = _require_project_name(project)
        return _workflow_view(
            self.programs.call(
                "definition_get",
                {"projectName": project_name, "processId": workflow_id},
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
        """Load schedules by the already scope-checked process id."""
        workflow_id = _require_native_id(
            workflow.native,
            resource=WORKFLOW_RESOURCE,
        )
        project_name = _require_project_name(project)
        page = self.programs.call(
            "schedule_page",
            {
                "projectName": project_name,
                "processDefinitionId": workflow_id,
                "searchVal": None,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        return _page(
            page,
            project=_schedule_view,
            requested_page_no=page_no,
            requested_page_size=page_size,
            resource="schedule.page",
        )


PageItemT = TypeVar("PageItemT")


def _page(
    page: OpaqueGeneratedValue,
    *,
    project: Callable[[OpaqueGeneratedValue], PageItemT],
    requested_page_no: int,
    requested_page_size: int,
    resource: str,
) -> WirePage[PageItemT]:
    items = _field(page, "totalList", resource=resource)
    if items is not None and not isinstance(items, list):
        raise _projection_error(
            resource=resource,
            field="totalList",
            reason="generated page items are not a list",
        )
    return WirePage(
        totalList=tuple(project(item) for item in items or ()),
        total=cast("int | None", _field(page, "total", resource=resource)),
        totalPage=cast(
            "int | None",
            _field(page, "totalPage", resource=resource),
        ),
        # 1.3.9's controller omits request pagination from the response map.
        pageSize=requested_page_size,
        currentPage=cast(
            "int | None",
            _field(page, "currentPage", resource=resource),
        ),
        pageNo=requested_page_no,
    )


def _project_view(project: OpaqueGeneratedValue) -> ProjectView:
    project_id = _positive_identity(
        _field(project, "id", resource="project"),
        resource="project",
        field="id",
    )
    return ProjectView(
        ref=ProjectRef(
            native=NativeId(project_id),
            name=cast("str | None", _field(project, "name", resource="project")),
            description=cast(
                "str | None",
                _field(project, "description", resource="project"),
            ),
        ),
        id=project_id,
        user_id=cast("int | None", _field(project, "userId", resource="project")),
        user_name=cast(
            "str | None",
            _field(project, "userName", resource="project"),
        ),
        create_time=cast(
            "str | None",
            _field(project, "createTime", resource="project"),
        ),
        update_time=cast(
            "str | None",
            _field(project, "updateTime", resource="project"),
        ),
        perm=cast("int", _field(project, "perm", resource="project")),
        definition_count=cast(
            "int",
            _field(project, "defCount", resource="project"),
        ),
    )


def _workflow_ref(workflow: OpaqueGeneratedValue) -> WorkflowRef:
    return WorkflowRef(
        native=NativeId(
            _positive_identity(
                _field(workflow, "id", resource="workflow"),
                resource="workflow",
                field="id",
            )
        ),
        name=cast("str | None", _field(workflow, "name", resource="workflow")),
        version=cast("int | None", _field(workflow, "version", resource="workflow")),
    )


def _workflow_list_view(workflow: OpaqueGeneratedValue) -> WorkflowListView:
    return WorkflowListView(
        ref=_workflow_ref(workflow),
        release_state=enum_value(
            _enum_field(workflow, "releaseState", resource="workflow")
        ),
        schedule_release_state=enum_value(
            _enum_field(workflow, "scheduleReleaseState", resource="workflow")
        ),
        schedule_id=None,
    )


def _workflow_view(workflow: OpaqueGeneratedValue) -> WorkflowView:
    return WorkflowView(
        ref=_workflow_ref(workflow),
        project_native=NativeId(
            _positive_identity(
                _field(workflow, "projectId", resource="workflow"),
                resource="workflow",
                field="projectId",
            )
        ),
        id=cast("int", _field(workflow, "id", resource="workflow")),
        description=cast(
            "str | None",
            _field(workflow, "description", resource="workflow"),
        ),
        global_params=cast(
            "str | None",
            _field(workflow, "globalParams", resource="workflow"),
        ),
        global_param_map=cast(
            "dict[str, str] | None",
            _field(workflow, "globalParamMap", resource="workflow"),
        ),
        create_time=cast(
            "str | None",
            _field(workflow, "createTime", resource="workflow"),
        ),
        update_time=cast(
            "str | None",
            _field(workflow, "updateTime", resource="workflow"),
        ),
        user_id=cast("int", _field(workflow, "userId", resource="workflow")),
        user_name=cast(
            "str | None",
            _field(workflow, "userName", resource="workflow"),
        ),
        project_name=cast(
            "str | None",
            _field(workflow, "projectName", resource="workflow"),
        ),
        timeout=cast("int", _field(workflow, "timeout", resource="workflow")),
        release_state=enum_value(
            _enum_field(workflow, "releaseState", resource="workflow")
        ),
        execution_type=None,
        include_execution_type=False,
        receivers=cast(
            "str | None",
            _field(workflow, "receivers", resource="workflow"),
        ),
        receivers_cc=cast(
            "str | None",
            _field(workflow, "receiversCc", resource="workflow"),
        ),
    )


def _schedule_view(schedule: OpaqueGeneratedValue) -> ScheduleView:
    return ScheduleView(
        id=cast("int | None", _field(schedule, "id", resource="schedule")),
        workflow_native=NativeId(
            _positive_identity(
                _field(schedule, "processDefinitionId", resource="schedule"),
                resource="schedule",
                field="processDefinitionId",
            )
        ),
        workflow_name=cast(
            "str | None",
            _field(schedule, "processDefinitionName", resource="schedule"),
        ),
        project_name=cast(
            "str | None",
            _field(schedule, "projectName", resource="schedule"),
        ),
        start_time=cast(
            "str | None",
            _field(schedule, "startTime", resource="schedule"),
        ),
        end_time=cast(
            "str | None",
            _field(schedule, "endTime", resource="schedule"),
        ),
        timezone_id=None,
        crontab=cast("str | None", _field(schedule, "crontab", resource="schedule")),
        failure_strategy=enum_value(
            _enum_field(schedule, "failureStrategy", resource="schedule")
        ),
        workflow_instance_priority=enum_value(
            _enum_field(
                schedule,
                "processInstancePriority",
                resource="schedule",
            )
        ),
        release_state=enum_value(
            _enum_field(schedule, "releaseState", resource="schedule")
        ),
    )


def _require_native_id(native: NativeIdentity, *, resource: str) -> int:
    if isinstance(native, NativeId):
        return native.value
    message = f"Id-native {resource} wire received a code identity"
    raise WireContractError(message)


def _require_project_name(project: ProjectRef) -> str:
    if project.name is not None:
        return project.name
    message = "Resolved 1.3.9 project was missing the name required by nested routes"
    raise ApiTransportError(message, details={"resource": PROJECT_RESOURCE})


def _positive_identity(
    value: OpaqueGeneratedValue, *, resource: str, field: str
) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    raise _projection_error(
        resource=resource,
        field=field,
        reason="identity must be a positive integer",
    )


def _field(
    value: OpaqueGeneratedValue, name: str, *, resource: str
) -> OpaqueGeneratedValue:
    try:
        return getattr(value, name)
    except AttributeError as error:
        raise _projection_error(
            resource=resource,
            field=name,
            reason="generated response field is missing",
        ) from error


def _enum_field(
    value: OpaqueGeneratedValue,
    name: str,
    *,
    resource: str,
) -> StringEnumValue | str | None:
    return cast(
        "StringEnumValue | str | None",
        _field(value, name, resource=resource),
    )


def _projection_error(*, resource: str, field: str, reason: str) -> ApiTransportError:
    message = "DolphinScheduler response did not match the exact 1.3.9 read contract"
    return ApiTransportError(
        message,
        details={
            "ds_version": "1.3.9",
            "resource": resource,
            "field": field,
            "reason": reason,
        },
    )


__all__ = ["IdNativeReadAdapter"]
