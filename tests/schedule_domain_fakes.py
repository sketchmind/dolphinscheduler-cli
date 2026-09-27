from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, cast

from dsctl.errors import ApiResultError
from dsctl.services import schedule as schedule_service
from dsctl.services.runtime import BoundDomainServiceRuntime
from dsctl.services.selection import ResourceDefaults
from dsctl.upstream.definition_models import (
    NativeCode,
    NativeId,
    ProjectRef,
    WorkflowRef,
    WorkflowScope,
    WorkflowView,
)
from dsctl.upstream.protocol import ScheduleCreateSpec
from dsctl.upstream.read_models import ReadPage
from dsctl.upstream.schedules import (
    LocatedSchedule,
    PreparedScheduleCreate,
    PreparedScheduleUpdate,
    ScheduleDomain,
    ScheduleListing,
    ScheduleOperations,
    SchedulePreview,
    ScheduleSnapshot,
    ScheduleState,
    ScheduleUpdatePatch,
)
from tests.fakes import (
    FakeEnvironmentAdapter,
    FakeHttpClient,
    FakeProject,
    FakeProjectAdapter,
    FakeProjectPreferenceAdapter,
    FakeSchedule,
    FakeScheduleAdapter,
    FakeUserAdapter,
    FakeWorkflow,
    FakeWorkflowAdapter,
)
from tests.support import make_profile

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import pytest

    from dsctl.config import ClusterProfile
    from dsctl.upstream.wire import PreparedCompiledWireCall


def install_schedule_domain_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_adapter: FakeProjectAdapter,
    workflow_adapter: FakeWorkflowAdapter,
    schedule_adapter: FakeScheduleAdapter,
    user_adapter: FakeUserAdapter | None = None,
    context: ResourceDefaults | None = None,
    profile: ClusterProfile | None = None,
    project_preference_adapter: FakeProjectPreferenceAdapter | None = None,
    environment_adapter: FakeEnvironmentAdapter | None = None,
) -> None:
    """Bind service tests to the new deep domain without reopening broad runtime."""
    selected_profile = profile or make_profile()
    selected_context = context or ResourceDefaults()
    operations = _FakeScheduleOperations(
        project_adapter=project_adapter,
        workflow_adapter=workflow_adapter,
        schedule_adapter=schedule_adapter,
        user_adapter=user_adapter,
        project_preference_adapter=project_preference_adapter,
        environment_adapter=environment_adapter,
        ds_version=selected_profile.ds_version,
    )
    runtime = BoundDomainServiceRuntime(
        profile=selected_profile,
        context=selected_context,
        http_client=FakeHttpClient(),
        domain=ScheduleDomain(
            schedules=cast("ScheduleOperations", operations),
        ),
    )

    def run(
        env_file: str | None,
        domain: object,
        operation: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        del env_file, domain
        return operation(runtime, *args, **kwargs)

    monkeypatch.setattr(
        schedule_service,
        "run_with_bound_domain_service_runtime",
        run,
    )


@dataclass
class _FakeScheduleOperations:
    project_adapter: FakeProjectAdapter
    workflow_adapter: FakeWorkflowAdapter
    schedule_adapter: FakeScheduleAdapter
    user_adapter: FakeUserAdapter | None
    project_preference_adapter: FakeProjectPreferenceAdapter | None
    environment_adapter: FakeEnvironmentAdapter | None
    ds_version: str

    def list(
        self,
        project_selector: str,
        *,
        workflow_selector: str | None,
        search: str | None,
        page_no: int,
        page_size: int,
        all_pages: bool,
    ) -> ScheduleListing:
        del all_pages
        project = self._project(project_selector)
        workflow = (
            None
            if workflow_selector is None
            else self._workflow(project, workflow_selector)
        )
        page = self.schedule_adapter.list(
            project_code=project.native.value,
            page_no=page_no,
            page_size=page_size,
            workflow_code=None if workflow is None else workflow.native.value,
            search=search,
        )
        items = tuple(self._snapshot(item) for item in (page.totalList or []))
        return ScheduleListing(
            project=project,
            workflow=workflow,
            page=ReadPage(
                totalList=items,
                total=page.total,
                totalPage=page.totalPage,
                pageSize=page.pageSize,
                currentPage=page.currentPage,
                pageNo=page.pageNo,
            ),
        )

    def get(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> LocatedSchedule:
        if project_selector is None:
            schedule = self.schedule_adapter.get(schedule_id=schedule_id)
            project = self._project(str(schedule.project_code_value))
            return LocatedSchedule(project=project, schedule=self._snapshot(schedule))

        project = self._project(project_selector)
        matches = [
            schedule
            for schedule in self.schedule_adapter.schedules
            if schedule.id == schedule_id
            and schedule.project_code_value == project.native.value
        ]
        if len(matches) != 1:
            raise ApiResultError(
                result_code=10203,
                result_message=f"schedule id {schedule_id} not found",
            )
        return LocatedSchedule(project=project, schedule=self._snapshot(matches[0]))

    def preview(
        self,
        project_selector: str,
        state: ScheduleState,
    ) -> SchedulePreview:
        project = self._project(project_selector)
        return SchedulePreview(project=project, times=self._preview(project, state))

    def preview_existing(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> tuple[LocatedSchedule, tuple[str, ...]]:
        located = self.get(schedule_id, project_selector=project_selector)
        return located, self._preview(located.project, located.schedule.state())

    def prepare_create(
        self,
        project_selector: str,
        workflow_selector: str,
        state: ScheduleState,
    ) -> PreparedScheduleCreate:
        project = self._project(project_selector)
        workflow = self._workflow(project, workflow_selector)
        state, tenant_source, preference_fields = self._create_defaults(project, state)
        return PreparedScheduleCreate(
            ds_version=self.ds_version,
            scope=WorkflowScope(
                project=project,
                workflow=workflow,
                view=self._workflow_view(project, workflow),
            ),
            state=state,
            preview_times=self._preview(project, state),
            # This domain fake executes from state; it never dispatches the token.
            form=cast("PreparedCompiledWireCall", object()),
            tenant_source=tenant_source,
            project_preference_used_fields=preference_fields,
        )

    def effective_create_state(
        self,
        project: ProjectRef,
        state: ScheduleState,
    ) -> ScheduleState:
        effective, _, _ = self._create_defaults(project, state)
        return effective

    def create(self, prepared: PreparedScheduleCreate) -> ScheduleSnapshot:
        state = prepared.state
        created = self.schedule_adapter.create(
            spec=ScheduleCreateSpec(
                project_code=prepared.scope.project.native.value,
                workflow_code=prepared.scope.workflow.native.value,
                crontab=state.crontab,
                start_time=state.start_time,
                end_time=state.end_time,
                timezone_id=state.timezone_id,
                failure_strategy=state.failure_strategy,
                warning_type=state.warning_type,
                warning_group_id=state.warning_group_id,
                workflow_instance_priority=state.workflow_instance_priority,
                worker_group=state.worker_group,
                tenant_code=state.tenant_code,
                environment_code=state.environment_code,
            )
        )
        return self._snapshot(created)

    def prepare_update(
        self,
        schedule_id: int,
        patch: ScheduleUpdatePatch,
        *,
        project_selector: str | None = None,
    ) -> PreparedScheduleUpdate:
        located = self.get(schedule_id, project_selector=project_selector)
        state = _merge_patch(located.schedule.state(), patch)
        if (
            "environmentCode" in patch.requested_fields()
            and state.environment_code is not None
            and state.environment_code > 0
            and self.environment_adapter is not None
        ):
            # Stable service behavior: update/explain validate only an explicit
            # positive environment. Create intentionally performs no lookup.
            self.environment_adapter.get(code=state.environment_code)
        return PreparedScheduleUpdate(
            ds_version=self.ds_version,
            current=located,
            state=state,
            requested_fields=patch.requested_fields(),
            preview_times=self._preview(located.project, state),
            form=cast("PreparedCompiledWireCall", object()),
            fingerprint=(schedule_id, located.schedule.crontab),
        )

    def update(self, prepared: PreparedScheduleUpdate) -> ScheduleSnapshot:
        state = prepared.state
        updated = self.schedule_adapter.update(
            project_code=prepared.current.project.native.value,
            schedule_id=prepared.current.schedule.id,
            crontab=state.crontab,
            start_time=state.start_time,
            end_time=state.end_time,
            timezone_id=state.timezone_id or "server-local",
            failure_strategy=state.failure_strategy,
            warning_type=state.warning_type,
            warning_group_id=state.warning_group_id,
            workflow_instance_priority=state.workflow_instance_priority,
            worker_group=state.worker_group,
            tenant_code=state.tenant_code,
            environment_code=state.environment_code,
        )
        return self._snapshot(updated)

    def delete(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> tuple[ScheduleSnapshot, bool]:
        current = self.get(
            schedule_id,
            project_selector=project_selector,
        ).schedule
        return current, self.schedule_adapter.delete(schedule_id=schedule_id)

    def online(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> ScheduleSnapshot:
        self.get(schedule_id, project_selector=project_selector)
        return self._snapshot(self.schedule_adapter.online(schedule_id=schedule_id))

    def offline(
        self,
        schedule_id: int,
        *,
        project_selector: str | None = None,
    ) -> ScheduleSnapshot:
        self.get(schedule_id, project_selector=project_selector)
        return self._snapshot(self.schedule_adapter.offline(schedule_id=schedule_id))

    def _project(self, selector: str) -> ProjectRef:
        project = _select_project(self.project_adapter.projects, selector)
        native_type = NativeId if self.ds_version == "1.3.9" else NativeCode
        return ProjectRef(
            native=native_type(project.code),
            name=project.name,
            description=project.description,
        )

    def _workflow(self, project: ProjectRef, selector: str) -> WorkflowRef:
        workflow = _select_workflow(
            self.workflow_adapter.workflows,
            project.native.value,
            selector,
        )
        native_type = NativeId if self.ds_version == "1.3.9" else NativeCode
        return WorkflowRef(
            native=native_type(workflow.code),
            name=workflow.name,
            version=workflow.version,
        )

    def _workflow_view(
        self,
        project: ProjectRef,
        workflow: WorkflowRef,
    ) -> WorkflowView:
        return WorkflowView(
            ref=workflow,
            project_native=project.native,
            id=None,
            description=None,
            global_params="[]",
            global_param_map={},
            create_time=None,
            update_time=None,
            user_id=0,
            user_name=None,
            project_name=project.name,
            timeout=0,
            release_state="ONLINE",
            execution_type=None,
            include_execution_type=False,
        )

    def _preview(
        self,
        project: ProjectRef,
        state: ScheduleState,
    ) -> tuple[str, ...]:
        return tuple(
            self.schedule_adapter.preview(
                project_code=project.native.value,
                crontab=state.crontab,
                start_time=state.start_time,
                end_time=state.end_time,
                timezone_id=state.timezone_id or "server-local",
            )
        )

    def _create_defaults(
        self,
        project: ProjectRef,
        state: ScheduleState,
    ) -> tuple[ScheduleState, str, tuple[str, ...]]:
        preferences = self._preferences(project.native.value)
        used: list[str] = []

        def preferred(field: str, fallback: object, output: str) -> object:
            if field in state.provided_fields or field not in preferences:
                return fallback
            used.append(output)
            return preferences[field]

        warning_type = str(preferred("warningType", state.warning_type, "warningType"))
        warning_group_id = int(
            cast(
                "int | str",
                preferred("alertGroups", state.warning_group_id, "warningGroupId"),
            )
        )
        priority = str(
            preferred(
                "taskPriority",
                state.workflow_instance_priority,
                "workflowInstancePriority",
            )
        )
        worker_group = str(preferred("workerGroup", state.worker_group, "workerGroup"))
        environment = preferred(
            "environmentCode",
            state.environment_code,
            "environmentCode",
        )
        environment_code = (
            None
            if environment in {None, 0, -1}
            else int(cast("int | str", environment))
        )

        tenant_code = state.tenant_code
        tenant_source = "flag"
        if "tenantCode" not in state.provided_fields:
            if "tenant" in preferences:
                tenant_code = str(preferences["tenant"])
                tenant_source = "project_preference"
                used.append("tenantCode")
            else:
                tenant_code = self._current_tenant() or "default"
                tenant_source = (
                    "current_user" if tenant_code != "default" else "default"
                )
        return (
            replace(
                state,
                warning_type=warning_type,
                warning_group_id=warning_group_id,
                workflow_instance_priority=priority,
                worker_group=worker_group,
                tenant_code=tenant_code,
                environment_code=environment_code,
            ),
            tenant_source,
            tuple(used),
        )

    def _preferences(self, project_code: int) -> dict[str, object]:
        if self.project_preference_adapter is None:
            return {}
        preference = self.project_preference_adapter.get(project_code=project_code)
        if (
            preference is None
            or preference.state != 1
            or preference.preferences is None
        ):
            return {}
        decoded = json.loads(preference.preferences)
        assert isinstance(decoded, dict)
        return cast("dict[str, object]", decoded)

    def _current_tenant(self) -> str | None:
        if self.user_adapter is None:
            return None
        try:
            return self.user_adapter.current().tenantCode
        except ApiResultError:
            return None

    def _snapshot(self, schedule: FakeSchedule) -> ScheduleSnapshot:
        if schedule.id is None:
            message = "service fake schedule must have an id"
            raise AssertionError(message)
        environment_code = schedule.environmentCode
        if environment_code is not None and environment_code <= 0:
            environment_code = None
        return ScheduleSnapshot(
            ds_version=self.ds_version,
            id=schedule.id,
            workflow_native=(
                NativeId(schedule.workflowDefinitionCode)
                if self.ds_version == "1.3.9"
                else NativeCode(schedule.workflowDefinitionCode)
            ),
            workflow_name=schedule.workflowDefinitionName,
            project_name=schedule.projectName,
            definition_description=schedule.definitionDescription,
            start_time=schedule.startTime,
            end_time=schedule.endTime,
            timezone_id=(None if self.ds_version == "1.3.9" else schedule.timezoneId),
            crontab=schedule.crontab,
            failure_strategy=_enum(schedule.failureStrategy) or "CONTINUE",
            warning_type=_enum(schedule.warningType) or "NONE",
            create_time=schedule.createTime,
            update_time=schedule.updateTime,
            user_id=schedule.userId,
            user_name=schedule.userName,
            release_state=_enum(schedule.releaseState),
            warning_group_id=schedule.warningGroupId,
            workflow_instance_priority=(
                _enum(schedule.workflowInstancePriority) or "MEDIUM"
            ),
            worker_group=schedule.workerGroup or "default",
            tenant_code=schedule.tenantCode or "default",
            environment_code=environment_code,
            environment_name=schedule.environmentName,
            workflow_field="workflowDefinitionCode",
            workflow_name_field="workflowDefinitionName",
            priority_field="workflowInstancePriority",
            has_timezone=self.ds_version != "1.3.9",
            has_tenant=self.ds_version != "1.3.9",
            has_environment=self.ds_version != "1.3.9",
            has_environment_name=self.ds_version != "1.3.9",
        )


def _select_project(projects: Sequence[FakeProject], selector: str) -> FakeProject:
    numeric = int(selector) if selector.isdigit() else None
    matches = [
        project
        for project in projects
        if (numeric is not None and project.code == numeric) or project.name == selector
    ]
    if len(matches) == 1:
        return matches[0]
    raise ApiResultError(
        result_code=10018, result_message=f"project {selector} not found"
    )


def _select_workflow(
    workflows: Sequence[FakeWorkflow],
    project_code: int,
    selector: str,
) -> FakeWorkflow:
    numeric = int(selector) if selector.isdigit() else None
    matches = [
        workflow
        for workflow in workflows
        if workflow.projectCode == project_code
        and (
            (numeric is not None and workflow.code == numeric)
            or workflow.name == selector
        )
    ]
    if len(matches) == 1:
        return matches[0]
    raise ApiResultError(
        result_code=50003, result_message=f"workflow {selector} not found"
    )


def _merge_patch(current: ScheduleState, patch: ScheduleUpdatePatch) -> ScheduleState:
    requested = patch.requested_fields()

    def value(attribute: str, field: str, fallback: object) -> object:
        return getattr(patch, attribute) if field in requested else fallback

    return ScheduleState(
        crontab=cast("str", value("crontab", "crontab", current.crontab)),
        start_time=cast("str", value("start_time", "startTime", current.start_time)),
        end_time=cast("str", value("end_time", "endTime", current.end_time)),
        timezone_id=cast(
            "str | None",
            value("timezone_id", "timezoneId", current.timezone_id),
        ),
        failure_strategy=cast(
            "str",
            value("failure_strategy", "failureStrategy", current.failure_strategy),
        ),
        warning_type=cast(
            "str",
            value("warning_type", "warningType", current.warning_type),
        ),
        warning_group_id=cast(
            "int",
            value("warning_group_id", "warningGroupId", current.warning_group_id),
        ),
        workflow_instance_priority=cast(
            "str",
            value(
                "workflow_instance_priority",
                "workflowInstancePriority",
                current.workflow_instance_priority,
            ),
        ),
        worker_group=cast(
            "str",
            value("worker_group", "workerGroup", current.worker_group),
        ),
        tenant_code=current.tenant_code,
        environment_code=cast(
            "int | None",
            value("environment_code", "environmentCode", current.environment_code),
        ),
        provided_fields=requested,
    )


def _enum(value: object) -> str | None:
    enum_value = getattr(value, "value", None)
    return enum_value if isinstance(enum_value, str) else None


__all__ = ["install_schedule_domain_runtime"]
