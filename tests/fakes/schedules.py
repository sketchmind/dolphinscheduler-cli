"""In-memory schedules collaborators with explicit test-owned state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from dsctl.errors import ApiResultError
from tests.fakes.common import (
    FakeEnumValue,
    _FakePage,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from dsctl.support.json_types import JsonObject
    from dsctl.upstream.protocol import (
        ScheduleCreateRequestPlan,
        ScheduleCreateSpec,
    )
    from tests.fakes.workflows import (
        FakeWorkflowAdapter,
    )


@dataclass(frozen=True)
class FakeSchedule:
    id: int | None = None
    workflow_definition_code_value: int = 0
    workflow_definition_name_value: str | None = None
    project_name_value: str | None = None
    definition_description_value: str | None = None
    start_time_value: str | None = None
    end_time_value: str | None = None
    timezone_id_value: str | None = None
    crontab_value: str | None = None
    failure_strategy_value: FakeEnumValue | None = None
    warning_type_value: FakeEnumValue | None = None
    create_time_value: str | None = None
    update_time_value: str | None = None
    user_id_value: int = 0
    user_name_value: str | None = None
    workflow_instance_priority_value: FakeEnumValue | None = None
    release_state_value: FakeEnumValue | None = None
    warning_group_id_value: int = 0
    worker_group_value: str | None = None
    tenant_code_value: str | None = None
    environment_code_value: int | None = None
    environment_name_value: str | None = None
    project_code_value: int = 0

    @property
    def workflowDefinitionCode(self) -> int:  # noqa: N802
        return self.workflow_definition_code_value

    @property
    def workflowDefinitionName(self) -> str | None:  # noqa: N802
        return self.workflow_definition_name_value

    @property
    def projectName(self) -> str | None:  # noqa: N802
        return self.project_name_value

    @property
    def definitionDescription(self) -> str | None:  # noqa: N802
        return self.definition_description_value

    @property
    def startTime(self) -> str | None:  # noqa: N802
        return self.start_time_value

    @property
    def endTime(self) -> str | None:  # noqa: N802
        return self.end_time_value

    @property
    def timezoneId(self) -> str | None:  # noqa: N802
        return self.timezone_id_value

    @property
    def crontab(self) -> str | None:
        return self.crontab_value

    @property
    def failureStrategy(self) -> FakeEnumValue | None:  # noqa: N802
        return self.failure_strategy_value

    @property
    def warningType(self) -> FakeEnumValue | None:  # noqa: N802
        return self.warning_type_value

    @property
    def createTime(self) -> str | None:  # noqa: N802
        return self.create_time_value

    @property
    def updateTime(self) -> str | None:  # noqa: N802
        return self.update_time_value

    @property
    def userId(self) -> int:  # noqa: N802
        return self.user_id_value

    @property
    def userName(self) -> str | None:  # noqa: N802
        return self.user_name_value

    @property
    def workflowInstancePriority(self) -> FakeEnumValue | None:  # noqa: N802
        return self.workflow_instance_priority_value

    @property
    def releaseState(self) -> FakeEnumValue | None:  # noqa: N802
        return self.release_state_value

    @property
    def warningGroupId(self) -> int:  # noqa: N802
        return self.warning_group_id_value

    @property
    def workerGroup(self) -> str | None:  # noqa: N802
        return self.worker_group_value

    @property
    def tenantCode(self) -> str | None:  # noqa: N802
        return self.tenant_code_value

    @property
    def environmentCode(self) -> int | None:  # noqa: N802
        return self.environment_code_value

    @property
    def environmentName(self) -> str | None:  # noqa: N802
        return self.environment_name_value


@dataclass(frozen=True)
class FakeSchedulePage(_FakePage[FakeSchedule]):
    pass


@dataclass
class FakeScheduleAdapter:
    schedules: list[FakeSchedule]
    preview_times_value: Sequence[str] | None = None
    ignore_workflow_filter: bool = False
    list_error: Exception | None = None
    list_errors_by_call: dict[int, Exception] = field(default_factory=dict)
    list_totals_by_call: dict[int, int | None] = field(default_factory=dict)
    list_calls: list[dict[str, object]] = field(default_factory=list)

    def list(
        self,
        *,
        project_code: int,
        page_no: int,
        page_size: int,
        workflow_code: int | None = None,
        search: str | None = None,
    ) -> FakeSchedulePage:
        self.list_calls.append(
            {
                "project_code": project_code,
                "page_no": page_no,
                "page_size": page_size,
                "workflow_code": workflow_code,
                "search": search,
            }
        )
        if self.list_error is not None:
            raise self.list_error
        call_error = self.list_errors_by_call.get(len(self.list_calls))
        if call_error is not None:
            raise call_error
        filtered = [
            schedule
            for schedule in self.schedules
            if schedule.project_code_value == project_code
        ]
        if workflow_code is not None and not self.ignore_workflow_filter:
            filtered = [
                schedule
                for schedule in filtered
                if schedule.workflowDefinitionCode == workflow_code
            ]
        if search is not None:
            filtered = [
                schedule
                for schedule in filtered
                if schedule.workflowDefinitionName is not None
                and search.lower() in schedule.workflowDefinitionName.lower()
            ]
        start = (page_no - 1) * page_size
        stop = start + page_size
        total = len(filtered)
        reported_total = self.list_totals_by_call.get(len(self.list_calls), total)
        total_pages = (
            None
            if reported_total is None
            else 0
            if reported_total == 0
            else ((reported_total - 1) // page_size) + 1
        )
        return FakeSchedulePage(
            total_list_value=filtered[start:stop],
            total=reported_total,
            total_page_value=total_pages,
            page_size_value=page_size,
            current_page_value=page_no,
            page_no_value=page_no,
        )

    def get(self, *, schedule_id: int) -> FakeSchedule:
        for schedule in self.schedules:
            if schedule.id == schedule_id:
                return schedule
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def preview(
        self,
        *,
        project_code: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
    ) -> Sequence[str]:
        del project_code, crontab, start_time, end_time, timezone_id
        if self.preview_times_value is not None:
            return list(self.preview_times_value)
        return [
            "2024-01-01 02:00:00",
            "2024-01-02 02:00:00",
            "2024-01-03 02:00:00",
            "2024-01-04 02:00:00",
            "2024-01-05 02:00:00",
        ]

    def create(
        self,
        *,
        spec: ScheduleCreateSpec[int],
    ) -> FakeSchedule:
        next_id = max((schedule.id or 0 for schedule in self.schedules), default=0) + 1
        created = FakeSchedule(
            id=next_id,
            workflow_definition_code_value=spec.workflow_code,
            start_time_value=spec.start_time,
            end_time_value=spec.end_time,
            timezone_id_value=spec.timezone_id,
            crontab_value=spec.crontab,
            failure_strategy_value=(
                None
                if spec.failure_strategy is None
                else FakeEnumValue(spec.failure_strategy)
            ),
            warning_type_value=(
                None if spec.warning_type is None else FakeEnumValue(spec.warning_type)
            ),
            workflow_instance_priority_value=(
                None
                if spec.workflow_instance_priority is None
                else FakeEnumValue(spec.workflow_instance_priority)
            ),
            release_state_value=FakeEnumValue("OFFLINE"),
            warning_group_id_value=spec.warning_group_id,
            worker_group_value=spec.worker_group,
            tenant_code_value=spec.tenant_code,
            environment_code_value=(
                -1 if spec.environment_code is None else spec.environment_code
            ),
            project_code_value=spec.project_code,
        )
        self.schedules.append(created)
        return created

    def plan_create(
        self,
        *,
        spec: ScheduleCreateSpec[int | str],
    ) -> ScheduleCreateRequestPlan:
        optional_fields: JsonObject = {
            key: value
            for key, value in {
                "failureStrategy": spec.failure_strategy,
                "warningType": spec.warning_type,
                "workflowInstancePriority": spec.workflow_instance_priority,
                "workerGroup": spec.worker_group,
                "tenantCode": spec.tenant_code,
            }.items()
            if value is not None
        }
        body: JsonObject = {
            "workflowDefinitionCode": spec.workflow_code,
            "crontab": spec.crontab,
            "startTime": spec.start_time,
            "endTime": spec.end_time,
            "timezoneId": spec.timezone_id,
            "warningGroupId": spec.warning_group_id,
        }
        if spec.environment_code is not None:
            body["environmentCode"] = spec.environment_code
            body.update(optional_fields)
            return {"method": "POST", "path": "/v2/schedules", "json": body}
        schedule = json.dumps(
            {
                "crontab": spec.crontab,
                "endTime": spec.end_time,
                "startTime": spec.start_time,
                "timezoneId": spec.timezone_id,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        form: JsonObject = {
            "workflowDefinitionCode": spec.workflow_code,
            "schedule": schedule,
            "warningGroupId": spec.warning_group_id,
        }
        form.update(optional_fields)
        return {
            "method": "POST",
            "path": f"/projects/{spec.project_code}/schedules",
            "form": form,
        }

    def update(
        self,
        *,
        project_code: int,
        schedule_id: int,
        crontab: str,
        start_time: str,
        end_time: str,
        timezone_id: str,
        failure_strategy: str | None = None,
        warning_type: str | None = None,
        warning_group_id: int = 0,
        workflow_instance_priority: str | None = None,
        worker_group: str | None = None,
        tenant_code: str | None = None,
        environment_code: int | None = None,
    ) -> FakeSchedule:
        del project_code
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                updated = replace(
                    schedule,
                    start_time_value=start_time,
                    end_time_value=end_time,
                    timezone_id_value=timezone_id,
                    crontab_value=crontab,
                    failure_strategy_value=(
                        None
                        if failure_strategy is None
                        else FakeEnumValue(failure_strategy)
                    ),
                    warning_type_value=(
                        None if warning_type is None else FakeEnumValue(warning_type)
                    ),
                    workflow_instance_priority_value=(
                        None
                        if workflow_instance_priority is None
                        else FakeEnumValue(workflow_instance_priority)
                    ),
                    warning_group_id_value=warning_group_id,
                    worker_group_value=worker_group,
                    tenant_code_value=tenant_code,
                    environment_code_value=(
                        -1 if environment_code is None else environment_code
                    ),
                )
                self.schedules[index] = updated
                return updated
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def delete(self, *, schedule_id: int) -> bool:
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                self.schedules.pop(index)
                return True
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )

    def online(self, *, schedule_id: int) -> FakeSchedule:
        return self._set_release_state(
            schedule_id=schedule_id,
            release_state="ONLINE",
        )

    def offline(self, *, schedule_id: int) -> FakeSchedule:
        return self._set_release_state(
            schedule_id=schedule_id,
            release_state="OFFLINE",
        )

    def _set_release_state(
        self,
        *,
        schedule_id: int,
        release_state: str,
    ) -> FakeSchedule:
        for index, schedule in enumerate(self.schedules):
            if schedule.id == schedule_id:
                updated = replace(
                    schedule,
                    release_state_value=FakeEnumValue(release_state),
                )
                self.schedules[index] = updated
                return updated
        raise ApiResultError(
            result_code=10203,
            result_message=f"schedule id {schedule_id} not found",
        )


def empty_schedule_adapter() -> FakeScheduleAdapter:
    return FakeScheduleAdapter(schedules=[])


def schedule_adapter_from_workflows(
    workflow_adapter: FakeWorkflowAdapter,
) -> FakeScheduleAdapter:
    schedules = [
        replace(
            schedule,
            workflow_definition_code_value=workflow.code,
            workflow_definition_name_value=workflow.name,
            project_name_value=workflow.projectName,
            project_code_value=workflow.projectCode,
        )
        for workflow in workflow_adapter.workflows
        if (schedule := workflow.schedule) is not None
    ]
    return FakeScheduleAdapter(schedules=schedules)
