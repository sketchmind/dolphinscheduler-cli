from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.failure_strategy import FailureStrategy
from ...common.enums.priority import Priority
from ...common.enums.warning_type import WarningType
from ...dao.entities.schedule import Schedule
from ..contracts.page_info import PageInfoScheduleVO
from ..views.schedule import ScheduleVO
from ..views.scheduler import ScheduleInsertResult

class QueryScheduleListPagingParams(BaseParamsModel):
    """
    Query Schedule List Paging
    
    Query parameters for SchedulerController.queryScheduleListPaging.
    """
    workflowDefinitionCode: int | None = Field(default=None, description='workflow definition code')
    searchVal: str | None = Field(default=None, description='search value')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[20])

class CreateScheduleParams(BaseParamsModel):
    """
    Create Schedule
    
    Form parameters for SchedulerController.createSchedule.
    """
    workflowDefinitionCode: int = Field(description='workflow definition code', examples=[100])
    schedule: str = Field(description='scheduler', examples=["{'startTime':'2019-06-10 00:00:00','endTime':'2019-06-13 00:00:00','timezoneId':'America/Phoenix','crontab':'0 0 3/6 * * ? *'}"])
    warningType: WarningType | None = Field(default=None, description='warning type')
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    failureStrategy: FailureStrategy | None = Field(default=None, description='failure strategy')
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, description='tenant code', examples=['default'])
    environmentCode: int | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None, description='workflow instance priority')

class PreviewScheduleParams(BaseParamsModel):
    """
    Preview Schedule
    
    Form parameters for SchedulerController.previewSchedule.
    """
    schedule: str = Field(description='schedule expression', examples=["{'startTime':'2019-06-10 00:00:00','endTime':'2019-06-13 00:00:00','crontab':'0 0 3/6 * * ? *'}"])

class UpdateScheduleByWorkflowDefinitionCodeParams(BaseParamsModel):
    """
    Update Schedule By Workflow Definition Code
    
    Form parameters for SchedulerController.updateScheduleByWorkflowDefinitionCode.
    """
    schedule: str = Field(description='scheduler', examples=["{'startTime':'2019-06-10 00:00:00','endTime':'2019-06-13 00:00:00','crontab':'0 0 3/6 * * ? *'}"])
    warningType: WarningType | None = Field(default=None, description='warning type')
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    failureStrategy: FailureStrategy | None = Field(default=None, description='failure strategy')
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, examples=['default'])
    environmentCode: int | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None, description='workflow instance priority')

class UpdateScheduleParams(BaseParamsModel):
    """
    Update Schedule
    
    Form parameters for SchedulerController.updateSchedule.
    """
    schedule: str = Field(description='scheduler', examples=['{\\"startTime\\":\\"1996-08-08 00:00:00\\",\\"endTime\\":\\"2200-08-08 00:00:00\\",\\"timezoneId\\":\\"America/Phoenix\\",\\"crontab\\":\\"0 0 3/6 * * ? *\\"}'])
    warningType: WarningType | None = Field(default=None, description='warning type')
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    failureStrategy: FailureStrategy | None = Field(default=None, description='failure strategy')
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, description='tenant code', examples=['default'])
    environmentCode: int | None = Field(default=None)
    workflowInstancePriority: Priority | None = Field(default=None, description='workflow instance priority')

class SchedulerOperations(BaseRequestsClient):
    def query_schedule_list_paging(
        self,
        project_code: int,
        params: QueryScheduleListPagingParams
    ) -> PageInfoScheduleVO:
        """
        Query Schedule List Paging
        
        Query schedule list paging
        
        DS operation: SchedulerController.queryScheduleListPaging | GET /projects/{projectCode}/schedules
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            schedule list page
        """
        path = f"projects/{project_code}/schedules"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoScheduleVO))

    def create_schedule(
        self,
        project_code: int,
        form: CreateScheduleParams
    ) -> ScheduleInsertResult:
        """
        Create Schedule
        
        Create schedule
        
        DS operation: SchedulerController.createSchedule | POST /projects/{projectCode}/schedules
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        path = f"projects/{project_code}/schedules"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(ScheduleInsertResult))

    def query_schedule_list(
        self,
        project_code: int
    ) -> list[ScheduleVO]:
        """
        Query Schedule List
        
        Query schedule list
        
        DS operation: SchedulerController.queryScheduleList | POST /projects/{projectCode}/schedules/list
        
        Args:
            project_code: project code
        
        Returns:
            schedule list
        """
        path = f"projects/{project_code}/schedules/list"
        payload = self._request("POST", path)
        return self._validate_payload(payload, TypeAdapter(list[ScheduleVO]))

    def preview_schedule(
        self,
        project_code: int,
        form: PreviewScheduleParams
    ) -> list[str]:
        """
        Preview Schedule
        
        Preview schedule
        
        DS operation: SchedulerController.previewSchedule | POST /projects/{projectCode}/schedules/preview
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            the next five fire time
        """
        path = f"projects/{project_code}/schedules/preview"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(list[str]))

    def update_schedule_by_workflow_definition_code(
        self,
        project_code: int,
        code: int,
        form: UpdateScheduleByWorkflowDefinitionCodeParams
    ) -> Schedule:
        """
        Update Schedule By Workflow Definition Code
        
        Update workflow definition schedule
        
        DS operation: SchedulerController.updateScheduleByWorkflowDefinitionCode | PUT /projects/{projectCode}/schedules/update/{code}
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/schedules/update/{code}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Schedule))

    def delete_schedule_by_id(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Delete Schedule By Id
        
        Delete schedule by id
        
        DS operation: SchedulerController.deleteScheduleById | DELETE /projects/{projectCode}/schedules/{id}
        
        Args:
            project_code: project code
            id: schedule id
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/schedules/{id}"
        self._request("DELETE", path)
        return None

    def update_schedule(
        self,
        project_code: int,
        id: int,
        form: UpdateScheduleParams
    ) -> Schedule:
        """
        Update Schedule
        
        UpdateWorkflowInstance schedule
        
        DS operation: SchedulerController.updateSchedule | PUT /projects/{projectCode}/schedules/{id}
        
        Args:
            project_code: project code
            id: scheduler id
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/schedules/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Schedule))

    def offline_schedule(
        self,
        project_code: int,
        id: int
    ) -> bool:
        """
        Offline
        
        DS operation: SchedulerController.offlineSchedule | POST /projects/{projectCode}/schedules/{id}/offline
        """
        path = f"projects/{project_code}/schedules/{id}/offline"
        payload = self._request("POST", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def publish_schedule_online(
        self,
        project_code: int,
        id: int
    ) -> bool:
        """
        Online
        
        DS operation: SchedulerController.publishScheduleOnline | POST /projects/{projectCode}/schedules/{id}/online
        """
        path = f"projects/{project_code}/schedules/{id}/online"
        payload = self._request("POST", path)
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["SchedulerOperations"]
