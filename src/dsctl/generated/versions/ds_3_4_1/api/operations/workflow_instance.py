from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.workflow_execution_status import WorkflowExecutionStatus
from ...dao.entities.workflow_definition import WorkflowDefinition
from ...dao.entities.workflow_instance import WorkflowInstance
from ..contracts.dynamic_sub_workflow_dto import DynamicSubWorkflowDto
from ..contracts.gantt.gantt_dto import GanttDto
from ..contracts.page_info import PageInfoWorkflowInstance
from ..views.workflow_instance import WorkflowInstanceParentInstanceView
from ..views.workflow_instance import WorkflowInstanceSubWorkflowInstanceView
from ..views.workflow_instance import WorkflowInstanceTaskListView
from ..views.workflow_instance import WorkflowInstanceVariablesView

class QueryWorkflowInstanceListParams(BaseParamsModel):
    """
    Query Workflow Instance List Paging
    
    Query parameters for WorkflowInstanceController.queryWorkflowInstanceList.
    """
    workflowDefinitionCode: int | None = Field(default=None, description='workflow definition code', examples=[100])
    searchVal: str | None = Field(default=None, description='search value')
    executorName: str | None = Field(default=None)
    stateType: WorkflowExecutionStatus | None = Field(default=None, description='state type')
    host: str | None = Field(default=None, description='host')
    startDate: str | None = Field(default=None, description='start time')
    endDate: str | None = Field(default=None, description='end time')
    otherParamsJson: str | None = Field(default=None, description='otherParamsJson handle other params')
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[10])

class BatchDeleteWorkflowInstanceByIdsParams(BaseParamsModel):
    """
    Batch Delete Workflow Instance By Ids
    
    Form parameters for WorkflowInstanceController.batchDeleteWorkflowInstanceByIds.
    """
    workflowInstanceIds: str = Field(description='workflow instance id')

class QueryDynamicSubWorkflowInstancesParams(BaseParamsModel):
    """
    Query Dynamic Sub Workflow Instances
    
    Query parameters for WorkflowInstanceController.queryDynamicSubWorkflowInstances.
    """
    taskId: int = Field(description='taskInstanceId', examples=[100])

class QueryParentInstanceBySubIdParams(BaseParamsModel):
    """
    Query Parent Instance By Sub Id
    
    Query parameters for WorkflowInstanceController.queryParentInstanceBySubId.
    """
    subId: int = Field(description='sub workflow id', examples=[100])

class QuerySubWorkflowInstanceByTaskIdParams(BaseParamsModel):
    """
    Query Sub Workflow Instance By Task Code
    
    Query parameters for WorkflowInstanceController.querySubWorkflowInstanceByTaskId.
    """
    taskId: int = Field(description='task id')

class QueryTopNLongestRunningWorkflowInstanceParams(BaseParamsModel):
    """
    Query Top Nlongest Running Workflow Instance
    
    Query parameters for WorkflowInstanceController.queryTopNLongestRunningWorkflowInstance.
    """
    size: int = Field(description='number of workflow instance', examples=[10])
    startTime: str = Field(description='start time')
    endTime: str = Field(description='end time')

class QueryWorkflowInstancesByTriggerCodeParams(BaseParamsModel):
    """
    Query Workflow Instance List By Trigger
    
    Query parameters for WorkflowInstanceController.queryWorkflowInstancesByTriggerCode.
    """
    triggerCode: int

class UpdateWorkflowInstanceParams(BaseParamsModel):
    """
    Update Workflow Instance
    
    Form parameters for WorkflowInstanceController.updateWorkflowInstance.
    """
    taskRelationJson: str = Field(description='workflow task relation json')
    taskDefinitionJson: str = Field(description='taskDefinitionJson')
    scheduleTime: str | None = Field(default=None, description='schedule time')
    syncDefine: bool = Field(description='sync define', examples=[False])
    globalParams: str | None = Field(default=None, examples=['[]'])
    locations: str | None = Field(default=None, description='locations')
    timeout: int | None = Field(default=None, examples=[0])

class WorkflowInstanceOperations(BaseRequestsClient):
    def query_workflow_instance_list(
        self,
        project_code: int,
        params: QueryWorkflowInstanceListParams
    ) -> PageInfoWorkflowInstance:
        """
        Query Workflow Instance List Paging
        
        Query workflow instance list paging
        
        DS operation: WorkflowInstanceController.queryWorkflowInstanceList | GET /projects/{projectCode}/workflow-instances
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            workflow instance list
        """
        path = f"projects/{project_code}/workflow-instances"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoWorkflowInstance))

    def batch_delete_workflow_instance_by_ids(
        self,
        project_code: int,
        form: BatchDeleteWorkflowInstanceByIdsParams
    ) -> None:
        """
        Batch Delete Workflow Instance By Ids
        
        Batch delete workflow instance by ids, at the same time, delete task instance and their mapping relation data
        
        DS operation: WorkflowInstanceController.batchDeleteWorkflowInstanceByIds | POST /projects/{projectCode}/workflow-instances/batch-delete
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-instances/batch-delete"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def query_dynamic_sub_workflow_instances(
        self,
        project_code: int,
        params: QueryDynamicSubWorkflowInstancesParams
    ) -> list[DynamicSubWorkflowDto]:
        """
        Query Dynamic Sub Workflow Instances
        
        Query dynamic sub workflow instance detail info by task id
        
        DS operation: WorkflowInstanceController.queryDynamicSubWorkflowInstances | GET /projects/{projectCode}/workflow-instances/query-dynamic-sub-workflows
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            sub workflow instance detail
        """
        path = f"projects/{project_code}/workflow-instances/query-dynamic-sub-workflows"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[DynamicSubWorkflowDto]))

    def query_parent_instance_by_sub_id(
        self,
        project_code: int,
        params: QueryParentInstanceBySubIdParams
    ) -> WorkflowInstanceParentInstanceView:
        """
        Query Parent Instance By Sub Id
        
        Query parent workflow instance detail info by sub workflow instance id
        
        DS operation: WorkflowInstanceController.queryParentInstanceBySubId | GET /projects/{projectCode}/workflow-instances/query-parent-by-sub
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            parent instance detail
        """
        path = f"projects/{project_code}/workflow-instances/query-parent-by-sub"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowInstanceParentInstanceView))

    def query_sub_workflow_instance_by_task_id(
        self,
        project_code: int,
        params: QuerySubWorkflowInstanceByTaskIdParams
    ) -> WorkflowInstanceSubWorkflowInstanceView:
        """
        Query Sub Workflow Instance By Task Code
        
        Query sub workflow instance detail info by task id
        
        DS operation: WorkflowInstanceController.querySubWorkflowInstanceByTaskId | GET /projects/{projectCode}/workflow-instances/query-sub-by-parent
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            sub workflow instance detail
        """
        path = f"projects/{project_code}/workflow-instances/query-sub-by-parent"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowInstanceSubWorkflowInstanceView))

    def query_top_nlongest_running_workflow_instance(
        self,
        project_code: int,
        params: QueryTopNLongestRunningWorkflowInstanceParams
    ) -> list[WorkflowInstance]:
        """
        Query Top Nlongest Running Workflow Instance
        
        Query top n workflow instance order by running duration
        
        DS operation: WorkflowInstanceController.queryTopNLongestRunningWorkflowInstance | GET /projects/{projectCode}/workflow-instances/top-n
        
        Args:
            project_code: project code
            params: Query parameters bag for this operation.
        
        Returns:
            list of workflow instance
        """
        path = f"projects/{project_code}/workflow-instances/top-n"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[WorkflowInstance]))

    def query_workflow_instances_by_trigger_code(
        self,
        project_code: int,
        params: QueryWorkflowInstancesByTriggerCodeParams
    ) -> list[WorkflowInstance]:
        """
        Query Workflow Instance List By Trigger
        
        DS operation: WorkflowInstanceController.queryWorkflowInstancesByTriggerCode | GET /projects/{projectCode}/workflow-instances/trigger
        
        Args:
            params: Query parameters bag for this operation.
        """
        path = f"projects/{project_code}/workflow-instances/trigger"
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            path,
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[WorkflowInstance]))

    def delete_workflow_instance_by_id(
        self,
        project_code: int,
        id: int
    ) -> None:
        """
        Delete Workflow Instance By Id
        
        Delete workflow instance by id, at the same time, delete task instance and their mapping relation data
        
        DS operation: WorkflowInstanceController.deleteWorkflowInstanceById | DELETE /projects/{projectCode}/workflow-instances/{id}
        
        Args:
            project_code: project code
            id: workflow instance id
        
        Returns:
            delete result code
        """
        path = f"projects/{project_code}/workflow-instances/{id}"
        self._request("DELETE", path)
        return None

    def query_workflow_instance_by_id(
        self,
        project_code: int,
        id: int
    ) -> WorkflowInstance:
        """
        Query Workflow Instance By Id
        
        Query workflow instance by id
        
        DS operation: WorkflowInstanceController.queryWorkflowInstanceById | GET /projects/{projectCode}/workflow-instances/{id}
        
        Args:
            project_code: project code
            id: workflow instance id
        
        Returns:
            workflow instance detail
        """
        path = f"projects/{project_code}/workflow-instances/{id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowInstance))

    def update_workflow_instance(
        self,
        project_code: int,
        id: int,
        form: UpdateWorkflowInstanceParams
    ) -> WorkflowDefinition:
        """
        Update Workflow Instance
        
        Update workflow instance
        
        DS operation: WorkflowInstanceController.updateWorkflowInstance | PUT /projects/{projectCode}/workflow-instances/{id}
        
        Args:
            project_code: project code
            id: workflow instance id
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{project_code}/workflow-instances/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(WorkflowDefinition))

    def query_task_list_by_workflow_instance_id(
        self,
        project_code: int,
        id: int
    ) -> WorkflowInstanceTaskListView:
        """
        Query Task List By Workflow Instance Id
        
        Query task list by workflow instance id
        
        DS operation: WorkflowInstanceController.queryTaskListByWorkflowInstanceId | GET /projects/{projectCode}/workflow-instances/{id}/tasks
        
        Args:
            project_code: project code
            id: workflow instance id
        
        Returns:
            task list for the workflow instance
        """
        path = f"projects/{project_code}/workflow-instances/{id}/tasks"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowInstanceTaskListView))

    def view_tree(
        self,
        project_code: int,
        id: int
    ) -> GanttDto:
        """
        Vie Gantt Tree
        
        Encapsulation gantt structure
        
        DS operation: WorkflowInstanceController.viewTree | GET /projects/{projectCode}/workflow-instances/{id}/view-gantt
        
        Args:
            project_code: project code
            id: workflow instance id
        
        Returns:
            gantt tree data
        """
        path = f"projects/{project_code}/workflow-instances/{id}/view-gantt"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(GanttDto))

    def view_variables(
        self,
        project_code: int,
        id: int
    ) -> WorkflowInstanceVariablesView:
        """
        View Variables
        
        Query workflow instance global variables and local variables
        
        DS operation: WorkflowInstanceController.viewVariables | GET /projects/{projectCode}/workflow-instances/{id}/view-variables
        
        Args:
            id: workflow instance id
        
        Returns:
            variables data
        """
        path = f"projects/{project_code}/workflow-instances/{id}/view-variables"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(WorkflowInstanceVariablesView))

__all__ = ["WorkflowInstanceOperations"]
