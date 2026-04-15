from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...common.enums.command_type import CommandType
from ...common.enums.complement_dependent_mode import ComplementDependentMode
from ...common.enums.execution_order import ExecutionOrder
from ...common.enums.failure_strategy import FailureStrategy
from ...common.enums.priority import Priority
from ...common.enums.run_mode import RunMode
from ...common.enums.task_depend_type import TaskDependType
from ...common.enums.warning_type import WarningType
from ..enums.execute_type import ExecuteType

class BatchControlWorkflowInstanceParams(BaseParamsModel):
    """
    Batch Execute
    
    Form parameters for ExecutorController.batchControlWorkflowInstance.
    """
    workflowInstanceIds: str = Field(description='workflow instance ids, delimiter by "," if more than one id')
    executeType: ExecuteType = Field(description='execute type')

class BatchTriggerWorkflowDefinitionsParams(BaseParamsModel):
    """
    Batch Start Workflow Instance
    
    Form parameters for ExecutorController.batchTriggerWorkflowDefinitions.
    """
    workflowDefinitionCodes: str = Field(description='workflow definition codes', examples=['1,2,3'])
    scheduleTime: str = Field(description='schedule time', examples=['2022-04-06 00:00:00,2022-04-06 00:00:00'])
    failureStrategy: FailureStrategy = Field(description='failure strategy')
    startNodeList: str | None = Field(default=None, description='start nodes list')
    taskDependType: TaskDependType | None = Field(default=None, description='task depend type')
    execType: CommandType | None = Field(default=None, description='execute type')
    warningType: WarningType = Field(description='warning type')
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    runMode: RunMode | None = Field(default=None, description='run mode')
    workflowInstancePriority: Priority | None = Field(default=None, description='workflow instance priority')
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, description='tenant code', examples=['default'])
    environmentCode: int | None = Field(default=None, examples=[-1])
    startParams: str | None = Field(default=None)
    expectedParallelismNumber: int | None = Field(default=None, description='the expected parallelism number when execute complement in parallel mode', examples=[8])
    dryRun: int | None = Field(default=None, examples=[0])
    complementDependentMode: ComplementDependentMode | None = Field(default=None)
    allLevelDependent: bool | None = Field(default=None, examples=[False])
    executionOrder: ExecutionOrder | None = Field(default=None, description='complement data in some kind of order')

class ControlWorkflowInstanceParams(BaseParamsModel):
    """
    Execute
    
    Form parameters for ExecutorController.controlWorkflowInstance.
    """
    workflowInstanceId: int = Field(examples=[100])
    executeType: ExecuteType

class ExecuteTaskParams(BaseParamsModel):
    """
    Execute Task
    
    Form parameters for ExecutorController.executeTask.
    """
    workflowInstanceId: int = Field(description='workflow instance id', examples=[100])
    startNodeList: str = Field(description='start node list')
    taskDependType: TaskDependType = Field(description='task depend type')

class TriggerWorkflowDefinitionParams(BaseParamsModel):
    """
    Start Workflow Instance
    
    Form parameters for ExecutorController.triggerWorkflowDefinition.
    """
    workflowDefinitionCode: int = Field(description='workflow definition code', examples=[100])
    scheduleTime: str = Field(description='schedule time when CommandType is COMPLEMENT_DATA  there are two ways to transfer parameters 1.date range, for example:{"complementStartDate":"2022-01-01 12:12:12","complementEndDate":"2022-01-6 12:12:12"} 2.manual input,  for example:{"complementScheduleDateList":"2022-01-01 00:00:00,2022-01-02 12:12:12,2022-01-03 12:12:12"}', examples=['2022-04-06 00:00:00,2022-04-06 00:00:00'])
    failureStrategy: FailureStrategy = Field(description='failure strategy')
    startNodeList: str | None = Field(default=None, description='start nodes list')
    taskDependType: TaskDependType | None = Field(default=None, description='task depend type')
    execType: CommandType | None = Field(default=None, description='execute type')
    warningType: WarningType = Field(description='warning type')
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    runMode: RunMode | None = Field(default=None, description='run mode')
    workflowInstancePriority: Priority | None = Field(default=None, description='workflow instance priority')
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, examples=['default'])
    environmentCode: int | None = Field(default=None, examples=[-1])
    startParams: str | None = Field(default=None)
    expectedParallelismNumber: int | None = Field(default=None, description='the expected parallelism number when execute complement in parallel mode', examples=[8])
    dryRun: int | None = Field(default=None, examples=[0])
    complementDependentMode: ComplementDependentMode | None = Field(default=None)
    allLevelDependent: bool | None = Field(default=None, examples=[False])
    executionOrder: ExecutionOrder | None = Field(default=None, description='complement data in some kind of order')

class StartStreamTaskInstanceParams(BaseParamsModel):
    """
    Start Task Instance
    
    Form parameters for ExecutorController.startStreamTaskInstance.
    """
    version: int = Field(description='taskDefinitionVersion', examples=[1])
    warningGroupId: int | None = Field(default=None, description='warning group id', examples=[100])
    workerGroup: str | None = Field(default=None, description='worker group', examples=['default'])
    tenantCode: str | None = Field(default=None, examples=['default'])
    environmentCode: int | None = Field(default=None, examples=[-1])
    startParams: str | None = Field(default=None)
    dryRun: int | None = Field(default=None, examples=[0])

class ExecutorOperations(BaseRequestsClient):
    def batch_control_workflow_instance(
        self,
        project_code: int,
        form: BatchControlWorkflowInstanceParams
    ) -> None:
        """
        Batch Execute
        
        Batch execute and do action to workflow instance
        
        DS operation: ExecutorController.batchControlWorkflowInstance | POST /projects/{projectCode}/executors/batch-execute
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            execute result code
        """
        path = f"projects/{project_code}/executors/batch-execute"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def batch_trigger_workflow_definitions(
        self,
        project_code: int,
        form: BatchTriggerWorkflowDefinitionsParams
    ) -> list[int]:
        """
        Batch Start Workflow Instance
        
        Batch execute workflow instance If any workflowDefinitionCode cannot be found, the failure information is returned and the status is set to failed. The successful task will run normally and will not stop
        
        DS operation: ExecutorController.batchTriggerWorkflowDefinitions | POST /projects/{projectCode}/executors/batch-start-workflow-instance
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            start workflow result code
        """
        path = f"projects/{project_code}/executors/batch-start-workflow-instance"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(list[int]))

    def control_workflow_instance(
        self,
        project_code: int,
        form: ControlWorkflowInstanceParams
    ) -> None:
        """
        Execute
        
        Do action to workflow instance: pause, stop, repeat, recover from pause, recover from stop
        
        DS operation: ExecutorController.controlWorkflowInstance | POST /projects/{projectCode}/executors/execute
        
        Args:
            form: Form parameters bag for this operation.
        """
        path = f"projects/{project_code}/executors/execute"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def execute_task(
        self,
        project_code: int,
        form: ExecuteTaskParams
    ) -> None:
        """
        Execute Task
        
        Do action to workflow instance: pause, stop, repeat, recover from pause, recover from stop
        
        DS operation: ExecutorController.executeTask | POST /projects/{projectCode}/executors/execute-task
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            execute result code
        """
        path = f"projects/{project_code}/executors/execute-task"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

    def trigger_workflow_definition(
        self,
        project_code: int,
        form: TriggerWorkflowDefinitionParams
    ) -> list[int]:
        """
        Start Workflow Instance
        
        Execute workflow instance
        
        DS operation: ExecutorController.triggerWorkflowDefinition | POST /projects/{projectCode}/executors/start-workflow-instance
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            start workflow result code
        """
        path = f"projects/{project_code}/executors/start-workflow-instance"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(list[int]))

    def start_stream_task_instance(
        self,
        project_code: int,
        code: int,
        form: StartStreamTaskInstanceParams
    ) -> bool:
        """
        Start Task Instance
        
        Execute task instance
        
        DS operation: ExecutorController.startStreamTaskInstance | POST /projects/{projectCode}/executors/task-instance/{code}/start
        
        Args:
            project_code: project code
            code: taskDefinitionCode
            form: Form parameters bag for this operation.
        
        Returns:
            start task result code
        """
        path = f"projects/{project_code}/executors/task-instance/{code}/start"
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(bool))

__all__ = ["ExecutorOperations"]
