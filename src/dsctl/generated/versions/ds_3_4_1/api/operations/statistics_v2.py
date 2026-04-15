from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ..contracts.define_user_dto import DefineUserDto
from ..contracts.project.statistics_state_request import StatisticsStateRequest
from ..contracts.task_count_dto import TaskCountDto

class StatisticsV2Operations(BaseRequestsClient):
    def query_task_states_counts(
        self,
        statistics_state_request: StatisticsStateRequest
    ) -> TaskCountDto:
        """
        Query All Task States Count
        
        Query all task states count
        
        DS operation: StatisticsV2Controller.queryTaskStatesCounts | GET /v2/statistics/tasks/states/count
        
        Args:
            statistics_state_request: Request body payload.
        
        Returns:
            tasks states count
        """
        payload = self._request(
            "GET",
            "v2/statistics/tasks/states/count",
        json=self._json_payload(statistics_state_request),
        )
        return self._validate_payload(payload, TypeAdapter(TaskCountDto))

    def query_one_task_states_counts(
        self,
        task_code: int
    ) -> TaskCountDto:
        """
        Query One Task States Count
        
        Query one task states count
        
        DS operation: StatisticsV2Controller.queryOneTaskStatesCounts | GET /v2/statistics/tasks/{taskCode}/states/count
        
        Args:
            task_code: taskCode
        
        Returns:
            tasks states count
        """
        path = f"v2/statistics/tasks/{task_code}/states/count"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(TaskCountDto))

    def query_workflow_states_counts(
        self,
        statistics_state_request: StatisticsStateRequest
    ) -> TaskCountDto:
        """
        Query All Workflow States Count
        
        Query all workflow states count
        
        DS operation: StatisticsV2Controller.queryWorkflowStatesCounts | GET /v2/statistics/workflows/states/count
        
        Args:
            statistics_state_request: Request body payload.
        
        Returns:
            workflow states count
        """
        payload = self._request(
            "GET",
            "v2/statistics/workflows/states/count",
        json=self._json_payload(statistics_state_request),
        )
        return self._validate_payload(payload, TypeAdapter(TaskCountDto))

    def count_definition_by_user(
        self,
        statistics_state_request: StatisticsStateRequest
    ) -> DefineUserDto:
        """
        Count Definition V2 By User Id
        
        Statistics the workflow quantities of certain user
        
        DS operation: StatisticsV2Controller.countDefinitionByUser | GET /v2/statistics/workflows/users/count
        
        Args:
            statistics_state_request: Request body payload.
        
        Returns:
            workflow count in project code
        """
        payload = self._request(
            "GET",
            "v2/statistics/workflows/users/count",
        json=self._json_payload(statistics_state_request),
        )
        return self._validate_payload(payload, TypeAdapter(DefineUserDto))

    def count_definition_by_user_id(
        self,
        user_id: int
    ) -> DefineUserDto:
        """
        Count Definition V2 By User
        
        Statistics the workflow quantities of certain userId
        
        DS operation: StatisticsV2Controller.countDefinitionByUserId | GET /v2/statistics/workflows/users/{userId}/count
        
        Args:
            user_id: userId
        
        Returns:
            workflow count in project code
        """
        path = f"v2/statistics/workflows/users/{user_id}/count"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(DefineUserDto))

    def count_definition_by_user_state(
        self,
        user_id: int,
        release_state: int
    ) -> DefineUserDto:
        """
        Count Definition V2 By User
        
        Statistics the workflow quantities of certain userId and releaseState
        
        DS operation: StatisticsV2Controller.countDefinitionByUserState | GET /v2/statistics/workflows/users/{userId}/{releaseState}/count
        
        Args:
            user_id: userId
            release_state: releaseState
        
        Returns:
            workflow count in project code
        """
        path = f"v2/statistics/workflows/users/{user_id}/{release_state}/count"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(DefineUserDto))

    def query_one_workflow_states(
        self,
        workflow_code: int
    ) -> TaskCountDto:
        """
        Query One Workflow States Count
        
        QUERY_One_WORKFLOW_STATES_COUNT
        
        Query one workflow states count
        
        DS operation: StatisticsV2Controller.queryOneWorkflowStates | GET /v2/statistics/{workflowCode}/states/count
        
        Args:
            workflow_code: workflowCode
        
        Returns:
            workflow states count
        """
        path = f"v2/statistics/{workflow_code}/states/count"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(TaskCountDto))

__all__ = ["StatisticsV2Operations"]
