from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.project_worker_group import ProjectWorkerGroup

class AssignWorkerGroupsParams(BaseParamsModel):
    """
    Assign Worker Groups
    
    Form parameters for ProjectWorkerGroupController.assignWorkerGroups.
    """
    workerGroups: list[str]

class ProjectWorkerGroupOperations(BaseRequestsClient):
    def query_assigned_worker_groups(
        self,
        project_code: int
    ) -> list[ProjectWorkerGroup]:
        """
        Query Assigned Worker Groups
        
        Query worker groups that assigned to the project
        
        DS operation: ProjectWorkerGroupController.queryAssignedWorkerGroups | GET /projects/{projectCode}/worker-group
        
        Args:
            project_code: project code
        
        Returns:
            worker group list
        """
        path = f"projects/{project_code}/worker-group"
        payload = self._request("GET", path)
        payload = self._project_status_data(payload)
        return self._validate_payload(payload, TypeAdapter(list[ProjectWorkerGroup]))

    def assign_worker_groups(
        self,
        project_code: int,
        form: AssignWorkerGroupsParams
    ) -> None:
        """
        Assign Worker Groups
        
        Assign worker groups to the project
        
        DS operation: ProjectWorkerGroupController.assignWorkerGroups | POST /projects/{projectCode}/worker-group
        
        Args:
            project_code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            create result code
        """
        path = f"projects/{project_code}/worker-group"
        data = self._model_mapping(form)
        self._request(
            "POST",
            path,
        data=data,
        )
        return None

__all__ = ["ProjectWorkerGroupOperations"]
