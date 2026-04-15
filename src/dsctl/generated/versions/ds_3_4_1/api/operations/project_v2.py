from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.project import Project
from ...dao.entities.user import User
from ..contracts.page_info import PageInfoProject
from ..contracts.project.project_create_request import ProjectCreateRequest
from ..contracts.project.project_query_request import ProjectQueryRequest
from ..contracts.project.project_update_request import ProjectUpdateRequest

class QueryAuthorizedProjectParams(BaseParamsModel):
    """
    Query Authorized Project
    
    Query parameters for ProjectV2Controller.queryAuthorizedProject.
    """
    userId: int = Field(description='user id', examples=[100])

class QueryAuthorizedUserParams(BaseParamsModel):
    """
    Query Authorized User
    
    Query parameters for ProjectV2Controller.queryAuthorizedUser.
    """
    projectCode: int = Field(description='project code', examples=[100])

class QueryUnauthorizedProjectParams(BaseParamsModel):
    """
    Query Unauthorized Project
    
    Query parameters for ProjectV2Controller.queryUnauthorizedProject.
    """
    userId: int = Field(description='user id', examples=[100])

class ProjectV2Operations(BaseRequestsClient):
    def query_project_list_paging(
        self,
        request: ProjectQueryRequest
    ) -> PageInfoProject:
        """
        Query Project List Paging
        
        Query project list paging
        
        DS operation: ProjectV2Controller.queryProjectListPaging | GET /v2/projects
        
        Args:
            request: Request payload.
        
        Returns:
            project list which the login user have permission to see
        """
        query_params = self._model_mapping(request)
        headers = {"Content-Type": "application/json"}
        payload = self._request(
            "GET",
            "v2/projects",
        params=query_params,
        headers=headers,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoProject))

    def create_project(
        self,
        project_create_request: ProjectCreateRequest
    ) -> Project:
        """
        Create
        
        Create project
        
        DS operation: ProjectV2Controller.createProject | POST /v2/projects
        
        Args:
            project_create_request: Request body payload.
        
        Returns:
            ProjectResponse ProjectResponse
        """
        payload = self._request(
            "POST",
            "v2/projects",
        json=self._json_payload(project_create_request),
        )
        return self._validate_payload(payload, TypeAdapter(Project))

    def query_authorized_project(
        self,
        params: QueryAuthorizedProjectParams
    ) -> list[Project]:
        """
        Query Authorized Project
        
        Query authorized project
        
        DS operation: ProjectV2Controller.queryAuthorizedProject | GET /v2/projects/authed-project
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            projects which the user have permission to see, Except for items created by this user
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "v2/projects/authed-project",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_authorized_user(
        self,
        params: QueryAuthorizedUserParams
    ) -> list[User]:
        """
        Query Authorized User
        
        Query authorized user
        
        DS operation: ProjectV2Controller.queryAuthorizedUser | GET /v2/projects/authed-user
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            users        who have permission for the specified project
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "v2/projects/authed-user",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def query_project_created_and_authorized_by_user(
        self
    ) -> list[Project]:
        """
        Query Project Created And Authorized By User
        
        Query authorized and user created project
        
        DS operation: ProjectV2Controller.queryProjectCreatedAndAuthorizedByUser | GET /v2/projects/created-and-authed
        
        Returns:
            projects which the user create and authorized
        """
        payload = self._request("GET", "v2/projects/created-and-authed")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_all_project_list(
        self
    ) -> list[Project]:
        """
        Query All Project List
        
        Query all project list
        
        DS operation: ProjectV2Controller.queryAllProjectList | GET /v2/projects/list
        
        Returns:
            all project list
        """
        payload = self._request("GET", "v2/projects/list")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_all_project_list_for_dependent(
        self
    ) -> list[Project]:
        """
        Query All Project List For Dependent
        
        Query all project list for dependent
        
        DS operation: ProjectV2Controller.queryAllProjectListForDependent | GET /v2/projects/list-dependent
        
        Returns:
            all project list
        """
        payload = self._request("GET", "v2/projects/list-dependent")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_unauthorized_project(
        self,
        params: QueryUnauthorizedProjectParams
    ) -> list[Project]:
        """
        Query Unauthorized Project
        
        Query unauthorized project
        
        DS operation: ProjectV2Controller.queryUnauthorizedProject | GET /v2/projects/unauth-project
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            the projects which user have no permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "v2/projects/unauth-project",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def delete_project(
        self,
        code: int
    ) -> bool:
        """
        Delete
        
        Delete project by code
        
        DS operation: ProjectV2Controller.deleteProject | DELETE /v2/projects/{code}
        
        Args:
            code: project code
        
        Returns:
            delete result code
        """
        path = f"v2/projects/{code}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def query_project_by_code(
        self,
        code: int
    ) -> Project:
        """
        Query Project By Code
        
        Query project details by project code
        
        DS operation: ProjectV2Controller.queryProjectByCode | GET /v2/projects/{code}
        
        Args:
            code: project code
        
        Returns:
            project detail information
        """
        path = f"v2/projects/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(Project))

    def update_project(
        self,
        code: int,
        project_update_req: ProjectUpdateRequest
    ) -> Project:
        """
        Update
        
        Update project
        
        DS operation: ProjectV2Controller.updateProject | PUT /v2/projects/{code}
        
        Args:
            code: project code
            project_update_req: Request body payload.
        
        Returns:
            result Result
        """
        path = f"v2/projects/{code}"
        payload = self._request(
            "PUT",
            path,
        json=self._json_payload(project_update_req),
        )
        return self._validate_payload(payload, TypeAdapter(Project))

__all__ = ["ProjectV2Operations"]
