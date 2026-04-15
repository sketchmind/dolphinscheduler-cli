from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.project import Project
from ...dao.entities.user import User
from ..contracts.page_info import PageInfoProject

class QueryProjectListPagingParams(BaseParamsModel):
    """
    Query Project List Paging
    
    Query parameters for ProjectController.queryProjectListPaging.
    """
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[10])
    pageNo: int = Field(description='page number', examples=[1])

class CreateProjectParams(BaseParamsModel):
    """
    Create
    
    Form parameters for ProjectController.createProject.
    """
    projectName: str = Field(description='project name')
    description: str | None = Field(default=None, description='description')

class QueryAuthorizedProjectParams(BaseParamsModel):
    """
    Query Authorized Project
    
    Query parameters for ProjectController.queryAuthorizedProject.
    """
    userId: int = Field(description='user id', examples=[100])

class QueryAuthorizedUserParams(BaseParamsModel):
    """
    Query Authorized User
    
    Query parameters for ProjectController.queryAuthorizedUser.
    """
    projectCode: int = Field(description='project code', examples=[100])

class QueryProjectWithAuthorizedLevelParams(BaseParamsModel):
    """
    Query Project With Authorized Level
    
    Query parameters for ProjectController.queryProjectWithAuthorizedLevel.
    """
    userId: int = Field(description='user id', examples=[100])

class QueryProjectWithAuthorizedLevelListPagingParams(BaseParamsModel):
    """
    Query Project With Authorized Level List Paging
    
    Query parameters for ProjectController.queryProjectWithAuthorizedLevelListPaging.
    """
    userId: int = Field(description='user id', examples=[100])
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[10])
    pageNo: int = Field(description='page number', examples=[1])

class QueryUnauthorizedProjectParams(BaseParamsModel):
    """
    Query Unauthorized Project
    
    Query parameters for ProjectController.queryUnauthorizedProject.
    """
    userId: int = Field(description='user id', examples=[100])

class UpdateProjectParams(BaseParamsModel):
    """
    Update
    
    Form parameters for ProjectController.updateProject.
    """
    projectName: str = Field(description='project name')
    description: str | None = Field(default=None, description='description')

class ProjectOperations(BaseRequestsClient):
    def query_project_list_paging(
        self,
        params: QueryProjectListPagingParams
    ) -> PageInfoProject:
        """
        Query Project List Paging
        
        Query project list paging
        
        DS operation: ProjectController.queryProjectListPaging | GET /projects
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            project list which the login user have permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoProject))

    def create_project(
        self,
        form: CreateProjectParams
    ) -> Project:
        """
        Create
        
        Create project
        
        DS operation: ProjectController.createProject | POST /projects
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            returns an error if it exists
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "projects",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Project))

    def query_authorized_project(
        self,
        params: QueryAuthorizedProjectParams
    ) -> list[Project]:
        """
        Query Authorized Project
        
        Query authorized project
        
        DS operation: ProjectController.queryAuthorizedProject | GET /projects/authed-project
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            projects which the user have permission to see, Except for items created by this user
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/authed-project",
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
        
        DS operation: ProjectController.queryAuthorizedUser | GET /projects/authed-user
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            users        who have permission for the specified project
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/authed-user",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def query_project_created_and_authorized_by_user(
        self
    ) -> list[Project]:
        """
        Query Project Created And Authorized By User
        
        Query authorized and user created project
        
        DS operation: ProjectController.queryProjectCreatedAndAuthorizedByUser | GET /projects/created-and-authed
        
        Returns:
            projects which the user create and authorized
        """
        payload = self._request("GET", "projects/created-and-authed")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_all_project_list(
        self
    ) -> list[Project]:
        """
        Query All Project List
        
        Query all project list
        
        DS operation: ProjectController.queryAllProjectList | GET /projects/list
        
        Returns:
            all project list
        """
        payload = self._request("GET", "projects/list")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_all_project_list_for_dependent(
        self
    ) -> list[Project]:
        """
        Query All Project List For Dependent
        
        Query all project list for dependent
        
        DS operation: ProjectController.queryAllProjectListForDependent | GET /projects/list-dependent
        
        Returns:
            all project list
        """
        payload = self._request("GET", "projects/list-dependent")
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_project_with_authorized_level(
        self,
        params: QueryProjectWithAuthorizedLevelParams
    ) -> list[Project]:
        """
        Query Project With Authorized Level
        
        Query all project with authorized level
        
        DS operation: ProjectController.queryProjectWithAuthorizedLevel | GET /projects/project-with-authorized-level
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            All projects with users' authorized level for them
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/project-with-authorized-level",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def query_project_with_authorized_level_list_paging(
        self,
        params: QueryProjectWithAuthorizedLevelListPagingParams
    ) -> PageInfoProject:
        """
        Query Project With Authorized Level List Paging
        
        Query project with authorized level list paging
        
        DS operation: ProjectController.queryProjectWithAuthorizedLevelListPaging | GET /projects/project-with-authorized-level-list-paging
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            project list which with the login user's authorized level
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/project-with-authorized-level-list-paging",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoProject))

    def query_unauthorized_project(
        self,
        params: QueryUnauthorizedProjectParams
    ) -> list[Project]:
        """
        Query Unauthorized Project
        
        Query unauthorized project
        
        DS operation: ProjectController.queryUnauthorizedProject | GET /projects/unauth-project
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            the projects which user have no permission to see
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "projects/unauth-project",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[Project]))

    def delete_project(
        self,
        code: int
    ) -> None:
        """
        Delete
        
        Delete project by code
        
        DS operation: ProjectController.deleteProject | DELETE /projects/{code}
        
        Args:
            code: project code
        
        Returns:
            delete result code
        """
        path = f"projects/{code}"
        self._request("DELETE", path)
        return None

    def query_project_by_code(
        self,
        code: int
    ) -> Project:
        """
        Query Project By Code
        
        Query project details by code
        
        DS operation: ProjectController.queryProjectByCode | GET /projects/{code}
        
        Args:
            code: project code
        
        Returns:
            project detail information
        """
        path = f"projects/{code}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(Project))

    def update_project(
        self,
        code: int,
        form: UpdateProjectParams
    ) -> Project:
        """
        Update
        
        Update project
        
        DS operation: ProjectController.updateProject | PUT /projects/{code}
        
        Args:
            code: project code
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        path = f"projects/{code}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(Project))

__all__ = ["ProjectOperations"]
