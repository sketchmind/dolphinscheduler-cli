from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.user import User
from ..contracts.page_info import PageInfoUser
from ..views.users import UsersBatchActivateResult

class ActivateUserParams(BaseParamsModel):
    """
    Activate User
    
    Form parameters for UsersController.activateUser.
    """
    userName: str = Field(description='user name')

class AuthorizedUserParams(BaseParamsModel):
    """
    Authorized User
    
    Query parameters for UsersController.authorizedUser.
    """
    alertgroupId: int = Field(description='alert group id')

class CreateUserParams(BaseParamsModel):
    """
    Create User
    
    Form parameters for UsersController.createUser.
    """
    userName: str
    userPassword: str
    tenantId: int = Field(examples=[100])
    queue: str | None = Field(default=None)
    email: str
    phone: str | None = Field(default=None)
    state: int | None = Field(default=None, examples=[1])

class DelUserByIdParams(BaseParamsModel):
    """
    Del User By Id
    
    Form parameters for UsersController.delUserById.
    """
    id: int = Field(description='user id', examples=[100])

class GrantDataSourceParams(BaseParamsModel):
    """
    Grant Data Source
    
    Form parameters for UsersController.grantDataSource.
    """
    userId: int = Field(description='user id', examples=[100])
    datasourceIds: str = Field(description='data source id array')

class GrantNamespaceParams(BaseParamsModel):
    """
    Grant Namespace
    
    Form parameters for UsersController.grantNamespace.
    """
    userId: int = Field(description='user id', examples=[100])
    namespaceIds: str = Field(description='namespace id array')

class GrantProjectParams(BaseParamsModel):
    """
    Grant Project
    
    Form parameters for UsersController.grantProject.
    """
    userId: int = Field(description='user id', examples=[100])
    projectIds: str = Field(description='project id array')

class GrantProjectByCodeParams(BaseParamsModel):
    """
    Grant Project By Code
    
    Form parameters for UsersController.grantProjectByCode.
    """
    userId: int = Field(description='user id', examples=[100])
    projectCode: int = Field(description='project code')

class GrantProjectWithReadPermParams(BaseParamsModel):
    """
    Grant Project With Read Perm
    
    Form parameters for UsersController.grantProjectWithReadPerm.
    """
    userId: int = Field(description='user id', examples=[100])
    projectIds: str = Field(description='project id array')

class QueryUserListParams(BaseParamsModel):
    """
    Query User List
    
    Query parameters for UsersController.queryUserList.
    """
    pageNo: int = Field(description='page number', examples=[1])
    pageSize: int = Field(description='page size', examples=[10])
    searchVal: str | None = Field(default=None, description='search avlue')

class RegisterUserParams(BaseParamsModel):
    """
    Register User
    
    Form parameters for UsersController.registerUser.
    """
    userName: str = Field(description='user name')
    userPassword: str = Field(description='user password')
    repeatPassword: str = Field(description='repeat password')
    email: str = Field(description='user email')

class RevokeProjectParams(BaseParamsModel):
    """
    Revoke Project
    
    Form parameters for UsersController.revokeProject.
    """
    userId: int = Field(description='user id', examples=[100])
    projectCode: int = Field(description='project code', examples=[100])

class RevokeProjectByIdParams(BaseParamsModel):
    """
    Revoke Project By Id
    
    Form parameters for UsersController.revokeProjectById.
    """
    userId: int = Field(description='user id', examples=[100])
    projectIds: str = Field(description='project id array')

class UnauthorizedUserParams(BaseParamsModel):
    """
    Unauthorized User
    
    Query parameters for UsersController.unauthorizedUser.
    """
    alertgroupId: int = Field(description='alert group id')

class UpdateUserParams(BaseParamsModel):
    """
    Update User
    
    Form parameters for UsersController.updateUser.
    """
    id: int = Field(description='user id', examples=[100])
    userName: str = Field(description='user name')
    userPassword: str = Field(description='user password')
    queue: str | None = Field(default=None, description='queue')
    email: str = Field(description='email')
    tenantId: int = Field(description='tennat id', examples=[100])
    phone: str | None = Field(default=None, description='phone')
    state: int | None = Field(default=None, examples=[1])
    timeZone: str | None = Field(default=None)

class VerifyUserNameParams(BaseParamsModel):
    """
    Verify User Name
    
    Query parameters for UsersController.verifyUserName.
    """
    userName: str = Field(description='user name')

class UsersOperations(BaseRequestsClient):
    def activate_user(
        self,
        form: ActivateUserParams
    ) -> User:
        """
        Activate User
        
        User activate
        
        DS operation: UsersController.activateUser | POST /users/activate
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "users/activate",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(User))

    def authorized_user(
        self,
        params: AuthorizedUserParams
    ) -> list[User]:
        """
        Authorized User
        
        Authorized user
        
        DS operation: UsersController.authorizedUser | GET /users/authed-user
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            authorized result code
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "users/authed-user",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def batch_activate_user(
        self,
        user_names: list[str]
    ) -> UsersBatchActivateResult:
        """
        Batch Activate User
        
        User batch activate
        
        DS operation: UsersController.batchActivateUser | POST /users/batch/activate
        
        Args:
            user_names: Request body payload.
        """
        payload = self._request(
            "POST",
            "users/batch/activate",
        json=self._json_payload(user_names),
        )
        return self._validate_payload(payload, TypeAdapter(UsersBatchActivateResult))

    def create_user(
        self,
        form: CreateUserParams
    ) -> User | None:
        """
        Create User
        
        DS operation: UsersController.createUser | POST /users/create
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "users/create",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(User | None))

    def del_user_by_id(
        self,
        form: DelUserByIdParams
    ) -> None:
        """
        Del User By Id
        
        Delete user by id
        
        DS operation: UsersController.delUserById | POST /users/delete
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            delete result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/delete",
        data=data,
        )
        return None

    def get_user_info(
        self
    ) -> User:
        """
        Get User Info
        
        Get user info
        
        DS operation: UsersController.getUserInfo | GET /users/get-user-info
        
        Returns:
            user info
        """
        payload = self._request("GET", "users/get-user-info")
        return self._validate_payload(payload, TypeAdapter(User))

    def grant_data_source(
        self,
        form: GrantDataSourceParams
    ) -> None:
        """
        Grant Data Source
        
        Grant datasource
        
        DS operation: UsersController.grantDataSource | POST /users/grant-datasource
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            grant result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/grant-datasource",
        data=data,
        )
        return None

    def grant_namespace(
        self,
        form: GrantNamespaceParams
    ) -> None:
        """
        Grant Namespace
        
        Grant namespace
        
        DS operation: UsersController.grantNamespace | POST /users/grant-namespace
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            grant result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/grant-namespace",
        data=data,
        )
        return None

    def grant_project(
        self,
        form: GrantProjectParams
    ) -> None:
        """
        Grant Project
        
        Grant project
        
        DS operation: UsersController.grantProject | POST /users/grant-project
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            grant result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/grant-project",
        data=data,
        )
        return None

    def grant_project_by_code(
        self,
        form: GrantProjectByCodeParams
    ) -> None:
        """
        Grant Project By Code
        
        Grant project by code
        
        DS operation: UsersController.grantProjectByCode | POST /users/grant-project-by-code
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            grant result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/grant-project-by-code",
        data=data,
        )
        return None

    def grant_project_with_read_perm(
        self,
        form: GrantProjectWithReadPermParams
    ) -> None:
        """
        Grant Project With Read Perm
        
        Grant project with read permission
        
        DS operation: UsersController.grantProjectWithReadPerm | POST /users/grant-project-with-read-perm
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            grant result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/grant-project-with-read-perm",
        data=data,
        )
        return None

    def list_user(
        self
    ) -> list[User]:
        """
        List User
        
        User list no paging
        
        DS operation: UsersController.listUser | GET /users/list
        
        Returns:
            user list
        """
        payload = self._request("GET", "users/list")
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def list_all(
        self
    ) -> list[User]:
        """
        User list no paging
        
        DS operation: UsersController.listAll | GET /users/list-all
        
        Returns:
            user list
        """
        payload = self._request("GET", "users/list-all")
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def query_user_list(
        self,
        params: QueryUserListParams
    ) -> PageInfoUser:
        """
        Query User List
        
        Query user list paging
        
        DS operation: UsersController.queryUserList | GET /users/list-paging
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            user list page
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "users/list-paging",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoUser))

    def register_user(
        self,
        form: RegisterUserParams
    ) -> User | None:
        """
        Register User
        
        User registry
        
        DS operation: UsersController.registerUser | POST /users/register
        
        Args:
            form: Form parameters bag for this operation.
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "users/register",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(User | None))

    def revoke_project(
        self,
        form: RevokeProjectParams
    ) -> None:
        """
        Revoke Project
        
        Revoke project
        
        DS operation: UsersController.revokeProject | POST /users/revoke-project
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            revoke result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/revoke-project",
        data=data,
        )
        return None

    def revoke_project_by_id(
        self,
        form: RevokeProjectByIdParams
    ) -> None:
        """
        Revoke Project By Id
        
        Revoke project By Id
        
        DS operation: UsersController.revokeProjectById | POST /users/revoke-project-by-id
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            revoke result code
        """
        data = self._model_mapping(form)
        self._request(
            "POST",
            "users/revoke-project-by-id",
        data=data,
        )
        return None

    def unauthorized_user(
        self,
        params: UnauthorizedUserParams
    ) -> list[User]:
        """
        Unauthorized User
        
        Unauthorized user
        
        DS operation: UsersController.unauthorizedUser | GET /users/unauth-user
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            unauthorize result code
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "users/unauth-user",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(list[User]))

    def update_user(
        self,
        form: UpdateUserParams
    ) -> User:
        """
        Update User
        
        Update user
        
        DS operation: UsersController.updateUser | POST /users/update
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            update result code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "users/update",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(User))

    def verify_user_name(
        self,
        params: VerifyUserNameParams
    ) -> None:
        """
        Verify User Name
        
        Verify username
        
        DS operation: UsersController.verifyUserName | GET /users/verify-user-name
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            true if user name not exists, otherwise return false
        """
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            "users/verify-user-name",
        params=query_params,
        )
        return None

__all__ = ["UsersOperations"]
