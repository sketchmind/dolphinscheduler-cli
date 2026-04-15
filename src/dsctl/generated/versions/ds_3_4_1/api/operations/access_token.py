from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ...dao.entities.access_token import AccessToken
from ..contracts.page_info import PageInfoAccessToken

class QueryAccessTokenListParams(BaseParamsModel):
    """
    Query Access Token List
    
    Query parameters for AccessTokenController.queryAccessTokenList.
    """
    pageNo: int = Field(description='page number', examples=[1])
    searchVal: str | None = Field(default=None, description='search value')
    pageSize: int = Field(description='page size', examples=[20])

class CreateTokenParams(BaseParamsModel):
    """
    Create Token
    
    Form parameters for AccessTokenController.createToken.
    """
    userId: int = Field(description='token for user id')
    expireTime: str = Field(description='expire time for the token', examples=['2021-12-31 00:00:00'])
    token: str | None = Field(default=None, description='token string (if it is absent, it will be automatically generated)', examples=['xxxx'])

class GenerateTokenParams(BaseParamsModel):
    """
    Generate token string
    
    Form parameters for AccessTokenController.generateToken.
    """
    userId: int = Field(description='token for user')
    expireTime: str = Field(description='expire time')

class UpdateTokenParams(BaseParamsModel):
    """
    Update Token
    
    Form parameters for AccessTokenController.updateToken.
    """
    userId: int = Field(description='token for user')
    expireTime: str = Field(description='token expire time', examples=['2021-12-31 00:00:00'])
    token: str | None = Field(default=None, description='token string (if it is absent, it will be automatically generated)', examples=['xxxx'])

class AccessTokenOperations(BaseRequestsClient):
    def query_access_token_list(
        self,
        params: QueryAccessTokenListParams
    ) -> PageInfoAccessToken:
        """
        Query Access Token List
        
        Query access token list paging
        
        DS operation: AccessTokenController.queryAccessTokenList | GET /access-tokens
        
        Args:
            params: Query parameters bag for this operation.
        
        Returns:
            token list of page number and page size
        """
        query_params = self._model_mapping(params)
        payload = self._request(
            "GET",
            "access-tokens",
        params=query_params,
        )
        return self._validate_payload(payload, TypeAdapter(PageInfoAccessToken))

    def create_token(
        self,
        form: CreateTokenParams
    ) -> AccessToken:
        """
        Create Token
        
        Create token
        
        DS operation: AccessTokenController.createToken | POST /access-tokens
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            create result state code
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "access-tokens",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AccessToken))

    def generate_token(
        self,
        form: GenerateTokenParams
    ) -> str:
        """
        Generate token string
        
        DS operation: AccessTokenController.generateToken | POST /access-tokens/generate
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            token string
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "access-tokens/generate",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(str))

    def query_access_token_by_user(
        self,
        user_id: int
    ) -> list[AccessToken]:
        """
        Query Access Token By User
        
        Query access token for specified user
        
        DS operation: AccessTokenController.queryAccessTokenByUser | GET /access-tokens/user/{userId}
        
        Args:
            user_id: user id
        
        Returns:
            token list for specified user
        """
        path = f"access-tokens/user/{user_id}"
        payload = self._request("GET", path)
        return self._validate_payload(payload, TypeAdapter(list[AccessToken]))

    def del_access_token_by_id(
        self,
        id: int
    ) -> bool:
        """
        Delete Token
        
        Delete access token by id
        
        DS operation: AccessTokenController.delAccessTokenById | DELETE /access-tokens/{id}
        
        Args:
            id: token id
        
        Returns:
            delete result code
        """
        path = f"access-tokens/{id}"
        payload = self._request("DELETE", path)
        return self._validate_payload(payload, TypeAdapter(bool))

    def update_token(
        self,
        id: int,
        form: UpdateTokenParams
    ) -> AccessToken:
        """
        Update Token
        
        Update token
        
        DS operation: AccessTokenController.updateToken | PUT /access-tokens/{id}
        
        Args:
            id: token id
            form: Form parameters bag for this operation.
        
        Returns:
            updated access token entity
        """
        path = f"access-tokens/{id}"
        data = self._model_mapping(form)
        payload = self._request(
            "PUT",
            path,
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(AccessToken))

__all__ = ["AccessTokenOperations"]
