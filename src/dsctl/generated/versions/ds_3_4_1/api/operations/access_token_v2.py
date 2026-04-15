from __future__ import annotations

from ._base import BaseRequestsClient

from pydantic import TypeAdapter

from ...dao.entities.access_token import AccessToken
from ..contracts.create_token_request import CreateTokenRequest

class AccessTokenV2Operations(BaseRequestsClient):
    def create_token(
        self,
        create_token_request: CreateTokenRequest
    ) -> AccessToken:
        """
        Create Token V2
        
        Create token
        
        DS operation: AccessTokenV2Controller.createToken | POST /v2/access-tokens
        
        Args:
            create_token_request: Request body payload.
        
        Returns:
            CreateTokenResponse CreateTokenResponse
        """
        payload = self._request(
            "POST",
            "v2/access-tokens",
        json=self._json_payload(create_token_request),
        )
        return self._validate_payload(payload, TypeAdapter(AccessToken))

__all__ = ["AccessTokenV2Operations"]
