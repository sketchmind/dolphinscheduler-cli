from __future__ import annotations

from pydantic import Field
from ..._models import BaseContractModel

class OAuth2ConfigurationOAuth2ClientProperties(BaseContractModel):
    authorizationUri: str | None = Field(default=None)
    clientId: str | None = Field(default=None)
    redirectUri: str | None = Field(default=None)
    clientSecret: str | None = Field(default=None)
    tokenUri: str | None = Field(default=None)
    userInfoUri: str | None = Field(default=None)
    callbackUrl: str | None = Field(default=None)
    iconUri: str | None = Field(default=None)
    provider: str | None = Field(default=None)

__all__ = ["OAuth2ConfigurationOAuth2ClientProperties"]
