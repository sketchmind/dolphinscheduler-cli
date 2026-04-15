from __future__ import annotations

from ._base import BaseRequestsClient, BaseParamsModel

from pydantic import Field, TypeAdapter

from ..configuration.oauth2_configuration import OAuth2ConfigurationOAuth2ClientProperties

class LoginParams(BaseParamsModel):
    """
    Login
    
    Form parameters for LoginController.login.
    """
    userName: str = Field(description='user name')
    userPassword: str = Field(description='user password')

class HandleOidcCallbackParams(BaseParamsModel):
    """
    Handle Oidc Callback
    
    Query parameters for LoginController.handleOidcCallback.
    """
    code: str | None = Field(default=None, description='authorization code')
    error: str | None = Field(default=None)
    state: str = Field(description='state parameter')

class LoginByAuth2Params(BaseParamsModel):
    """
    Redirect To Oauth2
    
    Query parameters for LoginController.loginByAuth2.
    """
    code: str
    provider: str

class LoginOperations(BaseRequestsClient):
    def clear_cookie_session_id(
        self
    ) -> None:
        """
        LoginController.clearCookieSessionId
        
        DS operation: LoginController.clearCookieSessionId | DELETE /cookies
        """
        self._request("DELETE", "cookies")
        return None

    def login(
        self,
        form: LoginParams
    ) -> dict[str, str]:
        """
        Login
        
        DS operation: LoginController.login | POST /login
        
        Args:
            form: Form parameters bag for this operation.
        
        Returns:
            login result
        """
        data = self._model_mapping(form)
        payload = self._request(
            "POST",
            "login",
        data=data,
        )
        return self._validate_payload(payload, TypeAdapter(dict[str, str]))

    def handle_oidc_callback(
        self,
        provider_id: str,
        params: HandleOidcCallbackParams
    ) -> None:
        """
        Handle Oidc Callback
        
        Handle OIDC callback
        
        DS operation: LoginController.handleOidcCallback | GET /login/oauth2/code/{providerId}
        
        Args:
            params: Query parameters bag for this operation.
        """
        path = f"login/oauth2/code/{provider_id}"
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            path,
        params=query_params,
        )
        return None

    def sso_login(
        self
    ) -> str | None:
        """
        Sso login
        
        DS operation: LoginController.ssoLogin | GET /login/sso
        
        Returns:
            sso server url
        """
        payload = self._request("GET", "login/sso")
        return self._validate_payload(payload, TypeAdapter(str | None))

    def oauth2_provider(
        self
    ) -> list[OAuth2ConfigurationOAuth2ClientProperties]:
        """
        Get Oauth2 Provider
        
        DS operation: LoginController.oauth2Provider | GET /oauth2-provider
        """
        payload = self._request("GET", "oauth2-provider")
        return self._validate_payload(payload, TypeAdapter(list[OAuth2ConfigurationOAuth2ClientProperties]))

    def redirect_to_oidc(
        self,
        provider_id: str
    ) -> None:
        """
        Redirect To Oidc
        
        DS operation: LoginController.redirectToOidc | GET /oauth2/authorization/{providerId}
        """
        path = f"oauth2/authorization/{provider_id}"
        self._request("GET", path)
        return None

    def oidc_providers(
        self
    ) -> list[dict[str, str]] | None:
        """
        Get Oidc Providers
        
        Get OIDC providers
        
        DS operation: LoginController.oidcProviders | GET /oidc-providers
        
        Returns:
            list of OIDC providers
        """
        payload = self._request("GET", "oidc-providers")
        return self._validate_payload(payload, TypeAdapter(list[dict[str, str]] | None))

    def login_by_auth2(
        self,
        params: LoginByAuth2Params
    ) -> None:
        """
        Redirect To Oauth2
        
        DS operation: LoginController.loginByAuth2 | GET /redirect/login/oauth2
        
        Args:
            params: Query parameters bag for this operation.
        """
        query_params = self._model_mapping(params)
        self._request(
            "GET",
            "redirect/login/oauth2",
        params=query_params,
        )
        return None

    def sign_out(
        self
    ) -> None:
        """
        Sign Out
        
        DS operation: LoginController.signOut | POST /signOut
        """
        self._request("POST", "signOut")
        return None

__all__ = ["LoginOperations"]
