"""
Module for authentication with cognito

WIP: This module is not yet complete

We have an issue with Pydantic version in order to this work
we need to migrate from pydantic v1 to v2 which inforce of to migrate
the fastapi version too.
"""

import os
from typing import Any
from fastapi import Depends, Request
from fastapi.security import HTTPBearer
from fastapi_cognito import CognitoAuth, CognitoSettings
from pydantic import BaseSettings, PyObject
from schemas.pydantic.cognito import CognitoJwt


USERPOOL = os.getenv("COGNITO_USERPOOL_ID", "")
CLIENT_ID = os.getenv("COGNITO_APP_CLIENT_ID", "")
REGION = os.getenv("COGNITO_REGION", "")


class CognitoAuthenticatorSettings(BaseSettings):
    """
    Settings for cognito authentication
    """

    check_expiration: bool = True
    jwt_header_prefix: str = "Bearer"
    jwt_header_name: str = "Authorization"
    userpools: dict[str, dict[str, Any]] = {
        "default": {
            "region": REGION,
            "userpool_id": USERPOOL,
            "app_client_id": CLIENT_ID,
        }
    }
    custom_cognito_token_model: PyObject = CognitoJwt


settings = CognitoAuthenticatorSettings()

cognito_default = CognitoAuth(
    settings=CognitoSettings.from_global_settings(settings),
    userpool_name="default",
)

security = HTTPBearer()


def auth_required(request: Request, _=Depends(security)):
    """
    This function is a dependency for FastAPI routes that require authentication using
    cognito. It will raise an exception if the user is not authenticated.

    :param request: FastAPI Request object

    :param _: HTTPBearer object
    This param is being ignored, but it is necessary to make FastAPI recognize This
    function as a dependency security dependency and allow the authorize button appear
    on the swagger UI
    """
    cognito_default.auth_required(request)
