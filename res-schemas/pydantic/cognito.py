from pydantic import BaseModel
from typing import Any, Dict


class CognitoJwt(BaseModel):
    token_use: str
    auth_time: int
    email: str
    iat: int
    exp: int
    given_name: str
    family_name: str
    raw_token: Dict[str, Any]

    def __init__(self, **kwargs):
        """
        Example decode token content

        {
            'at_hash': '',
            'sub': '',
            'cognito:groups': [''],
            'email_verified': False,
            'https://hasura.io/jwt/claims': '{"x-hasura-user-id": "", "x-hasura-user-email": "", "x-hasura-allowed-roles": ["user"], "x-hasura-default-role": "user", "x-hasura-brand-code": "RF", "x-hasura-brand-subdomain": "resonancemanufacturing"}',
            'iss': '',
            'cognito:username': '',
            'given_name': '',
            'picture': '',
            'aud': '4i6fnohp1um59dj2vlvhfma8tn',
            'identities': [{'userId': '', 'providerName': '', 'providerType': '', 'issuer': None, 'primary': 'true', 'dateCreated': ''}],
            'token_use': 'id',
            'auth_time': 1692888844,
            'name': '',
            'exp': 1693579680,
            'iat': 1693576080,
            'family_name': '',
            'email': 'agomez@resonance.nyc'
        }
        """
        super().__init__(
            token_use=kwargs.get("token_use"),
            auth_time=kwargs.get("auth_time"),
            email=kwargs.get("email"),
            iat=kwargs.get("iat"),
            exp=kwargs.get("exp"),
            given_name=kwargs.get("given_name"),
            family_name=kwargs.get("family_name"),
            raw_token=kwargs,
        )
