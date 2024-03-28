from res.utils.secrets import secrets_client
from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import res
from res.flows.meta.brand.queries import _get_brand_by_token
from res.utils import ping_slack


def ping_slack_sirsh(message):
    ping_slack(f"<@U01JDKSB196> - {message}", "sirsh-test")


security = HTTPBearer()


def determine_brand_context(token):
    """
    supply a token that should match  a brand
    """
    data = _get_brand_by_token(token)
    if data:
        return data
    else:
        raise HTTPException("token not registered or associated with a brand")


def verify_api_key(key: str):
    RES_META_ONE_API_KEY = secrets_client.get_secret("RES_META_TEST_PUB")

    from res.flows.meta.brand.queries import _get_brand_by_token

    res.utils.logger.info(f"verify {key}")
    brand_key = _get_brand_by_token(key)

    if brand_key:
        res.utils.logger.info(f"brand auth for {brand_key}")

    """
    you can use the global key or one specific to a brand
    """
    if key == RES_META_ONE_API_KEY or brand_key is not None:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid RES_META_TEST_PUB.  Please refresh this value from AWS secrets manager",
        )


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not verify_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Invalid token check. Brand key or Res Meta key not found",
        )
    return token
