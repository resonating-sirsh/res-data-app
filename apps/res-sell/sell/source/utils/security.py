from base64 import b64encode
from hashlib import sha256
import hmac
import traceback

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from res.utils import logger, secrets_client


_api_key_header = APIKeyHeader(
    name="X-API-KEY",
    scheme_name="Internal",
    auto_error=False,
)


def get_api_key(api_key_header: str = Security(_api_key_header)):
    """
    This function is used to validate the API key header.
    """
    logger.info("Validating API key header")
    secret = secrets_client.get_secret("SELL_API_KEY")
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )
    if type(secret) == dict:
        secret = secret.get("SELL_API_KEY")
    if api_key_header == secret:
        logger.info("API key header validated")
        return api_key_header
    else:
        logger.error("Invalid API key header")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )


_shopify_hmac_header = APIKeyHeader(
    name="X-Shopify-Hmac-SHA256",
    scheme_name="Shopify Header",
)


async def validate_shopify_hmac_header(
    request: Request,
    shopify_header: str = Security(_shopify_hmac_header),
):
    secret = secrets_client.get_secret("shopify/app/secret")
    if not type(secret) == str:
        logger.error(
            "Secret can't be used 'shopify/app/secret'",
            traceback=traceback.format_exc(),
        )
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server can't process request",
        )
    body = await request.body()
    logger.debug(
        "Validating hmac",
        extra={"hmac": shopify_header, "body": body},
    )

    computed_hmac = b64encode(hmac.new(secret.encode("utf-8"), body, sha256).digest())

    logger.debug("computed_hmac", extra={"value": computed_hmac})
    if not hmac.compare_digest(computed_hmac, shopify_header.encode("utf-8")):
        logger.warn(
            "Request HMAC is not valid",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    logger.info("Valid request can be accepted")
