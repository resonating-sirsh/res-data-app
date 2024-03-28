from typing import Dict
from datetime import date
from schemas.pydantic.cognito import CognitoJwt
from res.utils import logger
import json


def log_caller_info_(
    decode_token: CognitoJwt,
    operation: str,
    metadata: Dict | None = None,
    msg: str | None = None,
):
    """
    Log the decode_token content from the authentication module information about the caller of the operation

    :param decode_token: CognitoJwt object
    :param operation: str Operation Name - Example: "Create User", "POST /users"
    :param metadata: Dict | None - Optional metadata to be logged
    :param msg: str | None - Optional message to be logged

    """
    logger.info(
        f"{date.today()}: {operation} by {decode_token.email} {msg if f': {msg}' else ''}"
    )
    if metadata:
        logger.info("request metadata added:")
        logger.info(json.dumps(metadata))
