import os

from sqlalchemy import Engine
from sqlmodel import create_engine

from res.utils import secrets


def get_database_engine(options: str = "") -> Engine:
    """
    Returns a database engine
    """
    temp = secrets.secrets_client.get_secret(
        "HASURA_SECRET_DATA", load_in_environment=False
    )

    if not temp or type(temp) != dict:
        raise Exception("Unable to load HASURA_SECRET_DATA")
    return create_engine(
        temp["HASURA_GRAPHQL_DATABASE_URL"].join(options),
        echo=os.environ.get("RES_ENV", "development") == "development",
    )
