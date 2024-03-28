from datetime import datetime
from botocore.exceptions import ClientError
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TIMESTAMP, VARCHAR, INTEGER, NUMERIC, BOOLEAN, DATE
from res.utils import logger
import base64
import codecs
import hashlib
import json
import os
import pandas as pd
import boto3
import snowflake.connector
import hmac
import hashlib


def get_secret(secret_name):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        logger.info(e.response["Error"]["Code"])

        raise e

    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary,
        # one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)  # returns the secret as dictionary


def validate_signature(request_sig, request_raw_body):
    """
    Evaluates the raw request body and signature header from a POST request and
    checks it using the Resonance brand webhook secrets stored in S3. Used
    to reject requests with incorrect signatures
    """

    def generate_signature(webhook_secret):
        digest = hmac.new(
            key=webhook_secret.encode("utf-8"),
            msg=request_raw_body,
            digestmod=hashlib.sha256,
        ).digest()
        base64_hash = base64.b64encode(digest)
        return hmac.compare_digest(request_sig.encode("utf-8"), base64_hash)

    # Retrieve each account's name and webhook secret
    account_secrets = get_secret("DATA_TEAM_AFTERSHIP_API_KEYS")

    # Add valid webhook signatures based on retrieved webhook secrets
    matched_account = None
    for acc in account_secrets:
        is_verified = generate_signature(acc["returns_webhook_secret"])

        if is_verified:
            matched_account = acc["account"]
            logger.info(
                f"Webhook signature value is valid for account {matched_account}; returning corresponding Aftership account name"
            )

            return matched_account

    if matched_account is None:
        return ("Unauthorized; webhook signature value is invalid", 401)


def dict_to_sql(data, stage_table, engine, dtype=None):
    df = pd.DataFrame.from_dict([data])

    if dtype is not None:
        df.to_sql(
            stage_table,
            engine,
            index=False,
            chunksize=5000,
            if_exists="replace",
            dtype=dtype,
        )

    else:
        df.to_sql(
            stage_table,
            engine,
            index=False,
            chunksize=5000,
            if_exists="replace",
        )


def to_snowflake(data: dict, snowflake_creds: dict):
    """
    Processes a single record from a webhook and sends the data to Snowflake
    """

    # Add a unique key and sync timestamp to the record
    record_field_hash = hashlib.md5(str(data.values()).encode("utf-8")).hexdigest()
    data["primary_key"] = record_field_hash

    # Add sync time. This isn't part of the primary key because
    # that can create multiple records for observations of the
    # same record state
    data["synced_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Create Snowflake tables if they do not already exist. The techpirates shop
    # is used for testing (dev) and everything else is prod
    try:
        # Connect to Snowflake
        database = (
            "raw_dev" if data["account_name"] == "techpiratesaftership" else "raw"
        )
        base_connection = snowflake.connector.connect(
            user=snowflake_creds["user"],
            password=snowflake_creds["password"],
            account=snowflake_creds["account"],
            warehouse="loader_wh",
            database=database,
        )
        logger.info("Connected to Snowflake")

    except Exception as e:
        logger.error(f"{e}")

        raise

    # Create schema and tables
    logger.info("Creating schema and tables if they do not exist")
    base_connection.cursor().execute(f"create schema if not exists aftership")
    base_connection.cursor().execute(f"use schema aftership")

    # Create a string to execute as a SQL statement
    table_creation_str = f"create table if not exists returns ("

    # This section assigns datatypes to each column and constructs a dictionary
    # for sqlalchemy types. The first non-None value found is evaluated with
    # conditional logic. The type of the key value is then used to assign the
    # corresponding string for Snowflake table creation and a sqlalchemy
    # datatype.
    table_creation_str = f"create table if not exists returns ("
    dtypes = {}
    snowflake_types = {}

    # Check each column / key-value in the record
    for key, value in data.items():
        # If the column has already been evaluated in a prior loop skip it
        if key in dtypes:
            continue

        # If the column has not been evaluated but this instance of it is
        # None then skip it. The exception for this is timestamp fields
        # (keys containing "_at") since they can be identified via names
        elif value is None and key[-3:] != "_at":
            continue

        # Otherwise evaluate the value and add to dtype dict
        else:
            if key[-3:] == "_at":
                table_creation_str += f"{key} timestamp_tz,"
                dtypes[key] = TIMESTAMP(timezone=True)
                snowflake_types[key] = "timestamp_tz"

            elif key[-5:] == "_date":
                table_creation_str += f"{key} date,"
                dtypes[key] = DATE
                snowflake_types[key] = "date"

            # Insert dictionary and list values as strings. These can be
            # loaded as objects later in Snowflake / dbt. Not doing it now
            # saves the trouble of handling them as uploaded JSON files
            # within Snowflake stages. Uses type instead of isinstance to
            # prevent erroneous evaluation of subclasses
            elif type(value) is str or type(value) is dict or type(value) is list:
                table_creation_str += f"{key} varchar,"
                dtypes[key] = VARCHAR
                snowflake_types[key] = "varchar"

            elif type(value) is float:
                table_creation_str += f"{key} number(32, 16),"
                dtypes[key] = NUMERIC(32, 16)
                snowflake_types[key] = "number(32, 16)"

            elif type(value) is int:
                table_creation_str += f"{key} integer,"
                dtypes[key] = INTEGER
                snowflake_types[key] = "integer"

            elif type(value) is bool:
                table_creation_str += f"{key} boolean,"
                dtypes[key] = BOOLEAN
                snowflake_types[key] = "boolean"

    # Slice the table creation string to remove the final comma
    table_creation_str = table_creation_str[:-1] + ")"

    try:
        base_connection.cursor().execute(table_creation_str)

    except Exception as e:
        logger.error(f"Error executing query: {e}")

        raise

    try:
        # Create a SQLAlchemy connection to the Snowflake database
        logger.info("Creating SQLAlchemy Snowflake session")
        engine = create_engine(
            f"snowflake://{snowflake_creds['user']}:{snowflake_creds['password']}@{snowflake_creds['account']}/{database}/aftership?warehouse=loader_wh"
        )
        session = sessionmaker(bind=engine)()
        alchemy_connection = engine.connect()

        logger.info(f"SQLAlchemy Snowflake session and connection created")

    except Exception as e:
        logger.error(f"SQLAlchemy Snowflake session creation failed: {e}")

        raise

    # Convert dictionaries and lists to strings
    for key, value in data.items():
        if type(value) is dict or type(value) is list:
            data[key] = str(value)

    dict_to_sql(data, f"returns_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)
    logger.info("Merging data from stage table to target table")

    # Set table metadata
    stage_table = meta.tables[f"returns_stage"]
    target_table = meta.tables[f"returns"]

    # If any columns from the retrieved data aren't in the target table then
    # add them so the merge will work. This column name list isn't the same
    # as the list used to make the cols dict; that's metadata
    stage_col_names = [col.name for col in stage_table.columns._all_columns]
    target_col_names = [col.name for col in target_table.columns._all_columns]
    missing_col_names = list(set(stage_col_names).difference(target_col_names))

    if len(missing_col_names) > 0:
        logger.info(
            f"Found {len(missing_col_names)} columns in stage table not present in target table. Adding missing columns"
        )
        alter_command_str = f"alter table returns add "

        for col in missing_col_names:
            alter_command_str += f"{col} {snowflake_types.get(col, 'varchar')},"

        alter_command_str = alter_command_str[:-1]

        try:
            base_connection.cursor().execute(alter_command_str)

        except Exception as e:
            logger.error(f"Error adding missing columns to target table: {e}")

            raise

    # Structure merge
    merge = MergeInto(
        target=target_table,
        source=stage_table,
        on=target_table.c.primary_key == stage_table.c.primary_key,
    )

    # Create column metadata dict using column names from stage table
    cols = {}

    for column in stage_table.columns._all_columns:
        cols[column.name] = getattr(stage_table.c, column.name)

    # Insert new records and update existing ones
    merge.when_not_matched_then_insert().values(**cols)
    merge.when_matched_then_update().values(**cols)
    alchemy_connection.execute(merge)
    logger.info(f"Merge executed for returns")

    # Close connections
    base_connection.close()
    logger.info("Snowflake for python connection closed")
    alchemy_connection.close()
    engine.dispose()
    logger.info("sqlalchemy connection closed; sqlalchemy engine disposed of")
