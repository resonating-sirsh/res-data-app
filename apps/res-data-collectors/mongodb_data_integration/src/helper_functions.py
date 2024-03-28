from datetime import datetime
import re
from botocore.exceptions import ClientError
from res.utils import logger
from snowflake.connector.errors import DatabaseError, ProgrammingError
import base64
import boto3
import json
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TIMESTAMP, VARCHAR, INTEGER, NUMERIC, BOOLEAN
import pandas as pd


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


def get_latest_snowflake_timestamp(
    database,
    schema,
    table,
    snowflake_user,
    snowflake_password,
    snowflake_account,
    timestamp_field="synced_at",
):
    try:
        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
        )

        logger.info("Connected to Snowflake for timestamp retrieval")

    except DatabaseError as e:
        if e.errno == 250001:
            logger.error("Invalid credentials when creating Snowflake connection")
            return None

        else:
            return None

    # Fetch latest date from table
    schema = schema.upper()
    table = table.upper()

    try:
        latest_ts = (
            base_connection.cursor()
            .execute(f"select max({timestamp_field}) from {database}.{schema}.{table}")
            .fetchone()[0]
        )

        # Close base connection
        base_connection.close()
        logger.info("Snowflake for python connection closed for timestamp retrieval")

        if latest_ts is None:
            raise ValueError

    except ProgrammingError as e:
        logger.warn(
            f"Programming Error while retrieving timestamp; {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})"
        )

        latest_ts = None

    except ValueError as e:
        logger.warn(
            f"Value Error while retrieving timestamp; {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})"
        )

        latest_ts = None

    except Exception as e:
        logger.warn(f"Non-Categorized Error while retrieving timestamp; {e}")

        latest_ts = None

    finally:
        return latest_ts


def camel_to_snake(input_text):
    """
    Takes a text string as an input and converts from camel case (no spaces
    or punctuation with capital letters denoting new words) to snake case
    (all lowercase with underscores in places of spaces)
    """

    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    snake_string = pattern.sub("_", input_text).lower()

    return snake_string


def any_to_snake(input_text):
    """
    Takes a text string as an input and converts to snake case (all lowercase
    with underscores in places of spaces). More computationally expensive than
    camel_to_snake but not by an incredible amount
    """

    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|[^a-zA-Z]")
    string = (
        pattern.sub(" ", input_text.replace("%", "percent")).strip().replace(" ", "_")
    )

    return "".join(string.lower())


def dict_to_sql(input_dict_list, stage_table, engine, dtype=None):
    if len(input_dict_list) > 0:
        df = pd.DataFrame.from_dict(input_dict_list)

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

    else:
        pass


def to_snowflake(output_list, snowflake_creds, database, schema, table):
    if len(output_list) == 0:
        return

    # Create Snowflake tables if they do not already exist
    try:
        # Connect to Snowflake
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
    base_connection.cursor().execute(f"create schema if not exists {schema}")
    base_connection.cursor().execute(f"use schema {schema}")

    # Create a string to execute as a SQL statement
    table_creation_str = f"create table if not exists {table} ("

    # Assign datatypes to each column and construct a sqlalchemy type dictionary
    dtypes = {}
    snowflake_types = {}

    # Fields in Mongo may contain more than one BSON data type. This means that
    # a field may at one point contain entirely integer data and then switch to
    # strings or timestamps. The only way to ensure data is always able to be
    # loaded is to consider all fields as strings. Downstream, in dbt, these
    # fields can be try_cast to the appropriate datatypes such that entries
    # considered to be invalid will come through as null values

    # Convert all values to strings with the exception of the added synced_at
    for record in output_list:
        for key, value in record.items():
            if key == "synced_at":
                continue

            else:
                record[key] = str(value)

    # Create dicts for each column name. Add synced_at alone as a timestamp
    # since it isn't a MongoDB field and thus won't have multiple datatypes
    table_creation_str += "synced_at timestamp_tz,"
    dtypes["synced_at"] = TIMESTAMP(timezone=True)
    snowflake_types["synced_at"] = "timestamp_tz"

    for record in output_list:
        # Check each column / key-value in the record
        for key, value in record.items():
            # If the column has already been added in a prior loop skip it
            if key in dtypes:
                continue

            # Otherwise add the column to dtype dicts as a string
            else:
                table_creation_str += f"{key} varchar,"
                dtypes[key] = VARCHAR
                snowflake_types[key] = "varchar"

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
            f"snowflake://{snowflake_creds['user']}:{snowflake_creds['password']}@{snowflake_creds['account']}/{database}/{schema}?warehouse=loader_wh"
        )
        session = sessionmaker(bind=engine)()
        alchemy_connection = engine.connect()

        logger.info(f"SQLAlchemy Snowflake session and connection created")

    except Exception as e:
        logger.error(f"SQLAlchemy Snowflake session creation failed: {e}")

        raise

    try:
        logger.info("Creating stage table")
        dict_to_sql(output_list, f"{table}_stage", engine, dtypes)

    except Exception as e:
        logger.error(e)

        raise

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)
    logger.info("Merging data from stage table to target table")

    stage_table = meta.tables[f"{table}_stage"]
    target_table = meta.tables[f"{table}"]

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
        alter_command_str = f"alter table {table} add "

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
    logger.info(f"Merge executed for {table}")

    # Close connections
    base_connection.close()
    logger.info("Snowflake for python connection closed")
    alchemy_connection.close()
    engine.dispose()
    logger.info("sqlalchemy connection closed; sqlalchemy engine disposed of")
