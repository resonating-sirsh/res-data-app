from datetime import datetime
from botocore.exceptions import ClientError
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.api import FacebookAdsApi, FacebookSession, FacebookRequestError
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BOOLEAN, INTEGER, NUMERIC, TIMESTAMP, VARCHAR
from res.utils import logger, secrets_client
import base64
import hashlib
import json
import pandas as pd
import asyncio
import boto3
import snowflake.connector


def get_latest_snowflake_timestamp(
    database,
    schema,
    table,
    timestamp_field,
    is_date=False,
    is_string=False,
):

    # Retrieve Snowflake credentials
    snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    snowflake_user = snowflake_creds["user"]
    snowflake_password = snowflake_creds["password"]
    snowflake_account = snowflake_creds["account"]

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
            database=database,
        )

        logger.info("Connected to Snowflake for timestamp retrieval")

    except DatabaseError as e:

        if e.errno == 250001:

            logger.error("Invalid credentials when creating Snowflake connection")
            return None

        else:

            return None

    # Fetch latest date from table
    schema = schema.lower()
    table = table.lower()
    database = database.lower()

    try:

        # If the function is directed to return a date then cast the retrieved
        # value
        if is_date:

            timestamp_field += "::date"

        latest_ts = (
            base_connection.cursor()
            .execute(f"select max({timestamp_field}) from {database}.{schema}.{table}")
            .fetchone()[0]
        )

        if is_string:

            timestamp_format_str = "%Y-%m-%d" if is_date else "%Y-%m-%d %H:%M:%S.%f %z"
            latest_ts = latest_ts.strftime(timestamp_format_str)

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


def dict_to_sql(input_dict_list, stage_table, engine, dtype=None):
    if len(input_dict_list) > 0:
        df = pd.DataFrame.from_dict(input_dict_list).drop_duplicates()

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


def safe_strptime(time_string):
    """
    Takes a datetime / timestamp string as an input and converts to a datetime
    object. datetime.strptime returns an error for non-strings. This function
    allows for safe use of it even with values of None. Returned objects are
    UTC
    """

    try:

        time_object = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%f%z")
        utc_object = (time_object - time_object.utcoffset()).replace(tzinfo=None)

        return utc_object

    except:

        return time_string


async def get_data(
    ad_account_cred_dict,
    api_call_function,
    app_id,
    app_secret,
    endpoint_value_type,
    fields,
    output_list,
    params,
):

    id = "act_" + ad_account_cred_dict["ad_account_id"]
    access_token = ad_account_cred_dict["access_token"]
    account_name = ad_account_cred_dict["account_name"]

    try:

        # Working with multiple APIs and users requires creating multiple sessions
        # instead of one default one
        session = FacebookSession(
            app_id=app_id, app_secret=app_secret, access_token=access_token
        )

        api = FacebookAdsApi(session)

        logger.info(f"FacebookSession created for {account_name}")

    except Exception as e:

        logger.info(f"FacebookSession creation failed for {account_name}: {e}")

        raise

    # Set up a dictionary of fields being retrieved and their SQLAlchemy and
    # Snowflake datatypes. This will be used multiple times whenever the script
    # requires field names or datatypes

    try:

        iterator = api_call_function(id, api, fields, params)

    # If the request encounters a rate limit, pause execution of the coroutine.
    # Coroutines are broken out by Ad Account and each have their own System
    # User access token with its own individual rate limit. The RequestError for
    # rate-limit errors (response code 400) contains an estimate (in minutes) of
    # how much time needs to pass before requests will no longer be throttled
    except FacebookRequestError as e:

        # Handle error 400 (rate limit) by sleeping for time in header
        if e.http_status() == 400:

            usage_dict = json.loads(dict(e.http_headers())["x-business-use-case-usage"])
            wait_time = max(
                300,
                30
                + (
                    60
                    * int(
                        usage_dict[next(iter(usage_dict))][0][
                            "estimated_time_to_regain_access"
                        ]
                    )
                ),
            )

            # Rate info is the value of a dict using the account ID as the key
            logger.info(f"Rate limit reached. Sleeping for {wait_time} seconds")

            await asyncio.sleep(wait_time)

            # Recreate session after wait time to prevent errors
            session = FacebookSession(
                app_id=app_id, app_secret=app_secret, access_token=access_token
            )

            api = FacebookAdsApi(session)

            try:

                iterator = api_call_function(id, api, fields, params)

            except FacebookRequestError as e:

                # If it's still erroring out after failing once raise the Exception
                logger.error(dict(e.body())["error"]["message"])

                raise e

        else:

            # Otherwise raise the exception for other request status
            logger.error(dict(e.body())["error"]["message"])

            raise e

    except Exception as e:

        logger.error(f"Error connecting to Meta for {account_name}: {e}")

        raise

    # For endpoints that return one value, turn the cursor into a dict and
    # insert into a list. For endpoints that return multiple values add each
    # page of the response to the data list
    logger.info(f"Retrieving response values for {account_name}")

    if endpoint_value_type == "single":

        data = [dict(iterator)]

    else:

        data = []

        data.extend(iterator._queue)

        await asyncio.sleep(10)

        while iterator._finished_iteration == False:

            try:

                iterator.load_next_page()

            except FacebookRequestError as e:

                if e.http_status() == 400:

                    usage_dict = json.loads(
                        dict(e.http_headers())["x-business-use-case-usage"]
                    )

                    wait_time = max(
                        300,
                        60
                        * int(
                            usage_dict[next(iter(usage_dict))][0][
                                "estimated_time_to_regain_access"
                            ]
                        ),
                    )

                    logger.info(
                        f"Rate limit reached for {account_name}. Sleeping for {wait_time} seconds"
                    )

                    await asyncio.sleep(wait_time)

                    # Recreate session after wait time to prevent errors
                    session = FacebookSession(
                        app_id=app_id, app_secret=app_secret, access_token=access_token
                    )

                    api = FacebookAdsApi(session)

                    continue

                else:

                    logger.error(dict(e.body())["error"]["message"])

                    raise e

            except Exception as e:

                logger.error(f"Error connecting to Meta for {account_name}: {e}")

                raise e

            data.extend(iterator._queue)

            await asyncio.sleep(10)

    data = [dict(rec) for rec in data]
    output_list.extend(data)
    logger.info(f"API data retrieved for {account_name}")


async def get_all_data(
    ad_account_cred_dicts,
    api_call_function,
    app_id,
    app_secret,
    endpoint_value_type,
    fields,
    output_list,
    params,
):

    futures = [
        get_data(
            ad_account_cred_dict=ad_account_cred_dict,
            api_call_function=api_call_function,
            app_id=app_id,
            app_secret=app_secret,
            endpoint_value_type=endpoint_value_type,
            fields=fields,
            output_list=output_list,
            params=params,
        )
        for ad_account_cred_dict in ad_account_cred_dicts
    ]

    await asyncio.gather(*futures)


def to_snowflake(
    data: list[dict],
    database: str,
    schema: str,
    table: str,
    add_primary_key_and_ts: bool = False,
):
    if add_primary_key_and_ts:

        for rec in data:

            # Add a unique key and sync timestamp to the record
            record_field_hash = hashlib.md5(
                str(rec.values()).encode("utf-8")
            ).hexdigest()
            rec["primary_key"] = record_field_hash

            # Add sync time. This isn't part of the primary key because
            # that can create multiple records for observations of the
            # same record state
            rec["last_synced_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Retrieve Snowflake credentials
    snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

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

    # This section assigns datatypes to each column and constructs a dictionary
    # for sqlalchemy types. The first non-None value found is evaluated with
    # conditional logic. The type of the key value is then used to assign the
    # corresponding string for Snowflake table creation and a sqlalchemy
    # datatype.
    table_creation_str = f"create table if not exists {table} ("
    dtypes = {}
    snowflake_types = {}

    for record in data:
        # Check each column / key-value in the record
        for key, value in record.items():
            # If the column has already been evaluated in a prior loop skip it
            if key in dtypes:
                continue

            # If the column has not been evaluated but this instance of it is
            # None then skip it. The exception for this is timestamp fields
            # (keys containing "_at" or "_time") since they can be identified
            # via names
            elif value is None and (key[-3:] == "_at" or key[-5:] == "_time"):
                continue

            # Otherwise evaluate the value and add to dtype dict
            else:
                if key[-3:] == "_at" or key[-5:] == "_time":
                    table_creation_str += f"{key} timestamp_tz,"
                    dtypes[key] = TIMESTAMP(timezone=True)
                    snowflake_types[key] = "timestamp_tz"

                # Insert dictionary and list values as strings. These can be
                # loaded as objects later in Snowflake / dbt. Not doing it now
                # saves the trouble of handling them as uploaded JSON files
                # within Snowflake stages
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

                # If none of these types are valid (usually means there's some
                # module-specific class) make it a string
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

    # Convert dictionaries and lists to strings
    for record in data:
        for key, value in record.items():
            if type(value) is dict or type(value) is list:
                record[key] = str(value)

            elif key[-3:] == "_at" or key[-5:] == "_time":
                record[key] = safe_strptime(value)

    dict_to_sql(data, f"{table}_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)
    logger.info("Merging data from stage table to target table")

    if len(data) > 0:
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
