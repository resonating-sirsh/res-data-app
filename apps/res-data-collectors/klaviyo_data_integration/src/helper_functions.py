import asyncio
import base64
import hashlib
import json
import os
import random
from datetime import datetime

import aiohttp
import boto3
import pandas as pd
import snowflake.connector
from botocore.exceptions import ClientError
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BOOLEAN, INTEGER, NUMERIC, TIMESTAMP, VARCHAR

from res.utils import logger


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


def get_latest_snowflake_timestamp(
    database,
    schema,
    table,
    snowflake_user,
    snowflake_password,
    snowflake_account,
    timestamp_field="synced_at",
    timestamp_format_str=None,
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

        latest_ts = (
            latest_ts.strftime(timestamp_format_str)
            if timestamp_format_str
            else latest_ts
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


def parse_relationship_dict(relationship_obj):
    """
    Evaluates a entity-relationship object within Klaviyo data to determine
    if it contains foreign keys. If so, parses the dict and returns either
    the single foreign keys or arrays of multiple ones
    """

    output = {}

    for key, val in relationship_obj.items():
        # Key value pairs within a relationship object contain keys that
        # indicate information type and values objects containing data and
        # links. The links are not needed. Check the datatype of the data to
        # determine next steps
        if isinstance(val, dict):
            if "data" in val:
                if isinstance(val["data"], dict):
                    # If the val dict has a 'type' and 'id' then it is a single
                    # foreign key
                    if "type" in val["data"] and "id" in val["data"]:
                        # Add the foreign key to the output dictionary
                        f_key_name = val["data"]["type"] + "_id"
                        f_key_value = val["data"]["id"]
                        output[f_key_name] = f_key_value

                elif isinstance(val["data"], list):
                    if len(val["data"]) > 0:
                        # If the val list has a list of dicts containing 'type' and
                        # 'id then it is a list of foreign keys. Check first element
                        if "type" in val["data"][0] and "id" in val["data"][0]:
                            f_key_name = val["data"][0]["type"] + "_ids"
                            f_key_name = f_key_name.replace("-", "_")
                            f_key_value = [i["id"] for i in val["data"]]
                            output[f_key_name] = f_key_value

    return output


def underscore_and_plural(input_str: str):
    output_str = input_str.replace("-", "_")
    output_str += "s" if output_str[-1:] != "s" else ""

    return output_str


async def get_api_data(
    api_key: str,
    endpoint: str,
    aiohttp_session,
    sync_time: datetime,
    output_data,
    params=None,
    endpoint_label=None,
):
    # Set headers for account info and endpoint retrieval
    headers = {
        "accept": "application/json",
        "revision": "2023-09-15",
        "Authorization": f"Klaviyo-API-Key {api_key}",
    }
    sleep_time = None
    retries = 0

    while True and endpoint != "accounts":
        # Get account ID
        async with aiohttp_session.get(
            url="https://a.klaviyo.com/api/accounts/",
            headers=headers,
        ) as response:
            try:
                response.raise_for_status()
                data = await response.json(encoding="UTF-8")
                account = data["data"][0]
                account_id = account["id"]
                account_name = account["attributes"]["contact_information"][
                    "organization_name"
                ]

                break

            except Exception as e:
                if response.status == 429:
                    # Don't retry an 11th time
                    if retries == 10:
                        e = "Maximum rate-limited retries (10) reached"
                        logger.error(e)

                        raise Exception(e)

                    # Exponential backoff + randomness + max retries of 10
                    retry_after = int(response.headers.get("Retry-After", 0))
                    sleep_time = max(retry_after, (2**retries)) + (
                        random.randint(0, 1000) / 1000
                    )
                    logger.warning(
                        f"Rate limit reached while retrieving account info (attempt {retries+1}). Retrying after {sleep_time} seconds"
                    )
                    await asyncio.sleep(sleep_time)
                    retries += 1

                    continue

                else:
                    logger.error(e)

                    raise

    # Iterate over pages until a response does not return a "next" URL. For the
    # first page set the url outside of the loop. Initial params defined via
    # function input. For subsequent pages use the cursor provided in the API
    # response.
    url = f"https://a.klaviyo.com/api/{endpoint}/"
    has_more = True
    retries = 0

    while has_more:
        async with aiohttp_session.get(
            url=url,
            headers=headers,
            params=params,
        ) as response:
            try:
                response.raise_for_status()
                data = await response.json(encoding="UTF-8")

                # Some endpoints return a list of records and some a single
                # record
                if isinstance(data["data"], list):
                    endpoint_recs = data["data"]

                elif isinstance(data["data"], dict):
                    # Combine the single record returned by the endpoint and
                    # included records into a single list.
                    endpoint_recs = [data["data"]]

                # Label the endpoint record with the endpoint label
                for endpoint_rec in endpoint_recs:
                    endpoint_rec["type"] = endpoint_label

                included_recs = data.get("included", [])
                all_recs = endpoint_recs + included_recs

                # Process records
                for rec in all_recs:
                    if endpoint == "accounts":
                        rec_dict = {}

                    else:
                        rec_dict = {
                            "account_id": account_id,
                            "account_name": account_name,
                        }

                    for key, val in rec.items():
                        if key == "attributes":
                            rec_dict.update(val)

                        elif key == "relationships":
                            relationship_fields = parse_relationship_dict(val)
                            rec_dict.update(relationship_fields)

                        # Exclude key-value pairs containing links; those are
                        # API GET links and not useful to store
                        elif key == "links":
                            continue

                        else:
                            rec_dict[key] = val

                    # If any of the rec keys (field names) contain dashes
                    # replace those dashes with underscores
                    rec_dict = {
                        key.replace("-", "_"): value
                        for (key, value) in rec_dict.items()
                    }

                    # Create a unique key using the values of the recorded state
                    record_field_hash = hashlib.md5(
                        str(rec_dict.values()).encode("utf-8")
                    ).hexdigest()
                    rec_dict["primary_key"] = record_field_hash

                    # Add sync time. This isn't part of the primary key because
                    # that can create multiple records for observations of the
                    # same record state
                    rec_dict["synced_at"] = sync_time

                    # Add to output object based on record type
                    rec_type = underscore_and_plural(rec["type"])

                    if rec_type in output_data:
                        output_data[rec_type].append(rec_dict)

                # Iterate url using cursor provided in API response. Null
                # params. Using [] to raise exceptions if the utilized keys are
                # missing in response (they never should be)
                url = data["links"]["next"]
                has_more = url is not None
                params = None

                # Read response headers to determine if rate limit has been
                # reached
                if int(response.headers["RateLimit-Remaining"]) == 0:
                    sleep_time = int(response.headers["RateLimit-Reset"])

            except Exception as e:
                if response.status == 429:
                    # Don't retry an 11th time
                    if retries == 10:
                        e = "Maximum rate-limited retries (10) reached"
                        logger.error(e)

                        raise Exception(e)

                    # Exponential backoff + randomness + max retries of 10
                    retry_after = int(response.headers.get("Retry-After", 0))
                    sleep_time = max(retry_after, (2**retries)) + (
                        random.randint(0, 1000) / 1000
                    )
                    logger.warning(
                        f"Rate limit reached (attempt {retries+1}). Retrying after {sleep_time} seconds"
                    )
                    retries += 1

                else:
                    logger.error(e)

                    raise

        # Sleep either from a 429 response or no remaining requests in rate
        # limit
        if sleep_time:
            await asyncio.sleep(sleep_time)


async def get_all_api_data(
    api_keys,
    endpoint,
    params=None,
    endpoint_label=None,
):
    # Output dict will contain data for endpoint and any included data. This is
    # created first so that each coroutine can dump into the same place
    if endpoint_label is None:
        endpoint_label = underscore_and_plural(endpoint)
    output_data = {endpoint_label: []}

    if "include" in (params or {}):
        included_data = params["include"].split(",")

        for i in included_data:
            i = underscore_and_plural(i)
            output_data[i] = []

    timeout = aiohttp.ClientTimeout(total=None)
    sync_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    async with aiohttp.ClientSession(
        trust_env=(os.environ.get("AM_I_IN_A_DOCKER_CONTAINER") == "true"),
        timeout=timeout,
    ) as aiohttp_session:
        futures = [
            get_api_data(
                api_key,
                endpoint,
                aiohttp_session,
                sync_time,
                output_data,
                params,
                endpoint_label,
            )
            for api_key in api_keys
        ]

        await asyncio.gather(*futures)

    return output_data


async def get_account_additional_records(
    aiohttp_session,
    api_key,
    endpoint,
    output_data,
    params,
    sync_time,
    target_ids,
    endpoint_label,
):
    # Set headers for account info and endpoint retrieval
    headers = {
        "accept": "application/json",
        "revision": "2023-09-15",
        "Authorization": f"Klaviyo-API-Key {api_key}",
    }
    sleep_time = None
    retries = 0

    while True and endpoint != "accounts":
        # Get account ID
        async with aiohttp_session.get(
            url="https://a.klaviyo.com/api/accounts/",
            headers=headers,
        ) as response:
            try:
                response.raise_for_status()
                data = await response.json(encoding="UTF-8")
                account = data["data"][0]
                account_id = account["id"]
                account_name = account["attributes"]["contact_information"][
                    "organization_name"
                ]

                break

            except Exception as e:
                if response.status == 429:
                    # Don't retry an 11th time
                    if retries == 10:
                        e = "Maximum rate-limited retries (10) reached"
                        logger.error(e)

                        raise Exception(e)

                    # Exponential backoff + randomness + max retries of 10
                    retry_after = int(response.headers.get("Retry-After", 0))
                    sleep_time = max(retry_after, (2**retries)) + (
                        random.randint(0, 1000) / 1000
                    )
                    logger.warning(
                        f"Rate limit reached (attempt {retries+1}). Retrying after {sleep_time} seconds"
                    )
                    retries += 1
                    await asyncio.sleep(sleep_time)

                    continue

                else:
                    logger.error(e)

                    raise

    try:
        # Set target IDs for account based on matching account ID
        target_ids = target_ids[account_id]

    except KeyError as e:
        # Set target IDs to empty list so nothing is processed for this Klaviyo
        # account
        target_ids = []
        logger.warning(
            f"Account {account_name} (ID: {account_id}) does not have records within provided target IDs object"
        )

    except Exception as e:
        logger.error(e)
        raise

    # Loop through each ID and perform a GET request for its data
    for parent_id in target_ids:
        # If the provided endpoint has '/id/' within it then the parent ID
        # should replace that. Otherwise just add the ID after the endpoint
        if "/id/" in endpoint:
            id_endpoint = endpoint.replace("/id/", f"/{parent_id}/")
            url = f"https://a.klaviyo.com/api/{id_endpoint}"

        else:
            url = f"https://a.klaviyo.com/api/{endpoint}/{parent_id}/"

        has_more = True
        sleep_time = None
        retries = 0

        while has_more:
            async with aiohttp_session.get(
                url=url,
                headers=headers,
                params=params,
            ) as response:
                try:
                    response.raise_for_status()
                    data = await response.json(encoding="UTF-8")
                    included_recs = data.get("included", [])

                    # There is a currently a potential bug in Klaviyo's API
                    # wherein the ID used to perform a request is not included
                    # in the relationships dictionary for the retrieved records.
                    # Because of this the parent ID needs to be semi-manually
                    # added to the retrieved records

                    # Some endpoints return a list of records and some a single
                    # record
                    if isinstance(data["data"], list):
                        endpoint_recs = data["data"]

                        # If the endpoint data is a list that means that a
                        # single ID was provided in the GET url to retrieve a
                        # list of related records. In this case the parent ID is
                        # taken from the links and applied to endpoint records
                        links = data["links"]["self"]
                        parent_type = links.split("/api/")[1].split("/")[0]
                        parent_type = (
                            parent_type[:-1] if parent_type[-1:] == "s" else parent_type
                        )

                        for endpoint_rec in endpoint_recs:
                            endpoint_rec[f"{parent_type}_id"] = parent_id

                    elif isinstance(data["data"], dict):
                        endpoint_recs = [data["data"]]

                        # If the endpoint data is a dictionary that means that
                        # the GET request was to retrieve data for a single ID.
                        # In this case the parent ID is applied to included
                        # records
                        parent_type = (
                            endpoint_label[:-1]
                            if endpoint_label[-1:] == "s"
                            else endpoint_label
                        )

                        for included_rec in included_recs:
                            included_rec[f"{parent_type}_id"] = parent_id

                    # Label the endpoint record with the endpoint label
                    for endpoint_rec in endpoint_recs:
                        endpoint_rec["type"] = endpoint_label

                    # Combine the single record returned by the endpoint and
                    # included records into a single list.
                    all_recs = endpoint_recs + included_recs

                    # Process records
                    for rec in all_recs:
                        rec_dict = {
                            "account_id": account_id,
                            "account_name": account_name,
                        }

                        for key, val in rec.items():
                            if key == "attributes":
                                rec_dict.update(val)

                            elif key == "relationships":
                                relationship_fields = parse_relationship_dict(val)
                                rec_dict.update(relationship_fields)

                            # Exclude key-value pairs containing links; those are
                            # API GET links and not useful to store
                            elif key == "links":
                                continue

                            else:
                                rec_dict[key] = val

                        # If any of the rec keys (field names) contain dashes
                        # replace those dashes with underscores
                        rec_dict = {
                            key.replace("-", "_"): value
                            for (key, value) in rec_dict.items()
                        }

                        # Create a unique key using the values of the recorded state
                        record_field_hash = hashlib.md5(
                            str(rec_dict.values()).encode("utf-8")
                        ).hexdigest()
                        rec_dict["primary_key"] = record_field_hash

                        # Add sync time. This isn't part of the primary key because
                        # that can create multiple records for observations of the
                        # same record state
                        rec_dict["synced_at"] = sync_time

                        # Add to output object based on record type
                        rec_type = underscore_and_plural(rec["type"])

                        if rec_type in output_data:
                            output_data[rec_type].append(rec_dict)

                    # Iterate url using cursor provided in API response. Null
                    # params. Some responses are paginated and some are not; if
                    # there is no next key within the links dict then the loop
                    # ends. Non-paginated responses have links within their
                    # response data dict and will therefore return {} with the
                    # get method
                    if "next" in data.get("links", {}):
                        url = data["links"]["next"]
                        has_more = url is not None
                        params = None

                        # Read response headers to determine if rate limit has been
                        # reached
                        if int(response.headers["RateLimit-Remaining"]) == 0:
                            sleep_time = int(response.headers["RateLimit-Reset"])

                    else:
                        has_more = False

                except Exception as e:
                    if response.status == 429:
                        # Don't retry an 11th time
                        if retries == 10:
                            e = "Maximum rate-limited retries (10) reached"
                            logger.error(e)

                            raise Exception(e)

                        # Exponential backoff + randomness + max retries of 10
                        retry_after = int(response.headers.get("Retry-After", 0))
                        sleep_time = max(retry_after, (2**retries)) + (
                            random.randint(0, 1000) / 1000
                        )
                        logger.warning(
                            f"Rate limit reached for {account_name} (attempt {retries+1}). Retrying after {sleep_time} seconds"
                        )
                        retries += 1

                    else:
                        logger.error(e)

                        raise

            # Sleep either from a 429 response or no remaining requests in rate
            # limit
            if sleep_time:
                await asyncio.sleep(sleep_time)


async def get_additional_records(
    api_keys,
    endpoint,
    target_ids,
    params=None,
    endpoint_label=None,
):
    """
    Used to retrieve additional records from the related records of an API
    call. Sometimes an API endpoint's related records themselves have
    related records. Those related records, however, are not retrieved
    within the single API call and have to be retrieved individually. This
    function accepts a dictionary where each key is a Klaviyo account ID
    and the value is a list of URLs to perform GET requests for in order
    to retrieve the additional values. Returns the retrieved values in the
    same format as the input
    """

    # Output dict will contain data for endpoint and any included data. This is
    # created first so that each coroutine can dump into the same place
    if endpoint_label is None:
        endpoint_label = underscore_and_plural(endpoint)
    output_data = {endpoint_label: []}

    if "include" in (params or {}):
        included_data = params["include"].split(",")

        for i in included_data:
            i = underscore_and_plural(i)
            output_data[i] = []

    timeout = aiohttp.ClientTimeout(total=None)
    sync_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    async with aiohttp.ClientSession(
        trust_env=(os.environ.get("AM_I_IN_A_DOCKER_CONTAINER") == "true"),
        timeout=timeout,
    ) as aiohttp_session:
        futures = [
            get_account_additional_records(
                aiohttp_session,
                api_key,
                endpoint,
                output_data,
                params,
                sync_time,
                target_ids,
                endpoint_label,
            )
            for api_key in api_keys
        ]

        await asyncio.gather(*futures)

    return output_data


def to_snowflake(output_list, snowflake_creds, database, schema, table):
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

    # This section assigns datatypes to each column and constructs a dictionary
    # for sqlalchemy types. The first non-None value found is evaluated with
    # conditional logic. The type of the key value is then used to assign the
    # corresponding string for Snowflake table creation and a sqlalchemy
    # datatype.
    table_creation_str = f"create table if not exists {table} ("
    dtypes = {}
    snowflake_types = {}

    for record in output_list:
        # Check each column / key-value in the record
        for key, value in record.items():
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
    for record in output_list:
        for key, value in record.items():
            if type(value) is dict or type(value) is list:
                record[key] = str(value)

    dict_to_sql(output_list, f"{table}_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)
    logger.info("Merging data from stage table to target table")

    if len(output_list) > 0:
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
