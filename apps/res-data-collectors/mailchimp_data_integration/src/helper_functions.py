from botocore.exceptions import ClientError
from datetime import datetime
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from res.utils import logger
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BOOLEAN, INTEGER, TIMESTAMP, VARCHAR
import asyncio
import base64
import boto3
import hashlib
import json
import mailchimp_marketing as Chimp
import os
import pandas as pd
import requests
import snowflake.connector


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


def dict_to_sql(input_dict_list, stage_table_name, engine, dtype=None):

    if len(input_dict_list) > 0:

        df = pd.DataFrame.from_dict(input_dict_list)
        df = df.drop_duplicates(subset=["primary_key"])

        if dtype is not None:

            df.to_sql(
                stage_table_name,
                engine,
                index=False,
                chunksize=5000,
                if_exists="replace",
                dtype=dtype,
            )

        else:

            df.to_sql(
                stage_table_name,
                engine,
                index=False,
                chunksize=5000,
                if_exists="replace",
            )

    else:

        pass


async def get_data_with_offset(
    account_name: str,
    api_function,
    is_test,
    mailchimp_client,
    offset: int,
    output_list: list,
):

    """
    Retrieves data for a single account from the Mailchimp API for 1000
    records offset from the offset int provided and appends them to a list
    """

    # Add offset value to query parameters dictionary
    query_parameters = {"count": 50} if is_test == "true" else {"count": 1000}
    query_parameters["offset"] = offset

    while True:

        try:

            # Retrieve response using query parameters as kwargs for the function in
            # question
            response = api_function(mailchimp_client, query_parameters)
            data = response[next(iter(response))]

            # Add account info and primary key to data
            for record in data:

                record["account_name"] = account_name

                # Create a unique key using the values of the recorded state
                record_field_hash = hashlib.md5(
                    str(record.values()).encode("utf-8")
                ).hexdigest()
                record["primary_key"] = record_field_hash

                # Add sync time. This isn't included in the primary key because that would
                # cause a record update every sync even if data has not changed
                record["synced_at"] = datetime.utcnow().strftime(
                    "%Y-%m-%d %H:%M:%S.%f %Z"
                )

            output_list.extend(data)

            break

        except ApiClientError as e:

            if e.status_code == 429:

                logger.warning("Throttled; napping for a minute")

                await asyncio.sleep(60)

            else:

                logger.error(f"Error: {e}")

                raise


async def get_data(
    api_function,
    api_key_dict: dict,
    is_test,
    output_list: list,
):

    """
    Retrieves all data for an account using up to 10 concurrent
    connections to the Mailchimp API and a function passed as a parameter.
    """

    # Create credential variables
    account_name = api_key_dict["account_name"]
    api_key = api_key_dict["api_key"]
    server = api_key_dict["server"]

    # Create Mailchimp client
    mailchimp_client = Chimp.Client()
    mailchimp_client.set_config({"api_key": api_key, "server": server})
    logger.info(f"Created {account_name} Mailchimp client")

    # Retrieve total number of records and construct list of offsets to
    # iterate through. Max records per request is 1000; max number of
    # connections per API key is 10. For test syncs artificially enforce a
    # total of 50 records to test functionality.
    if is_test == "true":  # If is test

        # Value is cosmetic; means only one sync. Count is also limited to 50 in
        # get_data_with_offset when is_test = "true"
        total_items = 50

    else:

        total_items = api_function(mailchimp_client, {"count": 1}).get("total_items")

    if total_items > 1000:

        logger.info(f"Beginning retrieval of {total_items} records for {account_name}")
        offset_list = []

        # Adding 1 makes the range inclusive
        for i in range(0, total_items + 1, 1000):

            offset_list.append(i)

    else:

        # A single sync with no offset
        offset_list = [0]

    # Loop through slices of the offset list until there are none left
    while len(offset_list) > 0:

        # Capture of slice of 10 offsets to use in async tasks
        offset_slice = offset_list[:10]

        if len(offset_list) == 1:

            logger.info(f"Syncing for {account_name}; offset {offset_slice[0]}")

        else:

            logger.info(
                f"Syncing for {account_name}; offsets {min(offset_slice)} through {max(offset_slice)}"
            )

        # Create one task for each item in offset slice (10 max) and execute
        # concurrently
        slice_futures = [
            get_data_with_offset(
                account_name,
                api_function,
                is_test,
                mailchimp_client,
                offset,
                output_list,
            )
            for offset in offset_slice
        ]

        await asyncio.gather(*slice_futures)

        # Remove offsets from offset_list post iteration. Loop through remaining
        # offsets in 10-item slices until there are none left
        del offset_list[:10]


async def get_all_data(
    api_function,
    api_key_dicts: list,
    is_test,
    output_list: list,
):

    """
    Must pass a function for the API call to this function that accepts a
    Mailchimp client and kwargs
    """

    futures = [
        get_data(
            api_function,
            api_key_dict,
            is_test,
            output_list,
        )
        for api_key_dict in api_key_dicts
    ]

    await asyncio.gather(*futures)


def to_snowflake(database: str, output_list: list, snowflake_creds: dict, table: str):

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
            # None then skip it
            elif value is None:

                continue

            # Otherwise evaluate the value and add to dtype dict
            else:

                # The 'timeseries' and 'timewarp' fields show campaign performance
                # in some reports but are not themselves timestamps
                if (
                    (
                        "time" in key
                        or key[:5] == "date_"
                        or key[-5:] == "_date"
                        or key == "synced_at"
                    )
                    and key != "timeseries"
                    and key != "timewarp"
                ):

                    table_creation_str += f"{key} timestamp_tz,"
                    dtypes[key] = TIMESTAMP(timezone=True)
                    snowflake_types[key] = "timestamp_tz"

                # Insert dictionary and list values as strings. These can be
                # loaded as objects later in Snowflake / dbt. Not doing it now
                # saves the trouble of handling them as uploaded JSON files
                # within Snowflake stages
                elif (
                    type(value) is str
                    or type(value) is dict
                    or type(value) is list
                    or key == "timeseries"
                    or key == "timewarp"
                ):
                    table_creation_str += f"{key} varchar,"
                    dtypes[key] = VARCHAR
                    snowflake_types[key] = "varchar"

                elif type(value) is int:

                    table_creation_str += f"{key} integer,"
                    dtypes[key] = INTEGER
                    snowflake_types[key] = "integer"

                elif type(value) is bool:

                    table_creation_str += f"{key} boolean,"
                    dtypes[key] = BOOLEAN
                    snowflake_types[key] = "boolean"

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
    base_connection.cursor().execute("create schema if not exists mailchimp")
    base_connection.cursor().execute("use schema mailchimp")

    # Slice the table creation string to remove the final comma
    table_creation_str = table_creation_str[:-1]
    table_creation_str += ")"

    try:

        base_connection.cursor().execute(table_creation_str)

    except Exception as e:

        logger.error(f"Error executing query: {e}")

        raise

    try:

        # Create a SQLAlchemy connection to the Snowflake database
        logger.info("Creating SQLAlchemy Snowflake session")
        engine = create_engine(
            f"snowflake://{snowflake_creds['user']}:{snowflake_creds['password']}@{snowflake_creds['account']}/{database}/mailchimp?warehouse=loader_wh"
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

            if "_time" in key or "timestamp_" in key:

                try:

                    record[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")

                except:

                    record[key] = None

            elif type(value) is dict or type(value) is list:

                record[key] = str(value)

    dict_to_sql(output_list, f"{table}_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info(f"Merging data from {table} stage table to target table")

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


def records_to_s3(
    bucket_name: str,
    content_type: str,
    output_list: list,
    target_path: str,
    url_key: str,
):

    """
    Takes the output of a Mailchimp API call with a URL to an image as a
    parameter. Iterates through each record, retrieves its image file via a
    GET request + file stream, and uploads it to an S3 bucket. Amazon boto3
    isn't set up to work with asyncio and there is no official package with
    the functionality so this function uploads templates one at a time.

    Params:
        - bucket_name: Bucket to upload to
        - content_type: Self explanatory; used in the target s3 key
        - url_key: Dict key for the image URL value within the record
    """

    def to_s3(record: dict):

        """
        Uploads a file to an S3 bucket

        Param:
            - record: Record containing at least an ID and image URL
        """

        # Retrieve record details; perform GET request
        account_name = record["account_name"]
        id = record["id"]
        url = record[url_key]
        extension = os.path.splitext(url)[1]  # Is "" if there's no extension
        key = f"{target_path}{account_name}_{content_type}_{id}{extension}"
        url = record[url_key]
        r = requests.get(url, stream=True)

        # Upload the file
        try:

            bucket = s3.Bucket(bucket_name)
            bucket.upload_fileobj(r.raw, key)

        except ClientError as e:

            logger.error(e)

            raise

    session = boto3.Session()
    s3 = session.resource("s3")
    logger.info("s3 session created")

    # Loop through records
    count = 0

    for record in output_list:

        # Skip records with no url; these URLs can be None or empty strings
        if record.get(url_key, "") == "":

            continue

        else:

            to_s3(record)
            count += 1

        # Print a fun little update message every 500 records
        if count % 500 == 0:

            logger.info(f"{count} records uploaded; continuing...")

    logger.info(f"Uploads complete; {count} records uploaded")


async def to_snowflake_async(
    database: str, output_list: list, snowflake_creds: dict, table: str
):

    "Simple async wrapper for to_snowflake"

    to_snowflake(database, output_list, snowflake_creds, table)


async def async_records_to_s3(
    bucket_name: str,
    content_type: str,
    output_list: list,
    target_path: str,
    url_key: str,
):

    "Simple async wrapper for records_to_s3"

    records_to_s3(
        bucket_name,
        content_type,
        output_list,
        target_path,
        url_key,
    )


async def to_s3_and_snowflake(
    bucket_name: str,
    content_type: str,
    database: str,
    output_list: list,
    snowflake_creds: dict,
    table: str,
    target_path: str,
    url_key: str,
):

    "Async wrapper for to_snowflake_async and records_to_s3"

    futures = [
        to_snowflake_async(
            database,
            output_list,
            snowflake_creds,
            table,
        ),
        async_records_to_s3(
            bucket_name,
            content_type,
            output_list,
            target_path,
            url_key,
        ),
    ]

    await asyncio.gather(*futures)


async def get_with_semaphore(
    account_name,
    api_function,
    target_id: str,
    mailchimp_client: Client,
    output_list: list,
    semaphore: asyncio.Semaphore,
):

    """
    Retrieves the segments for a given Mailchimp account and email list
    (list_id)

    Parameters
    ----------
    - account_name:
        Name of the Mailchimp account
    - api_function:
        API function used to retrieve data
    - target_id:
        ID of the entity being passed to the API function
    - mailchimp_client:
        Client object with configs for api_key and server for a given
        Mailchimp account
    - output_list:
        List to add processed API records to from the output of the function
    - semaphore:
        Asyncio Semaphore class; used to limit the number of simultaneous
        actions. Tasks making use of the same Semaphore must wait for space
        to open up based on the integer value of the Semaphore
    """
    async with semaphore:

        # Set remaining records to 1 to start loop. Function will continuously
        # retrieve records until there are non left
        offset = 0
        remaining_items = 1

        while remaining_items > 0:

            non_rate_limit_retries = 0

            while True:

                try:

                    query_parameters = {"offset": offset, "count": 1000}
                    response = api_function(
                        mailchimp_client, target_id, query_parameters
                    )
                    data = response[next(iter(response))]

                    # Add account info and primary key to data
                    for record in data:

                        record["account_name"] = account_name

                        # Create a unique key using the values of the recorded state
                        record_field_hash = hashlib.md5(
                            str(record.values()).encode("utf-8")
                        ).hexdigest()
                        record["primary_key"] = record_field_hash

                        # Add sync time. This isn't included in the primary key because that would
                        # cause a record update every sync even if data has not changed
                        record["synced_at"] = datetime.utcnow().strftime(
                            "%Y-%m-%d %H:%M:%S.%f %Z"
                        )

                    output_list.extend(data)

                except ApiClientError as e:

                    if e.status_code == 429:

                        logger.warning("Throttled; napping for a minute")

                        await asyncio.sleep(60)

                    else:

                        if non_rate_limit_retries > 5:

                            return e

                        logger.warning(f"Retrying after error: {e.text}")
                        non_rate_limit_retries += 1

                        await asyncio.sleep(60)

                except Exception as e:

                    if non_rate_limit_retries > 5:

                        return e

                    logger.warning(f"Retrying after error: {e}")
                    non_rate_limit_retries += 1

                # Increment
                offset += 1000
                remaining_items = response["total_items"] - offset

                break


async def get_from_ids_for_account(
    api_key_dict: dict,
    api_function,
    target_ids: list,
    output_list: list,
):
    """
    Wrapper for the API call tasks of a given account. This function
    contains the Mailchimp Client class instance that each account will use.
    Additionally it makes use of the asyncio Semaphore class to create a
    concurrency limit for the tasks that it awaits. This limits the number
    of active connections for a given account to 10, which is the limit
    enforced by Mailchimp before they begin to throttle.

    Parameters
    ----------
    - api_key_dict:
        Dictionary containing the account api key, server, and name
    - api_function:
        Function for data retrieval
    target_ids:
        List of IDs to retrieve data for within the account
    - output_list:
        List to add processed API records to from the output of the function
    """

    account_name = api_key_dict["account_name"]
    api_key = api_key_dict["api_key"]
    server = api_key_dict["server"]

    # Create Mailchimp client
    mailchimp_client = Client()
    mailchimp_client.set_config({"api_key": api_key, "server": server})

    # Create a limit on concurrent calls; each account is allowed 10 connections
    semaphore = asyncio.Semaphore(10)

    tasks = [
        asyncio.create_task(
            get_with_semaphore(
                account_name,
                api_function,
                target_id,
                mailchimp_client,
                output_list,
                semaphore,
            )
        )
        for target_id in target_ids
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for task in pending:

        task.cancel()

    for task in done:

        e = task.exception()

        if isinstance(e, Exception):

            logger.error(e.text)
            raise e


async def get_from_ids_for_all_accounts(
    api_key_dicts,
    api_function,
    ids_object,
    output_list,
):

    """
    Wrapper for executing async API calls for all available Mailchimp
    accounts
    """

    futures = [
        get_from_ids_for_account(
            [i for i in api_key_dicts if i["account_name"] == account_name][0],
            api_function,
            target_ids,
            output_list,
        )
        for account_name, target_ids in ids_object.items()
    ]

    await asyncio.gather(*futures)
