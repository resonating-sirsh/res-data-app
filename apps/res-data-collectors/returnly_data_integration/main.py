from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.types import BOOLEAN, INTEGER, TIMESTAMP, VARCHAR, NUMERIC
from sqlalchemy.orm import sessionmaker
from res.utils import logger
import base64
import boto3
import gc
import hashlib
import json
import os
import pandas as pd
import aiohttp
import asyncio
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


def get_latest_snowflake_timestamp(
    database,
    snowflake_creds,
):

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_creds["user"],
            password=snowflake_creds["password"],
            account=snowflake_creds["account"],
            warehouse="loader_wh",
            database=database,
            schema="returnly",
        )
        logger.info("Connected to Snowflake for timestamp retrieval")

    except DatabaseError as e:

        if e.errno == 250001:

            logger.error("Invalid credentials when creating Snowflake connection")

            return None

        else:

            return None

    try:

        latest_ts = (
            base_connection.cursor()
            .execute(f"select max(updated_at) from {database}.returnly.returns")
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


def dict_to_sql(input_dict_list, stage_table_name, engine, dtype=None):

    if len(input_dict_list) > 0:

        df = pd.DataFrame.from_dict(input_dict_list)

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


async def get_returnly_data(
    api_token_dict,
    request_url,
    target_dict: dict,
    client_session,
):

    """

    Accepts a target dict, a request URL for returnly, and API
    tokens. Adds formatted data to the target dict according to its
    structure and included components.

    Each non-return item in the included things within the target dict will be a
    dict. The dict key value is the ID of the non-return item and the value will
    be a dict of eventual column values. This allows the code to assign the keys
    of returns and return relationships to one another. Once all non-return items
    are processed the list of dicts of dicts will become a list of dicts using the
    values of the innermost dict

    target_dict {
        returns: [
            {
                return_id: return_id,
                included_item_ids: [
                    included_item_id
                ]
            }
        ],
        included_items: [
            {
                included_item_id: {
                    return_id: source_return_id
                }
            }
        ]
    }

    """

    api_token = api_token_dict["key"]
    account = api_token_dict["account"]
    headers = {
        "X-Api-Token": api_token,
        "X-Api-Version": "2020-08",
        "Content-Type": "application/json",
    }

    def parse_api_response(data, target_dict, retrieved_at):

        for rec in data.get("data", []):

            # Create dictionary for the return record. Include its ID and a
            # surrogate primary key composed of the ID and last updated time.
            # Then assign its relationship foreign keys to underlying dicts
            # along with the source return's ID and created/updated timestamps
            rec_dict = {
                "historical_return_id": hashlib.md5(
                    f"{rec['id']}{rec['attributes']['updated_at']}".encode("utf-8")
                ).hexdigest(),
                "return_id": rec["id"],
                "retrieved_at": retrieved_at,
            }

            for key, value in rec.get("attributes", []).items():

                # non-dict and non-list fields can be added directly to the
                # target_dict.
                if type(value) is not dict and type(value) is not list:

                    rec_dict[key] = value

                # For dicts create new fields
                elif type(value) is dict:

                    for sub_key, sub_value in value.items():

                        rec_dict[f"{key}_{sub_key}"] = sub_value

                # Turn lists into strings
                elif type(value) is list:

                    rec_dict[key] = str(value)

            # Add foreign keys to underlying parts of the target dict and
            # lists of those keys to the return
            for key, value in rec.get("relationships", {}).items():

                # Create list for the IDs in the return record dict
                if key[-1:] == "s":

                    rec_dict[f"{key[:-1]}_ids"] = []

                else:

                    rec_dict[f"{key}_ids"] = []

                # Some included records (instant refund vouchers for example) are dicts
                # and not lists. Here we cast them as lists for convenience
                if type(value["data"]) is dict:

                    value["data"] = [value["data"]]

                for id_dict in value["data"]:

                    # Add to record dictionary
                    rel_id = id_dict["id"]

                    if key[-1:] == "s":

                        rec_dict.get(f"{key[:-1]}_ids").append(rel_id)
                        pkey_name = f"historical_{key[:-1]}_id"

                    else:

                        rec_dict.get(f"{key}_ids").append(rel_id)
                        pkey_name = f"historical_{key}_id"

                    # Store in included item dicts for later. This primary key
                    # is also based on the return. This is because the queries
                    # filter based on returns so we want to update the related
                    # records whenever the return updates
                    target_dict[key][rel_id] = {
                        pkey_name: hashlib.md5(
                            f"{rel_id}{rec['attributes']['updated_at']}".encode("utf-8")
                        ).hexdigest(),
                        "return_id": rec["id"],
                        "return_created_at": rec["attributes"]["created_at"],
                        "return_updated_at": rec["attributes"]["updated_at"],
                        "retrieved_at": retrieved_at,
                    }

            # Ext store ID -- some don't have this field populated for some
            # reason. Hard coding the values for the three core partner brands
            if account == "jcrt":

                rec_dict["ext_store_id"] = "11699020"

            elif account == "thekitnyc":

                rec_dict["ext_store_id"] = "14614882"

            elif account == "tucker":

                rec_dict["ext_store_id"] = "9150248"

            # Add return to list of return records in target_dict
            target_dict.get("returns").append(rec_dict)

        for included in data.get("included", []):

            rel_id = included["id"]
            rel_type = included["type"]

            if rel_type[-1:] == "s":

                target_dict[rel_type][rel_id][f"{rel_type[:-1]}_id"] = rel_id

            else:

                target_dict[rel_type][rel_id][f"{rel_type}_id"] = rel_id

            # Sort each included record into its correct location based on
            # previously flagged foreign key mappings
            for key, value in included.get("attributes", {}).items():

                # non-dict and non-list fields can be added directly to the
                # target_dict.
                if type(value) is not dict or type(value) is not list:

                    target_dict[rel_type][rel_id][key] = value

                # For dicts create new fields
                elif type(value) is dict:

                    for sub_key, sub_value in value.items():

                        target_dict[rel_type][rel_id][f"{key}_{sub_key}"] = sub_value

                # Turn lists into strings
                elif type(value) is list:

                    target_dict[rel_type][rel_id][key] = str(value)

    logger.info(f"Retrieving initial data for {account}")

    async with client_session.get(url=request_url, headers=headers) as response:

        retrieved_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        try:

            response.raise_for_status()
            data = await response.json(encoding="UTF-8")

        except Exception as e:

            logger.error(f"{e.response.status_code} {e.response.reason}")

            raise

        # Loop through return records and add to their section of the target_dict

        parse_api_response(data, target_dict, retrieved_at)
        next_link = data["links"]["next"]

    if next_link is not None:

        logger.info(f"Retrieving paginated data for {account}")

    while next_link is not None:

        async with client_session.get(url=next_link, headers=headers) as response:

            retrieved_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            try:

                response.raise_for_status()
                data = await response.json(encoding="UTF-8")

            except Exception as e:

                logger.error(f"{e.response.status_code} {e.response.reason}")

                raise

            parse_api_response(data, target_dict, retrieved_at)
            next_link = data["links"]["next"]


async def get_all_returnly_data(api_token_dicts, request_url, target_dict):
    async with aiohttp.ClientSession() as client_session:

        futures = [
            get_returnly_data(
                api_token_dict=api_token_dict,
                request_url=request_url,
                target_dict=target_dict,
                client_session=client_session,
            )
            for api_token_dict in api_token_dicts
        ]

        await asyncio.gather(*futures)

    # Transform target_dict to a dict of lists of dicts for each target table
    for key, value in target_dict.items():

        if type(value) is list:

            continue

        else:

            target_dict[key] = list(target_dict.get(key, {}).values())


def to_snowflake(parsed_data, snowflake_creds, database):

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
    base_connection.cursor().execute("create schema if not exists returnly")
    base_connection.cursor().execute("use schema returnly")

    # Loop through each section of the parsed_data object
    for table, data in parsed_data.items():

        if len(data) == 0:

            continue

        # Add an 's' to the table name if there isn't one
        table = table if table[-1:] == "s" else table + "s"

        # This section assigns datatypes to each column and constructs a
        # dictionary for sqlalchemy types. The first non-None value found is
        # evaluated with conditional logic. The type of the key value is then
        # used to assign the corresponding string for Snowflake table creation
        # and a sqlalchemy datatype.
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
                # None then skip it
                elif value is None:

                    continue

                # Otherwise evaluate the value and add to dtype dict
                else:

                    if "_at" in key:

                        table_creation_str += f"{key} timestamp_tz,"
                        dtypes[key] = TIMESTAMP(timezone=True)
                        snowflake_types[key] = "timestamp_tz"

                    elif "_code" in key or "_id" in key:
                        table_creation_str += f"{key} varchar,"
                        dtypes[key] = VARCHAR
                        snowflake_types[key] = "varchar"

                    elif "_amount" in key or key == "units":

                        table_creation_str += f"{key} number(32, 16),"
                        dtypes[key] = NUMERIC(32, 16)
                        snowflake_types[key] = "number(32, 16)"

                    elif str(value).isnumeric():

                        table_creation_str += f"{key} integer,"
                        dtypes[key] = INTEGER
                        snowflake_types[key] = "integer"

                    elif type(value) is bool:

                        table_creation_str += f"{key} boolean,"
                        dtypes[key] = BOOLEAN
                        snowflake_types[key] = "boolean"

                    else:

                        table_creation_str += f"{key} varchar,"
                        dtypes[key] = VARCHAR
                        snowflake_types[key] = "varchar"

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
                f"snowflake://{snowflake_creds['user']}:{snowflake_creds['password']}@{snowflake_creds['account']}/{database}/returnly?warehouse=loader_wh"
            )
            session = sessionmaker(bind=engine)()
            alchemy_connection = engine.connect()

            logger.info(f"SQLAlchemy Snowflake session and connection created")

        except Exception as e:

            logger.error(f"SQLAlchemy Snowflake session creation failed: {e}")

            raise

        # Convert lists to strings
        for record in data:

            for key, value in record.items():

                if type(value) is list:

                    record[key] = str(value)

                if value == "":

                    record[key] = None

        dict_to_sql(data, f"{table}_stage", engine, dtypes)

        # Bind SQLAlchemy session metadata
        meta = MetaData()
        meta.reflect(bind=session.bind)
        logger.info(f"Merging data from {table} stage table to target table")
        stage_table = meta.tables[f"{table}_stage"]
        target_table = meta.tables[f"{table}"]

        # If any columns from the retrieved data aren't in the target table then
        # add them so the merge will work. This column name list isn't the same
        # as the list used to make the cols dict; that's metadata
        stage_col_names = [col.name for col in stage_table.columns._all_columns]
        target_col_names = [col.name for col in target_table.columns._all_columns]
        missing_col_names = list(set(stage_col_names).difference(target_col_names))

        if len(missing_col_names) > 0:

            alter_command_str = f"alter table {table} add "

            for col in missing_col_names:

                # Add these columns as varchar so there's no import issue
                addition = snowflake_types.get(col) or "varchar"
                alter_command_str += f"{col} {addition},"

            alter_command_str = alter_command_str[:-1]

            try:

                base_connection.cursor().execute(alter_command_str)

            except Exception as e:

                logger.error(f"Error adding missing columns to target table: {e}")

                raise

        # Primary key column will have the pattern of historical_{something}_id
        p_key = [
            i
            for i in list(set(stage_col_names).intersection(target_col_names))
            if "historical_" in i and "_id" in i
        ][0]

        # Structure merge
        merge = MergeInto(
            target=target_table,
            source=stage_table,
            on=getattr(target_table.c, p_key) == getattr(stage_table.c, p_key),
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


# App will only run in production; running in dev environments requires an override
dev_override = os.getenv("dev_override")

if __name__ == "__main__" and (
    os.getenv("RES_ENV") == "production" or dev_override == "true"
):

    # Retrieve returnly API keys for managed accounts
    api_token_dicts = get_secret("RETURNLY_API_KEYS")

    # Retrieve Snowflake credentials from secrets manager
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    # Sync setup
    sync_type = os.getenv("sync_type")

    if os.getenv("RES_ENV") != "production" or sync_type == "test":

        database = "raw_dev"

    else:

        database = "raw"

    url = "https://api.returnly.com/returns.json?page=1&include=instant_refund_voucher,invoice_items,refunds,repurchases,return_line_items,shipping_labels"

    if sync_type == "test":

        # Just retrieve returns updated within the last seven days
        ts = datetime.utcnow() + timedelta(days=-365)

    elif sync_type == "incremental":

        ts = get_latest_snowflake_timestamp(
            database,
            snowflake_creds,
        )  # Forces full sync behavior when ts is None

        logger.info("Error retrieving timestamp; defaulting to full sync")

    if sync_type == "full" or ts is None:

        logger.info("Full sync or incremental timestamp unable to be retrieved")

    else:

        ts = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logger.info(f"{sync_type} sync for Returnly since {ts}")
        url += f"&min_updated_at={ts}"

    target_dict = {
        "returns": [],
        "instant_refund_voucher": {},
        "invoice_items": {},
        "refunds": {},
        "repurchases": {},
        "return_line_items": {},
        "shipping_labels": {},
    }
    asyncio.run(
        get_all_returnly_data(
            api_token_dicts=api_token_dicts,
            request_url=url,
            target_dict=target_dict,
        )
    )
    logger.info("Records retrieved; upserting to Snowflake")
    to_snowflake(target_dict, snowflake_creds, database)
    logger.info("Records upserted to Snowflake; Returnly sync complete")
