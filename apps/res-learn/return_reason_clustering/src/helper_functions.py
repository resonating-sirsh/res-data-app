import hashlib
from datetime import datetime
from typing import Dict, List

import pandas as pd
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BOOLEAN, INTEGER, NUMERIC, TIMESTAMP, VARCHAR

from res.utils import logger


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
            role="loader",
        )

        logger.info("Connected to Snowflake for timestamp retrieval")

    except DatabaseError as e:
        if e.errno == 250001:
            logger.error("Invalid credentials when creating Snowflake connection")

        raise

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

        else:
            return latest_ts

    except ProgrammingError as e:
        logger.warn(
            f"Programming Error while retrieving timestamp; {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})"
        )
        raise

    except ValueError as e:
        logger.warn(
            f"Value Error while retrieving timestamp; {e.errno} ({e.sqlstate}): {e.msg} ({e.sfqid})"
        )
        raise

    except Exception as e:
        logger.warn(f"Non-Categorized Error while retrieving timestamp; {e}")
        raise


def add_p_key_and_sync_time(records: List[Dict], sync_time: datetime):
    for rec in records:
        # Create a unique key using the values of the recorded state
        record_field_hash = hashlib.md5(str(rec.values()).encode("utf-8")).hexdigest()
        rec["primary_key"] = record_field_hash

        # Add sync time. This isn't part of the primary key because
        # that can create multiple records for observations of the
        # same record state
        rec["synced_at"] = sync_time

    return records


def dict_to_sql(input_dict_list, stage_table, engine, dtype=None):
    if len(input_dict_list) > 0:
        df = pd.DataFrame.from_dict(input_dict_list)
        df = df.drop_duplicates(subset=["primary_key"])
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
