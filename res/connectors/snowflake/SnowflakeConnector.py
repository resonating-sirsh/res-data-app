import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow  # document the dep
from snowflake.connector.errors import ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import BOOLEAN, INTEGER, NUMERIC, TIMESTAMP, VARCHAR

import res
from .. import DatabaseConnector
from res.connectors.s3 import S3Connector
from res.connectors.snowflake import query_builder
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import logger, secrets_client

DEFAULT_SCHEMA = "IAMCURIOUS_DEVELOPMENT"
PIPE_SCHEMA = DEFAULT_SCHEMA


class SnowflakeConnector(DatabaseConnector):
    """
    Main connector class wraps the connector/client
    """

    def __init__(self, *args, **kwargs):
        try:
            self._s3 = S3Connector()
            self._client = ResSnowflakeClient(**kwargs)
        except Exception as ex:
            logger.warn(str(ex))
            logger.warn(
                f"Unable to setup the snowflake client - check environment variables are set {repr(ex)}"
            )

    def make_engine(self, schema="IAMCURIOUS_DEVELOPMENT"):
        """
        See https://docs.snowflake.com/en/user-guide/sqlalchemy.html

        """
        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine

        c = self._client

        engine = create_engine(
            URL(
                account=c._account,
                user=c._user,
                password=c._password,
                database=c._db,
                schema=schema,
                warehouse=c._warehouse,
            )
        )

        return engine

    def get_pipe(self, pipe_name):
        """
        a pipe is a function of files for ingestion
        """
        self._client.initialize_pipe(pipe_name)

        def f(files):
            return self._client.ingest_files(pipe_name, files)

        return f

    def as_snowlfake_table_select(self, topic):
        return f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_{topic.upper().replace('.','_')}" """

    def read_kafka_topic_from_production(self, topic, decode_content=True):
        Q = self.as_snowlfake_table_select(topic)
        res.utils.logger.info(Q)
        d = self.execute(Q)
        if decode_content:
            import json

            res.utils.logger.info(
                "Mapping JSON text column [RECORD_CONTENT] to [payloads]"
            )
            d["payloads"] = d["RECORD_CONTENT"].map(json.loads)
        return d

    def execute(self, sql, return_type="pandas"):
        return self._client.execute(sql, return_type)

    def to_dataframe(self, sql):
        return pd.DataFrame(self.execute(sql))

    #### ######################### ####
    #   table migration functions     #
    #### ######################### ####

    def _ensure_using_schema(self, context=None, schema=DEFAULT_SCHEMA):
        # TODO: for each partition key issues a delete to snowflake staging
        logger.info(
            f"Enforcing schema {schema} - {context} - if this is not expected be sure to specify schema for subsequent queries"
        )
        self._client.cursor.execute(
            f"USE SCHEMA {schema}"
        )  # the user really should not call without this, this is a safety - it is a side effect

    def _get_table_creation_script(self, table):
        try:
            query = f"""select GET_DDL( 'TABLE', '"{table}"')"""

            return self._client.execute(query).iloc[0][0]
        except Exception as ex:
            logger.error(
                f"Failed to get DDL for table. Check connection details and that the table [{table}] exists in the database and schema - {repr(ex)}"
            )
            return None

    def _describe_table(self, table):
        try:
            query = f"""desc table "{table}" """
            res = pd.DataFrame(list(self._client.cursor.execute(query)))[[0, 1]]
            res.columns = ["column", "dtype"]
            return res
        except Exception as ex:
            logger.error(
                f"Failed to table description for table [{table}] - {repr(ex)}"
            )
            return None

    def upsert(
        self,
        dataframe,
        table,
        key,
        schema="IAMCURIOUS_DEVELOPMENT",
        alter_or_create=True,
    ):
        """
        use a temp table and then merge
        """

        from snowflake.connector.pandas_tools import write_pandas

        if alter_or_create:
            self._create_or_alter_table(table, dataframe.dtypes, schema=schema)

        def stringify(c):
            return f'"{c}"'

        if not isinstance(key, list) or not isinstance(key, tuple):
            key = [key]

        con = self._client._conn
        temp_table = f"TEMP_TABLE_{res.utils.res_hash()}".upper()
        create = f"""create table {schema}.{temp_table} like {schema}."{table}" """
        drop = f"""drop table if exists {schema}.{temp_table}"""

        # res.utils.logger.debug(create)
        # res.utils.logger.debug("")

        cur = con.cursor()
        cur.execute(drop)
        cur.execute(create)

        success, nchunks, nrows, _ = write_pandas(
            con, dataframe, table_name=temp_table, schema=schema
        )
        target_columns = [c for c in dataframe.columns]
        columns = ",".join([f'"{c}"' for c in target_columns])

        upsert_str = ""
        for c in key:
            upsert_str += "Target." + f'"{c}"' + "=SOURCE." + f'"{c}"' + " and "

        if upsert_str.endswith(" and "):
            upsert_str = upsert_str[:-5]

        source_columns = ",".join(["SOURCE." + stringify(i) for i in target_columns])
        update_set = ",".join(
            [stringify(i) + "=SOURCE." + stringify(i) for i in target_columns]
        )
        Q = (
            """MERGE INTO """
            + f"""{schema}."{table}" """
            + """ as Target USING """
            + f"{schema}.{temp_table}"
            + """ AS SOURCE ON """
            + upsert_str
            + """ WHEN NOT MATCHED THEN INSERT ("""
            + columns
            + """) VALUES ("""
            + source_columns
            + """) WHEN MATCHED THEN UPDATE set """
            + update_set
            + """;"""
        )

        cur = con.cursor()
        # res.utils.logger.debug(Q)
        cur.execute(Q)

        # res.utils.logger.info(f"Dropping staging table with {drop}")
        cur.execute(drop)
        con.commit()

        res.utils.logger.info("Data upserted successfully")

    def _create_or_alter_table(self, table, columns, schema, plan=False):
        """
        columns must be a dictionary of columns with python types
        If pandas dataframes are used with dtypes use res.utils.dataframes.to_python_types to map to python types
        The table creation script will use these types mapped to SQL/Snowflake types
        """

        table = query_builder.clean_table_name(table)
        self._ensure_using_schema(context="create or alter table", schema=schema)

        query = f"""desc table "{table}" """

        existing_columns = {}

        try:
            # note today we are not able to change column types
            existing_metadata = self._client.cursor.execute(query)
            existing_columns = set(pd.DataFrame((existing_metadata))[0].values)
            column_names = set(list(columns.keys()))
            new_columns = column_names - existing_columns
            if len(new_columns) > 0:
                new_columns = {k: columns[k] for k in new_columns}
                logger.info(
                    f"Alterting table {table} with {len(new_columns.keys())} columns. Had {len(existing_columns)}."
                )
                query = query_builder.alter_table(table, new_columns)
        # TODO: catch specifically table not exists or determine not exists for safer -> it seems create overwrites which i did not expect
        except Exception as ex:
            logger.debug(
                f"Creating table {table} with {len(columns.keys())} columns after failure {repr(ex)}"
            )
            query = query_builder.create_table(table, columns)
            if plan:
                return query
        try:
            res.utils.logger.debug(query)
            self._client.cursor.execute(query)
        except Exception as ex:
            logger.debug(
                f"Failed to alter or create table {table} in the staging schema with user {os.environ.get('SNOWFLAKE_USER')} - {repr(ex)}"
            )

    def _create_or_alter_pipe(self, table_name, namespace, column_types, schema):
        table_name = query_builder.clean_table_name(table_name)
        query, pipe_name = query_builder.create_pipe(
            table_name, namespace=namespace, column_types=column_types, schema=schema
        )
        logger.info(f"Ensuring pipe exists for ({namespace}) {table_name}: {query}")
        self._client.cursor.execute(query)
        logger.info(f"DONE with pipe - {pipe_name}")
        return pipe_name

    def create_external_table(self, table, stage=None):
        """
        S3__BODY_POINTS_OF_MEASURE_EXT
        S3__BODY_POINTS_OF_MEASURE_EXT_ENVSTAGE
        """

        stage = stage or f"{table}_ENVSTAGE"
        self._ensure_snow_stage_exists(stage)

        q = f"""CREATE OR REPLACE EXTERNAL TABLE {table}
            WITH LOCATION = @{stage}/
            FILE_FORMAT = (TYPE = PARQUET )"""

        self._client.cursor.execute(q)

    def refresh_stage(self, stage):
        """
        e.g S3__BODY_POINTS_OF_MEASURE_EXT REFRESH
        """
        q = f"""   ALTER EXTERNAL TABLE {stage}; """
        self._client.cursor.execute(q)

    def _ensure_snow_stage_exists(
        self, table_name, namespace, location, format="PARQUET"
    ):
        """
        we create a stage using AWS creds for a snowflake user over an encrypted connection
        For the moment this is using parquet but in future it could be anything
        """
        # TODO: check table name for bad chars when creating stages and pipes/ todo create a snowflake user
        table_ref = query_builder.clean_table_name(table_name)
        stage = (
            f"{namespace}_{table_ref}_envstage"
            if namespace
            else f"{table_ref}_envstage"
        ).upper()

        logger.info(f"Ensuring stage {stage} exists")
        create_stage_query = f"""
            create or replace stage {stage} url='{location}'
            credentials=(aws_key_id='{os.environ.get("AWS_ACCESS_KEY_ID")}' aws_secret_key='{os.environ.get("AWS_SECRET_ACCESS_KEY")}')
            file_format = (type = '{format}');"""

        self._client.cursor.execute(create_stage_query)

        return stage

    def _remove_staging_partitions(self, table, partition_info, schema=DEFAULT_SCHEMA):
        """
        Delete data from the staging table based on the partition keys
        This is done because we are re-ingesting a partition that was invalidated
        We could do this at row level for efficiency but its easier to drop the partition which is not big data for us generally

        @table: the name of the table in stating/prod
        @partition_info an s3.ls result with a list of paths and info such as size and last_modified in a dictionary
        [{'path': ..., 'size': ...., 'last_modified' : ... }, , ]

        """
        self._ensure_using_schema(context="remove stage partitions", schema=schema)

        logger.info("Checking for stale partitions using modified since last writes...")

        # determine all partitions for the table and the last time they were updated - do the diff and execute deletions for stale
        # maybe a stored proc that takes the two collections and queries for changed partitions (dry run or delete)

    def _merge_from_staging_to_prod(self, table, metadata):
        query = query_builder.merge_staging_to_prod(table, metadata)
        self._client.cursor.execute(query)

    def _ingest_files_to_pipe(
        self, table_name, namespace, files, pipe_name=None, plan=True
    ):
        # TODO: check table name for bad chars when creating stages and pipes
        table_name = query_builder.clean_table_name(table_name)
        pipe_name = pipe_name or (
            f"{namespace}_{table_name}_pipe" if namespace else f"{table_name}_pipe"
        )
        pipe = self.get_pipe(pipe_name)
        logger.info(f"beginning ingestion of {len(files)} files to {pipe_name}")
        return pipe(files)

    def ingest_from_s3_path(
        self,
        stage_root,
        name,
        namespace="res_data",
        snow_schema="IAMCURIOUS_DEVELOPMENT",
        modified_after=None,
    ):
        """
        Ingest from S3 partitions into snowflake
        stage_root is ideally a path to strongly typed parquet files so we can properly infer types for creating tables in snowflake
        The name of the table is prefixed with a namespace such as airtable or res_data
        Snowpipe will ingest all files under the stage root but the files can be filtered by date for efficiency using the modified_after
        The snow_schema can be set as a parameter and we do not currently choose from th environment here

        Example:

        snowflake_connector.ingest_from_s3_path(stage_root="s3://res-data-platform/entities/order_items/ALL_GROUPS/", name="batch_one_orders")
        """
        # read sample file to get types
        table_name = f"{namespace}_{name}"
        sample = None

        if stage_root[-1] != "/":
            stage_root += "/"

        sample = list(self._s3.ls(stage_root))[-1]

        logger.info(f"Reading a sample file {sample} to determine types")
        sample = self._s3.read(sample)

        if sample is None:
            logger.warn(f"There were no files to ingest at the path {stage_root}")
            return

        dtypes = sample.dtypes
        cols = sorted(list(sample.columns))
        dtypes = {c: dtypes[c] for c in cols}

        logger.debug(
            f"Reading a sample file that is the most recent with these columns and types for use in snowflake create or alter \n {dtypes}"
        )

        paths = [
            str(Path(str(info["path"])).relative_to(stage_root))
            for info in list(
                self._s3.ls_info(stage_root, modified_after=modified_after)
            )
        ]

        self._create_or_alter_table(table_name, sample.dtypes, schema=snow_schema)
        stage = self._ensure_snow_stage_exists(
            table_name, namespace, location=stage_root
        )
        pipe_name = self._create_or_alter_pipe(
            table_name, namespace, column_types=sample.dtypes, schema=snow_schema
        )

        # snowpipe will replace partitions without duplicates so just decide which paths for efficiency
        return self._ingest_files_to_pipe(
            table_name, namespace, paths, pipe_name=pipe_name
        )

    def write(self, table_name, key, data, schema="IAMCURIOUS_DEVELOPMENT"):
        self._create_or_alter_table(table_name, data.dtypes, schema=schema)

        # easiest way to upsert to snowflake from pandas

    def ingest_partitions(
        self,
        table_name,
        stage_root,
        namespace=None,
        table_metadata=None,
        partitions_modified_since=None,
        trigger_upsert_transaction=False,
        snow_schema="IAMCURIOUS_DEVELOPMENT",
        table_discriminator=None,
    ):
        """
        This is the second part of the table migration process (used for airtable currently)
        Airtable snaphots are moved and cleaned to parquet files on S3 and then piped to snowflake staging

        - @table_name: the table name used to create tables in Snowflake
        - @namespace is used to mange pipes and stages so that tables names are qualified e.g. to airtable
        - @stage_root: a collection of s3 collections with parquet encoded tables to ingest
        - @table_metadata: the metadata that we store and airtable stores per table - currently we are not using this and just the stored data sample
        - @partitions_modified_since: only consider partitions in the stage modified since a certain date e.g. since last time we ran
          - its ok to re-ingest things even if we have already ingested them but not efficient so this is only a hint
        - @trigger_upsert_transaction: optionally trigger an UPSERT from staging to prod in snowflake.
          - At the moment we control ingestion but with auto ingestion we would update how this works eg. make sure job is done before upserting

        in future we may use auto ingestion and then we need to "block" before doing the final merge

        stage root airtable convention example: 's3://airtable-latest/BASE_ID/TABLE_ID/partitions/'

        example:

        snowflake = res.connectors.load('snowflake')
        #the return value for this gives status - poll it - r.get_history()
        r = snowflake.ingest_partitions(table_name='S3__BODY_POINTS_OF_MEASURE',
                                        #the / currently at the end is required
                                        stage_root = 's3://res-data-platform/data/staging/rulers/',
                                        #use whatever schema
                                        snow_schema="IAMCURIOUS_PRODUCTION_STAGING",
                                        #only load files after a certain date
                                        partitions_modified_since=None)

        #r.get_history()
        snowflake.execute('SELECT * FROM IAMCURIOUS_PRODUCTION_STAGING."S3__BODY_POINTS_OF_MEASURE" ')


        """
        if table_discriminator is not None:
            table_name = f"{namespace}_{table_discriminator}_{table_name}"

        table_name = query_builder.clean_table_name(table_name)

        logger.info(f"ingesting partitions from {stage_root} for table {table_name}")
        # include dates so we can decide what partitions have changed since last ingestion
        partition_info = list(
            self._s3.ls_info(stage_root, modified_after=partitions_modified_since)
        )
        # for snowpipe we only refer to the relative path of the file from the stage
        paths = [
            str(Path(str(info["path"])).relative_to(stage_root))
            for info in partition_info
        ]
        if len(paths) == 0:
            logger.warn(f"There are no files in this staging location. Skipping")
            return
        logger.info("loading a sample file to infer schema info")
        sample = self._s3.read(partition_info[-1]["path"])

        self._ensure_using_schema(schema=snow_schema)
        python_types = res.utils.dataframes.to_python_types(sample.dtypes)
        self._create_or_alter_table(table_name, python_types, schema=snow_schema)
        self._ensure_snow_stage_exists(table_name, namespace, location=stage_root)
        pipe_name = self._create_or_alter_pipe(
            table_name, namespace, column_types=sample.dtypes, schema=snow_schema
        )
        # snowpipe will replace partitions without duplicates so just decide which paths for efficiency
        status_handler = self._ingest_files_to_pipe(
            table_name, namespace, paths, pipe_name=pipe_name
        )
        # check status ??
        if trigger_upsert_transaction:
            self._merge_from_staging_to_prod(table_name)

        return status_handler

    def get_daily_table_usage(
        self,
        tables,
        tday=datetime.now().date(),
        write_partitions=False,
        entity_name="infra/snow/selected_table_usage",
    ):
        """
        for a date offset such as today, get 24 hour aggregates for table statistics
        this is a basic usage report that we could update
        the query search for tables is fuzzy and should be used with care
        """

        end_day = tday + timedelta(days=1)

        QUERY = f"""
        select QUERY_TEXT, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED, ROWS_PRODUCED,COMPILATION_TIME, EXECUTION_TIME, CREDITS_USED_CLOUD_SERVICES 
            from snowflake.account_usage.query_history
            where START_TIME BETWEEN '{tday.isoformat()}' and '{end_day.isoformat()}'
            """

        res.utils.logger.info(f"Fetching query date for date: {QUERY}")

        data = self._client.execute(QUERY)

        res.utils.logger.info(f"loaded {len(data)} rows")

        METRIC_COLS = [
            "TOTAL_ELAPSED_TIME",
            "BYTES_SCANNED",
            "ROWS_PRODUCED",
            "COMPILATION_TIME",
            "EXECUTION_TIME",
            "CREDITS_USED_CLOUD_SERVICES",
        ]

        data["QUERY_TEXT"] = data["QUERY_TEXT"].map(lambda x: x.upper())
        data["contains_any"] = None

        res.utils.logger.info("Processing table usage based on queries")

        def get_tables(s):
            tt = [t for t in tables if t.upper() in s]
            return tt if len(tt) > 0 else None

        data["TABLES"] = data["QUERY_TEXT"].map(get_tables)
        data = data[data["TABLES"].notnull()].reset_index()
        if len(data) > 0:
            # WILL WE KNOW THE SCHEMA
            data = data.explode("TABLES").drop(["contains_any", "index"], 1)
            data["DATE"] = data["START_TIME"].dt.date

            # make sure of floating metrics
            for m in METRIC_COLS:
                data[m] = data[m].astype(float)

            # aggregate by table and date
            data = data.groupby(["TABLES", "DATE"]).sum()[METRIC_COLS].reset_index()

            res.utils.logger.info(f"Writing entities for system entity {entity_name}")
            s3 = res.connectors.load("s3")
            if write_partitions:
                s3.write_date_partition(
                    data, entity=entity_name, date_partition_key="DATE"
                )
        else:
            res.utils.logger.info(f"there are no records for these tables")

        return data

    # def _snippet_ingest_fromMongo():
    #     import res
    #     from snowflake.connector.pandas_tools import write_pandas

    #     snowflake = res.connectors.load("snowflake")
    #     mongo = res.connectors.load("mongo")
    #     b = mongo["resmagic"]["bodies"].find()
    #     conn = snowflake._client.conn

    #     """
    #     create table "IAMCURIOUS_DB"."IAMCURIOUS_SCHEMA"."MONGO_RESMAGIC_BODIES_TEST" (
    #     "_id" string,
    #     "legacyAttributes" variant
    #     )

    #     """

    #     b = m.copy()
    #     b["legacyAttributes"] = b["legacyAttributes"].map(str)

    #     success, nchunks, nrows, _ = write_pandas(
    #         conn,
    #         b,
    #         table_name="MONGO_RESMAGIC_MATERIALS_TEST",
    #         schema="IAMCURIOUS_SCHEMA",
    #     )

    def get_latest_snowflake_timestamp(
        self,
        database,
        schema,
        table,
        timestamp_field="synced_at",
        timestamp_format_str=None,
    ):

        # Fetch latest date from table
        schema = schema.upper()
        table = table.upper()

        try:
            latest_ts = (
                self._client._conn.cursor()
                .execute(
                    f"select max({timestamp_field}) from {database}.{schema}.{table}"
                )
                .fetchone()[0]
            )

            latest_ts = (
                latest_ts.strftime(timestamp_format_str)
                if timestamp_format_str
                else latest_ts
            )

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

    def to_snowflake(
        self,
        data: list[dict],
        database: str,
        schema: str,
        table: str,
        add_primary_key: bool = False,
        add_ts: bool = False,
        primary_key_fieldname: str = None,
        ts_fieldname: str = None,
    ):
        if add_primary_key:

            if primary_key_fieldname is None:

                logger.info(
                    "No primary key fieldname provided, using the name 'primary_key'"
                )
                primary_key_fieldname = "primary_key"

                for rec in data:

                    # Add a unique key and sync timestamp to the record
                    record_field_hash = hashlib.md5(
                        str(rec.values()).encode("utf-8")
                    ).hexdigest()
                    rec["primary_key"] = record_field_hash

        if add_ts:

            if ts_fieldname is None:

                logger.info(
                    "No timestamp fieldname provided, using the name 'last_synced_at'"
                )
                ts_fieldname = "last_synced_at"

                for rec in data:

                    # Add sync time. This isn't part of the primary key because
                    # that can create multiple records for observations of the
                    # same record state
                    rec["last_synced_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    )

        # Retrieve Snowflake credentials
        snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

        # Create schema and tables
        base_connection = self._client._conn
        logger.info("Creating schema and tables if they do not exist")
        base_connection.cursor().execute(f"use database {database}")
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
                    elif (
                        type(value) is str or type(value) is dict or type(value) is list
                    ):
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
        for record in data:
            for key, value in record.items():
                if type(value) is dict or type(value) is list:
                    record[key] = str(value)

        df = pd.DataFrame.from_dict(data).drop_duplicates()
        df.to_sql(
            f"{table}_stage",
            engine,
            index=False,
            chunksize=5000,
            if_exists="replace",
            dtype=dtypes,
        )

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
            on=getattr(target_table.c, primary_key_fieldname)
            == getattr(stage_table.c, primary_key_fieldname),
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

        # Close sqlalchemy connection
        alchemy_connection.close()
        engine.dispose()
        logger.info("sqlalchemy connection closed; sqlalchemy engine disposed of")
