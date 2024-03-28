import snowflake.connector
import os
import boto3, json, random, string
from res.utils import logger, secrets_client
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

# https://github.com/snowflakedb/snowflake-ingest-python
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from snowflake.connector.errors import DatabaseError, ProgrammingError

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "jk18804.us-east-1")
RES_ENV = os.getenv("RES_ENV", "development")
DEFAULT_SCHEMA = f"IAMCURIOUS_{RES_ENV}"
DEFAULT_USER = f"RES_DATA_{RES_ENV}"


class ResSnowflakeClient:
    def __init__(
        self,
        user=DEFAULT_USER,
        warehouse="IAMCURIOUS",
        db="IAMCURIOUS_DB",
        schema=DEFAULT_SCHEMA,
        profile="default",
    ):

        if profile == "data_team":

            # Retrieve Snowflake credentials
            snowflake_creds = secrets_client.get_secret(
                "SNOWFLAKE_AWS_LAMBDA_CREDENTIALS"
            )
            self._user = snowflake_creds["user"]
            self._password = snowflake_creds["password"]
            self._account = snowflake_creds["account"]

            try:

                self._conn = snowflake.connector.connect(
                    user=self._user,
                    password=self._password,
                    account=self._account,
                    warehouse=warehouse,
                )

            except DatabaseError as e:

                if e.errno == 250001:

                    logger.error(
                        "Invalid credentials when creating Snowflake connection"
                    )

                    raise

        else:

            self._user = user
            self._key = self.get_private_key()
            self._password = os.getenv("SNOWFLAKE_PASSWORD")
            self._warehouse = warehouse
            self._db = db
            self._schema = schema
            self._account = SNOWFLAKE_ACCOUNT

            try:

                self._conn = snowflake.connector.connect(
                    user=self._user,
                    account=self._account,
                    private_key=self._key,
                    warehouse=self._warehouse,
                    database=self._db,
                    schema=self._schema,
                )

            except DatabaseError as e:

                if e.errno == 250001:

                    logger.error(
                        "Invalid credentials when creating Snowflake connection"
                    )

                    raise

        self._cur = self._conn.cursor()
        self._pipe_ingesters = {}

    def initialize_pipe(self, pipe_name):
        key_text = self._p_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode("utf-8")
        pipe = f"{self._db}.{self._schema}.{pipe_name}"
        logger.info(f"Init pipe {pipe}")
        self._pipe_ingesters[pipe_name] = SimpleIngestManager(
            # we store our account with region so this is the way to split it out
            account=SNOWFLAKE_ACCOUNT.split(".")[0],
            host=f"{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
            user=self._user,
            pipe=pipe,
            private_key=key_text,
        )

    def get_private_key(self):
        # We use private key authentication instead of password. See https://coda.io/d/Platform-Documentation_dbtYplzht1S/Private-Keys_supx2#_luANX for more info
        self._p_key = serialization.load_pem_private_key(
            secrets_client.get_secret("SNOWFLAKE_PRIVATE_KEY")
            .replace(r"\n", "\n")
            .encode(),
            password=None,
        )

        pkb = self._p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return pkb

    @property
    def conn(self):
        return self._conn

    @property
    def cursor(self):
        return self._cur

    def execute(self, sql, return_type="list"):
        self.cursor.execute(sql)
        if return_type == "pandas":
            try:
                return self.cursor.fetch_pandas_all()
            except:
                import pandas as pd

                return pd.DataFrame(
                    self.cursor.fetchall(),
                    columns=[t[0] for t in self.cursor.description],
                )
        elif return_type == "list":
            results = []
            for row in self.cursor.fetchall():
                results.append(row)
            return results
        else:
            logger.error(
                "Return type not supported in Snowflake execute: {}".format(return_type)
            )

    def ingest_files(self, pipe_name, files):
        try:
            # use this type to wrap the file names
            files = [StagedFile(file, None) for file in files]
            self._pipe_ingesters[pipe_name].ingest_files(files)
            return self._pipe_ingesters[pipe_name]
        except Exception as ex:
            logger.error(f"failed to ingest files to {pipe_name} - {repr(ex)}")
            raise ex

    def load_json_data(self, json_data, table, primary_key):
        # Saves json data to S3, then loads into Snowflake with a single data column
        boto_client = boto3.client("s3")
        bucket = f"res-data-{RES_ENV}"
        random_file = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=10)
        )
        # Save to S3
        key = f"snowflake_json_loader/{table}/{random_file}.json"
        logger.debug(f"dumping to s3: {key}")
        boto_client.put_object(
            Body=json.dumps(json_data),
            Bucket=bucket,
            Key=key,
        )
        # Load to snowflake
        self.execute(
            f"CREATE OR REPLACE TABLE {DEFAULT_SCHEMA}_STAGING.{table}_temp (data VARIANT);"
        )
        temp_snowflake_client = ResSnowflakeClient(schema=f"{DEFAULT_SCHEMA}_STAGING")
        res = temp_snowflake_client.execute(
            f"""COPY INTO {DEFAULT_SCHEMA}_STAGING.{table}_temp
                            FROM @res_data_{RES_ENV}/{key}
                            FILE_FORMAT = (type = 'JSON' strip_outer_array = true);"""
        )
        logger.debug(res)
        # Copy into permanent table
        self.execute(
            f"CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{table} (data VARIANT, __filedate__ datetime);"
        )
        self.execute(
            f"""MERGE INTO {DEFAULT_SCHEMA}.{table} t1
                        USING {DEFAULT_SCHEMA}_STAGING.{table}_temp t2
                        ON t1.data:{primary_key} = t2.data:{primary_key}
                        when matched then update set
                        t1.data = t2.data,
                        t1.__filedate__=current_date
                        when not matched then insert
                        (data, __filedate__) values (t2.data, current_date);"""
        )
