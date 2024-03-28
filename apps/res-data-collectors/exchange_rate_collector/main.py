from botocore.exceptions import ClientError
from datetime import date, timedelta
from res.utils.logging.ResLogger import ResLogger
from snowflake.connector.errors import DatabaseError, ProgrammingError
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
import base64
import boto3
import datetime
import hashlib
import json
import os
import pandas as pd
import requests
import snowflake.connector

# Logger setup
logger = ResLogger()

# Create empty list to append to
flat_currency_data = []


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


# Retrieve secrets
api_key = get_secret("EXCHANGE_RATES_DATA_API_KEY").get("api_key")
snowflake_cred = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
snowflake_user = snowflake_cred["user"]
snowflake_password = snowflake_cred["password"]
snowflake_account = snowflake_cred["account"]


def create_snowflake_tables():

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
            database="raw",
        )

        logger.info("Connected to Snowflake")

    except DatabaseError as e:

        if e.errno == 250001:

            logger.error("Invalid credentials when creating Snowflake connection")
            return e

        else:

            return e

    # Create schema and tables
    logger.info("Creating schema and tables if they do not exist")

    base_connection.cursor().execute("create schema if not exists macroeconomic_data")
    base_connection.cursor().execute("use schema macroeconomic_data")
    base_connection.cursor().execute(
        """
            create table if not exists historical_rates (
                historical_exchange_rate_id varchar(16777216),
                base_currency_code varchar(16777216),
                conversion_currency_code varchar(16777216),
                conversion_rate number(32, 16),
                date_day date
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def get_rates(api_key, sync_type, start_date=None, end_date=None):
    def inclusive_range(start, end):

        return range(start, end + 1)

    if sync_type == "incremental":

        logger.info("Starting incremental sync")

        try:

            # Connect to Snowflake
            base_connection = snowflake.connector.connect(
                user=snowflake_user,
                password=snowflake_password,
                account=snowflake_account,
                warehouse="loader_wh",
                database="raw",
            )

            logger.info("Connected to Snowflake for timestamp retrieval")

        except DatabaseError as e:

            if e.errno == 250001:

                logger.error("Invalid credentials when creating Snowflake connection")
                return e

            else:

                return e

        try:

            # Fetch latest record year from rates table
            latest_date = (
                base_connection.cursor()
                .execute(
                    'select max(date_day) from "RAW"."MACROECONOMIC_DATA"."HISTORICAL_RATES"'
                )
                .fetchone()[0]
            ).strftime("%Y-%m-%d")

            # Close base connection
            base_connection.close()
            logger.info("Snowflake for python connection closed for date retrieval")

            if latest_date is None:
                raise ValueError

        except ProgrammingError as e:

            logger.error("Programming error while retrieving latest date")

            raise e

        except ValueError as e:

            logger.error("No date available")

            raise e

        except Exception as e:

            logger.error(f"Uncategorized error: {e}")

            raise e

        current_date = datetime.date.today()
        latest_year = int(latest_date[0:4])
        current_date = datetime.date.today()
        current_year = current_date.year

        for year in inclusive_range(latest_year, current_year):

            start_date = f"{latest_date}" if year == latest_year else f"{year}-01-01"
            end_date = f"{current_date}" if year == latest_year else f"{year}-12-31"

            url = f"https://api.apilayer.com/exchangerates_data/timeseries"
            headers = {"apikey": api_key}
            params = {"start_date": start_date, "end_date": end_date, "base": "USD"}

            try:

                response = requests.get(url=url, headers=headers, params=params)
                response.raise_for_status()

            except Exception as e:

                logger.error(f"{response.status} {response.reason}")

            data = json.loads(response.text)

            if data["success"] != True:

                logger.error("API response unsuccessful")

            for date_key in data["rates"]:

                date_day = str(date_key)

                for rate_key in data["rates"][date_key]:

                    historical_exchange_rate_concat = f"{rate_key}{date_day}"
                    historical_exchange_rate_id = hashlib.md5(
                        historical_exchange_rate_concat.encode("utf-8")
                    ).hexdigest()

                    flat_currency_data.append(
                        {
                            "historical_exchange_rate_id": historical_exchange_rate_id,
                            "base_currency_code": "USD",
                            "conversion_currency_code": rate_key,
                            "conversion_rate": data["rates"][date_key][rate_key],
                            "date_day": date_day,
                        }
                    )

    elif sync_type == "latest":

        logger.info("Starting latest data sync")

        url = f"https://api.apilayer.com/exchangerates_data/latest"
        headers = {"apikey": api_key}
        params = {"base": "USD"}

        response = requests.get(url=url, headers=headers, params=params)

        data = json.loads(response.text)

        for rate_key in data["rates"]:

            latest_date = str(data["date"])
            historical_exchange_rate_concat = f"{rate_key}{latest_date}"
            historical_exchange_rate_id = hashlib.md5(
                historical_exchange_rate_concat.encode("utf-8")
            ).hexdigest()

            flat_currency_data.append(
                {
                    "historical_exchange_rate_id": historical_exchange_rate_id,
                    "base_currency_code": "USD",
                    "conversion_currency_code": rate_key,
                    "conversion_rate": data["rates"][rate_key],
                    "date_day": latest_date,
                }
            )

    else:

        if sync_type == "full":

            logger.info("Starting full historical sync")

        else:

            logger.info("No sync type provided; starting full historical sync")

        current_date = datetime.date.today()
        current_year = int(current_date.year)

        if str(current_date)[6:] == "01-01":

            # The API returns at max the previous day's rates.
            # This ensures a wasted API call does not happen
            current_year -= 1
            current_date = date(current_year, 12, 31)

        for year in inclusive_range(2017, current_year):

            start_date = f"{year}-01-01"
            end_date = current_date if year == current_year else f"{year}-12-31"

            url = f"https://api.apilayer.com/exchangerates_data/timeseries"
            headers = {"apikey": api_key}
            params = {
                "start_date": f"{start_date}",
                "end_date": f"{end_date}",
                "base": "USD",
            }

            response = requests.get(url=url, headers=headers, params=params)

            data = json.loads(response.text)

            for date_key in data["rates"]:

                date_day = str(date_key)

                for rate_key in data["rates"][date_key]:

                    historical_exchange_rate_concat = f"{rate_key}{date_day}"
                    historical_exchange_rate_id = hashlib.md5(
                        historical_exchange_rate_concat.encode("utf-8")
                    ).hexdigest()

                    flat_currency_data.append(
                        {
                            "historical_exchange_rate_id": historical_exchange_rate_id,
                            "base_currency_code": "USD",
                            "conversion_currency_code": rate_key,
                            "conversion_rate": data["rates"][date_key][rate_key],
                            "date_day": date_day,
                        }
                    )


def upsert_rates_data(input_data):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/raw/macroeconomic_data?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()
    dtype = {
        "historical_exchange_rate_id": VARCHAR,
        "base_currency_code": VARCHAR,
        "conversion_currency_code": VARCHAR,
        "conversion_rate": NUMERIC(32, 16),
        "date_day": DATE,
    }

    # Convert data
    if len(input_data) > 0:

        df = pd.DataFrame.from_dict(input_data)
        df.to_sql(
            "historical_rates_stage",
            engine,
            index=False,
            chunksize=5000,
            if_exists="replace",
            dtype=dtype,
        )

    else:

        raise ValueError("Retrieved data is empty")

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stage table to target table")

    # Create stage and target tables
    historical_rates_stage_table = meta.tables["historical_rates_stage"]
    historical_rates_target_table = meta.tables["historical_rates"]

    # Structure merge
    historical_rates_merge = MergeInto(
        target=historical_rates_target_table,
        source=historical_rates_stage_table,
        on=historical_rates_target_table.c.historical_exchange_rate_id
        == historical_rates_stage_table.c.historical_exchange_rate_id,
    )

    historical_rates_merge.when_not_matched_then_insert().values(
        historical_exchange_rate_id=historical_rates_stage_table.c.historical_exchange_rate_id,
        base_currency_code=historical_rates_stage_table.c.base_currency_code,
        conversion_currency_code=historical_rates_stage_table.c.conversion_currency_code,
        conversion_rate=historical_rates_stage_table.c.conversion_rate,
        date_day=historical_rates_stage_table.c.date_day,
    )

    historical_rates_merge.when_matched_then_update().values(
        historical_exchange_rate_id=historical_rates_stage_table.c.historical_exchange_rate_id,
        base_currency_code=historical_rates_stage_table.c.base_currency_code,
        conversion_currency_code=historical_rates_stage_table.c.conversion_currency_code,
        conversion_rate=historical_rates_stage_table.c.conversion_rate,
        date_day=historical_rates_stage_table.c.date_day,
    )

    # Execute merge
    alchemy_connection.execute(historical_rates_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


# App will only run in production; running in dev environments requires an override
dev_override = os.getenv("dev_override")

if __name__ == "__main__" and (
    os.getenv("RES_ENV") == "production" or dev_override == "true"
):

    # Retrieve sync type from env, get rates, and send to Snowflake
    sync_type = os.getenv("sync_type")
    get_rates(api_key=api_key, sync_type=sync_type)
    create_snowflake_tables()
    upsert_rates_data(flat_currency_data)
