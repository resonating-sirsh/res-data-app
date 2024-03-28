import asyncio
import base64
import json
import os
import sys

import boto3
import requests
import snowflake.connector
from botocore.exceptions import ClientError

from res.utils import logger


class DBT_CLOUD_API_EXCEPTION(Exception):

    """
    Raised when dbt cloud metadata API returns an error. These error
    responses still have a general response code of 200
    """

    def __init__(self):

        primary_error = self["data"]["errors"][0]
        message = primary_error.get("message")
        path = (primary_error.get("path", []) or [])[0]
        code = primary_error.get("extensions", {}).get("code")
        other_errors = self["data"]["errors"][1:]


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


def get_dbt_job_surfaced_models(
    service_token: str,
    url: str,
    job_id: str,
):

    """
    Retrieves the models built in the most recent run corresponding to the
    dbt Cloud job_id provided to the function. Limits the retrieved items to
    fact, fct, dim, dm, br, and bridge models

    Arguments:

        - service_token: dbt cloud service token to access the Metadata API
        - url: dbt cloud URL corresponding to account type and region name
        - job_id: dbt cloud job ID to retrieve run metadata for
    """

    headers = {
        "authorization": f"Bearer {service_token}",
        "content-type": "application/json",
    }
    query_str = """
        {
            models(jobId: %s) {
                name
                database
                schema                                                                                                   
            }
        }
        """ % (
        job_id
    )

    while True:

        try:

            response = requests.post(
                url=url, headers=headers, json={"query": query_str}
            )
            response.raise_for_status()
            data = json.loads(response.text)

            # Response can either say data is none or not have the key
            if data.get("data") is None:

                raise DBT_CLOUD_API_EXCEPTION(data)

            break

        except DBT_CLOUD_API_EXCEPTION as e:

            logger.error(f"{e.code} + {e.message} for path {e.path}")

            raise

        except Exception as e:

            logger.error(f"{e.response.status_code} {e.response.reason}")

            raise

    # Surfaced models are models that are prefixed with fact, fct, dim, dm,
    # bridge, or br
    accepted_prefixes = [
        "fact",
        "fct",
        "dim",
        "dm",
        "bridge",
        "br",
    ]
    surfaced_models = [
        model_dict
        for model_dict in data["data"]["models"]
        if any(
            model_dict.get("name", "").split("_")[0] in prefix
            for prefix in accepted_prefixes
        )
    ]

    return surfaced_models


async def get_model_columns(
    model_dicts,
    snowflake_connection,
    target_database,
    target_schema,
    target_table,
):

    """
        Takes a list of table-defining dicts as an input and a target database, 
        schema, and table from a single item in that list as an input. From a 
        given Snowflake connection, creates cursors to retrieve column metadata 
        for each of those models (tables) and adds that data to the 
        corresponding model dict.

        Arguments:

        - model_dicts: List of dictionaries each of which represents a single \
            model / table within Snowflake built by a dbt cloud production run
        - snowflake_connection: Snowflake credentials / config connection object
        - target_database: Snowflake database of the model / table 
        - target_schema: Snowflake schema of the model / table  
        - target_table: Snowflake table name of the model / table         
    """

    try:

        # Get column information from the relevant information_schema.columns
        # table
        cur = snowflake_connection.cursor()
        cur.execute(
            f"""
                select
                    column_name,
                    data_type,
                    coalesce(comment, '') as comment
                from {target_database}.information_schema.columns
                where table_schema = '{target_schema.upper()}'
                    and table_name = '{target_table.upper()}'

            """
        )
        columns = []

        for column_name, data_type, comment in cur:

            columns.append(
                {
                    "column_name": column_name,
                    "data_type": data_type,
                    "comment": comment.replace('"', ""),
                }
            )

        # Sort columns. Looker alphabetizes columns when importing from
        # Snowflake; this is meant to mimic that behavior
        columns = sorted(columns, key=lambda d: d["column_name"])

    except Exception as e:

        logger.error(e)

        raise

    finally:

        cur.close()

    # Add the columns to the correct model in model dicts
    for model_dict in model_dicts:

        if (
            model_dict["database"] == target_database
            and model_dict["schema"] == target_schema
            and model_dict["name"] == target_table
        ):

            model_dict["columns"] = columns


async def get_all_models_columns(
    model_dicts,
    snowflake_user,
    snowflake_password,
    snowflake_account,
):

    "Async wrapper to retrieve column data for each model in model_dicts"

    try:

        # Connect to Snowflake
        snowflake_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            role="ACCOUNTADMIN",
            warehouse="DBT_TO_LOOKER",
        )
        logger.info("Connected to Snowflake")

    except Exception as e:

        logger.error(f"{e}")

        raise

    futures = [
        get_model_columns(
            model_dicts,
            snowflake_connection,
            model_dict["database"],
            model_dict["schema"],
            model_dict["name"],
        )
        for model_dict in model_dicts
    ]

    await asyncio.gather(*futures)


def write_looker_files(model_dicts, github_token):

    """
    Function to send generate lkml files and then send those files to
    github. Done synchronously because the Github API doesn't like it when
    endpoints get spammed
    """

    def model_to_github(model_name: str, looker_view: str):

        # Turn looker view string into base64 encoded string
        looker_view_bytes = looker_view.encode("utf-8")
        b64_looker_view_bytes = base64.b64encode(looker_view_bytes)
        b64_looker_view_str = b64_looker_view_bytes.decode("utf-8")

        # Create put request
        url = f"https://api.github.com/repos/resonance/looker-analytics/contents/views/generated_view_files/{model_name}.view.lkml"
        body = {
            "message": f"Added {model_name}.view.lkml to views/generated_view_files",
            "content": b64_looker_view_str,
            "branch": branch,
            "committer": {"name": "dbt-to-looker", "email": "data@resonance.nyc"},
        }

        # Check if the file being created already exists and thus requires a
        # blob SHA for the request
        if any(model_name == file["model_name"] for file in existing_files):

            body["sha"] = [
                file["sha"]
                for file in existing_files
                if file["model_name"] == model_name
            ][0]

        try:

            response = requests.put(url=url, headers=headers, json=body)
            response.raise_for_status()
            logger.info(f"Added model {model_name}")

        except Exception as e:

            logger.error(f"{e.response.status_code} {e.response.reason}")

            raise

    # Select target branch based on res_env
    if RES_ENV == "production" and not IS_TEST:

        branch = "main"

    else:

        # Set branch to dev
        branch = "dbt-to-looker-dev"

    # Set headers for authentication
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Only update files that are not already saved outside eof the output
    # folder. Use a recursive function to capture filenames from each level of
    # the repository and perform a GET request for any directories within a
    # directory
    files_to_exclude = []

    # Recursive function
    def excavate(data):

        # Capture files matching criteria and add to exclusion list
        files_to_exclude.extend(
            [
                file_dict.get("name").split(".")[0].lower()
                for file_dict in data
                if (".view.lkml" in file_dict["name"] and file_dict["type"] == "file")
            ]
        )

        buried_directories = [
            file_dict["path"]
            for file_dict in data
            if (
                file_dict["type"] == "dir"
                and "views/generated_view_files" not in file_dict["path"]
            )
        ]

        for path in buried_directories:

            try:

                response = requests.get(
                    url=root_dir_url + path, headers=headers, params={"ref": branch}
                )
                response.raise_for_status()
                data = json.loads(response.text)

            except Exception as e:

                logger.error(f"{e.response.status_code} {e.response.reason}")

                raise

            excavate(data)

    logger.info("Checking repo for files to exclude from Github upload")
    root_dir_url = "https://api.github.com/repos/resonance/looker-analytics/contents/"

    try:

        response = requests.get(
            url=root_dir_url, headers=headers, params={"ref": branch}
        )
        response.raise_for_status()
        data = json.loads(response.text)

    except Exception as e:

        logger.error(f"{e.response.status_code} {e.response.reason}")

        raise

    # Start diggin'
    excavate(data)

    # Exclude found files
    model_dicts = [
        model_dict
        for model_dict in model_dicts
        if not any(
            model_dict["name"].lower() == excluded_file.lower()
            for excluded_file in files_to_exclude
        )
    ]

    # Updating a file via the Github API requires the blob SHA of the file being
    # replaced/updated. Retrieve the SHA for existing files in the target folder
    # and update the PUT request when needed
    logger.info("Checking branch for existing versions of view files")
    url = "https://api.github.com/repos/resonance/looker-analytics/contents/views/generated_view_files"

    try:

        response = requests.get(url=url, headers=headers, params={"ref": branch})
        response.raise_for_status()
        existing_files = [
            {
                "model_name": file_dict.get("name").split(".")[0].lower(),
                "sha": file_dict["sha"],
            }
            for file_dict in json.loads(response.text)
        ]

    except Exception as e:

        logger.error(f"{e.response.status_code} {e.response.reason}")

        raise

    logger.info(f"Adding looker view files to branch {branch}")

    for model_dict in model_dicts:

        model_name = model_dict["name"].lower()

        # Create Looker file text starter
        looker_view = (
            f"view: {model_name}" + " {\n"
            "  sql_table_name:\n"
            f"    -- if dev -- @{{dev_connection_prefix}}.{model_name}\n"
            f"    -- if prod -- iamcurious_production_models.{model_name}\n"
            "    ;;        \n"
            "\n"
            "# This model was automatically generated using a Python script.\n"
            "# Please inspect the model to declare a primary_key, hide \n"
            "# dimensions, modify descriptions, and add measures\n"
            "\n"
            "  ## Dimensions\n"
        )

        # Move through columns alphabetically. Process timestamps separately
        timestamp_columns = []

        for col in model_dict["columns"]:

            if "timestamp" in col["data_type"].lower():

                timestamp_columns.append(col)

                continue

            else:

                # Determine column type
                if col["data_type"] == "BOOLEAN":

                    col_type = "yesno"

                elif col["data_type"] == "NUMBER":

                    col_type = "number"

                else:

                    col_type = "string"

                dimension_str = (
                    f"  dimension: {col['column_name'].lower()}"
                    " {\n"
                    "    description:\n"
                    f"      \"{col['comment']}\"\n"
                    f"    type: {col_type}\n"
                    f"    sql: ${{TABLE}}.\"{col['column_name'].upper()}\" ;;\n"
                    "  }\n"
                    "\n"
                )
                looker_view += dimension_str

        if len(timestamp_columns) > 0:

            looker_view += "\n"
            looker_view += "  ## Dates and Timestamps\n"

            for col in timestamp_columns:

                dimension_str = (
                    f"  dimension_group: {col['column_name'].lower()}"
                    "   {\n"
                    "    description:\n"
                    f"      \"{col['comment']}\"\n"
                    "    type: time\n"
                    "    timeframes: [\n"
                    "      raw,\n"
                    "      time,\n"
                    "      date,\n"
                    "      hour,\n"
                    "      hour_of_day,\n"
                    "      time_of_day,\n"
                    "      day_of_month,\n"
                    "      day_of_week,\n"
                    "      day_of_week_index,\n"
                    "      week,\n"
                    "      week_of_year,\n"
                    "      month,\n"
                    "      month_num,\n"
                    "      month_name,\n"
                    "      quarter,\n"
                    "      year\n"
                    "    ]\n"
                    f"    sql: ${{TABLE}}.\"{col['column_name'].upper()}\" ;;\n"
                    "  }\n"
                    "\n"
                )
                looker_view += dimension_str

        looker_view += "\n" "}" "\n"

        # Send to github
        model_to_github(model_name, looker_view)


# Constants and args. App only runs in production unless an override is provided
RES_ENV = os.getenv("RES_ENV", "development")
DEV_OVERRIDE = os.getenv("DEV_OVERRIDE", "").lower() == "true"
IS_TEST = os.getenv("IS_TEST", "").lower() == "true"
TARGET_DBT_MODEL_OVERRIDE = os.getenv("TARGET_DBT_MODEL_OVERRIDE", "none")
TARGET_JOB_ID_OVERRIDE = os.getenv("TARGET_JOB_ID_OVERRIDE", "none")

if __name__ == "__main__" and (RES_ENV == "production" or DEV_OVERRIDE):

    if TARGET_JOB_ID_OVERRIDE != "none":

        job_id = TARGET_JOB_ID_OVERRIDE

    else:

        job_id = "172873"  # Daily production run

    surfaced_models = get_dbt_job_surfaced_models(
        service_token=get_secret("ARGO-DBT-CLOUD-METADATA-SERVICE-TOKEN").get(
            "api_key"
        ),
        url="https://metadata.cloud.getdbt.com/graphql",
        job_id=job_id,
    )

    # Only consider override valid if it has text entered
    if TARGET_DBT_MODEL_OVERRIDE != "none":

        surfaced_models = [
            model
            for model in surfaced_models
            if model["name"].lower() == TARGET_DBT_MODEL_OVERRIDE.lower()
        ]

        if len(surfaced_models) == 0:

            e_str = "Input dbt model not found in latest full production run"
            logger.error(e_str)

            raise Exception(e_str)

    elif IS_TEST:

        # Only do 2 models for test syncs
        surfaced_models = surfaced_models[:2]

    # Add column info for each model from Snowflake Information Schema
    snowflake_creds = get_secret("DBT-CLOUD-SNOWFLAKE-CREDENTIALS")
    asyncio.run(
        get_all_models_columns(
            surfaced_models,
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
        )
    )

    # Construct Looker files and add to Github
    write_looker_files(
        surfaced_models, get_secret("DBT-CLOUD-DBT-TO-LOOKER-TOKEN")["token"]
    )
    logger.info(f"dbt to Looker sync for {RES_ENV} environment complete")
