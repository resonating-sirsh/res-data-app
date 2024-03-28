from datetime import datetime
from botocore.exceptions import ClientError
from res.utils import logger
from snowflake.connector.errors import DatabaseError, ProgrammingError
import aiohttp
import asyncio
import base64
import boto3
import json
import pandas as pd
import snowflake.connector
import sys


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
    schema,
    table,
    snowflake_user,
    snowflake_password,
    snowflake_account,
    timestamp_field="updated_at",
    is_date=False,
):

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
            return None

        else:

            return None

    # Fetch latest date from table
    schema = schema.upper()
    table = table.upper()

    try:

        timestamp_format_str = "%Y-%m-%d" if is_date else "%Y-%m-%dT%H:%M:%S.%fZ"

        latest_ts = (
            base_connection.cursor()
            .execute(f'select max({timestamp_field}) from "RAW"."{schema}"."{table}"')
            .fetchone()[0]
        ).strftime(timestamp_format_str)

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


async def get_shopify_data(
    shopify_access_token: str,
    shop_domain: str,
    api_endpoint: str,
    client_session,
    output_list,
    additional_params: dict = None,
    api_response_override_name: str = None,
    pagination_type: str = None,
    since_ts=None,
    since_target_field: str = None,
):

    logger.info(f"Beginning shop config API call for shop domain {shop_domain}")

    # Set constant request components
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}

    while True:

        # Shop name and id API request
        async with client_session.get(
            url=f"https://{shop_domain}.myshopify.com/admin/api/2024-01/shop.json?fields=id,name,currency",
            headers=headers,
        ) as response:

            try:

                response.raise_for_status()
                shop_config_data = (await response.json(encoding="UTF-8")).get("shop")
                shop_id = shop_config_data.get("id")
                shop_name = shop_config_data.get("name")
                shop_currency = shop_config_data.get("currency")

                break

            except:

                logger.warning(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

                if response.status == 429:

                    wait_time = int(response.headers.get("Retry-After"))
                    logger.info(
                        f"Rate limit reached; retrying after {wait_time} seconds"
                    )
                    await asyncio.sleep(wait_time)

                    continue

                else:

                    # End function for shop if API call fails after wait with non
                    # too many requests response code
                    return

    # Set up request url and params
    url = f"https://{shop_domain}.myshopify.com/admin/api/2024-01/{api_endpoint}.json"
    params = (
        {"limit": 250, f"{since_target_field}_min": since_ts}
        if since_ts
        else {"limit": 250}
    )

    # If additional params were provided add them to the params dict
    if additional_params is not None:

        params.update(additional_params)

    logger.info(f"Beginning initial API call for shop domain {shop_domain}")

    # Target endpoint first API call
    while True:

        async with client_session.get(
            url=url, headers=headers, params=params
        ) as response:

            try:

                response.raise_for_status()

                if api_response_override_name:
                    data = (await response.json(encoding="UTF-8")).get(
                        f"{api_response_override_name}"
                    )

                else:

                    data = (await response.json(encoding="UTF-8")).get(
                        f"{api_endpoint}"
                    )

            except Exception as e:

                if response.status == 429:

                    wait_time = int(response.headers).get("Retry-After")
                    logger.info(
                        f"Rate limit reached; retrying after {wait_time} seconds"
                    )
                    await asyncio.sleep(wait_time)

                    continue

                else:

                    logger.error(e)

                    raise

            # Save links for endpoints using link pagination
            if pagination_type == "next_link":

                try:

                    links = response.links
                    next_url = links.get("next", {}).get("url")

                except:  # Sometimes there are no links (i.e. no results)

                    next_url = None

            break

    # Shopify uses two types of pagination. Some endpoints provide 'next' links
    # in their response body for pagination. Others make use of a since ID
    # system.

    if pagination_type is None:

        raise Exception(
            "Function requires explicit declaration of endpoint pagination type"
        )

    # Some endpoints don't have enough records to support pagination. This is
    # distinct from a nonetype object
    elif pagination_type == "none":

        pass

    elif pagination_type == "next_link":

        if next_url:

            logger.info(f"Beginning paginated API calls for shop domain {shop_domain}")

            # Loop through additional pages if they exist
            while next_url is not None:

                while True:

                    async with client_session.get(
                        url=next_url, headers=headers
                    ) as response:

                        try:

                            if api_response_override_name:

                                response.raise_for_status()
                                page_data = (await response.json(encoding="UTF-8")).get(
                                    f"{api_response_override_name}"
                                )
                                data.extend(page_data)
                                links = response.links
                                next_url = links.get("next", {}).get("url")

                                break

                            else:

                                response.raise_for_status()
                                page_data = (await response.json(encoding="UTF-8")).get(
                                    f"{api_endpoint}"
                                )
                                data.extend(page_data)
                                links = response.links
                                next_url = links.get("next", {}).get("url")

                                break

                        except Exception as e:

                            if response.status == 429:

                                wait_time = int(response.headers).get("Retry-After")
                                logger.info(
                                    f"Rate limit reached; retrying after {wait_time} seconds"
                                )
                                await asyncio.sleep(wait_time)

                                continue

                            else:

                                logger.error(e)

                                raise

    elif pagination_type == "since_id":

        # This option uses since IDs to move through a list of records. A
        # response with less than 250 checkouts signifies the final 'page' of
        # records. Loop using the final id as a since_id until a response has
        # fewer than 250 checkouts returned

        record_count = len(data)

        if record_count == 250:

            final_record = data[len(data) - 1]["id"]

            logger.info(f"Beginning paginated API calls for shop domain {shop_domain}")

            # Loop through additional pages if they exist
            while record_count == 250:

                # Perform initial API call
                while True:

                    params = {"limit": 250, "since_id": final_record}

                    async with client_session.get(
                        url=url, headers=headers, params=params
                    ) as response:

                        try:

                            response.raise_for_status()

                            if api_response_override_name:

                                page_data = (await response.json(encoding="UTF-8")).get(
                                    f"{api_response_override_name}"
                                )

                            else:

                                page_data = (await response.json(encoding="UTF-8")).get(
                                    f"{api_endpoint}"
                                )

                            data.extend(page_data)
                            record_count = len(page_data)
                            final_record = page_data[len(page_data) - 1]["id"]

                            break

                        except:

                            if response.status == 200:

                                raise Exception("200 OK; Error parsing API response")

                            if response.status == 429:

                                wait_time = int(response.headers).get("Retry-After")
                                logger.info(
                                    f"Rate limit reached; retrying after {wait_time} seconds"
                                )
                                await asyncio.sleep(wait_time)

                                continue

                            else:

                                logger.warning(
                                    f"{response.status} {response.reason} for Shop domain {shop_domain} during paginated API call"
                                )

                                # Break inner loop and set record count to 0.
                                # Continue with function and use data from
                                # initial API call

                                # Break inner loop and set record count to 0
                                record_count = 0

                                break

    # Add shop details to data list
    for dict in data:

        dict["shop_id"] = f"{shop_id}"
        dict["shop_name"] = f"{shop_name}"
        dict["shop_currency"] = f"{shop_currency}"

    if len(data) > 0:

        # Add the shop's data to the collection of all responses
        output_list.extend(data)


# Create async function to wrap retrieval function in connection
async def fetch_all_data(
    api_endpoint,
    output_list,
    shopify_keys,
    sync_type,
    additional_params=None,
    api_response_override_name=None,
    pagination_type=None,
    since_target_field="updated_at",
    since_ts=None,
):

    async with aiohttp.ClientSession() as client_session:

        if sync_type == "full":

            logger.info(f"Full sync for Shopify {api_endpoint} endpoint")

            futures = [
                get_shopify_data(
                    shopify_access_token=shop["shop_app_api_key"],
                    shop_domain=shop["shop_domain_name"],
                    api_endpoint=api_endpoint,
                    client_session=client_session,
                    output_list=output_list,
                    additional_params=additional_params,
                    api_response_override_name=api_response_override_name,
                    pagination_type=pagination_type,
                )
                for shop in shopify_keys
            ]

        elif sync_type == "incremental":

            logger.info(f"Incremental sync for Shopify {api_endpoint} endpoint")

            if since_ts:

                futures = [
                    get_shopify_data(
                        shopify_access_token=shop["shop_app_api_key"],
                        shop_domain=shop["shop_domain_name"],
                        api_endpoint=api_endpoint,
                        client_session=client_session,
                        output_list=output_list,
                        additional_params=additional_params,
                        api_response_override_name=api_response_override_name,
                        pagination_type=pagination_type,
                        since_ts=since_ts,
                        since_target_field=since_target_field,
                    )
                    for shop in shopify_keys
                ]

            else:

                logger.warning(
                    f"Error retrieving timestamp; defaulting to full sync for Shopify {api_endpoint} endpoint"
                )

                futures = [
                    get_shopify_data(
                        shopify_access_token=shop["shop_app_api_key"],
                        shop_domain=shop["shop_domain_name"],
                        api_endpoint=api_endpoint,
                        client_session=client_session,
                        output_list=output_list,
                        additional_params=additional_params,
                        api_response_override_name=api_response_override_name,
                        pagination_type=pagination_type,
                    )
                    for shop in shopify_keys
                ]

        elif sync_type == "test":

            logger.info(f"Test sync for Shopify {api_endpoint} endpoint")

            futures = [
                get_shopify_data(
                    shopify_access_token=shop["shop_app_api_key"],
                    shop_domain=shop["shop_domain_name"],
                    api_endpoint=api_endpoint,
                    client_session=client_session,
                    output_list=output_list,
                    additional_params=additional_params,
                    api_response_override_name=api_response_override_name,
                    pagination_type=pagination_type,
                    since_ts=since_ts,
                    since_target_field=since_target_field,
                )
                for shop in shopify_keys
            ]

        else:

            logger.info(
                f"No sync_type; Defaulting to full sync for Shopify {api_endpoint} endpoint"
            )

            futures = [
                get_shopify_data(
                    shopify_access_token=shop["shop_app_api_key"],
                    shop_domain=shop["shop_domain_name"],
                    api_endpoint=api_endpoint,
                    client_session=client_session,
                    output_list=output_list,
                    additional_params=additional_params,
                    api_response_override_name=api_response_override_name,
                    pagination_type=pagination_type,
                )
                for shop in shopify_keys
            ]

        await asyncio.gather(*futures)


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


def traverse_nested_dict(target_dict, key_list, replace_none=False):

    # A method for traversing a nested dictionary. This is different than the
    # standard get because get does not replace falsy values with provided
    # defaults. For example, if the first level of a nested dict has a value of
    # None for the key provided, subsequent get calls will return an attribute
    # error

    length = len(key_list)
    depth = 0

    while length != depth:

        item = target_dict.get(key_list[depth])

        target_dict = item or {}

        depth += 1

    if replace_none == True and item is None:

        item = {}

    return item


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


async def get_bulk_query_response(
    shopify_access_token: str,
    shop_domain: str,
    client_session,
    body: str,
    since_str: str,
    until_str: str,
    output_data: list,
):

    # Set constant request components
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}

    while True:

        # Shop name and id API request
        async with client_session.get(
            url=f"https://{shop_domain}.myshopify.com/admin/api/2023-04/shop.json?fields=id,name",
            headers=headers,
        ) as response:

            try:

                response.raise_for_status()
                shop_config_data = (await response.json(encoding="UTF-8")).get("shop")
                shop_id = shop_config_data.get("id")
                shop_name = shop_config_data.get("name")

                break

            except:

                logger.warning(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

                if response.status == 429:

                    wait_time = int(response.headers.get("Retry-After"))
                    logger.info(
                        f"Rate limit reached; retrying after {wait_time} seconds"
                    )
                    await asyncio.sleep(wait_time)

                    continue

                else:

                    # End function for shop if API call fails after wait with non
                    # too many requests response code
                    return

    # Set up url, parameters, and JSON
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}
    url = f"https://{shop_domain}.myshopify.com/admin/api/2024-01/graphql.json"

    # Send POST request with query
    async with client_session.post(
        url=url, headers=headers, json={"query": body}
    ) as response:

        try:

            response.raise_for_status()
            data = await response.json(encoding="UTF-8")

            # The GraphQL API can return a 200 OK response code in cases
            # that would typically produce 4xx or 5xx errors in REST
            errors = (
                data.get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors")
            ) or []

            if response.status == 200 and len(errors) > 0:

                sys.exit(f"GraphQL Errors: {errors}")

        except:

            raise Exception(
                f"{response.status} {response.reason} for Shop domain {shop_domain}"
            )

    # Shopify allows for bulk operations when querying connection field that's
    # defined by the GraphQL Admin API schema. Shopify handles the query
    # execution and provides a URL where the data can be downloaded after
    # execution is complete. The app must check the link for query status. The
    # alternative is subscribing to a webhook but per Shopify those webhooks
    # sometimes trigger before execution is complete. Results are delivered in
    # a JSONL file.
    poll_query = """
        query {
            currentBulkOperation {
                id
                status
                errorCode
                createdAt
                completedAt
                objectCount
                fileSize
                url
                partialDataUrl
            }
        }    
    """

    # Wait before polling
    await asyncio.sleep(5)
    synced_ts = datetime.now()

    while True:

        async with client_session.post(
            url=url, headers=headers, json={"query": poll_query}
        ) as poll_response:

            try:

                poll_response.raise_for_status()
                poll_data = await poll_response.json(encoding="UTF-8")
                operation_status = (
                    poll_data.get("data", {})
                    .get("currentBulkOperation", {})
                    .get("status", "")
                ).lower()

                if operation_status in ["running", "created"]:

                    logger.info(
                        f"Awaiting bulk operation completion for domain {shop_domain}"
                    )
                    await asyncio.sleep(5)

                    continue

                if operation_status == "completed":

                    operation_created_at = poll_data["data"]["currentBulkOperation"][
                        "createdAt"
                    ]
                    output_url = poll_data["data"]["currentBulkOperation"]["url"]

                    break

                if operation_status == "failed":

                    error_code = poll_data.get("currentBulkOperation", {}).get(
                        "errorCode", []
                    )

                    raise Exception(
                        f"Error during bulk query execution; Code {error_code}"
                    )

                else:

                    raise Exception(
                        f"Error during bulk operation; bulk operation {operation_status}"
                    )

            except:

                raise Exception(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

    if output_url is None:

        logger.info(f"No output for {shop_name} from {since_str} to {until_str}")

    else:

        logger.info(f"Retrieving data for {shop_name} from {since_str} to {until_str}")

        async with client_session.get(url=output_url) as response:

            response.raise_for_status()

            async for row in response.content:

                if not row:

                    continue

                json_row = json.loads(row)
                row_dict = json_row | {
                    "shop_id": shop_id,
                    "shop_name": shop_name,
                    "synced_at": synced_ts,
                }
                output_data.append(row_dict)


# Async query wrapper
async def get_bulk_query_responses(
    shopify_keys,
    body: str,
    since_str: str,
    until_str: str,
):
    """
    Asynchronous function to retries the results of multiple Shopify bulk
    query operations

    """

    output_data = []
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as client_session:

        futures = [
            get_bulk_query_response(
                shopify_access_token=shop["shop_app_api_key"],
                shop_domain=shop["shop_domain_name"],
                client_session=client_session,
                body=body,
                since_str=since_str,
                until_str=until_str,
                output_data=output_data,
            )
            for shop in shopify_keys
        ]

        await asyncio.gather(*futures)

    return output_data
