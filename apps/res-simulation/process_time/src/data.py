import numpy as np
import pandas as pd

import os

from tenacity import retry, wait_fixed, stop_after_attempt
from functools import lru_cache
from pyairtable import Table
import json

from res.utils import logger
from res.utils.airtable import format_filter_conditions
from res.utils.dataframes import dataframe_from_airtable
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient

RES_ENV = os.getenv("RES_ENV", "DEVELOPMENT")

MAKE_ONE_PRODUCTION_BASE_ID = "appH5S4hIuz99tAjm"

PRODUCTION_REQUEST_COLUMNS = {
    "Factory Request Name": "factory_request_name",
    "Order Number v3": "one_code",
    "CreatedTime": "created_at",
    "DxA Exit Date": "exit_dxa_at",
    "Material Date": "exit_materials_at",
    "Fabric Print Date": "exit_print_at",
    "CUT Date": "exit_cut_at",
    "Prep Exit Date": "exit_prep_at",
    "Assembly Exit Date": "exit_assembly_at",
    "Style Code": "style_code",
    "Material Code": "material_code",
    "SKU": "sku",
    "Body Category": "body_category",
}

HISTORICAL_DATA_COLUMNS = {
    "make_one_production_request.order_number_v2": "one_code",
    "make_one_production_request.created_time": "created_at",
    "make_one_production_request.dx_a_exit_time": "exit_dxa_at",
    "make_one_production_request.material_exit_time": "exit_materials_at",
    "make_one_production_request.fabric_print_time": "exit_print_at",
    "make_one_production_request.__exit_cut_ts_time": "exit_cut_at",
    "make_one_production_request.prep_exit_time": "exit_prep_at",
    "make_one_production_request.assembly_exit_ts_time": "exit_assembly_at",
    "make_one_production_request.sku": "sku",
    "make_one_production_request.material_code": "material_code",
    "make_one_production_request.style_code": "style_code",
    "make_one_production_request.body_code": "body_code",
    "make_one_production_request.body_category": "body_category",
}

# Querying all the completed ONEs in the last 6 months
GET_HISTORICAL_DATA_QUERY = """
SELECT
    make_one_production_request."Order Number V2"  AS "make_one_production_request.order_number_v2",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."CreatedTime"::datetime  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.created_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."DxA Exit Date"::datetime  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.dx_a_exit_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."Material Date"  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.material_exit_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."Fabric Print Date"  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.fabric_print_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."__cut_exit_at"  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.__exit_cut_ts_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."Prep Exit Date"  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.prep_exit_time",
        (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(make_one_production_request."Assembly Exit Date"  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AS "make_one_production_request.assembly_exit_ts_time",
    make_one_production_request."SKU"  AS "make_one_production_request.sku",
    make_one_production_request."Material Code"  AS "make_one_production_request.material_code",
    make_one_production_request."Style Code"  AS "make_one_production_request.style_code",
    make_one_production_request."Body Code"  AS "make_one_production_request.body_code",
    make_one_production_request."Body Category"  AS "make_one_production_request.body_category"
FROM
    IAMCURIOUS_{env}.AIRTABLE__MAKE_ONE_PRODUCTION__PRODUCTION_REQUEST AS make_one_production_request
WHERE ((make_one_production_request."__brandcode" ) <> 'TT' OR (make_one_production_request."__brandcode" ) IS NULL) AND ((((( make_one_production_request."__cut_exit_at"  )) IS NOT NULL)) AND (((( make_one_production_request."Assembly Exit Date"  )) IS NOT NULL))) AND
 (((( make_one_production_request."CreatedTime"::datetime  ) >= ((CONVERT_TIMEZONE('America/Santo_Domingo', 'America/New_York', CAST(DATEADD('month', -5, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ))))) AS TIMESTAMP_NTZ)))) AND 
 ( make_one_production_request."CreatedTime"::datetime  ) < ((CONVERT_TIMEZONE('America/Santo_Domingo', 'America/New_York', CAST(DATEADD('month', 6, DATEADD('month', -5, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('America/New_York', 'America/Santo_Domingo', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)))))) AS TIMESTAMP_NTZ)))))) AND 
 ((((( make_one_production_request."Fabric Print Date"  )) IS NOT NULL)) AND (((( make_one_production_request."Prep Exit Date"  )) IS NOT NULL))))
ORDER BY
    2 DESC
"""


def process_input_data(event, context):
    current_make_queue = get_current_make_queue()
    historical_data = get_historical_data()
    return current_make_queue, historical_data


def construct_simulation_settings(event):
    return {}


def construct_output_messages():
    return []


@lru_cache(maxsize=1)
def cnx_production_request():
    return Table(
        os.environ.get("AIRTABLE_API_KEY"),
        MAKE_ONE_PRODUCTION_BASE_ID,
        "tblptyuWfGEUJWwKk",
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    response = getattr(cnx, method)(*args, **kwargs)
    logger.debug(json.dumps(response))
    return response


def get_current_make_queue():
    filter_formula = format_filter_conditions(
        [
            "{__brandcode}!='TT'",
            "{Assembly Exit Date}=BLANK()",
            "{RequestType}='Finished Goods'",
            "{Style Code}!=''",
            "{Material Code}!=''",
            "{SKU}!=''",
            "{Body Category}!=''",
        ]
    )
    logger.info(filter_formula)

    logger.info("fetching open production requests")

    current_make_queue = (
        dataframe_from_airtable(
            call_airtable(
                cnx_production_request(),
                formula=filter_formula,
                fields=PRODUCTION_REQUEST_COLUMNS.keys(),
            )
        )
        .rename(
            columns={**{"id": "production_request_id"}, **PRODUCTION_REQUEST_COLUMNS}
        )
        .fillna({"emphasis": 1.0})
    )

    logger.info(f"done. found {len(current_make_queue)} requests")
    logger.debug(current_make_queue)

    return current_make_queue


# Getting last 6 months
def get_historical_data():
    snow_client = ResSnowflakeClient(schema=f"IAMCURIOUS_{RES_ENV}")

    # Printing the query
    logger.debug(
        GET_HISTORICAL_DATA_QUERY.format(
            env=RES_ENV,
        )
    )

    #
    ones_df = snow_client.execute(
        GET_HISTORICAL_DATA_QUERY.format(
            env=RES_ENV,
        ),
        return_type="pandas",
    ).rename(columns={**HISTORICAL_DATA_COLUMNS})

    # Parsing string to Datetime
    cols = ones_df.columns[1:8]
    ones_df[cols] = ones_df[cols].apply(pd.to_datetime)

    # Calculate minutes in spent in node
    ones_df["minutes_in_dxa"] = (
        ones_df["exit_dxa_at"] - ones_df["created_at"]
    ) / np.timedelta64(1, "m")
    ones_df["minutes_in_materials"] = (
        ones_df["exit_materials_at"] - ones_df["exit_dxa_at"]
    ) / np.timedelta64(1, "m")
    ones_df["minutes_in_print"] = (
        ones_df["exit_print_at"] - ones_df["exit_materials_at"]
    ) / np.timedelta64(1, "m")
    ones_df["minutes_in_cut"] = (
        ones_df["exit_cut_at"] - ones_df["exit_print_at"]
    ) / np.timedelta64(1, "m")
    ones_df["minutes_in_prep"] = (
        ones_df["exit_prep_at"] - ones_df["exit_cut_at"]
    ) / np.timedelta64(1, "m")
    ones_df["minutes_in_assembly_exit"] = (
        ones_df["exit_assembly_at"] - ones_df["exit_prep_at"]
    ) / np.timedelta64(1, "m")

    ## Removing outliers
    ones_df = ones_df[ones_df["minutes_in_cut"] > 0]
    ones_df = ones_df[ones_df["minutes_in_prep"] > 0]
    ones_df = ones_df[ones_df["minutes_in_print"] > 0]
    ones_df = ones_df[ones_df["minutes_in_assembly_exit"] > 0]

    return ones_df

    # if __name__ == "__main__":

    print(process_input_data({}, {}))
