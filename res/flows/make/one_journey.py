import requests, os, json, arrow, time
from geopy import Nominatim, distance
from res.utils import secrets_client
import snowflake.connector
import res
import pandas as pd
from res.flows.infra.platform.main_jobs import (
    reload_pretreatment_data,
    refresh_assets_on_rolls_cache,
)

GRAPH_API = os.getenv("GRAPH_API", "https://api.resmagic.io/graphql")
GRAPH_API_KEY = secrets_client.get_secret("GRAPH_API_KEY")
GRAPH_API_HEADERS = {
    "x-api-key": GRAPH_API_KEY,
    "apollographql-client-name": "lambda",
    "apollographql-client-version": "test",
}

GET_ONE = """
    query getOne($id: ID!){
        one(id: $id){
            id
            oneId
            orderLineItemId
        }
    }
"""

CREATE_ONE_MUTATION = """
    mutation createOne($input: CreateOneInput){
        createOne(input: $input){
            one{
                id
            }
        }
    }
"""

UPDATE_ONE_MUTATION = """
    mutation updateOne($id: ID!, $input: UpdateOneInput){
        updateOne(id: $id, input: $input){
            one{
                id
            }
        }
    }
"""

GET_LINE_ITEM = """
    query getLineItem($id: ID!){
        orderLineItem(id: $id){
            order{
                id
            }
            makeOneProductionRequest{
                orderNumber
            }
            style{
                name
                id
                coverImages{
                    url
                    name
                }
            }
        }
    }
"""

GET_PRODUCTION_REQUEST = """
   query getMakeOneProductionRequest($where: MakeOneProductionRequestsWhere) {
    makeOneProductionRequests(
        first: 1
        where: $where 
    ) {
        makeOneProductionRequests {
        id
        orderNumber
        makeOneFlowJson
        assemblyFlowJson
        printAssetsRequests {
            id
            nestedKey
            assetsInNestedSetCount
        }
        orderLineItem {
            id
            salesChannel
            order {
            code
            channelOrderId
            shippingCity
            shippingCountry
            shippingProvince
            }
        }
        }
    }
    }
"""

GET_EWASIF_CONSUMPTION = """
    query getEwasifConsumption($where: EwasifConsumptionsWhere){
        ewasifConsumptions(first: 100, where: $where){
            ewasifConsumptions{
                id
                electricityConsumption
                waterConsumption
                createdAt
            }
        }
    }
"""

GET_TOTAL_ONES_ON_DATE = """
    query getTotalOnesOnDate($where: MakeOneProductionRequestsWhere){
        makeOneProductionRequests(first:1, where: $where){
            makeOneProductionRequests{
                id
            }
            count
        }
    }
"""

GET_NODES = """
    query getNodes($where: FlowerStatesWhere){
        flowerStates(first: 100, where: $where){
            flowerStates{
                name
                flowName
                dxaNode{
                    name
                    code
                }
                flow{
                    code
                    jsonModelFlowName
                    recordIdentifierFieldName
                    modelName
                }
                estimatedElectricityUsage
                estimatedWaterUsage
                slaDuration
            }
        }
    }
"""

GET_ONE_CONSUMPTION_ESTIMATES = """
    query getOneEstimateConsumption($where: OneConsumptionEstimatesWhere){
        oneConsumptionEstimates(first:10, where: $where){
            oneConsumptionEstimates{
                consumptionByOne
            }
        }
    }
"""

GET_MATERIALS_VENDORS_LOCATION = """
    query materials_origin($code: String){
        material(code: $code){
            vendor{
                cityAndCountry
            }
        }
    }
"""

GET_ROLL_SHIPPING_METHOD = """
    query roll_shipping_method($where: PurchasingItemWhere){
        purchasingItems(first: 1, where: $where){
            purchasingItems{
                shippingMethod
            }
        }
    }
"""

FLOWS = [
    "Make ONE - Finished Goods",
    "Assembly Flow - Finished Goods",
    "Print - Prepare Rolls",
    "Print - Process Rolls",
]

ELECTRICITY_KEY = "Electricity - Electricidad"
WATER_KEY = "Water - Agua"


def get_snowflake_connector():
    snowflake_connection = snowflake.connector.connect(
        account="JK18804",
        user=secrets_client.get_secret("SNOWFLAKE_USER"),
        password=secrets_client.get_secret("SNOWFLAKE_PASSWORD"),
        role="ACCOUNTADMIN",
        region="us-east-1",
    )
    return snowflake_connection


def graph_request(query, variables):
    response = requests.post(
        GRAPH_API,
        json={"query": query, "variables": variables},
        headers=GRAPH_API_HEADERS,
    )
    body = response.json()
    errors = body.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return body["data"]


def get_one_number(event):
    """
    Event should contain an order item id and a trim one number

    """
    line_item_id = event["record_id"]
    line_item = graph_request(GET_LINE_ITEM, {"id": line_item_id})["orderLineItem"]
    one_number = line_item["makeOneProductionRequest"]["orderNumber"]
    return {
        # sa: adding need to pair a trim number with the ordered one - for back compat defaulting to existing one but be careful!!
        "label_one_number": event.get("label_one_number", one_number),
        "one_number": one_number,
        "line_item_id": line_item_id,
    }


def get_id(oid):
    tab = res.connectors.load("airtable")["appgl4UPGwnsI0DLW"]["tblvlNxPTcHFnOlJB"]
    mapping = tab.to_dataframe(
        fields=["oneId", "Associated One Number"],
        filters=tab._key_lookup_predicate(oid),
    )
    if len(mapping):
        mapping = dict(mapping[["oneId", "record_id"]].values)
        return mapping.get(oid)


# retries
def write_event_to_airtable(event):
    """
    for example
    event = {
       "label_one_number": '22222222',
       "line_item_id": '123342345',
        "one_number": '22222222',
    }
    """

    tab = res.connectors.load("airtable")["appgl4UPGwnsI0DLW"]["tblvlNxPTcHFnOlJB"]
    oneid = event.get("label_one_number")
    payload = {
        "oneId": oneid,
        "__orderLineItemId": event.get("line_item_id"),
        "Associated One Number": event.get("one_number"),
    }

    record_id = get_id(oneid)
    if record_id:
        payload["record_id"] = record_id

    res.utils.logger.info(f"Logging event to airtable {payload}")

    tab.update_record(payload)

    return None


def generate_one(event):
    """
    event contains lineitemid which maps to one, and a label_one_number for association
    """
    res.utils.logger.info(f"Generating ONE Record for {event}")

    line_item_id = event["line_item_id"]
    label_one_number = event["label_one_number"]

    line_item = graph_request(GET_LINE_ITEM, {"id": line_item_id})["orderLineItem"]

    cover_iamges = []

    for image in line_item["style"]["coverImages"]:
        cover_iamges.append({"url": image["url"], "filename": image["name"]})
    payload = {
        "orderLineItemId": line_item_id,
        "orderId": line_item["order"]["id"],
        # SA: now we need a one number from the trim as an id + the actual one
        "oneId": label_one_number,
        # TODO: add the one number to the table too - update the model https://github.com/resonance/create-one-api/tree/master/graph-api/source/graphql/models/one
        # "oneNumber": one_number,
        ##
        "styleId": line_item["style"]["id"],
        "name": line_item["style"]["name"],
        "cover": cover_iamges,
    }

    one_id = get_id(event.get("label_one_number"))
    # create if we have not already
    if not one_id:
        one = graph_request(CREATE_ONE_MUTATION, {"input": payload})["createOne"]["one"]
        one_id = one["id"]

        res.utils.logger.info(f"response {one}")

    return {
        # the one id is mapped to the trim and saved - this response gives the airtable handle
        "one_record_id": one_id,
        #
        "line_item_id": event["line_item_id"],
        # the one number is passed in by prev method and resolved from line item id
        "one_number": event.get("one_number"),
    }


def carbon_footprint_from_route(origin, destination):
    estimatedConsumption = graph_request(
        GET_ONE_CONSUMPTION_ESTIMATES,
        {
            "where": {
                "origin": {"is": origin},
                "destination": {"is": destination},
                "unit": {"is": "Kg"},  # Kg for carbon footprint
            }
        },
    )["oneConsumptionEstimates"]["oneConsumptionEstimates"]

    return estimatedConsumption[0]["consumptionByOne"]


def get_consumption(from_date, to_date, node, flow_id, ewasif_type):
    cursor = get_snowflake_connector().cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")
    print(
        f"""SELECT * FROM "flower_events" WHERE "dxaNodeId" = {node["dxaNode"]["code"]} and "flowId" = '{flow_id}'"""
    )

    from_date = arrow.get(from_date)
    to_date = arrow.get(to_date)
    one_consumption = 0

    days_in_between = (to_date - from_date).days
    include_one_back_to = node["slaDuration"] / 1140
    i = 0
    ones_count = 0
    while i < days_in_between:
        time.sleep(0.5)
        if i != days_in_between and days_in_between > 0:
            end_at = from_date.replace(hour=23, minute=59, second=00)
            start_at = (
                from_date if i == 0 else from_date.replace(hour=1, minute=00, second=00)
            )
            start_at = start_at.shift(days=+i)
            end_at = end_at.shift(days=+1)
            count_from_date = from_date.shift(days=-include_one_back_to)

        elif i == 0:
            end_at = to_date
            start_at = from_date
            count_from_date = from_date.shift(days=-include_one_back_to)

        else:
            # Last day
            end_at = to_date
            start_at = from_date.shift(days=+days_in_between)
            count_from_date = from_date.shift(days=-include_one_back_to)

        cursor.execute(
            f"""
                select one_number, entered_at, exited_at, enter_state, exit_state from (
                select "oneNumber" as one_number, "eventStartDate" as entered_at,"currentState" as enter_state,
                LAG("eventStartDate") over (partition by "oneNumber" order by "oneNumber") exited_at,
                LAG("currentState") over (partition by "oneNumber" order by "oneNumber") exit_state
                from "flower_events"
                where "dxaNodeId" = {node["dxaNode"]["code"]} and "flowId" = '{flow_id}'
                order by "flowTransactionType"
            ) where enter_state <> 'Cancelled' and ((exited_at < '{end_at.isoformat()}' and exited_at > '{start_at.isoformat()}') or (entered_at > '{count_from_date.isoformat()}' and exited_at is null))   
        """
        )

        ones_count += cursor.rowcount
        if ewasif_type == ELECTRICITY_KEY:
            electricity_consumption = get_node_ewasif_consumption(
                start_at, end_at, node["estimatedElectricityUsage"], ewasif_type
            )

            one_consumption += electricity_consumption / ones_count
        elif ewasif_type == WATER_KEY:
            water_consumption = get_node_ewasif_consumption(
                start_at, end_at, node["estimatedWaterUsage"], ewasif_type
            )
            one_consumption += water_consumption / ones_count
        i += 1

    # close snow cursor
    cursor.close()

    return one_consumption


def get_node_ewasif_consumption(from_date, to_date, estimatedUsage, ewasif_type):
    estimatedUsage = estimatedUsage if estimatedUsage else 0
    consumption = 0
    ewasif_consumptions_response = graph_request(
        GET_EWASIF_CONSUMPTION,
        {
            "where": {
                "type": {"is": ewasif_type},
                "and": [
                    {
                        "createdAt": {
                            "isGreaterThan": from_date.replace(
                                hour=1, minute=00, second=00
                            ).isoformat()
                        }
                    },
                    {
                        "createdAt": {
                            "isLessThan": to_date.replace(
                                hour=23, minute=59, second=59
                            ).isoformat()
                        }
                    },
                ],
            }
        },
    )
    ewasif_consumptions = ewasif_consumptions_response["ewasifConsumptions"][
        "ewasifConsumptions"
    ]

    for consumption_record in ewasif_consumptions:
        if ewasif_type == ELECTRICITY_KEY:
            consumption += (
                consumption_record["electricityConsumption"]
                if consumption_record["electricityConsumption"]
                else 0
            )
        else:
            consumption += (
                consumption_record["waterConsumption"]
                if consumption_record["waterConsumption"]
                else 0
            )
    consumption = consumption * estimatedUsage
    return consumption


def get_one_node_information(one_code, dxa_node_id, flow_id):
    cursor = get_snowflake_connector().cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")
    print(
        f"""        SELECT * FROM "flower_events" WHERE "oneNumber" like '%{one_code}% and "dxaNodeId" = {dxa_node_id} and "flowId" = '{flow_id}'"""
    )
    cursor.execute(
        f"""
        SELECT * FROM "flower_events" WHERE "oneNumber" like '%{one_code}%' and "dxaNodeId" = {dxa_node_id} and "flowId" = '{flow_id}' order by "flowTransactionType"
    """
    )

    result = cursor.fetchall()

    # close snow cursor
    cursor.close()

    return result


def get_electricity_water_consumption(event):
    res.utils.logger.info(f"Calculating electricity consumption {event}")

    record_id = event["one_record_id"]
    one_number = event["one_number"]

    electricity_consumption = 0
    electricity_consumption_by_node = []
    water_consumption_by_node = []

    for flow_name in FLOWS:
        print("Working with flow: ", flow_name)
        nodes = graph_request(
            GET_NODES,
            {"where": {"isActive": {"is": True}, "flowName": {"is": flow_name}}},
        )
        for node in nodes["flowerStates"]["flowerStates"]:
            time.sleep(0.5)
            print("Working with node: ", node["name"])
            events = get_one_node_information(
                one_number, node["dxaNode"]["code"], node["flow"]["code"]
            )
            if len(events) >= 2:
                enter_event = events[0]
                exit_event = events[1]

                # getting electricity Usage
                electricity_consumption = get_consumption(
                    enter_event["eventStartDate"],
                    exit_event["eventStartDate"],
                    node,
                    node["flow"]["code"],
                    ELECTRICITY_KEY,
                )
                electricity_consumption_by_node.append(
                    {
                        "nodeId": node["dxaNode"]["code"],
                        "nodeName": node["dxaNode"]["name"],
                        "amount": round(electricity_consumption, 2),
                        "unit": "kW",
                    }
                )
                print(electricity_consumption_by_node)
                time.sleep(0.5)
                # getting water usage
                water_usage = get_consumption(
                    enter_event["eventStartDate"],
                    exit_event["eventStartDate"],
                    node,
                    node["flow"]["code"],
                    WATER_KEY,
                )
                water_consumption_by_node.append(
                    {
                        "nodeId": node["dxaNode"]["code"],
                        "nodeName": node["dxaNode"]["name"],
                        "amount": round(water_usage, 2),
                        "unit": "m3",
                    }
                )

    graph_request(
        UPDATE_ONE_MUTATION,
        {
            "id": record_id,
            "input": {
                "electricityUsageBreakdown": json.dumps(
                    electricity_consumption_by_node
                ),
                "waterUsageBreakdown": json.dumps(water_consumption_by_node),
            },
        },
    )

    print("end")

    return event


def get_ink_consumption(event, plan=False):
    res.utils.logger.info(f"Getting Ink Consumption {event}")

    record_id = event["one_record_id"]
    one_number = event["one_number"]

    cursor = get_snowflake_connector().cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    ink_mapping = {
        "yellow": "YELLOW_INK_USED_TO_PRINT",
        "orange": "ORANGE_INK_USED_TO_PRINT",
        "red": "RED_INK_USED_TO_PRINT",
        "black": "BLACK_INK_USED_TO_PRINT",
        "blue": "BLUE_INK_USED_TO_PRINT",
        "magenta": "MAGENTA_INK_USED_TO_PRINT",
        "cyan": "CYAN_INK_USED_TO_PRINT",
        "transparent": "TRANSPARENT_INK_USED_TO_PRINT",
    }

    ink_consumption = {
        "yellow": 0,
        "orange": 0,
        "red": 0,
        "black": 0,
        "blue": 0,
        "magenta": 0,
        "cyan": 0,
        "transparent": 0,
    }

    total_ink_consumption = 0

    # query = f"""
    #     select  print_asset_flow."Key" as "prinnt_asset_key", print_asset_flow."Material Code", "_assets_in_nested_set", "_nested_asset_key",
    #     "__order_number", "Confirmed Utilized Roll display", "Roll ID"
    #     from "Print_Assets_flow" as print_asset_flow
    #     join "Rolls" as rolls on "Confirmed Utilized Roll display" = rolls."Name"
    #     where "__order_number" = '{one_number}' limit 1    """
    # print_assets = cursor.execute(query).fetchall()

    print_assets = refresh_assets_on_rolls_cache()

    print_assets = print_assets[print_assets["one_number"] == one_number]

    for print_asset in print_assets.to_dict("records"):
        query = f"""
            WITH cte AS 
                (
                SELECT
                    RECORD_CONTENT,
                    row_number() OVER( partition BY RECORD_CONTENT:"MSjobinfo"."Printer", RECORD_CONTENT:"MSjobinfo"."Jobid", RECORD_CONTENT:"MSjobinfo"."Status" 
                ORDER BY
                    RECORD_CONTENT:"MSjobinfo"."EndTime" DESC ) rn 
                FROM
                    "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_PREMISES_PRINTER_DATA_COLLECTOR_PRINT_JOB_DATA" 
                )
                SELECT

                CAST(RECORD_CONTENT:"MSjobinfo"."Jobid" as number) as job_id,
                CAST(RECORD_CONTENT:"MSjobinfo"."Printer" as string) as printer_name,
                CAST(RECORD_CONTENT:"MSjobinfo"."Media" as string) as media,
                replace(replace(replace(RECORD_CONTENT:"MSjobinfo"."Name", '%20',' '),'%3A',':'),'%23','#') as job_name,
                RECORD_CONTENT:"MSjobinfo"."Name" as job_name_raw,
                CAST(RECORD_CONTENT:"MSjobinfo"."PrintedLength" as string) as printed_length_value,
                CAST(RECORD_CONTENT:"MSjobinfo"."RequestedLength" as string) as requested_length_value,
                CAST(RECORD_CONTENT:"MSjobinfo"."PrintMode"as string) as print_mode,
                CAST(RECORD_CONTENT:"MSjobinfo"."StartTime" as timestamp) as started,
                CAST(RECORD_CONTENT:"MSjobinfo"."EndTime" as timestamp) as end_time,
                CAST(RECORD_CONTENT:"MSjobinfo"."Status" as string) as job_status,
                CAST(RECORD_CONTENT:"MSjobinfo"."Width" as string) as width_value,
                CAST(RECORD_CONTENT:"MSjobinfo"."Direction" as string) as direction,
                CAST(RECORD_CONTENT:"MSjobinfo"."FoldDetected" as string) as fold_detected,
                CAST(RECORD_CONTENT:"MSjobinfo"."HeadGap" as string) as head_gap_value,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Cleanings"."Normal" as string) as cyan_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Cleanings"."Soft" as string) as cyan_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Cleanings"."Strong" as string) as cyan_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Type" as string) as cyan_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Usage"."ToClean" as string) as cyan_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[0]."Usage"."ToPrint" as string) as cyan_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Cleanings"."Normal" as string) as magenta_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Cleanings"."Soft" as string) as magenta_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Cleanings"."Strong" as string) as magenta_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Type" as string) as magenta_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Usage"."ToClean" as string) as magenta_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[1]."Usage"."ToPrint" as string) as magenta_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Cleanings"."Normal" as string) as yellow_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Cleanings"."Soft" as string) as yellow_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Cleanings"."Strong" as string) as yellow_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Type" as string) as yellow_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Usage"."ToClean" as string) as yellow_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[2]."Usage"."ToPrint" as string) as yellow_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Cleanings"."Normal" as string) as black_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Cleanings"."Soft" as string) as black_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Cleanings"."Strong" as string) as black_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Type" as string) as black_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Usage"."ToClean" as string) as black_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[3]."Usage"."ToPrint" as string) as black_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Cleanings"."Normal" as string) as red_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Cleanings"."Soft" as string) as red_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Cleanings"."Strong" as string) as red_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Type" as string) as red_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Usage"."ToClean" as string) as red_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[4]."Usage"."ToPrint" as string) as red_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Cleanings"."Normal" as string) as orange_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Cleanings"."Soft" as string) as orange_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Cleanings"."Strong" as string) as orange_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Type" as string) as orange_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Usage"."ToClean" as string) as orange_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[5]."Usage"."ToPrint" as string) as orange_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Cleanings"."Normal" as string) as blue_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Cleanings"."Soft" as string) as blue_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Cleanings"."Strong" as string) as blue_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Type" as string) as blue_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Usage"."ToClean" as string) as blue_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[6]."Usage"."ToPrint" as string) as blue_ink_used_to_print,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Cleanings"."Normal" as string) as transparent_cleaning_normal,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Cleanings"."Soft" as string) as transparent_cleaning_soft,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Cleanings"."Strong" as string) as transparent_cleaning_strong,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Type" as string) as transparent_type,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Usage"."ToClean" as string) as transparent_ink_used_to_clean,
                CAST(RECORD_CONTENT:"MSjobinfo"."MSinkusage"."Ink"[7]."Usage"."ToPrint" as string) as transparent_ink_used_to_print

                FROM cte

                WHERE rn = 1 and job_name = '{print_asset["printfile_name"]}' and end_time is not null
                """

        res.utils.logger.info("hitting snowflake...")
        rows = cursor.execute(query).fetchall()

        cursor.close()

        for key, val in ink_mapping.items():
            if len(rows) > 0:
                row = rows[0]
                value = float(row[val]) if row[val] else 0
                ink_consumption[key] += value / int(print_asset["one_number_count"])
                total_ink_consumption += value / int(print_asset["one_number_count"])
    ink_consumption["total"] = total_ink_consumption
    ink_consumption["unit"] = "ml"

    if not plan:
        graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {"inkUsageBreakdown": json.dumps(ink_consumption)},
            },
        )

    res.utils.logger.info(ink_consumption)

    return event


def get_asset_pretreatment_contributions(
    one_number, drop_duplicate_material_rolls=True
):
    """
    stuff
    """
    d = refresh_assets_on_rolls_cache()
    pt = reload_pretreatment_data()
    pt = pt.explode("Rolls")
    pt = pd.merge(d, pt, left_on="roll_id", right_on="Rolls")
    if drop_duplicate_material_rolls:
        pt = pt.drop_duplicates(subset=["material_code", "one_number"])
    if one_number is not None:
        pt = pt[pt["one_number"] == one_number]

    pt = pt.fillna(0).round(2)

    return pt


def get_chemicals_consumption(event, plan=False):
    res.utils.logger.info(f"Chemicals Consumption {event}")

    record_id = event["one_record_id"]
    one_number = event["one_number"]

    cursor = get_snowflake_connector().cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    chemicals_key_mapping = {
        "prepajet": "PREPAJET_KG",
        "silkSodiumBicarbonate": "SILK_SODIUM_BICARBONATE_KG",
        "cottonSodiumBicarbonate": "COTTON_SODIUM_BICARBONATE_KG",
        "urea": "UREA_KG",
        "lyoprint": "LYOPRINT_KG",
        "albaflow": "ALBAFLOW_L",
    }

    chemicals_consumption = {
        "prepajet": 0,
        "silkSodiumBicarbonate": 0,
        "cottonSodiumBicarbonate": 0,
        "urea": 0,
        "lyoprint": 0,
        "albaflow": 0,
    }

    total_chemical_consumption = 0

    res.utils.logger.info(f"Loading contributions for one {one_number}")
    rows = get_asset_pretreatment_contributions(one_number).to_dict("records")
    res.utils.logger.info(f"Retrieve {rows}")

    for row in rows:
        row = rows[0]
        for key in chemicals_consumption.keys():
            row = rows[0]
            value = (
                round(float(row[chemicals_key_mapping[key]]), 2)
                if row[chemicals_key_mapping[key]]
                else 0
            )
            # contribution
            chemicals_consumption[key] += value / int(row["one_number_count"])
            total_chemical_consumption += value / int(row["one_number_count"])

        chemicals_consumption["total"] = round(total_chemical_consumption, 2)
        chemicals_consumption["unit"] = "kg"
    if not plan:
        graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {"chemicalsUsageBreakdown": json.dumps(chemicals_consumption)},
            },
        )
    else:
        res.utils.logger.info(chemicals_consumption)
    return event


def get_shipping_carbon_emissions(event):
    res.utils.logger.info(f"Shipping Carbon Emissions {event}")

    record_id = event["one_record_id"]
    one_number = event["one_number"]

    production_request = graph_request(
        GET_PRODUCTION_REQUEST, {"where": {"orderNumber": {"is": one_number}}}
    )["makeOneProductionRequests"]["makeOneProductionRequests"][0]

    snowflake = res.connectors.load("snowflake")
    id = production_request["orderLineItem"]["order"]["channelOrderId"]

    res.utils.logger.info(f"Looking for shipping address of order id {id}")

    try:
        if production_request["orderLineItem"]["salesChannel"][0] == "ECOM":
            # just rename what we need
            Q = f""" select   "ORDER_ID" as "id", "SHIPPING_ADDRESS_ONE",  "SHIPPING_ADDRESS_TWO",  "SHIPPING_CITY" as "city", "SHIPPING_ZIP", "SHIPPING_PROVINCE",
               "SHIPPING_COUNTRY", "SHIPPING_NAME", "SHIPPING_LATITUDE",
               "SHIPPING_LONGITUDE", "SHIPPING_COUNTRY_CODE" as "country_code", "SHIPPING_PROVINCE_CODE" as "province_code"
                from "RAW"."SHOPIFY"."ORDERS"  where "ORDER_ID" = '{id}'"""
            address = snowflake.execute(Q).to_dict("records")[0]
        else:
            address = {
                "country_code": production_request["orderLineItem"]["order"][
                    "shippingCountry"
                ],
                "province_code": production_request["orderLineItem"]["order"][
                    "shippingProvince"
                ],
                "city": production_request["orderLineItem"]["order"]["shippingCity"],
            }

        d = distance.distance
        g = Nominatim(user_agent="one_journey")

        country_code = address["country_code"]

        # triptocarbon api only supports us and uk, the rest needs to be sent as def.
        if country_code != "US" and country_code != "UK":
            country_code = "def"
            fuel_type = "aviationGasoline"
            transportation_mode = "anyFlight"
            # Assuming for now that all ones are shipped from DR - Needs to be changed
            _, city_origin = g.geocode("Santo Domingo, Dominican Republic")

        else:
            fuel_type = "motorGasoline"
            country_code = "usa" if country_code == "US" else "gbr"
            transportation_mode = "anyCar"
            _, city_origin = g.geocode("Miami, FL")

        _, to_customer = g.geocode(f"{address['city']}, {address['province_code']}")

        distances_miles = (d(city_origin, to_customer)).miles

        transport_carbon_footprint_response = 0

        # If the destination is Miami (were we assumed the ground shipment begins) then it would not calculate the carbon emission
        if distances_miles > 0:
            transport_carbon_footprint_response = requests.get(
                f"https://api.triptocarbon.xyz/v1/footprint?activity={distances_miles}&activityType=miles&country={country_code}&fuelType={fuel_type}&mode={transportation_mode}"
            ).json()
            transport_carbon_footprint_response = (
                float(transport_carbon_footprint_response["carbonFootprint"]) * 0.000001
            )

        carbon_footprint_from_sti_sdq = carbon_footprint_from_route(
            "Santiago", "Santo Domingo"
        )
        carbon_footprint_from_sdq_to_mia = carbon_footprint_from_route(
            "Santo Domingo", "Miami"
        )

        total_carbon_emission = (
            carbon_footprint_from_sti_sdq
            + carbon_footprint_from_sdq_to_mia
            + transport_carbon_footprint_response
        )

        carbon_emissions_breakdown = {
            "origin": "Santiago, Dominican Republic",
            "destination": f"{address['city']}, {address['province_code']}",
            "mode": "Air & Ground",
            "emissions": total_carbon_emission,
            "emissionsUnit": "kg",
        }

        graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {
                    "carbonFootprintBreakdown": json.dumps(carbon_emissions_breakdown)
                },
            },
        )
    except Exception as ex:
        res.utils.logger.warn(f"failing to do the carbon omissions")

    return event


def get_rolls_shipment_carbon_emission(event):
    res.utils.logger.info(f"Rolls Shipment Carbon Emission {event}")

    record_id = event["one_record_id"]
    one_number = event["one_number"]

    cursor = get_snowflake_connector().cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    d = distance.distance
    g = Nominatim(user_agent="one_journey")

    query = f"""
    select "Print_Assets_flow"."Material Code", "__order_number", "Confirmed Utilized Roll display", "Roll ID" from "Print_Assets_flow" join "Rolls" as rolls on "Confirmed Utilized Roll display" = rolls."Name" where "__order_number" = '{one_number}' limit 1
    """
    print_assets = cursor.execute(query).fetchall()
    total_carbon_emission = 0
    for print_asset in print_assets:
        material = graph_request(
            GET_MATERIALS_VENDORS_LOCATION, {"code": print_asset["Material Code"]}
        )["material"]
        vendor_origin = material["vendor"]["cityAndCountry"]
        print(str(int(print_asset["Roll ID"])))
        roll_purchasing_item = graph_request(
            GET_ROLL_SHIPPING_METHOD,
            {"where": {"rollId": {"is": str(int(print_asset["Roll ID"]))}}},
        )["purchasingItems"]["purchasingItems"][0]

        shipping_method = (
            roll_purchasing_item["shippingMethod"][0]
            if len(roll_purchasing_item["shippingMethod"]) > 0
            else "Air"
        )

        _, city_origin = g.geocode(vendor_origin)
        _, city_destination = g.geocode("Santo Domingo, Dominican Republic")

        distances_miles = (d(city_origin, city_destination)).miles

        if shipping_method == "Air":
            fuel_type = "aviationGasoline"
        else:
            # Boat
            fuel_type = "motorGasoline"

        transport_carbon_footprint_response = requests.get(
            f"https://api.triptocarbon.xyz/v1/footprint?activity={distances_miles}&activityType=miles&country=def&fuelType={fuel_type}&mode=petrolCar"
        ).json()
        transport_carbon_footprint_response = (
            float(transport_carbon_footprint_response["carbonFootprint"]) * 0.00001
        )
        carbon_footprint_from_sdq_sti = carbon_footprint_from_route(
            "Santiago", "Santo Domingo"
        )

        total_carbon_emission += (
            carbon_footprint_from_sdq_sti + transport_carbon_footprint_response
        )

        carbon_emissions_breakdown = {
            "origin": vendor_origin,
            "destination": "Santiago, Dominican Republic",
            "mode": f"{shipping_method} & Ground",
            "emissions": total_carbon_emission,
            "emissionsUnit": "kg",
        }

        graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {
                    "rollsShipmentCarbonEmissionBreakdown": json.dumps(
                        carbon_emissions_breakdown
                    )
                },
            },
        )

    return event


def send_one_journey_payload_from_scan_event(event, plan=False):
    """
        {
       "trim_one_number": "10288332",
       "one_number": "10288332",
       "metadata": {},
       "scanning_at": "2023-01-06T04:10:23.000Z"
    }
    """

    one_number = event["one_number"]

    res.utils.logger.info(f"Received {event}")

    from res.connectors.airtable import AirtableConnector

    record = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.production_requests",
        filters=AirtableConnector.make_key_lookup_predicate(
            [one_number], "Order Number v3"
        ),
    )
    event = {
        "record_id": record.iloc[0]["order_line_item_id"],
        "label_one_number": event["trim_one_number"].split("/")[-1],  # safety
    }
    if plan:
        return event
    else:
        res.utils.logger.info(f"Sending {event}")
        argo = res.connectors.load("argo")
        name = f"one-journey-{one_number}-{res.utils.res_hash()[:6]}".lower()
        argo.handle_event(
            event,
            # context={"image_tag": image_tag},
            template="one-journey",
            # image_tag=image_tag,
            unique_job_name=name,
        )
    return


def read_file_and_send(
    filename="/Users/sirsh/Downloads/Order Line Items-Helen - Search - Order Line Items-Helen - Search.csv",
    one_field="Active ONE Number",
    trim_field="ONE Code URL",
    plan=False,
):
    """
    utility to read a csv and send e.g. from the one in the filename
    """
    # from res.flows.make.one_journey import send_one_journey_payload_from_scan_event

    df = pd.read_csv(filename)
    print(len(df), "read in from", filename)
    df = df[[one_field, trim_field]].dropna()
    df = df[df[one_field].map(lambda x: len(str(x)) > 5)]
    df["trim_one_number"] = df[trim_field].map(lambda x: x.strip("/").split("/")[-1])
    df["one_number"] = df[one_field].map(int).map(str)
    df["trim_one_number"] = df["trim_one_number"].map(int).map(str)
    sdf = df[["trim_one_number", "one_number"]]
    sdf["scanning_at"] = res.utils.dates.utc_now_iso_string()
    sdf["metadata"] = sdf["scanning_at"].map(lambda x: {})
    print(len(sdf), "valid for sending")
    # submit

    if not plan:
        print("sending now")
        for record in sdf.to_dict("records"):
            send_one_journey_payload_from_scan_event(record)
    return sdf


@res.flows.flow_node_attributes(
    memory="16Gi",
)
def handler(event, context={}):
    """
    The most basic consumer that can run on a schedule to listen to requests for this workflow to run and run it
    This could be replaced with an  app that consumes which seems a waste for this use case OR the caller can also just trigger the workflows directly
    """
    import traceback
    from res.flows.make.production.queries import update_one_label_association

    def try_int(x):
        try:
            return int(float(str(x)))
        except:
            return None

    REQUEST_TOPIC = "res_make.scanning_event.trim_one_number"
    with res.flows.FlowContext(event, context) as fc:
        kafka = fc.connectors["kafka"]
        assets = kafka[REQUEST_TOPIC].consume()

        assets["code"] = (
            assets["trim_one_number"].map(lambda x: str(x).split("/")[-1]).map(try_int)
        )
        assets["one_number"] = assets["one_number"].map(int)
        assets["oid"] = assets["one_number"].map(
            lambda x: res.utils.uuid_str_from_dict({"id": x})
        )
        update_one_label_association(assets)

        for record in assets.to_dict("records"):
            try:
                # old illegal states
                if record["one_number"] == record["trim_one_number"].split("/")[-1]:
                    continue
                send_one_journey_payload_from_scan_event(record)

                # SAVE to hasura

            except:
                res.utils.logger.warn(f"failing {traceback.format_exc()}")
    return {}


if __name__ == "__main__":
    event = {"record_id": "recVSVk2wNuzOyOtQ"}

    #     secrets_client.get_secret("GRAPH_API_KEY")
    #     secrets_client.get_secret("SNOWFLAKE_USER")
    #     secrets_client.get_secret("SNOWFLAKE_PASSWORD")

    #     print(os.getenv("GRAPH_API_KEY"))
    event = get_one_number(event)
#     event = generate_one(event)
#     # one_record_id = "recC9kT6i4KbWyyaq"
#     # get_electricity_water_consumption(event)
#     # get_ink_consumption(event)
#     get_chemicals_consumption(event)
#     get_shipping_carbon_emissions(event)
#     get_rolls_shipment_carbon_emission(event)
