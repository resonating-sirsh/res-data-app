import requests, os, json, arrow
import snowflake.connector
from geopy import Nominatim, distance

GRAPH_API = os.environ.get('GRAPH_API')
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY")
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

GET_PRODUCTION_REQUEST = """
    query getMakeOneProductionRequest($orderNumber: String){
        makeOneProductionRequest(orderNumber: $orderNumber){
            id
            makeOneFlowJson
            assemblyFlowJson
             printAssetsRequests{
                id
                nestedKey
                assetsInNestedSetCount
            }
            orderLineItem{
                id
                order{
                    code
                    channelOrderId
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

FLOWS = [
    "Make ONE - Finished Goods",
    "Assembly Flow - Finished Goods",
    "Print - Prepare Rolls",
    "Print - Process Rolls"
]

ELECTRICITY_KEY = "Electricity - Electricidad"
WATER_KEY = "Water - Agua"
MILES_FROM_STI_TO_SDQ = 123.031
CARBON_FOOTPRINT_FROM_STI_TO_SDQ = 0.25 #0.5% out of 51.8kg
MILES_FROM_SQD_TO_MIA = 826.72
CARBON_FOOTPRINT_FROM_SDQ_TO_MIA = 0.011 #0.01% OUT OF 115.64kg

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

def get_consumption(snowflake_connection, from_date, to_date, node, flow_id, ewasif_type):
    cursor = snowflake_connection.cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")
    print(f"""SELECT * FROM "flower_events" WHERE "dxaNodeId" = {node["dxaNode"]["code"]} and "flowId" = '{flow_id}'""")

    from_date = arrow.get(from_date)
    to_date = arrow.get(to_date)
    one_consumption = 0

    days_in_between = (to_date - from_date).days
    include_one_back_to = node["slaDuration"]/1140
    i = 0
    ones_count = 0
    while i < days_in_between:
        if(i != days_in_between and days_in_between > 0):
            end_at = from_date.replace(hour=23, minute=59, second=00)
            start_at = from_date if i==0 else from_date.replace(hour=1, minute=00, second=00)
            start_at = start_at.shift(days=+i)
            end_at = end_at.shift(days=+1)
            count_from_date = from_date.shift(days=-include_one_back_to)
           
        elif(i == 0):
            end_at = to_date
            start_at = from_date
            count_from_date = from_date.shift(days=-include_one_back_to)

        else:
            #Last day
            end_at = to_date
            start_at = from_date.shift(days=+days_in_between)
            count_from_date = from_date.shift(days=-include_one_back_to)
    
        cursor.execute(f"""
                select one_number, entered_at, exited_at, enter_state, exit_state from (
                select "oneNumber" as one_number, "eventStartDate" as entered_at,"currentState" as enter_state,
                LAG("eventStartDate") over (partition by "oneNumber" order by "oneNumber") exited_at,
                LAG("currentState") over (partition by "oneNumber" order by "oneNumber") exit_state
                from "flower_events"
                where "dxaNodeId" = {node["dxaNode"]["code"]} and "flowId" = '{flow_id}'
                order by "flowTransactionType"
            ) where enter_state <> 'Cancelled' and ((exited_at < '{end_at.isoformat()}' and exited_at > '{start_at.isoformat()}') or (entered_at > '{count_from_date.isoformat()}' and exited_at is null))   
        """)
        
        ones_count += cursor.rowcount
        if ewasif_type == ELECTRICITY_KEY:
            electricity_consumption = get_node_ewasif_consumption(start_at, end_at, node["estimatedElectricityUsage"], ewasif_type)

            one_consumption += electricity_consumption/ones_count
        elif ewasif_type == WATER_KEY:
            water_consumption = get_node_ewasif_consumption(start_at, end_at, node["estimatedWaterUsage"], ewasif_type)
            one_consumption += water_consumption/ones_count
        i+=1

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
                "type":{
                    "is": ewasif_type
                },
                "and": [
                    {
                        "createdAt": {
                            "isGreaterThan": from_date.replace(hour=1, minute=00, second=00).isoformat()
                        }
                    },
                    {
                        "createdAt": {
                            "isLessThan": to_date.replace(hour=23, minute=59, second=59).isoformat()
                        }
                    }
                ]
            }
            
        }
    )
    ewasif_consumptions = ewasif_consumptions_response["ewasifConsumptions"]["ewasifConsumptions"]

    for consumption_record in ewasif_consumptions:
        if ewasif_type == ELECTRICITY_KEY:
            consumption += consumption_record["electricityConsumption"] if consumption_record["electricityConsumption"]  else 0
        else:
            consumption += consumption_record["waterConsumption"] if consumption_record["waterConsumption"]  else 0
    consumption = consumption * estimatedUsage
    return consumption

def get_one_node_information(snowflake_connection, one_code, dxa_node_id, flow_id):
    cursor = snowflake_connection.cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")
    print(f"""        SELECT * FROM "flower_events" WHERE "oneNumber" like '%{one_code}% and "dxaNodeId" = {dxa_node_id} and "flowId" = '{flow_id}'""")
    cursor.execute(f"""
        SELECT * FROM "flower_events" WHERE "oneNumber" like '%{one_code}%' and "dxaNodeId" = {dxa_node_id} and "flowId" = '{flow_id}' order by "flowTransactionType"
    """)

    result = cursor.fetchall()

    # close snow cursor
    cursor.close()

    return result


def get_electricity_water_consumption(record_id, snow_connection):
    print("Calculating electricity consumption")

    one = graph_request(
        GET_ONE,
        {"id": record_id}
    )["one"]

    electricity_consumption = 0
    electricity_consumption_by_node = []
    water_consumption_by_node = []

    for flow_name in FLOWS:
        print("Working with flow: ", flow_name)
        nodes = graph_request(
            GET_NODES,
            {
                "where": {
                    "isActive": {"is": True},
                    "flowName": {"is": flow_name}
                }
            }
        )
        for node in nodes["flowerStates"]["flowerStates"]:
            print("Working with node: ", node["name"])
            events = get_one_node_information(snowflake_connection, one['oneId'], node["dxaNode"]["code"], node["flow"]["code"])
            if len(events) >=2:
                enter_event = events[0]
                exit_event = events[1]
                
                #getting electricity Usage
                electricity_consumption = get_consumption(snowflake_connection, enter_event["eventStartDate"], exit_event["eventStartDate"], node, node["flow"]["code"], ELECTRICITY_KEY)
                electricity_consumption_by_node.append({
                    "nodeId": node["dxaNode"]["code"],
                    "nodeName": node["dxaNode"]["name"],
                    "amount": round(electricity_consumption, 2),
                    "unit": "kW"
                })
                print(electricity_consumption_by_node)

                #getting water usage
                water_usage = get_consumption(snowflake_connection, enter_event["eventStartDate"], exit_event["eventStartDate"], node, node["flow"]["code"], WATER_KEY)
                water_consumption_by_node.append({
                    "nodeId": node["dxaNode"]["code"],
                    "nodeName": node["dxaNode"]["name"],
                    "amount": round(water_usage, 2),
                    "unit": "m3"
                })  

    graph_request(
        UPDATE_ONE_MUTATION,
        {
            "id": record_id,
            "input": {
                "electricityUsageBreakdown": json.dumps(electricity_consumption_by_node),
                "waterUsageBreakdown": json.dumps(water_consumption_by_node)
            }
        }
    )

    print("end")


def get_ink_consumption(record_id, snow_connection):
    print("Getting Ink Consumption")
    cursor = snow_connection.cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    one = graph_request(
        GET_ONE,
        {"id": record_id}
    )["one"]

    production_request = graph_request(
        GET_PRODUCTION_REQUEST,
        {"orderNumber": one["oneId"]}
    )["makeOneProductionRequest"]

    ink_consumption = {
        "yellow": 0,
        "orange": 0,
        "red": 0,
        "black": 0,
        "blue": 0,
        "magenta": 0,
        "cyan": 0,
        "transparent": 0
    }

    total_ink_consumption = 0

    for print_asset in production_request["printAssetsRequests"]:
        query = f"""
            WITH printer_data AS (
                SELECT 'FLASH' as "printer", 'JP7' as "model", "Blue_cleaning_normal", "Magenta_cleaning_normal", "Red_type", "job_name", "Yellow_cleaning_soft", "printer_name", "printed_length_value", "Black_cleaning_strong", "Blue_cleaning_soft", "Blue_ink_used_to_print", "Orange_cleaning_soft", "Transparent_ink_used_to_clean", "Transparent_type", "width", "Red_ink_used_to_clean", "Yellow_cleaning_normal", "Yellow_type", "Black_type", "media", "Magenta_ink_used_to_clean", "Magenta_ink_used_to_print", "Orange_ink_used_to_print", "average_speed_value", "head_gap_value", "started_time", "fold_detected", "order_number", "Yellow_cleaning_strong", "Black_cleaning_soft", "Black_ink_used_to_print", "Cyan_ink_used_to_print", "Orange_cleaning_normal", "requested_length_unit", "Black_cleaning_normal", "Magenta_cleaning_strong", "Red_cleaning_strong", "Transparent_cleaning_strong", "requested_length", "started_date", "Transparent_ink_used_to_print", "id", "CONFIG_NAME", "Blue_cleaning_strong", "Cyan_cleaning_normal", "Cyan_ink_used_total", "Transparent_ink_used_total", "Red_cleaning_normal", "Red_ink_used_to_print", "Transparent_cleaning_normal", "Yellow_ink_used_to_print", "ink_reports", "job_id", "average_speed", "Blue_ink_used_total", "Orange_cleaning_strong", "Transparent_cleaning_soft", "print_mode", "started", "width_value", "Magenta_type", "head_gap", "head_gap_unit", "operator", "Black_ink_used_total", "Blue_type", "Cyan_ink_used_to_clean", "Orange_ink_used_to_clean", "Red_ink_used_total", "Cyan_cleaning_strong", "Orange_type", "Red_cleaning_soft", "elapsed_time", "job_status", "Black_ink_used_to_clean", "Blue_ink_used_to_clean", "Cyan_cleaning_soft", "Cyan_type", "Magenta_cleaning_soft", "Orange_ink_used_total", "Yellow_ink_used_total", "printed_length", "Magenta_ink_used_total", "Yellow_ink_used_to_clean", "width_unit", "requested_length_value", "displayed_job_id", "printed_length_unit"
                FROM "MSPrinters_RESONANCE_flash_print_job_data" where "started" is not null
                UNION ALL
                SELECT 'LUNA' as "printer", 'JP7' as "model", "Blue_cleaning_normal", "Magenta_cleaning_normal", "Red_type", "job_name", "Yellow_cleaning_soft", "printer_name", "printed_length_value", "Black_cleaning_strong", "Blue_cleaning_soft", "Blue_ink_used_to_print", "Orange_cleaning_soft", "Transparent_ink_used_to_clean", "Transparent_type", "width", "Red_ink_used_to_clean", "Yellow_cleaning_normal", "Yellow_type", "Black_type", "media", "Magenta_ink_used_to_clean", "Magenta_ink_used_to_print", "Orange_ink_used_to_print", "average_speed_value", "head_gap_value", "started_time", "fold_detected", "order_number", "Yellow_cleaning_strong", "Black_cleaning_soft", "Black_ink_used_to_print", "Cyan_ink_used_to_print", "Orange_cleaning_normal", "requested_length_unit", "Black_cleaning_normal", "Magenta_cleaning_strong", "Red_cleaning_strong", "Transparent_cleaning_strong", "requested_length", "started_date", "Transparent_ink_used_to_print", "id", "CONFIG_NAME", "Blue_cleaning_strong", "Cyan_cleaning_normal", "Cyan_ink_used_total", "Transparent_ink_used_total", "Red_cleaning_normal", "Red_ink_used_to_print", "Transparent_cleaning_normal", "Yellow_ink_used_to_print", "ink_reports", "job_id", "average_speed", "Blue_ink_used_total", "Orange_cleaning_strong", "Transparent_cleaning_soft", "print_mode", "started", "width_value", "Magenta_type", "head_gap", "head_gap_unit", "operator", "Black_ink_used_total", "Blue_type", "Cyan_ink_used_to_clean", "Orange_ink_used_to_clean", "Red_ink_used_total", "Cyan_cleaning_strong", "Orange_type", "Red_cleaning_soft", "elapsed_time", "job_status", "Black_ink_used_to_clean", "Blue_ink_used_to_clean", "Cyan_cleaning_soft", "Cyan_type", "Magenta_cleaning_soft", "Orange_ink_used_total", "Yellow_ink_used_total", "printed_length", "Magenta_ink_used_total", "Yellow_ink_used_to_clean", "width_unit", "requested_length_value", "displayed_job_id", "printed_length_unit"
                FROM "MSPrinters_RESONANCE_luna_print_job_data" where "started" is not null
                UNION ALL
                SELECT 'YODA' as "printer", 'JP5' as "model", "Blue_cleaning_normal", "Magenta_cleaning_normal", "Red_type", "job_name", "Yellow_cleaning_soft", "printer_name", "printed_length_value", "Black_cleaning_strong", "Blue_cleaning_soft", "Blue_ink_used_to_print", "Orange_cleaning_soft", "Transparent_ink_used_to_clean", "Transparent_type", "width", "Red_ink_used_to_clean", "Yellow_cleaning_normal", "Yellow_type", "Black_type", "media", "Magenta_ink_used_to_clean", "Magenta_ink_used_to_print", "Orange_ink_used_to_print", "average_speed_value", "head_gap_value", "started_time", "fold_detected", "order_number", "Yellow_cleaning_strong", "Black_cleaning_soft", "Black_ink_used_to_print", "Cyan_ink_used_to_print", "Orange_cleaning_normal", "requested_length_unit", "Black_cleaning_normal", "Magenta_cleaning_strong", "Red_cleaning_strong", "Transparent_cleaning_strong", "requested_length", "started_date", "Transparent_ink_used_to_print", "id", "CONFIG_NAME", "Blue_cleaning_strong", "Cyan_cleaning_normal", "Cyan_ink_used_total", "Transparent_ink_used_total", "Red_cleaning_normal", "Red_ink_used_to_print", "Transparent_cleaning_normal", "Yellow_ink_used_to_print", "ink_reports", "job_id", "average_speed", "Blue_ink_used_total", "Orange_cleaning_strong", "Transparent_cleaning_soft", "print_mode", "started", "width_value", "Magenta_type", "head_gap", "head_gap_unit", "operator", "Black_ink_used_total", "Blue_type", "Cyan_ink_used_to_clean", "Orange_ink_used_to_clean", "Red_ink_used_total", "Cyan_cleaning_strong", "Orange_type", "Red_cleaning_soft", "elapsed_time", "job_status", "Black_ink_used_to_clean", "Blue_ink_used_to_clean", "Cyan_cleaning_soft", "Cyan_type", "Magenta_cleaning_soft", "Orange_ink_used_total", "Yellow_ink_used_total", "printed_length", "Magenta_ink_used_total", "Yellow_ink_used_to_clean", "width_unit", "requested_length_value", "displayed_job_id", "printed_length_unit"
                FROM "MSPrinters_RESONANCE_yoda_print_job_data" where "started" is not null
                UNION ALL
                SELECT 'NOVA' as "printer", 'JP7' as "model", "Blue_cleaning_normal", "Magenta_cleaning_normal", "Red_type", "job_name", "Yellow_cleaning_soft", "printer_name", "printed_length_value", "Black_cleaning_strong", "Blue_cleaning_soft", "Blue_ink_used_to_print", "Orange_cleaning_soft", "Transparent_ink_used_to_clean", "Transparent_type", "width", "Red_ink_used_to_clean", "Yellow_cleaning_normal", "Yellow_type", "Black_type", "media", "Magenta_ink_used_to_clean", "Magenta_ink_used_to_print", "Orange_ink_used_to_print", "average_speed_value", "head_gap_value", "started_time", "fold_detected", "order_number", "Yellow_cleaning_strong", "Black_cleaning_soft", "Black_ink_used_to_print", "Cyan_ink_used_to_print", "Orange_cleaning_normal", "requested_length_unit", "Black_cleaning_normal", "Magenta_cleaning_strong", "Red_cleaning_strong", "Transparent_cleaning_strong", "requested_length", "started_date", "Transparent_ink_used_to_print", "id", "CONFIG_NAME", "Blue_cleaning_strong", "Cyan_cleaning_normal", "Cyan_ink_used_total", "Transparent_ink_used_total", "Red_cleaning_normal", "Red_ink_used_to_print", "Transparent_cleaning_normal", "Yellow_ink_used_to_print", "ink_reports", "job_id", "average_speed", "Blue_ink_used_total", "Orange_cleaning_strong", "Transparent_cleaning_soft", "print_mode", "started", "width_value", "Magenta_type", "head_gap", "head_gap_unit", "operator", "Black_ink_used_total", "Blue_type", "Cyan_ink_used_to_clean", "Orange_ink_used_to_clean", "Red_ink_used_total", "Cyan_cleaning_strong", "Orange_type", "Red_cleaning_soft", "elapsed_time", "job_status", "Black_ink_used_to_clean", "Blue_ink_used_to_clean", "Cyan_cleaning_soft", "Cyan_type", "Magenta_cleaning_soft", "Orange_ink_used_total", "Yellow_ink_used_total", "printed_length", "Magenta_ink_used_total", "Yellow_ink_used_to_clean", "width_unit", "requested_length_value", "displayed_job_id", "printed_length_unit"
                FROM "MSPRINTERS_NOVA_PRINT_JOB_DATA" where "started" is not null
                )
                SELECT printer_data."job_name" as job_name
                    , printer_data."Yellow_ink_used_total" as yellow
                    , printer_data."Orange_ink_used_total" as orange
                    , printer_data."Red_ink_used_total" as red
                    , printer_data."Black_ink_used_total" as black
                    , printer_data."Blue_ink_used_total" as blue
                    , printer_data."Magenta_ink_used_total" as magenta
                    , printer_data."Cyan_ink_used_total" as cyan
                    , printer_data."Transparent_ink_used_total" as transparent
                    FROM printer_data  WHERE job_name='{print_asset["nestedKey"]}';
                """
        print(query)
        rows = cursor.execute(query).fetchall()
        cursor.close()

        for key in ink_consumption.keys():
            row = rows[0]
            value = float(row[key.upper()]) if row[key.upper()] else 0
            ink_consumption[key] += value / print_asset["assetsInNestedSetCount"]
            total_ink_consumption += value / print_asset["assetsInNestedSetCount"]
    ink_consumption["total"] = total_ink_consumption
    ink_consumption["unit"] = "ml"
    graph_request(
        UPDATE_ONE_MUTATION,
        {
            "id": record_id,
            "input": {
                "inkUsageBreakdown": json.dumps(ink_consumption)
            }
        }
    )

def get_chemicals_consumption(record_id, snowflake_connection):
    cursor = snowflake_connection.cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    one = graph_request(
        GET_ONE,
        {"id": record_id}
    )["one"]

    production_request = graph_request(
        GET_PRODUCTION_REQUEST,
        {"orderNumber": one["oneId"]}
    )["makeOneProductionRequest"]

    chemicals_key_mapping = {
        "prepajet": "PREPAJET_KG",
        "silkSodiumBicarbonate": "SILK_SODIUM_BICARBONATE_KG",
        "cottonSodiumBicarbonate": "COTTON_SODIUM_BICARBONATE_KG",
        "urea": "UREA_KG",
        "lyoprint": "LYOPRINT_KG",
        "albaflow": "ALBAFLOW_L"
    }

    chemicals_consumption = {
        "prepajet": 0,
        "silkSodiumBicarbonate": 0,
        "cottonSodiumBicarbonate": 0,
        "urea": 0,
        "lyoprint": 0,
        "albaflow": 0
    }

    total_chemical_consumption = 0
    
    for print_asset in production_request["printAssetsRequests"]:
        query=f"""
            WITH roll_pretreatment AS (SELECT "Name" as roll_name
                , "PRETREAT BATCH NUMBER" as pretreat_batch
                , "_record_id" as roll_id
            FROM  IAMCURIOUS_SCHEMA."Rolls" AS rolls
                ),
                pretreatment_batches AS (SELECT "OBJECT_ID" as object_id
                    , "1_Water_kg" as water_kg
                    , "2_Prepajet_PAB_kg" as prepajet_kg
                    , "3_SILK_Sodium_Bicarbonate_kg" as silk_sodium_bicarbonate_kg
                    , "3_COTTON_Sodium_Carbonate_kg" as cotton_sodium_bicarbonate_kg
                    , "4_Urea_kg" as urea_kg
                    , "5_Lyoprint_Rg_kg" as lyoprint_kg
                    , "6_Albaflow_L" as albaflow_l
                FROM IAMCURIOUS_SCHEMA."Pretreatment_Batch_flow"),
                nests AS (SELECT "Key" as nest_id
                    , get(parse_json("__roll_record_id"), 0) as roll_id
                    , "Nested Length in Yards" as nested_length
                    , "Roll_Length_Yards" as roll_length
                    , nested_length/roll_length as percentage
                FROM IAMCURIOUS_SCHEMA."nested_asset_sets" as nests)
                SELECT nest_id
                    , nests.roll_id
                    , water_kg*percentage as water_kg
                    , prepajet_kg*percentage as prepajet_kg
                    , silk_sodium_bicarbonate_kg*percentage as silk_sodium_bicarbonate_kg
                    , cotton_sodium_bicarbonate_kg*percentage as cotton_sodium_bicarbonate_kg
                    , urea_kg*percentage as urea_kg
                    , lyoprint_kg*percentage as lyoprint_kg
                    , albaflow_l*percentage as albaflow_l
                FROM roll_pretreatment
                INNER JOIN pretreatment_batches
                ON roll_pretreatment.pretreat_batch = pretreatment_batches."OBJECT_ID"
                INNER JOIN nests
                ON roll_pretreatment.roll_id = nests.roll_id
                WHERE nest_id='{print_asset["nestedKey"]}';
            """
        rows = cursor.execute(query).fetchall()
        row = rows[0]
        cursor.close()
        for key in chemicals_consumption.keys():
            row = rows[0]
            value = round(float(row[chemicals_key_mapping[key]]), 2)if row[chemicals_key_mapping[key]] else 0
            chemicals_consumption[key] += value / print_asset["assetsInNestedSetCount"]
            total_chemical_consumption += value / print_asset["assetsInNestedSetCount"]
        chemicals_consumption["total"] = round(total_chemical_consumption, 2)
        chemicals_consumption["unit"] = "kg"
        graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {
                    "chemicalsUsageBreakdown": json.dumps(chemicals_consumption)
                }
            }
        )

def get_shipping_carbon_emissions(record_id, snowflake_connection):
    print("Shipping Carbon Emissions")

    one = graph_request(
        GET_ONE,
        {"id": record_id}
    )["one"]

    production_request = graph_request(
        GET_PRODUCTION_REQUEST,
        {"orderNumber": one["oneId"]}
    )["makeOneProductionRequest"]

    cursor = snowflake_connection.cursor(snowflake.connector.cursor.DictCursor)
    cursor.execute(f"USE IAMCURIOUS_DB")
    cursor.execute(f"USE WAREHOUSE IAMCURIOUS")
    cursor.execute(f"USE SCHEMA IAMCURIOUS_SCHEMA")

    cursor.execute(f"""
        select "id", "shipping_address" from "SHOPIFY_ORDERS" where "id" = {production_request["orderLineItem"]["order"]["channelOrderId"]}
    """)

    result = cursor.fetchall()

    # close snow cursor
    cursor.close()

    address = json.loads(result[0]["shipping_address"])

    d = distance.distance
    g = Nominatim(user_agent="one_journey")
    _, from_miami = g.geocode('Miami, FL')
    _, to_customer = g.geocode(f"{address['city']}, {address['province_code']}")

    distances_miles = (d(from_miami, to_customer)).miles

    usa_carbon_footprint_response = requests.get(f"https://api.triptocarbon.xyz/v1/footprint?activity={distances_miles}&activityType=miles&country=usa&fuelType=motorGasoline&mode=anyCar").json()
    usa_carbon_footprint = float(usa_carbon_footprint_response["carbonFootprint"]) * 0.01

    total_carbon_emission = CARBON_FOOTPRINT_FROM_STI_TO_SDQ + CARBON_FOOTPRINT_FROM_SDQ_TO_MIA + usa_carbon_footprint
    
    carbon_emissions_breakdown = {
        "origin": "Santiago, Dominican Republic",
        "destination": f"{address['city']}, {address['province_code']}",
        "mode": "Air & Ground",
        "emissions": total_carbon_emission,
        "emissionsUnit": "kg"
    }
    
    graph_request(
            UPDATE_ONE_MUTATION,
            {
                "id": record_id,
                "input": {
                    "carbonFootprintBreakdown": json.dumps(carbon_emissions_breakdown)
                }
            }
        )

if __name__ == "__main__":
    print("here")
    snowflake_connection = snowflake.connector.connect(
        account=os.environ.get('snowflake_account'),
        user=os.environ.get('snowflake_user'),
        password=os.environ.get('snowflake_password'),
        role='ACCOUNTADMIN',
        region='us-east-1'
    )
    record_id = "recmHV2PcXrPXZXLV"
    get_electricity_water_consumption(record_id, snowflake_connection)
    get_ink_consumption(record_id, snowflake_connection)
    get_chemicals_consumption(record_id, snowflake_connection)
    get_shipping_carbon_emissions(record_id, snowflake_connection)