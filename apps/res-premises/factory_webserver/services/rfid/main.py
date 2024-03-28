import os
from res.utils import logger
from flask import (
    render_template,
    request,
    jsonify,
    make_response,
    Blueprint,
)
import res
from schemas.pydantic.premises import RfidEventRequest
from pyairtable import Api
from tenacity import retry, wait_fixed, stop_after_attempt
import arrow

app = Blueprint("rfid", __name__, static_folder="static", template_folder="templates")
kafka_url = "https://data.resmagic.io/kgateway/submitevent"
REQUEST_TOPIC = "res_premises.rfid_telemetry.submit_rfid_event_request"
kafka = res.connectors.load("kafka")
redis = res.connectors.load("redis")
rfid_cache = redis["RFID"]["RFID_TAGS"]
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_it_support = airtable.table("appyxEVba0zT38PP7", "tblcLo3gkFZlfDR3J")
cnx_tag_warehouse = airtable.table("appyxEVba0zT38PP7", "tblClfHBULHzWgBLt")


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    return getattr(cnx, method)(*args, **kwargs)


@app.route("/", methods=["GET", "POST"])
def handle_post():
    if request.method == "POST":
        payload = request.json
        logger.info(f"Working with: {payload}")
        reader_name = payload["reader_name"]
        for read in payload.get("tag_reads"):
            if read.get("antennaPort") == 1 and reader_name == "Assemble RFID Reader":
                check_if_tag_assigned_to_item(read.get("epc"))
                # remove_item_from_epc(epc)
                # remove_epc_from_item(epc)
            else:
                data = create_payload(payload["reader_name"], read)
                kafka[REQUEST_TOPIC].publish(data.dict(), use_kgateway=True)
    return "Nothing to see here"


def check_if_tag_assigned_to_item(epc):
    record = cnx_tag_warehouse.first(formula=f"AND({{Tag EPC}}='{epc}')")
    if record and record["fields"].get("Assigned Roll"):
        logger.info(
            f"Tag: {epc} already assigned to item {record['fields']['Attached Item Name'][0]}"
        )
        rfid_cache["LATEST_RFID_TAG"] = {
            "epc": epc,
            "item": record["fields"]["Attached Item Name"][0],
        }
    else:
        register_tag_warehouse(epc)
        rfid_cache["LATEST_RFID_TAG"] = {"epc": epc}


def register_tag_warehouse(tag_epc, num_of_cycles=0):
    record = cnx_tag_warehouse.all(formula=f"AND({{Tag EPC}} = '{tag_epc}')")
    if not record:
        record_for_warehouse = {
            "Tag EPC": tag_epc,
            "Status": "Recovered/Storage",
            "Number of cycles": num_of_cycles,
        }
        call_airtable(
            cnx_tag_warehouse,
            record_for_warehouse,
            typecast=True,
            method="create",
        )
    else:
        logger.info(f"Tag: {tag_epc} already registered in Tag Warehouse")


def create_payload(reader_name, data):
    event = RfidEventRequest(
        **{
            "reader": reader_name,
            "channel": str(data["antennaPort"]),
            "tag_epc": data["epc"],
            "observed_at": str(arrow.get(data.get("firstSeenTimestamp"))),
            "metadata": {},
        }
    )
    return event


@app.route("/roll_tag_assignation", methods=["GET", "POST"])
def roll_tag_assignation():
    if request.method == "POST":
        message = request.json
        roll = message.get("roll")
        tag1 = message.get("tag1")
        tag2 = message.get("tag2")
        event = {"roll": roll, "tag1": tag1, "tag2": tag2}
        status = submit_assignation(
            {
                "tagged_item": event["roll"],
                "rfid_tags": f"{event['tag1']}, {event['tag2']}",
                "item_type": "Roll",
                "item_state": [
                    {"Substation": "Pretreatment"},
                    {"Substation": "Print"},
                    {"Substation": "Steam"},
                    {"Substation": "Wash"},
                    {"Substation": "Dry"},
                    {"Substation": "Roll Inspection 1"},
                ],
            }
        )

        if status == True:
            response_data = {"message": "Success"}
            return make_response(jsonify(response_data), 200)

        elif status == False:
            response_data = {"message": "Something went wrong"}
            return make_response(jsonify(response_data), 400)

    return render_template("tag_assignation.html")


def submit_assignation(event):
    try:
        record = call_airtable(
            cnx_it_support,
            event,
            typecast=True,
            method="create",
        )

        epcs = record["fields"]["rfid_tags"].split(", ")
        for epc in epcs:
            tag_to_update = cnx_tag_warehouse.first(formula=f"AND({{Tag EPC}}='{epc}')")
            print(tag_to_update)

            call_airtable(
                cnx_tag_warehouse,
                tag_to_update["id"],
                {"Status": "Production", "Assigned Roll": record["id"]},
                typecast=True,
                method="update",
            )

        return True

    except Exception as e:
        print(f"Something went wrong: {e}")
        return False


def remove_item_from_epc(epc):
    epc_warehouse = cnx_tag_warehouse.first(formula=f"AND({{Tag EPC}}='{epc}')")
    if not epc_warehouse:
        register_tag_warehouse(epc, 1)

    elif epc_warehouse["fields"].get("Status") != "Recovered/Storage":
        call_airtable(
            cnx_tag_warehouse,
            epc_warehouse["id"],
            {
                "Assigned Roll": "",
                "Status": "Recovered/Storage",
                "Number of cycles": epc_warehouse["fields"]["Number of cycles"] + 1,
            },
            typecast=True,
            method="update",
        )
        if epc_warehouse["fields"].get("Assigned Roll"):
            logger.info(
                f"Tag: {epc} removed from item {epc_warehouse['fields']['Attached Item Name'][0]}"
            )
        else:
            logger.info(f"Tag: {epc} not assigned to any item but recovered")
    else:
        logger.info(f"Tag: {epc} already in Recovered/Storage status")


def remove_epc_from_item(epc):
    tracking_records = cnx_it_support.all(formula=f"SEARCH('{epc}', {{rfid_tags}})")

    for record in tracking_records:
        rfid_tags = record["fields"].get("rfid_tags")
        epcs = rfid_tags.split(", ") if ", " in rfid_tags else [rfid_tags]

        if epc in epcs:
            epcs.remove(epc)

        if not epcs:
            call_airtable(
                cnx_it_support,
                record["id"],
                method="update",
                fields={"rfid_tags": "", "item_state": "Done"},
            )
            logger.info(
                f"All tags removed from item: {record['fields']['tagged_item']}"
            )
        else:
            missing_epc = cnx_tag_warehouse.first(
                formula=f"AND({{Tag EPC}}='{epcs[0]}')"
            )
            if missing_epc:
                call_airtable(
                    cnx_it_support,
                    record["id"],
                    {"rfid_tags": epcs[0], "item_state": "Missing Tag"},
                    typecast=True,
                    method="update",
                )
                call_airtable(
                    cnx_tag_warehouse,
                    missing_epc["id"],
                    {"Status": "Missing"},
                    typecast=True,
                    method="update",
                )
            else:
                register_tag_warehouse(epcs[0])


@app.route("/get_epc")
def get_data():
    latest_tag = rfid_cache["LATEST_RFID_TAG"]

    if request.method == "GET" and latest_tag:
        print(latest_tag)
        if not latest_tag.get("item"):
            data = {"tag": latest_tag["epc"]}
            rfid_cache.delete("LATEST_RFID_TAG")
            return jsonify(data)
        else:
            print("here")
            data = {
                "tag": latest_tag["epc"],
                "item": latest_tag["item"],
            }
            rfid_cache.delete("LATEST_RFID_TAG")
            return jsonify(data)
    else:
        return jsonify({"tag": ""})


# if __name__ == "__main__":
#     event = RfidEventRequest(
#         **{
#             "reader": "Print RFID Reader",
#             "channel": "1",
#             "tag_epc": "300F4F573AD003C251D77C80",
#             # "tag_epc": "300F4F573AD003C251D77D13",
#             "observed_at": str(datetime.now()),
#             "metadata": {},
#         }
#     )

#     kafka[REQUEST_TOPIC].publish(event.dict(), use_kgateway=True)
