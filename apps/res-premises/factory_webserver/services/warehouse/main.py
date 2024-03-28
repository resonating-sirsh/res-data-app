import os
from flask import render_template, Blueprint, request, jsonify
from pyairtable import Api
from datetime import date

app = Blueprint(
    "warehouse", __name__, static_folder="static", template_folder="templates"
)


AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_print_rolls = airtable.table("apprcULXTWu33KFsh", "tblSYU8a71UgbQve4")


@app.route("/", methods=["GET", "POST"])
def warehouse_checkout_app():
    if request.method == "GET":
        return render_template("checkout.html")


@app.route("/checkout_logic", methods=["POST"])
def warehouse_checkout_logic():
    data = request.json
    location_name = data.get("locationName")
    roll = data.get("roll")
    queue = data.get("queue")
    roll_record = cnx_print_rolls.all(
        formula=f"AND(FIND('{roll}', {{Name}})>0)",
        fields=["Name", "__location_name", "Inspection Done"],
    )
    if roll_record:
        if (
            location_name == roll_record[0]["fields"].get("__location_name")[0]
            or location_name == "inspection"
        ):
            if queue == "Scour Rolls Queue":
                cnx_print_rolls.update(
                    record_id=roll_record[0]["id"],
                    fields={
                        "Location V1.1": ["recxcxImPhuY9JkUs"],
                        "Checkout Type": None,
                    },
                )
                return (
                    jsonify(
                        {
                            "roll": roll_record[0]["fields"].get("Name"),
                            "status": "Done",
                            "location": location_name,
                        }
                    ),
                    200,
                )

            elif queue == "Material Prep Queue":
                cnx_print_rolls.update(
                    record_id=roll_record[0]["id"],
                    fields={
                        "Picking  Status pretreat": "Done",
                    },
                )
                return (
                    jsonify(
                        {
                            "roll": roll_record[0]["fields"].get("Name"),
                            "status": "Done",
                            "location": location_name,
                        }
                    ),
                    200,
                )

            elif queue == "Inspection Queue" and not roll_record[0]["fields"].get(
                "Inspection Done"
            ):
                cnx_print_rolls.update(
                    record_id=roll_record[0]["id"],
                    fields={
                        "Location V1.1": ["recTz0xgT4ujPkvWd"],
                        "Checkout Type": None,
                    },
                )
                return (
                    jsonify(
                        {
                            "roll": roll_record[0]["fields"].get("Name"),
                            "status": "Done",
                            "location": location_name,
                        }
                    ),
                    200,
                )

            elif queue == "Development Queue":
                cnx_print_rolls.update(
                    record_id=roll_record[0]["id"],
                    fields={"Picking  Status pretreat": "Done", "Location V1.1": None},
                )
                return (
                    jsonify(
                        {
                            "roll": roll_record[0]["fields"].get("Name"),
                            "status": "Done",
                            "location": location_name,
                        }
                    ),
                    200,
                )
            elif queue == "Others Queue":
                cnx_print_rolls.update(
                    record_id=roll_record[0]["id"],
                    fields={"Picking  Status pretreat": "Done", "Location V1.1": None},
                )
                return (
                    jsonify(
                        {
                            "roll": roll_record[0]["fields"].get("Name"),
                            "status": "Done",
                            "location": location_name,
                        }
                    ),
                    200,
                )
            else:
                return jsonify({"status": "Error", "message": "Rollo Incorrecto"}), 200
        else:
            return jsonify({"status": "Error", "message": "Rollo Incorrecto"}), 200
    else:
        return jsonify({"status": "Error", "message": "Rollo no encontrado"}), 200


@app.route("/checkout/search_queue", methods=["GET", "POST"])
def search_queue():
    if request.method == "POST":
        queue = request.json
        print(queue)
        view = ""
        status_field = ""
        if queue == "Material Prep":
            view = "PPU Rolls - Warehouse Queue"
            status_field = "Picking  Status pretreat"
        elif queue == "Scour Rolls":
            view = "Queue to Scour Wash"
            status_field = "__location_name"
        elif queue == "Development":
            view = "Development Rolls - Warehouse Checkout"
            status_field = "Picking  Status pretreat"
        elif queue == "Inspection":
            view = "Roll Length Inspection Queue - Warehouse"
            status_field = "__location_name"
        elif queue == "Others":
            view = "Request Rolls Queue - Secondary Warehouse"
            status_field = "__location_name"

        warehouse_production_queue = cnx_print_rolls.all(
            view=view,
            fields=[
                "Name",
                "__location_name",
                status_field,
                "Available Inspected Raw Yards",
            ],
        )
        warehouse_grouped_queue = {}
        if warehouse_production_queue:
            if not queue == "Inspection":
                for roll in warehouse_production_queue:
                    print(roll["fields"]["Name"])
                    if not warehouse_grouped_queue.get(
                        f'{roll["fields"]["__location_name"][0]}'
                    ):
                        warehouse_grouped_queue[
                            f'{roll["fields"]["__location_name"][0]}'
                        ] = []
                        if status_field == "__location_name":
                            if "Inspection Area" in roll["fields"].get(
                                status_field, "To Do"
                            ) or "Transit" in roll["fields"].get(status_field, "To Do"):
                                data = {
                                    "roll": roll["fields"]["Name"],
                                    "status": "Done",
                                    "location": roll["fields"]["__location_name"][0],
                                }
                            else:
                                data = {
                                    "roll": roll["fields"]["Name"],
                                    "status": "To Do",
                                    "location": roll["fields"]["__location_name"][0],
                                }
                        else:
                            data = {
                                "roll": roll["fields"]["Name"],
                                "status": roll["fields"].get(status_field, "To Do"),
                                "location": roll["fields"]["__location_name"][0],
                            }

                        warehouse_grouped_queue[
                            f'{roll["fields"]["__location_name"][0]}'
                        ].append(data)
                    else:
                        if status_field == "__location_name":
                            if "Inspection Area" in roll["fields"].get(
                                status_field, "To Do"
                            ) or "Transit" in roll["fields"].get(status_field, "To Do"):
                                data = {
                                    "roll": roll["fields"]["Name"],
                                    "status": "Done",
                                    "location": roll["fields"]["__location_name"][0],
                                }
                            else:
                                data = {
                                    "roll": roll["fields"]["Name"],
                                    "status": "To Do",
                                    "location": roll["fields"]["__location_name"][0],
                                }
                        else:
                            data = {
                                "roll": roll["fields"]["Name"],
                                "status": roll["fields"].get(status_field, "To Do"),
                                "location": roll["fields"]["__location_name"][0],
                            }
                        warehouse_grouped_queue[
                            f'{roll["fields"]["__location_name"][0]}'
                        ].append(data)

            if queue == "Inspection":
                warehouse_grouped_queue = {
                    "Priority": list(
                        map(
                            lambda x: {
                                "roll": x["fields"]["Name"],
                                "status": "To Do",
                                "location": x["fields"]["__location_name"][0],
                                "priority": x["fields"][
                                    "Available Inspected Raw Yards"
                                ][0],
                            },
                            warehouse_production_queue,
                        )
                    )
                }
                bubbleSort(warehouse_grouped_queue["Priority"])
            return jsonify(warehouse_grouped_queue), 200
        else:
            return jsonify({"status": "Empty Queue"}), 200


def bubbleSort(arr):
    n = len(arr)
    for i in range(n):
        swapped = False

        for j in range(0, n - i - 1):
            if arr[j]["priority"] > arr[j + 1]["priority"]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                swapped = True
        if swapped == False:
            break
