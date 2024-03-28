import os
from flask import Flask, render_template, request, jsonify, Blueprint
from pyairtable import Api

app = Blueprint("ppu", __name__, static_folder="static", template_folder="templates")

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_ppu_order = airtable.table("appHqaUQOkRD8VbzX", "tblgUIevlJMrP28VT")


@app.route("/", methods=["GET", "POST"])
def ppu_creation():
    global cotton, silk, ppu_schedule
    cotton_started = False
    silk_started = False

    if request.method == "GET":
        cotton = cnx_ppu_order.all(
            view="PPU Construction Order",
            fields=[
                "Order",
                "Pretreat Type",
                "Roll Name",
                "Requires Construction",
                "Ready to Mount",
            ],
            formula="AND({Pretreat Type}='Cotton', {Roll Name} != 'LEADER', {Roll Name} != 'FOOTER', {Roll Name} != 'HEADER', {Requires Construction}=TRUE())",
        )

        silk = cnx_ppu_order.all(
            view="PPU Construction Order",
            fields=[
                "Order",
                "Pretreat Type",
                "Roll Name",
                "Requires Construction",
                "Ready to Mount",
            ],
            formula="AND({Pretreat Type}='Silk', {Roll Name} != 'LEADER', {Roll Name} != 'FOOTER', {Roll Name} != 'HEADER', {Requires Construction}=TRUE())",
        )

        # Create a new list only with the records that don't have Ready to Mount or that have Ready to mount = to False
        cotton_new = [
            item
            for item in cotton
            if "Ready to Mount" not in item["fields"]
            or not item["fields"]["Ready to Mount"]
        ]

        if cotton != cotton_new:
            cotton[:] = cotton_new
            cotton_started = True

        silk_new = [
            item
            for item in silk
            if "Ready to Mount" not in item["fields"]
            or not item["fields"]["Ready to Mount"]
        ]

        if silk != silk_new:
            silk[:] = silk_new
            silk_started = True

        if silk and cotton and not cotton_started and not silk_started:
            return render_template("ppu.html", both=True)

        elif (cotton_started and cotton) or (cotton and not silk):
            ppu_schedule = cotton + silk
            return render_template(
                "ppu.html",
                next_roll=f"<span class='badge bg-secondary'>Siguiente Rollo:</span> {ppu_schedule[0]['fields']['Roll Name']}",
            )
        elif (silk_started and silk) or (silk and not cotton):
            ppu_schedule = silk + cotton
            return render_template(
                "ppu.html",
                next_roll=f"<span class='badge bg-secondary'>Siguiente Rollo:</span> {ppu_schedule[0]['fields']['Roll Name']}",
            )
        else:
            return render_template("ppu.html", done=True)

    if request.method == "POST":
        if "value" in request.json and request.json["value"] == "cotton":
            ppu_schedule = cotton + silk
            return (
                jsonify(
                    {
                        "status": "Success",
                        "roll_name": '<span class="badge bg-secondary">Siguiente Rollo:</span>'
                        + " "
                        + ppu_schedule[0]["fields"]["Roll Name"],
                    }
                ),
                200,
            )
        else:
            ppu_schedule = silk + cotton
            return (
                jsonify(
                    {
                        "status": "Success",
                        "roll_name": '<span class="badge bg-secondary">Siguiente Rollo: </span>'
                        + " "
                        + ppu_schedule[0]["fields"]["Roll Name"],
                    }
                ),
                200,
            )
    else:
        return (
            jsonify({"status": "Error", "message": "Coudn't load Pretreatment Type"}),
            200,
        )


@app.route("/ppu_qr_code", methods=["POST"])
def handle_ppu_qr():
    roll_name = request.json["value"]
    if roll_name == ppu_schedule[0]["fields"]["Roll Name"].split(":")[0]:
        try:
            id = ppu_schedule[0]["id"]
            cnx_ppu_order.update(id, {"Ready to Mount": True})
            del ppu_schedule[0]
            return (
                jsonify(
                    {
                        "status": "Success",
                        "message": "Rollo listo para PPU",
                        "roll_name": '<span class="badge bg-secondary">Siguiente Rollo: </span>'
                        + " "
                        + ppu_schedule[0]["fields"]["Roll Name"],
                    }
                ),
                200,
            )
        except:
            return (
                jsonify({"status": "Error", "message": "Fallo al subir a Airtable"}),
                200,
            )

    else:
        return (
            jsonify(
                {
                    "status": "Error",
                    "message": f"Rollo incorrecto. Debería ser {ppu_schedule[0]['fields']['Roll Name']}",
                }
            ),
            200,
        )
