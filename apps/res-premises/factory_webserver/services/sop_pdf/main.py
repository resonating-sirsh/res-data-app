import os
from flask import render_template, Blueprint, request, jsonify
from pyairtable import Api
from datetime import date


app = Blueprint(
    "sop_pdf", __name__, static_folder="static", template_folder="templates"
)


AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_sop = airtable.table("app6tdm0kPbc24p1c", "tblbMK2flTbnuhWRD")
cnx_steps = airtable.table("app6tdm0kPbc24p1c", "tblUem7XkV0TgHxvo")


@app.route("/", methods=["GET", "POST"])
def sop_pdf_app():
    sop = cnx_sop.all(
        view="webserver - sop",
        fields=["sopName", "SOP Translation to Spanish"],
    )

    current_date = date.today().strftime("%A %d. %B %Y")

    return render_template("sop_pdf.html", sop=sop, current_date=current_date)


@app.route("/search_sop", methods=["GET", "POST"])
def search_sop():
    if request.method == "POST":
        id = request.json
        print(id)
        steps_formula = f"""AND(find('{id}', {{sop_id}})>0)"""
        steps = cnx_steps.all(
            formula=steps_formula,
        )

    return jsonify(steps), 200
