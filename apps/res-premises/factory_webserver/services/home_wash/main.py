import os
from flask import render_template, Blueprint, request, jsonify
from pyairtable import Api
from datetime import date

app = Blueprint(
    "home_wash", __name__, static_folder="static", template_folder="templates"
)


AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_print_rolls = airtable.table("apprcULXTWu33KFsh", "tblSYU8a71UgbQve4")
cnx_print_nests = airtable.table("apprcULXTWu33KFsh", "tbl7n4mKIXpjMnJ3i")
cnx_print_subnests = airtable.table("apprcULXTWu33KFsh", "tblMvfJeCOlauzb8v")


@app.route("/", methods=["GET", "POST"])
def home_wash_app():
    rolls = cnx_print_rolls.all(
        view="Soften Queue",
        fields=["Name", "_record_id"],
    )

    if request.method == "GET":
        return render_template("home_wash.html", rolls=rolls)


@app.route("/search_jobs", methods=["GET", "POST"])
def search_rolls():
    if request.method == "POST":
        data = request.json
        jobs_formula = f"""AND(find('{data["roll"]}', {{Roll Name}})>0)"""
        # jobs_formula = f"""AND(find('R29174: CTJ95', {{Roll Name}})>0)"""

        jobs = cnx_print_subnests.all(
            formula=jobs_formula,
        )

    return jsonify(jobs), 200


@app.route("/check_roll", methods=["GET", "POST"])
def check_rolls():
    if request.method == "POST":
        data = request.json
        print(data)
        jobs_formula = f"""AND(find('{data["name"]}', {{Printed Name}})>0)"""

        jobs = cnx_print_subnests.all(
            formula=jobs_formula,
        )
        cnx_print_subnests.update(
            record_id=jobs[0]["id"], fields={f"Home Wash {data['type']}": data["value"]}
        )

    return {}, 200


@app.route("/weight", methods=["GET", "POST"])
def save_weight():
    if request.method == "POST":
        data = request.json
        roll_formula = f"""AND({{Name}}='{data["name"]}')"""

        roll = cnx_print_rolls.all(
            formula=roll_formula,
        )
        cnx_print_rolls.update(
            record_id=roll[0]["id"],
            fields={f"Home Wash Weight (kg)": float(data["weight"])},
        )

    return {}, 200
