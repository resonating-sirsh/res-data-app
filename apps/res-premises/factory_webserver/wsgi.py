import os, sys

res_premises_path = os.path.join(os.path.expanduser("~/repos/res-data-platform/apps/res-premises/"), "factory_webserver")  # fmt: skip
sys.path.append(res_premises_path)

from flask import Flask
from services.ppu.main import app as ppu_creation
from services.rfid.main import app as tag_assignation
from services.warehouse.main import app as warehouse
from services.home_wash.main import app as home_wash
from services.sop_pdf.main import app as sop_pdf

app = Flask(__name__)

app.register_blueprint(tag_assignation, url_prefix="/rfid")
app.register_blueprint(ppu_creation, url_prefix="/ppu_creation")
app.register_blueprint(warehouse, url_prefix="/warehouse")
app.register_blueprint(home_wash, url_prefix="/home_wash")
app.register_blueprint(sop_pdf, url_prefix="/sop_pdf")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5001", debug=True)
