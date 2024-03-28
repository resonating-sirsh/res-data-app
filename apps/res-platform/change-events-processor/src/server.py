import json
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask
from flask import request

# _nudge

from src import processor

logger.info("Setting up flask app.................")
app = ResFlask(
    __name__,
)


@app.route("/res_create/asset", methods=["POST"])
def process_platform_event():
    try:
        e = json.loads(request.data)
        resp = processor.handle_create_asset_event(e)
        if resp is not None and not resp.get("success"):
            return {"message": resp.get("message")}, 400
        else:
            return {"success": True}, 200
    except Exception as e:
        logger.error("Error processing create asset event", exception=e)
        return {"message": str(e)}, 500


@app.route("/job/status_update", methods=["POST"])
def handle_status_update():
    try:
        e = json.loads(request.data)
        resp = processor.handle_status_update(e)
        if resp is not None and not resp.get("success"):
            return {"message": resp.get("message")}, 400
        else:
            return {"success": True}, 200
    except Exception as e:
        logger.error("Error processing status update event", exception=e)
        return {"message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0")
