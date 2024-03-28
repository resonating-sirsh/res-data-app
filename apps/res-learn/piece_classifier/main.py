"""
Res connect is a service that extends the res library so that that functions can be triggered via REST
_nudge . . .
"""

import res
from res.utils.flask import helpers as flask_helpers
from flask import Flask, jsonify, request, Response

PROCESS_NAME = "piece-classifier"


app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
flask_helpers.attach_metrics_exporter(app)

# get the model
bundle = res.learn.models.load("piece-classification/piece-model-10class-01")
class_map = bundle.classes_map
model = bundle.model
transform_image = bundle.image_transformer
model.eval()


@app.route("/predict_part", methods=["POST"])
def predict_part():
    """
    you can post examples with

    import requests

    resp = requests.post("http://localhost:5000/predict_part",
                     files={"file": open('PATH_TO_IMAGE','rb')})
    """

    def get_prediction(image_bytes):
        tensor = transform_image(image_bytes=image_bytes)
        outputs = model.forward(tensor)
        _, y_hat = outputs.max(1)
        predicted_idx = str(y_hat.item())
        return class_map[predicted_idx]

    if request.method == "POST":
        file = request.files["file"]
        img_bytes = file.read()
        class_id, class_name = get_prediction(image_bytes=img_bytes)
        return jsonify({"class_id": class_id, "class_name": class_name})


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return Response("ok", status=200)


if __name__ == "__main__":
    res.utils.secrets_client.get_secret("AIRTABLE_API_KEY")
    app.run(host="0.0.0.0")
