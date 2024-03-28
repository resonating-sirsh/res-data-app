import json
import jsonschema
import xmltodict
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
from res.utils.flask import helpers as flask_helpers
from flask import Flask, Response, request

app = Flask(__name__)
flask_helpers.attach_metrics_exporter(app)
flask_helpers.enable_trace_logging(app)


PROCESS_NAME = "kgateway"
VERSIONS = [
    "1.0.0",
]
INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "version": {"type": "string", "enum": VERSIONS},
        "process_name": {"type": "string"},
        "topic": {"type": "string"},
        "data": {"type": ["object", "string"]},
        "data_format": {"type": "string", "enum": ["json", "xml"]},
        "topic_format": {"type": "string", "enum": ["json", "avro"]},
    },
    "required": ["version", "process_name", "topic", "data"],
}
ERROR_TOPIC = "res_infrastructure.kgateway.errors"


def validateRequest(request_data):
    # Parse JSON
    try:
        data_dict = json.loads(
            request_data, strict=False
        )  # strict = False allow for escaped char....
    except ValueError as e:
        return str(e), None

    # Validate schema
    try:
        jsonschema.validate(instance=data_dict, schema=INPUT_SCHEMA)
    except jsonschema.exceptions.ValidationError as e:
        return str(e), None

    return None, data_dict


def processData(request_dict):
    # Default is JSON
    if "data_format" not in request_dict or request_dict["data_format"] == "json":
        # Make sure data is a dict
        if type(request_dict["data"]) is not dict:
            return "Error: data must be an object, not a JSON string", None
        return None, request_dict["data"]

    # Handle XML
    if request_dict["data_format"] == "xml":
        try:
            data = xmltodict.parse(request_dict["data"], dict_constructor=dict)
            return None, data
        except Exception as e:
            return "Error parsing XML: {}".format(str(e)), None

    # Should never reach here due to enum in validation above... ...
    return None, None


def sendBadData(kafka_client, bad_data, topic, process_name, version):
    logger.warning("Bad data found", bad_data=bad_data)
    error, schema = kafka_client.get_schema(ERROR_TOPIC)
    if error != None:
        logger.warning("Error getting the {} schema!".format(ERROR_TOPIC))
    else:
        stringified = json.dumps(bad_data, indent=2)
        data = {
            "version": version,
            "process_name": process_name,
            "topic": topic,
            "invalid_payload": stringified,
        }
        with ResKafkaProducer(kafka_client, ERROR_TOPIC, schema) as producer:
            error = producer.produce(data)
        if error != True:
            logger.warning("Error sending bad data to Kafka!", data=data, error=error)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return Response("ok", status=200)


@app.route("/kgateway/submitevent", methods=["GET", "POST"])
def submit_event():
    # steps:
    # 0. inital validation to make sure payload parses and conforms to schema
    # 1. convert XML to JSON if needed
    # 2. retrieve AVRO schema based on topic name (will likely need caching at some point)
    # 3. validate JSON by converting to AVRO, unless topic_format is json
    # 4. push raw AVRO bytes / json to Kafka topic

    if request.method != "POST":
        logger.warn("Invalid request sent to kgateway: non-POST request")
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )
    err, request_dict = validateRequest(request.data)
    if err != None:
        logger.warn(
            "Invalid request sent to kgateway: failed validation: {}".format(err)
        )
        return Response(
            "{'error':'Request is invalid: " + err + "'}",
            status=400,
            mimetype="application/json",
        )

    err, data = processData(request_dict)
    if err != None:
        logger.warn(
            "Invalid request sent to kgateway: error processing data: {}".format(err)
        )
        return Response(
            "{'error':'Error extracting data: " + err + "'}",
            status=400,
            mimetype="application/json",
        )

    topic = request_dict["topic"]
    kafka_client = ResKafkaClient(
        "{}-{}".format(PROCESS_NAME, request_dict["process_name"]),
    )
    topic_format = request_dict.get("topic_format", "avro")
    if topic_format == "avro":
        error, schema = kafka_client.get_schema(topic)
        if error != None:
            logger.warning("Invalid request sent to kgateway", error=error)
            return Response(
                "Error getting schema: "
                + error
                + ". You may need to add a schema and topic to Kafka. Did you add -value to the end of your topic?",
                status=400,
                mimetype="application/json",
            )
        with ResKafkaProducer(kafka_client, topic, schema) as producer:
            error = producer.produce(data)
    else:
        with ResKafkaProducer(kafka_client, topic, message_type="json") as producer:
            error = producer.produce(data)

    if error != True:
        sendBadData(
            kafka_client,
            data,
            topic,
            request_dict["process_name"],
            request_dict["version"],
        )
        return Response(
            "{'error':'Error sending to Kafka: " + error + "'}",
            status=500,
            mimetype="application/json",
        )
    return Response("success", status=200, mimetype="application/json")


if __name__ == "__main__":
    app.run(host="0.0.0.0")
