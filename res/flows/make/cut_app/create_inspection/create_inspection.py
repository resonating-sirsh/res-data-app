from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
import arrow

from res.flows.make.cut_app.create_inspection.queries import (
    QUERY_GET_MARKER_INFO,
    PROCESS_REQUEST_MUTATION,
    UPDATE_CUT_MARKER,
    CREATE_MEASUREMENT,
    CREATE_ONE_MEASUREMENT,
    DELETE_MEASUREMENT,
    QUERY_MEASUREMENT_REQUEST,
)


def handler(event, context=None):
    CLIENT = ResGraphQLClient()
    logger.info(f"event: {event}")
    record_ids = event.get("assets")[0].get("record_ids")
    for record_id in record_ids:
        try:
            logger.debug("Inside handler")
            station = "CUT_MARKER"

            get_marker_info = CLIENT.query(
                QUERY_GET_MARKER_INFO,
                {
                    "where": {
                        "id": {"is": record_id},
                    }
                },
                paginate=True,
            )
            logger.debug(f"get_marker_info: {get_marker_info}")

            if get_marker_info and get_marker_info.get("data").get("cutOneMarkers"):
                marker_info = (
                    get_marker_info.get("data")
                    .get("cutOneMarkers")
                    .get("cutOneMarkers")[0]
                )
                one = marker_info.get("makeOneProductionRequest")

                is_measurement_duplicate = verify_duplicate_measurement(
                    station, marker_info.get("name"), "Station Exit", CLIENT
                )
                logger.debug(f"is_measurement_duplicate: {is_measurement_duplicate}")

                if not is_measurement_duplicate:

                    measurement_request = create_measurement_request(
                        one, marker_info, CLIENT
                    )

                    one_measurement_id = create_one_measurement_request(
                        measurement_request, marker_info.get("name"), CLIENT
                    )

                    payload = {
                        "unitMeasurementsIds": one_measurement_id,
                        "unitMeasurementCount": 1,
                    }
                    CLIENT.query(
                        PROCESS_REQUEST_MUTATION,
                        {"requestId": measurement_request.get("id"), "input": payload},
                    )

                    CLIENT.query(
                        UPDATE_CUT_MARKER,
                        {
                            "id": record_id,
                            "input": {
                                "inspectionStatus": "Pending",
                                "inspectionCreatedAt": str(
                                    arrow.now("US/Eastern").isoformat()
                                ),
                                "isInspectionReady": False,
                            },
                        },
                    )
        except Exception as e:
            logger.error(
                f"Error with the record_id {record_id} in script create_inspection"
                + str(e)
            )
            continue
    logger.debug("Done\n")


def create_measurement_request(one, marker_info, CLIENT):
    request_dict = {
        "fingerprint": one.get("sku"),
        "requestNumber": int(one.get("orderNumber")),
        "cell": marker_info.get("resourceAssignment"),
        "station": "CUT_MARKER",
        "inspectionType": "Station Exit",
        "resource": "Current Node Resource",
        "drProductionRecordId": one.get("id"),
        "linkToBody": one.get("body").get("code"),
        "finishedAt": None,
        "status": "to do",
        "name": "M - " + one.get("orderNumber"),
        "bodyCode": one.get("body").get("code"),
        "markerRank": marker_info.get("Rank"),
        "rollAssignation": marker_info.get("rollAssignmentName"),
        "nameInStation": str(marker_info.get("name")),
    }

    measurement_request = None

    logger.debug("Creating Request")
    logger.debug(request_dict)
    create_cut_measurement = CLIENT.query(CREATE_MEASUREMENT, {"input": request_dict})
    if create_cut_measurement:
        logger.debug(f"Save measurement request {create_cut_measurement}")
        measurement_request = (
            create_cut_measurement.get("data")
            .get("createMeasurementRequest")
            .get("measurementRequest")
        )
        logger.debug(measurement_request)
    else:
        logger.debug("Error create measurement request")
    return measurement_request


def create_one_measurement_request(measurement_request, cut_marker_number, CLIENT):
    fields_to_insert = {
        "requestNumber": int(measurement_request.get("requestNumber")),
        "inspectionType": "Station Exit",
        "measurementRequestId": measurement_request.get("id"),
        "station": "CUT_MARKER",
        "nameInStation": str(cut_marker_number),
        "accepted": "false",
        "rejected": "false",
        "severity": "0",
    }
    logger.debug(fields_to_insert)
    one_measurement_id = None
    response_graph = CLIENT.query(CREATE_ONE_MEASUREMENT, {"input": fields_to_insert})
    if response_graph:
        one_measurement = (
            response_graph.get("data").get("createOneMeasurement").get("oneMeasurement")
        )
        one_measurement_id = one_measurement.get("id")
    return one_measurement_id


def verify_duplicate_measurement(station, cut_marker_number, inspection_type, CLIENT):
    result_duplicate = False
    where = {
        "where": {
            "nameInStation": {"is": str(cut_marker_number)},
            "inspectionType": {"is": inspection_type},
            "station": {"is": station},
        }
    }

    logger.debug(where)
    graph_response = CLIENT.query(QUERY_MEASUREMENT_REQUEST, where)
    if graph_response:
        measurement_table = (
            graph_response.get("data")
            .get("measurementRequests")
            .get("measurementRequests")
        )
        logger.debug(f" COUNT: {len(measurement_table)} INFO: {measurement_table}")
        if len(measurement_table) > 0:
            for measurement in measurement_table:
                delete_measurement = CLIENT.query(
                    DELETE_MEASUREMENT, {"id": measurement.get("id")}
                )
                logger.debug(f"delete_measurement: {delete_measurement}")
    return result_duplicate
