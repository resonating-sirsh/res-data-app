from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
import arrow

from res.flows.make.cut_app.close_inspection.queries import (
    QUERY_MEASUREMENT_REQUEST,
    QUERY_GET_MARKER_INFO,
    UPDATE_CUT_MARKER,
    process_request_mutation,
    UPDATE_PRODUCTION_REQUEST_QUERY,
)


def handler(event, context=None):

    CLIENT = ResGraphQLClient()
    logger.info(f"event: {event}")
    record_ids = event.get("assets")[0].get("record_ids")
    for record_id in record_ids:
        try:
            logger.debug("Inside handler")
            logger.debug("Inside handler\n")

            # get measurement info by id
            get_measurement = CLIENT.query(
                QUERY_MEASUREMENT_REQUEST, {"where": {"id": {"is": record_id}}}
            )
            logger.debug(f"get_measurement: {get_measurement}\n")
            if get_measurement:

                measurement = (
                    get_measurement.get("data")
                    .get("measurementRequests")
                    .get("measurementRequests")[0]
                )

                # get marker by nameInStation
                cut_marker = CLIENT.query(
                    QUERY_GET_MARKER_INFO,
                    {"where": {"name": {"is": measurement.get("nameInStation")}}},
                )
                logger.debug(f"cut_marker: {cut_marker}\n")
                if cut_marker:
                    cut_marker = (
                        cut_marker.get("data")
                        .get("cutOneMarkers")
                        .get("cutOneMarkers")[0]
                    )
                    # approve the marker
                    close_payload = {
                        "inspectionStatus": "Approved",
                        "inspectionCompletedAt": str(
                            arrow.now("US/Eastern").isoformat()
                        ),
                    }

                    logger.debug(f"Updating Marker {close_payload}\n")

                    update_cut_marker = CLIENT.query(
                        UPDATE_CUT_MARKER,
                        {"id": cut_marker.get("id"), "input": close_payload},
                    )
                    logger.debug(f"update_cut_marker: {update_cut_marker}\n")
                    if update_cut_marker:
                        CLIENT.query(
                            process_request_mutation,
                            {
                                "requestId": measurement.get("id"),
                                "input": {
                                    "finishedAt": str(
                                        arrow.now("US/Eastern").isoformat()
                                    )
                                },
                            },
                        )
                        # mark as finished the measurement request
                        check_all_markers(measurement, CLIENT)
                    else:
                        logger.debug("error to close marker record\n")

                else:
                    # mark as finished the measurement request
                    CLIENT.query(
                        process_request_mutation,
                        {
                            "requestId": measurement.get("id"),
                            "input": {
                                "finishedAt": str(arrow.now("US/Eastern").isoformat())
                            },
                        },
                    )

                    # Approved production request
                    update_production_request(measurement, CLIENT)
        except Exception as e:
            logger.error(
                f"Error with the record_id {record_id} of cut_request table in script close_inspection"
                + str(e)
            )
            continue
    logger.debug("Done!\n")


def check_all_markers(measurement, CLIENT):
    # get all markers of this ONE
    get_all_markers = CLIENT.query(
        QUERY_GET_MARKER_INFO,
        {"where": {"name": {"hasAnyOf": [measurement.get("requestNumber")]}}},
    )
    logger.debug(f"get_all_markers: {get_all_markers}\n")

    if get_all_markers:
        get_all_markers = (
            get_all_markers.get("data").get("cutOneMarkers").get("cutOneMarkers")
        )
        count_markers = len(get_all_markers)
        count_markers_done = 0

        # check if all markers that belong to this ONE are Approved
        for markers in get_all_markers:
            logger.debug(
                f"Key: {markers.get('name')} status: {markers.get('inspectionStatus')}"
            )
            if markers.get("inspectionStatus") == "Approved":
                count_markers_done += 1

        # if all markers are Approved. Update Make ONE Production
        logger.debug(
            f"count_markers: {count_markers} == count_markers_done: {count_markers_done}"
        )
        if count_markers == count_markers_done:
            logger.debug("All Markers are Approved")
            # Approved production request
            update_production_request(measurement, CLIENT)


def update_production_request(measurement, CLIENT):
    update_payload = {
        "nodeExitInspectionStatus": "Approved",
        "cutExitStatus": "EXIT CUT",
        "nodeExitInspectionApprovedTag": "Passed with No Defects",
        "cutExitAt": str(arrow.now("US/Eastern").isoformat()),
        "nodeExitStatus": "Completed",
    }
    update_make_one = CLIENT.query(
        UPDATE_PRODUCTION_REQUEST_QUERY,
        {
            "id": measurement.get("drProductionRecordId"),
            "input": update_payload,
        },
    )
    if not update_make_one:
        logger.debug("Error updating Make ONE Production")


if __name__ == "__main__":
    context = {"name": "context"}
    event = {"record_ids": ["62615a172e06c8dcee339a6a"], "dry_run": False}
    handler(event, context)
