from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
from res.flows.make.cut_app.inspection_add_roll_resource.queries import (
    GET_CUT_MARKER,
    GET_MEASUREMENT,
    PROCESS_REQUEST_MUTATION,
)


def handler(event, context=None):
    CLIENT = ResGraphQLClient()
    record_ids = event.get("assets")[0].get("record_ids")
    for record_id in record_ids:
        try:
            logger.debug("Inside handler")
            logger.debug(f"Working with record {record_id}")

            cut_markers = CLIENT.query(
                GET_CUT_MARKER, {"where": {"id": {"is": record_id}}}
            )
            logger.debug(f"cut_markers {cut_markers}")

            cut_marker = (
                cut_markers.get("data").get("cutOneMarkers").get("cutOneMarkers")[0]
            )

            if cut_marker.get("roll") and cut_marker.get("resource"):

                get_measurement = CLIENT.query(
                    GET_MEASUREMENT,
                    {
                        "where": {
                            "nameInStation": {"is": cut_marker.get("name")},
                            "station": {"is": "CUT_MARKER"},
                        }
                    },
                )
                if get_measurement:
                    measurement_id = (
                        get_measurement.get("data")
                        .get("measurementRequests")
                        .get("measurementRequests")[0]
                        .get("id")
                    )
                    update_obj = {
                        "requestId": measurement_id,
                        "input": {
                            "cell": cut_marker.get("resource").get("name"),
                            "rollAssignation": cut_marker.get("roll").get("name"),
                        },
                    }
                    logger.debug(f"Will update measurement: {update_obj}")
                    update_measurement_request = CLIENT.query(
                        PROCESS_REQUEST_MUTATION, update_obj
                    )
                    if not update_measurement_request:
                        raise ValueError(
                            "Fail adding Roll and Resource to measurement record. Please check"
                        )
        except Exception as e:
            logger.error(
                f"Error with the record_id {record_id} of cut_request table in script inspection_add_roll_resource"
                + str(e)
            )
            continue
    logger.debug("Done\n")


if __name__ == "__main__":
    handler({"record_ids": ["recKlnlhmq3WGvYs2"]})
