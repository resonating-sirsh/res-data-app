from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
import arrow
from res.flows.make.cut_app.bin_auto_assignment.queries import (
    UPDATE_CUT_REQUEST,
    GET_CUT_REQUEST,
    GET_BIN_LOCATION,
)


def handler(event, context):
    CLIENT = ResGraphQLClient()
    record_ids = event.get("assets")[0].get("record_ids")
    for record_id in record_ids:
        try:
            logger.debug("Inside handler")
            logger.debug(f"Working with record {record_id}")

            cut_requests = CLIENT.query(
                GET_CUT_REQUEST, {"where": {"id": {"is": record_id}}}
            )
            logger.debug(f"cut_requests {cut_requests}")

            cut_request = (
                cut_requests.get("data").get("cutRequests").get("cutRequests")[0]
            )

            locations = CLIENT.query(
                GET_BIN_LOCATION,
                {"where": {"canStoreOnes": {"is": True}, "factory": {"is": "STI"}}},
            )
            logger.debug(f"locations: {locations}")
            locations = (
                locations.get("data")
                .get("cutOneBinLocations")
                .get("cutOneBinLocations")
            )

            if len(locations) > 0:
                assigned_location_name = locations[0].get("Name")
                logger.debug(f"Bin Found\nAssigned to {assigned_location_name}")

                updated_cut_request = CLIENT.query(
                    UPDATE_CUT_REQUEST,
                    {
                        "id": record_id,
                        "input": {
                            "binLocationId": locations[0].get("id"),
                            "locationReservedAt": arrow.utcnow().isoformat(),
                        },
                    },
                )
                logger.debug(f"Done {updated_cut_request}")
            else:
                tags = []
                if cut_request.get("flaggedForReviewTag"):
                    for tag in cut_request.get("flaggedForReviewTag"):
                        tags.append(tag)
                tags.append("Bins Are Full")

                updated_cut_request = CLIENT.query(
                    UPDATE_CUT_REQUEST,
                    {
                        "id": record_id,
                        "input": {"isFlagForReview": True, "flaggedForReviewTag": tags},
                    },
                )
                logger.debug(f"Done {updated_cut_request}")
                raise ValueError("Bins Are Full! Please Investigate")
        except Exception as e:
            logger.warn(
                f"Error with the record_id {record_id} of cut_request table in script bin_auto_assignment"
                + str(e)
            )
            logger.incr_data_flow("cut_assignment", asset_group="any", status="FAILED")
            continue


if __name__ == "__main__":
    handler({"record_ids": ["recl7C3HIKDjaeh2L"]}, {})
