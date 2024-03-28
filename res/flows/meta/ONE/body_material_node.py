"""
This node processes anything related to the body material contract update
Costs:
 for costs we are migrating from a style level costing to a BMS (body material size costing. However we still process the style data anyway in the style node
 there are two types of reto cleanups, we need to use the latest costing logic with all nesting stats and sometimes these are updated - we update any old BMS that do not have the latest version in the reducer by requesting style costs for them in the reducer
 If we have representative styles in the right version, we can then send a kafka message to this node with mode BMS_COSTS - the generator will accept these requests from a node or otherwise
  - the generator also looks for styles in the right version that we have not saved in the BMS table
 The handler will then save the BMS cost to the database and relay to kafka

"""
import res
from schemas.pydantic.meta import BodyPieceMaterialRequest, BodyPieceMaterialResponse
from res.flows.meta.ONE.queries import get_style_body_cost_data, get_bms_checksum
from res.flows.api import FlowAPI
import pandas as pd

REQUEST_TOPIC = "res_meta.dxa.body_piece_material_requests"
RESPONSE_TOPIC = "res_meta.dxa.body_piece_material_updates"


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def reducer(event, context={}):
    """
    checks historic data where there is no latest version statistics in the BMC and updates BMCs by requesting costs for the delegate
    will probably do this offline
    """

    pass


def _get_jobs():
    """
    lookup the BMS that we have
    read from a stored CSV file of jobs and drop the ids we have
    the meta one should be able to generate this thing
    """
    return pd.DataFrame([])


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def generator(event, context={}):
    """
    consumes from kafka - deduplicating on the BMS id

    <id>
    <body_code>
    <body_version>
    <size_code>
    <mode>


    if no work, will look back for work to migrate missing BOM ids
    """
    with res.flows.FlowContext(event, context) as fc:
        # consume from the kafka topic for this guy using the right decorator
        # assets = fc.assets
        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            assets = fc.assets_dataframe
        else:
            res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")
            kafka = fc.connectors["kafka"]
            assets = kafka[REQUEST_TOPIC].consume(give_up_after_records=100)

            res.utils.logger.info(
                f"Consumed batch size {len(assets)} from topic {REQUEST_TOPIC}"
            )

        if assets is None or len(assets) == 0:
            res.utils.logger.info(
                f"Nobody gave me work, ill try to find some of my own because im industrious like that..."
            )

            # returns dataframe
            assets = _get_jobs()
            """
            the job here is a retro repair for migrating BMC->BMS cost data - here we make the requests we need and the handler will save and relay to kafka
            """

        if assets is not None and len(assets):
            assets = assets.drop_duplicates(subset="id")
        # many BMCs map onto one id so we can funnel them all in here and drop the dupes

        assets = fc.asset_list_to_payload(
            assets.to_dict("records"),
            metadata_provider=lambda asset: {"memory": "1G"},
        )

        res.utils.logger.info(
            f"returning the work of type {type(assets)} from the generator for session key {fc.key}"
        )

        return assets


def _parse_response(a, strong_type=True):
    s = a["meta_style_sizes"][0]
    b = a["meta_bodies"][0]
    pieces = [p["piece"] for p in s["style_size_pieces"]]
    pieces = {p["body_piece"]["piece_key"]: p["material_code"] for p in pieces}

    d = {
        # the sized hash is generated from the pieces hash and the size code - the thing comes from the style that was already saved
        "id": res.utils.uuid_str_from_dict(
            {
                "style_hash": s["style"]["piece_material_hash"],
                "size_code": s["size_code"],
            }
        ),
        "style_pieces_hash": s["style"]["piece_material_hash"],
        "number_of_body_pieces": b["body_piece_count_checksum"],
        "number_of_printable_pieces": len(pieces),
        "piece_map": pieces,
        "nesting_statistics": s["material_usage_statistics"],
        "size_code": s["size_code"],
        "number_of_materials": len(s["material_usage_statistics"]),
        "body_code": b["body_code"],
        "body_version": b["version"],
        "sewing_time": b["estimated_sewing_time"],
        "trim_costs": b["trim_costs"],
    }
    if strong_type:
        return BodyPieceMaterialRequest(**d)
    return d


def resend_to_kafka(event):
    return handle_event(event, force_send=True, use_kgateway=True)


def handle_event(event, force_send=False, use_kgateway=False):
    """
    event = {
      'reference_style_size_id': 'd4a83f34-8348-c5d9-1985-a7f0a78899a9',
      'body_code':'JR-3023',
      'body_version':3,
      'mode' : ??
    }
    """
    reference_style_size_id = event["reference_style_size_id"]
    body_code = event["body_code"]
    body_version = event["body_version"]
    size_code = event["size_code"]

    mode = event.get("mode") or "default"

    """
    the default mode is all we have for now
    """
    if mode == "default":
        res.utils.logger.info(
            f"Mode default - Processing asset {event} - will load the style size cost data and the body cost data and process"
        )
        a = get_style_body_cost_data(
            reference_style_size_id, body_code=body_code, body_version=body_version
        )
        res.utils.logger.info("Parsing response")
        bm = _parse_response(a)

        """
        lock this if we are doing in parallel - check if the data are already saved but acquire the lock
        """
        # with res.flows.lock(f"{bm.id}_locks_v0"):
        # normally we check if we need to resend
        if not force_send:
            data = get_bms_checksum(id=bm.id)
            if len(data):
                if data[0]["checksum"] == bm.checksum:
                    res.utils.logger.info(f"No change we have {data}")
                    return None

        res.utils.logger.info("Saving to api")
        api = FlowAPI(
            BodyPieceMaterialRequest,  # , response_type=BodyPieceMaterialResponse
            postgres="dont care",
        )
        res.utils.logger.info("...")

        # we could use flow api to relay the response but its not needed as yet and this avoids running the same thing back
        r = api.update_hasura(bm)

        # check the response contract for the response topic
        bmr = BodyPieceMaterialResponse(**bm.dict())
        payload = bmr.dict()
        payload["created_at"] = res.utils.dates.utc_now_iso_string()
        # we know what it is but the question is why it was not save on the style at this stage
        # payload["style_pieces_hash"] = payload.get("style_pieces_hash") or event.get(
        #     "style_pieces_hash"
        # )
        # this does not need to be in the lock but no harm keeping this transaction because if the hasura update fails then the next guy can relay
        res.connectors.load("kafka")[RESPONSE_TOPIC].publish(
            payload, use_kgateway=use_kgateway
        )

    return r


@res.flows.flow_node_attributes(
    memory="2Gi",
)
def handler(event, context={}):
    """
    takes requests to update the BMS data if it is new or modified
    - for new only requests
    - sends a kafka message about the change
    """

    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.decompressed_assets:
            handle_event(asset)

    return {}
