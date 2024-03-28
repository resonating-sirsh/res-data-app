"""
the style meta one node processes requests for any style update from create mode to updates to colors and materials
the meta one is regenerated and gates are re-run
sometimes we are placing color for the first time or replacing color and the meta one will only check the color files exist in the right location

see docs/notebooks/nodes/meta/NewMetaOneNode.ipynb which should work as an integration test on the node

like all nodes we receive requests on a kafka queue that processes jobs on the queue in parallel 
it is possible to group requests such as multiple size requests for the same queue, split out the sizes and then collect the results in the reducer to update a queue 
the handler in the node just saves the result and the results are cached per size in redis for the queue id and then in the reducer we relay status to the queue e.g. for airtable users

much of this is an excercise in the typing system
- kafka messages must map to the pydantic types
- the node takes a validated type from the flow context and processes it
- the node returns a response object that could be saved to airtable or can be summarized and sent to airtable in the reducer
- we must therefore test that the request goes from kafka via the mutation to hasura and that any response object can be created in airtable

"""

from res.flows.api.core.node import FlowApiNode
from schemas.pydantic.meta import MetaOneRequest, MetaOneResponse
from res.utils import uuid_str_from_dict
import res
from res.flows.meta.pieces import PieceName
from res.flows.meta.ONE.meta_one import MetaOne
from res.flows.api.core.node import FlowApiNode, NodeException
from gql.transport.exceptions import TransportQueryError
import itertools
import traceback
from res.flows.api.core.node import SHARED_STATE_KEY
from res.flows.dxa.styles import queries, helpers
from res.flows.meta.body.unpack_asset_bundle.body_db_sync import (
    sync_body_to_db_by_versioned_body,
)
from res.utils.env import RES_ENV
from res.flows.api import FlowAPI
from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE
from res.flows.meta.ONE.controller import MetaOnePieceManager
from res.media.images.geometry import LinearRing, to_geojson

# using the apply color queue base as the style nodes queue base for now
AIRTABLE_QUEUE_BASE_ID = (
    AIRTABLE_ONE_PLATFORM_BASE if RES_ENV == "development" else "appqtN4USHTmyC6Dv"
)
REQUEST_TOPIC = "res_meta.dxa.style_pieces_update_requests"
BMS_REQUEST_TOPIC = "res_meta.dxa.body_piece_material_requests"


class StyleNodeDataException(NodeException):
    def __init__(self, *args: object, data, transport_error, response=None) -> None:
        self._data = data
        self._tex = (transport_error,)
        self._explanation = self.db_error_explainer(data, transport_error)
        self._response = response
        super().__init__(self._explanation["message"], *args)

    def __repr__(self) -> str:
        return self._explanation["message"]

    @staticmethod
    def db_error_explainer(data, error):
        id_data = {"error": error, "message": str(error)}
        if "pieces_body_piece_id_fkey" in str(error):
            pids = [p["id"] for p in data["pieces"]]
            bid = MetaOneRequestIds._body_id_from_body_code_and_version(
                data["body_code"], data["body_version"]
            )

            id_data = {
                "pids": pids,
                "bid": bid,
                "size_code": data["size_code"],
                "contract": "BODY_SAVED",
                "message": f"Expect the piece ids on the styled generated from the body id {bid}  and pieces keys to exist but they do not - check the body {data['body_code']}-V{data['body_version']}  and its pieces have been saved for size {data['size_code']}",
                "error": error,
            }

        # there is a state i dont fully understand where we had style pieces but not the right ones and needed to re-import. will keep an eye on it for next time
        if "style_size_pieces_style_size_id_fkey" in str(error):
            id_data = {
                "style_size_id": data["id"],
                "size_code": data["size_code"],
                "contract": "REGISTER_STYLE",
                "message": f"The pieces cannot be created because the style/size named '{data.get('name', data.get('style_name'))}' ({data.get('sku')}) and body '{data.get('body_code')}: Size {data.get('size_code')}' does not exist - this will need to be registered",
                "error": error,
            }

        return id_data


class MetaOneRequestIds:
    """
    cooperates with the Pydantic Type MetaOneRequest to generate our server side ids based on key conventions

    we should deprecate the dxa.styles helpers which were temporary - the body update should be brought into node mode
    """

    def three_part_sku(s):
        return f" ".join([i.strip() for i in s.split(" ")[:3]])

    def _style_id_from_name(name, body_code):
        """
        Convention for generating an id to make names unique and a business handle to the style
        A brand should have a globally unique name for their style - but we allow per body for now
        (even though it would not really make sense to sell the same thing as two bodies?)

        `Name` is like "Vertical Dart Face Mask - Gold Leaves in Bru..." and `body_code` is like "CC-8068"

        """
        return uuid_str_from_dict(
            {"name": name.lstrip().rstrip(), "body_code": body_code}
        )

    def _style_size_id_from_style_id_and_size(sid, size_code):
        return res.utils.uuid_str_from_dict({"style_id": sid, "size_code": size_code})

    def _piece_id_from_style_size_and_piece_key(style_size_id, key):
        return res.utils.uuid_str_from_dict(
            {"key": key, "style_size_id": style_size_id}
        )

    def _body_piece_id_from_body_id_key_size(bid, key, size_code):
        d = uuid_str_from_dict(
            {
                "body_id": bid,
                "piece_key": key,
                "size": size_code,
                "profile": "default",
            }
        )
        return d

    def _body_id_from_body_code_and_version(body_code, body_version):
        return uuid_str_from_dict(
            {
                "code": body_code,
                "version": int(float(body_version)),
                "profile": "default",
            }
        )

    def get_style_size_history(garment_id, pieces):
        """ """
        return [
            {
                # refactor but this only happens here - never generate this uuid normally in non shared functions
                "id": res.utils.uuid_str_from_dict(
                    {
                        "style_size_id": garment_id,
                        "piece_id": p,
                    }
                ),
                "style_size_id": garment_id,
                "piece_id": p,
            }
            for p in [p["id"] for p in pieces]
        ]

    def build_ids(asset, style_id, body_id):
        """
        is there something on the input flow to do some of these inline - you still need the ids passed in from context
        """
        asset["id"] = MetaOneRequestIds._style_size_id_from_style_id_and_size(
            style_id, asset["size_code"]
        )
        for p in asset["pieces"]:
            # TODO: have not a found a nice way to do this - we want it but we dont want to have it on the database because the FK resolves what we need on the body
            key = p.pop("key")
            # print(key, asset["size_code"], body_id)
            assert (
                f"{asset['body_code']}-V{asset['body_version']}" in key
            ), f"The piece key {key} must be fully qualified with body code and version prefix"
            p["id"] = MetaOneRequestIds._piece_id_from_style_size_and_piece_key(
                asset["id"], key
            )
            p["body_piece_id"] = MetaOneRequestIds._body_piece_id_from_body_id_key_size(
                body_id, key, asset["size_code"]
            )

        asset["metadata"]["applied_body_id"] = body_id
        return asset


def _fix_sku(s):
    if not s:
        return s
    if s[2] != "-":
        s = f"{s[:2]}-{s[2:]}"
    return s


def ensure_dependencies(
    style_sku, body_code, body_version, size_code=None, brand_code=None, force=True
):
    """
    normally we have on-boarded the body and style before we add the color pieces.... but you never know ..
    """
    from dateparser import parse

    H = queries.get_body_header(body_code, body_version)["meta_bodies"]
    if not len(H):
        res.utils.logger.info(
            f"We need to import the body {body_code}-V{body_version} which does not exist yet"
        )
        sync_body_to_db_by_versioned_body(body_code, body_version)

    # actually if there is a new size we are in trouble

    s = sorted(
        queries.get_style_header_with_sizes(style_sku)["meta_styles"],
        key=lambda x: parse(x["modified_at"]),
    )
    sizes_in_header = []
    print_type = None
    contracts_failing = []
    given_saved_name = None
    if len(s):
        s = s[-1]
        print_type = s.get("metadata", {}).get("print_type")
        contracts_failing = s.get("contracts_failing")
        active_sizes = [
            ss["size_code"] for ss in s["style_sizes"] if ss["status"] == "Active"
        ]
        given_saved_name = s["name"].lstrip().lstrip()
        sizes_in_header = [ss["size_code"] for ss in s["style_sizes"]]

    # if "BODY_SAVED" in contracts_failing:
    #     res.utils.logger.info(
    #         f"We need to re-import the body {body_code}-V{body_version}because there was a constraint violation"
    #     )
    #     sync_body_to_db_by_versioned_body(body_code, body_version)

    payloads = MetaOneNode.get_style_as_request(style_sku)

    requested_name = payloads[0].style_name.lstrip().rstrip()

    sizes_required = [p.size_code for p in payloads]
    missing = set(sizes_required) - set(sizes_in_header)

    # adding the register style repair option; if we previously failed this contract we can try to register and update - later we should remove the contract (subtle)
    # problem here is to get caught in states
    if (
        len(missing) > 0
        or print_type is None
        or "REGISTER_STYLE" in (contracts_failing or [])
        or given_saved_name != requested_name
        or force
    ):
        payload = payloads[0].dict()
        res.utils.logger.info(
            f"We need to import the style header which does not exist yet for some sizes i.e. {style_sku}, {missing} or the print type and other contracts [{contracts_failing}] are not passing on the header with metadata ... {payload.get('metadata', {})}"
        )
        if given_saved_name != requested_name:
            res.utils.logger.warn(
                f"given name and saved name differ - saving update for that reason: {given_saved_name,requested_name}"
            )
        # we can use anything to update because in this mode we just upsert all sizes
        # changed to not reset the status if its already set

        payload["offset_size_inches"] = max(
            [p["offset_size_inches"] for p in payload["pieces"]]
        )
        helpers.style_model_upsert_style_header(payload)
    else:
        res.utils.logger.info(
            f"Checked contracts for {s.get('metadata', {})} and sizes missing is {missing} of required {sizes_required}"
        )


def generator(event, context={}):
    """
    Generate the events from kafka topics or similar inputs
    generators can be used to do batch airtable lookups and things like that rather than making the calls in the mapped parallel nodes


    TODO: support expand sizes on the style pieces change event
    """

    def decorate_assets(assets):
        """
        clean up the request for the meta one node
        add sample size if not already added in the accounting format
        add material properties
        add node discriminator for loading the right asset type in the queue i.e. Body or Style

        piece_name_mapping: is the contract for pieces in the style piece header and in the meta one request
        """

        def pop_queue_id(d):
            if "metadata" not in d:
                d["metadata"] = {}
            if "id" in d:
                qid = d.pop("id")
                # the queue.id will be used by system but dropped py pydantic so we move to metadata too
                d["metadata"]["queue_id"] = d["queue.id"] = qid
            return d

        def _qualify_piece_keys(d):
            body_code = d["body_code"]
            body_version = int(d["body_version"])
            prefix = f"{body_code}-V{body_version}"
            for p in d["pieces"]:
                key = p["key"]
                if prefix not in key:
                    p["key"] = f"{prefix}-{key}"

            return d

        mat_props = {}

        def mod_a(a):
            # default with back compatibility on the old meta one requests
            a["queue.key"] = a.get("style_sku", _fix_sku(a.get("unit_key")))
            a["res_entity_type"] = "meta.MetaOneRequest"
            a["name"] = a.get("name", a.get("style_name"))

            a = pop_queue_id(a)

            a = _qualify_piece_keys(a)

            for p in a["pieces"]:
                p["offset_size_inches"] = (
                    mat_props.get(p["material_code"])
                    if "offset_size_inches" not in p
                    else p["offset_size_inches"]
                )
            return a

        # only use the create mode for now until
        assets = [
            mod_a(a)
            for a in assets
            if str(a.get("mode", "create_meta_one")).lower()
            in [
                "null",
                "none",
                "create_meta_one",
                "update_meta_one",
                "costs",
                "material_swap",
            ]
        ]
        return assets

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
            # while True:
            #     res.utils.logger.info("flushing")
            assets = kafka[REQUEST_TOPIC].consume(give_up_after_records=100)

            res.utils.logger.info(
                f"Consumed batch size {len(assets)} from topic {REQUEST_TOPIC}"
            )

        # metadata provider adds a memory attribute for the sample size otherwise we are doing simple dB inserts
        metadata_provider = lambda asset: (
            {
                "memory": "1G",
                "asset_key": asset.get("sku"),
                "sample_size_test": False,
            }
            if asset.get("sample_size") != asset.get("size_code")
            else {
                "memory": "50G",
                "disk": "5G",
                "asset_key": asset.get("sku"),
                "sample_size_test": True,
            }
        )
        if len(assets):
            _chk = assets.drop_duplicates(subset=["style_sku", "body_version"])
            for record in _chk.to_dict("records"):
                res.utils.logger.info(
                    f"ensuring dependencies for {record['style_sku']}..."
                )
                try:
                    ensure_dependencies(
                        record["style_sku"],
                        record["body_code"],
                        record["body_version"],
                        record["sample_size"],
                        brand_code=record["metadata"].get("brand_code"),
                    )
                except Exception as ex:
                    res.utils.logger.warn(
                        f"Failed to ensure deps for record {record['style_sku']} - {traceback.format_exc()}"
                    )

            assets = decorate_assets(assets.to_dict("records"))
            fc.save_queue_ids({a["queue.id"]: a["queue.key"] for a in assets})
            assets = fc.asset_list_to_payload(
                assets,
                compress=True,
                parallel_set_id="queue.id",
                metadata_provider=metadata_provider,
            )

        else:
            return []

        res.utils.logger.info(
            f"returning the work of type {type(assets)} from the generator for session key {fc.key}"
        )

        return assets


def _pieces_images_exist(pieces):
    try:
        for p in pieces:
            if res.connectors.s3.exists(p["base_image_uri"]):
                return False
        return True
    except Exception as ex:
        res.utils.logger.warn(f"Fail in piece image check {repr(ex)}")
        return False


def _m1_to_old_queue(my_status, rid):
    from res.flows.meta.ONE.contract import ContractVariables

    """
    cvs
    """
    cv = ContractVariables(base_id="appqtN4USHTmyC6Dv")
    s3 = res.connectors.load("s3")
    tab = res.connectors.load("airtable")["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]

    contracts_vars = [cv.try_get(c) for c in my_status.contracts_failed]
    contracts_vars = [c.record_id for c in contracts_vars if c is not None]

    try:
        res.utils.logger.info(f"Updating the apply color queue for rid {rid}")
        tab.update_record(
            {
                "record_id": rid,
                "Meta.ONE Sizes Ready": my_status.sizes,
                "Meta ONE Contract Variables": contracts_vars,
                "Meta.ONE Piece Preview": [
                    {"url": s3.generate_presigned_url(f)}
                    for f in my_status.printable_pieces
                ],
                "Meta.ONE Validation Details": my_status.trace_log,
                "Meta.ONE Piece Preview Updated": res.utils.dates.utc_now_iso_string(),
            }
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"Failing to update the apply color request queue {repr(ex)}"
        )


def swap_event_handler(event):
    """
    When we receive a material swap for a SQU we can refresh
    WE could also send an event to DXA
    """

    sku = event.get("new_sku")
    if "-" != sku[2]:
        sku = f"{sku[:2]}-{sku[2:]}"
    """
    request a refresh of the meta one
    """
    # there is no point doing this in general if we have properly queue the asset
    # but if the asset exists but has not been migrated at meta one we can do this
    # MetaOneNode.refresh(sku, mode="material_swap")
    return event


def process_user_queue(last_run_time_delta_mins=None):
    """
    We are flushing the json on change rows to a kafka topic for metrics
    We are sometimes lifting contracts from the meta one
    """
    from res.flows.meta.ONE.queries import (
        _publish_events,
        publish_apply_color_queue_changes_as_model,
    )

    try:
        res.utils.logger.info("Processing queue changes...")
        watermark = res.flows.watermark.get("meta.one.style_node")

        if watermark:
            res.utils.logger.info(f"found a process watermark {watermark}")

        else:
            watermark = res.utils.dates.utc_minutes_ago(10)
            res.utils.logger.info(
                f"choosing a default watermark date from params {watermark}"
            )

        # determine a value based on a default or cached watermark
        _last_run_time_delta_mins = (
            res.utils.dates.utc_now() - watermark
        ).total_seconds() / 60

        # use the one passed in for testing or the cached
        last_run_time_delta_mins = last_run_time_delta_mins or _last_run_time_delta_mins
        if last_run_time_delta_mins == -1:
            res.utils.logger.info("Disabled watermark - fetch everything")
            last_run_time_delta_mins = None
            watermark = None

        df = MetaOneNode.get_apply_color_queue_changes(
            window_minutes=last_run_time_delta_mins
        )

        # this is specifically the top level exit date
        res.utils.logger.info(
            f"There are {len(df)} events in window {last_run_time_delta_mins} minutes"
        )

        if len(df) == 0:
            return

        topic = res.connectors.load("kafka")["res_meta.dxa.style_status_updates"]

        _publish_events(
            df,
            topic=topic,
            watermark=watermark,
            key="style_code",
            process="apply-color-queue",
        )

        # try:
        #     res.utils.logger.info(f"Publishing queue")
        #     publish_apply_color_queue_changes_as_model(df)
        #     res.utils.logger.info(f"Published queue")
        # except Exception as ex:
        #     res.utils.logger.info(f"Publish failed {ex}")

        res.flows.watermark.set("meta.one.style_node")

        # TODO add back the DXA EXIT TIME TOP LEVEL (not using watermark jere just forcing it)
        completed_df = df[df["exitTime"].notnull()]
        if len(completed_df) > 0:
            skus = list(completed_df["Style Code"].unique())
            res.utils.logger.info(
                f"Messages sent. Updating the contracts on the exiting styles. {len(skus)} styles exited the queue in the time window."
            )

            # remove contracts from the sku because it cannot exit DXA unless someone approved it (body contracts may still exist)
            queries.update_contracts_failing_by_skus(
                skus, contracts_failed=[], merge_set=False
            )

        res.utils.logger.info(f"Flow changes processed")

        return df
    except:
        res.utils.logger.warn("Failed update acq")
        res.utils.logger.warn(traceback.format_exc())


def handle_invalidations_task(event, context={}):
    """
    we consume from kafka to update the meta one node status with failed contracts at body or style
    maybe we send body invalidations to the body based on some rule rather that updating directly here for record keeping
    """
    INVALIDATION_REQUEST_TOPIC = "res_meta.dxa.style_invalidation_requests"
    kafka = res.connector.load("kafka")[INVALIDATION_REQUEST_TOPIC]
    records = kafka.consume()

    # consumes 100 requests by default on e.g. 5 minute interval
    for record in records.to_dict("records"):
        sample_size_sku = record["sku"]
        body_version = record["body_version"]
        contracts_failing = record["contracts_failing"]

        try:
            MetaOneNode.upsert_status_history(
                sku=sample_size_sku,
                body_version=body_version,
                contracts_failed=contracts_failing,
            )
        except Exception as ex:
            res.utils.logger.warning(
                f"Fatal error actually, did not invalidate something"
            )


def reducer(event, context={}):
    """ """

    # use the dev base or route to the apply color queue in prod
    res.connectors.load("airtable")[AIRTABLE_QUEUE_BASE_ID].reload_cache()
    api = FlowAPI(MetaOneResponse, base_id=AIRTABLE_QUEUE_BASE_ID)

    with res.flows.FlowContext(event, context) as fc:
        for rid, key in fc.restore_queue_ids().items():
            res.utils.logger.info(
                f"Will use state key {SHARED_STATE_KEY} to restore keys"
            )
            status = fc.get_shared_cache_for_set(rid)[SHARED_STATE_KEY] or {}
            keys = list(status.keys())
            defects = list(set(itertools.chain(*(status.values()))))
            # could this be a body meta one response
            res.utils.logger.info(f"{key} keys: {keys}")
            res.utils.logger.info(f"{key} failed_contracts: {defects}")
            # query the database for all style headers -> are the active and what are the contracts failed combined
            # we should save contracts failing for this run maybe but we assume preview sample saved and now we are just compiling status
            # its interesting to consider if the style already has some data but we failed to sync with the latest vs we have nothing at all
            # the contracts failing should be sent to the style header for review
            queries.update_contracts_failing_by_sku(key, contracts_failed=defects)
            m1 = MetaOne.for_sample_size(key)
            log_dir = fc.log_path
            # updating twice is a bit inefficient i.e. we did it in the mapper and now we are collecting for the set. we can skip it in the mapper maybe
            if m1:  # this will always exist if and when the sample size is processed
                my_status = m1.status_at_node(trace_log=log_dir)
                api.relay(my_status)
                if rid[:3] == "rec":
                    _m1_to_old_queue(my_status, rid)
            # legacy: use the rid for back compat save to apply color queue: when we finally switch over consider this but try without first
            # r = post_previews(m1, rid, contracts_failed=defects) - only if the rid is an airtable one not for the ones we run
            # TODO: if contracts call lorelays flag for review end point
        process_user_queue()
    return {}


@res.flows.flow_node_attributes(memory="10Gi", disk="5G")
def handler(event, context={}):
    """
    the new handler will respond to messages of a typed nature
    the platform manages all the internals and we just need to implement a node
    this pattern can be used in map-reduce or kafka consumer modes - just switch the iterator
    Think about the redis pattern for transactions in groups of sizes etc.

     size_cache = redis["meta"]["meta_one_processed_sizes"] <- we cache different things keyd by the tasks id field

     THIS Things is designed to be generic enough that flow context could do it all
     - parse payloads
     - load a node which handles a type
     - run the node

     Work on the API factories which are just type loaders from schema and node loaders from subclasses of FlowApiNode
    """
    from res.utils import ignore_warnings
    from res.media import images

    images.text.ensure_s3_fonts()
    ignore_warnings()

    with res.flows.FlowContext(event, context) as fc:
        # 1. these are serialized into schema objects including decompression if needed
        for asset in fc.decompressed_assets:
            res.utils.logger.debug(asset)
            # 2. this handles status transactions - a well behaved node will fail gracefully anyway. Here we also manage some casting to a strong type
            with fc.asset_processing_context(asset) as ac:
                res.utils.logger.debug(f"Processing asset of type {type(ac.asset)}")
                # lesson: notice the response type, we receive data on the request but we are always sending updates using the response object
                node = MetaOneNode(
                    ac.asset, response_type=MetaOneResponse, queue_context=fc
                )
                res.utils.logger.debug(
                    f"saving asset @ {node} - will update queue for sample size: {ac.asset.is_sample_size}"
                )

                if ac.asset.mode == "costs":
                    res.utils.logger.info("Saving costs")
                    node._save_costs(ac.asset)
                else:
                    skip_previews = False
                    # skipping previews is for efficiency
                    if ac.asset.mode in ["material_swap"]:
                        skip_previews = True
                    # experimental queue update logic: relay to kafka and airtable and as its a relay dont try to save anything to hasura which we will do in the reducer for this node
                    _update_queue = False  # ac.asset.is_sample_size <- we can do this or just collect in the reducer and update the set
                    _ = node.save_with_relay(
                        queue_update_on_save=_update_queue, skip_previews=skip_previews
                    )

    return {}


class MetaOneNode(FlowApiNode):
    # we handle these types
    atype = MetaOneRequest
    # when in doubt, this is our unique node id
    uri = "one.platform.meta.style"

    def __init__(self, asset, response_type=None, queue_context=None):
        super().__init__(
            asset,
            response_type=response_type,
            queue_context=queue_context,
            base_id=AIRTABLE_QUEUE_BASE_ID,
        )
        self._name = MetaOneNode.uri

    @staticmethod
    def get_body_id(body_code, body_version):
        return MetaOneRequestIds._body_id_from_body_code_and_version(
            body_code=body_code, body_version=body_version
        )

    @staticmethod
    def update_style_status_for_piece_delta_test(
        style_code,
        style_body_version,
        bw_file_body_version,
        significant_change,
        bw_file_uri,
        bw_file_updated,
        hasura=None,
    ):
        d = {
            "style_body_version": style_body_version,
            "bw_file_body_version": bw_file_body_version,
            "significant_change": significant_change,
            "bw_file_uri": bw_file_uri,
            "bw_file_updated_at": bw_file_updated,
            "test_run_at": res.utils.dates.utc_now_iso_string(),
        }

        res.utils.logger.debug(
            f"Updating style status history for test completion on pieces delta: {d}"
        )

        hasura = hasura or res.connectors.load("hasura")
        return hasura.execute_with_kwargs(
            queries.UPSERT_STYLE_CHECKS_BY_SKU,
            metadata_sku={"sku": style_code},
            piece_version_delta_test_results=d,
        )

    @staticmethod
    def get_piece_outline_if_exists(ref_piece):
        """
        if we have saved piece outlines (normally should be done in generate-meta-one) we can import them into the database
        """
        s3 = res.connectors.load("s3")
        # the name is renamed for both path and extension
        uri = ref_piece.replace("/pieces/", "/outlines/").replace(".png", ".feather")

        if s3.exists(uri):
            try:
                res.utils.logger.info(f"Reading the outline at {uri}")
                return uri
                # data = s3.read(uri)
                # return  to_geojson(LinearRing(data.values))
            except Exception as ex:
                res.utils.logger.warn(f"Failed reading the outline at {uri} - {ex}")
                pass

    @staticmethod
    def check_piece_version_against_expected(expected_version: int, piece_key: str):
        """
        KT-2011-V2-XXXXXX-X
        """
        v = int(piece_key.split("-")[2].replace("V", ""))
        return v == expected_version

    def _save_costs(self, typed_record: MetaOneRequest):
        try:
            m1 = MetaOne(typed_record.sku)
            queries._migrate_m1_costs(m1)
            payload = m1.get_bms_request_payload()
            # was tempted to dedup on key we but we need to send all the events and allow the BMS to determine if its interesting
            res.connectors.load("kafka")[BMS_REQUEST_TOPIC].publish(
                payload, use_kgateway=True
            )

        except:
            res.utils.logger.warn(f"{traceback.format_exc()}")

    def _save(
        self,
        typed_record: MetaOneRequest,
        body_version_override=None,
        hasura=None,
        plan=False,
        validate_piece_images=False,
        on_error=None,
        **kwargs,
    ):
        """
        Object contracted
        we add ids when we receive
        we can lookup up some things in some cases but if we assume the prep we are fine not too
        """
        hasura = hasura or res.connectors.load("hasura")

        body_version = body_version_override or typed_record.body_version

        style_id = MetaOneRequestIds._style_id_from_name(
            typed_record.style_name, typed_record.body_code
        )
        body_id = MetaOneRequestIds._body_id_from_body_code_and_version(
            typed_record.body_code, body_version=body_version
        )
        brand_code = typed_record.metadata.get("brand_code")
        res.utils.logger.info(
            f"Body id {body_id} for {typed_record.body_code, body_version} - for style {typed_record.style_name} ({style_id}) for brand {brand_code}"
        )
        record = typed_record.db_dict()
        pieces = record["pieces"]
        sorted_keys = sorted([p["key"] for p in pieces])
        for p in pieces:
            # we can infer the piece name and it will be validated anyway
            if p["base_image_uri"] is None:
                uri = typed_record.get_inferred_piece_image_uri(p["key"])
                p["base_image_uri"] = uri
            p["base_image_outline"] = MetaOneNode.get_piece_outline_if_exists(
                ref_piece=p["base_image_uri"]
            )
            # broken abstraction - type has pieces - we could change the database to store this but for now what is on the db dict is what we get / we could do nested db dict too
            if "offset_size_inches" in p:
                p.pop("offset_size_inches")

            p["piece_set_size"] = len(pieces)
            p["piece_ordinal"] = sorted_keys.index(p["key"]) + 1
            p["normed_size"] = typed_record.normed_size

            # what this really tests is if we posted a request a piece on s3 in version 5 to be used to save a style in v6 for example
            # this could happen for example if we extracted an old request from acq on the older version and then requested the meta one in that older version
            # (would be better if the workflow maybe was not allow to post at all by using the same MetaOneNode.refresh())
            valid_version = MetaOneNode.check_piece_version_against_expected(
                expected_version=typed_record.body_version,
                piece_key=p["base_image_uri"].split("/")[-1],
            )
            if not valid_version:
                track_fail += ["PIECE_NAMES_VALID"]
                raise Exception(
                    f"Unable to confirm all color pieces have the correct version when compared to the requested version {body_version} e.g. piece {p['key']}"
                )

        # if we can save it its active
        record["status"] = "Active"
        # now we generate some of our ids from the contract objects
        record = MetaOneRequestIds.build_ids(record, style_id, body_id)
        garment_id = record["id"]
        history = MetaOneRequestIds.get_style_size_history(garment_id, record["pieces"])
        if plan:
            return record, history

        """
        invalidate the make cache for the cases where we are recreating color
        ...
        """

        MetaOnePieceManager.make_cache_dirty(
            sku=typed_record.sku, body_version=body_version
        )

        response = None
        # whatever we are told is failing on the way is failing
        # now someone needs to unblock this to move things through - now we need a bot that checks for changes on certain user fields in the flow

        track_fail = typed_record.contracts_failing or []
        res.utils.logger.info(f"Incoming contracts failing are {track_fail}")
        try:
            if len(pieces) == 0:
                raise Exception(
                    "You are trying to insert a style with zero pieces - check the data are added or parsed properly"
                )

            # check the color pieces exist or dont save if the option is set
            # this prevents for examples saving a dud version over a good one
            if validate_piece_images:
                if not _pieces_images_exist(pieces):
                    track_fail += ["ALL_COLOR_PIECES"]
                    raise Exception("Unable to confirm all color pieces")

            res.utils.logger.debug(f"Saving the stuff...")
            response = hasura.tenacious_execute_with_kwargs(
                queries.BATCH_INSERT_GARMENT_PIECES,
                pieces=pieces,  # the garment id is used in the mutation
                garment_id=garment_id,
                history=history,
                ended_at=res.utils.dates.utc_now_iso_string(None),
            )
            res.utils.logger.debug(f"Response = {response}, Reloading....")

            m1 = MetaOne.safe_load(typed_record.sku)

            # compute piece hash - these pieces are constructed from kafka payloads to typed pieces
            if typed_record.is_sample_size:
                """
                This retrieves but registers both hashes for the latest meta one!!
                """
                pieces_hash = m1.get_pieces_hash(write=True)

                # queries.update_piece_material_hash()

            # sid = dict(m1._data.iloc[0])["id"]
            #
            try:
                gid = dict(m1._data.iloc[0])["style_sizes_id"]
                assert (
                    gid == garment_id
                ), f"The meta one read back for the sku has a different id to the one we are using on the garment and saving costs against: {gid} != {garment_id}"
                material_usage_statistics = m1.get_costs()
                # we augment the cost object with more data
                # TODO - update the database to be a dict on fall back to list of materials
                c = m1.style_cost_capture(
                    costs={"piece_stats_by_material": material_usage_statistics}
                )
                cost_at_onboarding = c["Cost"].sum()
                costs = c.to_dict("records")
                size_yield = c[(c["Category"] == "Materials") & (c["Unit"] == "Yards")][
                    "Quantity"
                ].sum()
                body_piece_count_for_costs = m1.body_piece_count
                # prepare all costs for upsert

                # style_material_yield add this to the response
                # piece_stats_by_material
                response = hasura.tenacious_execute_with_kwargs(
                    queries.UPSERT_STYLE_COSTS,
                    id=garment_id,
                    # change this column name to be material stats
                    material_usage_statistics=material_usage_statistics,
                    size_yield=size_yield,
                    # add a foreign key table for actual costs - ends any values that are outdated and keep the latest
                    costs=costs,
                    piece_count_checksum=body_piece_count_for_costs,
                    # add the snapshot total
                    cost_at_onboarding=cost_at_onboarding,
                )
            except:
                res.utils.logger.warn(
                    f"Failing to compute costs {traceback.format_exc()}"
                )
                track_fail.append("STYLE_COSTS")
                raise

            # request BMS

            try:
                res.utils.logger.info(f"BMS request....")
                kafka = res.connectors.load("kafka")
                payload = m1.get_bms_request_payload()
                # was tempted to deup on key we but we need to send all the events and allow the BMS to determine if its interesting
                kafka[BMS_REQUEST_TOPIC].publish(payload, use_kgateway=True)
            except:
                res.utils.logger.warn(
                    "Unable to send the payload for BMS request to kafka"
                )
                res.utils.logger.warn(f"{traceback.format_exc()}")

            try:
                s3 = res.connectors.load("s3")

                # e.g. 'TK-3080 CHRST CORAFA 2ZZSM V3'
                # [body_code, _, color_code, _, _] = (
                #     m1.sku_versioned.lower().replace("-", "_").split(" ")
                # )
                # root = f"s3://meta-one-assets-prod/color_on_shape/{body_code}/v{body_version}/{color_code}"
                # SA refactor
                root = m1.input_objects_s3
                glb = f"{root}/3d.glb"
                point_cloud = f"{root}/point_cloud.json"
                front_image = f"{root}/front.png"

                # temporary while the new "dynamic" flow is a work in progress: if the point_cloud
                # doesn't exist for this style, try to get it from the body if it's not there, no
                # biggy, we'll just skip it, 3d.glb is the important one
                ext = f"{m1.body_code.replace('-', '_')}/v{m1.body_version}".lower()
                body_root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{ext}"

                body_point_cloud = f"{body_root}/extracted/point_cloud.json"
                if not s3.exists(point_cloud) and s3.exists(body_point_cloud):
                    s3.copy(body_point_cloud, point_cloud)

                # until we have a way of generating a glb outside of a browser, if there's no glb
                # but there is one for a previous version, copy it over
                try:
                    old_glb = glb.replace(
                        f"/v{m1.body_version}/", f"/v{m1.body_version-1}/"
                    )
                    if not s3.exists(glb) and s3.exists(old_glb):
                        res.utils.logger.info(f"Copying old glb {old_glb} to {glb}")
                        s3.copy(old_glb, glb)
                except:
                    res.utils.logger.warn(
                        f"Unable to copy old glb {old_glb} to {glb} {traceback.format_exc()}"
                    )

                if s3.exists(glb):  # and s3.exists(point_cloud):
                    # front_image is not a requirement for now
                    front_image = front_image if s3.exists(front_image) else None
                    res.utils.logger.info(
                        f"3D model files exist {glb} {point_cloud} {front_image}"
                    )
                    style_id = dict(m1._data.iloc[0])["id"]
                    hasura.tenacious_execute_with_kwargs(
                        queries.ADD_STYLE_3D_MODEL_URIS,
                        id=style_id,
                        metadata={
                            "has_3d_model": True,
                            "3d_model": glb,
                            "point_cloud": point_cloud,
                            **({"front_image": front_image} if front_image else {}),
                            "body_version": body_version,
                        },
                        model_3d_uri=glb,
                        point_cloud_uri=point_cloud,
                        front_image_uri=front_image,
                    )
                else:
                    res.utils.logger.warn(f"3D model files don't exist")
                    track_fail.append("3D_MODEL_FILES")
            except:
                res.utils.logger.warn(
                    f"3D model files don't exist {traceback.format_exc()}"
                )
                track_fail.append("3D_MODEL_FILES")

            # use some AI to check the piece images for anomalies
            # just do it once
            if typed_record.is_sample_size:
                try:
                    from res.flows.dxa.inspectors import buttongate

                    buttongate.check_for_buttons(
                        sku=typed_record.style_sku,
                        max_pieces=0,
                        retries=1,
                        slack_channel="C06DVEE12JV",  # U03Q8PPFARG @John, C06DVEE12JV #inspector_gpt4vet
                    )
                except:
                    res.utils.logger.warn(
                        f"Unable to check pieces for buttons etc. {traceback.format_exc()}"
                    )
                    # no need to fail (for now at least)

                # check for simulation issues
                try:
                    from res.flows.dxa.inspectors import simulation

                    if "-9" in m1.body_code:
                        res.utils.logger.info(
                            f"Skipping simulation check for {m1.body_code} as it's a swatch"
                        )
                    else:
                        simulation.inspect_simulation(
                            m1.body_code,
                            m1.body_version,
                            m1.primary_color_code,
                            slack_channel="C06DVEE12JV",
                        )
                except Exception as e:
                    msg = traceback.format_exc()
                    res.utils.logger.warn("Exception inspecting simulation", error=msg)

            if typed_record.is_sample_size:
                track_fail += m1.validate()
                try:
                    skip_previews = kwargs.get("skip_previews", False)
                    # in the meta data of the request, it is possible to skip previews
                    skip_previews = typed_record.metadata.get(
                        "skip_previews", skip_previews
                    )
                    spoof_one = 10001000
                    # for preview purposes
                    m1._one_number = spoof_one
                    if not skip_previews:
                        res.utils.logger.info("Generating previews....")
                        _ = m1.build_asset_previews()
                except:
                    track_fail += ["PREVIEWS_GENERATED"]
                    raise Exception("Unable to generate previews")

            # this response will be built and sent for samples sizes - it includes previews uris just generated
            response = m1.status_at_node(
                sample_size=typed_record.sample_size,
                request=typed_record,
                trace_log=traceback.format_exc(),
                object_type=MetaOneResponse,
                # we will collect validation at the end of the session but returning her for this context in the returned object
                bootstrap_failed=track_fail,
                reset_validate=True,
            )

            # update shared state in redis for map-reduce collect state
            self.try_update_shared_state(record["size_code"], response.contracts_failed)
            res.utils.logger.info(
                f"Saved and prepared response for {record['sku']} in body version {typed_record.body_version} - is sample size: {typed_record.is_sample_size} - Contracts failing :{response.contracts_failed}"
            )
            return response
        except TransportQueryError as tex:
            res.utils.logger.warn(f"hit a transport error {tex}  ")
            nex = StyleNodeDataException(
                data=record, transport_error=tex, response=response
            )
            res.utils.logger.warn(f"explained {nex._explanation}")
            if on_error == "raise":
                raise nex

            # otherwise collect context in the response
            response = record
            response["contracts_failed"] = record["contracts_failing"]

            response["status"] = "Failed"
            response["trace_log"] = traceback.format_exc()
            contract = nex._explanation.get("contract")
            if contract:
                response["contracts_failed"] = list(
                    set(
                        (response["contracts_failed"] or [])
                        + [contract, "STYLE_PIECES_SAVED"]
                        + (track_fail or [])
                    )
                )

        except Exception as ex:
            if on_error == "raise":
                raise ex

            # otherwise collect context in the response
            res.utils.logger.warn(traceback.format_exc())
            response = record
            response["contracts_failed"] = record["contracts_failing"] or []
            response["status"] = "Failed"
            db_errors = ["STYLE_PIECES_SAVED"]
            response["contracts_failed"] = list(
                set(response["contracts_failed"] + db_errors + track_fail)
            )
            response["trace_log"] = traceback.format_exc()

        self.try_update_shared_state(
            response["size_code"], response["contracts_failed"]
        )

        res.utils.logger.debug(f"Finally response with possible failures {response}")

        return MetaOneResponse(**response)

    @staticmethod
    def get_style_as_request(
        sku,
        body_version=None,
        alias_sku=None,
        with_initial_contracts=None,
        mode=None,
        style_id=None,
        skip_previews=False,
    ):
        """
        todo mimic the graph call for style pieces and create the request
        we should return the request that is size specific so really we should add 4 part skus here

        body version should be passed to avoid making a request in the latest body version by default
        """
        from res.flows.meta.ONE.queries import (
            GET_GRAPH_API_STYLE_BY_SKU,
            GET_GRAPH_API_STYLE_BY_ID,
        )
        from stringcase import titlecase

        g = res.connectors.load("graphql")

        parts = sku.split(" ")
        size_code = parts[-1] if len(parts) == 4 else "*"
        # take the first three components

        # make sure its hyphenated
        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"
        style_sku = " ".join(sku.split(" ")[:3])

        if style_id is None:
            res.utils.logger.info(f"Requesting style for sku {style_sku}")
        else:
            res.utils.logger.info(
                f"Using the more specific style id {style_id} to get the request for sku {style_sku}"
            )

        r = (
            g.query_with_kwargs(GET_GRAPH_API_STYLE_BY_SKU, sku=style_sku)
            if style_id is None
            else g.query_with_kwargs(GET_GRAPH_API_STYLE_BY_ID, id=style_id)
        )

        if alias_sku:
            res.utils.logger.info(f"Aliasing request from {sku} -> {alias_sku}")
            sku = sku.replace(style_sku, alias_sku)
            style_sku = style_sku.replace(style_sku, alias_sku)

        def make_piece(p, body_code, body_version):
            def _file_uri(f):
                try:
                    return f"s3://{f['s3']['bucket']}/{f['s3']['key']}"
                except:
                    res.utils.logger.warn(f"Failed to resolve uri in {f}")
                return None

            return {
                "key": f"{body_code}-V{body_version}-{p['bodyPiece']['code']}",
                "color_code": p["color"]["code"],
                "material_code": p["material"]["code"],
                "base_image_uri": None,
                # assume exists but safety
                "artwork_uri": _file_uri(p["artwork"]["file"]),
                "offset_size_inches": p["material"].get("offsetSizeInches", 0.0) or 0.0,
            }

        s = r["data"]["style"]
        s["body_code"] = s["body"]["code"]
        # use the latest unless specified
        s["body_version"] = body_version or s["body"]["patternVersionNumber"]
        size_map = {s["code"]: s["name"] for s in s["body"]["availableSizes"]}
        s["sizes"] = [s["code"] for s in s["body"]["availableSizes"]]
        s["style_name"] = s["name"]
        s["sample_size"] = s["body"]["basePatternSize"]["code"]
        s["sku"] = sku
        # NB: this is where we determine the pieces that we except from the graph to make a style: printable pieces
        s["pieces"] = [
            make_piece(p, s["body_code"], s["body_version"]) for p in s["pieceMapping"]
        ]
        s["pieces"] = [p for p in s["pieces"] if PieceName(p["key"]).is_printable]
        s["style_sku"] = style_sku
        # we can specify a particular size code or wild card it
        s["size_code"] = size_code
        s["normed_size"] = size_map.get(size_code)
        s["print_type"] = titlecase(s["printType"].lower())

        if "metadata" not in s:
            s["metadata"] = {}
        s["metadata"]["brand_code"] = s["body"]["brandCode"]

        if skip_previews:
            s["metadata"]["brand_code"] = True

        if mode:
            s["mode"] = mode

        try:
            # "bundle mode"
            if size_code == "*":
                payloads = []
                for size in s["sizes"]:
                    p = dict(s)
                    p["sku"] = f"{style_sku} {size}"
                    p["size_code"] = size
                    p["normed_size"] = size_map.get(size)
                    p["contracts_failing"] = (
                        with_initial_contracts if with_initial_contracts else None
                    )
                    payloads.append(MetaOneRequest(**p))
                return payloads
            return MetaOneRequest(**s)
        except:
            res.utils.logger.warn(
                f"failed to parse check your data {traceback.format_exc()}"
            )
        return s

    @staticmethod
    def get_full_status(sku):
        data = queries.get_style_status_by_sku(MetaOneRequestIds.three_part_sku(sku))[
            "meta_styles"
        ]
        data = {} if len(data) == 0 else data[0]
        data["diffs"] = MetaOneNode.test_style_mapping(sku)

        if data.get("style_sizes"):
            data["pending_sizes"] = [
                d["size_code"] for d in data["style_sizes"] if d["status"] != "Active"
            ]
        data["has_diffs"] = len(data["diffs"]) > 0
        return data

    @staticmethod
    def last_run(style_sku):
        """
        reads the last run from the tracelog in the meta one response airtable
        """

        tab = res.connectors.load("airtable")["appqtN4USHTmyC6Dv"]["tblzHAvWPWu2bJVDc"]
        d = tab[style_sku]
        uri = d["Trace Log"]
        uri = f"/".join([a for a in uri.split("/")[:-1]])
        res.utils.logger.info(f"Reading {uri}")
        return res.connectors.load("s3").read_logs_in_folder(uri)

    @staticmethod
    def refresh(
        sku,
        should_refresh_header=False,
        use_record_id=None,
        alias_sku=None,
        mode=None,
        body_version=None,
        style_id=None,
        skip_sizes=[],
        clear_cache=False,
    ):
        """
        sku is a three part BMC but we generate the requests for all sizes of the sku and send the bundle to kafka
        generate the payloads for the sizes and update as a bundle on the given kafka env

        pass in the record if fully simulates triggering from upstream which is needed for example to update the upstream queue

        if alias sku exists we will go and generate the alias but using all the data for the sku - once we get a payload we no longer care about what is in res.magic and we expect the color pieces to resolve to something

        """

        res.utils.logger.info(f"Loading payload for sku {sku} (aliases: {alias_sku})")

        # if redis is open and we want this as an option.
        if clear_cache and use_record_id:
            res.utils.logger.info("Cache reset enabled")
            reset_cache_for_ids(use_record_id)

        payloads = MetaOneNode.get_style_as_request(
            sku, alias_sku=alias_sku, mode=mode, style_id=style_id
        )
        if should_refresh_header:
            helpers.style_model_upsert_style_header(payloads[0].dict())
        # for prod assume this : os.environ['KAFKA_KGATEWAY_URL'] = 'https://data.resmagic.io/kgateway/submitevent'
        payloads = [p.dict() for p in payloads]
        payloads = [p for p in payloads if p["size_code"] not in skip_sizes]

        for p in payloads:
            p["id"] = use_record_id
            if body_version:
                p["body_version"] = body_version

        if use_record_id:
            res.utils.logger.info(
                f"Using the passed in record id for all requests {use_record_id}"
            )

        res.connectors.load("kafka")[
            "res_meta.dxa.style_pieces_update_requests"
        ].publish(payloads, use_kgateway=True, coerce=True)

    @staticmethod
    def get_logs_for_style(style_code="CC-3054 CTW70 CORESX"):
        import json
        from res.learn.agents import InterpreterAgent
        from res.connectors.airtable import AirtableConnector

        airtable = res.connectors.load("airtable")
        a = InterpreterAgent()
        s3 = res.connectors.load("s3")
        pred = AirtableConnector.make_key_lookup_predicate([style_code], "Style Sku")
        t = airtable["appqtN4USHTmyC6Dv"]["tblzHAvWPWu2bJVDc"]
        d = t.to_dataframe(fields=["Style Sku", "Trace Log"], filters=pred).iloc[0][
            "Trace Log"
        ]
        d = "/".join(d.split("/")[:-1])
        d = s3.read_logs_in_folder(d)
        import json

        prompt = f"""
             Consider the log output below.
     Each chunk of data ares from a single node/pod.
     The data have style codes such as this format `{style_code}` (which is a body code, material code and color code) and each style is a separate entity.
     We process styles size by size which produces a four part code like `{style_code} XXXXX`.
     There may be errors, exceptions or "contracts failings". Ignore `Cache ready check failed` and `there was a problem calling the handler` which are just warnings.
     If there are multiple styles, you only need to return data for `{style_code}`
    
     Use the response format below to provide a JSON response.
    
    **Response Model**
    ```
    class StyleInfo(BaseModel):
      style_code: str
      size_code: str
      contracts_failing: List[str]
      exception_logs: str
      s3_uris: List[str]
      notes: str
      
    class Events(BaseModel):
        styles: List[StyleInfo]
    ```
    
    **Log Output**
     ```json
     {json.dumps(d)}
     ```

     Be exhaustive in uncovering issues in the processing of each element
        """
        d = json.loads(a.ask(prompt, response_format={"type": "json_object"}))
        return d

    @staticmethod
    def test_style_mapping(sku):
        """
        simple util for now to show that are differences between the style mapping and what we saved on major attributes
        this has the down side of not being a batch call ..
        """

        a = MetaOneNode.get_style_as_request(sku)
        sot = {p.key: p.dict() for p in a.pieces}
        m = MetaOne(sku)
        diffs = []

        for p in m:
            p._row["offset_size_inches"] = p._row["piece_metadata"][
                "offset_size_inches"
            ]
            for c1, c2 in [
                ("garment_piece_material_code", "material_code"),
                ("garment_piece_color_code", "color_code"),
                ("artwork_uri", "artwork_uri"),
                ("offset_size_inches", "offset_size_inches"),
            ]:
                val = sot[p.key].get(c2)

                if p._row[c1] != val:
                    diffs.append(
                        {
                            "key": p.key,
                            "sku": m.sku,
                            "category": c1,
                            "our_value": p._row[c1],
                            "their_value": val,
                        }
                    )

        return diffs

    @staticmethod
    def get_apply_color_queue_changes(window_minutes=60):
        """
        we can look at recent stage changes on the queue and take some actions
        for example for things recently exiting we should check that contracts are lifted
        """
        from res.flows.meta.ONE.queries import get_apply_color_queue_changes

        return get_apply_color_queue_changes(window_minutes=window_minutes)

    @staticmethod
    def repair_locally(
        skus,
        skip_previews=True,
        force_bootstrap_header=False,
        queue_update_on_save=False,
        sample_size_only=False,
        body_version=None,
        style_id=None,
    ):
        """
        this is not only a local repair but an integration test with the last style node code
        """
        # save all the styles
        if isinstance(skus, str):
            skus = [skus]
        contracts = []
        for sku in skus:
            payloads = MetaOneNode.get_style_as_request(
                sku, body_version=body_version, style_id=style_id
            )
            # sometimes we need to force it
            if force_bootstrap_header:
                helpers.style_model_upsert_style_header(payloads[0].dict())

            # if we dont trust the style header we can do it again
            for p in payloads:
                if sample_size_only and not p.is_sample_size:
                    continue

                res.utils.logger.info(f"doing the size {p.size_code}")
                node = MetaOneNode(
                    p,
                    response_type=MetaOneResponse,
                    queue_context=res.flows.FlowContext({}),
                )

                # for testing useful to test the airtable link
                queue_update_on_save = (
                    queue_update_on_save if p.is_sample_size else False
                )

                c = node.save_with_relay(
                    queue_update_on_save=queue_update_on_save,
                    skip_previews=skip_previews,
                )
                contracts += c.contracts_failed
                print(c.contracts_failed)

    @staticmethod
    def get_payloads_since(since_days=1):
        import json
        import pandas as pd

        d = str(res.utils.dates.utc_days_ago(since_days).date())
        Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_META_DXA_STYLE_PIECES_UPDATE_REQUESTS" WHERE TO_TIMESTAMP_TZ(parse_json(RECORD_CONTENT):created_at)   > '{d}'"""

        data = res.connectors.load("snowflake").execute(Q)

        data["payload"] = data["RECORD_CONTENT"].map(json.loads)
        data["created_at"] = data["payload"].map(lambda x: x.get("created_at"))
        data["created_at"] = pd.to_datetime(data["created_at"], utc=True)

        return list(data["payload"])

    @staticmethod
    def upsert_status_history(
        sku, body_version, contracts_failed, append=True, hasura=None
    ):
        """
        - 4 part sku to search. must be sample size. may split into 3 part and size in DB TBD
        - get
        """
        hasura = hasura or res.connectors.load("hasura")
        # get the current style header
        body_code = sku.split(" ")[0].strip()
        header = hasura.execute_with_kwargs(
            queries.GET_STYLE_SIZE_KEYS_BY_SKU, metadata_sku={"sku": sku}
        )["meta_style_sizes"][0]
        ref_piece = header["style_size_pieces"][0]["piece"]
        bid = ref_piece["body_piece"]["body"]["id"]
        # infer the bid
        sid = header["style"]["id"]
        current_body_version = ref_piece["body_piece"]["body"]["version"]
        res.utils.logger.info(
            f"Sku: {sku} currently on body version {current_body_version} and style id {sid}, body version {bid}"
        )

        # ####
        #
        #  the inferred bid is this one we want to save for any given version but it might not exist
        #  constraints will be added and checked later
        #  the body version is the requested one which can be compared to the current styles version
        #
        #####
        request_bid = MetaOneNode.get_body_id(body_code, body_version)

        # if request bid is current bud
        # queries.update_contracts_failing_by_sku(sku, contracts_failed=rp.contracts_failed)

        # get the history object
        H = hasura.execute_with_kwargs(
            queries.GET_STYLE_STATUS_HISTORY_BY_STYLE_ID_AND_BODY_VERSION,
            style_id=sid,
            body_version=body_version,
        )
        records = H["meta_style_status_history"]
        contracts_failing = []
        if len(records):
            contracts_failing = records[0]["contracts_failing"]
            res.utils.logger.info(f"We have existing contracts on {records[0]}")
        update_status = {
            # this will be unique for any style and body version
            "id": res.utils.uuid_str_from_dict({"sid": sid, "bid": request_bid}),
            "style_id": sid,
            "body_id": request_bid,
            "body_version": body_version,
            "contracts_failing": list(set(contracts_failing) | set(contracts_failed)),
        }
        res.utils.logger.debug(f"Updating the current status history {update_status}")
        return hasura.execute_with_kwargs(
            queries.UPDATE_STYLE_STATUS_HISTORY, style_status=update_status
        )

    @staticmethod
    def list_brand_styles(brand_code, body_code=None):
        hasura = res.connectors.load("hasura")

        result = hasura.tenacious_execute_with_kwargs(
            queries.GET_STYLES_BY_BRAND, brand_code=brand_code
        )["meta_styles"]

        # TODO support body filter push down -for now lame test code
        if body_code:
            result = [r for r in result if r["metadata"]["sku"][:7] == body_code]

        return result

    @staticmethod
    def get_full_style_status(
        style_code,
        body_version,
        include_full_status=True,
        brand_code=None,
        include_cost=False,
    ):
        from res.flows.meta.ONE.style_piece_deltas import request_style_tests_for_body
        from res.flows.meta.ONE.body_node import BodyMetaOneNode

        # todo maybe add style 3d onboarded from the graph too / also converge to validate-meta-one test

        # include in this more dates including the s3 piece dates
        # use the style pieces updated date which should be correlated with s3??
        # good augment this one in deep mode if we are just using an SQL query - question if we have to validate all size pieces or just sample size
        status = MetaOne.get_style_status_history(style_code, full=True)

        # get the style id from the graph and if the style is onboarded - maybe the payload fulf uses...
        if not status:
            return {
                "style_id": None,
                "sku": style_code,
                "name": None,
                "body_version": body_version,
                "contracts_failing": ["STYLE_PIECES_SAVED"],
                "piece_version_delta_test_results": {},
                "audit_history": [],
                "message": "There is no style saved for this sku in this environment",
            }

        if body_version and include_full_status:
            """
            doing a test for both the style body version and the requested body version
            """
            body_code = style_code.split(" ")[0].strip()

            res.utils.logger.info(
                f"Requesting a test for style { style_code} in body version {body_version}"
            )

            status["versioned_body_contracts"] = BodyMetaOneNode.get_contracts_for_body(
                body_code, body_version, style_body_version=status.get("body_version")
            )

            # i can add style airtable stuff here like is-s3-onboarded but because we are moving the contracts to the meta one maybe not worth the call
        if include_cost:
            # may not implement this here since costs are maybe best request separately...
            pass

        if not len(status.get("piece_version_delta_test_results")):
            res.utils.logger.info(f"Queue request for the style test for {style_code}")
            try:
                request_style_tests_for_body(
                    body_code, body_version, style_codes=style_code
                )
            except:
                # we use this ti make a singleton because the argo name in this mode is the style and body version
                res.utils.logger.warn(
                    f"Could not submit workflow - are you trying to reuse the same key"
                )

        return status

    #######
    ##      RRM interface
    #######
    def _key_parser(k):
        parts = k.split(" ")
        if len(parts) == 4:
            return f" ".join(parts[:3])
        return k

    def _make_textual_description(attributes):
        # sizes = attributes["meta_one_sizes_required"]
        description = f"""This is garment style entity (also know as a Meta.ONE) made up of a "Body" code, "Material" code and "Color code" e.g {attributes['style_code']}. It can be ordered in multiple sizes from the list of sizes avaialable. The order has a Sku which has a fourth component "Size"
        The kanban style queue where styles are processed is called the "Apply Color Queue" and has various statuses or stages such as "Generate Meta One" or "Done". Here are more details about this style: {attributes}"""
        return description

    def _reimport_style_rrm():
        """
        bootstrapper - todo incremental
        """
        from res.flows.api.rrm import ResilientResonanceMap
        from stringcase import snakecase
        from res.utils.dataframes import replace_nan_with_none

        # define enum single select and multi select :> store somewhere in settings hot and cold
        # define jargon context inheriting from some node root

        res.utils.logger.info(f"fetching apply color queue (acq) data...")
        data = res.connectors.load("airtable").get_table_data(
            "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w"
        )
        res.utils.logger.info(f"cleaning data in preparation for saving map base")
        a = data.sort_values("Created At")
        a["Request Auto Number"] = a["Request Auto Number"].map(int)

        # remove cancelled - treat as not on queue
        # this is because sometimes after doneness a bogus request is added and cancelled - maybe we need an ever better way to confirm the existence of done
        a = a[a["Apply Color Flow Status"] != "Cancelled"]
        ###########
        a = a.sort_values("Request Auto Number")
        counter = dict(
            a.groupby("Style Code").count()["Created At"].reset_index().values
        )

        a = a.drop_duplicates(subset=["Style Code"], keep="last")[
            [
                "Created At",
                "Color Type",
                "Style Code",
                "Request Auto Number",
                "Flag for Review",
                "Body Version",
                "Apply Color Flow Status",
                "Last Updated At",
                "Meta.ONE Flags",
                "Meta ONE Sizes Required",
            ]
        ]

        a["Apply Color Flow Status"] = a["Apply Color Flow Status"].map(lambda a: a[0])
        a["Body Version"] = a["Body Version"].fillna(-1).map(int)
        a["contract_failure_names"] = a["Meta.ONE Flags"]  # .map(str)
        a["is_flagged"] = a["Flag for Review"].fillna(0).map(int)
        a["is_cancelled"] = 0
        a["is_done"] = 0
        a["number_of_times_in_queue"] = a["Style Code"].map(lambda x: counter.get(x))

        a = a.drop("Meta.ONE Flags", 1)
        a.columns = [snakecase(c.lower()).replace("__", "_") for c in a.columns]

        a.loc[a["apply_color_flow_status"] == "Cancelled", "is_cancelled"] = 1
        a.loc[a["apply_color_flow_status"] == "Done", "is_done"] = 1

        res.utils.logger.info(f"Preview row {dict(a.iloc[0])}")

        res.utils.logger.info(f"building and saving map")
        rrm_styles = ResilientResonanceMap("meta", "styles")
        a = replace_nan_with_none(a)

        # rename on the base of a number also sounding like a counts or if the column is ambiguous
        a = a.rename(
            columns={
                "request_auto_number": "request_auto_id",
                "meta_one_sizes_required": "available_size",
            }
        )

        rrm_styles.import_entities(a, key_field="style_code")

        return rrm_styles

    @staticmethod
    def get_rrm(as_agent=False, **kwargs):
        """
        implements the agent interface
        """
        from res.flows.api.rrm import ResilientResonanceMap

        rrm_styles = ResilientResonanceMap(
            "meta",
            "styles",
            key_parser=MetaOneNode._key_parser,
            text_factory=MetaOneNode._make_textual_description,
            **kwargs,
        )
        return rrm_styles if as_agent == False else rrm_styles.as_agent()


@res.flows.flow_node_attributes(memory="4Gi")
def compute_style_nesting_savings(event, context={}):
    s3 = res.connectors.load("s3")
    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            res.utils.logger.info(f"processing {asset}")
            df = MetaOne.for_sample_size(asset).make_nesting_comparison(factor=50)
            pt = f"s3://res-data-platform/samples/nesting-stat/{asset.replace(' ','-')}.feather"
            res.utils.logger.info(f"Writing {pt} of len {len(df)}")
            s3.write(pt, df)
    return {}


def reset_cache_for_ids(ids):
    """
    helper if we get in a weird so purge the redis cache
    one case we needed this was after removing some sizes and the old ones are cached
    in this case we cannot purge them if they have contract failures
    """
    from res.flows import FlowEventProcessor

    try:
        if not isinstance(ids, list):
            ids = [ids]
        for rid in ids:
            event = FlowEventProcessor().make_sample_flow_payload_for_function(
                "meta.ONE.style_node"
            )
            SHARED_STATE_KEY = "Flow.Api.Shared.State"

            with res.flows.FlowContext(event, {}) as fc:
                redis = res.connectors.load("redis")
            res.utils.logger.info(f"resetting cache in style node for {rid}")
            tmap = redis[f"FLOW_CONTEXT_CACHE_{fc._flow_id.upper()}"][rid]
            ## print(tmap[SHARED_STATE_KEY])

            for k in tmap[SHARED_STATE_KEY].keys():
                tmap.update_named_map(SHARED_STATE_KEY, k, [])
    except Exception as ex:
        res.utils.logger.warn(f"Failed to purge the cache - {ex}")


@res.flows.flow_node_attributes(memory="2Gi")
def retry_generate_meta_one(event, context={}, reset_cache=True):
    with res.flows.FlowContext(event, context) as fc:
        airtable = res.connectors.load("airtable")
        d = airtable.get_view_data(
            "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwiu6edbsVEd5b4H"
        )
        uri = "https://data.resmagic.io/res-connect/flows/generate-meta-one"
        for record in d.to_dict("records"):
            try:
                rid = record["record_id"]
                if reset_cache:
                    reset_cache_for_ids(rid)
                MetaOneNode.refresh(
                    record["Style Code"], use_record_id=rid, should_refresh_header=True
                )
            except Exception as ex:
                res.utils.logger.warn(
                    f"Failed to retry style {record['Style Code']} because {traceback.format_exc()}"
                )


@res.flows.flow_node_attributes(memory="8Gi")
def migrate_all_style_costs(
    event={},
    context={},
    only_if_nest_stats_exist=True,
    filter_out_existing_size_yield=True,
):
    """
    utility job to run all cost updates - by default this is the fast most where we check for material states
    there is another mode to run the entire style node for styles in map reduce fashion but in the costs mode
    """

    with res.flows.FlowContext(event, context) as fc:
        res.utils.logger.info("loading all headers")
        data = queries.get_all_active_style_header_with_costs()
        if filter_out_existing_size_yield:
            data = data[data["size_yield"].isnull()]
            res.utils.logger.info(f"Considering items with no size yield")

        # run a test-> the body check sum should match the check sum on the style and there should be a pieces in the cut costs field that matches
        # this shows that we have run the process with the latest understanding
        # we can run warnings on the missing sew data ..

        if fc.args.get("bodies"):
            data["sku"] = data["metadata"].map(lambda x: x.get("sku"))
            data["style_sku"] = data["sku"].map(lambda x: f" ".join(x.split(" ")[:3]))
            data["body_code"] = data["sku"].map(
                lambda x: x.split(" ")[0].lstrip().rstrip()
            )
            l = len(data)
            data = data[data["body_code"].isin(fc.args.get("bodies"))]
            res.utils.logger.info(
                f"Filtered from {l} to {len(data)} for bodies {fc.args.get('bodies')}"
            )

        data["_has_mat_usage"] = data["material_usage_statistics"].map(
            lambda x: len(x) > 0
        )

        if only_if_nest_stats_exist:
            data = data[data["_has_mat_usage"]]

        data["sku"] = data["metadata"].map(lambda x: x.get("sku"))

        from tqdm import tqdm

        for record in tqdm(data.to_dict("records")):
            try:
                m1 = MetaOne(record["sku"])
                # when we dont have it we do the slow thing of making it
                mcosts = (
                    record["material_usage_statistics"]
                    if record["_has_mat_usage"]
                    else m1.get_costs()
                )
                x = queries._migrate_m1_costs(m1, mcosts)
                res.utils.logger.info(f'saved for {record["sku"]}')
                # print(x['update_meta_style_sizes']['returning'][0]['id'])
            except:
                # raise
                res.utils.logger.warn(f"Error in migration {traceback.format_exc()} ")
                pass

    return {}


def request_touch_files(payloads):
    """
    submit a job on argo
    payload examples - pass dict or list of dicts

      {'body_code': 'TK-6129', 'body_version': 12}
      {'style_code': 'TK-6129 CHRST IVORVJ', 'body_version': 12}
      [{'body_code': 'TK-6129', 'color_code': 'blacy', 'body_version': 12}]

    """
    from res.flows import FlowEventProcessor

    e = FlowEventProcessor().make_sample_flow_payload_for_function(
        "meta.ONE.style_node.touch_files_handler"
    )
    if not isinstance(payloads, list):
        payloads = [payloads]
    e["assets"] = payloads
    argo = res.connectors.load("argo")
    return argo.handle_event(
        e, unique_job_name=f"touch-files-{res.utils.res_hash()}".lower()
    )


def touch_files_handler(event, context={}):
    """

    handler that takes assets which are args for the function e.g.
      {'body_code': 'TK-6129', 'body_version': 12}
      {'style_code': 'TK-6129 CHRST IVORVJ', 'body_version': 12}
      {'body_code': 'TK-6129', 'color_code': 'blacy', 'body_version': 12}

    color codes are optional but if we have a sku it would override the others
    """
    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            style_code = asset.get("style_code")
            if style_code:
                asset["body_code"] = style_code.split(" ")[0].strip()
                asset["color_code"] = style_code.split(" ")[2].strip()
            touch_color_pieces_files(**asset)
    return {}


def touch_color_pieces_files(
    body_code, body_version, color_code=None, plan=False, **kwargs
):
    """
    all outlines and pieces are copied to update metadata
    """
    s3 = res.connectors.load("s3")

    body_code = body_code.lower().replace("-", "_")
    files_path = f"s3://meta-one-assets-prod/color_on_shape/{body_code}/v{body_version}"
    if color_code:
        files_path = f"{files_path}/{color_code.lower()}"
    l = [f for f in s3.ls(files_path) if "/pieces/" in f or "/outlines/" in f]

    if not plan:
        for f in l:
            s3.copy(f, f)
    return l
