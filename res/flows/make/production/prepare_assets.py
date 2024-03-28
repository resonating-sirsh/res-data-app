"""
***
see: https://app.shortcut.com/resonanceny/write/IkRvYyI6I3V1aWQgIjY1MGE1MGE3LTUzMDktNDRmMy1hOTJiLTMzNmUxMDk4MzIzMCI=
***

This is a refactor of the PPP process
It is a much simpler code to do the same sort of thing
we have some caching of styles that can be nested and a bunch of updates to kafka and airtable
"""
import res
import re
from res.airtable.misc import PRODUCTION_REQUESTS
from res.flows.meta.ONE.controller import MetaOnePieceManager
from res.flows.meta.ONE.meta_one import MetaOne
from res.flows.make.production.queries import get_airtable_make_one_request
from res.flows.meta.ONE.controller import get_meta_one
import traceback
from schemas.pydantic.make import OnePieceSetUpdateRequest, PrepPrintAssetRequest
from warnings import filterwarnings
from res.media import images
from tenacity import retry, stop_after_attempt, wait_fixed
from res.utils import validate_record_for_topic, ping_slack
from res.flows.make.production.inbox import bump_make_increment
from res.flows.make.production.healing import generate_healing_request_from_ppp_response
from res.flows.make.production.utils import try_slack_event_for_ppp_request

filterwarnings("ignore")  # shapely warnings are annoying

PPP_REQUEST_TOPIC = "res_meta.dxa.prep_pieces_requests"
PPP_RESPONSE_TOPIC = "res_meta.dxa.prep_pieces_responses"
PIECE_OBSERVATION_TOPIC = "res_make.piece_tracking.make_piece_observation_request"
USE_KGATEWAY = True
UPDATE_AIRTABLE = True
MAX_REQUEST_RETRIES = 5
DEFAULT_MAKE_SEQUENCE = 0


def _is_leader(event, use_sku_lock=True):
    """
    two modes for leader. this is a way to get the multiple PPP requests to only send cache once
     the logic is the primary self pieces will lead the caching and the others will await
     the healing pieces should never need to wait for the cache - this would lead to deadlock
     it may be the case we need to refresh things and then heal e.g. if things go back to DXA
     today we cannot really invalidate assets and we need to bump the version which would invalidate the cache
     we should allow healing into a new body version
    acquire sku lock:
     anything processing the sku can lock the ownership of cache creation and others wait
    """
    if not use_sku_lock:
        rank = event.get("metadata", {}).get("rank")
        if rank == "Primary":
            if event.get("piece_type") == "self":
                return True
        return False
    else:
        # here we are trusting either we are locked and working, we failed and released, or the cache is created and nobody cares
        return res.flows.try_acquire_lock(
            event["sku"], hold_minutes=15, context_key="prepare_assets"
        )


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def requeue_request_with_retries(event):
    """
    post a message to kafka with some resilience
    the retry counters are added to the metadata
    we will retry up to N times and then slack the error
    """
    kafka = res.connectors.load("kafka")
    slack = res.connectors.load("slack")
    request_count = event.get("metadata", {}).get("request_count", 0)
    event["metadata"]["request_count"] = request_count + 1
    # dont invalidate the cache on a retry because it leads to an epic cluster fuck.
    if "invalidate_style_annotation_cache" in event["metadata"]:
        del event["metadata"]["invalidate_style_annotation_cache"]
    if request_count > MAX_REQUEST_RETRIES:
        slack.message_channel(
            channel="autobots",
            message=f"[PPP] The request {event['id']} was retried too many times without success. Check if the SKU {event['sku']} is properly cached with annotations",
        )
        return

    return kafka[PPP_REQUEST_TOPIC].publish(event, use_kgateway=USE_KGATEWAY)


def ppp_piece_observation(
    id,
    one_number,
    piece_instance_map,
    one_code=None,
    node="Make.Print.RollPacking",
    contracts_failed=None,
):
    """
    Generate the prepared print pieces observation for node enter
    """

    d = {
        "observed_at": res.utils.dates.utc_now_iso_string(),
        "id": id,
        "node": node,
        "status": "Enter",
        "one_number": one_number,
        "one_code": one_code or f"{one_number}",
        "contracts_failed": contracts_failed or [],
        "pieces": [
            {
                "code": k,
                # the make instance is 0 indexed in kafka but sadly 1 indexed in hasura
                "make_instance": v.get(
                    "make_sequence_number", DEFAULT_MAKE_SEQUENCE + 1
                )
                - 1,
            }
            for k, v in piece_instance_map.items()
        ],
        "metadata": {},
    }

    d = OnePieceSetUpdateRequest(**d).project_pieces().dict()

    # we exclude it for some reason -  needed to be excluded vs no db-write?
    d["id"] = id

    # if we have validation errors debug with this
    # vres = validate_record_for_topic(
    #     d, "res_make.piece-tracking.make_piece_observation_request"
    # )
    # print(vres)

    return d


def get_piece_selection(event, cache, audit_healing_request=False):
    """
    this both formats the selection and makes sure that we have the pieces in the meta one that belong to the event
    we are filtering by material however we also check if its block fuse or not
    anything that is not block fuse is considered as self piece
    we return a map of maps because we also want to track the make increment etc

    the PPP request can have entries like this to control certain counters per piece

    "piece_sequence_numbers": [
       {'key': 'PNTWTWBN-BF', 'make_sequence_number':0}
    ]
    """
    body_code = event.get("body_code")
    body_version = event.get("body_version")
    versioned_body_code = f"{body_code}-V{body_version}"
    is_healing = event.get("metadata", {}).get("rank", "").lower() == "healing"
    piece_seqs = {p["key"]: p for p in event.get("piece_sequence_numbers", [])}

    """
    select the pieces that we care about "KT-6079-V1-BODBKPNL-S",
    """
    pieces = {
        r.piece_code: piece_seqs.get(r.key, {"make_sequence_number": 1})
        for _, r in cache.iterrows()
        if r.key in piece_seqs or len(piece_seqs) == 0
    }

    res.utils.logger.info(f"Pieces in cache: {cache.key.values}")
    res.utils.logger.info(f"Pieces in request: {piece_seqs}")

    if len(pieces) == 0:
        # possibly healing piece without the proper cache path and where a version update has happened lets just hope for the best
        unversioned_pieces = {
            re.sub("V[0-9]+", "", k): v for k, v in piece_seqs.items()
        }
        cache["unversioned_key"] = cache["key"].apply(
            lambda k: re.sub("V[0-9]+", "", k)
        )
        pieces = {
            r.piece_code: unversioned_pieces[r.unversioned_key]
            for _, r in cache.iterrows()
            if r.unversioned_key in unversioned_pieces
        }

    try:
        if audit_healing_request and is_healing:
            instances = bump_make_increment(
                event.get("id"),
                int(event.get("one_number")),
                pieces.keys(),
                versioned_body_code=versioned_body_code,
            )
    except:
        # SA: safety since i dont understand if this needs to be called yet - we just want a record of each healing request from somewhere
        res.utils.logger.warn("Tried and failed to audit the healing")

    return pieces


def update_mone(mone_id, contracts, **kwargs):
    """
    do we need to make attachments - probably not since another process can do that now
    we just need to add or remove contract state
    """

    from res.flows.meta.ONE.contract.ContractVariables import ContractVariables

    df = ContractVariables.load(base_id="appH5S4hIuz99tAjm")
    crid = dict(df[df["variable_name"] == "Prep pieces preview Failed"].iloc[0])[
        "record_id"
    ]
    existing = PRODUCTION_REQUESTS.get(mone_id)["fields"].get("Contract Variables", [])

    if crid not in existing and contracts:
        existing = [crid] + existing
    elif crid in existing and not contracts:
        existing = [e for e in existing if e != crid]

    res.utils.logger.info(f"Updating record")

    if UPDATE_AIRTABLE:
        PRODUCTION_REQUESTS.update(mone_id, {"Contract Variables": existing})
    else:
        res.utils.logger.info(f"Airtable updates off. Payload: {record}")


def update_print_assets(asset, contracts, **kwargs):
    """
    update all the PPP variables using the ppp asset input
    update any contract state

    TODO: double check the nesting contract - we cannot nest if we dont PPP so flagging and unflagging does not do anything
          adding the contract variables is enough
    """
    print_queue = res.connectors.load("airtable")["apprcULXTWu33KFsh"][
        "tblwDQtDckvHKXO4w"
    ]
    s3 = res.connectors.load("s3")
    record = {
        "record_id": asset.get("id"),
        "Prepared Pieces Count": asset.get("piece_count"),
        # if we distinguish physical properties take them instead
        "Prepared Pieces Area": asset.get(
            "area_pieces_yds_physical", asset.get("area_pieces_yds")
        ),
        "Prepared Pieces Nest Length": asset.get(
            "height_nest_yds_physical", asset.get("height_nest_yds")
        ),
        "Piece Validation Flags": contracts,
        "Prepared Pieces Key": asset.get("piece_set_key"),
        "Prepared Pieces Preview": [
            {"url": purl}
            for purl in [
                s3.generate_presigned_url(f)
                for f in [p["filename"] for p in asset.get("make_pieces") or []]
            ]
        ],
        "PPP Job Key": kwargs.get("ppp_key"),
    }

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
    def _update_with_retry():
        if UPDATE_AIRTABLE:
            return print_queue.update_record(record)
        else:
            res.utils.logger.info(f"Airtable updates off. Payload: {record}")
            return record

    return _update_with_retry()


def cache_handler(event, context={}):
    """
    this is a version of PPP that precaches what we would use
    it does not relay info to other systems
    this can be involved by requesting PPP requests with flow==cache_only
    """

    # ist about time we just added these on the docker image!

    images.text.ensure_s3_fonts()
    images.icons.extract_icons()

    with res.flows.FlowContext(event, context) as fc:
        try:
            res.utils.logger.info("Running the cache handler")
            sku = event["sku"]
            MetaOnePieceManager.write_meta_one_cache(sku)
        except:
            res.utils.logger.warn(f"Encountered error: {traceback.format_exc()}")
            metric = res.utils.logger.metric_node_state_transition_incr(
                node="asset-caching", asset_key=sku, status="EXCEPTION"
            )
            res.utils.logger.info(f"{metric=}")
            raise
        finally:
            pass
    return {}


# , disk="5G"
def handler(event, context={}, use_sku_lock=True, **kwargs):
    """
    handles vs ppp responses or similar kafka message

    talk to Rob about the following things in old ppp not (yet) in there
    - the bias pieces are normalized in ppp - do we still do that in this process
    - the label * 50 stuff should be moved upstream to caller and added into the piece multiplicity thing
    - we no longer Flag print assets when they fail PPP but we add the contract variable as the indicator it failed
    - make sure we run some gates for **trims, stampers and factory PDF*  in the make asset endpoints before we call ppp + add to MONE
    """
    ##with res.flows.FlowContext(event,context) as fc: <- enable this when we remove the outer caller for metrics etc

    # ist about time we just added these on the docker image!
    images.text.ensure_s3_fonts()
    images.icons.extract_icons()

    kafka = res.connectors.load("kafka")
    sku = event["sku"]
    body_version = event["body_version"]
    contracts_failed = []
    one_number = event["one_number"]
    print_asset_record_id = event["id"]
    invalidate_cache = event.get("metadata", {}).get(
        "invalidate_style_annotation_cache", False
    )
    # request["metadata"]["logs"] = fc.log_path TODO

    # this could run here OR in the make production request end point
    # it makes sense here only if we do one ppp per asset and not split them here by materials etc.
    # reconcile_order_for_sku(event["order_number"], sku)

    try:
        # the event doubles as the response for now for this flow since it carries some info
        ppp = PrepPrintAssetRequest(**event)
        """
        SOFT SWAP IN EFFECT: see also creation of print asset - the request is expected to come in for the "wrong" material
        Here we just want to load the meta one as though it was in the other material and later we want to select the pieces that match
        """
        # this is to make sure old healing requests are automatically pushed into the new material - safety
        if event["material_code"] == "LSG19":
            event["material_code"] == "CHRST"
        # this is to pretend to be in the other material to match new requests in L
        ###########################################################
        contracts_failed = []

        """
        0] prepare the cache or requeue non leader e.g. combos while waiting for the cache
        we cache annotations at the sku level one time but here we just check we have them or "burn in" 
        we should also have (API) schema version check so if we bump the version we can invalidate cache
        in the below block we only need to cache once per sku but we cheat and let any leader build the cache
        a leader is the primary self unless sku-lock is used and this its first-leads 
        if two skus are ordered they will both try to build the cache and the first one will get the lock
        """

        if event.get("meta_one_cache_path") is not None:
            res.utils.logger.info(
                f"Loading meta one cache from path on the event: {event.get('meta_one_cache_path')}"
            )
            result = MetaOnePieceManager.read_meta_one_cache(
                sku, cache_path=event.get("meta_one_cache_path")
            )
        elif not MetaOnePieceManager.is_cache_ready(sku) or invalidate_cache:
            if _is_leader(event, use_sku_lock=use_sku_lock):
                # try_slack_event_for_ppp_request(event, "Caching annotations for SKU")
                result = MetaOnePieceManager.write_meta_one_cache(
                    sku,
                    m1=get_meta_one(sku, apply_soft_swaps={"LSG19": "CHRST"}),
                )
                metric = res.utils.logger.metric_node_state_transition_incr(
                    node="asset-caching", asset_key=sku, status="BUILDING_CACHE"
                )
                ping_slack(f"[PPP] Cached pieces for {sku}", "autobots")
            else:
                res.utils.logger.info(
                    f"The cache is not ready - we will let the leader build it and try again later"
                )
                metric = res.utils.logger.metric_node_state_transition_incr(
                    node="asset-caching", asset_key=sku, status="PENDING_CACHE"
                )
                # pop it back on the queue
                requeue_request_with_retries(event)
                return
        else:
            # try_slack_event_for_ppp_request(event, "Loading cached annotations for request")
            result = MetaOnePieceManager.read_meta_one_cache(sku)

        """
        although we cached for the entire SKU we can send prep print requests for each print asset
        below are numbered 1,2,3,4 side effects
        """
        # as a map: this schema  maps make_sequence_number -> make_instance
        # SA adding a flag audit healing request to true to make sure we store for all pieces the record of record_id+piece_name
        requested_pieces_sequences = get_piece_selection(
            event, result, audit_healing_request=True
        )
        # build the pieces map for the groups that were selected in the request
        res.utils.logger.info(
            f"This requests has the following piece groups\n{requested_pieces_sequences}"
        )

        """
        1 the prep piece response will have the stats etc which we now read from the piece material stats
        """
        ppp = MetaOnePieceManager.prepare_prep_piece_response(
            request=event,
            cache=result,
            piece_instance_map=requested_pieces_sequences,
        )
        res.utils.logger.info(f"Writing ppp response: {ppp}")
        kafka[PPP_RESPONSE_TOPIC].publish(ppp.dict(), use_kgateway=USE_KGATEWAY)

        """
        2 Update piece observations - these kafka messages have become the main point of all this now
          and other contract variable states
        """
        piece_observations = ppp_piece_observation(
            id=print_asset_record_id,
            one_number=one_number,
            contracts_failed=contracts_failed,
            piece_instance_map=requested_pieces_sequences,
        )
        kafka[PIECE_OBSERVATION_TOPIC].publish(
            piece_observations, use_kgateway=USE_KGATEWAY
        )
        ping_slack(f"[PPP] Prepared assets for {one_number} ({sku})", "autobots")

        generate_healing_request_from_ppp_response(ppp.dict())
    except:
        res.utils.logger.warn(f"Encountered error: {traceback.format_exc()}")
        contracts_failed.append(["Prep pieces preview Failed"])
        metric = res.utils.logger.metric_node_state_transition_incr(
            node="asset-caching", asset_key=sku, status="EXCEPTION"
        )
        # try_slack_event_for_ppp_request(event, "There was an error handling the request") #<-good ideas to create a signed url log link fc.log_path_presigned
        res.utils.logger.info(f"{metric=}")
        ping_slack(
            f"[PPP] Failed to prepare assets for {one_number} ({sku}) -- {kwargs.get('ppp_key')}: ```{traceback.format_exc()}```",
            "autobots",
        )
        raise
    finally:
        """
        3,4 set contracts variables on two bases - if contracts variables are empty the removal action is taken for a known set for this node
            set any flags or other things like asset links
        """

        update_mone(event["upstream_request_id"], contracts=contracts_failed)
        update_print_assets(
            ppp.dict(),
            contracts=contracts_failed,
            ppp_key=kwargs.get("ppp_key"),
        )

    return {"contracts_failed": contracts_failed}
