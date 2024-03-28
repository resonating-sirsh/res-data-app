from res.flows.api.core.node import FlowApiNode
from schemas.pydantic.meta import (
    BodyMetaOneRequest,
    BodyMetaOneResponse,
    UpsertBodyResponse,
)
import res
from res.flows.meta.ONE.meta_one import BodyMetaOne
from res.flows.api.core.node import FlowApiNode
from res.flows.meta.ONE.geometry_ex import (
    make_body_cutfiles,
    make_body_printable_pieces_cutfiles,
)
from res.flows.dxa.styles import queries
from res.flows.meta.body.unpack_asset_bundle.body_db_sync import (
    sync_body_to_db_by_versioned_body,
)
import itertools
import traceback
from res.flows.api.core.node import SHARED_STATE_KEY
from res.utils.env import RES_ENV
from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE
from tenacity import retry, wait_fixed, stop_after_attempt
import pandas as pd
from warnings import filterwarnings
from res.flows.meta.ONE.style_piece_deltas import request_style_tests_for_body
from res.connectors.airtable import AirtableConnector
from res.connectors.s3 import fetch_to_s3
from res.utils import ping_slack

filterwarnings("ignore")

#  using the meta.ONE base as the bodies base
AIRTABLE_QUEUE_BASE_ID = (
    AIRTABLE_ONE_PLATFORM_BASE if RES_ENV != "production" else "appa7Sw0ML47cA8D1"
)
REQUEST_TOPIC = "res_meta.dxa.body_update_requests"


def sync_meta_assets():
    try:
        from res.flows.meta.ONE.trims import run_sync as sync_trims
        from res.flows.meta.ONE.fusings import run_sync as sync_fusing

        """
        check for changes on the body queue before checking if we need to check for fusing and trim changes
        """
        tab = res.connectors.load("airtable")["appa7Sw0ML47cA8D1"]["tblrq1XfkbuElPh0l"]
        # df_raw = tab.updated_rows(
        #     minutes_ago=15,
        #     last_modified_field="Status Last Updated At",
        #     fields=[
        #         "Body Number",
        #         "Active Body Version",
        #         "Body ONE Ready Request Flow JSON",
        #         "Status Last Updated At",
        #     ],
        # )

        # if len(df_raw):
        sync_trims()
        sync_fusing()
        res.utils.logger.info(f"Sync of meta assets complete")
    except:
        res.utils.logger.warn(
            f"Failed when trying to sync fusings and trims {traceback.format_exc()}"
        )

    return {}


def ensure_dependencies(body_code, body_version):
    """
    normally we have on-boarded the body and style before we add the color pieces.... but you never know
    """
    if not len(queries.get_body_header(body_code, body_version)["meta_bodies"]):
        res.utils.logger.info(
            f"We need to import the body {body_code}-V{body_version} which does not exist yet"
        )
        sync_body_to_db_by_versioned_body(body_code, body_version)


def get_contract_failures_from_latest_job(body_code, body_version=None):
    from res.flows.meta.utils import parse_status_details_for_issues

    contracts_failed = []
    contract_failure_context = {}

    try:
        hasura = res.connectors.load("hasura")
        LATEST_BODY_JOB = """
            query get_latest_body_job($details: jsonb) {
                dxa_flow_node_run(
                    where: {
                        preset_key: {_eq: "body_bundle_v0"}
                        details: {_contains: $details}
                    }
                    limit: 1
                    order_by: {ended_at: desc}
                ) {
                    id
                    details
                    status
                    status_details
                }
            }
        """

        details = {"body_code": body_code}
        details = (
            details
            if body_version is None
            else {**details, "body_version": body_version}
        )

        result = hasura.execute_with_kwargs(LATEST_BODY_JOB, details=details)
        if len(result["dxa_flow_node_run"]) == 0:
            res.utils.logger.warn(f"No body job found for {body_code} v{body_version}")
            return [], {}
        else:
            body_job = result["dxa_flow_node_run"][0]
            status = body_job["status"]
            status_details = body_job["status_details"]

            if status == "COMPLETED_ERRORS":
                issues = parse_status_details_for_issues(status_details)

                contracts_failed = [issue["issue_type_code"] for issue in issues]
                contract_failure_context = {
                    issue["issue_type_code"]: issue["context"] for issue in issues
                }
    except Exception as ex:
        res.utils.logger.warn(f"Failed to get the latest body job {ex}")
    finally:
        return contracts_failed, contract_failure_context


def generator(event, context={}):
    """
    Generate the events from kafka topics or similar inputs
    It may be that based on the UNIT_TYPE in this payload we will map the payload for bodies and groups separately
    updating the kafka messages with standard res_entity_type might be a useful hint too

    should not have to do this for the style pieces change event but also regression testing for the meta one request

    TODO: support expand sizes on the style pieces change event

    d = {
        'body_code': "BG-2004",
        'body_version': 1,
        "sample_size": 'ZZZ04',
        "size_code": 'ZZZ04',
        "sizes" : ['ZZZZ0', 'ZZZ10', 'ZZZ06', 'ZZZ12', 'ZZZ14', 'ZZZ08', 'ZZZ02', 'ZZZ04']
    }
    """

    def decorate_assets(assets):
        """
        clean up the request for the meta one node
        add sample size if not already added in the accounting format
        add material properties
        add node discriminator for loading the right asset type in the queue i.e. Body or Style
        """

        def pop_queue_id(d):
            if "metadata" not in d:
                d["metadata"] = {}
            if "id" in d:
                qid = d.pop("id")
                # the queue.id will be used by system but dropped py pydantic so we move to metadata too
                d["metadata"]["queue_id"] = d["queue.id"] = qid
            return d

        def mod(a):
            a = pop_queue_id(a)
            a["queue.key"] = f"{a['body_code']}-V{a['body_version']}"
            a["res_entity_type"] = "meta.BodyMetaOneRequest"
            return a

        return [mod(a) for a in assets]

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

        metadata_provider = lambda asset: (
            {"memory": "1G"}
            if asset.get("sample_size") != asset.get("size_code")
            else {"memory": "50G"}
        )
        if len(assets):
            if len(assets) > 1:
                # if there are any changes we check if we need to sync meta assets
                sync_meta_assets()

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


def reducer(event, context={}):
    node = FlowApiNode(
        BodyMetaOneRequest,
        response_type=BodyMetaOneResponse,
        base_id="appa7Sw0ML47cA8D1",
    )

    with res.flows.FlowContext(event, context) as fc:
        for rid, key in fc.restore_queue_ids().items():
            # override the one used in the session

            status = fc.get_shared_cache_for_set(rid)[SHARED_STATE_KEY] or {}
            keys = list(status.keys())
            defects = list(set(itertools.chain(*(status.values()))))
            # could this be a body meta one response
            res.utils.logger.info(f"{key} keys: {keys}")
            res.utils.logger.info(f"{key} failed_contracts: {defects}")
            body_version = int(key.split("-")[-1].replace("V", ""))
            body_code = "-".join(key.split("-")[:2])
            # if we know the version of the body we can do this
            # note that we resolve the record id for the response for this node here
            make_body_printable_pieces_cutfiles(
                body_code, body_version, upload_record_id=node.key_cache[key]
            )

        process_user_queue(fc=fc)
        # we could use this to update the previews etc. but saver to do it in the nodes save
    return {}


def process_user_queue(last_run_time_delta_mins=None, fc=None):
    """
    We are flushing the json on change rows to a kafka topic for metrics
    We are sometimes lifting contracts from the meta one
    """
    from res.flows.meta.ONE.queries import (
        _publish_events,
        publish_body_queue_changes_as_model,
    )

    try:
        res.utils.logger.info("Processing queue changes...")
        watermark = res.flows.watermark.get("meta.one.body_node")

        if watermark:
            res.utils.logger.debug(f"found a process watermark {watermark}")

        else:
            watermark = res.utils.dates.utc_minutes_ago(10)
            res.utils.logger.debug(
                f"choosing a default watermark date from params {watermark}"
            )

        # determine a value based on a default or cached watermark
        _last_run_time_delta_mins = (
            res.utils.dates.utc_now() - watermark
        ).total_seconds() / 60

        # hard code for now
        _last_run_time_delta_mins = 60

        # use the one passed in for testing or the cached
        last_run_time_delta_mins = last_run_time_delta_mins or _last_run_time_delta_mins
        if last_run_time_delta_mins == -1:
            res.utils.logger.info("Disabled watermark - fetch everything")
            last_run_time_delta_mins = None
            watermark = None

        data = BodyMetaOneNode.get_body_queue_changes(
            window_minutes=last_run_time_delta_mins
        )
        # this is specifically the top level exit date
        res.utils.logger.info(
            f"There are {len(data)} events in window {last_run_time_delta_mins} minutes"
        )

        if len(data) == 0:
            return

        topic = res.connectors.load("kafka")["res_meta.dxa.body_status_updates"]
        res.utils.logger.info(f"Sending queue changes to kafka...")

        _publish_events(
            data,
            topic=topic,
            watermark=res.utils.dates.utc_days_ago(2),
            key="body_code",
            process="body-queue",
        )

        # try:
        #     res.utils.logger.info(f"Publishing queue")
        #     publish_body_queue_changes_as_model(data)
        #     res.utils.logger.info(f"Published queue")
        # except Exception as ex:
        #     res.utils.logger.info(f"Publish failed {ex}")

        res.flows.watermark.set("meta.one.body_node")

        # TODO: any updates to body meta one contracts here

        res.utils.logger.info(f"Flow changes processed")

        return data
    except:
        res.utils.logger.warn("Failed update body status queue updates")
        res.utils.logger.warn(traceback.format_exc())


def handle_invalidations_task(event, context={}):
    """
    we consume from kafka to update the meta one node status with failed contracts at body or style

    """
    return {}


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def handler(e, c={}):
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

    ignore_warnings()

    with res.flows.FlowContext(e, c) as fc:
        # 1. these are serialized into schema objects including decompression if needed
        for asset in fc.decompressed_assets:
            res.utils.logger.debug(asset)
            # 2. this handles status transactions - a well behaved node will fail gracefully anyway. Here we also manage some casting to a strong type
            with fc.asset_processing_context(asset) as ac:
                try:
                    res.utils.logger.debug(f"Processing asset of type {type(ac.asset)}")
                    # lesson: notice the response type, we receive data on the request but we are always sending updates using the response object
                    node = BodyMetaOneNode(
                        ac.asset, response_type=BodyMetaOneResponse, queue_context=fc
                    )
                    res.utils.logger.debug(
                        f"saving asset @ {node} - will update queue for sample size: {ac.asset.is_sample_size}"
                    )

                    # experimental queue update logic: relay to kafka and airtable and as its a relay dont try to save anything to hasura which we will do in the reducer for this node
                    _ = node.save_with_relay(
                        queue_update_on_save=ac.asset.is_sample_size
                    )
                except Exception as ex:
                    res.utils.logger.warn("Failed to process asset")
                    if ac.asset.is_sample_size:
                        send_exception_to_slack(asset, repr(ex))
    return {}


def kafka_request(request):
    """
    here we schedule the preview or whatever in a triggered request
    """
    try:
        request_new = BodyMetaOneRequest(**request)
        kafka = res.connectors.load("kafka")
        kafka[REQUEST_TOPIC].publish(request_new.dict(), use_kgateway=True)
    except:
        res.utils.logger.warn(traceback.format_exc())


class BodyMetaOneNode(FlowApiNode):
    # we handle these types
    atype = BodyMetaOneRequest
    # when in doubt, this is our unique node id
    uri = "one.platform.meta.body"

    def __init__(self, asset, response_type=None, queue_context=None):
        super().__init__(
            asset,
            response_type=response_type,
            queue_context=queue_context,
            base_id=AIRTABLE_QUEUE_BASE_ID,
        )
        self._name = BodyMetaOneNode.uri

    def update_preview_links(self, body_code, body_version, paths):
        """"""

        rid = self.key_cache[f"{body_code}-V{body_version}"]

        res.utils.logger.debug(f"recover rid {rid}")

        from res.flows.meta.pieces import PieceName

        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")

        def is_print_path(u):
            p = u.split("/")[-1].split(".")[0]
            p = PieceName(p)
            return p.is_printable

        pfiles = [
            {"url": s3.generate_presigned_url(u)} for u in paths if is_print_path(u)
        ]
        cfiles = [
            {"url": s3.generate_presigned_url(u)} for u in paths if not is_print_path(u)
        ]

        res.utils.logger.info(f"Updating preview files ")
        res.utils.logger.debug(f"{pfiles=}, {cfiles=}")
        airtable["appa7Sw0ML47cA8D1"]["tbl0ZcEIUzSLCGG64"].update_record(
            {
                "record_id": rid,
                "Printable Pieces": pfiles,
                "Complementary Pieces": cfiles,
            }
        )

        # Complementary Pieces

    def _save(
        self,
        typed_record: BodyMetaOneRequest,
        body_version_override=None,
        hasura=None,
        plan=False,
        on_error=None,
        ensure_deps=True,
        **kwargs,
    ):
        # the data have already been saved
        # TODO create the stampers in the unpack
        body_version = body_version_override or typed_record.body_version

        res.utils.logger.info(f"------------Running the body node ------------")
        res.utils.logger.info(f"{typed_record.dict()}")
        res.utils.logger.info(f"------------                      ------------")
        try:
            if ensure_deps:
                ensure_dependencies(
                    body_code=typed_record.body_code, body_version=body_version
                )

            bor = None
            bor_status = None

            bm = BodyMetaOne(
                typed_record.body_code, body_version, typed_record.size_code
            )

            preview_mode_enabled = typed_record.metadata.get("make_previews", False)

            contracts_failed = typed_record.contracts_failed or []
            contract_failure_context = typed_record.contract_failure_context or {}

            # these are stamper files and whatnot - we save all for some sizes and more for sample sizes + make the previews
            bm.save_complementary_pieces()
            if typed_record.is_sample_size:
                make_body_cutfiles(bm.body_code, bm.body_version)

                if preview_mode_enabled:
                    paths = bm.build_asset_previews()
                    self.update_preview_links(
                        typed_record.body_code, typed_record.body_version, paths
                    )
                    # return None to not save anything on the node
                    return

                    # after building the asset previews fetch existing contracts
                else:
                    e = BodyMetaOneNode.refresh(
                        typed_record.body_code,
                        typed_record.body_version,
                        contract_failures=contracts_failed,
                        contract_failure_context=contract_failure_context,
                        make_previews=True,
                        sample_size_only=True,
                        single_instance=True,
                        plan=False,
                    )
                try:
                    b = BodyMetaOneNode.get_body_as_request(bm.body_code)
                    bor = b["body_one_ready_request"]
                    bor_status = b["body_one_ready_request_status"]
                except:
                    res.utils.logger.warn(f"Failed to get the body request data")

            # final checks - for now it's just one
            self.check_each_piece_has_a_zone(
                [p for p in bm], contracts_failed, contract_failure_context
            )

            # status at node collects everything we want to say about the asset at this node
            response = bm.status_at_node(
                sample_size=typed_record.sample_size,
                request=typed_record,
                object_type=BodyMetaOneResponse,
                bootstrap_failed=contracts_failed,
                contract_failure_context=contract_failure_context,
                previous_differences=typed_record.previous_differences or {},
                trace_log=self.try_get_log_path(),
                body_request_id=bor,
            )

            if typed_record.is_sample_size:
                res.utils.logger.info(
                    f"Making updates for the meta one response with the BORR id {response.body_one_ready_request}"
                )

                BodyMetaOneNode.move_body_request_to_node_for_contracts(
                    response.body_one_ready_request,
                    contracts_failed,
                    current_status=bor_status,
                )

            # redis cache across jobs
            self.try_update_shared_state(
                typed_record.size_code, response.contracts_failed
            )

            if response.contracts_failed and typed_record.is_sample_size:
                send_contract_failures_to_slack(response.dict())

            # disable for now
            try:
                res.utils.logger.info(
                    f"Making a sample available of nested print files"
                )
                BodyMetaOneNode.nest_body_into_material(
                    body_code=typed_record.body_code,
                    # load the default one
                    material_codes=None,
                    body_version=typed_record.body_version,
                    attach_airtable=True,
                )

                pass
                # for now we are only going to check this if the body version is greater than once since there will not even be any styles yet otherwise
                # if typed_record.body_version > 1:
                #     request_style_tests_for_body(
                #         typed_record.body_code, typed_record.body_version
                #     )
            except:
                res.utils.logger.info(
                    f"***Failed to check styles for changes***\n {traceback.format_exc()}"
                )

            return response
        except:

            # ping sirsh on slack here
            ping_slack(
                f"<@U01JDKSB196> - Fatal error in the body node - go check it out {traceback.format_exc()}",
                "sirsh-test",
            )

            res.utils.logger.warn(
                f"Failing in the body node posting status - error is {traceback.format_exc()}"
            )

            d = typed_record.dict()
            d["contracts_failed"] = d["contracts_failed"] + ["BODY_ONE_READY"]
            if len(d["contracts_failed"]) > 0:
                d["status"] = "Failed"
                if typed_record.is_sample_size:
                    send_contract_failures_to_slack(d)
            response = BodyMetaOneResponse(
                **d,
                trace_log=traceback.format_exc(),
            )

            BodyMetaOneNode.move_body_request_to_node_for_contracts(
                response.body_one_ready_request,
                d["contracts_failed"],
                current_status=bor_status,
            )

            """
            TODO: we have not saved contracts to the database but we could do it here
            """
            return response

    def check_each_piece_has_a_zone(
        self, pieces=[], contracts_failed=[], contract_failure_context={}
    ):
        try:
            """
            we can check that each piece has a zone
            """
            airtable = res.connectors.load("airtable")
            piece_qualifiers = airtable["appa7Sw0ML47cA8D1"]["tbllT2w5JpN8JxqkZ"]
            pqdf = piece_qualifiers.to_dataframe()
            pqdf = pqdf[
                (pqdf["Commercial Acceptability Zone"].notnull())
                & (pqdf["Tag Category"] == "Part Tag")
            ]

            missing_zones = []
            for p in pieces:
                part_tag = p.piece_name.part
                if part_tag not in pqdf["Tag Abbreviation"].values:
                    missing_zones.append(p.key)

            if len(missing_zones):
                contracts_failed.append("NO_PARTS_WITHOUT_ZONES")
                contract_failure_context["NO_PARTS_WITHOUT_ZONES"] = missing_zones
        except:
            pass

    @staticmethod
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
    def move_body_request_to_node_for_contracts(
        request_id, contracts, current_status=None, plan=False
    ):
        """
        resolve the status from the contracts
        https://coda.io/d/Resonance-Contract-Implementation_dvjyCY1qJSp/Contracts-Definition-Implementation-Report_sucM0?utm_source=slack#Contract-Priorities_tuqWU/r1

        the

        """
        from res.flows.meta.ONE.contract import ContractVariables

        if not request_id:
            res.utils.logger.warn(
                f"We were not sent an airtable record id for BORR so we cannot update the status"
            )
            return

        if current_status:
            if current_status in ["Done", "Cancelled", "Rejected"]:
                res.utils.logger.info(
                    f"We will not move the body meta one response if its in an advanced state state - contracts are {contracts}"
                )
                return

        DONE_NODE = "Upload Simulation"
        node = DONE_NODE
        airtable = res.connectors.load("airtable")
        tab = airtable["appa7Sw0ML47cA8D1"]["tblrq1XfkbuElPh0l"]
        status_cols = []
        node_statuses = {}
        if contracts:
            res.utils.logger.info(
                f"the contracts {contracts} will be used to resolve the node..."
            )
            node, node_statuses = (
                ContractVariables.resolve_node_from_contracts(contracts) or node
            )
            res.utils.logger.info(
                f"matched the following nodes to failed contracts {node_statuses}"
            )

        if node:
            res.utils.logger.info(
                f"Moving request {request_id} to node/status [{node}]"
            )
            record = {
                "record_id": request_id,
                "Status": node,
                "Validate Body Meta ONE Response_Status": (
                    "Done" if node == DONE_NODE else None
                ),
                **node_statuses,
            }

            if plan:
                return record
            try:
                res.utils.logger.info(f"Updating BORR with {record}")
                tab.update_record(record)
            except Exception as ex:
                # send_exception_to_slack(
                #     {f"body_one_ready_request": request_id},
                #     f"Failing to update determined nodes for contracts {contracts} we tried to update with {node_statuses}",
                # )
                # in case of schema changes etc
                res.utils.logger.warn(
                    f"Failed to update table ({ex}) trying without status cols"
                )
                for c in status_cols:
                    record.pop(c)
                tab.update_record(record)

    def get_body_metadata(body_code):
        Q = """query body($code: String){
                body(number:$code){
                name
                code
                availableSizes{
                    code
                }
                basePatternSize{
                    code
                }
                patternVersionNumber
             }
            }"""
        g = res.connectors.load("graphql")
        r = g.query_with_kwargs(Q, code=body_code)
        d = r["data"]["body"]
        d["body_code"] = d["code"]
        d["body_version"] = d["patternVersionNumber"]
        d["id"] = res.utils.res_hash()
        d["sizes"] = [f["code"] for f in d["availableSizes"]]
        d["sample_size"] = d["basePatternSize"]["code"]
        return d

    @staticmethod
    def get_body_queue_changes(window_minutes=60):
        """
        we can look at recent stage changes on the queue and take some actions
        for example for things recently exiting we should check that contracts are lifted
        """
        from res.flows.meta.ONE.queries import get_body_queue_changes

        return get_body_queue_changes(window_minutes=window_minutes)

    @staticmethod
    def get_body_as_request(body_code, size_code=None):
        """
        we can coerce this payload to kick off work at

        res.connectors.load('kafka')['res_meta.dxa.body_update_requests'].publish(payload, use_kgateway=True, coerce=True)

        """

        Q = """query body($code: String){
                body(number:$code){
                name
                code
                availableSizes{
                    code
                }
                basePatternSize{
                    code
                }
                bodyOneReadyRequests(
                    first: 10
                    sort: [{ field: CREATED_AT, direction: DESCENDING }]
                    ) {
                    bodyOneReadyRequests {
                        id
                        bodyVersion
                        status
                    }
                }
                patternVersionNumber
                pieces{
                id
                code
                pieceQualifiersDictionaries{
                    id
                    name
                }
                }
            }
            }"""
        g = res.connectors.load("graphql")
        r = g.query_with_kwargs(Q, code=body_code)
        d = r["data"]["body"]

        bor = d.get("bodyOneReadyRequests", {}).get("bodyOneReadyRequests", [])
        # im not sure if its worth filtering here - in this mode the latest will always be the latest
        # in future if we support multiple bodies at a time we could maybe implement a filter here
        # bor = [b for b in bor if b["bodyVersion"] == d["patternVersionNumber"]]
        d["body_one_ready_request"] = bor[0].get("id") if len(bor) else None
        d["body_one_ready_request_status"] = bor[0].get("status") if len(bor) else None
        d["body_code"] = d["code"]
        d["body_version"] = d["patternVersionNumber"]
        d["id"] = res.utils.res_hash()
        d["sizes"] = [f["code"] for f in d["availableSizes"]]
        d["sample_size"] = d["basePatternSize"]["code"]
        d["piece_name_mapping"] = [
            {"key": f"{d['body_code']}-V{d['body_version']}-{f['code']}"}
            for f in d["pieces"]
        ]
        d["metadata"] = {}
        d["created_at"] = res.utils.dates.utc_now_iso_string()
        d["size_code"] = size_code
        return d

    @staticmethod
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
    def get_contracts_for_body(body_code, body_version, style_body_version=None):
        """
        check the body code and one or more body versions to see what meta one contracts they have
        """

        airtable = res.connectors.load("airtable")
        body_versions = (
            [body_version]
            if not style_body_version
            else list(set([body_version, style_body_version]))
        )
        pred = f""" AND({{Body Code}}='{body_code}', {AirtableConnector.make_key_lookup_predicate( body_versions, 'Body Version')})"""
        data = airtable["appa7Sw0ML47cA8D1"]["tbl0ZcEIUzSLCGG64"].to_dataframe(
            filters=pred,
            fields=["Body Code", "Body Version", "Contracts Failed Codes"],
        )
        if len(data):
            data = res.utils.dataframes.replace_nan_with_none(data)
            return data.drop("__timestamp__", 1).to_dict("records")
        return None

    @staticmethod
    def get_static_meta_one_borr_payload(
        body_code, tech_pack, dxf_file, record_id=None
    ):
        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")

        if not record_id:
            try:
                record_id = airtable.get_table_data(
                    "appa7Sw0ML47cA8D1/tblXXuR9kBZvbRqoU",
                    filters=f"OR({{Body Number}}='{body_code}')",
                    fields=["Body", "Pattern Version Number"],
                ).iloc[0]["record_id"]
            except:
                raise Exception(f"could not load body {body_code} from bodies table")

        as_attachment = lambda uri: (
            [{"uri": s3.generate_presigned_url(uri)}] if uri else None
        )

        return {
            "body_id": record_id,
            "requested_by_email": "techpirates@resonance.nyc",
            "body_one_ready_request_type": "Static Meta ONE",
            "requestor_layers": ["Platform"],
            "body_one_ready_flow": "2D",
            # "techpack_files": as_attachment(tech_pack),
            # "pattern_files": as_attachment(dxf_file),
        }

    @staticmethod
    def resolve_body_attribute_ids(
        name,
        category_name,
        onboarding_materials,
        onboarding_combo_materials=None,
        onboarding_lining_materials_ids=None,
        size_scale_name=None,
        base_size=None,
        brand_code="TT",
    ):
        airtable = res.connectors.load("airtable")

        def _get_table_key_lookup(table, key_column_name, alternate_record_id=None):
            cols = (
                [key_column_name, "Created At"]
                if not alternate_record_id
                else [key_column_name, alternate_record_id]
            )

            data = airtable.get_table_data(table, fields=cols)
            data = dict(
                data[[key_column_name, alternate_record_id or "record_id"]].values
            )
            return data

        res.utils.logger.info(f"resolving ids, please wait...")
        materials = _get_table_key_lookup("appa7Sw0ML47cA8D1/tbl89gdhA3JZQmyYs", "Key")
        size_scales = _get_table_key_lookup(
            "appa7Sw0ML47cA8D1/tblS3AkeOdjaTTUTj", "Name"
        )

        # $i think this needs to be the meta one sizes not the older
        sizes = _get_table_key_lookup(
            "appjmzNPXOuynj6xP/tblvexT7dliEamnaK",
            "Size Chart",
            alternate_record_id="__record_id",
        )
        categories = _get_table_key_lookup(
            "appa7Sw0ML47cA8D1/tblTZrz94jj2txsqV",
            "Name_",
            alternate_record_id="_record_id",
        )

        def material_resolver(l):
            if not l:
                return l
            if not isinstance(l, list):
                l = [l]
            return [materials.get(i) for i in l]

        return {
            "name": name,
            "brand_code": brand_code,
            "category_id": categories.get(category_name),
            # "cover_images_files_ids": [
            #   "6124ec0ee4589d0008fa8be6"
            # ],
            "onboarding_materials_ids": material_resolver(onboarding_materials),
            "onboarding_combo_material_ids": material_resolver(
                onboarding_combo_materials
            ),
            "onboarding_lining_materials_ids": material_resolver(
                onboarding_lining_materials_ids
            ),
            "size_scales_ids": [size_scales.get(size_scale_name)],
            "base_pattern_size_id": sizes.get(base_size),
            "created_by_email": "techpirates@resonance.nyc",
        }

    @staticmethod
    def create_body(
        name,
        size_scale,
        base_size,
        body_piece_codes,
        category,
        cover_image_uri=None,
        tech_pack_uri=None,
        dxf_uri=None,
        brand_code="TT",
        plan=True,
        onboarding_materials=None,
        create_borr=False,
        _existing_body_code=None,
    ):
        """
        This is temp: should ask Lorelay for proper end point to create a body + initial request/pipeline. might be useful to at once i.e register a body on a flow straight away
        creates a minimal body in airtable and generate a request
        we can add this to the meta one api eventually
        currently there are various fields im not sure i need to create and im not sure the proper way to add size data etc.

        not sure what happens on BORR for lots of those gates (construction mapping, sewing minutes etc)

        lorelay had attachment fields as text which i think was largely to support versioning but i think attachment fields might work IF we use semantic S3 links for named files with versions and its nicer for the user experience
        also cover images i attached from s3 but i think the system uses mongo files normally


        not sure what happens when we add a request on top of a request?
        ```python
          BodyMetaOneNode.create_body('A test body',
                            size_scale='2XS-2XL',
                            base_size='XS',
                            body_piece_codes=['PNTFTPNLRT-S'],
                            category='Dress',
                            cover_image_uri='s3://res-data-platform/doc-parse/bodies/EP-6000-V0/0/page_images/image_0.jpeg',
                            dxf_uri='s3://res-data-platform/doc-parse/bodies/EP-6000-V0/dxf/input.dxf',
                            tech_pack_uri='s3://res-data-platform/doc-parse/bodies/EP-6000-V0/input_tech_doc.pdf',
                            #to avoid creating the same body again if we just want to test field updating
                            #_existing_body_code='TT-6048',
                            create_borr=True,
                            onboarding_materials='CTW70',
                            plan=False)
        ```
        """

        from res.connectors.airtable import AirtableConnector

        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")

        bod_table = airtable["appa7Sw0ML47cA8D1"]["tblXXuR9kBZvbRqoU"]

        if not plan:
            if _existing_body_code:
                record = airtable["appa7Sw0ML47cA8D1"][
                    "tblXXuR9kBZvbRqoU"
                ].to_dataframe(
                    filters=AirtableConnector.make_key_lookup_predicate(
                        [_existing_body_code], "Body Number"
                    ),
                    fields=["Body Number", "Brand"],
                )
                if not len(record):
                    raise Exception(f"Body {_existing_body_code} does not exist")
                graph_body_id = record.iloc[0]["record_id"]
                graph_body_code = record.iloc[0]["Body Number"]
                res.utils.logger.info(f"Loaded body")
            else:
                res.utils.logger.info(f"Creating body")
                p = BodyMetaOneNode.resolve_body_attribute_ids(
                    name,
                    brand_code=brand_code,
                    category_name=category,
                    onboarding_materials=onboarding_materials,
                    size_scale_name=size_scale,
                    base_size=base_size,
                )

                ep = res.get_meta_one_endpoint("/meta-one/body", "post")
                res.utils.logger.debug(f"Creating body {p}")
                r = ep(json=p)
                if r.status_code not in [200, 201]:
                    raise Exception(
                        f"Failed to create body: {r.text} ({r.status_code})"
                    )

                res.utils.logger.debug(f"{r.json()}")
                graph_body_id = r.json()["body"]["id"]
                graph_body_code = r.json()["body"]["code"]

            as_attachment = lambda uri: (
                [{"url": s3.generate_presigned_url(uri)}] if uri else None
            )
            # addendum - lorelay supports on api ? - pieces and attachments
            # get the piece names - can filter in future
            predicate = AirtableConnector.make_key_lookup_predicate(
                body_piece_codes, "Generated Piece Code"
            )

            res.utils.logger.info(f"Airtable with predicate {predicate}")
            piece_names = dict(
                airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"]
                .to_dataframe(
                    fields=["Generated Piece Code", "Name"],
                    filters=predicate,
                )[["Generated Piece Code", "record_id"]]
                .values
            )

            body_piece_ids = list(piece_names.values())
            assert len(body_piece_ids) == len(
                body_piece_codes
            ), f"Did not match all the piece names requested. Have {list(piece_names.keys())} - needed {piece_names}"

            p = {
                "record_id": graph_body_id,
                "Body Pieces": body_piece_ids,
                "Pattern Files": as_attachment(dxf_uri),
                "Techpack Files": as_attachment(tech_pack_uri),
                "Cover Image": as_attachment(cover_image_uri),
                "Body Image": as_attachment(cover_image_uri),
                # TODO add the image for the PoMs / Trim mapping? / Pieces Layout / Size Scale / Base Size / Avail Size  / Target Sew / Onboarding Materials
            }

            res.utils.logger.debug(f"Updating body with {p}")
            response = bod_table.update_record(p)
            if response.status_code != 200:
                raise Exception(f"Failed to update {response.text}")
            res.utils.logger.info(
                f"Updated body {graph_body_code=}, {graph_body_id=} with attachments, pieces and sizes"
            )

            if create_borr:
                # TODO add tech packs again
                p = BodyMetaOneNode.get_static_meta_one_borr_payload(
                    graph_body_code,
                    tech_pack_uri,
                    dxf_file=dxf_uri,
                    record_id=graph_body_id,
                )

                # check if we have something maybe

                ep = res.get_meta_one_endpoint(
                    "/meta-one/body-one-ready-request", "post"
                )
                res.utils.logger.debug(p)
                r = ep(json=p)
                if r.status_code not in [200, 201]:
                    raise Exception(
                        f"Failed to create BORR: {r.text} ({r.status_code})"
                    )
                res.utils.logger.debug(r)
            return {}

    @staticmethod
    def submit_one(event, plan=False):
        from res.flows import FlowEventProcessor

        fp = FlowEventProcessor()

        event_payload = fp.make_sample_flow_payload_for_function(
            "meta.ONE.body_node.handler"
        )
        # we have a simple typing system for this node - see handler and underlying flow context
        event["res_entity_type"] = "meta.BodyMetaOneRequest"
        # this should not ne needed and confused the schema
        event.pop("pieces")
        event_payload["assets"] = [event]
        # generate stuff
        event_payload = generator(event_payload)[0]
        argo = res.connectors.load("argo")
        if plan:
            return event_payload
        res.utils.logger.info(f"submitting {event_payload}")
        try:
            argo.handle_event(
                event_payload,
                unique_job_name=f"{event['body_code']}-submit-one".lower(),
            )
        except:
            res.utils.logger.warn(f"Attempting to double submit ")

    @staticmethod
    def refresh(
        body_code,
        body_version=None,
        contract_failures=[],
        contract_failure_context={},
        previous_differences={},
        sample_size_only=False,
        make_previews=False,
        plan=False,
        single_instance=False,
        get_original_contract_failures=True,
    ):
        """
        For the given kafka env post requests to the node
        this generates the request payload structure for the body. the latest body version is passed or optionally refresh a specific version
        the requests are queued and processed by the node

        """
        request = BodyMetaOneNode.get_body_as_request(
            body_code=body_code, size_code=None
        )

        sample_size = request["basePatternSize"]["code"]

        if get_original_contract_failures:
            orig_failures, orig_failure_context = get_contract_failures_from_latest_job(
                body_code, body_version
            )
            contract_failures = contract_failures + orig_failures
            contract_failure_context = {
                **contract_failure_context,
                **orig_failure_context,
            }

        payloads = []
        for size in request["sizes"]:
            if sample_size_only and size != sample_size:
                continue
            payload = dict(request)
            if make_previews:
                payload["metadata"]["make_previews"] = True
            if body_version:
                payload["body_version"] = body_version
                payload["contracts_failed"] = contract_failures
                payload["contract_failure_context"] = contract_failure_context
                payload["previous_differences"] = previous_differences
            payload["size_code"] = size
            payloads.append(payload)

        if single_instance:
            return BodyMetaOneNode.submit_one(payloads[0], plan=plan)
        if plan:
            return payloads

        else:
            res.connectors.load("kafka")["res_meta.dxa.body_update_requests"].publish(
                payloads, use_kgateway=True, coerce=True
            )

    @staticmethod
    def nest_body_into_material(
        body_code,
        material_codes,
        body_version=None,
        size_codes=None,
        attach_airtable=False,
        **kwargs,
    ):
        """

        this is a flow to get the nest file for a particular body and material in whatever size
        some defaults are choosen as required

        """
        from res.flows.meta.bodies import get_body_asset_status

        s3 = res.connectors.load("s3")

        def get_nested_cutfiles_link(s3_files_root):
            """attach and link and s3 files at the root (not just the ones recently added)"""
            files = [
                {"url": s3.generate_presigned_url(f)} for f in s3.ls(s3_files_root)
            ]

            tab = res.connectors.load("airtable")["appa7Sw0ML47cA8D1"][
                "tbl0ZcEIUzSLCGG64"
            ]
            keys = [f"{body_code}-V{body_version}"]
            pred = AirtableConnector.make_key_lookup_predicate(keys, "Name")
            data = tab.to_dataframe(
                fields=[
                    "Name",
                    "Body Version",
                ],
                filters=pred,
            )
            link = None
            for row in data.to_dict("records"):
                link = f"https://airtable.com/appa7Sw0ML47cA8D1/tbl0ZcEIUzSLCGG64/viwbk6nURGdxKixZo/{row['record_id']}/fldIvJjcGrxItoeNW/attPQSj4WlOPqOKjQ?blocks=hide"
                tab.update_record(
                    {
                        "record_id": row["record_id"],
                        "Nested Print Pieces Dxf Files": files,
                    }
                )

            res.utils.logger.info(f"{files} files made available at {link}")

            return link

        """
        
             LOGIC TO NEST DIFFERENT COMBINATIONS OF SAMPLE NESTS
        
        """

        force_renest = kwargs.get("force_renest", True)
        sign_urls = kwargs.get("sign_urls", False)

        bc = get_body_asset_status(body_code)
        body_version = body_version or bc["body_version"]
        if not size_codes:
            size_codes = bc["sample_size"]
        if not isinstance(size_codes, list):
            size_codes = [size_codes]
        if not isinstance(material_codes, list) and material_codes is not None:
            material_codes = [material_codes]

        res.utils.logger.info(
            f"Body {body_code} V{body_version} in sizes {size_codes} onto materials {material_codes}"
        )

        if material_codes is None:
            res.utils.logger.warn(
                f"Because the material was not specified, will use the first of the onboarding materials in {bc['onboardingMaterials']}"
            )
            material_codes = [bc["onboardingMaterials"][0]["code"]]
        mat_props = res.connectors.load(
            "airtable"
        ).get_airtable_table_for_schema_by_name(
            "make.material_properties",
            f"""FIND({{Material Code}}, '{",".join(material_codes)}')""",
        )
        mat_dict = {r["key"]: r for r in mat_props.to_dict("records")}
        root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code.lower().replace('-','_')}/v{body_version}/by-materials"

        sign_or_not = lambda x: x if not sign_urls else s3.generate_presigned_url(x)
        nested_files = {}
        for size_code in size_codes:
            m1 = BodyMetaOne(body_code, body_version, size_code)
            for m in mat_dict.keys():
                if m in material_codes:
                    file = f"{root}/{m}/{body_code}_V{body_version}_{size_code}_{m}.dxf"
                    # save yourself the trouble
                    if s3.exists(file) and not force_renest:
                        res.utils.logger.warn(
                            f"The file {file} exists so will reuse due to option force renest {force_renest}"
                        )
                        nested_files[sign_or_not(file)] = "Succeeded"
                        continue
                    try:
                        res.utils.logger.info(f"making {file}")
                        m1._data["garment_piece_material_code"] = m
                        m1.save_nested_cutlines(
                            file,
                            mat_dict[m]["cuttable_width"],
                            mat_dict[m]["paper_marker_compensation_width"],
                            mat_dict[m]["paper_marker_compensation_length"],
                            material=m,
                        )
                        res.utils.logger.info("...")
                        nested_files[sign_or_not(file)] = "Succeeded"
                    except Exception as ex:
                        nested_files[file] = f"Failed ({ex})"

        link = None if not attach_airtable else get_nested_cutfiles_link(root)

        return {"files": nested_files, "links": link}

    @staticmethod
    def upsert_response(data: UpsertBodyResponse):
        """
        primarily used to create a placeholder record with optional contracts failing
        """

        from res.flows.meta.ONE.queries import update_body_file_updated_at

        body_code = data.body_code

        request = BodyMetaOneNode.get_body_as_request(
            body_code=body_code, size_code=None
        )
        request.update(data.dict())
        # two paths: if we have a meta one for the body version we can turn it to a response
        # otherwise we generate an empty one
        try:
            # for an existing body use it and merge in the status update
            # test for overwriting contracts
            bm = BodyMetaOne(
                request["body_code"], request["body_version"], request["sample_size"]
            )
            try:
                b = BodyMetaOneNode.get_body_as_request(bm.body_code)
                bor = b["body_one_ready_request"]
            except:
                res.utils.logger.warn(f"Failed to get the body request data")

            b = bm.status_at_node(body_request_id=bor)
            b.update(request)
            b.pop("pieces")
            b = BodyMetaOneResponse(**b)

        except Exception as ex:
            res.utils.logger.warn(f"{traceback.format_exc()}")
            res.utils.logger.info(f"We dont have a match for: {data} ")
            request.pop("pieces")
            request["contracts_failed"] = []

            b = BodyMetaOneResponse(**request)

        node = BodyMetaOneNode(
            b,
            response_type=BodyMetaOneResponse,
            queue_context=res.flows.FlowContext({}),
        )

        update_body_file_updated_at(
            body_code,
            bm.body_version,
            b.body_file_uploaded_at,
            contracts=request.get("contracts_failed", []),
        )

        node._api.update_airtable(b)

        return b.dict()

    @staticmethod
    def post_status(
        body_code,
        body_version=None,
        contracts_failed=None,
        contract_failure_context={},
        previous_differences={},
        trace_log=None,
        bor_status=None,
        body_one_ready_request_id=None,
    ):
        """

        the status is determined from the status at node or the compiled status

        although we take the status in here we do not move the status on the BORR - its only done when we run the node
        lets leave it like that for now
        """
        from res.flows.meta.bodies import get_body_asset_status

        r = get_body_asset_status(body_code, body_version)
        r["trace_log"] = trace_log
        r["body_one_ready_request"] = body_one_ready_request_id

        if contracts_failed:
            r["contracts_failed"] += contracts_failed
            r["contracts_failed"] = list(set(["contracts_failed"]))
            r["contract_failure_context"] = contract_failure_context

            send_contract_failures_to_slack(r)

        r["previous_differences"] = previous_differences

        # we were not using this updated field and now we are
        r["updated_at"] = res.utils.dates.utc_now_iso_string()
        b = BodyMetaOneResponse(**r)
        node = BodyMetaOneNode(
            b,
            response_type=BodyMetaOneResponse,
            queue_context=res.flows.FlowContext({}),
        )
        # we could get the full status and merge it ??
        return node._api.update_airtable(b)

    @staticmethod
    def get_payloads_since(since_days=1):
        import json
        import pandas as pd

        d = str(res.utils.dates.utc_days_ago(since_days).date())
        Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_META_DXA_BODY_UPDATE_REQUESTS"  WHERE TO_TIMESTAMP_TZ(parse_json(RECORD_CONTENT):created_at)   > '{d}'"""

        data = res.connectors.load("snowflake").execute(Q)

        data["payload"] = data["RECORD_CONTENT"].map(json.loads)
        data["created_at"] = data["payload"].map(lambda x: x.get("created_at"))
        data["created_at"] = pd.to_datetime(data["created_at"], utc=True)

        return list(data["payload"])

    @staticmethod
    def list_brand_bodies(brand_code, sign_link_fields=None):
        """
        list brand bodies and optionally sign link attributes in the top level result set
        """
        hasura = res.connectors.load("hasura")
        s3 = res.connectors.load("s3")
        res.utils.logger.info(f"{brand_code=}")
        result = hasura.tenacious_execute_with_kwargs(
            queries.GET_BODIES_BY_BRAND, brand_code=brand_code
        )["meta_bodies"]

        if sign_link_fields:
            try:
                for r in result:
                    for u in sign_link_fields:
                        if u in r:
                            r[u] = s3.generate_presigned_url(r[u])
            except Exception as ex:
                res.utils.logger.warn(
                    f"Tried and failed to sign {sign_link_fields} - {ex}"
                )
        return result

    @staticmethod
    def get_body_status(body_code, body_version=None):
        from res.flows.dxa.styles.queries import get_body_header
        from res.flows.meta.bodies import get_body_asset_status
        from res.flows.meta.ONE.contract import ContractVariables
        from res.connectors.airtable import AirtableConnector
        import pandas as pd
        from schemas.pydantic.meta import BodyMetaOneStatus, BodyMetaOneResponse

        """
        TODO cases
        - meaningful errors if asked for a version but not yet on that version
        - meaningful areas if the body does not exist
        - meaningful errors if one of services times out or fails in a weird way
        - test how the user experience works when we provide the latest body version because the user does not specify
        """
        airtable = res.connectors.load("airtable")

        # below there are 3 separate calls which is a bit silly
        # 1. this calls the graph for what we have in airtable (but i need to get contract variable names too)
        a = get_body_asset_status(body_code, body_version)
        # 2. this checks what is in hasura - but we are not fully syncing queue status with hasura yet
        b = get_body_header(a["body_code"], body_version or a["body_version"])[
            "meta_bodies"
        ]
        b = b[0] if len(b) else {}
        b.update(a)

        try:
            # 3. this then pulls the contract failing names
            tab = airtable["appa7Sw0ML47cA8D1"]["tbl0ZcEIUzSLCGG64"]
            cf = dict(
                tab.to_dataframe(
                    filters=AirtableConnector.make_key_lookup_predicate(
                        [f"{a['body_code']}-V{a['body_version']}"], "Name"
                    ),
                    fields=["Name", "Contract Failed Names", "Last Modified At"],
                )
                .sort_values("Last Modified At")
                .iloc[-1]
            ).get("Contract Failed Names")

            cf = [a for a in cf.split(",")] if pd.notnull(cf) else []
            b["contracts_failed"] = ContractVariables.as_codes(cf)
            b = BodyMetaOneStatus(**b)
        except:
            b["status"] = "ERROR"
        return b


def send_contract_failures_to_slack(event):
    try:
        slack = res.connectors.load("slack")
        owner = "Followers <@U01JDKSB196> "
        q_url = f"https://airtable.com/appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l/viwUT0BwnCLNNbUu3/{event['body_one_ready_request']}"

        slack(
            {
                "slack_channels": ["meta-one-status-updates"],
                "message": f"""The following contracts failed on body <{q_url}|{event['body_code']}>. {event['contracts_failed']} \n {owner}""",
            }
        )
    except:
        pass


def send_exception_to_slack(event, msg):
    try:
        slack = res.connectors.load("slack")
        body_code = event.get("body_code", "(Body Link)")
        owner = "Followers <@U01JDKSB196> "
        q_url = f"https://airtable.com/appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l/viwUT0BwnCLNNbUu3/{event['body_one_ready_request']}"

        slack(
            {
                "slack_channels": ["meta-one-status-updates"],
                "message": f"""Body response <{q_url}|{body_code}> crashed 🔥: {msg} \n {owner}""",
            }
        )
    except:
        pass


def _make_body_svg_files(bodies=["CC-4004", "RB-6018"]):
    """
    we are going to generate these on S3 eventually but for now its a test utility
    """
    from res.flows.meta.ONE.geometry_ex import geometry_df_to_svg
    from res.flows.meta.bodies import get_body_asset_status
    from pathlib import Path

    for b in bodies:
        b = get_body_asset_status(b)
        v = b["max_body_version"]
        for size in b["sizes"]:
            m1 = BodyMetaOne(b["code"], v, size)
            df = m1.nest()

            """
            with lines
            """
            S = geometry_df_to_svg(
                df[["key", "nested.original.geometry"]].rename(
                    columns={"nested.original.geometry": "outline"}
                )
            )

            file = f"/Users/sirsh/Downloads/svgs/{b['code']}/print_pieces_with_sew_lines_{b['code']}-V{v}-{size}_300_dpi_image_space.svg"
            res.utils.logger.info(f"writing {file}")
            Path(file).parent.mkdir(exist_ok=True)
            with open(file, "w") as f:
                f.write(S)

            """
            just outlines
            """
            S = geometry_df_to_svg(
                df[["key", "nested.original.outline"]].rename(
                    columns={"nested.original.outline": "outline"}
                )
            )

            file = f"/Users/sirsh/Downloads/svgs/{b['code']}/print_pieces_{b['code']}-V{v}-{size}_300_dpi_image_space.svg"
            res.utils.logger.info(f"writing {file}")
            with open(file, "w") as f:
                f.write(S)


def process_missing_plt_files(event={}, context={}, missing_only=True):
    """ """
    import requests
    from res.media.images.providers.dxf import DxfFile

    def fetch_dxfs(dxf_data, lower_body_code):
        for record in dxf_data:
            filename = record["filename"]
            url = record["url"]

            if ".dxf" in filename.lower():
                fetch_to_s3(
                    url,
                    f"s3://res-data-platform/data/static-meta-one/bodies/{lower_body_code}/input.dxf",
                )
            if ".rul" in filename.lower():
                fetch_to_s3(
                    url,
                    f"s3://res-data-platform/data/static-meta-one/bodies/{lower_body_code}/input.rul",
                )

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")

    data = airtable.get_table_data("appa7Sw0ML47cA8D1/tblunjhEa4EmdOUnX")
    if "Processed PLT File" not in data:
        data["Processed PLT File"] = None
    tab = airtable["appa7Sw0ML47cA8D1"]["tblunjhEa4EmdOUnX"]
    data = data[
        [
            "BODY CODE",
            "2D Input File",
            "record_id",
            "Processed PLT File",
            "Generated Files At",
            "Dxf Input Updated At",
        ]
    ].dropna(subset=["2D Input File"])

    # data["dxf_url"] = data["2D Input File"].map(lambda x: x[-1]["url"])

    if missing_only:
        # if we have not generated files or there is something new
        data = data[
            data["Generated Files At"].isnull()
            | (data["Generated Files At"] < data["Dxf Input Updated At"])
        ]

    if len(data) == 0:
        res.utils.logger.info("No work todo")
        return

    for rec in data.to_dict("records"):
        lower_body_code = rec["BODY CODE"].lower().replace("-", "_")

        # r = requests.get(rec["dxf_url"], allow_redirects=True)
        uri = f"s3://res-data-platform/data/static-meta-one/bodies/{lower_body_code}/input.dxf"
        # fetch_to_s3(rec["dxf_url"], uri)
        fetch_dxfs(rec["2D Input File"], lower_body_code)

        export_path = f"s3://res-data-platform/data/static-meta-one/bodies/{lower_body_code}/meta_one"
        dxf = DxfFile(uri)
        # TODO tale this from the revord
        dxf.export_static_meta_one(export_path, "PIMA7")

        f1 = f"{export_path}.plt"
        f2 = f"{export_path}_outlines.plt"
        f3 = f"{export_path}.dxf"

        def as_attach(s):
            s = s3.generate_presigned_url(s)
            return [{"url": s}]

        tab.update_record(
            {
                "record_id": rec["record_id"],
                "Processed PLT File": as_attach(f1),
                "PLT Outlines Only": as_attach(f2),
                "Output Nested DXF": as_attach(f3),
            }
        )

        res.utils.logger.info(f"updated body assets for {rec}")
