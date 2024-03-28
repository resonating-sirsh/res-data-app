"""
processes queue res_make.make_one_request.reproduce
see also bot https://github.com/resonance/create-one-bots/blob/master/lambda/source/make_one_production/serverless/functions/production_requests/create_print_requests/function.py
"""

import res
import traceback
from res.airtable.misc import PRODUCTION_REQUESTS, ORDER_LINE_ITEMS
from res.utils import logger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from . import graphql_queries as queries


def create_new_issue(old_one, new_one):
    gql = ResGraphQLClient()
    resolution = "This ONE has more than 1 reproduce"
    dxa_info = gql.query(
        """
        query ($nodename: WhereValue) {
            dxaNodes(first: 1, where: {name: { is: $nodename }, escalationEmail: { isNot: null }}){
                dxaNodes {
                    number
                    name
                    escalationEmail
                }
            }
        }
        """,
        {
            "nodename": old_one["fields"]["Current Make Node"],
        },
    )["data"]["dxaNodes"]["dxaNodes"]
    dxa_info = {} if len(dxa_info) == 0 else dxa_info[0]
    dxa_node_id = dxa_info.get("number") if dxa_info.get("number") else "7"
    dxa_node_email_escalation = (
        dxa_info.get("escalationEmail")
        if dxa_info.get("escalationEmail")
        else "techpirates@resonance.nyc"
    )

    get_issue_type = gql.query(
        queries.GET_ISSUE_TYPE,
        {
            "where": {
                "name": {"is": resolution},
                "dxaNodeNumber": {"is": str(dxa_node_id)},
            }
        },
    )["data"]

    issue_type_id = "rec48rwqB3M9IM0MX"
    if get_issue_type and len(get_issue_type.get("issueTypes").get("issueTypes")) > 0:
        issue_type_id = get_issue_type.get("issueTypes").get("issueTypes")[0].get("id")
    else:
        issue_type_payload = {"name": resolution, "dxaNodeNumber": str(dxa_node_id)}
        create_issue_type = gql.query(
            queries.CREATE_ISSUE_TYPE, {"input": issue_type_payload}
        )
        if create_issue_type:
            issue_type_id = (
                create_issue_type.get("createIssueType").get("issueType").get("id")
            )

    issue_payload = {
        "type": resolution,
        "context": resolution,
        "dxaNodeId": str(dxa_node_id),
        "sourceRecordId": old_one["id"],
        "issueTypesIds": issue_type_id,
        "ownerEmail": dxa_node_email_escalation,
        "subject": old_one["fields"]["Factory Request Name"],
        "oneNumbers": [new_one["fields"]["Order Number V2"]],
        "issuePath": f"https:/create.one/assemblyone/make/make-one?search={old_one['id']}",
        "sourceFlowId": "F-75",
    }

    logger.info(f"Issue Payload {issue_payload}")
    graph_response = gql.query(queries.CREATE_ISSUE, {"input": issue_payload})
    if graph_response:
        issue_info = (
            graph_response.get("createIssues").get("issues")[0]
            if graph_response.get("createIssues")
            else None
        )

        if issue_info is not None:
            PRODUCTION_REQUESTS.uppdate(
                new_one["id"], {"Active Issue Id": issue_info.get("id")}
            )


def record_cancelled_one(order_key, sku, one_number):
    res.utils.logger.info(f"Cancel: {one_number} from {order_key}:{sku}")
    from res.flows.make.production.queries import (
        record_cancelled_one,
        set_one_cancelled,
    )

    try:
        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"
        set_one_cancelled(one_number, cancelled_at=res.utils.dates.utc_now_iso_string())
        # this other table update will be redundant if we re-engineer
        record_cancelled_one(order_key, sku, one_number)
    except:
        res.utils.logger.info(f"{traceback.format_exc()}")


def reproduce_one(original_one_id):
    one_to_reproduce = PRODUCTION_REQUESTS.get(original_one_id)

    # hybrid update - tell the other system we are cancelling a ONE
    cancelled_one = one_to_reproduce["fields"]["Order Number V2"]
    sku = one_to_reproduce["fields"]["SKU"]
    order_number = one_to_reproduce["fields"]["Belongs to Order"]
    record_cancelled_one(order_number, sku, cancelled_one)

    if one_to_reproduce["fields"]["Sales Channel"] == "OPTIMUS":
        raise Exception(f"Not reproducing this one since it was not a customer order.")

    if (
        one_to_reproduce["fields"]["_order_line_item_id"] is None
        or ORDER_LINE_ITEMS.get(one_to_reproduce["fields"]["_order_line_item_id"])
        is None
    ):
        raise Exception(f"Not reproducing since the order went away.")

    repro_count = int(one_to_reproduce["fields"].get("Reproduce Count", 0)) + 1
    needs_flag = repro_count > 1

    new_request_payload = {
        "_open": True,
        "Botless Print Flow": True,
        "Digital Asset Status": "Processing Request",
        "Factory": "res.Factory.DR ✅",
        "_request_pending_assets": True,
        "Original Request": original_one_id,
        "Reproduced ONEs Ids": ",".join(
            one_to_reproduce["fields"].get("Reproduced ONEs Ids", "").split(",")
            + [original_one_id]
        ),
        "Reproduce Count": repro_count,
        "Flag for Review: Tag": (
            ["This ONE has more than 1 reproduce"] if needs_flag else None
        ),
        **{
            f: one_to_reproduce["fields"].get(f)
            for f in [
                "RequestType",
                "Request Name",
                "Sales Channel",
                "_order_line_item_id",
                "Color Placed in 3D",
                "Allocation Status",
                "Line Item ID",
                "_channel_order_id",
                "_original_request_placed_at",
                "Belongs to Order",
                "Exit Factory Date",
                "_request_processed_at",
                "__brandcode",
                "Brand",
                # stuff about the sku
                "SKU",
                "Body Code",
                "Material Code",
                "Color Code",
                "Size Code",
                # style stuff
                "Body Version",
                "Style Code",
                "styleId",
                "Style Version Number",
                "Pattern Version Number",
                "Styles",
                "Body",
                "Color",
                "Print Type",
                "Body Category",
                "Body Pieces",
                "Number of Body Operations",
                # stuff from the previous one
                "Sew Assignment: Sewing Node",
                "Manual Override Factory",
            ]
        },
        # file attachments
        **{
            f: [{"url": one_to_reproduce["fields"][f]["url"]}]
            for f in [
                "Production Cover Photo",
                "ArtworkFile Preview",
                "Paper Markers",
            ]
            if "url" in one_to_reproduce["fields"].get(f, {})
        },
    }
    response = PRODUCTION_REQUESTS.create(new_request_payload, typecast=True)
    new_request_id = response["id"]

    PRODUCTION_REQUESTS.update(
        original_one_id,
        {
            "Reproduced ONE": [new_request_id],
        },
    )

    ORDER_LINE_ITEMS.update(
        one_to_reproduce["fields"].get("_order_line_item_id"),
        {
            "_make_one_production_request_id": new_request_id,
            "Reproduce Count": repro_count,
            "_make_one_status": None,
            "__last_synced_with_mongodb_at": None,
            "ONE Status": None,
            "_ts_order_management_exit_time": None,
            "_ts_dxa_exit_time": None,
            "_ts_make_exit_time": None,
            "_ts_warehouse_exit_time": None,
            "_ts_fulfillment_exit_time": None,
            "_ts_shipping_exit_time": None,
            "_ts_shipping_exit_time": None,
            "_ts_order_management_exit_sla": None,
            "_ts_dxa_exit_sla": None,
            "_ts_make_exit_sla": None,
            "_ts_warehouse_exit_sla": None,
            "_ts_fulfillment_exit_sla": None,
            "_ts_shipping_exit_sla": None,
            "_ts_customer_exit_sla": None,
            "_ts_expected_flow_exit_date": None,
        },
    )

    if needs_flag:
        try:
            create_new_issue(one_to_reproduce, response)
        except Exception as ex:
            # the graphql api fusses over bullshit and this thing is just some tracking crap
            logger.warn(
                f"Failed to make a issue about reproducing {original_one_id}: {repr(ex)}"
            )

    return response
