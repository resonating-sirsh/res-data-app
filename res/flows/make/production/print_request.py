# - utility methods for handling print requests
# - these are used by the make one api as well as by the healing piece generation
import json
import res
from res.airtable.print import (
    ASSETS as PRINT_ASSETS,
    REQUESTS as PRINT_REQUESTS,
    print_material_id,
)


def create_print_asset_requests(
    one_number,
    sku,
    piece_specs_by_material_and_type,  # map of (material code, piece type) -> list of ppp specs {"key": "foo", "make_sequence_number":1}
    is_healing=False,
    original_order_timestamp=None,
    make_one_prod_request_id=None,
    factory_request_name=None,
    brand_name=None,
    add_to_existing_print_request=False,
    factory_order_pdf_url=None,
    plan=True,
    flag_reason=None,
    ppp_flow=None,
    sales_channel=None,
    sku_rank=None,
    order_priority_type=None,
):
    print_assets = []
    sku_body, sku_material, sku_color, sku_size = sku.split(" ")
    # see if we are adding to an existing request or not (i.e., healing or not).
    # starting at 1 for posteritys sake
    asset_offset = 1
    existing_asset_ids = []
    print_request = None
    if add_to_existing_print_request:
        try:
            print_request = PRINT_REQUESTS.first(
                formula=f"{{Order Number}}='{one_number}'"
            )
            existing_asset_ids = print_request["fields"].get("Print File Queue", [])
            asset_offset += len(existing_asset_ids)
        except:
            raise ValueError(f"Failed to find print request for {one_number}")

    for (
        material_code,
        piece_type,
    ), piece_specs in piece_specs_by_material_and_type.items():
        # LSG19 -> CHRST
        """
        Meta ones old and new designed to apply soft swap to match request in new material
        but if request is in the old material we also replace it effectively forcing the request in the new material
        """
        if material_code == "LSG19":
            material_code = "CHRST"
        print_assets.append(
            {
                "_primary_key": f"{one_number}_{asset_offset}",
                "Pieces Type": piece_type,
                "Rank": "Healing"
                if is_healing
                else "Combo"
                if sku_material != material_code and sku_material != "LSG19"
                else "Primary",
                "Print Queue": "TO DO",
                "Job Queued By": "Optimus",
                "SKU": " ".join(
                    [sku_body.replace("-", ""), sku_material, sku_color, sku_size]
                ),
                "Color Code": sku_color,
                "Material Code": material_code,
                "Number of Pieces": len(piece_specs),
                "Original Order Place At": original_order_timestamp,
                "File Number": asset_offset,
                "Materials": [print_material_id(material_code)],
                "Flag For Review": flag_reason is not None,
                "Flag For Review Reason": flag_reason or "",
                "Prep Pieces Spec Json": json.dumps(piece_specs),
                "Required Piece Names": ",".join([p["key"] for p in piece_specs]),
            }
        )
        asset_offset += 1
    res.utils.logger.info(f"Requesting print assets: {print_assets}")
    if not plan:
        asset_record_ids = [
            r["id"] for r in PRINT_ASSETS.batch_create(print_assets, typecast=True)
        ]
        res.utils.logger.info(f"Created asset records {asset_record_ids}")
        try:
            req_id = None
            if add_to_existing_print_request:
                req_id = PRINT_REQUESTS.update(
                    print_request["id"],
                    {"Print File Queue": existing_asset_ids + asset_record_ids},
                )["id"]
            else:
                req_id = PRINT_REQUESTS.create(
                    {
                        "Request Number - Name": factory_request_name,
                        "Unit Qty": 1,
                        "Resonance Code": " ".join(sku.split(" ")[0:3]),
                        "make_one_production_request_id": make_one_prod_request_id,
                        "Number of Print Jobs": len(print_assets),
                        "Color Placed in 3D": True,
                        "Order Number": str(one_number),
                        "Print File Queue": asset_record_ids,
                        "Brand Name": brand_name,
                        "Factory Order PDF": [{"url": factory_order_pdf_url}],
                        "Original Order Placed At": original_order_timestamp,
                        "Sales Channel": sales_channel,
                        "SKU Rank": sku_rank,
                        "Order Type": order_priority_type or "Normal",
                    }
                )["id"]
            # send a PPP request.
            from res.flows.dxa.prep_ordered_pieces import ppp_from_pa

            ppp_requests = ppp_from_pa(record_ids=asset_record_ids, ppp_flow=ppp_flow)
            res.utils.logger.info(f"Sending PPP Requests {ppp_requests}")
            ppp_topic = res.connectors.load("kafka")[
                "res_meta.dxa.prep_pieces_requests"
            ]
            for ppp_req in ppp_requests:
                ppp_topic.publish(ppp_req, use_kgateway=True)
            return req_id, dict(
                zip(
                    asset_record_ids,
                    [p["Required Piece Names"].split(",") for p in print_assets],
                )
            )
        except:
            res.utils.logger.warn(f"Deleting orphaned print assets {asset_record_ids}")
            # clean up the print assets which would otherwise be orphaned.
            PRINT_ASSETS.batch_delete(asset_record_ids)
            if req_id and not add_to_existing_print_request:
                res.utils.logger.warn(f"Deleting orphaned print request {req_id}")
                PRINT_REQUESTS.delete(req_id)
            raise
    return None, None
