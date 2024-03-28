import requests
import res
import pdfkit
from collections import Counter
from res.airtable.misc import PRODUCTION_REQUESTS, ORDER_LINE_ITEMS, ORDERS
from res.utils import logger
from res.utils.dates import utc_now_iso_string
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from . import graphql_queries as queries

# utilities to make the pdf for the one.
# this is the same thing that happens in the bot here:
# https://github.com/resonance/create-one-bots/blob/master/lambda/source/make_one_production/serverless/functions/production_requests/add_to_assembly_queue/function.py
# code is all due to Geovanny.


def get_factory_order_html(
    one_number,
    brand_name,
    size_name,
    style_name,
    sku,
    body_version,
    sales_channel,
    is_optimus=False,
    order_name=None,
    number_items_in_order=None,
    order_create_date=None,
    assembly_add_date=None,
    expected_delivery_date=None,
    body_img_url=None,
    artwork_img_url=None,
    total_pieces=0,
    piece_type_counts={},
    thread_colors=[],
):
    if body_img_url is None:
        body_img_url = (
            "https://s3.amazonaws.com/factoryorders-v0/global-assets/anycolor.png"
        )

    if is_optimus:
        order_data = (
            "<p style='display: inline; font-size: 40px;'><b>Optimus order<b/></p>"
        )
    elif order_name is not None:
        order_data = f"""
            <p style='display: inline; font-size: 20px;'">
                                    ORDER NUMBER:        <b>{order_name}</b></br>
                                    ONES IN ORDER:       <b>{number_items_in_order}</b></br> 
                                    ORDER DATE:          <b>{order_create_date.split("T")[0] if order_create_date is not None else ""}</b></br>
                                    ENTERED MAKE DATE:   <b>{(assembly_add_date if assembly_add_date is not None else utc_now_iso_string()).split('T')[0]}</b></br>
                                    MUST EXIT BY DATE:   <b>{expected_delivery_date.split("T")[0] if expected_delivery_date else ""}</b></br>
                                </p>
        """
    else:
        order_data = "<p style='display: inline; font-size: 40px;'><b>Unknown order -- ping engineering because this is a bug.<b/></p>"

    return f"""
        <html style="margin: 20px; font-family: Helvetica;">
            <div class='pdf-container'>
                <div class='header'>
                    <table>
                        <tr>
                            <td width='35%'><p style='display: inline; font-size: 20px;' >ONE #:</p> <p style='display: inline;font-size: 36px;' >{one_number}</p></td>
                        </tr>
                        <tr>
                            <td style='text-align: center; ' width='35%'><img class="qr-img" src="https://api.qrserver.com/v1/create-qr-code/?size=100x100&data={one_number}" /></td>
                            <td style='text-align: left; padding-left:50px;' width='60%'>
                                {order_data}
                            </td>
                        </tr>
                    </table>
                </div>
                <br/><br/>
                <div class='style' style='border-top: 1px solid black; border-bottom: 1px solid black; margin: 0; padding: 0; height: 100px;'>
                    <table style='width: 95%;'>
                        <tr>
                            <td style='width: 150px;'>
                                <p style='text-align:center; font-size: 17px; font-weight: bold; margin: 1px;'>{brand_name}</p>
                                <p style='text-align:center; font-size: 17px; font-weight: bold; margin: 1px;'>Size {size_name}</p>
                                <p style='text-align:center; font-size: 17px; font-weight: bold; margin: 1px;'>{sales_channel}</p>
                                <p style='text-align:center; font-size: 17px; font-weight: bold; margin: 1px;'>Body Version {body_version}</p>
                            </td>
                            <td style='width: 25%;' ></td>
                            <td style='margin-left: 40px;'>
                                <table >
                                    <tr>
                                        <td style=''>
                                            <p style='text-align:center; font-size: 17px; font-weight: bold; margin: 1px;'>{sku}</p>
                                            <p style='text-align:center; font-size: 15px; margin: 1px;'>{style_name}</p>
                                        </td>
                                        <td style='text-align: center;'>
                                            <img class="qr-img" src="https://api.qrserver.com/v1/create-qr-code/?size=80x80&data={sku}" />
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>
                </div>
                <br/> <br/>
                <div class='info'>
                    <table>
                        <tr>
                            <td style='text-align: center'><img style='max-height:100%; max-width:250px' class="qr-img" src="{artwork_img_url}" /></td>
                            <td style='text-align: center'><img style='max-height:100%; max-width:250px' class="qr-img" src="{body_img_url}" /></td>
                            <td style='padding: 0; margin:0; max-width:300px'> 
                                <p style='font-size: 22px; line-height: 25%;'>Cantidad de piezas: {total_pieces}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas impresas: {piece_type_counts.get("Self-S", 0)+
                                                                                                piece_type_counts.get("Combo-C", 0)+
                                                                                                piece_type_counts.get("Block Fuse-BF", 0)+
                                                                                                piece_type_counts.get("Lining-LN", 0)
                                                                                                }</p>  
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Self: {piece_type_counts.get("Self-S", 0)}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Combo: {piece_type_counts.get("Combo-C", 0)}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Fusing: {piece_type_counts.get("Fusible-F", 0)}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Block Fuse: {piece_type_counts.get("Block Fuse-BF", 0)}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Lining: {piece_type_counts.get("Lining-LN", 0)}</p>
                                <p style='font-size: 22px; line-height: 25%;'>Piezas de Stamper: {piece_type_counts.get("Stamper-X", 0)}</p>
                                <p style='font-size: 16px; line-height: 100%; width: 90%; '>{','.join(thread_colors)}</p>
                            </td>
                        </tr>
                    </table>
                </div>
                <br/>
                <p style='text-align:center; font-size: 20px;'>Created by Resonance ONE Platform, (c) 2015-2023 All Rights Reserved.</p>
            </div>
        </html>
    """


def get_one_pdf(make_one_request_id, s3_output_path):
    one_record = PRODUCTION_REQUESTS.get(make_one_request_id)
    one_fields = one_record["fields"]
    style = ResGraphQLClient().query(
        queries.QUERY_PDF_STYLE_INFO, {"id": one_record["fields"]["styleId"]}
    )["data"]["style"]
    size_name = ResGraphQLClient().query(
        """
        query ($code: String) {
            size(code: $code) {
                name
            }
        }
        """,
        {"code": one_record["fields"]["Size Code"]},
    )["data"]["size"]["name"]
    order_line_item = ORDER_LINE_ITEMS.get(one_fields["_order_line_item_id"]) or {}
    order = (
        None
        if len(order_line_item) == 0
        else ORDERS.get(order_line_item["fields"]["ORDER_LINK"][0])
    )
    body_imgs = [
        body_image["largeThumbnail"]
        for body_image in style["body"]["images"]
        if requests.get(body_image["largeThumbnail"]).status_code == 200
    ] + [img["url"] for img in style.get("coverImages", [])]
    if order is None and one_fields["Sales Channel"] != "OPTIMUS":
        raise ValueError("Order no longer exists.")
    one_html = get_factory_order_html(
        one_fields["Order Number V2"],
        one_fields["Brand"],
        size_name,
        style["name"],
        one_fields["SKU"],
        one_fields["Body Version"],
        one_fields["Sales Channel"],
        is_optimus=one_fields["Sales Channel"] == "OPTIMUS",
        order_name=order["fields"].get("__number"),
        number_items_in_order=order["fields"].get("total_line_items_ordered"),
        order_create_date=order["fields"].get("created_at"),
        assembly_add_date=one_fields.get("__move_to_assembly_queue_bot_ts"),
        expected_delivery_date=order_line_item["fields"].get("_ts_customer_exit_sla"),
        body_img_url=body_imgs[0] if len(body_imgs) > 0 else None,
        artwork_img_url=style["color"]["images"][0]["largeThumbnail"],
        total_pieces=len(style["pieceMapping"]),
        piece_type_counts=Counter(
            p["bodyPiece"]["pieceType"] for p in style["pieceMapping"]
        ),
        thread_colors=[
            # lol
            b["trimItem"]["longName"]
            for b in style.get("styleBillOfMaterials", [])
            if b is not None
            and "trimItem" in b
            and b["trimItem"] is not None
            and "longName" in b["trimItem"]
            and "HILO" in b["trimItem"]["longName"]
            and "ELASTIC" not in b["trimItem"]["longName"]
        ],
    )
    res.connectors.load("s3").write(
        s3_output_path,
        pdfkit.from_string(
            one_html,
            options={
                "page-size": "A4",
                "encoding": "UTF-8",
            },
        ),
    )
    logger.info(f"Wrote pdf for {one_fields['Order Number V2']} to {s3_output_path}")
