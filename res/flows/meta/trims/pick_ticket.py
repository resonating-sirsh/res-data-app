import arrow
import res

# from collections import defaultdict
import traceback
from res.utils import ping_slack
from res.utils.logging import logger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from . import queries

from schemas.pydantic.trims import (
    PrepRequest,
)


def create_prep_request(create_pick_ticket_input: PrepRequest):
    gql = ResGraphQLClient()
    logger.info(f"create_pick_ticket_input: {create_pick_ticket_input}")

    try:
        production_request = gql.query(
            queries.QUERY_GET_ONE_REQUEST,
            {"id": create_pick_ticket_input.make_one_production_id},
        )["data"]["makeOneProductionRequest"]
        if not production_request:
            raise Exception(
                f"Production request {create_pick_ticket_input.make_one_production_id} not found"
            )

        logger.info(f"production_request: {production_request}")

        # get all PDF and cover photos into an array
        cover_photo_url, factory_pdf = get_factoryPDF_coverPhoto(production_request)

        # get the material care instructions for the request
        care_label = get_care_label(
            production_request.get("style").get("material").get("code")
        )

        # validate if the style is trashed
        is_style_trashed = validate_if_style_is_trashed(
            production_request.get("style").get("id")
        )

        # validate if the bill of materials has trims
        bill_of_material_has_trims = check_if_bill_of_material_has_trims(
            production_request.get("body").get("code")
        )

        logger.info(
            {
                "cover_photo_url": cover_photo_url,
                "factory_pdf": factory_pdf,
                "care_label": care_label,
                "is_style_trashed": is_style_trashed,
                "bill_of_material_has_trims": bill_of_material_has_trims,
            }
        )

        # build the request payload
        prep_request_payload = build_prep_request_payload(
            create_pick_ticket_input,
            care_label,
            cover_photo_url,
            factory_pdf,
            production_request,
        )

        prep_request_payload["flagForReviewReasons"] = ""
        if not bill_of_material_has_trims:
            prep_request_payload[
                "flagForReviewReasons"
            ] += "This Request no Required Pick Trims"

        if is_style_trashed:
            prep_request_payload[
                "flagForReviewReasons"
            ] += "This Style is Marked as Trashed!"

        if is_style_trashed or not bill_of_material_has_trims:
            prep_request_payload["isFlagForReview"] = bill_of_material_has_trims

        # create prep request
        prep_request = gql.query(
            queries.QUERY_CREATE_PREP_REQUEST,
            {"input": prep_request_payload},
        )["data"]["createPrepRequest"]["prepRequest"]

        return_message = " Prep request created "

        if not is_style_trashed and bill_of_material_has_trims:
            create_pick_ticket(prep_request.get("id"))
            return_message += " and pick ticket created"

        return return_message

    except Exception as e:
        logger.info({"error": e})
        ping_slack(
            f"[PICK-TICKET] <@U018UETQEH1> somethings fucked with create pick ticket ```{traceback.format_exc()}```",
            "autobots",
        )
        raise e


def get_factoryPDF_coverPhoto(production_request):
    """
    Get Factory PDF and Cover Photo from Production Request
    """
    # get all url of cover photos into an array
    cover_photo_url = []
    if production_request.get("productionCoverPhotos"):
        for i in production_request.get("productionCoverPhotos"):
            cover_photo_url.append(i.get("url"))

    # get all PDF into an array
    factory_pdf = []
    if production_request.get("makeOrderPdf"):
        for i in production_request.get("makeOrderPdf"):
            factory_pdf.append(i.get("url"))

    return cover_photo_url, factory_pdf


def get_care_label(material_code):
    """
    Get care label info
    """
    gql = ResGraphQLClient()
    material_care = gql.query(
        queries.GET_MATERIAL_CARE,
        {"where": {"code": material_code}},
    )["data"]["materials"]["materials"][0]

    # Initialize an empty list to store care labels
    care_label_arr = []

    # Check if each care instruction exists in the material_care dictionary
    # If it does, append it to the care_label_arr list
    for care_type in ["washCare", "dryCare", "ironCare"]:
        if material_care.get(care_type):
            care_label_arr.append(material_care.get(care_type))

    # If the "content" key exists in the material_care dictionary,
    # split its value by comma and append each item to the care_label_arr list
    if material_care.get("content"):
        care_label_arr.extend(material_care.get("content").split(","))

    return care_label_arr


def check_if_bill_of_material_has_trims(body_code):
    """
    Check if a bill of materials has trims
    """
    gql = ResGraphQLClient()
    has_trims = True

    # Get the bill of materials for the body code
    bill_of_materials = gql.query(
        queries.GET_BILL_OF_MATERIALS,
        {"where": {"code": body_code}},
    )["data"]["billsOfMaterials"]["billsOfMaterials"]

    # If the bill of materials exists, check if it has trims
    if bill_of_materials:
        for bill_of_material in bill_of_materials:
            if not bill_of_material.get("trimTaxonomy"):
                has_trims = False

    return has_trims


def validate_if_style_is_trashed(style_id):
    """
    Validate if a style is trashed
    """
    gql = ResGraphQLClient()
    is_trashed = False

    # Get the style for the style id
    style = gql.query(
        queries.GET_STYLE,
        {"where": {"id": {"is": style_id}, "isTrashed": {"is": True}}},
    )["data"]["styles"]["styles"][0]

    # If the style exists, check if it is trashed
    if style:
        is_trashed = True

    return is_trashed


def build_prep_request_payload(
    create_pick_ticket_input,
    care_label,
    cover_photo_url,
    factory_pdf,
    production_request,
):
    request_payload = {
        "makeOneProductionId": create_pick_ticket_input.make_one_production_id,
        "oneNumber": str(create_pick_ticket_input.one_number),
        "trimNotes": create_pick_ticket_input.trim_notes,
        "sku": create_pick_ticket_input.sku,
        "materialCode": create_pick_ticket_input.material_code,
        "productionCoverPhotosRaw": cover_photo_url,
        "factoryPdfRaw": factory_pdf,
        "size": create_pick_ticket_input.size,
        "careLabels": care_label,
        "manualOverrideFactory": production_request.get("manualOverride"),
        "exitFactoryDate": production_request.get("exitFactoryDate"),
        "bodyCode": create_pick_ticket_input.body_code,
        "binLoction": production_request.get("binLocations").get("name")
        if production_request.get("binLocations")
        else "",
        "status": "To Do",
        "brand": create_pick_ticket_input.brand,
        "styleCode": create_pick_ticket_input.style_code,
    }

    return request_payload


def create_pick_ticket(prep_request_id):
    logger.info(f"create_pick_ticket_input: {prep_request_id}")
    gql = ResGraphQLClient()
    # check if alrady exists pick ticket record for this request and delete it
    gql.query(
        queries.DELETE_PICK_TICKET,
        {"where": {"prepRequestId": prep_request_id}},
    )

    # get prep request info
    prep_request = gql.query(
        queries.GET_PREP_REQUEST,
        {"where": {"id": prep_request_id}},
    )["data"]["prepRequests"]["prepRequests"][0]

    # get style info
    style = gql.query(
        queries.GET_STYLE,
        {"where": {"code": prep_request.get("styleCode")}},
    )["data"]["styles"]["styles"][0]

    if style:
        pick_tickets_id = []
        brand_code = style.get("brand").get("code")
        is_main_label_with_size = style["brand"].get("isMainLabelWithSize")
        size_one = prep_request.get("size")
        size_label_color = style["brand"].get("sizeLabelColor")
        styleBillOfMaterials = style.get("styleBillOfMaterials")

        logger.info(
            {
                "brand_code": brand_code,
                "is_main_label_with_size": is_main_label_with_size,
                "size_one": size_one,
                "size_label_color": size_label_color,
                "styleBillOfMaterials": styleBillOfMaterials,
            }
        )

        # build each pick ticket record
        for style_bom in styleBillOfMaterials:
            bill_of_material_record = style_bom.get("billOfMaterial")
            trim_item = style_bom.get("trimItem")

            if bill_of_material_record:
                if bill_of_material_record.get(
                    "trimCategory"
                ) != "Shipping Trim" and bill_of_material_record.get("trimTaxonomy"):
                    material_item_id = None
                    will_create_pick_ticket = True
                    is_include = True

                    trim_friendly_name = bill_of_material_record.get(
                        "styleBomFriendlyName"
                    )

                    logger.info(
                        f"Using trim type: {trim_friendly_name}"
                        + " bill_of_material_record: {bill_of_material_record}"
                    )

                    if trim_friendly_name == "Main Label" and is_main_label_with_size:
                        logger.info("searching for main label with size")
                        material_item_id = get_main_label_with_size(
                            brand_code, size_one
                        )

                    if trim_friendly_name == "Size Label":
                        logger.info("Get the material for Size Label")
                        if is_main_label_with_size:
                            logger.info(
                                "this style use a single label with size and main label information so no needed create size label pick ticket record"
                            )
                            will_create_pick_ticket = False
                        else:
                            material_item_id = get_size_label(
                                bill_of_material_record.get("trimTaxonomy"),
                                brand_code,
                                size_label_color,
                                size_one,
                            )

                    # Check if material is a Hang Tag
                    if trim_friendly_name == "Hang Tag":
                        logger.info("Checking for Hang Tag")

                        # Check if brand code is JR, KT, or TK
                        if brand_code in ["JR", "KT", "TK"]:
                            material_item_id = find_hangtag(brand_code)
                        else:
                            will_create_pick_ticket = False

                    if "Thread" in trim_friendly_name or "Piping" in trim_friendly_name:
                        is_include = False

                    if trim_friendly_name == "Tracking Label":
                        material_item_id = "recOcbnsdfnJEQWoF"

                    if material_item_id is None:
                        material_item_id = trim_item.get("id") if trim_item else None

                    sku_trim = (
                        trim_item.get("skuCode")
                        if trim_item
                        else bill_of_material_record.get("code")
                    )

                    name_trim = (
                        trim_item.get("longName")
                        if trim_item
                        else bill_of_material_record.get("name")
                    )

                    size_trim = bill_of_material_record.get("size", "")

                    if will_create_pick_ticket:
                        logger.info("Creating pick ticket")

                        pick_ticket_payload = {
                            "name": str(sku_trim),
                            "prepRequestId": prep_request.get("id"),
                            "sku": str(sku_trim),
                            "materialName": str(name_trim),
                            "billOfMaterialId": bill_of_material_record.get("id"),
                            "materialItemId": material_item_id,
                            "styleBomId": style_bom.get("id"),
                            "quantity": float(bill_of_material_record.get("quantity")),
                            "sizeTrim": str(size_trim),
                            "isItemPicked": False,
                            "isInclude": is_include,
                        }

                        logger.info(f"pick_ticket_payload: {pick_ticket_payload}")

                        create_pick_ticket = gql.query(
                            queries.CREATE_PICK_TICKETS,
                            {"input": pick_ticket_payload},
                        )

                        if create_pick_ticket:
                            created_pick_ticket = create_pick_ticket.get(
                                "createPrepPickTicket"
                            ).get("prepPickTicket")
                            logger.info(f"Pick Ticket Id: {created_pick_ticket}")
                            pick_tickets_id.append(created_pick_ticket.get("id"))
                            # TODO: insert pick_ticket_payload on hasura

        # update prep request with pick tickets
        payload_request = {
            "pickTicketGeneratedAt": str(arrow.now("US/Eastern").isoformat()),
            "pickTicketIds": pick_tickets_id,
            "status": "In Progress",
        }
        logger.info(payload_request)

        update_prep_request = gql.query(
            queries.UPDATE_PREP_REQUEST,
            {"id": prep_request_id, "input": payload_request},
        )
        logger.info(update_prep_request)


def get_main_label_with_size(brand_code, size_one):
    gql = ResGraphQLClient()
    material_item_id = None
    get_materials = gql.query(
        queries.GET_MATERIAL,
        {
            "where": {
                "trimType": {"is": "Woven Label"},
                "brandCode": {"is": brand_code},
                "size": {"is": size_one},
                "developmentStatus": {"isNot": "Inactive"},
            }
        },
    )

    if get_materials and len(get_materials.get("materials").get("materials")) > 0:
        material_item_id = get_materials.get("materials").get("materials")[0].get("id")
    return material_item_id


def get_size_label(trim_taxonomy, brand_code, size_label_color, size_one):
    gql = ResGraphQLClient()
    logger.info(f"Is Size Label TaxonomyId: {trim_taxonomy.get('id')}")
    material_info = []
    get_materials = gql.query(
        queries.GET_TAXONOMY,
        {"where": {"metaOneId": {"is": trim_taxonomy.get("id")}}},
    )
    material_info = build_material_info(get_materials)
    brand_code_size = "TK" if brand_code == "TK" else "CC"
    trim_color_name = "Light Cream White" if brand_code == "TK" else size_label_color
    request_size = size_one

    # search for the size label id
    material_item_id = search_for_size_label(
        trim_color_name,
        material_info,
        request_size,
        brand_code_size,
    )

    # if the current trim_color_name is not found, search for white size label
    if material_item_id is None and trim_color_name != "White":
        trim_color_name = "White"
        brand_code_size = "CC"
        material_item_id = search_for_size_label(
            trim_color_name,
            material_info,
            request_size,
            brand_code_size,
        )

    return material_item_id


def build_material_info(materials):
    material_info = []
    purchasing_taxonomies = materials.get("purchasingTrimTaxonomies", {}).get(
        "purchasingTrimTaxonomies", []
    )

    if purchasing_taxonomies:
        first_taxonomy = purchasing_taxonomies[0]
        child_taxonomies = first_taxonomy.get("childTaxonomy", {}).get(
            "purchasingTrimTaxonomies", []
        )

        if child_taxonomies:
            for taxonomy in child_taxonomies:
                material_info.extend(taxonomy.get("materials", []))
        else:
            material_info.extend(first_taxonomy.get("materials", []))

    return material_info


def search_for_size_label(trim_color_name, material_info, request_size, brand_code):
    material_item_id = None
    for material_item in material_info:
        if material_item.get("brand"):
            size_match = material_item.get("size") == request_size
            brand_match = material_item.get("brand").get("code") == brand_code
            color_match = material_item.get("trimColorName") == trim_color_name
            is_active = material_item.get("developmentStatus") != "Inactive"

            if size_match and brand_match and color_match and is_active:
                logger.info(
                    f"Found size label ID: {material_item.get('id')} materialItemSize: {material_item.get('size')} requestSize: {request_size} BrandCode: {brand_code} trim_color_name: {trim_color_name}"
                )
                material_item_id = material_item.get("id")
    return material_item_id


def find_hangtag(brand_code):
    gql = ResGraphQLClient()

    material_item_id = None
    # Define query parameters
    query_params = {
        "where": {
            "trimType": {"is": "Hangtag"},
            "brandCode": {"is": brand_code},
            "developmentStatus": {"isNot": "Inactive"},
        }
    }

    # Query for materials
    materials_data = gql.query(queries.GET_MATERIAL, query_params)

    # If materials are found, get the first one
    materials = materials_data.get("materials", {}).get("materials", [])
    if materials:
        material_item_id = materials[0].get("id")

    return material_item_id
