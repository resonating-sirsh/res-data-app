from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res.flows.meta.style.graphql_queries as graphql_queries
from res.utils import logger
import json
import res
from datetime import datetime


def map_style_pieces_to_onboarding_materials(record_id):
    """
    When a style is created, Body x Material x Color should be assigned at style level.
    that given "Material" is the Main material for the style and all body pieces are initially mapped to it
    by default unless the stylePieces mapping specifies a different material.

    combo & lining pieces require to be mapped to different materials (not necessarily the main/self material)
    """
    gql = ResGraphQLClient()
    style_response = gql.query(graphql_queries.GET_STYLE, variables={"id": record_id})

    if style_response.get("errors"):
        raise Exception(f"Error with the request: {style_response.get('errors')}")
    style = style_response["data"]["style"]

    body_onboarding_combo_materials = style["body"]["onboardingComboMaterials"]
    body_onboarding_lining_materials = style["body"]["onboardingLiningMaterials"]

    piece_id_to_piece_type_code_mapping = {
        p["id"]: p["pieceTypeCode"] for p in style["body"]["pieces"]
    }

    body_piece_types = list(piece_id_to_piece_type_code_mapping.values())
    if "C" or "L" in body_piece_types:
        if "C" in body_piece_types and (
            not body_onboarding_combo_materials
            or len(body_onboarding_combo_materials) == 0
        ):
            raise Exception("Body is missing Onboarding Combo Materials to be mapped.")
        if "L" in body_piece_types and (
            not body_onboarding_lining_materials
            or len(body_onboarding_lining_materials) == 0
        ):
            raise Exception("Body is missing Onboarding Lining Materials to be mapped.")

        onboarding_combo_material = (
            style["body"]["onboardingComboMaterials"][0]
            if body_onboarding_combo_materials
            and len(body_onboarding_combo_materials) > 0
            else None
        )

        onboarding_lining_material = (
            style["body"]["onboardingLiningMaterials"][0]
            if body_onboarding_lining_materials
            and len(body_onboarding_lining_materials) > 0
            else None
        )

        style_piece_mapping_updates = []
        for (
            body_piece_id,
            piece_type_code,
        ) in piece_id_to_piece_type_code_mapping.items():
            logger.info(f"{body_piece_id} {piece_type_code}")
            if piece_type_code == "C":
                style_piece_mapping_updates.append(
                    {
                        "bodyPieceId": body_piece_id,
                        "materialCode": onboarding_combo_material["code"],
                    }
                )
            elif piece_type_code == "LN":
                style_piece_mapping_updates.append(
                    {
                        "bodyPieceId": body_piece_id,
                        "materialCode": onboarding_lining_material["code"],
                    }
                )
        logger.info(
            f"mapping lining/combo materials to pieces: {style_piece_mapping_updates}"
        )
        style = gql.query(
            graphql_queries.UPDATE_STYLE_PIECES,
            variables={"id": record_id, "stylePieces": style_piece_mapping_updates},
        )["data"]["updateStylePieces"]["style"]

    return {"style": style}


def assign_trims_to_first_one_style(style_id):
    gql = ResGraphQLClient()

    style = gql.query(graphql_queries.GET_STYLE, variables={"id": style_id})["data"][
        "style"
    ]

    bill_of_materials = gql.query(
        graphql_queries.GET_BILL_OF_MATERIAL_BY_BODY_CODE,
        variables={"where": {"body": {"is": style["body"]["code"]}}},
    )["data"]

    style_bom_ids = []
    if (
        bill_of_materials
        and bill_of_materials["billOfMaterials"]
        and bill_of_materials["billOfMaterials"]["billOfMaterials"]
    ):
        for trim in bill_of_materials.get("billOfMaterials").get("billOfMaterials"):
            if trim.get("trimTaxonomy"):
                create_payload = {
                    "name": trim.get("styleBomFriendlyName"),
                    "styleCode": style.get("code"),
                    "styleId": style.get("id"),
                    "styleVersion": style.get("version"),
                    "billOfMaterialId": trim.get("id"),
                    "include": True,
                }

                if "label" in trim["styleBomFriendlyName"].lower():
                    print(f"{trim['styleBomFriendlyName']} is a label")
                    if (
                        trim["styleBomFriendlyName"] == "Mask Care/Legal Label"
                        or trim["styleBomFriendlyName"] == "Care Label"
                    ):
                        create_payload["materialId"] = get_care_label_item_id(
                            trim["styleBomFriendlyName"],
                            style,
                            style.get("brand").get("name"),
                        )
                    if (
                        trim["styleBomFriendlyName"] == "Main Label"
                        and style.get("brand").get("materialMainLabel") is not None
                    ):
                        create_payload["materialId"] = (
                            style.get("brand").get("materialMainLabel").get("id")
                        )

                else:
                    create_payload["materialId"] = get_trim_item_id_by_taxonomy_id(
                        trim.get("trimTaxonomy").get("id")
                    )

                create_bom = gql.query(
                    graphql_queries.CREATE_STYLE_BILL_OF_MATERIAL,
                    variables={"input": create_payload},
                )
                style_bom_ids.append(
                    create_bom.get("data")
                    .get("createStyleBillOfMaterials")
                    .get("styleBillOfMaterials")
                    .get("id")
                )
    update_style = gql.query(
        graphql_queries.UPDATE_STYLE,
        variables={
            "id": style_id,
            "input": {
                "styleBillOfMaterialsIds": style_bom_ids,
                "requiredTrimGate": None,
                "requiredTrimCheckedAt": datetime.utcnow().strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            },
        },
    )


def get_trim_item_id_by_taxonomy_id(taxonomy_id):
    gql = ResGraphQLClient()
    # get purchasing taxonomy
    get_purchasing_taxonomies = gql.query(
        graphql_queries.GET_ITEM_TRIM_TAXONOMY,
        variables={"where": {"metaOneId": {"is": taxonomy_id}}},
    )["data"]

    if get_purchasing_taxonomies:
        materials = []
        purchasing_taxonomies = get_purchasing_taxonomies["purchasingTrimTaxonomies"][
            "purchasingTrimTaxonomies"
        ][0]

        # get materials record from child trim taxonomy.
        if (
            len(
                purchasing_taxonomies.get("childTaxonomy").get(
                    "purchasingTrimTaxonomies"
                )
            )
            > 0
        ):
            for taxonomy in purchasing_taxonomies.get("childTaxonomy").get(
                "purchasingTrimTaxonomies"
            ):
                for material in taxonomy.get("materials"):
                    materials.append(material)
        else:
            for material in purchasing_taxonomies.get("materials"):
                materials.append(material)

        # check for item white color
        if len(materials) > 0:
            for material in materials:
                if "white" in material.get("trimColorName").lower():
                    return material.get("id")


def get_care_label_item_id(trim_name, style_info, brand_name):
    import res.flows.meta.material.graphql_queries as material_graphql_queries

    gql = ResGraphQLClient()
    item_id = None

    if style_info.get("brand"):
        brand_code = (
            "- NON BRANDED" if brand_name else style_info.get("brand").get("code")
        )

        _brand_name = (
            "TUCKER"
            if brand_code == "TK"
            else "THE KIT"
            if brand_code == "KT"
            else "- NON BRANDED"
        )

        search = "Care Content & Legal Mask Label"
        if trim_name == "Care Label":
            search = (
                "ONE Experience Care Label"
                if brand_code == "RV"
                else f"CARE LABEL {_brand_name}"
            )

        if style_info["material"] is not None:
            get_materials = gql.query(
                material_graphql_queries.GET_MATERIALS,
                variables={
                    "where": {
                        "trimType": {"is": "Woven Label"},
                        "developmentStatus": {"isNot": "Inactive"},
                        "applicableMaterial": {"isNot": None},
                    },
                    "search": search,
                },
            )["data"]

            if get_materials:
                material_id = None
                for material in get_materials["materials"]["materials"]:
                    if (
                        material.get("applicableMaterial").find(
                            style_info.get("material").get("code")
                        )
                        >= 0
                    ):
                        material_id = material.get("id")

                if material_id:
                    item_id = (
                        material_id
                        if material_id
                        else style_info.get("material").get("careLabelItemID")
                    )
                else:
                    # validate is brand_name is not NON BRANDED we can do a recursion to search for Non Branded Care Label
                    if _brand_name != "- NON BRANDED":
                        return get_care_label_item_id(
                            trim_name, style_info, "- NON BRANDED"
                        )
                    else:
                        item_id = style_info.get("material").get("careLabelItemID")
            else:
                if _brand_name != "- NON BRANDED":
                    return get_care_label_item_id(
                        trim_name, style_info, "- NON BRANDED"
                    )
                else:
                    item_id = style_info.get("material").get("careLabelItemID")

    return item_id


def get_style_bill_of_material(style_code, style_id):
    gql = ResGraphQLClient()

    where = {"styleCode": {"is": style_code}}
    if style_code is None:
        where = {"styleId": {"is": style_id}}

    if style_code is None and style_id is None:
        raise Exception("Style Code or Style ID must be provided")

    get_style_bom = gql.query(
        graphql_queries.GET_STYLE_BILL_OF_MATERIAL,
        variables={"where": where},
    )["data"]

    result = []

    if get_style_bom:
        for style_bom in get_style_bom["styleBillsOfMaterials"][
            "styleBillsOfMaterials"
        ]:
            if style_bom["billOfMaterial"]["styleBomFriendlyName"]:
                bill_of_material_id = style_bom["billOfMaterial"]["id"]
                trim_bom_name = style_bom["billOfMaterial"]["styleBomFriendlyName"]

                # Initialize trim_item_id and trim_item_name as None
                trim_item_id = None
                trim_item_name = None

                if style_bom.get("trimItem"):
                    trim_item_id = style_bom.get("trimItem", {}).get("id", "")
                    trim_item_name = style_bom.get("trimItem", {}).get("longName", "")

                record = {
                    "bill_of_material_id": bill_of_material_id,
                    "trim_bom_name": trim_bom_name,
                    "trim_item_id": trim_item_id,
                    "trim_item_name": trim_item_name,
                }

                result.append(record)

    return result
