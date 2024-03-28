import os
import re
import uuid
from res.flows.FlowContext import FlowContext
from res.media.images import outlines
from res.media.images.providers import DxfFile
from res.media.images.providers.dxf import SIZE_NAMES
import res
from res.connectors.graphql import ResGraphQL, Queries

from res.flows.meta.generate_style_size_pieces.graphql_queries import *
from res.media.images import validators as image_validators

META_ONE_S3_BUCKET = "meta-one-assets-prod"
MISSING_PIXELS_ISSUE_TYPE_ID = "recbRgi9sCIp9UtQE"
DOESNT_FIT_CUTTABLE_WIDTH_ISSUE_TYPE_ID = "recyUeYO5ZzEdoWKX"
COLOR_ON_SHAPE_NODE_ID = "346"

gcl = ResGraphQL()


def _to_snake_case(name):
    # this is a really lazy version of this - need to do better
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("__([A-Z])", r"_\1", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower().replace(" ", "_").replace("__", "_")


def request(query, variables):
    body = gcl.query(query=query, variables=variables)
    errors = body.get("errors")
    if errors:
        error = errors[0]
        raise ValueError(error)
    return body["data"]


def get_generate_style_size_pieces_payload(event, context=None):
    record_id = event.get("record_id")
    environment = os.environ.get("RES_ENV")
    if environment == "production":
        updated_asset_request = request(
            UPDATE_ASSET_REQUEST_MUTATION,
            {
                "id": record_id,
                "input": {"colorOnShapePiecesCollectionStatus": "In Progress"},
            },
        )["updateAssetRequest"]["assetRequest"]
    else:
        updated_asset_request = request(
            GET_ASSET_REQUEST,
            {
                "id": record_id,
            },
        )["assetRequest"]

    style = updated_asset_request["style"]
    size = updated_asset_request["size"]

    body = style["body"]
    color = style["color"]
    # marker = asset_request['marker']

    prepared_net_file = updated_asset_request["preparedNetFile"]
    prepared_net_file_bucket = prepared_net_file["s3"]["bucket"]
    prepared_net_file_key = prepared_net_file["s3"]["key"]

    snake_cased_body_code = _to_snake_case(body["code"]).replace("-", "_")
    snake_cased_color_code = _to_snake_case(color["code"])

    base_key = f"color_on_shape/{snake_cased_body_code}/v{body['patternVersionNumber']}/{snake_cased_color_code}/{size['code'].lower()}"
    rasterized_image_key = f"{base_key}/rasterized_files/{str(uuid.uuid4())}.png"

    split_pieces_input_uri = f"s3://{META_ONE_S3_BUCKET}/{base_key}/pieces"

    return {
        "asset_request_record_id": record_id,
        "style_id": style["id"],
        "size_id": size["id"],
        ##SA adding#########################
        "key": style["code"],
        "body_code": body["code"],
        "color": color["code"],
        "size": size["code"],
        "version": f"v{body['patternVersionNumber']}",
        #########################################
        "rasterization_input_file_bucket": prepared_net_file_bucket,
        "rasterization_input_file_key": prepared_net_file_key,
        "rasterization_output_file_bucket": META_ONE_S3_BUCKET,
        "rasterization_output_file_key": rasterized_image_key,
        "split_pieces_input_uri": split_pieces_input_uri,
    }


#### add piece splitting flow here (two functions)
#    use this function instead of the one in split_pieces
#    this will split and name pieces but also create new records in "Make DB"
#    to act as a gate for a one specification i.e. something that is makeable


def get_body_color_file_path(body, version, color, size):
    """
    example
    body: CC-6001
    version: v2
    color: PERPIT
    size: zzzsm
    """

    snake_cased_body_code = _to_snake_case(body).replace("-", "_")
    snake_cased_color_code = _to_snake_case(color)
    meta_one_bucket = "meta-one-assets-prod"
    base_key = f"color_on_shape/{snake_cased_body_code}/{version}/{snake_cased_color_code}/{size.lower()}"

    return f"s3://{meta_one_bucket}/{base_key}"


def fetch_piece_materials(body, version, color):
    """
    Fetch the materials for each piece in a given style.
    e.g {'TOPFT': 'CTNSP','AHBDG': 'CTNBA',...}
    """

    res.utils.logger.debug(
        f"Making request for styles with body, version, color { (body, version, color)}"
    )

    style_pieces = request(
        GET_STYLE_PIECES,
        {
            "where": {
                "colorCode": {"is": color},
                "bodyCode": {"is": body},
                # "materialCode": {"is": "CTNSP"},
            }
        },
    )
    styles = style_pieces.get("styles", {}).get("styles", {})

    piece_code_to_material_mappings = []
    for style in styles:
        version_num = style.get("body").get("patternVersionNumber")
        if f"v{version_num}" != version:
            continue

        default_material = style.get("material", {}).get("code")
        piece_codes = [p.get("code") for p in style.get("body").get("pieces")]

        material_overrides = {
            s.get("bodyPiece").get("code"): s.get("material").get("code")
            for s in style.get("stylePieces")
        }

        piece_code_to_material = {
            pc: material_overrides.get(pc) or default_material for pc in piece_codes
        }
        piece_code_to_material_mappings.append(piece_code_to_material)

    return piece_code_to_material_mappings


def _save_named_image_pieces(fc, path, named_pieces):
    """
    loops over all rasters TODO
    add more gates on the images

    """
    res.utils.logger.debug(
        f"Using image path {path} to find rasterized input files and to save named pieces"
    )

    s3 = fc.connectors["s3"]
    im = s3.read(
        f"{path}/rasterized_files/{fc.args.get('rasterized_filename', 'rasterized_no_buffer.png')}"
    )
    # body color assets
    # using ""named pieces""" to keep separate from the existing pieces for now
    for name, part in outlines.enumerate_pieces(
        im,
        named_pieces,
        # old conventions needed to this because of the shape of the outline but we dont
        invert_named_pieces=False,
        # we will log masks etc to a node location for this job
        log_file_fn=fc.get_logging_function("mask", key=res.utils.hash_of(path)),
    ):
        piece_image = f"{path}/named_pieces/{name}.png"
        res.utils.logger.debug(f"saving file {piece_image}")
        s3.write(piece_image, part)

    if len(named_pieces) > 0:
        res.utils.logger.warn(
            f"The following pieces were not named!! {list(named_pieces.keys())}"
        )
        raise Exception(
            "The step failed - unable to name pieces which is the entire point of this step!"
        )
        # this should really fail a gate but for testing i am letting it pass - maybe depend on the env


def _piece_export_payload_from_asset_request(id):
    """
    helper to unpack
    """
    gcl = ResGraphQL()
    # xample of a CC-6001 PERPIT recOcgJepblEly9UJ
    rec = gcl.query_with_kwargs(
        Queries.GET_ASSET_REQUEST, path="data.assetRequest", id=id
    )

    style = rec["style"]

    return {
        "metadata": {"name": "dxa.generate_style", "node": "name_pieces"},
        "assets": [
            {
                "key": style["code"],
                "body_key": style["body"]["code"],
                "version": f"v{style['body']['patternVersionNumber']}",
                "color": style["color"]["code"],
                "size": rec["size"]["code"],
            }
        ],
    }


def split_pieces_and_export_spec(event, context=None):
    """
    Given the asset structure we should know how to create specs

    Example
        event = {
            "assets": [{
                "key" : "style_code_here",
                "body_code": "TK-6093",
                "version": "v5",
                "color": "LEOPAR",
                "size": "2ZZSM",
            }  ],
            "args":{
                "fuse_buffer_function": True
            }
        }

    #given an asset request id from [here](https://airtable.com/appqtN4USHTmyC6Dv/tblwhgsCzGpt7odBg/viwFtmmk6Hhhouxg9?blocks=hide)
    we can use a helper to create a payload for this step

        example_req_id = 'recnx7v19ZcVjc7TR'
        _piece_export_payload_from_asset_request(example_req_id)

    If we want we can force arg: "rasterized_filename" : "rasterized_no_buffer.png" and not use the fuse buffer
    this is a back compat thing to match pieces even when they have a buffer -< not for production but just to test against other files
    """
    # ensure event has all properties and promote to asset structure

    from res.flows.meta.styles import export_styles

    with FlowContext(event, context, node="name_style_pieces") as fc:
        # for testing in production adding error suppression to the flow  #
        # THIS MEANS FOR TESTING WE WILL NOT KILL ARGO WHEN THIS STEP FAILS BUT JUST LOG ERRORS
        fc.suppress_failures = True
        # for testing turn off sentry by incrementing the logger error count to anything
        fc._logged_error_count = 1
        #####################
        fc.logger.debug(f"Processing event {event}")

        # TODO: Today most pieces have a block fuse buffer but we can change this or try to auto detect
        # this is used to name pieces with confidence as we need to buffer the astm to match the color on shape
        block_fuse_buffer = fc.args.get(
            "block_fuse_buffer",
            0.75 * 300,
        )

        # TODO use the style exporter here instead based on fc.assets
        # want to isolate this....

        export_styles(
            fc.assets,
            resolve_asset_contract=True,
            block_fuse_buffer=block_fuse_buffer,
            fc=fc,
        )
        return

        buffer_fn = fc.args.get("fuse_buffer_function")
        for body_color_style in fc.assets:
            # try:
            # contract set lineage
            body_color_style["prev_key"] = body_color_style.get(
                "asset_request_record_id"
            )
            fc.publish_asset_status(body_color_style, "name_pieces", "", "IN_PROGRESS")
            body = body_color_style["body_code"]
            res_size = body_color_style["size"]
            version = body_color_style["version"]
            color = body_color_style["color"]

            normed_size = (
                request(GET_SIZES, {"size_code": res_size}).get("size", {}).get("name")
            )
            NORMED_SIZE_NAMES = {v: k for k, v in SIZE_NAMES.items()}
            gerber_size = NORMED_SIZE_NAMES.get(normed_size)
            fc.logger.debug(
                f"Using size mapping res->normed->gerber: {res_size}->{normed_size}->{gerber_size} for body {body} and color {color}"
            )

            snakey_body = _to_snake_case(body).replace("-", "_")
            body_path = f"s3://meta-one-assets-prod/bodies/{snakey_body}/pattern_files/body_{snakey_body}_{version}_pattern.dxf"

            fc.logger.debug(f"Using ASTM file {body_path}")
            dxf = DxfFile(body_path, fuse_buffer_function=buffer_fn)
            # derive the name of the file that has body on color or in future maybe generate something from grid awareness here
            path = get_body_color_file_path(body, version, color, res_size)
            named_pieces = dxf.get_named_piece_image_outlines(
                size=gerber_size, piece_types=["self", "block_fuse"]
            )
            _save_named_image_pieces(fc, path, named_pieces)

            # exported thing is 1:1 with a style, should name with a hash (maybe for now just hash style_id), for legacy maybe include style_id
            # Validate that all pieces are accounted for
            piece_materials = fetch_piece_materials(body, version, color)
            res.utils.logger.debug(f"got piece material mapping {piece_materials}")
            fc.logger.debug(
                f"exporting size {gerber_size} for materials {piece_materials}"
            )

            DxfFile.export_meta_marker(
                dxf,
                sizes={gerber_size: res_size},
                color_code=[color],
                piece_material_map=piece_materials,
                # todo add material props
                # todo try fetch sew instructions
            )

            # df = dxf.export_body(sizes=[gerber_size])
            # # TODO - for the spec we might want some way to actually assign the style to the spec for querying
            # df = dxf.export_body_specs(
            #     sizes={gerber_size: res_size},
            #     # TODO we would later map colors to pieces but we can now generate all colors as a cross product
            #     colors=[color],
            #     # materials are mapped to pieces
            #     materials=piece_materials,
            # )
            fc.publish_asset_status(body_color_style, "name_pieces", "", "DONE")
        # except Exception as ex:
        #     exc_type, exc_obj, exc_tb = sys.exc_info()
        #     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        #     logger.warn(
        #         f"Failed when processing asset {body_color_style} -> {fname}, {exc_tb.tb_lineno} : {repr(ex)}"
        #     )
        #     fc.publish_asset_status(
        #         body_color_style, "name_pieces", repr(ex), "FAILED"
        #     )

        res.utils.logger.info("done - goodbye and thanks to all the fishes")


#####


def look_for_missing_pixels(s3_file_uri):
    return image_validators.look_for_missing_pixels(s3_file_uri)


def validate_piece_exceeds_material_cuttable_width(s3_file_uri, material_code):
    return image_validators.validate_piece_exceeds_material_cuttable_width(
        s3_file_uri, material_code
    )


def validate_generated_pieces(pieces_s3_keys, material_code):
    pieces_with_missing_pixels = []
    pieces_exceeding_cuttable_width = []

    for piece_s3_key in pieces_s3_keys:
        piece_uri = f"s3://{META_ONE_S3_BUCKET}/{piece_s3_key}"
        has_missing_pixels = look_for_missing_pixels(piece_uri)

        if has_missing_pixels:
            pieces_with_missing_pixels.append(piece_uri)

        # validate pieces are too wide
        piece_its_too_wide, _ = validate_piece_exceeds_material_cuttable_width(
            piece_uri, material_code
        )
        if piece_its_too_wide:
            pieces_exceeding_cuttable_width.append(piece_uri)

    return {
        "pieces_with_missing_pixels": pieces_with_missing_pixels,
        "pieces_exceeding_cuttable_width": pieces_exceeding_cuttable_width,
    }


def finish_generate_style_size_pieces_job(event):
    record_id = event.get("record_id")
    pieces_s3_folder = event.get("pieces_s3_folder")
    pieces_keys = event.get("pieces_keys")

    environment = os.environ.get("RES_ENV")

    asset_request = request(GET_ASSET_REQUEST, {"id": record_id})["assetRequest"]
    material_code = asset_request["marker"]["material"]["code"]
    pieces_validation_result = validate_generated_pieces(pieces_keys, material_code)

    total_pieces_with_missing_pixels = len(
        pieces_validation_result["pieces_with_missing_pixels"]
    )
    total_pieces_exceeding_cuttable_width = len(
        pieces_validation_result["pieces_exceeding_cuttable_width"]
    )
    should_flag_request = (
        total_pieces_with_missing_pixels > 0
        or total_pieces_exceeding_cuttable_width > 0
    )

    if should_flag_request:
        context = f"""{f"WARNING: There may {total_pieces_exceeding_cuttable_width} pieces that will exceeds the material cuttable width." if total_pieces_exceeding_cuttable_width > 0 else ""}
        {f"WARNING: There may be missing pixels in {total_pieces_with_missing_pixels} pieces." if total_pieces_with_missing_pixels > 0 else ""}"""

        issue_type_ids = []
        if total_pieces_with_missing_pixels > 0:
            issue_type_ids.append(MISSING_PIXELS_ISSUE_TYPE_ID)

        if total_pieces_exceeding_cuttable_width > 0:
            issue_type_ids.append(DOESNT_FIT_CUTTABLE_WIDTH_ISSUE_TYPE_ID)

    if environment == "production":
        if should_flag_request:
            assignee_email = (
                asset_request["assignee"]["email"]
                if asset_request["assignee"] and asset_request["assignee"]["email"]
                else "techpirates@resonance.nyc"
            )
            request(
                FLAG_ASSET_REQUEST,
                {
                    "id": asset_request["id"],
                    "input": {
                        "flaggedForReviewByEmail": "techpirates@resonance.nyc",
                        "flagForReviewOwnerEmail": assignee_email,
                        "isFlaggedForReview": True,
                        "flagForReviewReason": context,
                        "issues": {
                            "context": context,
                            "subject": asset_request["name"],
                            "dxaNodeId": COLOR_ON_SHAPE_NODE_ID,
                            "issueTypesIds": issue_type_ids,
                            "sourceRecordId": asset_request["id"],
                            "ownerEmail": assignee_email,
                            "type": "Flag for Review",
                        },
                    },
                },
            )

        payload_input = {
            "totalPiecesInNetFile": len(pieces_keys),
            "piecesS3FolderPath": pieces_s3_folder,
            "colorOnShapePiecesCollection": pieces_keys,
            "colorOnShapePiecesCollectionStatus": "Done",
            "piecesValidation": {},
        }

        if total_pieces_with_missing_pixels > 0:
            payload_input["piecesValidation"][
                "piecesWithMissingPixels"
            ] = pieces_validation_result["pieces_with_missing_pixels"]

        if total_pieces_exceeding_cuttable_width > 0:
            payload_input["piecesValidation"][
                "piecesExceedingCuttableWidth"
            ] = pieces_validation_result["pieces_exceeding_cuttable_width"]

        updated_asset_request = request(
            SET_COLOR_ON_SHAPE_PIECES_MUTATION,
            {
                "id": record_id,
                "input": payload_input,
            },
        )["setColorOnShapePiecesInformation"]["assetRequest"]

        # auto approve pieces if we are re-using a prepared net file.
        pieces_matches = updated_asset_request.get(
            "totalPiecesInNetFile"
        ) == updated_asset_request.get("numberOfPiecesInMarker")
        should_auto_approve_pieces = (
            updated_asset_request.get("autoApproveColorOnShapePieces", False)
            and pieces_matches
        )

        if should_auto_approve_pieces:
            request(APPROVE_COLOR_ON_SHAPE_PIECES, {"id": updated_asset_request["id"]})

        return updated_asset_request
    else:
        return None
