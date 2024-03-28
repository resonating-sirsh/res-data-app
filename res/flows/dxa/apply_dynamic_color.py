import res
from res.flows import FlowContext
from res.utils import logger
from res.flows.meta.ONE.style_node import MetaOneNode
from PIL import Image, ImageDraw, ImageChops, ImageFont
import numpy as np
from res.utils.meta.sizes.sizes import get_alias_to_size_lookup
from shapely.geometry import shape, Point
from shapely.affinity import rotate, translate
import os
import math
from res.flows.meta.one_marker.apply_color_request import flag_apply_color_request
from res.flows.meta.one_marker.apply_color_request import unflag_apply_color_request
import traceback
import json
import re
import os
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows.meta.pieces import PRINTABLE_PIECE_TYPES
from tenacity import retry, wait_exponential, stop_after_attempt

# from schemas.pydantic.design import PlaceholderValues

wait = wait_exponential(multiplier=1, min=4, max=10)
stop = stop_after_attempt(3)

STYLE_EDITOR_TYPE_3D = "3D Style Editor"
STYLE_EDITOR_TYPE_2D = "2D Style Editor"

os.environ["RES_APP_NAME"] = "apply-dynamic-color"

# enumerate the issues here:
PIECE_MATERIAL_MAPPING = "PIECE_MATERIAL_MAPPING"
PIECE_FITS_CUTTABLE_WIDTH = "PIECE_FITS_CUTTABLE_WIDTH"
STYLE_PIECES_EXIST = "STYLE_PIECES_EXIST"
SINGLE_MATCHING_STYLE = "SINGLE_MATCHING_STYLE"
META_ARTWORK_EXISTS = "META_ARTWORK_EXISTS"
ALL_COLOR_PIECES = "ALL_COLOR_PIECES"
UNKNOWN_ERROR = "UNKNOWN_ERROR"


DPI = 300
UUID_REGEX = (
    "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

GET_BODY_METADATA = """
    query ($body_code: String!){
        meta_bodies(where: {body_code: {_eq: $body_code}}, limit: 1) {
            metadata
        }
    }
"""

GET_BODY_SIZES = """
    query get_unique_sizes($body_code: String!, $version:numeric){
        meta_bodies(
          where: {
            body_code: {_eq: $body_code},
            version: {_eq: $version},
          }
      	) {
        	body_pieces(
            where: { deleted_at:{_is_null: true}}
            distinct_on: size_code
          ) {
            size_code
          }
      }
    }
"""

GET_BODY_PIECE = """
    query ($key: String!){
        meta_body_pieces(where: {key: {_eq: $key}}, limit: 1) {
            id
            key
            outer_geojson
            inner_geojson
        }
    }
"""

GET_ARTWORK_BY_SOURCE_ID = """
query GetArtworkBySourceId($source_id: String) {
      meta_artworks(where: {
            metadata: {
                _contains: {
                    source_id: $source_id
                }
            }
        }, limit: 1) {
            name
            dpi_300_uri
            dpi_72_uri
        }
    }
"""

GET_ARTWORK_BY_ID = """
query GetArtworkById($id: uuid!) {
    meta_artworks(where: {id: {_eq: $id}}, limit: 1) {
        name
        dpi_300_uri
        dpi_72_uri
    }
}
"""

GET_FONT_BY_ID = """
query GetFontById($id: uuid!) {
    meta_fonts(where: {id: {_eq: $id}}, limit: 1) {
        name
        uri
    }
}
"""

GET_STYLE_PIECE_MAPPING = """
query get_piece_materials($sku: String, $id: ID) {
  style(code: $sku, id: $id) {
    pieceMapping {
      bodyPiece {
        code
        id
        name
      }
      artwork {
        id
        file {
            s3 {
                key
                bucket
            }
        }
      }
      material {
        code
        cuttableWidth
        printFileCompensationWidth
        offsetSizeInches
      }
    }
  }
}
"""

GET_DEFAULT_ARTWORK = """
query get_default_artwork($sku: String, $id: ID) {
  style(code: $sku, id: $id) {
    artworkFile {
        id
        file {
            s3 {
                key
                bucket
            }
        }
    }
  }
}
"""

GET_DESIGN = """
query get_design($id: uuid!) {
    meta_designs(where: {id: {_eq: $id}}, limit: 1) {
        body_id
        body_code
        body_version
        color_code
        design
        body {
            body_pieces {
                piece_key
            }
        }
    }
}
"""

GET_SIZE_ALIAS = """
query get_size_alias($id: uuid!) {
    meta_body_pieces(where: {body_id: {_eq: $id}}) {
        vs_size_code
        size_code
    }
}"""


def generate_design(
    design_id, sizeless_sku, body_version, apply_color_rec_id=None, testing=False
):
    import res
    from res.flows import FlowEventProcessor

    key = design_id.split("-")[0]

    event = FlowEventProcessor().make_sample_flow_payload_for_function(
        "dxa.apply_dynamic_color",
        key=key,
    )
    sku_parts = sizeless_sku.split(" ")
    body_code = sku_parts[0]

    event["args"] = {
        "design_id": design_id,
        "body_code": body_code,
        "body_version": body_version,
        "sizeless_sku": sizeless_sku,
        "publish_kafka": not testing,
    }

    if apply_color_rec_id is not None:
        event["args"]["apply_color_request_record_id"] = apply_color_rec_id

    unique_job_name = f"dxa-apply-dynamic-color-{key}"

    argo = res.connectors.load("argo")

    df = argo.get_workflows()
    q = (df["name"] == unique_job_name) & (
        (df["phase"] == "Running") | (df["phase"] == "Pending")
    )
    if len(df) > 0 and len(df[q]) > 0:
        logger.info(
            f"    Existing workflow found for {unique_job_name}, terminating..."
        )
        df[q].apply(lambda x: argo.delete_workflow(x["name"]), axis=1)

    argo.handle_event(event, unique_job_name=unique_job_name)


def sku_to_sizeless_sku(sku):
    # make sure sku is just 3 parts
    sku_parts = sku.split(" ")
    if len(sku_parts) < 3:
        raise Exception(f"Invalid sku {sku}")
    return " ".join(sku_parts[:2])


@retry(wait=wait, stop=stop)
def sku_to_airtable_style_record_id(sku):
    sku = sku_to_sizeless_sku(sku)

    airtable = res.connectors.load("airtable")
    styles = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
    filters = f"""AND(SEARCH("{sku}",{{Resonance_code}}))"""
    # fields = ["_recordid", "Style Code"]
    df = styles.to_dataframe(filters=filters)  # , fields=fields)
    if len(df) == 0:
        return None
    return df["_recordid"][0]


@retry(wait=wait, stop=stop)
def sku_to_airtable_apply_color_request_record_id(sku):
    sku = sku_to_sizeless_sku(sku)

    airtable = res.connectors.load("airtable")
    apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    filters = f"""AND(SEARCH("{sku}",{{Style Code}}))"""
    # fields = ["_recordid"]
    df = apply_color_requests.to_dataframe(filters=filters)  # , fields=fields)
    if len(df) == 0:
        return None
    return df["record_id"][0]


@retry(wait=wait, stop=stop)
def set_airtable_flow_fields(
    sku=None,
    style_record_id=None,
    apply_color_request_record_id=None,
    editor_type=STYLE_EDITOR_TYPE_3D,
):
    airtable = res.connectors.load("airtable")

    style_record_id = style_record_id or sku_to_airtable_style_record_id(sku)
    apply_color_request_record_id = (
        apply_color_request_record_id
        or sku_to_airtable_apply_color_request_record_id(sku)
    )

    if style_record_id:
        logger.info(f"    Updating style record id...")
        styles = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
        styles.update_record(
            {
                "record_id": style_record_id,
                "Style Editor Type": editor_type,
                "Print Type": "Placement",
                "Apply Color Flow Type": "Apply Dynamic Color Workflow",
            }
        )
        logger.info(f"    Updating style record id...done")

    if apply_color_request_record_id:
        logger.info(f"    Updating apply color request...")
        apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
        apply_color_requests.update_record(
            {
                "record_id": apply_color_request_record_id,
                "Apply Dynamic Color Status": "To Do",
                "Color Application Mode": "Automatically",
                "Apply Color Flow Type": "Apply Dynamic Color Workflow",
                "Color Type": "Custom",
                "Requires Turntable Brand Feedback": False,
            }
        )
        logger.info(f"    Updating apply color request...done")


@retry(wait=wait, stop=stop)
def update_apply_color_request_status(fc, status):
    record_id = fc.args.get("apply_color_request_record_id")
    if record_id:
        logger.info(f"    Updating apply color request status to {status}...")
        airtable = fc.connectors["airtable"]
        apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
        apply_color_requests.update_record(
            {"record_id": record_id, "Apply Dynamic Color Status": status}
        )


@retry(wait=wait, stop=stop)
def get_style_record_id(fc):
    style_record_id = None
    try:
        record_id = fc.args.get("apply_color_request_record_id")
        if record_id:
            logger.info(f"    Getting style record id...")
            airtable = fc.connectors["airtable"]
            apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
            filters = f"""OR(SEARCH("{record_id}",{{_record_id}}))"""
            df = apply_color_requests.to_dataframe(filters=filters)
            if len(df) > 0:
                style_record_id = df["Style ID"][0]
    except Exception as e:
        logger.info(f"    Error getting style record id: {e}, {traceback.format_exc()}")
    finally:
        return style_record_id


@retry(wait=wait, stop=stop)
def get_style_piece_mapping(fc):
    sizeless_sku = fc.args.get("sizeless_sku")
    gql = ResGraphQLClient()

    results = gql.query(GET_STYLE_PIECE_MAPPING, variables={"sku": sizeless_sku})
    if results.get("data", {}).get("style") is None:
        logger.info(f"    No style found for {sizeless_sku}, trying by style id...")
        style_id = get_style_record_id(fc)
        if style_id:
            results = gql.query(GET_STYLE_PIECE_MAPPING, variables={"id": style_id})
        else:
            results = None

    if results and results.get("data", {}).get("style") is not None:
        return results["data"]["style"]["pieceMapping"]

    # if we get here there's no matching style, if it's a test one then we can return
    # a default, we'll use publish_kafka as the check for if it's outside of normal flow
    if fc.args.get("publish_kafka"):
        raise Exception(f"No style found for {sizeless_sku}")

    return {}


@retry(wait=wait, stop=stop)
def get_default_artwork_id(fc):
    sizeless_sku = fc.args.get("sizeless_sku")
    gql = ResGraphQLClient()

    results = gql.query(GET_DEFAULT_ARTWORK, variables={"sku": sizeless_sku})
    if results is None:
        logger.error("------------------ No results from GraphQL GET_DEFAULT_ARTWORK")

    if results is None or results.get("data", {}).get("style") is None:
        logger.info(f"    No style found for {sizeless_sku}, trying by style id...")
        style_id = get_style_record_id(fc)
        if style_id:
            results = gql.query(GET_DEFAULT_ARTWORK, variables={"id": style_id})
        else:
            return None

    return results["data"]["style"]["artworkFile"]["id"]


@retry(wait=wait, stop=stop)
def get_default_artwork_placements(fc, sizeless_sku):
    style_piece_mapping = get_style_piece_mapping(fc)
    piece_code_by_id = {
        pm["bodyPiece"]["id"]: pm["bodyPiece"]["code"] for pm in style_piece_mapping
    }

    default_artwork_id = get_default_artwork_id(fc)
    if not default_artwork_id:
        raise Exception(f"No default artwork found for {sizeless_sku}")

    assets = []
    for piece_code in piece_code_by_id.values():
        assets.append(
            {
                "piece_key": piece_code,
                "artworks": [
                    {
                        "tiled": True,
                        "horizontalFraction": 0.5,
                        "verticalFraction": 0.5,
                        "artworkId": default_artwork_id,
                        "pieceName": piece_code,
                    }
                ],
            }
        )

    return assets


@retry(wait=wait, stop=stop)
def get_placements_from_airtable(fc, sizeless_sku):
    # hasura = fc.connectors["hasura"] # create-one schema stitching is broken
    airtable = fc.connectors["airtable"]

    style_piece_mapping = get_style_piece_mapping(fc)
    piece_code_by_id = {
        pm["bodyPiece"]["id"]: pm["bodyPiece"]["code"] for pm in style_piece_mapping
    }

    styles_table = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
    filters = f"""AND(SEARCH("{sizeless_sku}",{{Resonance_code}}))"""
    fields = ["Resonance_code", "Style Pieces", "Print Type"]
    df = styles_table.to_dataframe(filters=filters, fields=fields)

    if len(df) != 1:
        raise Exception(f"Expected 1 AirTable style for {sizeless_sku}, got {len(df)}")

    # looks like this
    #   "style_pieces": [
    #     {
    #       "body_piece_id": "rectguoEEtjhTUsP8",
    #       "material_code": "default",
    #       "artwork_id": "recUoPpny0ql2octW"
    #     },
    if "Style Pieces" not in df.columns:
        # if it's a directional try to get the default artwork
        if df["Print Type"].iloc[0] == "Directional":
            try:
                return get_default_artwork_placements(fc, sizeless_sku)
            except:
                pass  # if not raise original error

        raise Exception(f"No 'Style Pieces' found in AirTable style for {sizeless_sku}")

    style_pieces_json = df["Style Pieces"].iloc[0]
    style_pieces = json.loads(style_pieces_json)["style_pieces"]

    assets = []
    for style_piece in style_pieces:
        piece_key = piece_code_by_id[style_piece["body_piece_id"]]
        assets.append(
            {
                "piece_key": piece_key,
                "artworks": [
                    {
                        "tiled": True,
                        "horizontalFraction": 0.5,
                        "verticalFraction": 0.5,
                        "artworkId": style_piece["artwork_id"],
                        "pieceName": piece_key,
                    }
                ],
            }
        )

    return assets


def get_placements_from_graph_api(fc):
    style_piece_mapping = get_style_piece_mapping(fc)

    # looks like this
    # [
    #     {
    #       "artwork": {"id": "65eb5c9bac026d0008ec856f"},
    #       "bodyPiece": {"code": "SWSBNFAC-S"},
    #       "material": {"code": "CFT97"}
    #     },
    #     {
    #       "artwork": None,                        # None means no artwork, transparent
    #       "bodyPiece": {"code": "SWSBKPNL-S"},
    #       "material": {"code": "CFT97"}
    #     },
    #     ...
    # ]

    assets = []
    for style_piece in style_piece_mapping:
        assets.append(
            {
                "piece_key": style_piece["bodyPiece"]["code"],
                "artworks": [
                    {
                        "tiled": True,
                        "horizontalFraction": 0.5,
                        "verticalFraction": 0.5,
                        "artworkId": (style_piece.get("artwork", {}) or {}).get("id"),
                        "pieceName": style_piece["bodyPiece"]["code"],
                    }
                ],
            }
        )

    return assets


@retry(wait=wait, stop=stop)
def get_assets_from_design_api(fc, sizeless_sku):
    """
    Retrieves a design object from meta_designs which contains a placements object that
    contains a mapping of artworks for each size of each piece. The bits that are relevant
    to us look like this:

    {
        "placements": {
            "TSTBKPNL-S": {
                "0ZZXX": [
                    {
                        "scale": null,
                        "tiled": true,
                        "zIndex": 0,
                        "radians": 0,
                        "artworkId": "c13602f6-8a3f-3fdf-12cb-448b3bd9eab4",
                        "isDefault": false,
                        "pieceName": "TSTBKPNL-S",
                        "verticalFraction": -0.12118902030413856,
                        "horizontalFraction": 0.3777197386500009
                    },
                    { ... }
                ],
                "1ZZXS": [
                    ...
                ],
                ...
            },
            "TSTBNBDG-S": {
                ...
            },
            ...
        }
    }

    In general the jobs are split out per piece so we'll get these into an asset list
    for the handler to process.
    """
    design_id = fc.args.get("design_id")

    hasura = fc.connectors["hasura"]
    design_record = hasura.execute_with_kwargs(GET_DESIGN, id=design_id)
    if (
        not design_record
        or not design_record["meta_designs"]
        or not design_record["meta_designs"][0]
    ):
        raise Exception(f"No design found for {design_id}")

    design = design_record["meta_designs"][0]["design"]

    # make sure we generate images for all the pieces, even if no artworks placed
    piece_names = [
        k["piece_key"] for k in design_record["meta_designs"][0]["body"]["body_pieces"]
    ]
    piece_names = [remove_body_code_and_version_from_piece_name(p) for p in piece_names]
    piece_names = [p for p in piece_names if p.split("-")[-1] in PRINTABLE_PIECE_TYPES]

    assets_by_piece_name = {**{k: {} for k in set(piece_names)}, **design["placements"]}

    assets = []
    for piece_key, placements_by_size in assets_by_piece_name.items():
        assets.append(
            {
                "piece_key": piece_key,
                "design_id": design_id,  # placements_by_size is hefty so each handler can get it
                # "placements_by_size": placements_by_size,
            }
        )

    return fc.asset_list_to_payload(assets)


def mask_piece(piece):
    coords = list(piece.coords)
    size = int_shape_size(piece)
    mask = Image.new("L", size, color=0)
    draw = ImageDraw.Draw(mask)
    draw.polygon(coords, fill="white", outline="white")

    return mask


# np directional tiling, no rotation etc. but fast
def tile_artwork_np(img, mask):
    arr = np.array(img)
    width_diff = mask.size[1] - arr.shape[0]
    height_diff = mask.size[0] - arr.shape[1]
    padding = np.array(
        [[0, np.max([0, width_diff])], [0, np.max([0, height_diff])], [0, 0]]
    )

    if padding.sum() > 0:
        arr = np.pad(arr, padding, "wrap")

    if width_diff < 0 or height_diff < 0:
        arr = arr[: mask.size[1], : mask.size[0], :]

    wrapped = Image.fromarray(arr)
    wrapped.putalpha(mask)
    return wrapped


def tile_artwork(x, y, rotated_image, degrees, original_img, piece_image):
    piece_width, piece_height = piece_image.size
    img_width, img_height = original_img.size
    rotated_img_width, rotated_img_height = rotated_image.size

    piece_centre = Point(piece_width / 2, piece_height / 2)
    img_centre = Point(x, y)

    # we're going to make a square that fits the piece no matter the rotation
    diagonal_len = math.sqrt(piece_width**2 + piece_height**2)

    # we will draw the image from the bottom left corner of this square
    top_left_diagonal = Point(
        piece_centre.x - diagonal_len / 2,
        piece_centre.y - diagonal_len / 2,
    )

    # making sure that it will line up with the start tile position
    art_centre_rotated = rotate(img_centre, degrees, origin=piece_centre)
    offx = (art_centre_rotated.x - top_left_diagonal.x) % img_width - img_width / 2
    offy = (art_centre_rotated.y - top_left_diagonal.y) % img_height - img_height / 2

    tiles_across = math.ceil(diagonal_len / img_width)
    tiles_down = math.ceil(diagonal_len / img_height)
    for i in range(-1, tiles_across + 1):
        for j in range(-1, tiles_down + 1):
            # get the image coords if it were drawn unrotated in the diagonal square
            centre_tile_point = Point(
                top_left_diagonal.x + i * img_width + offx + img_width / 2,
                top_left_diagonal.y + j * img_height + offy + img_height / 2,
            )
            # rotate the coords to match the start tile position
            centre_tile_point = rotate(centre_tile_point, -degrees, origin=piece_centre)

            piece_image.paste(
                rotated_image,
                (
                    int(centre_tile_point.x - rotated_img_width / 2),
                    int(centre_tile_point.y - rotated_img_height / 2),
                ),
                mask=rotated_image,
            )


def shape_from(geojson):
    if geojson is None or geojson == {}:
        return None
    return shape(geojson)


class BodyVars:
    def __init__(self, fc):
        body_code = fc.args.get("body_code")
        version = fc.args.get("body_version")

        self.body_key = f"{body_code}-V{version}"
        self.body_code_upper = body_code.upper()
        self.body_dir_lower = self.body_code_upper.replace("-", "_").lower()
        self.version = version


@retry(wait=wait, stop=stop)
def get_sizes(fc):
    hasura = fc.connectors["hasura"]
    bv = BodyVars(fc)

    def get_all_sizes():
        logger.info("    Getting all sizes for body")
        results = hasura.execute_with_kwargs(
            GET_BODY_SIZES, body_code=bv.body_code_upper, version=bv.version
        )
        results = results["meta_bodies"][0]["body_pieces"]

        return [r["size_code"] for r in results]

    def get_base_size():
        logger.info("    Getting base size for body")
        vs_size = hasura.execute_with_kwargs(
            GET_BODY_METADATA, body_code=bv.body_code_upper
        )
        vs_size = vs_size["meta_bodies"][0]["metadata"]["base_size_name"]

        # base size is direct from VStitcher so need to convert to 3ZZMD
        size = get_alias_to_size_lookup([vs_size])[vs_size]
        return size

    sizes = fc.args.get("sizes", ["all"])
    if ["base"] == sizes:
        sizes = [get_base_size()]
    elif "all" in sizes:
        sizes = get_all_sizes()

    logger.info(f"    Sizes: {sizes}")
    return sizes


def int_shape_size(shape):
    minx, miny, maxx, maxy = shape.bounds
    return int(maxx - minx) + 1, int(maxy - miny) + 1


def add_body_code_and_version_to_piece_name(body_code_upper, version, piece_name):
    if piece_name.startswith(body_code_upper):
        return piece_name
    return f"{body_code_upper}-V{version}-{piece_name}"


def remove_body_code_and_version_from_piece_name(piece_name):
    parts = piece_name.split("-")
    if len(parts) > 3:
        return "-".join(parts[3:])
    return piece_name


def get_material_for_piece(piece_mapping, piece_name):
    piece_name = remove_body_code_and_version_from_piece_name(piece_name)
    for mapping in piece_mapping:
        if mapping["bodyPiece"]["code"] == piece_name:
            return mapping["material"]

    return None


def get_shared_cache(fc):
    return fc.connectors["redis"]["dynamic_style_generator"][fc.key]


class DynamicStyleGenerator:
    def __init__(self, fc, context):
        self.fc = fc
        self.hasura = fc.connectors["hasura"]
        self.s3 = fc.connectors["s3"]
        self.redis = fc.connectors["redis"]
        self.design = None

        logger.info(
            f"    Storing piece paths in ['dynamic_style_generator']['{fc.key}']"
        )
        self.shared_cache = get_shared_cache(fc)

        self.testing = context.get("testing", False)
        self.should_apply_artwork = context.get("apply_artwork", True)

        self.sizeless_sku = fc.args["sizeless_sku"]
        style_lower = self.sizeless_sku.split(" ")[2].lower()

        bv = BodyVars(fc)
        self.bv = bv
        self.sizes = get_sizes(fc)

        self.shared_cache.update_named_map("metadata", "sizes", self.sizes)

        sub_folder = f"{bv.body_dir_lower}/v{bv.version}/{style_lower}".lower()
        base = "s3://meta-one-assets-prod/color_on_shape"
        if self.testing:
            base = "s3://meta-one-assets-prod/dynamic_color_on_shape"
        self.target_dir = f"{base}/{sub_folder}"

    def get_placements_by_size(self, piece):
        if self.design:
            key = remove_body_code_and_version_from_piece_name(piece["piece_key"])
            return self.design.get("placements", {}).get(key, {})
        else:
            return {}

    def cache_design(self):
        design_id = self.fc.args.get("design_id")
        if not design_id:
            return None
        logger.info("    Getting design...")
        design_record = self.hasura.execute_with_kwargs(GET_DESIGN, id=design_id)
        if (
            not design_record
            or not design_record["meta_designs"]
            or not design_record["meta_designs"][0]
        ):
            raise Exception(f"No design found for {design_id}")

        self.design = design_record["meta_designs"][0]["design"]
        body_id = design_record["meta_designs"][0]["body_id"]

        logger.info("    Getting size aliases...")
        size_aliases = self.hasura.execute_with_kwargs(GET_SIZE_ALIAS, id=body_id)
        size_aliases = size_aliases["meta_body_pieces"]
        self.size_alias_mapping = {}
        if size_aliases:
            self.size_alias_mapping = {
                r["size_code"]: r["vs_size_code"] for r in size_aliases
            }

        return self.design

    def generate_piece_images_for_each_size(self):
        # get the piece mapping for this style
        piece_mapping_materials = get_style_piece_mapping(self.fc)
        self.cache_design()
        for piece in self.fc.assets:
            try:
                piece["material"] = get_material_for_piece(
                    piece_mapping_materials, piece["piece_key"]
                )
                piece["piece_key"] = add_body_code_and_version_to_piece_name(
                    self.bv.body_code_upper, self.bv.version, piece["piece_key"]
                )
                piece["placements_by_size"] = piece.get(
                    "placements_by_size", self.get_placements_by_size(piece)
                )

                logger.info(f"***********************")
                logger.info(f"    Generating: {piece['piece_key']}")

                self.generate_print_images(piece)
            except Exception as e:
                msg = traceback.format_exc()
                msg = f"Error generating piece images for {piece.get('piece_key', 'keyless piece')}: {msg}"
                logger.error(msg)
                self.shared_cache.append_to_list(
                    "contracts_failed",
                    {
                        "context": msg,
                        "issueTypeCode": exception_to_issue_type_code(msg),
                    },
                )

    @retry(wait=wait, stop=stop)
    def get_piece_shape(self, piece_key, size, type="outer_geojson"):
        key = f"{piece_key}_{size}"
        db_piece = self.hasura.execute_with_kwargs(GET_BODY_PIECE, key=key)
        db_piece = db_piece["meta_body_pieces"][0]

        return shape_from(db_piece[type]["geometry"])

    @retry(wait=wait, stop=stop)
    def download_artwork(self, id):
        by_id = None
        if re.search(UUID_REGEX, id):
            by_id = self.hasura.execute_with_kwargs(GET_ARTWORK_BY_ID, id=id)
            by_id = by_id if by_id["meta_artworks"] else None

        by_source_id = self.hasura.execute_with_kwargs(
            GET_ARTWORK_BY_SOURCE_ID, source_id=id
        )

        result = (by_id or by_source_id)["meta_artworks"]
        if not result:
            raise Exception(f"Could not find artwork for {id}")
        result = result[0]

        uri = result["dpi_300_uri"]
        name = result.get("name", uri)
        target = f"/tmp/{id}.png"
        if not os.path.exists(target):
            logger.info(f"        Downloading artwork {name}...")
            self.s3._download(uri, "/tmp", f"{id}.png")
        else:
            logger.info(f"        Using cached artwork {name}...")

        return target

    @retry(wait=wait, stop=stop)
    def download_font(self, id):
        result = self.hasura.execute_with_kwargs(GET_FONT_BY_ID, id=id)
        if not result:
            raise Exception(f"Could not find font for {id}")

        font = result["meta_fonts"][0]
        uri = font["uri"]
        name = font.get("name", uri)
        ext = uri.split(".")[-1] if "." in uri else "ttf"
        target = f"/tmp/{id}.{ext}"
        if not os.path.exists(target):
            logger.info(f"        Downloading font {name}...")
            self.s3._download(uri, "/tmp", f"{id}.{ext}")
        else:
            logger.info(f"        Using cached font {name}...")

        return target

    # TODO: will we get material stuff in payload or look it up
    # or handle both cases?
    def check_cuttable_width(self, piece_shape, piece, size):
        piece_width_pixels = int_shape_size(piece_shape)[0]
        piece_key = piece["piece_key"]
        material = piece["material"]

        if material is None:
            # if self.testing:
            #     return {}
            return {
                "context": f"Piece {piece_key} has no material",
                "issueTypeCode": PIECE_MATERIAL_MAPPING,
            }

        code = material.get("code")
        cuttable_width = material.get("cuttableWidth") or 0
        compensation_width_percent = material.get("printFileCompensationWidth") or 0
        # I think there's no need for this as piece will be buffered already?
        # offset_size_inches = material.get("offsetSizeInches")

        piece_width_in_inches = piece_width_pixels / DPI
        compensated_piece_width = (
            piece_width_in_inches * compensation_width_percent / 100
        )

        if compensated_piece_width >= cuttable_width:
            return {
                "context": f"Piece {piece_key} for {size} has a width of {compensated_piece_width} exceeding {code} usable width of {cuttable_width}",
                "issueTypeCode": PIECE_FITS_CUTTABLE_WIDTH,
            }

    def replace_variables(self, text, variable_values):  #: PlaceholderValues):
        """
        Replace variables in a string with their values
        """
        for variable, value in variable_values.dict().items():
            text = text.replace(f"{{{variable}}}", value)

        return text

    def apply_placements(
        self, piece, piece_shape, size, offset_x, offset_y, inner_piece_shape
    ):
        piece_key = piece["piece_key"]
        placements = piece.get("placements_by_size", {}).get(size)
        placements = placements or piece.get("artworks") or []

        default_artwork_id = get_default_artwork_id(self.fc)

        mask = mask_piece(piece_shape)

        piece_width, piece_height = int_shape_size(piece_shape)
        piece_image = Image.new("RGBA", (piece_width, piece_height), (255, 255, 255, 0))

        # a piece will always need some sort of line around the edge for masking etc.
        coords = list(piece_shape.coords)
        draw = ImageDraw.Draw(piece_image)
        draw.polygon(coords, fill="white")

        inner_piece_width, inner_piece_height = int_shape_size(inner_piece_shape)

        # sort artworks by zIndex
        placements = sorted(placements, key=lambda a: a.get("zIndex", 0))
        for placement in placements:
            logger.info(f"        Applying placement {placement}...")

            horizontal_fraction = placement["horizontalFraction"]
            vertical_fraction = placement["verticalFraction"]

            # get its centre position, the fractions are based on the VStitcher sized texture
            # (I used to compensate for the ~15px border around the edge of the piece thanks to
            # VS but I will assume that the client has already compensated for this)
            width = inner_piece_width
            height = inner_piece_height
            x = int(offset_x + horizontal_fraction * width)
            y = int(offset_y + vertical_fraction * height)

            # get the image
            artwork_id = placement.get("artworkId")
            if (artwork_id or "").lower() == "default":
                if not default_artwork_id:
                    logger.warn(f"        No default artwork found for {piece_key}")
                    continue
                # no guarantees with this one, no idea if 300dpi
                # or if it will open/it's an ai file etc...
                logger.info(
                    f"        Using default artwork for {piece_key} as id is '{artwork_id}'..."
                )
                artwork_id = default_artwork_id

            if artwork_id:
                logger.info(f"            Applying artwork {placement}...")
                # need to convert to RGBA so we don't get transparency mask issues
                img = Image.open(self.download_artwork(artwork_id)).convert("RGBA")

                # scale it
                scale = placement.get("scale", 1)
                scale = 1 if scale is None else scale
                img_width, img_height = tuple(int(s * scale) for s in img.size)
                img = img.resize((img_width, img_height))

                # rotate it
                degrees = placement.get("radians", 0) * 180 / np.pi
                center = (img_width / 2, img_height / 2)
                rotated_img = img.rotate(degrees, expand=True, center=center)
                rotated_img_width, rotated_img_height = rotated_img.size

                if placement["tiled"]:
                    tile_artwork(x, y, rotated_img, degrees, img, piece_image)
                else:
                    # place it, x and y are the centre so adjust...
                    x -= int(rotated_img_width / 2)
                    y -= int(rotated_img_height / 2)

                    piece_image.paste(rotated_img, (x, y), mask=rotated_img)

                img.close()

            if placement.get("textBox"):
                logger.info(f"            Applying text box {placement['textBox']}...")

                # scale it first so text is sharp
                scale = placement.get("scale", 1)
                scale = 1 if scale is None else scale

                box_width = int(placement["textBox"]["widthInches"] * DPI * scale)
                box_height = int(placement["textBox"]["heightInches"] * DPI * scale)

                # make it a little bigger to handle a g or something going off the bottom
                buffer = int(DPI * scale)
                box_width += int(buffer * 2)
                box_height += int(buffer * 2)

                # need to convert to RGBA so we don't get transparency mask issues
                a = 0  # 100 if self.testing else 0
                img = Image.new("RGBA", (box_width, box_height), (255, 255, 255, a))

                # load the font
                font_path = self.download_font(placement["textBox"]["fontId"])
                text_height = int(placement["textBox"]["textSizeInches"] * DPI * scale)

                font = ImageFont.truetype(font_path, text_height)
                text = placement["textBox"]["text"]
                # variable_values = PlaceholderValues(
                #     size_code=size,
                #     one_number="123456789",
                #     size_alias=self.size_alias_mapping.get(size),
                # )
                variable_values = {
                    "size_code": size,
                    "one_number": "123456789",
                    "size_alias": self.size_alias_mapping.get(size),
                }
                text = self.replace_variables(text, variable_values)
                rgba = placement["textBox"]["rgba"]  # "rgba(0, 0, 0, 1)"
                open_bracket = rgba.find("(")
                close_bracket = rgba.find(")")
                rgba = tuple(
                    map(int, rgba[open_bracket + 1 : close_bracket].split(","))
                )

                draw = ImageDraw.Draw(img)

                # alignment must match the algo on the front end
                lines = []
                current_line = ""
                for word in text.split(" "):
                    test_line = (
                        word if len(current_line) == 0 else f"{current_line} {word}"
                    )
                    test_width = draw.textlength(test_line, font=font)
                    if test_width > box_width - 2 * buffer:
                        lines.append(current_line)
                        current_line = word
                    else:
                        current_line = test_line
                lines.append(current_line)

                (text_x, text_y) = (buffer, buffer)
                horizontal_align = placement["textBox"]["horizontalAlign"]
                vertical_align = placement["textBox"]["verticalAlign"]
                for i, line in enumerate(lines):
                    text_width = draw.textlength(line, font=font)

                    if horizontal_align == "center":
                        text_x = (box_width - text_width) / 2
                    elif horizontal_align == "right":
                        text_x = box_width - text_width - buffer
                    elif horizontal_align == "left":
                        text_x = buffer

                    if vertical_align == "middle":
                        text_y = (box_height - text_height) / 2 + (
                            0.5 + i - len(lines) / 2
                        ) * text_height
                    elif vertical_align == "bottom":
                        text_y = box_height - (len(lines) - i) * text_height - buffer
                    elif vertical_align == "top":
                        text_y = i * text_height + buffer

                    draw.text((text_x, text_y), line, font=font, fill=rgba)

                # if self.testing:
                #     draw.rectangle(
                #         [buffer, buffer, box_width - buffer, box_height - buffer],
                #     )

                # rotate it
                degrees = placement.get("radians", 0) * 180 / np.pi
                center = (box_width / 2, box_height / 2)
                rotated_img = img.rotate(degrees, expand=True, center=center)
                rotated_img_width, rotated_img_height = rotated_img.size

                # place it, x and y are the centre so adjust...
                x -= int(rotated_img_width / 2)
                y -= int(rotated_img_height / 2)

                piece_image.paste(rotated_img, (x, y), mask=rotated_img)

                img.close()

        # piece_image.putalpha(mask) # this will override the alpha channel of the piece
        piece_image = ImageChops.composite(
            piece_image, Image.new("RGBA", piece_image.size, (255, 255, 255, 0)), mask
        )
        mask.close()

        return piece_image

    def generate_print_images(self, piece):
        piece_key = piece["piece_key"]
        for size in self.sizes:
            contracts_failed = []
            logger.info(f"    Generating {piece_key} for {size}...")
            outer_piece_shape = self.get_piece_shape(piece_key, size, "outer_geojson")
            inner_piece_shape = self.get_piece_shape(piece_key, size, "inner_geojson")

            dest_dir = f"{self.target_dir}/{size.lower()}/pieces"
            dest_path = f"{dest_dir}/{piece_key}.png"

            # NOTE: any placement/transformation stuff on buffered pieces will
            # not be guaranteed accurate (for now at least)
            offset_x = inner_piece_shape.bounds[0] - outer_piece_shape.bounds[0]
            offset_y = inner_piece_shape.bounds[1] - outer_piece_shape.bounds[1]
            shift_x = shift_y = 0
            pixels_buffer = int((piece.get("offset_size_inches", 0) or 0) * DPI)
            if pixels_buffer > 0:
                outer_piece_shape = outer_piece_shape.buffer(pixels_buffer).exterior
                [shift_x, shift_y] = outer_piece_shape.bounds[:2]
                # shift it over so it's still centred
                outer_piece_shape = translate(outer_piece_shape, -shift_x, -shift_y)
                inner_piece_shape = translate(inner_piece_shape, -shift_x, -shift_y)
                offset_x -= shift_x
                offset_y -= shift_y

                dest_path = f"{dest_dir}/{pixels_buffer}/{piece_key}.png"

            failed_contract = self.check_cuttable_width(outer_piece_shape, piece, size)
            if failed_contract:
                # TODO: better context than this
                msg = f"Piece {piece_key} is not within cuttable width for {size}"
                logger.warn(failed_contract.get("context", msg))
                contracts_failed.append(failed_contract)

            if self.should_apply_artwork:
                logger.info(f"    Generating {dest_path}...")
                piece_image = self.apply_placements(
                    piece,
                    outer_piece_shape,
                    size,
                    offset_x,
                    offset_y,
                    inner_piece_shape,
                    # original_piece_shape,
                )
            else:
                logger.info(f"    Not applying artworks/generating {dest_path}...")
                piece_image = None

            # piece["artworks"].get("artworkId")artwork["defaultArtworkUri"]
            # if there's only one artwork and it's the default, we will set the unplaced
            # key so it can be used to groupby for block buffering if necessary
            unplaced_key = None
            artworks = piece.get("artworks", [])
            if len(artworks) == 1 and not artworks[0].get("artworkId"):
                unplaced_key = artworks[0]["defaultArtworkUri"]

            self.shared_cache.update_named_map(
                size,
                piece_key,
                {
                    "uri": dest_path,
                    "contracts_failed": contracts_failed,
                    "unplaced_key": unplaced_key,
                },
            )
            # logger.info(f"    Updated {size} {piece_key} in shared cache")

            if not self.testing:
                logger.info(f"    Uploading {dest_path}...")
                self.s3.write(dest_path, piece_image, dpi=(300, 300))
            elif piece_image is not None:
                self._debug_show_image(piece_image, piece_key, size, -shift_x, -shift_y)

            if piece_image is not None:
                piece_image.close()

    def _debug_show_image(self, piece_image, piece_key, size, offset_x, offset_y):
        inner_piece = self.get_piece_shape(piece_key, size, "inner_geojson")
        inner_piece = translate(inner_piece, offset_x, offset_y)
        coords = list(inner_piece.coords)
        draw = ImageDraw.Draw(piece_image)
        draw.line(coords, fill="black", width=5)

        from IPython.display import display

        smaller = piece_image.resize(tuple(s // 2 for s in piece_image.size))
        display(smaller)
        smaller.close()


def exception_to_issue_type_code(exception):
    if "No 'Style Pieces' found in AirTable style" in str(exception):
        return STYLE_PIECES_EXIST
    if "Expected 1 AirTable style" in str(exception):
        return SINGLE_MATCHING_STYLE
    if "Could not find artwork" in str(exception):
        return META_ARTWORK_EXISTS
    return "UNKNOWN_ERROR"


def generator(event, context={}):
    with FlowContext(event, context) as fc:
        update_apply_color_request_status(fc, "In Progress")

        shared_cache = get_shared_cache(fc)
        shared_cache.put("metadata", {})
        shared_cache.put("contracts_failed", [])

        try:
            # I expect the assets to be a list of all the pieces, but if some are missing I
            # can construct them from sku info
            # this assumes the piece metadata for one size is the same for all
            sizes = get_sizes(fc)
            bv = BodyVars(fc)

            sizeless_sku = fc.args["sizeless_sku"]
            sku = f"{sizeless_sku} {sizes[0]}"

            # if a design id is provided, hasura should have all the specific placements
            # per piece per size for us, if not it's some sort of default placement
            design_id = fc.args.get("design_id")
            if design_id:
                logger.info(
                    f"    Getting placements from Design API for {design_id}..."
                )
                return get_assets_from_design_api(fc, sizeless_sku)

            # get the data we will ultimately populate, it will have things like offset
            # and default artwork etc.
            payload = MetaOneNode.get_style_as_request(
                sku, body_version=bv.version, style_id=get_style_record_id(fc)
            ).dict()

            event_assets = event.get("assets", [])
            # if there's no passed in artwork placements to go on, try to get info from
            # hasura desgins or AirTable Styles->Style Pieces
            if not event_assets:
                if fc.args.get("use_default_artwork"):
                    logger.info("    No placements provided, using default artwork")
                    event_assets = get_default_artwork_placements(fc, sizeless_sku)
                else:
                    logger.info("    No placements provided, getting from AirTable")
                    event_assets = get_placements_from_airtable(fc, sizeless_sku)

            # when there are blank artworks in graphapi we can use this
            # event_assets = get_placements_from_graph_api(fc)

            assets = []
            assets_by_key = {
                add_body_code_and_version_to_piece_name(
                    bv.body_code_upper, bv.version, a["piece_key"]
                ): a
                for a in event_assets
            }

            for p in payload["pieces"]:
                if p["key"] in assets_by_key:
                    logger.info(f"placements provided for piece {p['key']}")
                    asset = assets_by_key[p["key"]]
                    asset["piece_key"] = p["key"]
                    for artwork in asset.get("artworks", []):
                        artwork["pieceName"] = p["key"]

                    asset["offset_size_inches"] = p["offset_size_inches"]
                else:
                    logger.info(
                        f"no placements for piece {p['key']}, will be white with no artwork"
                    )
                    asset = {
                        "piece_key": p["key"],
                        "artworks": [
                            {
                                "tiled": True,
                                "horizontalFraction": 0.5,
                                "verticalFraction": 0.5,
                                "defaultArtworkUri": p["artwork_uri"],
                                "pieceName": p["key"],
                            }
                        ],
                        "offset_size_inches": p["offset_size_inches"],
                    }
                assets.append(asset)

            return fc.asset_list_to_payload(assets)
        except Exception as e:
            msg = traceback.format_exc()
            msg = f"Error generating {fc.args['sizeless_sku']}: {msg}"
            logger.error(msg)
            shared_cache.append_to_list(
                "contracts_failed",
                {"context": msg, "issueTypeCode": exception_to_issue_type_code(msg)},
            )
            update_apply_color_request_status(fc, "Failed")


@res.flows.flow_node_attributes(
    memory="16Gi",
)
def handler(event, context):
    """
    {
        "apiVersion": "v0",
        "kind": "resFlow",
        "metadata": {"name": "dxa.apply_dynamic_color", "version": "dev"},
        "args": {
            "sizeless_sku": "TT-3072 CTNBA LMONMR",
            "sizes": ["all"], # or ["base"] or ["3ZZMD", "2ZZSM", etc.],
            "body_code": "TT-3072",
            "body_version": 1,
            "publish_kafka": true,
            "use_default_artwork": false,
            "apply_color_request_record_id": "rec..."
        },
        "assets": [
            {
                "piece_key": "TT-3072-V1-TSTFTPNL-S", # or "TSTFTPNL-S"
                "artworks": [
                    {
                        "id": "5e4f74da-2071-aaf5-e9d7-01029f0fd202",
                        "tiled": "true",
                        "verticalFraction": 0,
                        "horizontalFraction": 0,
                        "scale": 1,
                        "radians": 0
                    },
                    {...}
                ]
                // OR
                "placements_by_size": {
                    "3ZZMD": [
                        {
                            "id": "5e4f74da-2071-aaf5-e9d7-01029f0fd202",
                            "tiled": "true",
                            "verticalFraction": 0,
                            "horizontalFraction": 0,
                            "scale": 1,
                            "radians": 0
                        },
                        { ...}
                    ],
                    "2ZZSM": [
                        ...
                    ]
                }
            },
        ],
    }
    """

    with FlowContext(event, context) as fc:
        try:
            logger.info(f"***********************")
            logger.info(f"   Generating pieces for: {fc.args['sizeless_sku']}")

            # as this is generating images etc., it's populating a redis object for reducing
            DynamicStyleGenerator(fc, context).generate_piece_images_for_each_size()

            logger.info("    Done")
        except:
            msg = traceback.format_exc()
            msg = f"Error handling {fc.args['sizeless_sku']}: {msg}"
            logger.error(msg)
            get_shared_cache(fc).append_to_list(
                "contracts_failed",
                {
                    "context": msg,
                    "issueTypeCode": exception_to_issue_type_code(msg),
                },
            )
            update_apply_color_request_status(fc, "Failed")


def reducer(event, context={}):
    try:
        with res.flows.FlowContext(event, context) as fc:
            sizeless_sku = fc.args["sizeless_sku"]
            record_id = fc.args.get("apply_color_request_record_id")
            bv = BodyVars(fc)
            shared_cache = get_shared_cache(fc)

            # for each size
            sizes = shared_cache["metadata"].get("sizes", [])
            sizes_contracts_failed = []
            for size in sizes:
                piece_dict = shared_cache[size] or {}
                # logger.info(f" **** shared_cache[{size}]")
                # logger.info(piece_dict)
                sku = f"{sizeless_sku} {size}"

                # get a prepopulated payload that just needs image uris and contracts failed
                payload = MetaOneNode.get_style_as_request(
                    sku, body_version=bv.version
                ).dict()

                # populate the image uris we have generated, and add any contracts failed
                size_contracts_failed = []
                for p in payload["pieces"]:
                    if p["key"] in piece_dict:
                        p["base_image_uri"] = piece_dict[p["key"]]["uri"]
                        p["unplaced_key"] = piece_dict[p["key"]].get("unplaced_key")

                        # if that piece has contracts failed, add them to the payload
                        size_contracts_failed += piece_dict[p["key"]][
                            "contracts_failed"
                        ]
                    else:
                        msg = f"Missing uri for piece {p['key']} in size {size}"
                        logger.warning(msg)
                        contract_failed = {
                            "context": msg,
                            "issueTypeCode": ALL_COLOR_PIECES,
                        }
                        size_contracts_failed.append(contract_failed)

                payload["contracts_failing"] = list(
                    set([c["issueTypeCode"] for c in size_contracts_failed])
                )
                sizes_contracts_failed += size_contracts_failed

                if record_id:
                    payload["id"] = record_id

                logger.info("Payload:")
                logger.info(payload)

                if fc.args.get("publish_kafka", True):
                    logger.info(f"Publishing payload for {size}")
                    fc.connectors["kafka"][
                        "res_meta.dxa.style_pieces_update_requests"
                    ].publish(payload, use_kgateway=True)

            contracts_failed = shared_cache["contracts_failed"] + sizes_contracts_failed
            if contracts_failed:
                logger.warn(f"Contracts failed: {contracts_failed}")

            if record_id:
                if contracts_failed:
                    logger.warn(f"Flagging {record_id}")
                    flag_apply_color_request(record_id, contracts_failed)
                    update_apply_color_request_status(fc, "Failed")
                else:
                    logger.info(f"No contracts failed, unflagging {record_id}")
                    unflag_apply_color_request(record_id)
                    update_apply_color_request_status(fc, "Done")
    except:
        msg = traceback.format_exc()
        msg = f"Error reducing {event['args']['sizeless_sku']}: {msg}"
        logger.error(msg)
        update_apply_color_request_status(fc, "Failed")
        if record_id:
            flag_apply_color_request(
                record_id,
                [{"context": msg, "issueTypeCode": UNKNOWN_ERROR}],
            )

    return {}
