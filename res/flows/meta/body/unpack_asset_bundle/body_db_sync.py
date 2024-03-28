import res
import traceback
import os
from shapely.affinity import translate
from shapely.geometry import MultiPoint
import pandas as pd
import numpy as np
from res.media.images.geometry import to_geojson, Point
from res.media.images.providers.dxf import DxfFile
from res.media.images.providers.vstitcher_pieces import VstitcherPieces
from res.utils import logger
from res.utils.meta.sizes.sizes import get_alias_to_size_lookup
from res.flows.dxa.styles.helpers import (
    _body_id_from_body_code_and_version,
    _body_piece_id_from_body_id_key_size,
)
from res.flows.meta.pieces import VALID_PIECE_TYPES
from tenacity import retry, wait_fixed, stop_after_attempt

s3 = res.connectors.load("s3")

# nudge

INSERT_BODY_PIECES = """
    mutation upsert_body_pieces($body: meta_bodies_insert_input!, $body_id: uuid!, $size_code: String!, $piece_ids: [uuid!]!) {
        update_meta_body_pieces(
            where: {
                body_id: {_eq: $body_id}, 
                size_code: {_eq: $size_code}, 
                id: {_nin: $piece_ids},
                deleted_at: {_is_null: true}
            },
            _set: {deleted_at: "now()"}
        ) {
            affected_rows
        }

        update_meta_body_pieces_many(
            updates: {
                where: {
                    id: {_in: $piece_ids}
                },
                _set: {deleted_at: null}
            }
        ) {
            affected_rows
        }

        insert_meta_bodies(
            on_conflict: {constraint: bodies_pkey, update_columns: [updated_at, metadata, model_3d_uri, point_cloud_uri, front_image_uri, trim_costs, estimated_sewing_time, body_piece_count_checksum]}, 
            objects: [$body]
        ) {
            returning {
                id
                body_pieces {
                    id
                    key
                }
            }
        }
    }"""

DELETE_ORPHANED_BODY_PIECES = """
    mutation delete_orphaned_body_pieces($body_code: String!, $body_version: numeric!, $excluding_size_codes: [String!]!) {
        update_meta_body_pieces(
            where: {
                body: {
                    body_code: {_eq: $body_code},
                    version: {_eq: $body_version}
                },
                size_code: {_nin: $excluding_size_codes},
                deleted_at: {_is_null: true}
            },
            _set: {deleted_at: "now()"}
        ) {
            affected_rows
        }
    }
"""


def metric(status, subject="ADD_3D", etype="BODY"):
    metric_name = f"data_flow.meta_one.{etype}.{subject}.{status}"
    res.utils.logger.incr(metric_name, 1)


def get_dxf_data(dxf_file, vspieces, size_category, **kwargs):
    dxf = DxfFile(dxf_file)
    cl = dxf.compact_layers

    # only get pieces that are in the size category
    dxf_df = cl[cl["size"] == size_category]

    if len(dxf_df) == 0:
        return None

    # change the df around so for each key we have a row with the outline, edges, and notches
    dxf_df = dxf_df.pivot("key", "layer", "geometry").reset_index()
    if 4 not in dxf_df.columns:
        # rare but possible - create an empty point
        dxf_df[4] = Point()

    dxf_df = dxf_df.rename(columns={1: "outline", 4: "notches"})[
        ["key", "outline", "notches"]
    ]
    # get rid of the size but keep the size category so ...S_P_2X -> ...S_P
    # and ...S_2X -> ...S
    dxf_df["key"] = dxf_df["key"].apply(lambda x: "_".join(x.split("_")[:-1]))

    # do some processing to get corners etc.
    return dxf_df.apply(lambda x: transform_dxf_for_db(x, vspieces), axis=1)


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def delete_orphans(body_code, body_version, excluding_size_codes):
    hasura = res.connectors.load("hasura")
    hasura.execute_with_kwargs(
        DELETE_ORPHANED_BODY_PIECES,
        body_code=body_code,
        body_version=body_version,
        excluding_size_codes=excluding_size_codes,
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def upsert_body_pieces(
    body,
    version,
    size_code,
    pieces,
    body_metadata,
    model_3d_uris,
    trim_costs,
    estimated_sewing_time,
    body_pieces_count,
    execute_query=True,
    copied_from_path=None,
):
    hasura = res.connectors.load("hasura")

    # TK-6121, v3
    parts = body.split("-")
    brand = parts[0]

    # on conflict, update all the columns except for created_at :> body_id added
    # too because we could move things between bodies
    cols = list(set(next(iter(pieces), {}).keys()) - {"created_at"}) + ["body_id"]
    body_id = _body_id_from_body_code_and_version(body, version)

    body = {
        "id": body_id,
        "brand_code": brand,
        "body_code": body,
        "version": version,
        "trim_costs": trim_costs,
        "estimated_sewing_time": estimated_sewing_time,
        "body_piece_count_checksum": body_pieces_count,
        "metadata": {
            "flow": "3d",
            "copied_from_path": copied_from_path,
            **body_metadata,
        },
        "body_pieces": {
            "data": pieces,
            "on_conflict": {
                "constraint": "body_pieces_key_key",
                "update_columns": cols,
            },
        },
        **model_3d_uris,
    }

    if execute_query:
        hasura.execute_with_kwargs(
            INSERT_BODY_PIECES,
            body=body,
            body_id=body_id,
            size_code=size_code,
            piece_ids=[p["id"] for p in pieces],
        )

    return body


def piece_is_suspicious(inner_ring, outer_ring):
    from shapely.geometry import Polygon
    from shapely.affinity import scale, translate
    import math

    inner = Polygon(inner_ring)
    outer = Polygon(outer_ring)

    # centre both
    inner = translate(inner, -inner.centroid.x, -inner.centroid.y)
    outer = translate(outer, -outer.centroid.x, -outer.centroid.y)

    # scale the inner to .999
    inner = scale(inner, 0.999, 0.999)

    overlap = (inner - outer).area / inner.area
    return not math.isclose(overlap, 0, abs_tol=0.001)


def create_piece_data(
    dxf_df,
    piece,
    vs_size,
    size_code,
    body_id,
    contract_failures,
    contract_failure_context,
):
    assert piece.key in list(
        dxf_df["key"]
    ), f"The piece {piece.key} is not in the DXF key list {list(dxf_df['key'])}"
    dxf_piece_df = dxf_df[dxf_df["key"] == piece.key].iloc[0]
    size_code = size_code or vs_size

    # check for and fix possible foldbacks
    piece.rectify_seam_guides_using_dxf_outline(dxf_piece_df["outline"])

    # move everything to the origin using the outline as the bbox
    bounds_for_moving_to_origin = dxf_piece_df["outline"].bounds
    piece.shift_all_geometries_to_origin(bounds_for_moving_to_origin)
    [x, y, *_] = bounds_for_moving_to_origin
    shift = lambda geom: translate(geom, -x, -y)
    for col in ["outline", "edges", "notches", "corners"]:
        value = dxf_piece_df.get(col)
        if value is not None:
            # is it an array of geometries?
            if isinstance(value, list):
                dxf_piece_df[col] = [shift(geom) for geom in value]
            else:
                dxf_piece_df[col] = shift(dxf_piece_df[col])

    corners = piece.corner_reasons or dxf_piece_df["corners"]
    # piece.key is something like ...-S_P_2X
    # key is something like ...-S_1PZZ
    piece_key_no_size = piece.key.split("_")[0]
    key = piece_key_no_size + "_" + size_code

    grain_line_degrees = (
        int(piece.grain_line_degrees) if piece.grain_line_degrees is not None else None
    )

    # if piece_is_suspicious(piece.geometry, dxf_piece_df["outline"]):
    #     contract = "SUSPICIOUS_PIECE_SHAPE"
    #     logger.warn(f"Piece {piece.key} intersects its outline size: {size_code}")
    #     contract_failures.append(contract)
    #     existing = contract_failure_context.get(contract)
    #     context = f"{piece.key} ({size_code})"
    #     sus_shapes = context if existing is None else f"{existing}, {context}"
    #     contract_failure_context[contract] = sus_shapes

    return {
        "id": _body_piece_id_from_body_id_key_size(
            body_id, piece_key_no_size, size_code
        ),
        "key": key,
        "piece_key": piece.key,
        "type": DxfFile.type_from_piece_name(piece_key_no_size),
        "vs_size_code": vs_size,
        "size_code": size_code,
        "outer_geojson": to_geojson(dxf_piece_df["outline"]),  # from DXF
        "inner_geojson": to_geojson(piece.geometry),
        "outer_edges_geojson": to_geojson(dxf_piece_df["edges"]),  # from DXF
        "inner_edges_geojson": to_geojson(piece.edges_df),
        "outer_corners_geojson": to_geojson(corners),
        "outer_notches_geojson": to_geojson(dxf_piece_df["notches"]),  # from DXF
        "seam_guides_geojson": to_geojson(piece.seam_guides),
        "internal_lines_geojson": to_geojson(piece.internal_lines_df),
        "placeholders_geojson": to_geojson(piece.placeholders),
        "pins_geojson": to_geojson(piece.pins),
        "pleats_geojson": to_geojson(piece.pleats),
        "buttons_geojson": to_geojson(piece.buttons),
        "symmetry": piece.symmetry,
        "grain_line_degrees": grain_line_degrees,
        "sew_identity_symbol_old": piece.sew_identity_symbol_id_old,
        "sew_identity_symbol": piece.sew_identity_symbol_id,
    }


def transform_dxf_for_db(row, vspieces, plot=False):
    # we'll use the overall outline for translation after transformation
    key = row["key"]
    piece = vspieces.pieces.get(key)

    outline = vspieces.align_dxf_with_piece(row["outline"], key)

    notches = None
    if row["notches"] is not np.nan:
        notches = vspieces.project_dxf_to_vs_space(row["notches"])

    if plot:
        import geopandas as gpd

        gpd.GeoDataFrame(
            {
                "title": ["orig", "new", "piece"],
                "geometry": [row["outline"], outline, piece.geometry],
            }
        ).plot(column="title", legend=True).get_legend().set_bbox_to_anchor((2, 1))

    # internal = None
    # if row["internal"] is not np.nan:
    #     internal = translate(vspieces.project_dxf_to_vs_space(row["internal"]), *shift)

    def fallback():
        corners = DxfFile.make_corners({**row, "geometry": outline})
        return DxfFile.make_edges(
            {**row, "geometry": outline, "corners": corners}, buffer=11.8
        )

    # try to get the corners from the vstitcher piece or fall back to the calculated corners
    try:
        corners = (
            [c["geometry"] for c in piece.corner_reasons] if piece is not None else []
        )

        # if there are no corners or seam guides, let the dxf calcs have a go
        seam_guides = piece.seam_guides if piece is not None else []
        if len(corners) < 2 and len(seam_guides) == 0:
            edges = fallback()
        else:
            edges = DxfFile.make_edges(
                {"geometry": outline, "corners": MultiPoint(corners)}, buffer=11.8
            )
    except:
        edges = fallback()

    return pd.Series(
        {
            "key": key,
            "outline": outline,
            "notches": notches,
            # "internal": internal,
            "corners": corners,
            "edges": edges,
            "exterior": row["outline"] if piece is None else piece.exterior_geometry,
        }
    )


def get_files_by_size(path):
    ls_result = []
    if path.startswith("s3://"):
        ls_result = s3.ls_info(path)
    else:
        from glob import glob
        import os

        files = list(filter(os.path.isfile, glob(path + "/**/*", recursive=True)))
        ls_result = [{"path": f, "last_modified": os.path.getmtime(f)} for f in files]

    files = [
        f
        for f in ls_result
        if f["path"].endswith("pieces.json") or f["path"].endswith(".dxf")
    ]

    sizes = list(set([f["path"].split("/")[-2] for f in files]))
    size_lookup = get_alias_to_size_lookup(sizes)

    # sort the files by last_modified so any size scale changes are overwritten
    # with the newest e.g. MED and M are both 3ZZMD, pick the newest
    files = sorted(files, key=lambda f: f["last_modified"])

    files_by_size = {}
    for f in files:
        path = f["path"]
        size = path.split("/")[-2]

        sku_size = size_lookup.get(size, size)
        if sku_size not in files_by_size:
            files_by_size[sku_size] = {}

        if path.endswith("pieces.json"):
            files_by_size[sku_size]["pieces"] = path
        elif path.endswith(".dxf"):
            files_by_size[sku_size]["dxf"] = path

    # if any sizes don't have the pieces/dxf pair, remove them
    for size in list(files_by_size.keys()):
        if len(files_by_size[size]) != 2:
            del files_by_size[size]

    return files_by_size, size_lookup


@retry(wait=wait_fixed(3), stop=stop_after_attempt(2), reraise=True)
def get_body_info(body_code, contract_failures):
    """
    added to get other information we want to import into the body
    in this case costing information
    """

    BODY_INFO = """query body($code: String){
            body(number:$code){
            name
            code
            trimCost
            targetSewingTime
            numberOfPieces
            
        }
        }"""
    d = res.connectors.load("graphql").query_with_kwargs(BODY_INFO, code=body_code)[
        "data"
    ]["body"]

    if d.get("trimCost") is None:
        contract_failures.append("MISSING_TRIM_COST")

    if d.get("targetSewingTime") is None:
        contract_failures.append("MISSING_TARGET_SEWING_TIME")

    unfilled = [k for k, v in d.items() if v is None]
    if len(unfilled) > 0:
        return None

    return {
        "body_code": d["code"],
        "name": d["name"],
        "trim_costs": d["trimCost"],
        "body_pieces_count": d["numberOfPieces"],
        "estimated_sewing_minutes": d["targetSewingTime"] / 60.0,
    }


@retry(wait=wait_fixed(3), stop=stop_after_attempt(2), reraise=True)
def get_bom_data_for_body_version(body_code, version, contract_failures):
    from res.flows.meta.ONE.bom import get_body_bom

    def _qualify(k):
        return f"{body_code}-V{version}-{k}"

    bom_data = get_body_bom(body_code)

    if bom_data == None:
        contract_failures.append("MISSING_BOM_DATA")
        return {}

    bom_data = bom_data["piece_fusing"]
    bom_data = {_qualify(k): v["code"] for k, v in bom_data.items()}
    return bom_data


def sync_body_to_db(
    body_code,
    version,
    s3_path_root,
    contract_failures,
    contract_failure_context,
    copy_from_body_code=None,
    copy_from_body_version=None,
    **kwargs,
):
    """

    copy from is used to target the body code version but from a surrogate asset -> the s3 path is expected to be the surrogate one
    """

    def rename_from_copy(s, error_on_fail=False):
        """
        When we copy pieces, we rename the pieces from the old body to the new one IF the body is contained in the piece
        """
        key_old = f"{copy_from_body_code}-V{copy_from_body_version}"
        key_new = f"{body_code}-V{version}"
        if error_on_fail:
            assert (
                key_old in s
            ), f"You are trying to copy from a piece that does not contain the body code which can lead to unexpected behavior - {key_old} not in {s}"

        return s.replace(key_old, key_new)

    try:
        # SA: adding first call to body info so we can get extra info - upstream calls might have more context
        body_info = get_body_info(body_code, contract_failures)
        if not body_info:
            return 0

        testing = kwargs.get("testing", False)

        bom_data = get_bom_data_for_body_version(
            body_code=body_code, version=version, contract_failures=contract_failures
        )

        res.utils.logger.debug(f"BOM={bom_data}")

        model_3d_uris = {
            "model_3d_uri": f"{s3_path_root}/3d.glb",
            "point_cloud_uri": f"{s3_path_root}/point_cloud.json",
            "front_image_uri": f"{s3_path_root}/front.png",
        }
        if s3_path_root.startswith("s3://"):
            if not all([s3.exists(model_3d_uris[k]) for k in model_3d_uris]):
                model_3d_uris = {}
        else:
            if not all([os.path.exists(model_3d_uris[k]) for k in model_3d_uris]):
                model_3d_uris = {}

        sizes_processed = []
        files_by_size, size_lookup = get_files_by_size(s3_path_root)

        # if version is a string
        if isinstance(version, str) and version.startswith("v"):
            version = int(version[1:])

        body_id = _body_id_from_body_code_and_version(body_code, version)

        payloads = {}
        for sku_size in files_by_size:
            # can get the old size from the path
            orig_size = files_by_size[sku_size]["pieces"].split("/")[-2]
            logger.info(
                f"Syncing {body_code} {version} {sku_size}... to body id {body_id}"
            )

            # size category is P-2X or 2X
            category_and_size = orig_size.replace("-", "_")
            category = category_and_size.split("_")[0] if "-" in orig_size else ""

            # there's some odd bug in the techpack export for petite sizes
            # it exports _L where it should be exporting _P_2X for instance
            # for now, if this is a petite, use the corresponding non-petite size
            # which also has the petite size in it... two bugs make a right
            if category:
                non_petite_size = category_and_size.split("_")[1]
                non_petite_sku_size = size_lookup.get(non_petite_size, non_petite_size)
                dxf_file = files_by_size[non_petite_sku_size]["dxf"]
            else:
                dxf_file = files_by_size[sku_size]["dxf"]

            pieces_file = files_by_size[sku_size]["pieces"]

            vsp = VstitcherPieces(pieces_file)
            dxf_df = get_dxf_data(dxf_file, vsp, category_and_size, **kwargs)

            if dxf_df is None:
                logger.error(
                    f"Failed to get dxf data for appropriate size in {dxf_file}"
                )
                continue

            pieces_data = []
            is_in_category = lambda p: "_".join(p.split("_")[1:]) == category
            pieces_in_category = [p for p in vsp.pieces if is_in_category(p)]
            vs_size = orig_size
            size_code = sku_size
            for p in pieces_in_category:
                if p.split("-")[-1] not in VALID_PIECE_TYPES and kwargs.get(
                    "skip_invalid_piece_types"
                ):
                    res.utils.logger.warn(f"Skipping an invalid piece type {p}")
                    continue

                res.utils.logger.info(f"    Adding {p}...")

                piece_data = create_piece_data(
                    dxf_df,
                    vsp.pieces.get(p),
                    vs_size,
                    size_code,
                    body_id,
                    contract_failures,
                    contract_failure_context,
                )
                piece_data["metadata"] = {
                    "dxf_file": dxf_file,
                    "pieces_file": pieces_file,
                    "flow": "3d",
                    "units": "300dpi",
                }
                # add the fusing type to the piece - it may be we find a smarter source for this
                piece_data["fuse_type"] = bom_data.get(piece_data["piece_key"])

                is_base_size = vsp.pieces.get(p).is_base_size
                if is_base_size is not None:
                    piece_data["metadata"]["is_base_size"] = is_base_size
                pieces_data.append(piece_data)

            """
            SA: we could rename body pieces from the surrogate around here or in create_piece_data
            ^ we have already used the body code and version for the target for the body id
            we now need to rename or data from the 'copy-from' values to our true values
            both the key and piece key would need to be rename
            the only reason to do it here is to have only one method/level be aware of the renaming
            """

            if copy_from_body_code:
                res.utils.logger.info(
                    f"renaming pieces from {copy_from_body_code}-V{copy_from_body_version} -> {body_code}-V{version}"
                )

                for p in pieces_data:
                    p["key"] = rename_from_copy(p["key"])
                    p["piece_key"] = rename_from_copy(p["piece_key"])

            payload = upsert_body_pieces(
                body_code,
                version,
                size_code,
                pieces_data,
                vsp.metadata,
                model_3d_uris,
                body_pieces_count=body_info.get("body_pieces_count"),
                trim_costs=body_info.get("trim_costs"),
                estimated_sewing_time=body_info.get("estimated_sewing_minutes"),
                execute_query=not testing,
                copied_from_path=s3_path_root if copy_from_body_code else None,
            )
            sizes_processed.append(size_code)
            payloads[sku_size] = payload
            for p in pieces_data:
                logger.info(
                    f"    Added {p['piece_key']} with id {p['id']} to body {body_id}"
                )

        # logger.info(f"Synced {body_code} {version} to {body_id}...")
        # result = result["insert_meta_bodies"]["returning"][0]
        # id_keys = result["body_pieces"]
        # id_keys.sort(key=lambda x: x["key"])
        # for id_key in id_keys:
        #     logger.info(f"    {id_key['key']} with id {id_key['id']}")

        # soft delete any orphaned pieces with no size
        if not testing:
            delete_orphans(body_code, version, sizes_processed)

        metric("OK")

        return len(sizes_processed) if not testing else payloads
    except Exception as e:
        metric("FAILED")
        logger.warning(
            f"Failed to sync {body_code} {version} {s3_path_root}: {traceback.format_exc()}"
        )
        raise e


def sync_body_pieces_from_piece_data(
    body_code,
    body_version,
    size_code,
    pieces_data_by_size,
    vsp_metadata={},
    model_3d_uris={},
    copy_from_body_code=None,
    body_info={},
):
    """
    a different entry point to save bodies to the database
    in this mode we bypass some of the expectations from v-stitcher sourced pieces
    more generally we might have pieces from a DXF file for example and we want to dump the pieces
    the pieces data can be any compliant geometry set; normally these contain outlines, notches/seam guides, internal lines and corners for example
    there is a minimum spec required to basically sew a garment (i.e. pattern pieces and stamper-type data) but we also have the ability
    to add resonance specific advanced features like dynamic placing

    """

    sizes_processed = pieces_data_by_size.keys()

    for size_code, pieces_data in pieces_data_by_size.items():
        # this inserts a size batch
        res.utils.logger.info(
            f"Inserting body pieces {body_code=}, {body_version=}, {size_code=}, - number of pieces ({len(pieces_data)})"
        )
        payload = upsert_body_pieces(
            body_code,
            body_version,
            size_code,
            pieces_data,
            vsp_metadata,
            model_3d_uris,
            body_pieces_count=body_info.get("body_pieces_count"),
            trim_costs=body_info.get("trim_costs"),
            estimated_sewing_time=body_info.get("estimated_sewing_minutes"),
            copied_from_path=copy_from_body_code,
        )

    res.utils.logger.debug(f"Checking for orphans for all sizes")
    delete_orphans(body_code, body_version, sizes_processed)

    res.utils.logger.info(f"Upserted body pieces for {body_code=}, {body_version=}")


def sync_body_to_db_by_versioned_body(
    body_code, version, copy_from_body_code=None, copy_from_body_version=None, **kwargs
):
    version_str = str(version).lower().replace("v", "")
    body_code_lower = body_code.lower().replace("-", "_")
    # for an aliased mode we can read the data from the 'copy from' asset - later we need to rename some stuff
    path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code_lower}/v{version_str}/extracted"
    res.utils.logger.info(f"searching path {path} for body {body_code} V{version}")
    return sync_body_to_db(
        body_code=body_code,
        version=version,
        contract_failure_context=None,
        contract_failures=None,
        s3_path_root=path,
        copy_from_body_code=copy_from_body_code,
        copy_from_body_version=copy_from_body_version,
        **kwargs,
    )
