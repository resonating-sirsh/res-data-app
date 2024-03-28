"""
NB Hidden aspects of the style contract;
- The style key is the combo of the name and body. The designer cannot change the name so we should keep the "original name" as our key if they want to
- All uuids are generated in the model and supplied to the database 
- There is a trigger on the Styles that generates a key based on the body and style to use as a ranking. The rank is the style number of the styles in the group and effectively a ranking of Color Material based on the styles birth day
  If we start creating styles in our database we use our created date but when importing from another system we take its birthday. 
  We must be very careful here so that the rank becomes fixed no matter what we insert in future 
----------------------------------------------------------------------------------------------------------------
The style model is for 2d and 3d
It is particular useful to full process for 2d directionals but we can do for all
We need to get either a setup file or pieces feather to get geometry data
- In V2 we can add the extended seam info to the database

in this version we support a laxy check and wait to see if the color is placed on the piece - the piece should exist in the abstract but the uri for the png may be blank
- in 2d this is just because its slow for directionals or requires input for placements (we will never know them for 2d placements)
- in 3d we may have them actually in the pieces feather or DXF  and that is fine or we may need to wait for the meta one. better to wait now because there could be version issues
  - the DXF is better except we dont know the materials etc that are validated for the meta one. we could try trusting the pieces names
  - we could try finding the meta one processing if it exists but how do you know its valid
"""

import res
from res.utils.dataframes import expand_column, rename_and_whitelist
import pandas as pd
import json
from res.connectors.airtable import AirtableConnector
from res.utils import uuid_str_from_dict
from res.media.images.providers.dxf import DxfFile
from . import queries
from tenacity import retry, wait_fixed, stop_after_attempt, wait_random_exponential
from ast import literal_eval
from res.media.images.geometry import (
    name_part,
    Polygon,
    detect_castle_notches_from_outline,
)
from res.media.images.outlines import get_piece_outline, name_part
from res.media.images.providers.dxf import DxfFile
from res.flows.dxa.styles.exceptions import *
from res.flows.dxa.setup_files import net_dxf_location_cache

"""
Some meta ones features

"""


def merge_swaps_to_cache(bootstrap_from=None):
    """
    we can load a cached one time from airtable and then from there we can get updates from kafka
    "/Users/sirsh/Downloads/known_swaps.pkl"
    """
    swaps = res.connectors.load("snowflake").execute(
        """  SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_SELL_SWAP_MATERIAL_SELL_SWAP_MATERIAL" """
    )
    swaps["rc"] = swaps["RECORD_CONTENT"].map(json.loads)
    swaps = pd.DataFrame([d for d in swaps["rc"]])
    for col in ["new_sku", "old_sku"]:
        swaps[col] = swaps[col].map(lambda x: f"{x[:2]}-{x[2:]}")
    # example seed from historic
    if bootstrap_from:
        sw = pd.read_pickle(bootstrap_from).rename(
            columns={"alias": "old_sku", "Resonance_code": "new_sku"}
        )
        swaps = pd.concat([swaps, sw])[["old_sku", "new_sku"]].reset_index(drop=True)
    res.connectors.load("s3").write(
        "s3://res-data-production/data-lake/cache/swaps/swaps.parquet", swaps
    )
    res.utils.logger.info(
        "Ok - cached swaps - s3://res-data-production/data-lake/cache/swaps/swaps.parquet - select from duckdb"
    )


def _bootstrap_style(sku, skip_previews=True):
    """
    particularly useful to do everything that is needed to generate a style in the local database
    may take some time to load the body and save and also generate all the previews for the sample size (may create an option to skip previews)
    """
    from res.flows.meta.ONE.style_node import MetaOneNode, ensure_dependencies

    from schemas.pydantic.meta import MetaOneResponse

    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"
    body = sku.split(" ")[0].strip()
    payloads = MetaOneNode.get_style_as_request(sku)
    bv = payloads[0].body_version

    ensure_dependencies(sku, body, bv, force=True)

    for p in payloads:
        print(p.sku)
        node = MetaOneNode(
            p, response_type=MetaOneResponse, queue_context=res.flows.FlowContext({})
        )
        c = node.save_with_relay(
            queue_update_on_save=False, skip_previews=skip_previews
        )


def accounting_size_to_normed_size_lu():
    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    # predicate on sizes that we have
    sizes_lu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
    return dict(sizes_lu[["_accountingsku", "Size Normalized"]].values)


def accounting_size_to_size_chart(size_code):
    """
    temp measure as we move data to the database
    """
    data = res.connectors.load("s3").read(
        "s3://res-data-platform/cache/size_lookup.feather"
    )

    data = dict(data[["_accountingsku", "Size Chart"]].values)

    return data[size_code]


def metric(etype, subject, status):
    """
    - match: "data_flow.*.*.*.*"
        name: "data_flow"
        labels:
        flow: "$1"
        node: "$2"
        asset_type: "$3"
        data_group: "$4"
        status: "$5"
    """

    metric_name = f"data_flow.meta_one.{etype}.{subject}.{status}"
    res.utils.logger.incr(metric_name, 1)


def get_offset_size_map():
    """
    hard code for now - find a DB solution
    """
    return {
        "CT170": 2.0,
        "CTJ95": 2.0,
        "ICTNS": 1.0,
        "CTNSP": 1.0,
        "OCTHR": 1.0,
        "CFTGR": 1.0,
        "CTF19": 1.0,
        "CFT97": 2.0,
        "PIQUE": 2.0,
        "WCTNS": 1.0,
        "RYJ01": 3.0,
        "OCTCJ": 1.5,
        "VISCR": 1.0,
        "CTNPT": 1.0,
        "CTNBA": 1.5,
        "TNSPJ": 1.5,
        "FBTRY": 1.0,
    }


def get_version_component(s):
    """
    simpler helper to look for a version component in the path by convention they are normally /v[integer]/
    """
    parts = s.split("/")
    for p in parts:
        if len(p) and p[0] == "v":
            try:
                return int(float(p.replace("v", "")))
            except:
                pass
    return None


def make_meta_one_from_style_and_color_files(
    row, color_filenames, plan=False, debug=False, **kwargs
):
    """
    check the row contract below. We usually use the style helpers to compile all data into a row that can map to the meta one
    we need color files and some material props in the kwargs
    for buffers, we buffer the incoming DXF body outlines to match to the placed color files
     - keep in mind color pieces for knits actually have piece names embedded so we can test easily with this even if sometimes piece conventions differ on names
    """
    sku = row["sku"]
    piece_map = row["piece_mapping_info"]
    body_code = row["body_code"]
    material_code = row["material_code"]
    color_code = row["color_code"]
    size_code = row["size_code"]
    body_version = row["body_version"]
    artwork_uri = row.get("artwork_uri")
    net_dxf_path = row["filename"]

    s3 = res.connectors.load("s3")
    piece_map = piece_map or {}
    # here we are not supporting multiple offsets per material but that would be easy to do
    offset_size_inches = kwargs.get("offset_size_inches")
    color_version = None
    if len(color_filenames):
        color_version = get_version_component(color_filenames[0])
        if color_version != body_version:
            res.utils.logger.warn(
                f"Version mismatch in color files version {color_version} and body version {body_version}"
            )

    def _get_piece_outlines_from_paths(paths):
        res.utils.logger.info(f"reading files...")
        for p in paths:
            res.utils.logger.debug(f"reading {p}")
            yield p, get_piece_outline(s3.read(p))

    def export_file(path, df):
        NF = df.copy()
        for col in ["outline", "polylines", "geometry", "notches"]:
            NF[col] = NF[col].map(str)
        for col in ["bounds", "original_shape_bounds"]:
            NF[col] = NF[col].map(str)
            path = f"{path}/pieces.feather"
        s3.write(path, NF)
        return path

    res.utils.logger.debug(f"loading body asset {net_dxf_path}")
    _, nf = DxfFile.make_setup_file_svg(net_dxf_path, return_shapes=True)
    named_piece_outlines = dict(nf[["key", "outline"]].values)
    if offset_size_inches:
        offset = offset_size_inches * 300
        res.utils.logger.debug(f"Buffering by offset size inches {offset_size_inches}")
        named_piece_outlines = {
            k: Polygon(g).buffer(offset).exterior
            for k, g in named_piece_outlines.items()
        }

    piece_names_metadata = []
    # use a larger threshold for knits
    dist_threshold = 1000 if not offset_size_inches else 7000

    for idx, t in enumerate(_get_piece_outlines_from_paths(color_filenames)):
        g, uid = t[1], t[0]
        name, confidence = name_part(
            g,
            named_piece_outlines,
            idx=idx,
            min_dist=kwargs.get("min_dist", dist_threshold),
        )

        piece_names_metadata.append(
            {
                "name": str(name),
                "confidence": confidence,
                "uri": uid,
                "uid": uid.split("/")[-1].split(".")[0],
            }
        )

    if len(named_piece_outlines):
        res.utils.logger.warn(f"did not name pieces { named_piece_outlines }")
    else:
        res.utils.logger.info(f"All pieces named!")

    path = f"{body_code.replace('-', '_')}/v{body_version}/{material_code}/{color_code}/{size_code}/pieces.feather".lower()
    path = f"s3://res-data-platform/2d-meta-ones/{path}"
    res.utils.logger.info(f"target path for {sku}: {path}")

    # we save the meta one to the path but we can write back the metadata beside the color files we named
    if not plan and color_filenames:
        cpath = color_filenames[0].split("/")
        cpath = "/".join(cpath[:-1])
        res.utils.logger.info(f"Saving metadata for color file names {cpath}")
        s3.write(f"{cpath}/metadata/piece_names.json", json.dumps(piece_names_metadata))

    df = pd.DataFrame(piece_names_metadata)
    if debug:
        return df

    df = pd.merge(nf, df, left_on="key", right_on="name", how="left")

    df["failed_to_name_color"] = df["key"].map(
        lambda k: named_piece_outlines.get(k) is not None
    )
    # try to get some properties at piece level or fall back to the style level
    df["artwork_uri"] = df["key"].map(
        lambda x: piece_map.get(x, {}).get("artwork_uri", artwork_uri)
    )
    df["material_code"] = df["key"].map(
        lambda x: piece_map.get(x, {}).get("material_code", material_code)
    )
    df["body_version"] = body_version
    df["color_version"] = color_version

    if not plan:
        res.utils.logger.info(f"Saving meta one")
        return export_file(path, df)

    return df


def _named_outlines_from_net_dxf(
    net_dxf_path, offset_size_inches=None, combined_combo_asset=None
):
    """
    a second asset can be passed in - we do this one the assets are split to combo but the style is not i.e. one material
    """
    _, nf = DxfFile.make_setup_file_svg(
        net_dxf_path, return_shapes=True, ensure_valid=True
    )
    if combined_combo_asset is not None:
        _, nf2 = DxfFile.make_setup_file_svg(
            combined_combo_asset, return_shapes=True, ensure_valid=True
        )
        nf = pd.concat([nf, nf2]).reset_index(drop=True)

    named_piece_outlines = dict(nf[["key", "outline"]].values)
    buffered_outlines = named_piece_outlines
    if offset_size_inches:
        offset = offset_size_inches * 300
        res.utils.logger.debug(f"Buffering by offset size inches {offset_size_inches}")
        buffered_outlines = {
            k: Polygon(g).buffer(offset).exterior
            for k, g in named_piece_outlines.items()
        }
    return buffered_outlines, named_piece_outlines


def _from_outlines(d, artwork_uri, rank, piece_map, material_code):
    """
    simpler helper that takes the named pieces e.g. from a NET DXF and makes a meta one from it
    it makes sense for some cases e.g. directionals where we do not need the placed color at all
    """

    def _get_piece_key(s):
        try:
            return "-".join(s.split("-")[3:])
        except:
            return s

    data = []
    for name, g in d.items():
        piece_key = _get_piece_key(name)
        notches = detect_castle_notches_from_outline(g)
        data.append(
            {
                "name": str(name),
                "key": str(name),
                "piece_key": piece_key,
                "confidence": -1,
                "material_code": material_code,
                "uri": None,
                "uid": None,
                "notches": notches,
                "outline": g,
                "rank": rank,
                "artwork_uri": piece_map.get(piece_key, {}).get(
                    "artwork_uri", artwork_uri
                ),
            }
        )
    return data


def process_dxa_asset(a):
    """
    we can have different amounts of information
    if we have wovens, then the outline of the color pieces are trusted but we may not know the name
    if we have a knit and its directional, its better to trust the pieces in the body asset which will not be buffered even if we dont know the name
     but if there are multiple artworks, without all piece names we cannot know the artwork
    """
    s3 = res.connectors.load("s3")

    def _get_piece_key(s):
        try:
            return "-".join(s.split("-")[3:])
        except:
            return s

    # for knits we need to apply the buffer to match names
    offset_size_inches = a.get("offset_size_inches")
    offset_size_inches_combo = a.get("offset_size_inches_combo")
    metadata = a.get("metadata", {})
    piece_map = {}
    if "piece_mapping" in metadata and isinstance(metadata["piece_mapping"], str):
        piece_map = json.loads(metadata["piece_mapping"])

    artwork_uri = a.get("artwork_uri", metadata.get("artwork_uri"))
    # num_artworks = metadata.get("num_artworks")
    material_code = metadata.get("material_code")
    material_code_combo = metadata.get("material_code_combo")
    # NB: the combo material is not known in general; the piece mapping is needed to determine each pieces material
    # typically the combo is in the other material as determined by the piece mapping
    dist_threshold = 1000 if not offset_size_inches else 7000
    offset = 0

    data = []
    idx = 0
    # if there exists a combo body asset but this styles is in a single material, we can check piece counts
    # we may need to merge in the pieces from the combo NET DXF to match the pieces - not too mention complete the style even without pieces
    named_piece_outlines, raw_outlines = (
        _named_outlines_from_net_dxf(
            a["body_asset"],
            offset_size_inches=offset_size_inches,
            combined_combo_asset=metadata.get("added_combo_body_asset"),
        )
        if a.get("body_asset")
        else None
    )
    _store_outlines = dict(raw_outlines)
    if a.get("color_pieces"):
        for (
            idx,
            p,
        ) in enumerate(a["color_pieces"]):
            p = f"s3://meta-one-assets-prod/{p}"
            im = s3.read(p)
            g = get_piece_outline(im)
            name, confidence = name_part(
                g, named_piece_outlines, idx=idx, min_dist=dist_threshold
            )
            # what we do is use the raw outline in these files if we have it and can map the name
            g = raw_outlines.get(name, g)
            piece_key = _get_piece_key(name)
            notches = detect_castle_notches_from_outline(g)

            data.append(
                {
                    "name": str(name),
                    "key": str(name),
                    "piece_key": piece_key,
                    "confidence": confidence,
                    "material_code": piece_map.get(piece_key, {}).get(
                        "material_code", material_code
                    ),
                    "uri": p,
                    "uid": p.split("/")[-1].split(".")[0],
                    "notches": notches,
                    "outline": g,
                    "rank": "Primary",
                    "artwork_uri": piece_map.get(piece_key, {}).get(
                        "artwork_uri", artwork_uri
                    ),
                }
            )
            offset = idx + 1

    # TODO special case where we fallback if we cannot name pieces -> if we have duplicates names we should probably not trust either
    if len(named_piece_outlines) > 0 and a["print_type"] == "Directional":
        res.utils.logger.debug(
            "because we could not name all pieces we cannot trust the mapping to placed color pieces - trusting body asset instead"
        )
        data = _from_outlines(
            _store_outlines, artwork_uri, "Primary", piece_map, material_code
        )

    # now collect the combo data if it exists

    combo_data = []
    if a.get("color_pieces_combo"):
        named_piece_outlines, raw_outlines = (
            _named_outlines_from_net_dxf(
                a["body_asset_combo"], offset_size_inches=offset_size_inches_combo
            )
            if a.get("body_asset_combo")
            else None
        )
        _store_outlines = dict(raw_outlines)
        for idx, p in enumerate(a["color_pieces_combo"]):
            p = f"s3://meta-one-assets-prod/{p}"
            im = s3.read(p)
            g = get_piece_outline(im)
            name, confidence = name_part(
                g, named_piece_outlines, idx=idx + offset, min_dist=dist_threshold
            )
            # what we do is use the raw outline in these files if we have it and can map the name
            g = raw_outlines.get(name, g)
            piece_key = _get_piece_key(name)
            notches = detect_castle_notches_from_outline(g)
            combo_data.append(
                {
                    "name": str(name),
                    "key": str(name),
                    "piece_key": piece_key,
                    "confidence": confidence,
                    "material_code": piece_map.get(piece_key, {}).get(
                        "material_code", material_code
                    ),
                    "uri": p,
                    "uid": p.split("/")[-1].split(".")[0],
                    "notches": notches,
                    "outline": g,
                    "rank": "Combo",
                    "artwork_uri": piece_map.get(piece_key, {}).get(
                        "artwork_uri", artwork_uri
                    ),
                }
            )

        # TODO special case where we fallback if we cannot name pieces -> if we have duplicates names we should probably not trust either
        if len(named_piece_outlines) > 0 and a["print_type"] == "Directional":
            res.utils.logger.debug(
                "because we could not name all pieces we cannot trust the mapping to placed color pieces - trusting body asset instead"
            )
            combo_data = _from_outlines(
                _store_outlines,
                artwork_uri,
                "Combo",
                piece_map,
                material_code_combo,
            )

    return data + combo_data


def add_meta_data_to_placed_color_files(
    net_dxf_path, color_filenames, piece_map=None, plan=False, debug=False
):
    """
    DEPRECATE IN PLACE OF THE META ONE BUILDER

    how we read from color pieces and then look for them on a body file with named pieces in the right scale and orientation
    we both save the NET DXF to the location for the style although its fine to add it to the body one time too
    we then add the named pieces with the metadata of the color pieces

    We should really just build meta one 2d with this and refactor a little for different evs
    """
    s3 = res.connectors.load("s3")

    def _get_piece_outlines_from_paths(paths):
        for p in paths:
            yield p, get_piece_outline(s3.read(p))

    def export_file(path, df):
        NF = df.copy()
        for col in ["outline", "polylines", "geometry", "notches"]:
            NF[col] = NF[col].map(str)
        for col in ["bounds", "original_shape_bounds"]:
            NF[col] = NF[col].map(str)
            path = f"{path}/pieces.feather"
        s3.write(path, NF)
        return path

    s, nf = DxfFile.make_setup_file_svg(net_dxf_path, return_shapes=True)
    named_piece_outlines = dict(nf[["key", "outline"]].values)
    piece_names_metadata = []
    for idx, t in enumerate(_get_piece_outlines_from_paths(color_filenames)):
        g, uid = t[1], t[0]
        name, confidence = name_part(
            g, named_piece_outlines, idx=idx, invert_named_pieces=False, min_dist=3000
        )
        piece_names_metadata.append(
            {
                "name": name,
                "confidence": confidence,
                "uri": uid,
                "uid": uid.split("/")[-1].split(".")[0],
            }
        )

    res.utils.logger.warn(f"did not name { named_piece_outlines }")
    path = color_filenames[0].split("/")
    path = "/".join(path[:-1])
    if not plan:
        s3.write(f"{path}/piece_names.json", json.dumps(piece_names_metadata))

    df = pd.DataFrame(piece_names_metadata)
    if debug:
        return df

    df = pd.merge(nf, df, left_on="key", right_on="name", how="left")

    if not plan:
        return export_file(path, df)

    return df


def get_garment_images(data):
    """
    For a style data result, we can generate the images
    """

    from res.media.images.outlines import place_directional_color_in_outline
    from res.media.images.geometry import mirror_on_y_axis
    from shapely.wkt import loads

    s3 = res.connectors.load("s3")

    def name(row):
        return row["pieces_code"]

    for row in data.to_dict("records"):
        im = place_directional_color_in_outline(
            mirror_on_y_axis(loads(row["pieces_geometry"])),
            None,
            None,
            tile=s3.read(row["pieces_artwork_uri"]),
        )
        yield name(row), im


def _save_to_queue(data, cache_dir="s3://res-data-platform/samples/images/piece_bot"):
    """
    Each style should have a map to a product sku alias a garment:size
    We can lookup airtable for the existing record ids
    WE can generate the images from the pieces data and upload to airtable queue
    """

    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")

    raise Exception("Not implemented yet - need to make a map of names to attachments")

    for name, style_data in data.groupby("product_sku_alias"):
        urls = []

        for piece_name, im in get_images(style_data):
            urls.append(f"{cache_dir.rstrip('/')}/{piece_name}.png")

        # we should pre look up the ids so we can upsert
        airtable["appqtN4USHTmyC6Dv"]["tblGlC0FTUo3ZMryB"].update_record(
            {
                "Name": name,
                # "record_id" : 'rechjYcGnmyCogTNT',
                "Attachments": [{"url": s3.generate_presigned_url(u)} for u in urls],
            }
        )


"""

"""


def make_metadata(row):
    return {
        "is_combo": row["is_combo"],
        "print_type": row["print_type"],
        "artwork_id": row["artwork_file_id"],
        "normed_sized": row["normed_size"],
        "net_file": row["filename"],
    }


def make_key(row):
    m = row.get("customization", {})
    for k in [
        "body_code",
        "color_code",
        "material_code",
        "size_code",
        "body_version",
        "piece_name",
    ]:
        m[k] = row[k]
    # as uuid
    return uuid_str_from_dict(m)


def get_artwork(ids=None):
    mongo = res.connectors.load("mongo")
    from res.connectors.mongo.MongoConnector import ObjectId

    if ids is not None:
        if not isinstance(ids, list):
            ids = [ids]
        ids = {"_id": {"$in": ids}}

    artwork = pd.DataFrame(mongo["resmagic"]["artworkFiles"].find(ids))

    # res.utils.logger.debug(f"{len(artwork)} from {ids}")
    ids = artwork["fileId"].dropna()
    ids = {"_id": {"$in": [ObjectId(_id) for _id in ids]}}

    files = pd.DataFrame(mongo["resmagic"]["files"].find(ids))
    files["path"] = files["s3"].map(lambda x: f"s3://{x.get('bucket')}/{x.get('key')}")
    file_map = dict(files[["_id", "path"]].astype(str).values)

    artwork["uri"] = artwork["fileId"].map(lambda x: file_map.get(x))
    artwork_expanded = expand_column(artwork, "legacyAttributes")
    artwork_expanded = rename_and_whitelist(
        artwork_expanded,
        columns={
            "_id": "id",
            "approvedForMaterialCodes": "approved_materials",
            "uri": "uri",
            "legacyAttributes_Color Type": "color_type",
            "legacyAttributes_Created At": "created_at",
            "legacyAttributes_Last Updated At": "modified_at",
            "legacyAttributes___colorcode": "color_code",
            "legacyAttributes_Artwork File Name": "name",
            "legacyAttributes___colorcollection": "collection",
            "legacyAttributes": "legacyAttributes",
        },
    )
    return artwork_expanded


def explode_pieces(chk, artwork_lu):
    chk = (
        chk.explode("piece_names")
        .reset_index()[
            [
                "body_code",
                "body_version",
                "color_code",
                "material_code",
                "size_code",
                "artwork_uri",
                "piece_names",
                "sizes",
                "type",
                "is_combo",
                "metadata",
                "filename",
                "sku",
            ]
        ]
        .rename(columns={"piece_names": "piece_name"})
    )
    chk["id"] = chk.apply(make_key, axis=1)
    chk["customization"] = chk["body_code"].map(lambda x: {})

    chk["artwork_uri"] = chk["artwork_file_id"].map(lambda x: artwork_lu.get(x))
    chk["metadata"] = chk.apply(make_metadata, axis=1)
    return chk


def process_pieces(pieces):
    results = []
    # this should be perfect circle - get the files that gave the names and lookup the geometries
    for f, g in pieces.groupby("filename"):
        try:
            s, sdata = DxfFile.make_setup_file_svg(f, return_shapes=True)
            sdata = sdata.set_index("key")
            g["geometry"] = g["piece_name"].map(lambda x: sdata.loc[x]["geometry"])
            g["notches"] = g["piece_name"].map(lambda x: sdata.loc[x]["notches"])

            results.append(g)
        except:
            pass

        #####
        ## BREAK just to show one example of doing all the colors/styles that we have for this NET (body and sizes)
        #####
        break
    pieces_with_geoms = pd.concat(results).reset_index().drop("filename", 1)
    pieces_with_geoms = (
        pieces_with_geoms.drop("type", 1).drop("sizes", 1).drop("is_combo", 1)
    )
    return pieces_with_geoms


def get_bodies(keys):
    if not isinstance(keys, list) and keys is not None:
        keys = [keys]
    pred = (
        AirtableConnector.make_key_lookup_predicate(keys, "Body Number")
        if keys
        else None
    )

    bodies = AirtableConnector.get_airtable_table_for_schema_by_name(
        "meta.bodies", filters=pred
    )
    bodies["is_3d_enabled"] = bodies["is_3d_enabled"].fillna(False)
    bodies["available_sizes"] = bodies["available_sizes"].map(
        lambda sizes: [
            i.lstrip().rstrip() for i in str(sizes).split(",") if isinstance(sizes, str)
        ]
    )
    bodies["version"] = bodies["version"].fillna(0).astype(int)
    return bodies


def get_styles_and_their_bodies(keys=None, one_ready_only=True, by_body_code=None):
    def three_component_skus(l):
        if isinstance(l, list):
            return [" ".join(s.split(" ")[:3]) for s in l]
        return l

    if not isinstance(keys, list):
        keys = [keys] if keys is not None else None
    keys = three_component_skus(keys)

    if not by_body_code:
        pred = (
            AirtableConnector.make_key_lookup_predicate(keys, "Resonance_code")
            if keys
            else None
        )
    else:
        if not isinstance(by_body_code, list):
            by_body_code = [by_body_code]
        pred = AirtableConnector.make_key_lookup_predicate(by_body_code, "Body Code")

    styles = AirtableConnector.get_airtable_table_for_schema_by_name(
        "meta.styles", filters=pred
    )

    if len(styles) == 0:
        return pd.DataFrame()

    if one_ready_only:
        styles = styles[styles["is_one_ready"] == 1].reset_index()
        res.utils.logger.debug(f"Filtering to {len(styles)} one ready styles")

    res.utils.logger.info(f"Loading bodies for {len(styles)} styles")

    bodies = None
    if keys is not None:
        bodies = list(styles["body_code"].unique())
    bodies = get_bodies(bodies)
    return styles, bodies


def get_3d_body_pieces_file_path(body, normed_size, version):
    """
    example file:
      s3://meta-one-assets-prod/bodies/3d_body_files/th_1002/v2/extracted/dxf_by_size/2X/printable_pieces.feather
    we first try to just directly match otherwise we look up the aliases

    We could in a more advance mode match any version but dont do that here because that file should be created as soon as any body is onboarded
    and does not need any human placement etc to be done - which is to say if the body version exists we should almost always have the pieces
    """

    s3 = res.connectors.load("s3")

    size = normed_size

    path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body.lower().replace('-','_')}/v{version}/extracted/dxf_by_size/{size}/printable_pieces.feather"
    if s3.exists(path):
        return path

    res.utils.logger.debug(f"Did not match on normed size trying aliases")

    root = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body.lower().replace('-','_')}/v{version}/extracted/dxf_by_size/"
    sizes = [f.split("/")[-2] for f in s3.ls(root) if "printable_pieces.feather" in f]
    size = DxfFile.map_res_size_to_gerber_size_from_size_list(sizes, normed_size)

    path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body.lower().replace('-','_')}/v{version}/extracted/dxf_by_size/{size}/printable_pieces.feather"
    if s3.exists(path):
        return path

    return None


def get_net_data_for_body_sizes(body, sizes, version=None, load_names=False):
    """
    split the name up and determine things components
    normal case
    pd.DataFrame(get_piece_name_for_body_sizes('KT-3034',sizes=body_sizes['KT-3034'],version=4))

    """
    s3 = res.connectors.load("s3")

    def names_from_setups(f):
        names = []

        with s3.file_object(f) as f:
            for l in [l.decode() for l in f.readlines()]:
                if body in l and len(l.split(" ")) == 1:
                    names.append(l.rstrip("\n").rstrip("\r"))

        return names

    def get_combo_component(l):
        for item in l:
            filename = item.split("/")[-1]
            filename_components = filename.split("_")
            if "c" in filename_components:
                return filename_components.index("c")
        return None

    body_lower = body.lower().replace("-", "_")

    sizes = [
        s.lower().replace("-", "_").rstrip().lstrip().replace("size ", "")
        for s in sizes
    ]

    search = f"s3://meta-one-assets-prod/bodies/net_files/{body_lower}"
    if version:
        version = str(version).lower().replace("v", "")
        search = f"{search}/v{version.lower()}"
    l = list(s3.ls(search))

    # check if there is a combo component
    combo_comp = get_combo_component(l)

    for s in sizes:
        for item in l:
            filename = item.split("/")[-1]

            # handling the annoying exceptions because there is no global disambig.
            if f"_{s}_" in filename and not f"p_{s}_" in filename:
                d = {
                    "body_code": body,
                    "version": item.split("/")[-2],
                    "normed_size": s.upper(),
                    "type": "self",
                    "filename": item,
                }
                if combo_comp is not None:
                    # in the first two cases we have the combo or self in the name
                    # in the last case we have a hybrid where the combo may be in the name but the self is omitted
                    #  's3://meta-one-assets-prod/bodies/net_files/jr_2000/v18/jr_2000_v18_net_37_15x15.dxf',
                    #  's3://meta-one-assets-prod/bodies/net_files/jr_2000/v18/jr_2000_v18_net_37_c_15x15.dxf',
                    if f"_{s}_c" in filename:
                        cd = dict(d)
                        cd["type"] = "combo"
                        if load_names:
                            cd["piece_names"] = names_from_setups(item)
                        yield cd
                    elif f"_{s}_s" in filename:
                        cd = dict(d)
                        cd["type"] = "self"
                        if load_names:
                            cd["piece_names"] = names_from_setups(item)
                        yield cd
                    elif f"_{s}_" in filename:
                        cd = dict(d)
                        cd["type"] = "self"
                        if load_names:
                            cd["piece_names"] = names_from_setups(item)
                        yield cd

                else:
                    if load_names:
                        d["piece_names"] = names_from_setups(item)
                    yield d


def get_body_pieces_mapping():
    mongo = res.connectors.load("mongo")
    bp = pd.DataFrame(mongo["resmagic"]["bodyPieces"].find())
    bp["code"] = bp["legacyAttributes"].map(lambda la: la.get("Generated Piece Code"))
    return dict(bp[["_id", "code"]].values)


def get_style_costs_for_style_skus(skus):
    """
    Using snowflake as a source of truth is temporary because its not a good store for fast lookups

    example format:
       style_codes = ['TK-3001 CHRST RODOSG', 'TK-6077 PIMA7 ROSEBH', 'JR-9003 COMCT HAUNQP']

    """
    if isinstance(skus, str):
        skus = [skus]
    skus = list(skus)
    snowflake = res.connectors.load("snowflake")
    style_codes = ",".join([f"'{s}'" for s in skus])
    style_cost = snowflake.execute(
        f""" SELECT "SKU", "STYLE_COST" FROM "IAMCURIOUS_DB"."IAMCURIOUS_SCRATCH"."SCD_STYLE_COSTS" WHERE VALID_TO is NULL and  STYLE_CODE in ({style_codes}) ; """
    )
    return dict(style_cost.values)


def get_active_markers_for_skus(skus):
    """
    Four- part size sku needed e.g. CC3075 CHRST BAROLJ 3ZZMD or CC-3075 CHRST BAROLJ 3ZZMD
    """
    if isinstance(skus, str):
        skus = [skus]
    if skus:
        skus = [s.replace("-", "") for s in skus]
    airtable = res.connectors.load("airtable")
    res.utils.logger.debug(f"Loading the DXA queue")
    data = (
        airtable["appqtN4USHTmyC6Dv"]["tblwhgsCzGpt7odBg"]
        .to_dataframe(
            fields=[
                "Style Code",
                "ONE Marker Flow Status",
                "Status",
                "_color_on_shape_pieces_collection",
                "Body Version",
                "Markers IDs",
                # "Last Updated At",
                "Created At",
                "SKU",
                "NET DXF Info",
            ],
            filters=(
                AirtableConnector.make_key_lookup_predicate(skus, "SKU")
                if skus
                else None
            ),
        )
        .sort_values("Created At")
    )

    data["status"] = data["ONE Marker Flow Status"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )

    data = data[data["status"] == "Done"]

    res.utils.logger.debug(f"Loading the markers")
    mdata = AirtableConnector.get_airtable_table_for_schema_by_name(
        "meta.markers",
        filters=(
            AirtableConnector.make_key_lookup_predicate(
                list(data["Markers IDs"].unique()), "_record_id"
            )
            if skus
            else None
        ),
    )
    res.utils.logger.debug(f"Merging queue and markers")

    def try_load(s):
        try:
            return json.load(s)
        except:
            try:
                return literal_eval(s)
            except:
                return s

    data = pd.merge(
        data,
        mdata,
        how="left",
        left_on="Markers IDs",
        right_on="record_id",
        suffixes=["", "_marker"],
    )
    data["_color_on_shape_pieces_collection"] = data[
        "_color_on_shape_pieces_collection"
    ].map(try_load)
    data["filenames"] = data["filenames"].map(try_load)

    # keep the latest marker done per sku and rank
    data = data.sort_values("Created At", ascending=True).drop_duplicates(
        subset=["SKU", "rank"], keep="last"
    )

    return data


def get_net_files(body_versioned_sized):
    """
    pass in a selection from the bodies table that gives current version and avail size and load the net dxf files
    """
    allofit = []
    FAILURES = {}
    for record in body_versioned_sized.to_dict("records"):
        try:
            df = pd.DataFrame(
                get_net_data_for_body_sizes(
                    record["key"],
                    sizes=record["available_sizes"],
                    version=record["version"],
                    load_names=True,
                )
            )
            allofit.append(df)
        except Exception as ex:
            FAILURES[record["key"]] = ex

    print(f"There were {len(FAILURES)}")

    return pd.concat(allofit)


def explode_style_sizes(data, lookup):
    res.utils.logger.info(f"Loading sizes")

    sizes = res.connectors.load("airtable")["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    sizes = sizes.to_dataframe(
        fields=["_accountingsku", "Size Normalized", "Size Chart"]
    )
    # size_map = dict(sizes[["_accountingsku", "record_id"]].values)
    # inv_size_map = {v: k for k, v in size_map.items()}
    size_chart_lu = dict(sizes[["Size Chart", "_accountingsku"]].values)
    normed_size_lu = dict(sizes[["Size Chart", "Size Normalized"]].values)

    data = data.explode("sizes").reset_index()
    data["normed_size"] = data["sizes"].map(lambda x: normed_size_lu.get(x))
    data["size_code"] = data["sizes"].map(lambda x: size_chart_lu.get(x))
    data["style_code"] = data.apply(
        lambda row: f"{row['body_code']} {row['material_code']} {row['color_code']}",
        axis=1,
    )
    data["sku"] = data.apply(
        lambda row: f"{row['body_code']} {row['material_code']} {row['color_code']} {row['size_code']}",
        axis=1,
    )

    if len(lookup):
        data = pd.merge(
            data,
            lookup,
            on=["body_code", "body_version", "type", "normed_size"],
            how="left",
        )
        data["has_assets"] = data["filename"].notnull().astype(int)
    else:
        data["filename"] = None
        data["has_assets"] = False

    res.utils.logger.info(f"Loading costs from cache")
    costs = get_style_costs_for_style_skus(data["style_code"].unique())

    data["cost"] = data["sku"].map(lambda s: costs.get(s, -1)).map(float).fillna(-1)

    return data


def get_all_styles_sizes(include_dxa=True):
    """
    gets all styles sizes - this can be passed as inout record by record into compiler with or without trying to name pieces i.e. color pieces matched to names
    not naming pieces means we can separate concerns which is partially conceptual i.e using style info only without DXA up front but mainly to do the heavier data processing on the server
    """
    s = get_styles()
    s = s.explode("sizes")
    s["size_code"] = s["sizes"]
    s["style"] = s["sku"]
    s["sku"] = s.apply(lambda row: f"{row['sku']} {row['sizes']}", axis=1)
    if not include_dxa:
        return s
    dxa = get_dxa_asset(None)
    s = pd.merge(s, dxa, how="left", on="sku", suffixes=["", "_dxa"])
    s = s.where(pd.notnull(s), None)
    s["body_version"] = s["body_version"].fillna(0).map(int)
    return s


def map_styles_to_style_pieces(data):
    """

    create records that can be posted to res_meta.dxa.style_pieces_update_requests
    Validated with meta.StyleMetaOneRequest from Pydantic schema
    """
    mat_props = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.material_properties"
    )
    airtable = res.connectors.load("airtable")
    sizes = dict(
        airtable.get_table_data("appjmzNPXOuynj6xP/tblvexT7dliEamnaK")[
            ["record_id", "_accountingsku"]
        ].values
    )
    bods = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tblXXuR9kBZvbRqoU",
        fields=["Body", "Body Pieces", "Body Number", "Base Pattern Size ID"],
    )
    pieces = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tbl4V0x9Muo2puF8M", fields=["Name", "Generated Piece Code"]
    )
    pieces_lookup = dict(pieces[["record_id", "Generated Piece Code"]].values)
    bods["body_pieces"] = bods["Body Pieces"].map(
        lambda x: None if not isinstance(x, list) else [pieces_lookup.get(i) for i in x]
    )
    body_pieces = dict(bods[["Body Number", "body_pieces"]].dropna().values)
    body_base_size_ids = dict(
        bods[["Body Number", "Base Pattern Size ID"]].dropna().values
    )
    body_base_sizes = {k: sizes.get(v) for k, v in body_base_size_ids.items()}
    mat_offsets = dict(mat_props[["key", "offset_size"]].dropna().values)

    records = []
    for record in data.to_dict("records"):
        for size_code in record["sizes"]:
            if not pd.isnull(size_code):
                d = {
                    "id": record["record_id"],
                    "sku": f"{record['sku']} {size_code}",
                    "style_sku": record["sku"],
                    "size_code": size_code,
                    "style_name": record["style_name"].lstrip().rstrip(),
                    "body_version": int(record["body_version"]),
                    "print_type": record["print_type"],
                    "body_code": record["body_code"],
                    "sample_size": body_base_sizes[record["body_code"]],
                }
                piece_mapping = record.get("style_pieces", {}) or {}
                pieces = body_pieces.get(record["body_code"], [])
                d["piece_name_mapping"] = [
                    {
                        "key": f"{record['body_code']}-V{int(record['body_version'])}-{p}",
                        "material_code": piece_mapping.get(p, {}).get(
                            "material_code", record["material_code"]
                        ),
                        "artwork_uri": piece_mapping.get(p, {}).get(
                            "artwork_uri", record["artwork_uri"]
                        ),
                        "color_code": piece_mapping.get(p, {}).get(
                            "color_code", record["color_code"]
                        ),
                    }
                    for p in pieces
                    if p.split("-")[-1] not in ["X", "F"]
                ]
                for p in d["piece_name_mapping"]:
                    p["offset_size_inches"] = mat_offsets.get(p["material_code"], 0)

                records.append(d)
    return records


def get_styles(skus=None, by_body_code=None):
    """
    relatively light weight way to read everything we care about styles
    we factored out the body stuff which we assume to be managed separately
    """

    def body_piece_resolver():
        pm = get_body_pieces_mapping()

        def f(pid):
            return pm.get(pid)

        return f

    def style_pieces_resolver(artwork_resolver=None):
        f = body_piece_resolver()

        def g(s):
            if isinstance(s, list):
                d = {}
                for a in s:
                    if artwork_resolver and "artwork_id" in a:
                        a["artwork_uri"] = artwork_resolver(a.pop("artwork_id"))
                    # if artwork resolver resolver the artwork to a uri
                    d[f(a.pop("body_piece_id"))] = a
                return d

        return g

    def try_second_material(row):
        l = row["materials_used"]
        if isinstance(l, list):
            l = [m for m in l if m != row["material_code"]]
            if len(l):
                return l[0]

    def sp(x):
        import json

        try:
            return json.loads(x)["style_pieces"]
        except:
            return None

    def size_resolver():
        sizes = res.connectors.load("airtable")["appjmzNPXOuynj6xP"][
            "tblvexT7dliEamnaK"
        ]
        sizes = sizes.to_dataframe(
            fields=["_accountingsku", "Size Normalized", "Size Chart"]
        )
        size_chart_lu = dict(sizes[["Size Chart", "_accountingsku"]].values)

        def f(x):
            if isinstance(x, list):
                return [size_chart_lu.get(a) for a in x]
            return size_chart_lu.get(x)

        return f

    res.utils.logger.info("loading material offsets")
    mat_props = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.material_properties"
    )
    offset_sizes = dict(mat_props[["key", "offset_size"]].dropna().values)
    # unstable_materials = list(mat_props[mat_props['material_stability']=='Unstable']['key'])

    res.utils.logger.info(f"loading resolvers")
    artwork_map = dict(get_artwork()[["id", "uri"]].values)
    artwork_resolver = lambda x: artwork_map.get(x)
    sizes = size_resolver()
    f = style_pieces_resolver(artwork_resolver)

    res.utils.logger.info(f"loading styles")
    a, b = get_styles_and_their_bodies(
        keys=skus, one_ready_only=False, by_body_code=by_body_code
    )
    # this uses the schema res-schema/res/meta/styles
    s = a[
        [
            "style_name",
            "res_key",
            "body_version",
            "materials_used",
            "style_pieces",
            "artwork_file_id",
            "is_one_ready",
            "is_3d_onboarded",
            "record_id",
            "print_type",
            "created_at",
            "style_pieces_status",
            "style_pieces_status_internal",
        ]
    ]
    s["sku"] = s["res_key"].map(lambda x: f"{x[:2]}-{x[2:]}")
    s["body_code"] = s["sku"].map(lambda x: x.split(" ")[0].strip())
    s["material_code"] = s["sku"].map(lambda x: x.split(" ")[1].strip())

    s["color_code"] = s["sku"].map(lambda x: x.split(" ")[2].strip())
    s["style_pieces"] = s["style_pieces"].map(sp).map(f)
    s["artwork_uri"] = s["artwork_file_id"].map(artwork_resolver)

    s = pd.merge(s, b[["key", "available_sizes"]], left_on="body_code", right_on="key")
    s["sizes"] = s["available_sizes"].map(sizes)

    s["is_3d_onboarded"] = s["is_3d_onboarded"].fillna(False)

    s["material_code_combo"] = s.apply(try_second_material, axis=1)
    s["offset_size_inches"] = s["material_code"].map(
        lambda x: offset_sizes.get(x) or None
    )
    s["offset_size_inches_combo"] = s["material_code_combo"].map(
        lambda x: offset_sizes.get(x) or None
    )
    s["piece_mapping_approved"] = False
    s.loc[
        (s["style_pieces_status"] == "Style Pieces Mapping Approved")
        & (s["style_pieces_status_internal"] == "Style Pieces Mapping Approved"),
        "piece_mapping_approved",
    ] = True

    return s


def process_styles(skus, explode_sizes=False, one_ready_only=True):
    """
    load the styles and the refs for bodies, sizes, net files and pieces
    skus are the res codes e.g  AB-1234 XXXX XXXXX

    We can explode sizes to split out the garments from the style
    """

    res.utils.logger.info(f"Loading styles and their bodies")
    styles, bodies = get_styles_and_their_bodies(skus, one_ready_only=one_ready_only)
    res.utils.logger.info(f"Loading net files or 3d meta data files for bodies")
    net_files = get_net_files(bodies)
    res.utils.logger.info(f"Loading pieces mapping")
    piece_keys = get_body_pieces_mapping()

    res.utils.logger.info(f"Processing {len(styles)} styles")
    return _process_styles(
        styles,
        bodies,
        net_files,
        piece_keys=piece_keys,
        explode_sizes=explode_sizes,
        one_ready_only=one_ready_only,
    )


def _process_styles(
    styles, bodies, net_files, piece_keys, explode_sizes=False, one_ready_only=True
):
    """
    the net file should be known by convention if it exists? caching the lookup makes sense though

    """

    def unpack_style_pieces(row):
        sp = row["style_pieces"]
        try:
            d = json.loads(sp)
            _sp = d.get("style_pieces")
            if len(_sp) == 0:
                return None
            for rec in _sp:
                rec["piece_key"] = piece_keys.get(rec["body_piece_id"])
            return _sp
        except:
            return None

    def extract_names(x):
        if isinstance(x, list):
            return [r["piece_key"] for r in x]
        return []

    def extract_artwork(row):
        x = row["style_pieces_unpacked"]
        if isinstance(x, list):
            return list(
                set(
                    [row["artwork_file_id"]]
                    + [
                        r.get("artwork_id")
                        for r in x
                        if r.get("artwork_id") not in [None, "default"]
                    ]
                )
            )
        return [row["artwork_file_id"]]

    def make_piece_info(row, pieces_lookup, artwork_lookup):
        m = {}
        pa = row.get("style_pieces_unpacked", [])
        pa = pa if pa else []

        for rec in pa:
            k = pieces_lookup.get(rec["body_piece_id"])
            if k:
                m[k] = {
                    "material_code": rec.get("material_code"),
                    "artwork_uri": artwork_lookup.get(rec["artwork_id"]),
                }
        return m

    S = styles[
        [
            "style_id",
            "res_key",
            "record_id",
            "style_name",
            "body_code",
            "color_code",
            "material_code",
            "sizes",
            "artwork_file_id",
            "style_pieces",
            "materials_used",
            "print_type",
            "is_one_ready",
            "body_version",
            "is_3d_onboarded",
            "retail_price",
        ]
    ]
    S["price"] = S["retail_price"].fillna(-1)

    S["materials_used"] = S["materials_used"].map(
        lambda x: [x] if isinstance(x, str) else x if isinstance(x, list) else []
    )
    S["style_pieces_unpacked"] = S.apply(unpack_style_pieces, axis=1)
    S["is_combo"] = S["materials_used"].map(len) > 1
    S["sizes"] = S["sizes"].map(
        lambda x: (
            [a.rstrip().lstrip() for a in x.split(",")]
            if not isinstance(x, float)
            else None
        )
    )
    S["versioned_body_code"] = S.apply(
        lambda row: f"{row['body_code']}-V{row['body_version']}", axis=1
    )
    S["stype_piece_names"] = S["style_pieces_unpacked"].map(extract_names)
    S["artworks_used"] = S.apply(extract_artwork, axis=1)
    if one_ready_only:
        S = S[S["is_one_ready"] == 1]
    S["body_version"] = S["body_version"].astype(int)
    S["is_3d_onboarded"] = S["is_3d_onboarded"].fillna(False)
    # we have setup files for thes

    S = S.drop("style_pieces", 1).explode("materials_used")
    S["type"] = "self"
    S.loc[S["materials_used"] != S["material_code"], "type"] = "combo"
    # return S
    if len(net_files) > 0:
        net_files["body_version"] = net_files["version"].map(
            lambda x: int(float(x.replace("v", "")))
        )

        lookup = (
            net_files[["body_code", "piece_names", "type", "body_version", "filename"]]
            .sort_values("body_version")
            .drop_duplicates(subset=["body_code", "type"], keep="last")
        )

    S["num_artworks"] = S["artworks_used"].map(len)

    # styles_2d_one_ready = S[(S['is_3d_body']==False)&(S['is_one_ready']==1)]
    S["num_artworks"] = S["artworks_used"].map(len)

    artwork_map = dict(
        get_artwork(list(S["artworks_used"].explode()))[["id", "uri"]].values
    )
    S["artwork_uri"] = S["artwork_file_id"].map(lambda a: artwork_map.get(a))
    S["piece_mapping_info"] = S.apply(
        lambda row: make_piece_info(
            row, pieces_lookup=piece_keys, artwork_lookup=artwork_map
        ),
        axis=1,
    )

    if explode_sizes:
        S = explode_style_sizes(S, lookup=net_files)
    elif len(net_files) > 0:
        S = pd.merge(S, lookup, on=["body_code", "body_version", "type"], how="left")
        S["has_all_style_piece_names"] = S["stype_piece_names"].map(
            lambda x: True if None not in x or len(x) == 0 else False
        )

    return S


def extract_pieces(garment, **kwargs):
    """
    given a garment, it must be possible to lookup assets for geometry and other data for each piece
    this is the final resolution of the piece
    """
    on_piece_load_error = kwargs.get("on_piece_load_error", "set_pending_status")

    def id_for_piece(p, code, instance=0):
        d = {
            k: p[k]
            for k in [
                "versioned_body_code",
                "color_code",
                "material_code",
                "size_code",
                "customization",
            ]
            if k in p
        }
        d["code"] = code
        # hack to test for duplicate piece names in files
        if instance > 0:
            res.utils.logger.debug(
                f"Found a duplicated piece name {code} for asset {garment.get('filename')}"
            )
            d["instance"] = instance
        return uuid_str_from_dict(d)

    # if in 3d do this otherwise in 2d load something else
    try:
        s, sdata = DxfFile.make_setup_file_svg(garment["filename"], return_shapes=True)
    except Exception as ex:
        res.utils.logger.warn(f"Failing to process setup file {garment['filename']}")
        if pd.isnull(garment["filename"]):
            res.utils.logger.warn(
                f"Because there is no known setup we can continue to process and try geometries later"
            )
        if on_piece_load_error == "raise":
            raise
        else:
            res.utils.logger.warn(
                f"ignoring load error for NET file {garment.get('filename')} {res.utils.ex_repr(ex)}"
            )
            return []

    d = {}
    instances = []

    piece_names = garment["piece_names"]
    piece_mapping = garment["piece_mapping_info"]
    # piece->artwork_uri map
    # piece->material code map
    for p in piece_names:
        if p not in d:
            d[p] = 0
        else:
            d[p] += 1
        instances.append(d[p])

    def try_geom_for_piece(code, geom):
        try:
            return sdata.set_index("key").loc[code][geom]
        except:
            return None

    def piece_name_from_code(code, versioned_body):
        """
        because the code contains the BODY-VERSION and we dont want that in the lookup here
        """
        return code.replace(f"{versioned_body}-", "")

    def try_get_field(code, field):
        """
        TODO: this field mapping is a major contractual issue
        if the piece mapping exists we MUST get the artwork or fail
        """
        code = piece_name_from_code(code, garment["versioned_body_code"])
        d = piece_mapping.get(code)
        if d:
            return d.get(field, garment[field])
        return garment[field]

    return [
        {
            "id": id_for_piece(garment, code, instance=instances[i]),
            "code": code,
            "versioned_body_code": garment["versioned_body_code"],
            "color_code": garment["color_code"],
            "size_code": garment["size_code"],
            # we get materials and artworks from the mapping if they are known
            # - we need to be CARFUL here though because we are falling back to the artwork of the garment which is not correct to do and get lead to major make issues
            "artwork_uri": try_get_field(code, "artwork_uri"),
            "material_code": try_get_field(code, "material_code"),
            # to string for saving
            "geometry": str(try_geom_for_piece(code, "geometry")),
            "notches": str(try_geom_for_piece(code, "notches")),
            "metadata": garment.get(
                "metadata",
                {
                    # the logic for this is if the pieces are in the other material and the idea is there should be a second setup. check this
                    "combo_type": garment.get("type"),
                    "print_type": garment.get("print_type"),
                },
            ),
            # "customization" : p.get('customization',{})
        }
        for i, code in enumerate(piece_names)
    ]


def validate_pieces(pieces):
    """
    check unique ids for a set going to the database - JR-2000-V18-PNTFTFLFRT-C is an example of duplicate piece names
    Test case for missing artwork uri
    """
    return []


def _style_id_from_name(name, body_code):
    """
    Convention for generating an id to make names unique and a business handle to the style
    A brand should have a globally unique name for their style - but we allow per body for now
    (even though it would not really make sense to sell the same thing as two bodies?)

     `Name` is like "Vertical Dart Face Mask - Gold Leaves in Bru..." and `body_code` is like "CC-8068"

    """
    return uuid_str_from_dict({"name": name.lstrip().rstrip(), "body_code": body_code})


def _style_size_id_from_style_id_and_size(sid, size_code):
    return res.utils.uuid_str_from_dict({"style_id": sid, "size_code": size_code})


def _piece_id_from_style_size_and_piece_key(style_size_id, key):
    return res.utils.uuid_str_from_dict({"key": key, "style_size_id": style_size_id})


def _body_piece_id_from_body_id_key_size(bid, key, size_code):
    d = res.utils.uuid_str_from_dict(
        {
            "body_id": bid,
            "piece_key": key,
            "size": size_code,
            "profile": "default",
        }
    )
    # print(bid, key, size_code, " ---> ", d)
    return d


def _body_id_from_body_code_and_version(body_code, body_version):
    return res.utils.uuid_str_from_dict(
        {
            "code": body_code,
            "version": int(float(body_version)),
            "profile": "default",
        }
    )


def _bootstrap_add_style_headers(skus, on_error=None):
    """
    convenience to insert styles into a database; these are style headers without pieces
    - body onboarded separately
    - then color/pieces added separately when generate the meta one
    - other things like trims should be attached to the style OR asset links

    you can run this and check for failures as follows.
    SET YOUR HASURA ENDPOINT - default is probably your local docker based hasura

        failures = helpers._bootstrap_add_style_headers(skus=None)

    """
    res.utils.logger.info("firstly to load the styles - filtering on skus if supplied")
    styles = get_styles(skus)
    styles = styles.where(pd.notnull(styles), None)

    res.utils.logger.info(f"upserting {len(styles)} styles")
    failed_records = []
    hasura = res.connectors.load("hasura")
    from tqdm import tqdm

    for s in tqdm(styles.to_dict("records")):
        try:
            style_model_upsert_style_header(s, hasura=hasura)
        except Exception as ex:
            if on_error and on_error == "raise":
                raise ex
            failed_records.append({"record": s, "exception": ex})
    return failed_records


def style_model_upsert_style_header(
    style_record,
    default_status="Pending",
    add_product=True,
    hasura=None,
    body_version=None,
):
    """
    #required
        body_code
        name
        sizes
    #optional
        color_code

    deprecate any other way of adding styles and sizes
    - gate keeper on how ids and statuses are generated
    """
    _hyphenate_res_code = lambda s: s if "-" == s[2] else f"{s[:2]}-{s[2:]}"
    clean_string = lambda s: s.strip()
    # contract
    name, body_code, sizes, created_at = (
        style_record["style_name"],
        style_record["body_code"],
        style_record["sizes"],
        style_record["created_at"],
    )
    sku = _hyphenate_res_code(style_record["sku"])
    brand_code = style_record["metadata"].get("brand_code")
    body_version = body_version or int(style_record.get("body_version", 0))
    name = clean_string(name)
    sid = _style_id_from_name(name, body_code)
    style_object = {
        "id": sid,
        # sometimes the names have spaces at start and end which we should not encourage
        "name": name,
        "status": default_status,
        "brand_code": brand_code,
        "body_code": body_code,
        "labels": [],
        "style_birthday": created_at,
        "modified_at": res.utils.dates.utc_now_iso_string(),
        "metadata": {
            "airtable_record_id": style_record.get("record_id", ""),
            # its really a legacy thing that we should match by this
            "sku": queries.style_sku(sku),
            # body version is explicity but it will be mapped in the pieces that we add later
            "body_version": body_version,
            "print_type": style_record.get("print_type"),
            "is_unstable_material": bool(
                pd.notnull(style_record.get("offset_size_inches"))
                and style_record.get("offset_size_inches", 0) > 0
            ),
        },
    }

    style_object["contracts_failing"] = style_record.get("contracts_failing", [])

    style_size_objects = [
        {
            "id": _style_size_id_from_style_id_and_size(sid, s),
            "size_code": s,
            "style_id": sid,
            # it does not matter if sku is the 3 or 4 part for different use cases TBD
            "metadata": {"sku": f"{queries.style_sku(sku)} {s}"},
            # "status": "PENDING_PIECES", #this is the default - its safer not to set this when bootsrapping and we can just set to active when ready
            "labels": {},
        }
        for s in set(sizes)
    ]

    hasura = hasura or res.connectors.load("hasura")

    res.utils.logger.debug(f"Style header: {style_object}")
    r = hasura.tenacious_execute_with_kwargs(
        queries.BATCH_INSERT_GARMENT_NO_PIECES,
        style=style_object,
        style_sizes=style_size_objects,
    )

    if add_product:
        try:
            _update_product(sid, queries.style_sku(sku), name=name, hasura=hasura)
            res.utils.logger.info("Added product ref")
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to add style as product {ex} - product trying to use style id {sid}"
            )

    return r


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def _update_product(style_id, style_sku, name, add_self_ref=False, hasura=None):
    """

    add product to the sell model for this style - just registers as a product that can be sold
    """
    from res.flows.sell.orders.queries import UPDATE_PRODUCTS

    product = {
        # product_sku -> style id
        "id": res.utils.uuid_str_from_dict({"product_sku": style_sku}),
        "style_id": style_id,
        "sku": style_sku,
        "name": name,
        "metadata": {"style_sku": style_sku},
    }

    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(UPDATE_PRODUCTS, products=[product])


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def update_styles(styles, plan=False, **kwargs):
    """
    Model to drill down the tree and update the styles model
    we apply some cleaning logic and build meta data objects
    """

    clean_string = lambda s: s.lstrip().rstrip()
    _determine_status = lambda row: (
        "Pending" if pd.isnull(row["filename"]) else "Active"
    )
    _hyphenate_res_code = lambda s: s if "-" == s[2] else f"{s[:2]}-{s[2:]}"
    on_missing_asset_filename = kwargs.get("on_missing_asset_filename", "warn")

    if len(styles[styles["filename"].isnull()]):
        if on_missing_asset_filename == "raise":
            raise Exception(
                f"There are records missing assets filename and mode is `on_missing_asset_filename`=[{on_missing_asset_filename}]"
            )
        elif on_missing_asset_filename == "warn":
            styles = styles[styles["filename"].notnull()].reset_index(drop=True)
            res.utils.logger.warn(f"We are missing some asset file names for styles")
        elif on_missing_asset_filename == "ignore":
            styles = styles[styles["filename"].notnull()].reset_index(drop=True)
        elif on_missing_asset_filename == "process":
            # process them anyway and set the styles to Pending
            pass

    for key, style in styles.groupby(["style_name", "body_code"]):
        name, body_code = key[0], key[1]
        delegate = dict(style.iloc[0])
        sid = _style_id_from_name(name, body_code)
        # the body code does not have hyhen and we want to standardize in our data model
        style_sku = _hyphenate_res_code(delegate.get("res_key")).lstrip().rstrip()
        style_object = {
            "id": sid,
            # sometimes the names have spaces at start and end which we should not encourage
            "name": clean_string(name),
            "status": _determine_status(delegate),
            "body_code": body_code,
            "labels": [],
            "metadata": {
                "airtable_record_id": delegate.get("record_id"),
                "res_id": delegate.get("key"),
                "sku": delegate.get("res_key"),
                "body_version": int(delegate.get("body_version", 0)),
                # we may want to add the body version but trying to keep this implicit in the pieces data for now
            },
        }
        #########

        # ITS import to check that we have both combo and other in same transaction
        for size, garments in style.groupby("size_code"):
            delegate = garments.iloc[0]
            cost = float(delegate["cost"])
            price = float(delegate["price"])

            g = {
                "id": _style_size_id_from_style_id_and_size(sid, size),
                "size_code": size,
                "status": _determine_status(delegate),
                "labels": [],
                "metadata": {"sku": f"{style_sku} {size}"},
                "price": price,
                "cost_at_onboarding": cost,
                "style_id": sid,
            }

            res.utils.logger.debug(f"Processing garment {name} {body_code} {g}")
            #########
            pieces = []
            # usually over piece sets

            for piece_set in garments.to_dict("records"):
                # We must have a filename which is the body level data and the artwork which is used to place the color
                if pd.notnull(piece_set.get("filename")) and pd.notnull(
                    piece_set.get("artwork_uri")
                ):
                    pieces += extract_pieces(piece_set, **kwargs)
                else:
                    # for now at least, we treat things without body data in the filename as pending assets
                    g["status"] = style_object["status"] = "Pending"

            # validate piece set
            # assert there are not two pieces with the same name as that is a sure thing to fuck up the database (constraints)
            # other things can still mess things up but that definitely will
            # validate_pieces()

            if not plan:
                try:
                    queries.update_style(style_object, g, pieces)

                except Exception as ex:
                    res.utils.logger.warn(
                        f"FAILED TO UPDATE - SENDING YOU PIECES TO CHECK IF EXISTS {style_object}, {g}, {pieces}: {ex}"
                    )
                    raise ex
    # sample returned
    return pieces


def _lookup_body_assets(
    latest_versions_only=True,
    validate=False,
    recheck_invalid=False,
    recheck_all=False,
    body_filter=None,
):
    """
    using a cache of body assets which are net dxf files we can load and validate them
    we can use a special case for a single set of bodies like this
        helpers._lookup_body_assets(validate=True, recheck_all=True, body_filter=['TK-3075'])
    but generally this is batch background operation to maintain a status on body assets
    """
    from res.flows.dxa.setup_files import net_dxf_location_cache

    cc = net_dxf_location_cache()

    all_bodies = cc._db.read()
    all_bodies["body_code"] = all_bodies["key"].map(lambda x: x[:7])
    all_bodies["parts"] = all_bodies["key"].map(lambda x: len(x.split("-")))
    all_bodies = all_bodies[all_bodies["parts"] == 4]
    all_bodies["version"] = all_bodies["key"].map(
        lambda x: x.split("-")[2].replace("V", "")
    )
    all_bodies["size_code"] = all_bodies["key"].map(lambda x: x.split("-")[3])
    if latest_versions_only == False:
        return all_bodies
    latest_versions = (
        all_bodies[["body_code", "version"]]
        .groupby("body_code")
        .max()
        .reset_index()
        .rename(columns={"version": "max_version"})
    )

    if body_filter:
        all_bodies = (
            all_bodies[all_bodies["body_code"].isin(body_filter)]
            .reset_index()
            .drop("index", 1)
        )

    chk = pd.merge(all_bodies, latest_versions, on="body_code")
    chk = chk[chk["version"] == chk["max_version"]].reset_index(drop=True)

    if validate:
        net_dxf_location_cache.validate(
            chk, recheck_invalid=recheck_invalid, recheck_all=recheck_all
        )

    chk.to_csv("s3://res-data-platform/cache/bodies.csv", index=None)

    return chk


def _bootstrap_bodies(data, skip_3d_bodies=True, hasura=None):
    """
    Load bodies from somewhere
    ideally validate them first which takes time
    then save them

    Example:

     A useful think to do is look up recent valid bodies and save them
     stuff = _lookup_body_assets(validate=True)
     #could filter by validated or updated after some date
     _bootstrap_bodies(stuff)
    """
    if data is None:
        res.utils.logger.info(
            "Loading all the body assets we have in cache with their latest veresions only"
        )
        data = _lookup_body_assets()
    failed = []
    hasura = hasura or res.connectors.load("hasura")
    for record in data.to_dict(
        "records"
    ):  # [all_bodies['body_code']=='KT-3015'] <- one example - make sure we get multiple versions and all the sizes
        body_code = record["body_code"]

        body_version = int(record["body_version"])
        if skip_3d_bodies:
            if queries._is_3d_body(body_code, body_version, hasura=hasura) == True:
                res.utils.logger.info(f"Skipping the 3d body {body_code}")
                continue
        try:
            update_2d_body_from_net_dxf(
                body_code,
                body_version,
                record["size_code"],
                hasura=hasura,
            )
            metric("BODY", "ADD_2D", "OK")
        except Exception as ex:
            failed.append({"record": record, "reasons": ex})
            metric("BODY", "ADD_2D", "FAILED")
    return failed


def import_valid_2d_body(body_code, body_version, skip_3d_bodies=True, hasura=None):
    bods = net_dxf_location_cache().load(
        body_filter=body_code, body_version_filter=body_version
    )

    if skip_3d_bodies:
        if queries._is_3d_body(body_code, body_version, hasura=hasura) == True:
            res.utils.logger.info(f"Skipping the 3d body {body_code}")
            return

    for v in bods[bods["is_valid"] == True].to_dict("records"):
        res.utils.logger.info(f"adding {v}")
        update_2d_body_from_net_dxf(
            body_code=v["body_code"],
            version=v["body_version"],
            size_code=v["size_code"],
        )
        res.utils.logger.info(f"added {v}")
    return bods


def update_2d_body_from_net_dxf(
    body_code,
    version,
    size_code,
    net_dxf=None,
    brand_code=None,
    profile="default",
    hasura=None,
    plan=False,
):
    """

    examples bodies can be found in the dynamo cache body_net_dxfs warpped by  setup files

      from res.flows.dxa.setup_files import net_dxf_location_cache
      cc = net_dxf_location_cache()
      #JR-3106-V2-8ZZ4X
      cc.get('JR-3106', 2, '8ZZ4X')

    we can register 2d bodies using only the net dxf pair for self and combo
    after doing this we can create style pieces from the DXA asset - the style headers and body will exist at this point

    we use the default profile body and generate ids for body and pieces using the body version profile and size and key of body pieces
    we convert to geojson and default geojson for things we dont support in 2d
    TODO:
    - the piece type from name
    -

    check with john on nullable size code and non nullable vs code
    """
    from res.media.images.providers.dxf import DxfFile
    from res.media.images.geometry import geometry_to_geojson
    from res.flows.dxa.setup_files import net_dxf_location_cache

    def default_dict(x):
        return {}

    def to_geojson(x):
        try:
            return geometry_to_geojson(x)
        except:
            return {}

    bid = _body_id_from_body_code_and_version(body_code, version)

    hasura = hasura or res.connectors.load("hasura")
    brand_code = brand_code or body_code.split("-")[0]
    if net_dxf is None:
        cc = net_dxf_location_cache()
        # JR-3106-V2-8ZZ4X
        net_dxf = cc.get(body_code, version, size_code)
        if net_dxf is None:
            raise Exception(
                "The net dxf files were neither resolvable or found in the cache"
            )

    body = {
        "id": bid,
        "body_code": body_code,
        "version": version,
        "profile": profile,
        "brand_code": brand_code,
    }
    body["metadata"] = {"flow": "2d"}
    body["metadata"]["body_asset"] = net_dxf

    def determine_type(key, rank):
        return rank

    def body_piece_id(x):
        _id = _body_piece_id_from_body_id_key_size(bid, x, size_code)
        return _id

    all_data = []
    for rank, path in net_dxf.items():
        # res.utils.logger.info(f"Opening {path}")
        _, data = DxfFile.make_setup_file_svg(path, return_shapes=True)
        data = data.drop(
            [c for c in data.columns if c not in ["key", "notches", "outline"]], 1
        )

        ###########KEYS##############
        data["piece_key"] = data["key"]
        data["size_code"] = size_code
        # note that these keys are different to some legacy ones where we used the normed size and not the accounting size
        data["key"] = data.apply(lambda row: f"{row['key']}_{row['size_code']}", axis=1)
        # the id is a function of the size but we show it explicitly as f(key_without_size, size, body_id)
        # which is how other parts of the system will resolve it - the key is done so that we only have one thing like this in the database primarily
        # because we use the key and not the id as the db constraint, it is not possible to generate lots of similar pieces
        data["id"] = data["piece_key"].map(body_piece_id)
        ###############################

        for col in ["outline", "notches"]:
            data[col] = data[col].map(to_geojson)

        data = data.rename(
            columns={"outline": "outer_geojson", "notches": "outer_notches_geojson"}
        )

        data["body_id"] = bid
        for col in [
            "inner_geojson",
            "outer_corners_geojson",
            "outer_edges_geojson",
            "inner_edges_geojson",
        ]:
            data[col] = data.index.map(default_dict)

        data["type"] = data["key"].map(lambda x: determine_type(x, rank))
        # for correct piece names actually we can map the type here e.g. block fuse
        all_data.append(data)

    data = pd.concat(all_data).reset_index(drop=True)

    # check unique keys
    if data.groupby("key").count()["id"].max() > 1:
        raise Exception("There are duplicate piece names")

    if plan:
        return data

    return hasura.execute_with_kwargs(
        queries.UPDATE_BODY, body=body, body_pieces=data.to_dict("records")
    )


@retry(wait=wait_random_exponential(multiplier=2, max=10), stop=stop_after_attempt(3))
def _save_meta_one_as_style(
    style_name, meta_one_or_uri, created_at, force_upsert_style_header=False
):
    from res.flows.meta.marker import MetaMarker

    if isinstance(meta_one_or_uri, str):
        meta_one_or_uri = MetaMarker(meta_one_or_uri)

    # meta one implement the contract
    record = {
        "style_name": style_name,
        "body_version": meta_one_or_uri.body_version.replace("v", ""),
        "color_code": meta_one_or_uri._meta["color_code"],
        "sku": meta_one_or_uri.sku,
        "body_code": meta_one_or_uri.body,
        "sizes": [meta_one_or_uri.size],
        "piece_name_mapping": meta_one_or_uri.get_piece_name_mapping(),
        "created_at": created_at,
    }

    res.utils.logger.info(
        f"Saving the meta one for style {style_name} using {meta_one_or_uri._home_dir}"
    )

    return update_style_sizes_pieces_from_record(
        record,
        lookup_body_piece_names=True,
        force_upsert_style_header=force_upsert_style_header,
    )


# @retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def update_style_sizes_pieces_from_record(
    record,
    force_upsert_style_header=False,
    lookup_body_piece_names=False,
    body_version=None,
    hasura=None,
):
    """

    WIP

    requires only a fully resolve piece name mapping

    force_upsert_style_header: we generally assume the style headers are created in advance but for some errors we can then force insert it
    for example if we get a meta one status we can see if there is no header and when we build we can force adding it. but in batch mode we never check

    normally we want the NET DXF / Body to by all or nothing but in practice some times sizes are aprtiall onboarded
    this means we will have the style, possibly the DXA color assets but not the body asset so the function below fails on some sizes and not others

    this record should have the style but also the style pieces mapped
     {
       "[PIECE CODE]" : {
          'material_code',
          'artwork_uri',
          'color_code',
          'is_color_placement',
          'uri' ?? # the URI is the named piece if we can fully process the DXA asset -> IF we have placement types that are directional then this is required,
          'custom X',
          'cardinality'
          'metadata'.offset_size_inches
       }
    }

    """

    offset_size_map = get_offset_size_map()

    def get_style_size_history(garment_id, pieces):
        """ """
        return [
            {
                # refactor but this only happens here - never generate this uuid normally in non shared functions
                "id": res.utils.uuid_str_from_dict(
                    {
                        "style_size_id": garment_id,
                        "piece_id": p,
                    }
                ),
                "style_size_id": garment_id,
                "piece_id": p,
            }
            for p in [p["id"] for p in pieces]
        ]

    def get_color_pieces(
        style_size_id,
        bid,
        pieces_mapping,
        size_code,
        color_code=None,
        body_version=None,
        lookup_body_piece_names=False,
    ):
        """
        this generates style pieces in the correct schema - link to body id and add color pieces to the style
        note these pieces can just be added as universally unique things and the id is resolved outside when reating the history table

        body verison can be passed in to build a specific body version
        """

        def trim_safety(x):
            # a bug in piece name conventions - masking here
            return x.split("_")[0]

        if lookup_body_piece_names:
            res.utils.logger.info(
                f"Resolving piece keys for the body {body_code} {body_version} {size_code}"
            )
            pcs = queries.get_body_pieces(body_code, body_version, size_code)
            # this is a safety that we need for this one lookup use case because they key has _P for example as used in VS so we dont want to resolve the ids that way
            # NOTE the ids still are a function f(bid, clean_key, size_code) so that label should not have a suffix _P
            pcs = {k.split("_")[0]: v for k, v in pcs.items()}

            keys = pcs.keys()  # pieces_mapping.keys()
            # print(pieces_mapping)
            # print(pcs)
            for k in keys:
                # masking a bug in saved meta one names - the piece mapping which is right does not know about it
                # it was nicer to lead with the meta one to filter out stampters etc. but we need to invert so we can remove extra chars from the db names
                # if we dont clear, we then allow things that should be there to be omitted
                pid = pcs[k]
                k = trim_safety(k)
                if k in pieces_mapping:
                    pieces_mapping[k]["id"] = pid

        return [
            {
                # style piece id is a function of the style and the key
                "id": _piece_id_from_style_size_and_piece_key(style_size_id, k),
                # the body piece id is a function of the body id (which is a function of version) and the piece key and size
                # but in another mode it could be looked up
                "body_piece_id": v.get(
                    "id", _body_piece_id_from_body_id_key_size(bid, k, size_code)
                ),
                "artwork_uri": v["artwork_uri"] if pd.notnull(v["artwork_uri"]) else "",
                "material_code": v["material_code"],
                "color_code": v.get("color_code", color_code),
                "base_image_uri": v["base_image_uri"],
                # "type": v.get("type", DxfFile.type_from_piece_name(k)),
                # the color is placed with a certain size but this would need to be updated dynamically so is a little risky here
                "metadata": {
                    "offset_size_inches": offset_size_map.get(v["material_code"], 0)
                },
            }
            for k, v in pieces_mapping.items()
        ]

    hasura = hasura or res.connectors.load("hasura")

    # temp loading offset size map

    name = record["style_name"]
    # TODO can we have mutliple colors and do we care beyond what the artwork is
    inferred_color_code = record["sku"].split(" ")[2].strip()
    inferred_body_code = record["sku"].split(" ")[0].strip()
    color_code = record.get("color_code", inferred_color_code)
    body_code = record.get("body_code", inferred_body_code)
    if body_version:
        res.utils.logger.warn(
            f"Using the passed in body version {body_version} instead of the one on the style - the body lookup option is {lookup_body_piece_names}"
        )
    body_version = body_version or int(float(record["body_version"]))
    record["body_version"] = body_version
    sizes = record.get("size_code", record.get("sizes"))
    if isinstance(sizes, str):
        sizes = [a.strip() for a in sizes.split(",")] if "," in sizes else [sizes]

    pieces_mapping = record["piece_name_mapping"]

    # cast to a dictionary of keys - either input is supported once it has a key
    if isinstance(pieces_mapping, list):
        pieces_mapping = {pm["key"]: pm for pm in pieces_mapping}

    # assumed to exists as is the stype size
    sid = _style_id_from_name(name, body_code)
    bid = _body_id_from_body_code_and_version(body_code, body_version)

    # add artwork and material and we are done
    for size in sizes:
        try:
            # we could update state on the style and style size later - ignore for now
            pieces = []
            # usually over piece sets -> style id size code is the way it must work
            style_size_id = _style_size_id_from_style_id_and_size(sid, size)
            # resolve the pieces from the body
            pieces += get_color_pieces(
                style_size_id,
                bid,
                pieces_mapping,
                size,
                color_code=color_code,
                body_version=body_version,
                lookup_body_piece_names=lookup_body_piece_names,
            )
            style_size_history = get_style_size_history(style_size_id, pieces)
            #         print(style_size_history)
            #         print(sid,bid, size, pieces)

            # the upsert mutation should set the style to ACTIVE if we can successfuly add pieces. it should fail in client if not perfect
            # at this stage we are expecting style headers to always exist but we can retry with style headers for some errors
            if force_upsert_style_header:
                res.utils.logger.debug(f"Forcing first that the style header exists")
                style_model_upsert_style_header(record, body_version=body_version)

            presult = hasura.execute_with_kwargs(
                queries.BATCH_INSERT_GARMENT_PIECES,
                pieces=pieces,  # the garment id is used in the mutation
                garment_id=style_size_id,
                history=style_size_history,
                ended_at=res.utils.dates.utc_now_iso_string(None),
            )
            res.utils.logger.info(
                f"""Wrote style pieces for sku [{record['sku']}] ("{record['style_name']}") """
            )  # Response is {presult}
            metric("STYLE", "ADD", "OK")
        except Exception as ex:
            metric("STYLE", "ADD", "FAILED")
            # res.utils.logger.info(f"Failed to add pieces {pieces} to body {bid}")
            # TODO - add explainer
            # 1 if `pieces_body_piece_id_fkey` is violated it is because a body piece we think should exists by (bid,piece-key,size-code) does not exist.
            #  this may be becasue there is nothign for the body, size or piece. body level is usually oversight and we can add. size may be a validation error at source for body. likewise for piece but more subtle e.g. a misnamed piece in one flow or another
            # InvalidStyleException(key, data = {'piece_mapping' ..., 'piece_insert' ... 'bid, 'garment_id'})
            raise ex


def _update_products(named_products, plan=False, **kwargs):
    """
    named_products{ name, sku, body_code }
    For testing we will seed the products with named styles
    this just needs any set of names with a sku BMC
    for example this could be loaded from histories

    we store for each possible product, a link to the style
    we assume the style is a function of name and body code which can change
    """

    def _product_id_from_name(name, sku):
        """
        Convention for generating an id to make names unique and a business handle to the style
        """
        return res.utils.uuid_str_from_dict(
            {"name": name.lstrip().rstrip(), "sku": sku}
        )

    # assume dataframe here

    named_products["style_id"] = named_products.apply
    named_products["id"] = named_products.apply
    named_products["sku"] = named_products.apply

    for product in named_products[["id", "sku", "style_id"]].to_dict("records"):
        queries.update_products([product])

    raise Exception("Not implemented")


def _body_asset_version(s):
    if isinstance(s, dict) and len(s):
        # take any - it should not make sense to mix versions on valid assets
        f = list(s.values())[0]
        return int(f.split("/")[-2].replace("v", ""))


def get_dxa_asset(sku):
    """
    examples:
        RR-3004 CFT97 OLEAOK 2ZZSM, a combo at body level that has only self pieces at style level
        KT-3034 CFT97 POWDCO 2ZZSM, a normal combo

        KT-3038 CTNBA CRISGP 1ZZXS

    """

    def is_invalidated_factory():
        import json

        """
        for a given SKU on a row, look into the filenames that map out per version and size the status of the marke
        """
        sizes = res.connectors.load("airtable")["appjmzNPXOuynj6xP"][
            "tblvexT7dliEamnaK"
        ]
        sizes = sizes.to_dataframe(
            fields=["_accountingsku", "Size Normalized", "Size Chart"]
        )
        sm = dict(sizes[["record_id", "_accountingsku"]].values)

        def f(row):
            try:
                v = row["Body Version"]
                size = row["SKU"].split(" ")[-1].strip()
                if isinstance(row["filenames"], str):
                    for f in json.loads(row["filenames"]):
                        if sm[f["sizeId"]] == size and v == f.get("bodyVersion", v):
                            return not f.get("wasInvalidated", False)
                return True
            except:
                print(dict(row))
                raise

        return f

    def color_asset_version(s):
        if isinstance(s, list):
            return int(s[0].split("/")[2].replace("v", ""))

    def try_parse_info(s):
        try:
            d = json.loads(s)
            print(d)
            return d
        except Exception as ex:
            print("fail", ex)
            return None

    f = is_invalidated_factory()

    # get the request and the marker
    dxa = get_active_markers_for_skus(sku)
    dxa["is_marker_valid"] = dxa.apply(f, axis=1)

    dxa = dxa[dxa["is_marker_valid"] == True].reset_index(drop=True)

    if len(dxa) == 0:
        res.utils.logger.warn(f"We were unable to find a valid marker")
        return dxa
    for c in ["_color_on_shape_pieces_collection", "NET DXF Info"]:
        if c not in dxa.columns:
            dxa[c] = None

    dxa = dxa[
        [
            "SKU",
            "Body Version",
            "Created At",
            "_color_on_shape_pieces_collection",
            "ONE Marker Flow Status",
            "rank",
            "print_type",
            "is_marker_valid",
            # "NET DXF Info",
        ]
    ]
    dxa["style"] = dxa["SKU"].map(lambda x: x[:-6])
    dxa["body"] = dxa["SKU"].map(lambda x: x[:6])
    dxa["ONE Marker Flow Status"] = dxa["ONE Marker Flow Status"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    dxa["SKU"] = dxa["SKU"].map(lambda x: f"{x[:2]}-{x[2:]}")
    dxa["body"] = dxa["body"].map(lambda x: f"{x[:2]}-{x[2:]}")
    dxa["style"] = dxa["style"].map(lambda x: f"{x[:2]}-{x[2:]}")
    dxa.loc[dxa["rank"] == "Primary", "rank"] = "self"
    dxa.loc[dxa["rank"] == "Combo", "rank"] = "combo"
    dxa = dxa.rename(
        columns={
            "SKU": "sku",
            # "NET DXF Info": "net_dxf_info",
            "Body Version": "body_version",
            "Created At": "created_at",
            "_color_on_shape_pieces_collection": "color_pieces",
            "ONE Marker Flow Status": "status",
        }
    )

    primary = dxa[dxa["rank"] == "self"]
    combo = dxa[dxa["rank"] != "self"]

    dxa = pd.merge(
        primary.drop("rank", 1),
        combo.drop(["body_version", "rank", "style", "body"], 1),
        on="sku",
        how="left",
        suffixes=["", "_combo"],
    )
    # TODO add asset versions and resolve the body assets vased on rules and we pretty much have the payload - dxa does not the ful resolution until we determine the piece mapping but we could add piece mapping on DXA
    # dxa["net_dxf_info"] = dxa["net_dxf_info"].map(try_parse_info)
    # dxa["body_asset_version"] = dxa["net_dxf_info"].map(body_asset_version)
    dxa["color_asset_version"] = dxa["color_pieces"].map(color_asset_version).fillna(0)

    return dxa


def get_body_styles_by_size(bodies, only_2d=True):
    """
    takes a while to load but will fetch the skus that are ready for the body
    """
    s = get_styles()
    s = s[s["is_one_ready"] == 1]
    if only_2d:
        s = s[~s["is_3d_onboarded"]]
    s = s[s["body_code"].isin(bodies)]
    s = s.explode("sizes")
    s["key"] = s.apply(lambda row: f"{row['sku']} {row['sizes']}", axis=1)
    return list(s["key"].unique())


def determine_piece_type(key):
    return "self"


def piece_info_from_2dm1_file(data, **kwargs):
    """
    this is in the schema of the piece/style database so maybe we need to move this
    """
    if isinstance(data, str):
        data = pd.read_feather(data)
    pi = {}
    for record in data.to_dict("records") if not isinstance(data, list) else data:
        key = record["key"]
        pi[key] = {
            "artwork_uri": record["artwork_uri"],
            "material_code": record["material_code"],
            "base_image_uri": record["uri"],
            # determine piece type
            "type": determine_piece_type(key),
        }
    return pi


def get_2dm1_request_payload(skus, as_df=False, request_now=False):
    """
    Assuming we can trust the NET DXF loaded into the DXA request, this can be used to combine the DXA request and style data to make a 2dm1 request


    If you have a record you can add a piece mapping to it and save an entire style object to the database as follows
    This is done we post a payload to kafka and process it - we save data in multiple places including the meta database

        data = piece_info_from_2dm1_file(process_dxa_asset(record))
        record['piece_name_mapping'] = data
        update_style_sizes_pieces_from_record(record)
    """

    from res.flows.dxa.setup_files import net_dxf_location_cache

    cache = net_dxf_location_cache()

    def load_net_dxf_info(row):
        return cache.get(
            row["body_code"], row["body_version"], skus.split(" ")[-1].strip()
        )

    def make_metadata(row):
        d = {}
        for c in [
            "is_marker_valid",
            "is_marker_valid_combo",
            "material_code",
            "material_code_combo",
            "artwork_uri",
        ]:
            d[c] = row.get(c)
        if isinstance(row["style_pieces"], dict):
            d["piece_mapping"] = json.dumps(row["style_pieces"])

        if not row["has_multiple_material"]:
            net = row["net_dxf_info"]
            if isinstance(net, dict) and "combo" in net:
                d["added_combo_body_asset"] = net["combo"]

        d = {k: v if pd.notnull(v) else None for k, v in d.items()}

        if request_now:
            kafka = res.connectors.load("kafka")
            kafka["res_meta.dxa.legacy_meta_one_request"].publish(
                d, use_kgateway=True, coerce=True
            )
        return d

    def combo_asset_multi_materials(row):
        if row["has_multiple_material"]:
            net = row["net_dxf_info"]
            if isinstance(net, dict):
                return net.get("combo")

    s = get_styles(skus)
    s["style"] = s["res_key"].map(lambda s: s if "-" in s else f"{s[:2]}-{s[2:]}")
    s["has_multiple_material"] = s["materials_used"].map(
        lambda l: isinstance(l, list) and len(l) > 1
    )

    try:
        dxa = get_dxa_asset(skus)
        # note the sku is dropped because it has not been expanded to the size yet bt the DXA one is
        df = pd.merge(
            s.drop("sku", 1), dxa, how="left", on="style", suffixes=["", "_dxa"]
        )
    except Exception as ex:
        res.utils.logger.info(
            f"Failed to load DXA - for directionals we will continue but {ex}"
        )
        df = s

    df["net_dxf_info"] = df.apply(load_net_dxf_info, axis=1)
    df["body_asset_version"] = df["net_dxf_info"].map(_body_asset_version)
    df["body_asset"] = df["net_dxf_info"].map(
        lambda x: x.get("self") if isinstance(x, dict) else None
    )
    df["body_asset_combo"] = df.apply(combo_asset_multi_materials, axis=1)
    # if we are only using one material, remove the body_asset_combo and put it in the metadata as added combo
    df["metadata"] = df.apply(make_metadata, axis=1)
    df = df.where(pd.notnull(df), None)
    df["id"] = df.index.map(res.utils.res_hash)
    df["created_at"] = res.utils.dates.utc_now_iso_string()

    # checks (especially in the metadata which is a more dluid contract):
    # 1. if its directional and there is no artwork uri, we cannot generate color. for placements its dodgy but we can use the placed color files and ignore the artwork
    # 2. if there are two DXA body assets but only one material, its odd so we need to put the added_combo_body_asset into the metadata
    # 3. for knits it is important to add the offset sizes here or we will not catch it in the processor

    return df if as_df else df.to_dict("records")


##############
### . functions for workflows and generating payloads
##############


def get_named_pieces(
    body_code,
    body_version,
    size_code,
    color_file_names,
    allow_missing_uris=False,
    **kwargs,
):
    """
    hybrid roll of asset lookups
    - body asset names from cache for self and combo - resolves to full key set of piece names
    - color files can be passed in and we try to match them
    """
    try_naming = kwargs.get("try_naming", True)
    offset_size_inches = kwargs.get("offset_size_inches")
    offset_size_inches_combo = kwargs.get("offset_size_inches_combo")

    from res.flows.dxa.setup_files import net_dxf_location_cache

    # access to a cache of net dxf file locations
    cc = net_dxf_location_cache()
    s3 = res.connectors.load("s3")
    DPI = 300
    thresholds = 1000
    if offset_size_inches or offset_size_inches_combo:
        thresholds = 7000

    def name_color_files(files, named_piece_outlines, dist_threshold=thresholds):
        """
        we can pass in some context about where we get the named outlines from as a hint but lets not go there for now
        """
        from res.media.images.outlines import name_part, get_piece_outline

        files = list(set(files))
        assert len(files) == len(
            named_piece_outlines
        ), f"the number of files does not match the number of names: {len(files)} != {len(named_piece_outlines)}"
        named = {}
        for idx, p in enumerate(files):
            g = get_piece_outline(s3.read(p))
            name, confidence = name_part(
                g, named_piece_outlines, idx=idx, min_dist=dist_threshold
            )
            named[name] = p
        assert len(named) == len(
            files
        ), "we did not assign unique names to all the files"
        assert (
            len(named_piece_outlines) == 0
        ), "we did not assign names to all the files"

        return named

    # get the files
    files = cc.get(body_code, body_version, size_code)
    if files is None:
        raise BodyAssetMissingException(
            f"The BODY assets do not exists for {body_code, body_version, size_code} so we cannot build a complete set of piece names"
        )

    self_buffer = lambda g: (
        g
        if pd.isnull(offset_size_inches)
        else Polygon(g).buffer(offset_size_inches * DPI).exterior
    )
    combo_buffer = lambda g: (
        g
        if pd.isnull(offset_size_inches_combo)
        else Polygon(g).buffer(offset_size_inches_combo * DPI).exterior
    )

    _, self_pieces = DxfFile.make_setup_file_svg(
        files["self"], return_shapes=True, ensure_valid=True
    )
    self_pieces["buffered_outline"] = self_pieces["outline"].map(self_buffer)

    combo_pieces = files.get("combo")
    if combo_pieces:
        _, combo_pieces = DxfFile.make_setup_file_svg(
            files["combo"], return_shapes=True, ensure_valid=True
        )
        combo_pieces["buffered_outline"] = combo_pieces["outline"].map(combo_buffer)

    all_pieces = pd.concat([combo_pieces, self_pieces])
    piece_keys = all_pieces["key"]
    if len(list(piece_keys)) != len(list(set(piece_keys))):
        raise FailedPieceNamingException(
            f"The pieces in the body asset {body_code, body_version, size_code} are not unique"
        )

    if try_naming:
        # take all the pieces that are self and try to name them with just the self pieces
        # then add the combo pieces to the mix and try to name the rest of the pieces
        named_outlines = dict(self_pieces[["key", "buffered_outline"]].values)
        # assign names
        if combo_pieces is not None:
            more_outlines = dict(combo_pieces[["key", "buffered_outline"]].values)
            named_outlines.update(more_outlines)
        # assign more names
        try:
            res.utils.logger.info(
                f"Naming pieces. Offset used: {offset_size_inches} Combo offset {offset_size_inches_combo}..."
            )
            # return named_outlines, color_file_names
            named_files = name_color_files(color_file_names, named_outlines)
            return named_files
        except Exception as ex:
            if not allow_missing_uris:
                raise FailedPieceNamingException(
                    f"Failed to name color pieces for non-directional style {kwargs.get('sku')}, {ex}"
                )

    return {k: None for k in all_pieces["key"]}


def compile_piece_info(sized_style_record, body_version=None, try_naming=True):
    """

    A kafka payload will take a style header and a bunch of piece details to save to our database - this compiles this structure and calls out to resolve assets

    This should be treated as a temporary thing. In the limit we dont need to name pieces because we will only support 2d directionals if even
    The normal flow will be to receive an update on style pieces changed and not require any URI on placed pieces
    IF we do, we should resolve the URIs inline

    the style record has the data without the assets
    - load the body assets which provide piece names completely
    - load the placed color if given
    - optinally match the pieces

    we only need to name pieces if we are doing 2d placements which is unlikely
    createing all the valid directional cases makes most sense - but maybe we can cheery pick a few anyway to test

    Examples:
    Classes
    - combo_custom_woven_with_pieces: KT-2045 CT406 ABSTSH ZZZ22

    """

    ##  CONTRACT   ################
    # color_pieces, color_pieces_combo should beprovided if we wish to resolve names
    material_props = {}
    sku = sized_style_record["sku"]
    style_sku = sized_style_record["style"]
    body_code = sized_style_record["body_code"]
    body_version = body_version or int(float(sized_style_record["body_version"]))
    res.utils.logger.info(f"Compiling style for body version {body_version}")
    sized_style_record["body_version"] = body_version
    style_name = sized_style_record["style_name"].strip()
    size_code = sized_style_record["size_code"]
    material_code = sized_style_record["material_code"]
    color_code = sized_style_record["color_code"]
    artwork_uri = sized_style_record.get("artwork_uri")
    style_pieces = sized_style_record.get("style_pieces", {})
    if pd.isnull(style_pieces):
        style_pieces = {}
    # required in contract to validate if we need to assert on missing uri
    print_type = sized_style_record["print_type"]
    # these are part of contract if given and passed in kwargs to get named piecesd
    offset_size_inches = sized_style_record.get("offset_size_inches")
    offset_size_inches_combo = sized_style_record.get("offset_size_inches_combo")
    # optional
    color_file_names = list(sized_style_record.get("color_pieces") or [])
    color_file_names += list(sized_style_record.get("color_pieces_combo") or [])
    color_file_names = [f"s3://meta-one-assets-prod/{f}" for f in color_file_names]

    named_files = {}
    ################################
    # qualify with body version

    style_pieces = {
        f"{body_code}-V{body_version}-{k}": v for k, v in style_pieces.items()
    }

    # this thing serves two roles - just loose piece names which we need but also try to name uris if we can
    # for directionals its not an error to not have the uris otherwise it is
    named_files = get_named_pieces(
        color_file_names=color_file_names,
        allow_missing_uris=print_type == "Directional",
        try_naming=try_naming,
        **sized_style_record,
    )

    pieces = {}

    # setup the default pieces from the style BMC - later we overlay the style pieces data
    for k in named_files.keys():
        # set defaults
        pieces[k] = {
            "material_code": material_code,
            "artwork_uri": artwork_uri,
            "color_code": color_code,
            "base_image_uri": named_files.get(k),
            "offset_size_inches": 0,
            # "type": DxfFile.type_from_piece_name(k), #type is know to the body but we may add it here later
        }
    # overlay the properties mapped in the style pieces
    for k, v in style_pieces.items():
        # merge in values from the style def
        pieces[k].update(v)
        pieces[k]["offset_size_inches"] = material_props.get(
            pieces[k]["material_code"], 0
        )

    # as list
    for k, v in pieces.items():
        v["key"] = k
    pieces = list(pieces.values())

    # PIECE KEY VALIDATION:
    # -no nulls coming from the style
    # -the correct number shuld be determined in the read files thing

    # THIS IS THE KAFKA SCHEMA
    # we add material offsets as metadata and we add color files for placements so that we can resolve the pieces too
    return {
        "id": res.utils.uuid_str_from_dict({"sku": sku, "size_code": size_code}),
        "style_name": style_name,
        "sku": sku,
        "size_code": size_code,
        "style_sku": style_sku,
        "body_code": body_code,
        "body_version": body_version,
        "color_pieces": color_file_names,
        "piece_name_mapping": pieces,
        "print_type": print_type,
        "created_at": res.utils.dates.utc_now_iso_string(),
        "metadata": {},
    }


def compile_piece_info_for_style_size(
    sku, preview=False, try_naming=True, body_version=None
):
    """
    sku: e.g. TK-3001 CDCBM MARIDA 2ZZSM
    one level above compiple pieces;
    - first we get the style, split it by size and merge the DXA asset if we can
    - we can return the payload preview or we can go and build
    - building means getting the body asset if we can and matching shapes if we can
    this returns a complete piece info mapping that can be used to request a meta one
    """
    assert (
        len(sku.split(" ")) == 4
    ), "A 4-part sku BMCS is needed to resolve a style by size"
    s = get_styles(queries.style_sku(sku)).explode("sizes")
    s = s.explode("sizes")
    s = s[s["sizes"] == sku.split(" ")[-1].strip()]
    s["size_code"] = s["sizes"]
    s["style"] = s["sku"]
    s["sku"] = s.apply(lambda row: f"{row['sku']} {row['sizes']}", axis=1)
    s["color_pieces"] = None
    s = dict(s.iloc[0])

    dxa_record = {}
    try:
        dxa = get_dxa_asset(sku)
        dxa_record = dxa.iloc[0]
        if isinstance(dxa_record["color_pieces"], list):
            color_pieces = dxa_record["color_pieces"]
            s["marker_valid"] = dxa_record["is_marker_valid"]
            if isinstance(dxa_record["color_pieces_combo"], list):
                color_pieces += dxa_record["color_pieces_combo"]
            s["color_pieces"] = color_pieces

    except Exception as ex:
        res.utils.logger.debug(f"Could not merge dxa assets {ex}")

    return (
        compile_piece_info(s, try_naming=try_naming, body_version=body_version)
        if not preview
        else s
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def get_style_size_status(sku, body_version=None, hasura=None):
    """
    convenience to quickly lookup the kind of checks we would do to validate a style
    """
    cc = net_dxf_location_cache()
    hasura = hasura or res.connectors.load("hasura")
    body = sku.split(" ")[0].strip()
    size = sku.split(" ")[-1].strip()

    try:
        # try determine latest version from airtable
        bods = get_bodies(body)
        max_version = bods["version"].max()
        if body_version is None:
            body_version = max_version
    except:
        pass
    rec = cc.get_full_record(body, body_version, size)

    body_valid = False
    flow = None
    try:
        # we can load it from the database, our source of truth. otherwise we can see if we have a cached asset to build from
        b = queries.get_body(
            body_code=body,
            version=body_version,
            size_code=size,
            hasura=hasura,
            flatten=False,
        ).get("meta_bodies")[0]
        flow = b.get("metadata", {}).get("flow")
        body_valid = True
    except Exception as ex:
        pass

    if not body_valid:
        if rec:
            body_valid = rec.get("is_valid", False)

    # we return true, false or None. None is if there is no style header at all -> this should not happen
    style_active = queries.is_style_size_active(sku, hasura=hasura)

    return {
        "style_sku": " ".join(sku.split(" ")[:3]),
        "sku": sku,
        "flow": flow,
        "body_version": body_version,
        "is_body_valid": body_valid,
        "has_style_header": style_active is not None,
        "is_style_active": style_active == True,
    }


# repassed shoud work for the mode where we want to assign uris but we have everything else
def add_placed_piece_uris_to_pieces(
    payload, color_file_names, offset_size_inches, offset_size_inches_combo
):
    """
    this is an augmentation method - we suppose we have almost everything compiled on pieces everytime a piece mapping changes and then we want to assocaite the DXA placed color asset afterward
    WE WILL ONLY CARE ABOUT THIS FOR PLACEMENTS BECAUSE DIRECTIONAL IS AUTO
    supposed we already have a payload with easy to scrape style piece data
    now we can match the colorfiles to the payload by just providing the expected offset size for the materials
    in this case we dont try to be to clever about failure modes but we pass in if its directional and allowed so that we still get back the keys from the underlying function
    this is designed so that there is a hybrid mode of a full key look from the body asset but also a matched piece->uri if we can do it
    """
    print_type = payload.get("print_type")
    inferred_body_code = payload["sku"].split(" ")[0].strip()

    named_files = get_named_pieces(
        body_code=payload.get("body_code", inferred_body_code),
        body_version=payload["body_version"],
        size_code=payload["size_code"],
        color_file_names=color_file_names,
        allow_missing_uris=print_type == "Directional",
        offset_size_inches=offset_size_inches,
        offset_size_inches_combo=offset_size_inches_combo,
    )

    for p in payload["piece_name_mapping"]:
        k = p["key"]
        new_value = named_files.get(k, p["base_image_uri"])
        p["base_image_uri"] = new_value

    return payload


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2))
def try_register_pieces_hash(
    style_id, pieces_hash, piece_material_hash, body_code, created_at, hasura=None
):
    try:
        queries.register_pieces_hash(
            style_id, pieces_hash, piece_material_hash, body_code, created_at, hasura
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"failing to register the piece hash {res.utils.ex_repr(ex)}"
        )


def _rerank_styles(hasura=None):
    """
    we can see if styles need to be re-ranked but this must be done with care as one codes are generated from this
    this is just a snippet for possible use
    """
    from tqdm import tqdm

    M = """mutation nudge_ranks($id: uuid = "", $body_code: String = "") {
          update_meta_pieces_hash_registry(where: {id: {_eq: $id}, rank: {}}, _set: {body_code: $body_code}) {
            returning {
              rank
              id
            }
          }
        }
        """

    Q = """query MyQuery {
      meta_pieces_hash_registry {
        id
        rank
        body_code
        created_at
        updated_at
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    ranks = []

    data = pd.DataFrame(hasura.execute_with_kwargs(Q)["meta_pieces_hash_registry"])
    for record in tqdm(data.to_dict("records")):
        d = dict(record)
        try:
            r = hasura.execute_with_kwargs(
                M, id=record["id"], body_code=record["body_code"]
            )["update_meta_pieces_hash_registry"]["returning"]

            print(r)
            d["new_rank"] = r[0]["rank"]

        except Exception as ex:
            print(ex)
            d["failed"] = True

        ranks.append(d)

    return pd.DataFrame(ranks)
